import { Channel, ChannelModel, connect } from 'amqplib';
import { QueryTypes, Sequelize, Transaction } from 'sequelize';
import { ReconnectConfig, ReconnectLoop } from './backoff';
import { createConsoleLogger, Logger } from './logger';

export interface OutboxRelayConfig {
  /** Instancia de Sequelize ya conectada del servicio consumidor. */
  sequelize: Sequelize;
  rabbitmqUrl: string;
  /** Exchange topic donde se publican los eventos (ej. 'crm.events'). */
  exchange: string;
  /** Tabla del outbox. Default 'outbox_messages' (mismo esquema que ya usan
   *  product-service/auth-service-node/billing-service/fiscal-ecuador:
   *  id, type, payload, occurred_at, processed_at). */
  tableName?: string;
  /** Filas por lote en cada drain(). Default 50. */
  batchSize?: number;
  /** Intervalo del timer de respaldo. Ya no es el mecanismo principal -
   *  notify() cubre el caso normal - solo cubre el caso borde en que el
   *  proceso muere entre el commit y el notify(). Default 30000ms. */
  safetyNetIntervalMs?: number;
  /** Ventana (ms) que notify() espera antes de drenar, para que los commits que
   *  lleguen dentro de ella compartan UN solo lote (una transaccion, un commit,
   *  un fsync) en vez de uno por evento. Medido en billing a ~27 RPS: cada
   *  commit disparaba su propio drain, o sea ~1 commit extra por factura, y el
   *  disco (fsync de redo + binlog) era el cuello. 0 = drenar al instante (el
   *  comportamiento de 0.2.2). Default 100ms: el evento sale con esa demora. */
  drainDelayMs?: number;
  reconnect?: ReconnectConfig;
  logger?: Logger;
}

interface OutboxRow {
  id: string;
  type: string;
  payload: unknown;
}

/**
 * Relay del patron Outbox: drena `outbox_messages` y publica a RabbitMQ.
 *
 * - notify() dispara un drain() inmediato (pensado para llamarse desde
 *   tx.afterCommit(), asi el evento sale en milisegundos en el caso normal).
 * - El timer (safetyNetIntervalMs) es solo la red de seguridad para el caso
 *   en que el proceso muere entre el commit y el notify(), o la conexion a
 *   Rabbit no estaba disponible en ese momento.
 * - drain() usa `FOR UPDATE SKIP LOCKED` para que sea seguro correr con
 *   varias replicas del mismo servicio sin publicar el mismo evento dos veces.
 * - Reconexion con backoff exponencial capado; al reconectar vuelve a drenar
 *   por si quedaron filas pendientes mientras Rabbit no estaba disponible.
 */
export class OutboxRelay {
  private readonly sequelize: Sequelize;
  private readonly rabbitmqUrl: string;
  private readonly exchange: string;
  private readonly tableName: string;
  private readonly batchSize: number;
  private readonly safetyNetIntervalMs: number;
  private readonly drainDelayMs: number;
  private readonly logger: Logger;
  private readonly reconnectLoop: ReconnectLoop;

  private model: ChannelModel | null = null;
  private channel: Channel | null = null;
  private safetyNetTimer: ReturnType<typeof setInterval> | null = null;
  private draining = false;
  private drainAgain = false;
  /** Quedan filas de sobra tras un lote lleno: el siguiente drain va sin demora. */
  private drainBacklog = false;
  private drainTimer: ReturnType<typeof setTimeout> | null = null;
  private stopped = false;

  constructor(config: OutboxRelayConfig) {
    this.sequelize = config.sequelize;
    this.rabbitmqUrl = config.rabbitmqUrl;
    this.exchange = config.exchange;
    this.tableName = config.tableName ?? 'outbox_messages';
    this.batchSize = config.batchSize ?? 50;
    this.safetyNetIntervalMs = config.safetyNetIntervalMs ?? 30_000;
    this.drainDelayMs = config.drainDelayMs ?? 100;
    this.logger = config.logger ?? createConsoleLogger('[outbox-relay]');
    this.reconnectLoop = new ReconnectLoop(() => this.connectOnce(), config.reconnect);
  }

  async start(): Promise<void> {
    this.stopped = false;
    await this.connectOnce().catch((err) => {
      this.logger.error('no se pudo conectar a RabbitMQ, reintentando', { err: String(err) });
      this.reconnectLoop.trigger();
    });

    this.safetyNetTimer = setInterval(() => this.notify(), this.safetyNetIntervalMs);
    this.notify();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.reconnectLoop.stop();
    if (this.safetyNetTimer) clearInterval(this.safetyNetTimer);
    if (this.drainTimer) clearTimeout(this.drainTimer);
    this.drainTimer = null;
    await this.channel?.close().catch(() => undefined);
    await this.model?.close().catch(() => undefined);
  }

  /** Enganchar a una transaccion: publica solo si el commit fue exitoso. */
  attachToTransaction(tx: Transaction): void {
    tx.afterCommit(() => this.notify());
  }

  /**
   * Dispara un drain() inmediato. Coalesced: si ya hay un drain en curso,
   * marca que hace falta correr otro mas apenas termine (no dispara N en
   * paralelo).
   */
  notify(): void {
    if (this.draining) {
      this.drainAgain = true;
      return;
    }
    if (this.drainDelayMs > 0 && !this.drainBacklog) {
      // Ya hay un drain agendado: este evento entra en ese mismo lote.
      if (this.drainTimer) return;
      this.drainTimer = setTimeout(() => {
        this.drainTimer = null;
        this.runDrain();
      }, this.drainDelayMs);
      return;
    }
    this.runDrain();
  }

  private runDrain(): void {
    if (this.draining) {
      this.drainAgain = true;
      return;
    }
    this.draining = true;
    this.drainBacklog = false;
    this.drain()
      .catch((err) => this.logger.error('error al drenar outbox', { err: String(err) }))
      .finally(() => {
        this.draining = false;
        if (this.drainBacklog) {
          // Lote lleno: quedan filas pendientes, se sigue sin demora.
          this.runDrain();
        } else if (this.drainAgain) {
          // Llegaron commits mientras drenaba: nuevo lote, con la ventana de agrupacion.
          this.drainAgain = false;
          this.notify();
        }
      });
  }

  private async connectOnce(): Promise<void> {
    this.model = await connect(this.rabbitmqUrl);
    this.channel = await this.model.createChannel();
    await this.channel.assertExchange(this.exchange, 'topic', { durable: true });

    this.model.on('close', () => {
      if (this.stopped) return;
      this.logger.warn('conexion a RabbitMQ cerrada, reconectando...');
      this.channel = null;
      this.model = null;
      this.reconnectLoop.trigger();
    });
    this.model.on('error', (err) => {
      this.logger.error('error de conexion a RabbitMQ', { err: String(err) });
    });

    this.logger.info('conectado a RabbitMQ', { exchange: this.exchange });
    // Puede haber quedado trabajo pendiente mientras no habia conexion.
    this.notify();
  }

  private async drain(): Promise<void> {
    if (!this.channel) return;

    // READ COMMITTED (no el REPEATABLE READ por defecto) a proposito: bajo
    // REPEATABLE READ, el FOR UPDATE de mas abajo sobre `processed_at IS
    // NULL` toma un next-key lock que cubre el "bucket" NULL del indice
    // secundario, y bloquea cualquier INSERT nuevo con processed_at NULL
    // (o sea, cada evento nuevo que otro proceso intente encolar) mientras
    // esta transaccion siga abierta - incluyendo el tiempo que tarda el
    // loop de publish() a RabbitMQ de aqui abajo. Bajo READ COMMITTED,
    // InnoDB no toma gap locks en busquedas por indice secundario no unico,
    // solo bloquea las filas que realmente selecciona - el SKIP LOCKED
    // sigue evitando que dos replicas publiquen el mismo evento dos veces,
    // pero deja de frenar los INSERT de otros procesos. Medido en
    // stress-petitions/billing: sin esto, un INSERT concurrente a un drain()
    // en curso esperaba el commit completo (hasta varios segundos bajo
    // carga); con esto, no espera al lock de rango, solo al de fila si
    // coincide una en curso (raro, y ya cubierto por SKIP LOCKED).
    await this.sequelize.transaction({ isolationLevel: Transaction.ISOLATION_LEVELS.READ_COMMITTED }, async (t) => {
      const rows = await this.sequelize.query<OutboxRow>(
        `SELECT id, type, payload FROM ${this.tableName}
          WHERE processed_at IS NULL
          ORDER BY occurred_at ASC
          LIMIT :batchSize
          FOR UPDATE SKIP LOCKED`,
        { replacements: { batchSize: this.batchSize }, type: QueryTypes.SELECT, transaction: t },
      );

      const publishedIds: string[] = [];
      for (const row of rows) {
        const published = this.channel!.publish(
          this.exchange,
          row.type,
          Buffer.from(JSON.stringify(row.payload)),
          { persistent: true, headers: { eventId: row.id } },
        );

        if (!published) {
          this.logger.warn('publish devolvio backpressure, se reintenta en el proximo drain', {
            eventId: row.id,
          });
          continue;
        }
        publishedIds.push(row.id);
      }

      // Un solo UPDATE para todo el lote: antes era uno por fila, o sea N idas y
      // vueltas a MySQL con la transaccion (y sus locks) abierta.
      if (publishedIds.length > 0) {
        await this.sequelize.query(
          `UPDATE ${this.tableName} SET processed_at = NOW() WHERE id IN (:ids)`,
          { replacements: { ids: publishedIds }, type: QueryTypes.UPDATE, transaction: t },
        );
      }

      if (rows.length === this.batchSize && publishedIds.length > 0) {
        // Puede haber mas filas de las que trajo el batch: seguir drenando.
        // (Solo si se publico algo: con todo en backpressure reintentar al instante
        // seria un bucle caliente.)
        this.drainBacklog = true;
      }
    });
  }
}
