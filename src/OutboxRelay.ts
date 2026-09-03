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
  private readonly logger: Logger;
  private readonly reconnectLoop: ReconnectLoop;

  private model: ChannelModel | null = null;
  private channel: Channel | null = null;
  private safetyNetTimer: ReturnType<typeof setInterval> | null = null;
  private draining = false;
  private drainAgain = false;
  private stopped = false;

  constructor(config: OutboxRelayConfig) {
    this.sequelize = config.sequelize;
    this.rabbitmqUrl = config.rabbitmqUrl;
    this.exchange = config.exchange;
    this.tableName = config.tableName ?? 'outbox_messages';
    this.batchSize = config.batchSize ?? 50;
    this.safetyNetIntervalMs = config.safetyNetIntervalMs ?? 30_000;
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
    this.draining = true;
    this.drain()
      .catch((err) => this.logger.error('error al drenar outbox', { err: String(err) }))
      .finally(() => {
        this.draining = false;
        if (this.drainAgain) {
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

    await this.sequelize.transaction(async (t) => {
      const rows = await this.sequelize.query<OutboxRow>(
        `SELECT id, type, payload FROM ${this.tableName}
          WHERE processed_at IS NULL
          ORDER BY occurred_at ASC
          LIMIT :batchSize
          FOR UPDATE SKIP LOCKED`,
        { replacements: { batchSize: this.batchSize }, type: QueryTypes.SELECT, transaction: t },
      );

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

        await this.sequelize.query(
          `UPDATE ${this.tableName} SET processed_at = NOW() WHERE id = :id`,
          { replacements: { id: row.id }, type: QueryTypes.UPDATE, transaction: t },
        );
      }

      if (rows.length === this.batchSize) {
        // Puede haber mas filas de las que trajo el batch: seguir drenando.
        this.drainAgain = true;
      }
    });
  }
}
