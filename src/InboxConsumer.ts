import { Channel, ChannelModel, ConsumeMessage, connect } from 'amqplib';
import { QueryTypes, Sequelize } from 'sequelize';
import { ReconnectConfig, ReconnectLoop } from './backoff';
import { createConsoleLogger, Logger } from './logger';

/** Handler comodin: se usa cuando ninguna routing key exacta coincide. Pensado
 *  para consumidores catch-all (bitacora de auditoria) que deben procesar todo
 *  lo que llegue sin mantener un catalogo de eventos que se queda viejo. */
export const CATCH_ALL_EVENT_TYPE = '#';

export interface EventHandler {
  /** Routing key exacta que dispara este handler (ej. 'product.product.created'),
   *  o `CATCH_ALL_EVENT_TYPE` ('#') para atender lo que no case con ninguna. */
  eventType: string;
  handle: (payload: unknown, msg: ConsumeMessage) => Promise<void>;
}

export interface EventFailureInfo {
  eventId: string | undefined;
  routingKey: string;
  payload: unknown;
  errors: string[];
}

export interface RetryConfig {
  /** Reintentos inmediatos (en el mismo consume, con delay corto) antes de
   *  escalar a la cola de retry. Default 3. */
  immediateAttempts?: number;
  /** Delay entre reintentos inmediatos. Default 500ms. */
  immediateDelayMs?: number;
  /** Reentregas via la cola de retry antes de marcar el evento como `failed`
   *  en la DB. Default 5. */
  maxRedeliveries?: number;
  /** TTL de la cola de retry: cuanto espera antes de rebotar al exchange
   *  principal via dead-letter-exchange. Default 10000ms. */
  retryTtlMs?: number;
}

export interface InboxConsumerConfig {
  sequelize: Sequelize;
  rabbitmqUrl: string;
  exchange: string;
  queue: string;
  /** Routing key patterns a bindear en la cola principal (ej. ['product.product.#']). */
  bindings: string[];
  handlers: EventHandler[];
  /** Tabla de idempotencia + estado. Default 'processed_events' (mismo
   *  nombre que ya usan notification-service/product-service/customer-service,
   *  ampliada con columnas `status`/`last_error` - ver README). */
  tableName?: string;
  retry?: RetryConfig;
  /** Se invoca cuando un evento agota todos los reintentos y queda marcado
   *  `failed` en la DB. Pensado para notificar (ej. publicar un evento
   *  propio para que notification-service alerte a un humano), no para
   *  persistencia - eso ya lo hace la fila en `tableName`. */
  onFailure?: (info: EventFailureInfo) => Promise<void> | void;
  reconnect?: ReconnectConfig;
  logger?: Logger;
}

const HEADER_REDELIVERY_COUNT = 'x-relay-redelivery-count';

/** La routing key original no sobrevive al rebote por la cola de espera (que
 *  usa una key fija propia del consumidor), asi que viaja en un header. */
const HEADER_ORIGINAL_ROUTING_KEY = 'x-relay-original-routing-key';

/** Key fija con la que se mueve un mensaje dentro del circuito de retry de UN
 *  consumidor. No es una routing key de negocio: nunca sale al exchange comun. */
const RETRY_ROUTING_KEY = 'retry';

/**
 * Consumidor con escalera de reintentos: inmediatos (in-process) -> cola de
 * retry con TTL+DLX (delay antes de reintentar) -> estado `failed` en la
 * tabla de idempotencia.
 *
 * Reemplaza el patron actual de `channel.nack(msg, false, true)` sin limite
 * (que puede dejar un mensaje envenenado reintentando para siempre) por uno
 * con techo explicito. El estado final vive en MySQL (misma tabla que ya se
 * usa para idempotencia), no en una cola de dead-letter de RabbitMQ: queda
 * consultable por SQL, sobrevive aunque la cola tenga TTL/limite de tamano,
 * y "reprocesar" es simplemente volver a llamar al handler con el `payload`
 * ya guardado - no depende de que el mensaje AMQP original siga existiendo.
 */
export class InboxConsumer {
  private readonly sequelize: Sequelize;
  private readonly rabbitmqUrl: string;
  private readonly exchange: string;
  private readonly queue: string;
  private readonly bindings: string[];
  private readonly handlers: Map<string, EventHandler>;
  private readonly tableName: string;
  private readonly retryQueue: string;
  private readonly retryExchange: string;
  private readonly returnExchange: string;
  private readonly retry: Required<RetryConfig>;
  private readonly onFailure?: InboxConsumerConfig['onFailure'];
  private readonly logger: Logger;
  private readonly reconnectLoop: ReconnectLoop;

  private model: ChannelModel | null = null;
  private channel: Channel | null = null;
  private stopped = false;

  constructor(config: InboxConsumerConfig) {
    this.sequelize = config.sequelize;
    this.rabbitmqUrl = config.rabbitmqUrl;
    this.exchange = config.exchange;
    this.queue = config.queue;
    this.bindings = config.bindings;
    this.handlers = new Map(config.handlers.map((h) => [h.eventType, h]));
    this.tableName = config.tableName ?? 'processed_events';
    // TODO el circuito de retry es POR CONSUMIDOR. Antes se compartia un unico
    // `${exchange}.retry` al que todas las colas `.retry` se bindeaban con '#',
    // y cada una devolvia el mensaje al exchange principal: un solo fallo en un
    // servicio reinyectaba el evento N veces a TODOS los consumidores.
    // El nombre de la cola de espera cambia (`.retry` -> `.retry.wait`) porque
    // los argumentos ya no son compatibles y `assertQueue` fallaria sobre la
    // cola vieja. Ver README: hay que borrar las `*.retry` heredadas.
    this.retryQueue = `${config.queue}.retry.wait`;
    this.retryExchange = `${config.queue}.retry`;
    this.returnExchange = `${config.queue}.return`;
    this.retry = {
      immediateAttempts: config.retry?.immediateAttempts ?? 3,
      immediateDelayMs: config.retry?.immediateDelayMs ?? 500,
      maxRedeliveries: config.retry?.maxRedeliveries ?? 5,
      retryTtlMs: config.retry?.retryTtlMs ?? 10_000,
    };
    this.onFailure = config.onFailure;
    this.logger = config.logger ?? createConsoleLogger(`[inbox-consumer:${config.queue}]`);
    this.reconnectLoop = new ReconnectLoop(() => this.connectOnce(), config.reconnect);
  }

  async start(): Promise<void> {
    this.stopped = false;
    await this.connectOnce().catch((err) => {
      this.logger.error('no se pudo conectar a RabbitMQ, reintentando', { err: String(err) });
      this.reconnectLoop.trigger();
    });
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.reconnectLoop.stop();
    await this.channel?.close().catch(() => undefined);
    await this.model?.close().catch(() => undefined);
  }

  private async connectOnce(): Promise<void> {
    this.model = await connect(this.rabbitmqUrl);
    this.channel = await this.model.createChannel();

    await this.channel.assertExchange(this.exchange, 'topic', { durable: true });
    // Ambos exchanges son privados de ESTE consumidor: nada de lo que circule
    // por ellos puede alcanzar a otro servicio.
    await this.channel.assertExchange(this.retryExchange, 'direct', { durable: true });
    await this.channel.assertExchange(this.returnExchange, 'direct', { durable: true });

    await this.channel.assertQueue(this.queue, { durable: true });
    for (const pattern of this.bindings) {
      await this.channel.bindQueue(this.queue, this.exchange, pattern);
    }
    // La vuelta del retry entra por aqui, no por el exchange comun.
    await this.channel.bindQueue(this.queue, this.returnExchange, RETRY_ROUTING_KEY);

    // Cola de espera: sin consumidor propio, expira por TTL y su
    // dead-letter-exchange la devuelve SOLO a la cola de este consumidor.
    // Es unicamente el mecanismo de "esperar antes de reintentar"; el estado
    // final vive en la DB (ver markOutcome()).
    await this.channel.assertQueue(this.retryQueue, {
      durable: true,
      deadLetterExchange: this.returnExchange,
      deadLetterRoutingKey: RETRY_ROUTING_KEY,
      messageTtl: this.retry.retryTtlMs,
    });
    await this.channel.bindQueue(this.retryQueue, this.retryExchange, RETRY_ROUTING_KEY);

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

    await this.channel.consume(this.queue, (msg) => {
      if (!msg) return;
      this.handleMessage(msg).catch((err) => {
        this.logger.error('error inesperado procesando mensaje', { err: String(err) });
        this.channel?.ack(msg);
      });
    });

    this.logger.info('escuchando eventos', { queue: this.queue, bindings: this.bindings });
  }

  private async handleMessage(msg: ConsumeMessage): Promise<void> {
    // Si el mensaje vuelve de la cola de espera, `fields.routingKey` es la key
    // interna del circuito de retry; la de negocio viaja en el header.
    const routingKey =
      (msg.properties.headers?.[HEADER_ORIGINAL_ROUTING_KEY] as string | undefined) ??
      msg.fields.routingKey;
    // Los handlers reciben el `msg` crudo y muchos leen `fields.routingKey`
    // para saber que evento es. Se restaura la key de negocio para que un
    // reintento sea indistinguible de la primera entrega.
    msg.fields.routingKey = routingKey;
    const eventId = msg.properties.headers?.eventId as string | undefined;

    if (eventId && (await this.alreadyProcessed(eventId))) {
      this.channel!.ack(msg);
      return;
    }

    const handler = this.handlers.get(routingKey) ?? this.handlers.get(CATCH_ALL_EVENT_TYPE);
    if (!handler) {
      this.logger.warn('sin handler para este evento, se descarta', { routingKey });
      this.channel!.ack(msg);
      return;
    }

    const payload = JSON.parse(msg.content.toString());
    const errors: string[] = [];

    for (let attempt = 1; attempt <= this.retry.immediateAttempts; attempt++) {
      try {
        await handler.handle(payload, msg);
        if (eventId) await this.markOutcome(eventId, routingKey, payload, 'processed', null);
        this.channel!.ack(msg);
        return;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        errors.push(message);
        this.logger.warn('fallo al procesar, reintentando', {
          routingKey,
          attempt,
          of: this.retry.immediateAttempts,
          err: message,
        });
        if (attempt < this.retry.immediateAttempts) await this.delay(this.retry.immediateDelayMs);
      }
    }

    const redeliveryCount = (msg.properties.headers?.[HEADER_REDELIVERY_COUNT] as number) ?? 0;

    if (redeliveryCount < this.retry.maxRedeliveries) {
      this.channel!.publish(this.retryExchange, RETRY_ROUTING_KEY, msg.content, {
        persistent: true,
        headers: {
          ...msg.properties.headers,
          [HEADER_REDELIVERY_COUNT]: redeliveryCount + 1,
          [HEADER_ORIGINAL_ROUTING_KEY]: routingKey,
        },
      });
      this.logger.warn('escalado a cola de retry', { routingKey, redeliveryCount: redeliveryCount + 1 });
      this.channel!.ack(msg);
      return;
    }

    // Se agotaron todos los reintentos: queda como responsabilidad de un
    // humano. Si no se puede dejar constancia en la DB, NO se hace ack -
    // mejor que RabbitMQ lo reentregue mas tarde a que el evento desaparezca
    // sin que quede registro en ningun lado.
    try {
      if (eventId) {
        await this.markOutcome(eventId, routingKey, payload, 'failed', errors.join('; '));
      }
      this.logger.error('evento marcado como failed, se agotaron los reintentos', { routingKey, errors });
      await this.onFailure?.({ eventId, routingKey, payload, errors });
      this.channel!.ack(msg);
    } catch (err) {
      this.logger.error('no se pudo registrar el evento como failed, se reintenta mas tarde', {
        routingKey,
        err: err instanceof Error ? err.message : String(err),
      });
      // Se espera ANTES de devolverlo a la cola. Sin la pausa, un fallo
      // permanente al escribir en la tabla (no solo un blip de conexion) hacia
      // girar el mensaje miles de veces por minuto contra la DB. Se sigue
      // prefiriendo reencolar a perder el evento, pero a ritmo humano.
      await this.delay(this.retry.retryTtlMs);
      this.channel!.nack(msg, false, true);
    }
  }

  private async alreadyProcessed(eventId: string): Promise<boolean> {
    const rows = await this.sequelize.query(
      `SELECT id FROM ${this.tableName} WHERE id = :id AND status = 'processed' LIMIT 1`,
      { replacements: { id: eventId }, type: QueryTypes.SELECT },
    );
    return rows.length > 0;
  }

  /**
   * Registra el resultado final de un evento (exito o fallo permanente) en
   * la tabla de idempotencia. Un evento previamente marcado `failed` que se
   * reprocesa con exito despues (replay manual, o el propio flujo si el
   * routing key se volviera a publicar) actualiza la misma fila a `processed`.
   */
  private async markOutcome(
    eventId: string,
    eventType: string,
    payload: unknown,
    status: 'processed' | 'failed',
    lastError: string | null,
  ): Promise<void> {
    // Se recorta a la anchura de las columnas ANTES de insertar. Sin esto, un
    // routing key largo (event_type es VARCHAR(100)) hacia fallar el INSERT; el
    // catch de handleMessage lo interpretaba como fallo transitorio y hacia
    // nack(requeue) -> el mensaje volvia, fallaba igual, y se quedaba en un
    // bucle caliente para siempre. Justo la clase de mensaje envenenado que
    // este paquete existe para evitar. Los dos campos son copias del routing
    // key para consulta, no claves: recortarlos no pierde nada relevante.
    await this.sequelize.query(
      `INSERT INTO ${this.tableName} (id, event_type, routing_key, payload, status, last_error, processed_at)
       VALUES (:id, :eventType, :routingKey, :payload, :status, :lastError, NOW())
       ON DUPLICATE KEY UPDATE
         status = VALUES(status),
         last_error = VALUES(last_error),
         processed_at = VALUES(processed_at)`,
      {
        replacements: {
          id: eventId.slice(0, 36),
          eventType: eventType.slice(0, 100),
          routingKey: eventType.slice(0, 200),
          payload: JSON.stringify(payload),
          status,
          lastError: lastError === null ? null : lastError.slice(0, 2000),
        },
        type: QueryTypes.INSERT,
      },
    );
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
