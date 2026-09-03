# @facturero/outbox-relay

Modulo de resiliencia de eventos para los servicios del monorepo facturero
(Node + Hono + Sequelize + MySQL + RabbitMQ). Encapsula dos problemas que
hoy estan resueltos por separado, y de forma incompleta, en cada servicio:

1. **Lado productor (Outbox)** — publicar eventos con delay de hasta 5s
   porque el relay solo drena la tabla `outbox_messages` en un
   `setInterval` fijo, en vez de al momento de escribir.
2. **Lado consumidor (Inbox)** — `channel.nack(msg, false, true)` sin
   limite, que puede dejar un mensaje envenenado reintentando para siempre
   sin escape.

Este paquete no reemplaza RabbitMQ ni Sequelize: opera sobre la conexion y
la tabla que el servicio ya tiene.

## Requisitos (obligatorios, no genericos)

Este paquete **no es multi-dialecto**. Asume la misma arquitectura que ya
usan todos los servicios del monorepo, y lo hace explicito en vez de
pretender portabilidad que no existe:

- **Sequelize `^6.37.5`** — peer dependency. Se usa la instancia que el
  servicio ya tiene, no una nueva.
- **`mysql2` `^3.11.5`** — peer dependency. Sequelize por si solo no trae
  ningun driver de base de datos; sin `mysql2` instalado en el servicio
  consumidor, `sequelize.query(...)` falla en tiempo de ejecucion aunque
  Sequelize este bien configurado.
- **MySQL 8.0+** (no 5.7) — el `drain()` de `OutboxRelay` usa
  `FOR UPDATE SKIP LOCKED`, que MySQL recien soporta desde 8.0. El
  `markOutcome()` de `InboxConsumer` usa `ON DUPLICATE KEY UPDATE`,
  sintaxis exclusiva de MySQL/MariaDB (no es SQL portable a Postgres u
  otro motor).

Ambas son **peer dependencies obligatorias** (`peerDependenciesMeta` las
marca explicitamente como no-opcionales) — si un servicio consumidor no
las tiene instaladas, `npm install` va a advertir el faltante.

## Instalacion

Publicado en GitHub Packages bajo el scope `@facturero`. En el servicio
consumidor, agregar un `.npmrc`:

```
@facturero:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${NODE_AUTH_TOKEN}
```

```bash
npm install @facturero/outbox-relay
```

Ver [Requisitos](#requisitos-obligatorios-no-genericos) arriba para
`sequelize`/`mysql2`. `amqplib` va empaquetado como dependencia normal (uso
interno, sin impacto en la version que ya tenga el servicio para otras
cosas).

## OutboxRelay (productor)

Reemplaza el `OutboxRelay` que hoy esta copiado en 4 servicios
(`product-service`, `auth-service-node`, `billing-service`,
`fiscal-ecuador`). Espera el mismo esquema de tabla que ya usan:

```sql
-- outbox_messages: id (uuid/string), type, payload (json), occurred_at, processed_at (nullable)
```

Uso:

```ts
import { OutboxRelay } from '@facturero/outbox-relay';

export const relay = new OutboxRelay({
  sequelize,
  rabbitmqUrl: config.RABBITMQ_URL,
  exchange: 'crm.events',
  // tableName: 'outbox_messages',       // default
  // batchSize: 50,                      // default
  // safetyNetIntervalMs: 30_000,        // default; ya no es el mecanismo
  //                                      // principal, ver mas abajo
  // reconnect: { initialDelayMs: 5000, maxDelayMs: 60_000 }, // default
});

await relay.start();
```

En cada caso de uso, sin cambiar el unit-of-work que ya usan
(`sequelize.transaction(async (tx) => work(buildRepositories(tx)))`):

```ts
await sequelize.transaction(async (tx) => {
  await work(buildRepositories(tx)); // escribe la entidad + la fila outbox
  relay.attachToTransaction(tx);     // dispara el publish SOLO si el commit fue exitoso
});
```

Por que `afterCommit` y no publicar directo en el caso de uso: si se
publica antes de que la transaccion confirme y esta despues hace rollback,
se manda un evento de algo que nunca paso (el "dual-write problem" del
patron Outbox). `tx.afterCommit()` de Sequelize garantiza que solo se
dispara si el commit fue exitoso.

Con esto el delay pasa de "hasta 5000ms" (el `POLL_INTERVAL` actual) a
milisegundos en el caso normal. El timer (`safetyNetIntervalMs`) se queda
como red de seguridad para el caso borde: el proceso muere entre el commit
y el `notify()`, o RabbitMQ no estaba disponible en ese momento — en ambos
casos la fila queda con `processed_at IS NULL` y el timer (o la reconexion)
la recoge despues.

`drain()` usa `SELECT ... FOR UPDATE SKIP LOCKED`, asi que es seguro correr
con varias replicas del mismo servicio sin publicar el mismo evento dos
veces.

## InboxConsumer (consumidor)

Agrega la escalera de reintentos que hoy no existe (los consumidores
actuales hacen `nack(msg, false, true)` sin techo): reintentos inmediatos
en el mismo proceso -> cola de retry con TTL (delay antes de reintentar,
via dead-letter-exchange) -> estado `failed` en la tabla de idempotencia.

**Por que estado en la DB y no una cola de dead-letter de RabbitMQ**: el
estado final queda en la misma tabla que ya se usa para idempotencia — una
sola fuente de verdad, consultable por SQL directo sin abrir la
management UI de RabbitMQ, y que sobrevive aunque la cola tenga TTL o
limite de tamano. "Reprocesar" un evento fallido es leer su `payload` de
esa fila y volver a llamar al handler — no depende de que el mensaje AMQP
original siga existiendo en alguna cola.

```ts
import { InboxConsumer } from '@facturero/outbox-relay';

const consumer = new InboxConsumer({
  sequelize,
  rabbitmqUrl: config.RABBITMQ_URL,
  exchange: 'crm.events',
  queue: 'notification-service.events',
  bindings: ['identity.user.created', 'auth.user.logged_in'],
  handlers: [
    { eventType: 'identity.user.created', handle: async (payload) => sendWelcomeEmail(payload) },
    { eventType: 'auth.user.logged_in', handle: async (payload) => sendLoginAlert(payload) },
  ],
  // tableName: 'processed_events',      // default, mismo nombre que ya usan
  retry: {
    immediateAttempts: 3,      // default
    immediateDelayMs: 500,     // default
    maxRedeliveries: 5,        // default
    retryTtlMs: 10_000,        // default
  },
  // se invoca cuando un evento agota los reintentos y queda `failed` en la
  // DB - pensado para alertar, no para persistencia (eso ya lo hizo markOutcome).
  // Ejemplo: escribir la propia fila outbox (via el OutboxRelay de este mismo
  // servicio) para publicar 'ops.event.failed' y que notification-service alerte.
  onFailure: async (info) => {
    await sequelize.transaction(async (tx) => {
      await OutboxMessageModel.create({ type: 'ops.event.failed', payload: info }, { transaction: tx });
      relay.attachToTransaction(tx);
    });
  },
});

await consumer.start();
```

Tabla de idempotencia + estado esperada (mismo shape que
`ProcessedEventModel` ya usado en
notification-service/product-service/customer-service, ampliada con
`status`/`last_error`):

```sql
CREATE TABLE IF NOT EXISTS processed_events (
  id VARCHAR(36) PRIMARY KEY,
  event_type VARCHAR(255) NOT NULL,
  routing_key VARCHAR(255) NOT NULL,
  payload JSON NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'processed',  -- 'processed' | 'failed'
  last_error TEXT NULL,
  processed_at DATETIME NOT NULL
);
```

Si un servicio ya tiene esta tabla sin `status`/`last_error`, hace falta
una migracion chica para agregar esas dos columnas (con default
`'processed'` para las filas existentes, ya que hoy toda fila ahi
representa un evento exitoso). Un evento marcado `failed` que se
reprocesa con exito despues actualiza la misma fila a `processed` — no
crea una fila nueva ni requiere borrar nada a mano.

Si el `INSERT`/`UPDATE` de `markOutcome()` falla (blip de conexion justo
en ese momento), el consumidor hace `nack(msg, false, true)` en vez de
`ack` — el evento se reintenta mas tarde en vez de perderse sin dejar
registro en ningun lado.

## Lo que este paquete NO hace (a proposito)

- No reemplaza el observability stack (SigNoz/OTel) que ya corre — no trae
  un canal de eventos de estado propio (socket.io, dashboard, etc.); usa el
  `Logger` inyectado, pensado para conectarse a lo que cada servicio ya
  tenga.
- No incluye rotacion de tablas via SFTP ni cliente SMTP embebido para
  alertar sobre eventos `failed` — para eso ya existe `notification-service`,
  publicando un evento propio y dejando que lo consuma con una plantilla.
- No usa una cola de dead-letter de RabbitMQ para el estado final de un
  evento — ver la seccion de `InboxConsumer` arriba para el porque.
- No fuerza un singleton global — cada servicio instancia su propio
  `OutboxRelay`/`InboxConsumer`, mas facil de testear.

## Build

```bash
npm install
npm run build      # tsc -> dist/
npm run typecheck  # solo type-check, sin emitir
```
