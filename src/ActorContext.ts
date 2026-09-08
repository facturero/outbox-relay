import { AsyncLocalStorage } from 'node:async_hooks';

/**
 * Quien origina la accion, propagado desde la peticion HTTP hasta el outbox.
 *
 * El problema que resuelve: los eventos publicados dicen QUE paso y CUANDO,
 * pero casi nunca QUIEN lo hizo. Cada caso de uso tendria que arrastrar el
 * userId hasta el `outbox.add()`, y en la practica no lo hace: la bitacora de
 * auditoria acaba con `user_id`, `ip` y `request_id` a NULL.
 *
 * Ojo con la alternativa facil: reutilizar un `userId` que ya venga en el
 * payload NO sirve, porque en los eventos de identidad ese campo es el usuario
 * AFECTADO, no quien ejecuta. Por eso los campos de actor tienen nombre propio
 * (`actorId`, `actorEmail`) y no se mezclan con los del dominio.
 */
export interface ActorContext {
  /** Id del usuario autenticado (claim `sub`, header X-User-Id del gateway). */
  actorId?: string | null;
  actorEmail?: string | null;
  /** IP del cliente tal y como la resolvio el gateway. */
  actorIp?: string | null;
  /** Correlacion de la peticion, para atar varios eventos a una misma accion. */
  requestId?: string | null;
}

const storage = new AsyncLocalStorage<ActorContext>();

/** Ejecuta `fn` con el actor asociado al contexto asincrono (una peticion). */
export function runWithActor<T>(actor: ActorContext, fn: () => T): T {
  return storage.run(actor, fn);
}

/** Actor de la peticion en curso, o undefined fuera de una (p.ej. un consumidor). */
export function getActor(): ActorContext | undefined {
  return storage.getStore();
}

/**
 * Anade los campos de actor al payload de un evento. No pisa lo que el caso de
 * uso haya puesto a mano (un evento que ya sabe su actor manda), ni inventa
 * claves para valores vacios: mejor NULL en la bitacora que un dato inventado.
 */
export function withActor<T extends Record<string, unknown>>(payload: T): T & ActorContext {
  const actor = getActor();
  if (!actor) return payload as T & ActorContext;

  const merged: Record<string, unknown> = { ...payload };
  for (const key of ['actorId', 'actorEmail', 'actorIp', 'requestId'] as const) {
    const value = actor[key];
    if (merged[key] === undefined && value !== undefined && value !== null && value !== '') {
      merged[key] = value;
    }
  }
  return merged as T & ActorContext;
}
