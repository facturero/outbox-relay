export interface ReconnectConfig {
  /** Delay before the first reconnect attempt. Default 5000ms. */
  initialDelayMs?: number;
  /** Upper bound for the exponential backoff. Default 60000ms. */
  maxDelayMs?: number;
}

/**
 * Reconexion con backoff exponencial capado. No dispara dos intentos en
 * paralelo: mientras uno esta en curso, llamadas adicionales a trigger()
 * son ignoradas.
 */
export class ReconnectLoop {
  private readonly initialDelayMs: number;
  private readonly maxDelayMs: number;
  private currentDelayMs: number;
  private reconnecting = false;
  private stopped = false;

  constructor(
    private readonly attempt: () => Promise<void>,
    config: ReconnectConfig = {},
  ) {
    this.initialDelayMs = config.initialDelayMs ?? 5000;
    this.maxDelayMs = config.maxDelayMs ?? 60000;
    this.currentDelayMs = this.initialDelayMs;
  }

  reset(): void {
    this.currentDelayMs = this.initialDelayMs;
  }

  stop(): void {
    this.stopped = true;
  }

  trigger(): void {
    if (this.reconnecting || this.stopped) return;
    this.reconnecting = true;
    setTimeout(() => this.run(), this.currentDelayMs);
  }

  private async run(): Promise<void> {
    if (this.stopped) {
      this.reconnecting = false;
      return;
    }
    try {
      await this.attempt();
      this.reconnecting = false;
      this.reset();
    } catch {
      this.currentDelayMs = Math.min(this.currentDelayMs * 2, this.maxDelayMs);
      this.reconnecting = false;
      this.trigger();
    }
  }
}
