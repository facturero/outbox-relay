export interface Logger {
  info: (msg: string, meta?: Record<string, unknown>) => void;
  warn: (msg: string, meta?: Record<string, unknown>) => void;
  error: (msg: string, meta?: Record<string, unknown>) => void;
}

export function createConsoleLogger(prefix: string): Logger {
  const fmt = (msg: string, meta?: Record<string, unknown>) =>
    meta ? `${prefix} ${msg} ${JSON.stringify(meta)}` : `${prefix} ${msg}`;

  return {
    info: (msg, meta) => console.log(fmt(msg, meta)),
    warn: (msg, meta) => console.warn(fmt(msg, meta)),
    error: (msg, meta) => console.error(fmt(msg, meta)),
  };
}
