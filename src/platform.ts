/**
 * Structural ports for the platform bindings. Deliberately minimal so the real
 * Workers bindings (D1Database, Queue) satisfy them without casting, while the
 * test suite can provide an in-process implementation.
 */

/** Database binding (`env.DB`): every statement the Repository runs goes through it. */
export interface D1DatabaseLike {
  prepare(query: string): D1StatementLike;
  batch(statements: readonly D1StatementLike[]): Promise<readonly unknown[]>;
}

/** A prepared statement; values are always bound, never interpolated into the SQL text. */
export interface D1StatementLike {
  bind(...values: readonly unknown[]): D1StatementLike;
  first<T = Record<string, unknown>>(): Promise<T | null>;
  all<T = Record<string, unknown>>(): Promise<D1ResultLike<T>>;
  run(): Promise<D1ResultLike<never>>;
}

/**
 * Projection of a D1 result: `results` for selects, `meta.changes` for write
 * counts.
 */
export interface D1ResultLike<T = Record<string, unknown>> {
  results?: T[];
  success?: boolean;
  meta?: {
    changes?: number;
    last_row_id?: number;
    [key: string]: unknown;
  };
}

/** Producer binding (`env.NOTIFICATIONS`) used to enqueue outbound jobs. */
export interface QueueProducerLike<T> {
  sendBatch(messages: readonly { body: T }[]): Promise<unknown>;
}

/** Consumer batch; each delivered message must end in `ack` or `retry`. */
export interface MessageBatchLike<T> {
  readonly queue: string;
  readonly messages: readonly QueueMessageLike<T>[];
}

/** One delivered message with its acknowledgement and retry handles. */
export interface QueueMessageLike<T> {
  readonly body: T;
  ack(): void;
  retry(options?: { delaySeconds?: number }): void;
}

/** Correlates a queue message with the outbound_jobs row it must deliver. */
export interface OutboundJobMessage {
  jobId: string;
}
