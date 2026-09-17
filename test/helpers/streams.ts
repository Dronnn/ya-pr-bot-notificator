/**
 * Stream, clock-gate and interleaving helpers shared by the regression suites.
 */

import { Repository } from '../../src/data/repository.ts';
import type { D1StatementLike } from '../../src/platform.ts';
import type { SqliteD1 } from './d1-sqlite.ts';

export interface Deferred {
  promise: Promise<void>;
  resolve: () => void;
}

/** A promise a test can resolve at a chosen point. */
export function deferred(): Deferred {
  let resolve!: () => void;
  const promise = new Promise<void>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

export interface StalledResponseOptions {
  status?: number;
  headers?: Record<string, string>;
  /** Chunks delivered before the stream stalls. */
  chunks?: Uint8Array[];
  onCancel?: () => void;
  /** Never-resolving cancellation promise, like a misbehaving upstream. */
  cancelNeverSettles?: boolean;
  /** Milliseconds after which the stream errors instead of stalling. */
  failAfterMs?: number;
}

/** A response whose headers arrive immediately and whose body then stalls. */
export function stalledResponse(options: StalledResponseOptions = {}): Response {
  return new Response(
    new ReadableStream<Uint8Array>({
      start(controller) {
        for (const chunk of options.chunks ?? []) {
          controller.enqueue(chunk);
        }
        if (options.failAfterMs !== undefined) {
          setTimeout(() => {
            controller.error(new Error('stream failed'));
          }, options.failAfterMs);
        }
      },
      pull() {
        return new Promise(() => {});
      },
      cancel() {
        options.onCancel?.();
        if (options.cancelNeverSettles === true) {
          return new Promise(() => {});
        }
        return undefined;
      },
    }),
    { status: options.status ?? 200, headers: options.headers },
  );
}

export interface StalledRequestBodyOptions {
  /** Chunks delivered before the stream stalls or fails. */
  chunks?: Uint8Array[];
  onCancel?: () => void;
  onPull?: () => void;
  /** When set, the stream errors with this error after the chunks are delivered. */
  failWith?: Error;
}

/**
 * A request body stream that delivers `chunks`, then either errors with
 * `failWith` or stalls until it is cancelled - mirroring `stalledResponse` for
 * the inbound webhook path.
 */
export function stalledRequestBody(
  options: StalledRequestBodyOptions = {},
): ReadableStream<Uint8Array> {
  const chunks = options.chunks ?? [];
  let index = 0;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      options.onPull?.();
      const chunk = chunks[index];
      if (chunk !== undefined) {
        index += 1;
        controller.enqueue(chunk);
        return;
      }
      if (options.failWith !== undefined) {
        controller.error(options.failWith);
        return;
      }
      return new Promise(() => {});
    },
    cancel() {
      options.onCancel?.();
    },
  });
}

/**
 * A repository whose first batch call pauses after the statements executed, so
 * a test can interleave work (or advance a clock) while the caller is mid-sync.
 */
export function pausingRepository(db: SqliteD1, onPause: () => Promise<void>): Repository {
  let armed = true;
  return new Repository({
    prepare: (query: string) => db.prepare(query),
    async batch(statements: readonly D1StatementLike[]): Promise<readonly unknown[]> {
      const results = await db.batch(statements);
      if (armed) {
        armed = false;
        await onPause();
      }
      return results;
    },
  });
}

/**
 * A repository that runs `onBatch` before the given 1-based batch call executes,
 * so a test can advance a clock immediately before a specific D1 batch.
 */
export function gatedRepository(
  db: SqliteD1,
  batchNumber: number,
  onBatch: () => void,
): Repository {
  let calls = 0;
  return new Repository({
    prepare: (query: string) => db.prepare(query),
    async batch(statements: readonly D1StatementLike[]): Promise<readonly unknown[]> {
      calls += 1;
      if (calls === batchNumber) {
        onBatch();
      }
      return db.batch(statements);
    },
  });
}
