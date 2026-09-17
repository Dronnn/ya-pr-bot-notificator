/**
 * Deterministic test doubles. No network, no real timers.
 */

import type { CalendarParser } from '../../src/calendar/parser.ts';
import type { ParsedCalendar } from '../../src/domain/calendar.ts';
import type { QueueProducerLike } from '../../src/platform.ts';
import type { Logger, LogSink } from '../../src/util.ts';

export interface TestClock {
  now(): number;
  set(ms: number): void;
  advance(ms: number): void;
  /** Advances the clock by `ms` and resolves, emulating elapsed wall time. */
  sleep(ms: number): Promise<void>;
}

export function createTestClock(startMs: number): TestClock {
  let current = startMs;
  return {
    now: () => current,
    set: (ms) => {
      current = ms;
    },
    advance: (ms) => {
      current += ms;
    },
    sleep: async (ms) => {
      current += ms;
    },
  };
}

export interface FetchCall {
  url: string;
  init: RequestInit | undefined;
}

export interface FetchSpy {
  fetch: typeof fetch;
  calls: FetchCall[];
}

export type FetchHandler = (
  url: string,
  init: RequestInit | undefined,
) => Response | Promise<Response>;

/** Records every call; responds using the supplied handler. */
export function createFetchSpy(handler: FetchHandler): FetchSpy {
  const calls: FetchCall[] = [];
  const fetchImpl = async (
    input: Request | URL | string,
    init?: RequestInit,
  ): Promise<Response> => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    calls.push({ url, init });
    return handler(url, init);
  };
  return { fetch: fetchImpl as typeof fetch, calls };
}

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

export function textResponse(body: string, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(body, { status, headers });
}

/** Never resolves until its AbortSignal fires. Use as a FetchHandler. */
export const hangingHandler: FetchHandler = (_url, init) => {
  return new Promise<Response>((_resolve, reject) => {
    const signal = init?.signal;
    if (signal === null || signal === undefined) {
      return;
    }
    const onAbort = (): void => {
      reject(new DOMException('aborted', 'AbortError'));
    };
    if (signal.aborted) {
      onAbort();
      return;
    }
    signal.addEventListener('abort', onAbort);
  });
};

/** `hangingHandler` shaped as `typeof fetch` for direct client injection. */
export const hangingFetch: typeof fetch = async (input, init) =>
  hangingHandler(
    typeof input === 'string' ? input : input instanceof URL ? input.href : input.url,
    init,
  );

export interface QueueSpy<T> {
  producer: QueueProducerLike<T>;
  batches: T[][];
}

export function createQueueSpy<T>(): QueueSpy<T> {
  const batches: T[][] = [];
  const producer: QueueProducerLike<T> = {
    async sendBatch(messages): Promise<unknown> {
      batches.push(messages.map((message) => message.body));
      return undefined;
    },
  };
  return { producer, batches };
}

export function createParser(calendar: ParsedCalendar): CalendarParser {
  return {
    async parse(): Promise<ParsedCalendar> {
      return calendar;
    },
  };
}

export function createThrowingParser(error: Error): CalendarParser {
  return {
    async parse(): Promise<ParsedCalendar> {
      throw error;
    },
  };
}

export interface CapturedLogger {
  logger: Logger;
  lines: string[];
  sink: LogSink;
}

export function createCapturedLogger(): CapturedLogger {
  const lines: string[] = [];
  const sink: LogSink = (line) => {
    lines.push(line);
  };
  const logger: Logger = {
    debug: (event, fields) => emit('debug', event, fields),
    info: (event, fields) => emit('info', event, fields),
    warn: (event, fields) => emit('warn', event, fields),
    error: (event, fields) => emit('error', event, fields),
  };
  const emit = (level: string, event: string, fields?: Record<string, unknown>): void => {
    sink(JSON.stringify({ level, event, ...(fields ?? {}) }));
  };
  return { logger, lines, sink };
}

export function createSeededRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state * 1_664_525 + 1_013_904_223) >>> 0;
    return state / 0x1_0000_0000;
  };
}

export function createSequenceIds(prefix = 'id'): () => string {
  let counter = 0;
  return () => {
    counter += 1;
    return `${prefix}-${counter}`;
  };
}
