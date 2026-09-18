/**
 * Deterministic virtual-time simulation harness for finding 11 (the
 * 1,000-recipient delivery target).
 *
 * The harness runs the real `Repository` on the local SQLite D1 shim, the real
 * `runSchedulerTick` and the real `processQueueBatch` against an injected clock.
 * Virtual time only moves when every simulated task is blocked, so overlapping
 * cron ticks, D1/Queue handoff, pacing sleeps, 429 cooldowns, Telegram response
 * latency and lease expiry are exact functions of virtual time instead of
 * wall-clock races. Nothing sleeps for real and nothing touches the network.
 *
 * Platform model (wrangler.jsonc): queue batch size 10, batch timeout 1 s,
 * consumer concurrency 1, cron every minute. `cronAlignmentMs` is the delay
 * from the logical `send_at` to the first tick: the worst allowed alignment is
 * 60 s. `tickWorkDelayMs` models the scheduler's D1 work before enqueue,
 * `queueHandoffMs` the D1/Queue handoff between `sendBatch` and delivery.
 *
 * The Telegram double records every request start and response end in virtual
 * time, tracks simultaneous in-flight requests, answers one configurable
 * request ordinal with 429 + retry_after, and cycles response latency through a
 * deterministic sequence with the configured exact mean. The queue records
 * batch sizes, timeout flushes and concurrency; ticks record the backlog they
 * overlapped; the consumer's own `ConsumeSummary` is aggregated so a deferred
 * or terminal path cannot hide behind a green target.
 *
 * The real consumer's rolling admission runs unmodified (up to five in-flight
 * tasks, each admitted while `used + reserved + MESSAGE_BUDGET_FLOOR` fits). The Queue side is
 * modeled: a budget-deferred message is redelivered after the consumer's
 * explicit `delaySeconds`, and after `maxQueueDeliveries` deliveries the
 * platform drops it so the job falls back to scheduler lease repair.
 *
 * Scheduler, consumer and seeding each get their own `Repository` over the
 * shared shim, mirroring the separate Worker invocations of production so
 * per-invocation statement budgets never interleave.
 */

import { TelegramClient } from '../../src/telegram/adapter.ts';
import { Repository, SEND_PACE_WINDOW_MS, type SendReservation } from '../../src/data/repository.ts';
import { processQueueBatch, type ConsumerDeps, type ConsumeSummary } from '../../src/queue/consumer.ts';
import { runSchedulerTick, type TickDeps } from '../../src/scheduler/tick.ts';
import type { CalendarParser } from '../../src/calendar/parser.ts';
import type {
  D1DatabaseLike,
  OutboundJobMessage,
  QueueMessageLike,
  QueueProducerLike,
} from '../../src/platform.ts';
import {
  BOT_SEND_PACE_PER_SECOND,
  BUDGET_DEFER_RETRY_DELAY_SECONDS,
  JOB_LEASE_MS,
  MESSAGE_BUDGET_FLOOR,
  MS_PER_MINUTE,
} from '../../src/util.ts';
import { applyMigrations, createSqliteD1, type SqliteD1 } from './d1-sqlite.ts';
import { createCapturedLogger, createSequenceIds, jsonResponse } from './fakes.ts';
import { occurrence, seedSource } from './seed.ts';

/** wrangler.jsonc: `max_batch_size: 10`. */
export const QUEUE_BATCH_SIZE = 10;
/** wrangler.jsonc: `max_batch_timeout: 1` second. */
export const QUEUE_BATCH_TIMEOUT_MS = 1_000;
/** wrangler.jsonc: `max_concurrency: 1`. */
export const QUEUE_CONSUMER_CONCURRENCY = 1;

const COMPLETION_POLL_MS = 250;
const DUPLICATE_DELIVERY_DELAY_MS = 150;
/** Real-time Telegram client deadline; virtual runs finish far below it. */
const TELEGRAM_CLIENT_TIMEOUT_MS = 60_000;
const MAX_DRIVER_STEPS = 2_000_000;
const DEFAULT_START_MS = 1_700_000_000_000;

export interface TargetSimulationOptions {
  /** Reminder recipients, split evenly across the basic/extended courses. */
  readonly recipients: number;
  /** Virtual start; the logical `send_at` shared by every seeded reminder. */
  readonly startMs: number;
  /** Delay from `startMs` to the first cron tick; 60_000 is the worst allowed. */
  readonly cronAlignmentMs: number;
  /** Modeled scheduler-side D1 work before claims are enqueued. */
  readonly tickWorkDelayMs: number;
  /** Modeled D1/Queue handoff between `sendBatch` and queue delivery. */
  readonly queueHandoffMs: number;
  /** Mean Telegram response latency; the deterministic cycle has this exact mean. */
  readonly telegramLatencyMs: number;
  /** 1-based request ordinal answered with 429; null disables the 429. */
  readonly rateLimitOrdinal: number | null;
  /** Telegram `retry_after` for the modeled 429. */
  readonly rateLimitRetryAfterMs: number;
  /** Duplicate every Nth scheduler queue message (at-least-once); 0 disables. */
  readonly duplicateEvery: number;
  /** Virtual visibility delay applied to a queue-level `message.retry()`. */
  readonly queueRetryDelayMs: number;
  /** Hard virtual deadline after which the run stops waiting. */
  readonly maxSimulatedMs: number;
  /** 0-based position of the first delivery to crash after claiming its lease. */
  readonly crashMessageIndex: number | null;
  /**
   * Negative-control probe: wraps the consumer repository so one started
   * Telegram call increments `attempt_count` twice. The attempt-inflation
   * assertion must report the affected jobs; never set this for a target run.
   */
  readonly attemptInflationProbe?: boolean;
  /** Answer every Nth recipient chat id with 403; 0 disables. */
  readonly forbiddenEvery: number;
  /**
   * Adversarial reset-boundary traffic (finding 5): before the first tick,
   * distinct owners race `acquireSendSlot` in bursts aimed at the old
   * fixed-window reset (1 slot at T, 19 at T+900 ms, 20 at T+1000 ms, with
   * clock ties inside each burst). The granted starts must hold zero rolling
   * violations. Runs at `startMs`, a full minute before any real traffic, so
   * the defined 1,000-user measurements are unaffected.
   */
  readonly pacingBoundaryProbe: boolean;
  /**
   * Queue delivery attempts (initial + retries) before a retried message is
   * dropped. Cloudflare's consumer default is `max_retries = 3`, i.e. four
   * deliveries; the job then falls back to scheduler lease repair.
   */
  readonly maxQueueDeliveries: number;
}

export const DEFAULT_TARGET_SIMULATION_OPTIONS: TargetSimulationOptions = {
  recipients: 1_000,
  startMs: DEFAULT_START_MS,
  cronAlignmentMs: MS_PER_MINUTE,
  tickWorkDelayMs: 100,
  queueHandoffMs: 500,
  telegramLatencyMs: 200,
  rateLimitOrdinal: 137,
  rateLimitRetryAfterMs: 1_000,
  duplicateEvery: 37,
  queueRetryDelayMs: 1_000,
  maxSimulatedMs: 10 * MS_PER_MINUTE,
  crashMessageIndex: null,
  forbiddenEvery: 0,
  maxQueueDeliveries: 4,
  pacingBoundaryProbe: false,
};

export interface SimulatedRequestStart {
  readonly ordinal: number;
  readonly chatId: number;
  readonly atMs: number;
}

export interface CooldownWindow {
  readonly recordedAtMs: number;
  readonly untilMs: number;
  readonly retryAfterMs: number;
}

/**
 * Durable pacing invariant: a 20/s pacer permits at most 20 starts in any
 * one-second window, so the (i + 20)th start can never begin less than
 * `windowMs` after the i-th. Exported separately so a regression can feed it a
 * fabricated violation and prove the detector is not vacuous.
 */
export function findPacingViolations(
  starts: readonly { readonly atMs: number }[],
  maxPerWindow: number = BOT_SEND_PACE_PER_SECOND,
  windowMs: number = SEND_PACE_WINDOW_MS,
): number {
  const sorted = [...starts].sort((left, right) => left.atMs - right.atMs);
  let violations = 0;
  for (let index = 0; index + maxPerWindow < sorted.length; index += 1) {
    const first = sorted[index];
    const boundary = sorted[index + maxPerWindow];
    if (first !== undefined && boundary !== undefined && boundary.atMs - first.atMs < windowMs) {
      violations += 1;
    }
  }
  return violations;
}

/** Global 429 cooldown invariant: no request may start strictly inside a recorded window. */
export function findCooldownViolations(
  starts: readonly { readonly atMs: number }[],
  cooldowns: readonly CooldownWindow[],
): number {
  let violations = 0;
  for (const window of cooldowns) {
    for (const start of starts) {
      if (start.atMs > window.recordedAtMs && start.atMs < window.untilMs) {
        violations += 1;
      }
    }
  }
  return violations;
}

export interface PacingBoundaryProbe {
  readonly grantTimesMs: readonly number[];
  readonly violations: number;
}

/**
 * Adversarial reset-boundary traffic for finding 5. Bursts are aimed at the
 * old fixed-window reset — 1 slot at T, 19 at T+900 ms, 20 at T+1000 ms —
 * issued concurrently by distinct owners with clock ties inside each burst,
 * the way overlapping Worker invocations would race. A rolling-window pacer
 * grants 21 of the 40 (the T+1000 burst almost entirely denies) with zero
 * rolling violations; the fixed-window gate grants all 40, which
 * `findPacingViolations` flags. Mutation check: point this at a fixed-window
 * `acquireSendSlot` and `violations` must be nonzero.
 */
export async function runPacingBoundaryProbe(
  repository: Repository,
  baseMs: number,
  maxPerWindow: number = BOT_SEND_PACE_PER_SECOND,
): Promise<PacingBoundaryProbe> {
  const granted: number[] = [];
  const bursts: { readonly atMs: number; readonly count: number }[] = [
    { atMs: baseMs, count: 1 },
    { atMs: baseMs + 900, count: 19 },
    { atMs: baseMs + 1_000, count: 20 },
  ];
  for (const burst of bursts) {
    const attempts = await Promise.all(
      Array.from({ length: burst.count }, () => repository.acquireSendSlot(burst.atMs, maxPerWindow)),
    );
    for (const ok of attempts) {
      if (ok) {
        granted.push(burst.atMs);
      }
    }
  }
  return {
    grantTimesMs: granted,
    violations: findPacingViolations(granted.map((atMs) => ({ atMs }))),
  };
}

export interface SimulatedRequestEnd {
  readonly ordinal: number;
  readonly chatId: number;
  readonly atMs: number;
  readonly outcome: 'ok' | 'rate-limit' | 'forbidden';
}

export interface TickRecord {
  /** Exact virtual instant the cron tick fired; `startMs + cronAlignmentMs` for the first. */
  readonly firedAtMs: number;
  /** Instant the tick's modeled D1 work finished and the tick body ran. */
  readonly atMs: number;
  readonly repaired: number;
  readonly enqueued: number;
  readonly syncStatuses: readonly string[];
  /** Non-terminal jobs when the tick fired (backlog the tick overlapped). */
  readonly outstandingAtStart: number;
  /** Queue messages buffered or in flight when the tick fired. */
  readonly queuePendingAtStart: number;
  readonly queueBusyAtStart: boolean;
}

export interface TargetSimulationMetrics {
  readonly recipients: number;
  readonly expectedJobs: number;
  readonly totalJobs: number;
  readonly sentJobs: number;
  readonly terminalJobs: number;
  /** Jobs neither sent nor terminal; must be zero when the run settles. */
  readonly unprocessedJobs: number;
  readonly lostJobs: number;
  readonly jobsWithoutSuccess: number;
  readonly jobsByStatus: Readonly<Record<string, number>>;
  readonly completionSamples: number;
  readonly maxCompletionFromSendAtMs: number | null;
  readonly p95CompletionFromSendAtMs: number | null;
  readonly requestStarts: number;
  readonly requestEnds: number;
  /** Every request start in virtual time, ascending. */
  readonly requestStartTimesMs: readonly number[];
  readonly firstRequestAtMs: number | null;
  readonly lastRequestAtMs: number | null;
  readonly startSpanMs: number;
  /** Average starts per second over the measurement span, initial burst included. */
  readonly startsPerSecond: number;
  /** Sustained rate after the initial 20-request burst; bounded by 20 when compliant. */
  readonly sustainedStartsPerSecond: number;
  readonly maxInFlight: number;
  readonly pacingViolations: number;
  readonly cooldownViolations: number;
  readonly cooldownWindows: readonly CooldownWindow[];
  /** Granted starts of the adversarial boundary probe (empty when disabled). */
  readonly pacingProbeGrantTimesMs: readonly number[];
  /** Rolling violations over the probe grants; must be 0 with the probe on. */
  readonly pacingProbeViolations: number;
  readonly successCountsByChatId: ReadonlyMap<number, readonly number[]>;
  readonly duplicateSuccessChatIds: readonly number[];
  readonly callCountsByChatId: ReadonlyMap<number, number>;
  readonly attemptMismatchJobIds: readonly string[];
  readonly queueDeliveries: number;
  readonly queueBatches: number;
  readonly queueBatchSizes: readonly number[];
  /** Batches flushed by the 1 s batch timeout instead of a full 10-message batch. */
  readonly queueTimeoutFlushes: number;
  /** High-water mark of batches processed simultaneously; concurrency 1 in the model. */
  readonly maxConcurrentQueueBatches: number;
  readonly duplicateQueueDeliveries: number;
  /** Duplicate (at-least-once redelivery) queue messages actually run by the consumer. */
  readonly duplicateDeliveriesProcessed: number;
  /** Aggregated `ConsumeSummary` across every consumer invocation. */
  readonly consumerSummary: Readonly<ConsumeSummary>;
  readonly maxQueueDeliveriesPerJob: number;
  readonly queueRetries: number;
  /** Budget-deferred deliveries re-enqueued with an explicit retry delay. */
  readonly budgetDeferralRetries: number;
  readonly exhaustedQueueMessages: number;
  readonly exhaustedJobIds: readonly string[];
  readonly schedulerBacklogDuplicates: number;
  readonly crashedJobIds: readonly string[];
  readonly repairedLeases: number;
  readonly ticksWithOutstandingJobs: number;
  readonly ticksWithQueueBacklog: number;
  readonly firstTickFiredAtMs: number | null;
  readonly telegramLatencySamples: number;
  readonly meanTelegramLatencyMs: number | null;
  readonly minTelegramLatencyMs: number | null;
  readonly maxTelegramLatencyMs: number | null;
  readonly timedOut: boolean;
  readonly elapsedVirtualMs: number;
  readonly errors: readonly string[];
  readonly ticks: readonly TickRecord[];
  readonly startMs: number;
  readonly assumptions: {
    readonly cronAlignmentMs: number;
    readonly tickWorkDelayMs: number;
    readonly queueHandoffMs: number;
    readonly telegramLatencyMs: number;
    readonly queueBatchSize: number;
    readonly queueBatchTimeoutMs: number;
    readonly consumerConcurrency: number;
    readonly rateLimitOrdinal: number | null;
    readonly rateLimitRetryAfterMs: number;
    readonly duplicateEvery: number;
    readonly crashMessageIndex: number | null;
    readonly maxSimulatedMs: number;
    /** Statements reserved per message; drives per-chunk admission. */
    readonly messageBudgetFloor: number;
    /** Queue retry delay the consumer uses for a budget-deferred chunk. */
    readonly budgetDeferRetryDelaySeconds: number;
    readonly maxQueueDeliveries: number;
    readonly forbiddenEvery: number;
    readonly pacingBoundaryProbe: boolean;
  };
}

// ---------------------------------------------------------------------------
// Virtual timeline and driver
// ---------------------------------------------------------------------------

interface VirtualEvent {
  readonly atMs: number;
  readonly seq: number;
  readonly callback: () => void;
}

/**
 * Discrete-event virtual clock. `delay` and `at` register events; `advance`
 * moves the clock to the earliest event and fires every event due by then. A
 * promise chain blocked on timers therefore advances time only when nothing
 * else can run.
 */
export class VirtualTimeline {
  #nowMs: number;
  #seq = 0;
  #events: VirtualEvent[] = [];

  constructor(startMs: number) {
    this.#nowMs = startMs;
  }

  now(): number {
    return this.#nowMs;
  }

  hasEvents(): boolean {
    return this.#events.length > 0;
  }

  delay(ms: number): Promise<void> {
    return new Promise((resolve) => {
      this.#insert(this.#nowMs + Math.max(0, ms), resolve);
    });
  }

  at(atMs: number, callback: () => void): void {
    this.#insert(Math.max(atMs, this.#nowMs), callback);
  }

  /** Moves to the next scheduled instant and runs every event now due. */
  advance(): void {
    const next = this.#events.shift();
    if (next === undefined) {
      return;
    }
    this.#nowMs = Math.max(this.#nowMs, next.atMs);
    next.callback();
    for (;;) {
      const event = this.#events[0];
      if (event === undefined || event.atMs > this.#nowMs) {
        break;
      }
      this.#events.shift();
      event.callback();
    }
  }

  #insert(atMs: number, callback: () => void): void {
    const event: VirtualEvent = { atMs, seq: this.#seq, callback };
    this.#seq += 1;
    let index = this.#events.length;
    while (index > 0) {
      const previous = this.#events[index - 1];
      if (previous !== undefined && previous.atMs > atMs) {
        index -= 1;
      } else {
        break;
      }
    }
    this.#events.splice(index, 0, event);
  }
}

/**
 * Runs `task` until it settles, advancing the virtual timeline whenever the
 * task (and every detached task it spawned) is blocked on a virtual timer.
 * Throws when a promise stays pending with no virtual event left to run.
 */
export async function driveSimulation<T>(
  timeline: VirtualTimeline,
  task: Promise<T>,
): Promise<T> {
  let settled = false;
  let failed = false;
  let failure: unknown = null;
  let value: T | undefined;
  void task.then(
    (result) => {
      value = result;
      settled = true;
    },
    (error: unknown) => {
      failure = error;
      failed = true;
      settled = true;
    },
  );
  let steps = 0;
  while (!settled) {
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    if (settled) {
      break;
    }
    steps += 1;
    if (steps > MAX_DRIVER_STEPS) {
      throw new Error('simulation driver exceeded its step limit');
    }
    if (timeline.hasEvents()) {
      timeline.advance();
      continue;
    }
    // One more macrotask turn in case a resolved microtask chain is still
    // draining; only then is a missing event a real deadlock.
    await new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
    if (!settled && !timeline.hasEvents()) {
      throw new Error('simulation deadlocked: task pending with no virtual events left');
    }
  }
  if (failed) {
    throw failure;
  }
  return value as T;
}

// ---------------------------------------------------------------------------
// Simulated Telegram
// ---------------------------------------------------------------------------

interface TelegramSimState {
  readonly starts: SimulatedRequestStart[];
  readonly ends: SimulatedRequestEnd[];
  readonly callsByChatId: Map<number, number>;
  readonly successesByChatId: Map<number, number[]>;
  readonly cooldowns: CooldownWindow[];
  maxInFlight: number;
  inFlight: number;
}

function errorText(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}

/**
 * Negative-control wrapper: every started send increments `attempt_count`
 * twice. Used only by the metric-sensitivity regression that proves the
 * attempt-inflation detector notices a second increment for one real call.
 */
function createAttemptInflationProbeRepository(db: D1DatabaseLike): Repository {
  class DoubleCountingRepository extends Repository {
    override async beginSendAttempt(
      jobId: string,
      owner: string,
      now: number,
      maxAttempts: number,
    ): Promise<SendReservation> {
      const first = await super.beginSendAttempt(jobId, owner, now, maxAttempts);
      if (first.status === 'reserved') {
        await super.beginSendAttempt(jobId, owner, now, maxAttempts);
      }
      return first;
    }
  }
  return new DoubleCountingRepository(db);
}

function countOutstandingJobs(db: SqliteD1): number {
  const row = db.database
    .prepare(
      "SELECT COUNT(*) AS n FROM outbound_jobs WHERE status NOT IN ('sent', 'cancelled', 'failed')",
    )
    .get() as { n: number } | undefined;
  return Number(row?.n ?? 0);
}

function jsonResponseWithRetryAfter(retryAfterMs: number): Response {
  const seconds = Math.max(1, Math.ceil(retryAfterMs / 1_000));
  return new Response(
    JSON.stringify({ ok: false, error_code: 429, parameters: { retry_after: seconds } }),
    {
      status: 429,
      headers: { 'content-type': 'application/json', 'retry-after': String(seconds) },
    },
  );
}

/**
 * Telegram transport double. Every request start and response end is stamped in
 * virtual time; a request answers at `start + latency` when the timeline
 * reaches that instant. The response clock also defines the 429 cooldown start,
 * exactly like `persistSendOutcome`'s `responseAtMs`.
 */
function createSimulatedTelegram(
  timeline: VirtualTimeline,
  options: Pick<
    TargetSimulationOptions,
    'telegramLatencyMs' | 'rateLimitOrdinal' | 'rateLimitRetryAfterMs' | 'forbiddenEvery'
  >,
): { client: TelegramClient; state: TelegramSimState } {
  const state: TelegramSimState = {
    starts: [],
    ends: [],
    callsByChatId: new Map(),
    successesByChatId: new Map(),
    cooldowns: [],
    maxInFlight: 0,
    inFlight: 0,
  };
  // Cycle with an exact zero mean: 150 ms, 200 ms, 250 ms for a 200 ms mean.
  const latencyOffsets = [-50, 0, 50] as const;
  let ordinal = 0;

  const fetchImpl = async (
    input: Request | URL | string,
    init?: RequestInit,
  ): Promise<Response> => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    if (!url.includes('/sendMessage')) {
      return jsonResponse({ ok: true, result: { message_id: 1 } });
    }
    ordinal += 1;
    const requestOrdinal = ordinal;
    const parsed = JSON.parse(String(init?.body ?? '{}')) as { chat_id?: unknown };
    const chatId = typeof parsed.chat_id === 'number' ? parsed.chat_id : -1;
    const startAtMs = timeline.now();
    state.starts.push({ ordinal: requestOrdinal, chatId, atMs: startAtMs });
    state.callsByChatId.set(chatId, (state.callsByChatId.get(chatId) ?? 0) + 1);
    state.inFlight += 1;
    if (state.inFlight > state.maxInFlight) {
      state.maxInFlight = state.inFlight;
    }
    const offset = latencyOffsets[requestOrdinal % latencyOffsets.length] ?? 0;
    const latencyMs = Math.max(1, options.telegramLatencyMs + offset);

    return new Promise<Response>((resolve) => {
      timeline.at(startAtMs + latencyMs, () => {
        state.inFlight -= 1;
        const settledAtMs = timeline.now();
        const rateLimited = options.rateLimitOrdinal === requestOrdinal;
        const forbidden =
          !rateLimited && options.forbiddenEvery > 0 && chatId % options.forbiddenEvery === 0;
        state.ends.push({
          ordinal: requestOrdinal,
          chatId,
          atMs: settledAtMs,
          outcome: rateLimited ? 'rate-limit' : forbidden ? 'forbidden' : 'ok',
        });
        if (forbidden) {
          resolve(
            new Response(
              JSON.stringify({ ok: false, error_code: 403, description: 'Forbidden' }),
              { status: 403, headers: { 'content-type': 'application/json' } },
            ),
          );
          return;
        }
        if (rateLimited) {
          // The adapter floors a 429 backoff at one second and reads
          // `parameters.retry_after` in whole seconds, so the recorded cooldown
          // must be the parsed duration, not the raw requested one.
          const retryAfterMs = Math.max(1_000, Math.ceil(options.rateLimitRetryAfterMs / 1_000) * 1_000);
          state.cooldowns.push({
            recordedAtMs: settledAtMs,
            untilMs: settledAtMs + retryAfterMs,
            retryAfterMs,
          });
          resolve(jsonResponseWithRetryAfter(retryAfterMs));
          return;
        }
        const successes = state.successesByChatId.get(chatId) ?? [];
        successes.push(settledAtMs);
        state.successesByChatId.set(chatId, successes);
        resolve(jsonResponse({ ok: true, result: { message_id: requestOrdinal } }));
      });
    });
  };

  const client = new TelegramClient({
    botToken: '123456789:AAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    fetch: fetchImpl as typeof fetch,
    timeoutMs: TELEGRAM_CLIENT_TIMEOUT_MS,
  });
  return { client, state };
}

// ---------------------------------------------------------------------------
// Simulated Queue
// ---------------------------------------------------------------------------

interface TrackedMessage extends QueueMessageLike<OutboundJobMessage> {
  acked: boolean;
  retried: boolean;
  /** Explicit `retry({delaySeconds})`, i.e. a budget deferral; null for a plain retry. */
  retryDelaySeconds: number | null;
}

/**
 * Queue message double. Keeps the acknowledgement/retry calls a fake binding
 * must support, and distinguishes an explicit-delay retry (the consumer's
 * budget deferral) from a plain transport retry so the simulation can verify
 * the durable deferral path.
 */
function createTrackedMessage(jobId: string): TrackedMessage {
  const message: TrackedMessage = {
    body: { jobId },
    acked: false,
    retried: false,
    retryDelaySeconds: null,
    ack(): void {
      message.acked = true;
    },
    retry(options?: { delaySeconds?: number }): void {
      message.retried = true;
      message.retryDelaySeconds = options?.delaySeconds ?? null;
    },
  };
  return message;
}

interface QueuePendingMessage {
  readonly jobId: string;
  readonly message: TrackedMessage;
  readonly availableAtMs: number;
  readonly duplicate: boolean;
  /** Delivery attempts this queue message has consumed (1 on first delivery). */
  readonly deliveries: number;
}

/**
 * Queue model: deliveries become visible after the handoff delay; a batch is
 * delivered when 10 messages are available or 1 s after the oldest available
 * message; only one batch is processed at a time (concurrency 1). Duplicate
 * deliveries and queue-level retries re-enter the same visibility buffer. A
 * retry with an explicit `delaySeconds` (the consumer's budget deferral) uses
 * that delay; a plain retry uses `queueRetryDelayMs`. A message is redelivered
 * at most `maxQueueDeliveries` times in total (Cloudflare's default
 * `max_retries = 3` plus the initial delivery); after that the platform drops
 * it and the job falls back to scheduler lease repair, so an exhausted message
 * is never silently lost.
 */
class SimulatedQueue {
  readonly producer: QueueProducerLike<OutboundJobMessage>;
  readonly deliveriesByJobId = new Map<string, number>();
  readonly crashedJobIds: string[] = [];
  readonly errors: string[] = [];
  readonly batchSizes: number[] = [];
  readonly consumerTotals: ConsumeSummary = {
    processed: 0,
    sent: 0,
    retried: 0,
    terminal: 0,
    skipped: 0,
    deferred: 0,
  };
  duplicateDeliveriesProcessed = 0;
  queueTimeoutFlushes = 0;
  maxConcurrentBatches = 0;
  /** Queue messages re-enqueued through `message.retry()`. */
  queueRetries = 0;
  /** Retries with an explicit `delaySeconds`: the consumer's budget deferrals. */
  budgetDeferralRetries = 0;
  /** Messages dropped after `maxQueueDeliveries`; the job falls back to lease repair. */
  exhaustedQueueMessages = 0;
  readonly exhaustedJobIds: string[] = [];

  #timeline: VirtualTimeline;
  #repository: Repository;
  #consumerDeps: ConsumerDeps;
  #handoffMs: number;
  #duplicateEvery: number;
  #retryDelayMs: number;
  #maxQueueDeliveries: number;
  #crashMessageIndex: number | null;
  #buffer: QueuePendingMessage[] = [];
  #busy = false;
  #wakeScheduled = false;
  #arrivalSeq = 0;
  #deliveredCount = 0;
  #duplicateDeliveries = 0;
  #backlogDuplicates = 0;
  #batchCount = 0;
  #concurrentBatches = 0;

  constructor(
    timeline: VirtualTimeline,
    repository: Repository,
    consumerDeps: ConsumerDeps,
    options: TargetSimulationOptions,
  ) {
    this.#timeline = timeline;
    this.#repository = repository;
    this.#consumerDeps = consumerDeps;
    this.#handoffMs = options.queueHandoffMs;
    this.#duplicateEvery = options.duplicateEvery;
    this.#retryDelayMs = options.queueRetryDelayMs;
    this.#maxQueueDeliveries = options.maxQueueDeliveries;
    this.#crashMessageIndex = options.crashMessageIndex;
    this.producer = {
      sendBatch: (messages) => this.#accept(messages),
    };
  }

  get batchCount(): number {
    return this.#batchCount;
  }

  get duplicateDeliveries(): number {
    return this.#duplicateDeliveries;
  }

  get schedulerBacklogDuplicates(): number {
    return this.#backlogDuplicates;
  }

  get pendingCount(): number {
    return this.#buffer.length;
  }

  get isBusy(): boolean {
    return this.#busy;
  }

  idle(): boolean {
    return !this.#busy && this.#buffer.length === 0;
  }

  async #accept(messages: readonly { readonly body: OutboundJobMessage }[]): Promise<unknown> {
    for (const message of messages) {
      const jobId = message.body.jobId;
      this.#arrivalSeq += 1;
      const base = this.#timeline.now() + this.#handoffMs + (this.#arrivalSeq % 4) * 25;
      if (this.#buffer.some((pending) => pending.jobId === jobId)) {
        // The scheduler re-enqueued a job whose message is still queued or in
        // flight: overlapping-tick backlog duplication, not a job-level retry.
        this.#backlogDuplicates += 1;
      }
      this.#push({
        jobId,
        message: createTrackedMessage(jobId),
        availableAtMs: base,
        duplicate: false,
        deliveries: 1,
      });
      if (this.#duplicateEvery > 0 && this.#arrivalSeq % this.#duplicateEvery === 0) {
        this.#duplicateDeliveries += 1;
        this.#push({
          jobId,
          message: createTrackedMessage(jobId),
          availableAtMs: base + DUPLICATE_DELIVERY_DELAY_MS,
          duplicate: true,
          deliveries: 1,
        });
      }
    }
    this.#kick();
    return undefined;
  }

  #push(pending: QueuePendingMessage): void {
    let index = this.#buffer.length;
    while (index > 0) {
      const previous = this.#buffer[index - 1];
      if (previous !== undefined && previous.availableAtMs > pending.availableAtMs) {
        index -= 1;
      } else {
        break;
      }
    }
    this.#buffer.splice(index, 0, pending);
  }

  #scheduleWake(atMs: number): void {
    if (this.#wakeScheduled) {
      return;
    }
    this.#wakeScheduled = true;
    this.#timeline.at(atMs, () => {
      this.#wakeScheduled = false;
      void this.#deliver();
    });
  }

  #kick(): void {
    void this.#deliver();
  }

  async #deliver(): Promise<void> {
    if (this.#busy || this.#buffer.length === 0) {
      return;
    }
    const head = this.#buffer[0];
    if (head === undefined) {
      return;
    }
    const now = this.#timeline.now();
    if (head.availableAtMs > now) {
      this.#scheduleWake(head.availableAtMs);
      return;
    }
    let readyCount = 0;
    while (readyCount < this.#buffer.length) {
      const candidate = this.#buffer[readyCount];
      if (candidate === undefined || candidate.availableAtMs > now) {
        break;
      }
      readyCount += 1;
    }
    if (readyCount < QUEUE_BATCH_SIZE) {
      const flushAt = head.availableAtMs + QUEUE_BATCH_TIMEOUT_MS;
      if (now < flushAt) {
        this.#scheduleWake(flushAt);
        return;
      }
      // Fewer than 10 ready and the 1 s batch timeout elapsed: a real flush.
      this.queueTimeoutFlushes += 1;
    }
    const taken = this.#buffer.splice(0, Math.min(QUEUE_BATCH_SIZE, readyCount));
    this.#busy = true;
    try {
      await this.#processBatch(taken);
      for (const pending of taken) {
        if (!pending.message.retried) {
          continue;
        }
        if (pending.deliveries >= this.#maxQueueDeliveries) {
          // Cloudflare retries exhausted: the platform drops the message and the
          // job falls back to scheduling lease repair (JOB_ENQUEUE_LEASE_MS).
          this.exhaustedQueueMessages += 1;
          this.exhaustedJobIds.push(pending.jobId);
          continue;
        }
        const delaySeconds = pending.message.retryDelaySeconds;
        const delayMs = delaySeconds === null ? this.#retryDelayMs : delaySeconds * 1_000;
        this.queueRetries += 1;
        if (delaySeconds !== null) {
          // The consumer's budget deferral is a retry with an explicit delay.
          this.budgetDeferralRetries += 1;
        }
        this.#push({
          jobId: pending.jobId,
          message: createTrackedMessage(pending.jobId),
          availableAtMs: this.#timeline.now() + delayMs,
          duplicate: pending.duplicate,
          deliveries: pending.deliveries + 1,
        });
      }
    } catch (error) {
      this.errors.push(errorText(error));
    } finally {
      this.#busy = false;
    }
    this.#kick();
  }

  async #processBatch(batch: readonly QueuePendingMessage[]): Promise<void> {
    this.batchSizes.push(batch.length);
    this.#batchCount += 1;
    this.#concurrentBatches += 1;
    if (this.#concurrentBatches > this.maxConcurrentBatches) {
      this.maxConcurrentBatches = this.#concurrentBatches;
    }
    try {
      await this.#runBatch(batch);
    } finally {
      this.#concurrentBatches -= 1;
    }
  }

  async #runBatch(batch: readonly QueuePendingMessage[]): Promise<void> {
    this.#deliveredCount += batch.length;
    for (const pending of batch) {
      this.deliveriesByJobId.set(
        pending.jobId,
        (this.deliveriesByJobId.get(pending.jobId) ?? 0) + 1,
      );
      if (pending.duplicate) {
        this.duplicateDeliveriesProcessed += 1;
      }
    }
    let remaining = [...batch];
    const crashIndex = this.#crashMessageIndex;
    if (crashIndex !== null) {
      const firstDelivered = this.#deliveredCount - batch.length;
      if (crashIndex >= firstDelivered && crashIndex < this.#deliveredCount) {
        const victim = batch[crashIndex - firstDelivered];
        this.#crashMessageIndex = null;
        if (victim !== undefined) {
          // The Worker dies after taking the processing lease and before any
          // Telegram call; the message is lost and D1 lease repair recovers it.
          const context = await this.#repository.claimJobContext(
            victim.jobId,
            'crashed-worker',
            this.#timeline.now(),
            JOB_LEASE_MS,
          );
          if (context !== null) {
            this.crashedJobIds.push(victim.jobId);
          }
          remaining = batch.filter((pending) => pending !== victim);
        }
      }
    }
    if (remaining.length === 0) {
      return;
    }
    const summary = await processQueueBatch(
      remaining.map((pending) => pending.message),
      this.#consumerDeps,
    );
    this.consumerTotals.processed += summary.processed;
    this.consumerTotals.sent += summary.sent;
    this.consumerTotals.retried += summary.retried;
    this.consumerTotals.terminal += summary.terminal;
    this.consumerTotals.skipped += summary.skipped;
    this.consumerTotals.deferred += summary.deferred;
  }
}

// ---------------------------------------------------------------------------
// Scenario seeding and metrics
// ---------------------------------------------------------------------------

/**
 * Seeds the two-course scenario: half the recipients follow `basic` with a
 * 30-minute offset and a lesson 30 minutes after `startMs`; the other half
 * follow `extended` with a 1-day offset and a lesson 24 hours after `startMs`.
 * Every reminder's logical `send_at` is exactly `startMs`, and the planner's
 * course-wide fanout yields exactly one job per recipient. Distinct occurrence
 * rows per course/offset group are the domain-correct reading of the audit's
 * "distinct occurrences" (the planner joins occurrences to users by course).
 */
async function seedTargetScenario(
  repository: Repository,
  options: TargetSimulationOptions,
): Promise<void> {
  const startMs = options.startMs;
  await seedSource(repository, 'basic', startMs);
  await seedSource(repository, 'extended', startMs);
  const basicRecipients = Math.ceil(options.recipients / 2);
  for (let userId = 1; userId <= basicRecipients; userId += 1) {
    await repository.activateUser(userId, userId, startMs);
    await repository.setUserTimeZone(userId, 'Europe/Moscow', startMs);
    await repository.setUserReminderOffsets(userId, [30], startMs);
  }
  for (let userId = basicRecipients + 1; userId <= options.recipients; userId += 1) {
    await repository.activateUser(userId, userId, startMs);
    await repository.setUserTimeZone(userId, 'Europe/Moscow', startMs);
    await repository.setUserCourse(userId, 'extended', startMs);
    await repository.setUserReminderOffsets(userId, [1440], startMs);
  }
  await repository.upsertOccurrences(
    [
      occurrence({
        id: 'basic:sim-30m',
        sourceId: 'basic',
        uid: 'sim-uid-basic',
        course: 'basic',
        occurrenceKey: 'sim-30m',
        startsAtMs: startMs + 30 * MS_PER_MINUTE,
      }),
      occurrence({
        id: 'extended:sim-1d',
        sourceId: 'extended',
        uid: 'sim-uid-extended',
        course: 'extended',
        occurrenceKey: 'sim-1d',
        startsAtMs: startMs + 1440 * MS_PER_MINUTE,
      }),
    ],
    startMs,
  );
}

interface JobRow {
  readonly id: unknown;
  readonly telegram_user_id: unknown;
  readonly send_at_ms: unknown;
  readonly status: unknown;
  readonly attempt_count: unknown;
}

function percentile(sorted: readonly number[], fraction: number): number | null {
  if (sorted.length === 0) {
    return null;
  }
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil(sorted.length * fraction) - 1),
  );
  return sorted[index] ?? null;
}

function collectMetrics(
  db: SqliteD1,
  options: TargetSimulationOptions,
  timeline: VirtualTimeline,
  telegram: TelegramSimState,
  queue: SimulatedQueue,
  ticks: readonly TickRecord[],
  errors: readonly string[],
  timedOut: boolean,
  expectedJobs: number,
  probe: PacingBoundaryProbe,
): TargetSimulationMetrics {
  const jobRows = db.database
    .prepare('SELECT id, telegram_user_id, send_at_ms, status, attempt_count FROM outbound_jobs')
    .all() as unknown as JobRow[];
  const jobsByStatus: Record<string, number> = {};
  const attemptMismatchJobIds: string[] = [];
  const completions: number[] = [];
  let sentJobs = 0;
  let terminalJobs = 0;
  let jobsWithoutSuccess = 0;

  for (const row of jobRows) {
    const status = String(row.status);
    jobsByStatus[status] = (jobsByStatus[status] ?? 0) + 1;
    if (status === 'cancelled' || status === 'failed') {
      terminalJobs += 1;
    }
    const chatId = Number(row.telegram_user_id);
    const calls = telegram.callsByChatId.get(chatId) ?? 0;
    if (Number(row.attempt_count) !== calls) {
      attemptMismatchJobIds.push(String(row.id));
    }
    if (status !== 'sent') {
      continue;
    }
    sentJobs += 1;
    const successes = telegram.successesByChatId.get(chatId);
    const lastSuccess = successes?.[successes.length - 1];
    if (lastSuccess === undefined) {
      jobsWithoutSuccess += 1;
    } else {
      completions.push(lastSuccess - Number(row.send_at_ms));
    }
  }
  completions.sort((left, right) => left - right);

  const starts = [...telegram.starts].sort((left, right) => left.atMs - right.atMs);
  const pacingViolations = findPacingViolations(starts);
  const cooldownViolations = findCooldownViolations(starts, telegram.cooldowns);
  const lastStart = starts[starts.length - 1];
  const firstStart = starts[0];
  const startSpanMs =
    lastStart !== undefined && firstStart !== undefined ? lastStart.atMs - firstStart.atMs : 0;
  const startsPerSecond = startSpanMs > 0 ? (starts.length / startSpanMs) * 1_000 : 0;
  const sustainedStartsPerSecond =
    startSpanMs > 0 && starts.length > 20 ? ((starts.length - 20) / startSpanMs) * 1_000 : 0;

  const startAtByOrdinal = new Map<number, number>();
  for (const start of telegram.starts) {
    startAtByOrdinal.set(start.ordinal, start.atMs);
  }
  const latencies: number[] = [];
  for (const end of telegram.ends) {
    const startAtMs = startAtByOrdinal.get(end.ordinal);
    if (startAtMs !== undefined) {
      latencies.push(end.atMs - startAtMs);
    }
  }
  latencies.sort((left, right) => left - right);
  const latencyTotal = latencies.reduce((sum, latency) => sum + latency, 0);

  const duplicateSuccessChatIds: number[] = [];
  for (const [chatId, times] of telegram.successesByChatId) {
    if (times.length > 1) {
      duplicateSuccessChatIds.push(chatId);
    }
  }

  let queueDeliveries = 0;
  let maxQueueDeliveriesPerJob = 0;
  for (const count of queue.deliveriesByJobId.values()) {
    queueDeliveries += count;
    if (count > maxQueueDeliveriesPerJob) {
      maxQueueDeliveriesPerJob = count;
    }
  }
  const repairedLeases = ticks.reduce((sum, tick) => sum + tick.repaired, 0);
  const firstTickFiredAtMs = ticks[0]?.firedAtMs ?? null;

  return {
    recipients: options.recipients,
    expectedJobs,
    totalJobs: jobRows.length,
    sentJobs,
    terminalJobs,
    unprocessedJobs: jobRows.length - sentJobs - terminalJobs,
    lostJobs: expectedJobs - sentJobs,
    jobsWithoutSuccess,
    jobsByStatus,
    completionSamples: completions.length,
    maxCompletionFromSendAtMs: completions[completions.length - 1] ?? null,
    p95CompletionFromSendAtMs: percentile(completions, 0.95),
    requestStarts: starts.length,
    requestEnds: telegram.ends.length,
    requestStartTimesMs: starts.map((start) => start.atMs),
    firstRequestAtMs: firstStart?.atMs ?? null,
    lastRequestAtMs: lastStart?.atMs ?? null,
    startSpanMs,
    startsPerSecond,
    sustainedStartsPerSecond,
    maxInFlight: telegram.maxInFlight,
    pacingViolations,
    cooldownViolations,
    cooldownWindows: telegram.cooldowns,
    pacingProbeGrantTimesMs: probe.grantTimesMs,
    pacingProbeViolations: probe.violations,
    successCountsByChatId: telegram.successesByChatId,
    duplicateSuccessChatIds,
    callCountsByChatId: telegram.callsByChatId,
    attemptMismatchJobIds,
    queueDeliveries,
    queueBatches: queue.batchCount,
    queueBatchSizes: [...queue.batchSizes],
    queueTimeoutFlushes: queue.queueTimeoutFlushes,
    maxConcurrentQueueBatches: queue.maxConcurrentBatches,
    duplicateQueueDeliveries: queue.duplicateDeliveries,
    duplicateDeliveriesProcessed: queue.duplicateDeliveriesProcessed,
    consumerSummary: { ...queue.consumerTotals },
    maxQueueDeliveriesPerJob,
    queueRetries: queue.queueRetries,
    budgetDeferralRetries: queue.budgetDeferralRetries,
    exhaustedQueueMessages: queue.exhaustedQueueMessages,
    exhaustedJobIds: [...queue.exhaustedJobIds],
    schedulerBacklogDuplicates: queue.schedulerBacklogDuplicates,
    crashedJobIds: queue.crashedJobIds,
    repairedLeases,
    ticksWithOutstandingJobs: ticks.filter((tick) => tick.outstandingAtStart > 0).length,
    ticksWithQueueBacklog: ticks.filter(
      (tick) => tick.queuePendingAtStart > 0 || tick.queueBusyAtStart,
    ).length,
    firstTickFiredAtMs,
    telegramLatencySamples: latencies.length,
    meanTelegramLatencyMs: latencies.length > 0 ? latencyTotal / latencies.length : null,
    minTelegramLatencyMs: latencies[0] ?? null,
    maxTelegramLatencyMs: latencies[latencies.length - 1] ?? null,
    timedOut,
    elapsedVirtualMs: timeline.now() - options.startMs,
    errors: [...errors, ...queue.errors],
    ticks,
    startMs: options.startMs,
    assumptions: {
      cronAlignmentMs: options.cronAlignmentMs,
      tickWorkDelayMs: options.tickWorkDelayMs,
      queueHandoffMs: options.queueHandoffMs,
      telegramLatencyMs: options.telegramLatencyMs,
      queueBatchSize: QUEUE_BATCH_SIZE,
      queueBatchTimeoutMs: QUEUE_BATCH_TIMEOUT_MS,
      consumerConcurrency: QUEUE_CONSUMER_CONCURRENCY,
      rateLimitOrdinal: options.rateLimitOrdinal,
      rateLimitRetryAfterMs: options.rateLimitRetryAfterMs,
      duplicateEvery: options.duplicateEvery,
      crashMessageIndex: options.crashMessageIndex,
      maxSimulatedMs: options.maxSimulatedMs,
      messageBudgetFloor: MESSAGE_BUDGET_FLOOR,
      budgetDeferRetryDelaySeconds: BUDGET_DEFER_RETRY_DELAY_SECONDS,
      maxQueueDeliveries: options.maxQueueDeliveries,
      forbiddenEvery: options.forbiddenEvery,
      pacingBoundaryProbe: options.pacingBoundaryProbe,
    },
  };
}

const SIMULATION_PARSER: CalendarParser = {
  async parse(): Promise<never> {
    throw new Error('calendar parsing is out of scope for the delivery simulation');
  },
};

const NETWORK_REFUSING_FETCH = (() =>
  Promise.reject(new Error('no network in the delivery simulation'))) as unknown as typeof fetch;

/**
 * Runs the full 1,000-recipient scenario end to end and returns its metrics.
 * Every delivery is measured from each reminder's logical `send_at` in
 * `startMs`, never from the tick that discovered it.
 */
export async function runTargetSimulation(
  overrides: Partial<TargetSimulationOptions> = {},
): Promise<TargetSimulationMetrics> {
  const options: TargetSimulationOptions = { ...DEFAULT_TARGET_SIMULATION_OPTIONS, ...overrides };
  const timeline = new VirtualTimeline(options.startMs);
  const db = createSqliteD1();
  applyMigrations(db);
  const seedRepository = new Repository(db);
  const schedulerRepository = new Repository(db);
  const consumerRepository = options.attemptInflationProbe
    ? createAttemptInflationProbeRepository(db)
    : new Repository(db);
  const logger = createCapturedLogger();
  const telegram = createSimulatedTelegram(timeline, options);

  const consumerDeps: ConsumerDeps = {
    repository: consumerRepository,
    telegram: telegram.client,
    now: () => timeline.now(),
    logger: logger.logger,
    random: () => 0.5,
    ownerFactory: createSequenceIds('consumer'),
    sleep: (ms) => timeline.delay(ms),
  };
  const queue = new SimulatedQueue(timeline, consumerRepository, consumerDeps, options);

  await seedTargetScenario(seedRepository, options);
  const expectedJobs = options.recipients;

  // Adversarial boundary traffic first: at `startMs`, a full minute before the
  // first tick, so the defined delivery measurements cannot observe it.
  const probe = options.pacingBoundaryProbe
    ? await runPacingBoundaryProbe(consumerRepository, options.startMs)
    : { grantTimesMs: [], violations: 0 };

  const errors: string[] = [];
  const ticks: TickRecord[] = [];
  let timedOut = false;
  let doneResolved = false;
  let resolveDone = (): void => {};
  const done = new Promise<void>((resolve) => {
    resolveDone = resolve;
  });

  const schedulerDeps: TickDeps = {
    repository: schedulerRepository,
    queue: queue.producer,
    now: () => timeline.now(),
    logger: logger.logger,
    parser: SIMULATION_PARSER,
    fetch: NETWORK_REFUSING_FETCH,
    sourceTimeZone: 'Europe/Moscow',
    fetchTimeoutMs: 1_000,
    sources: [],
    ownerFactory: createSequenceIds('scheduler'),
  };

  const firstTickAt = options.startMs + options.cronAlignmentMs;

  const runTick = (tickAt: number): void => {
    void (async () => {
      try {
        const firedAtMs = timeline.now();
        const outstandingAtStart = countOutstandingJobs(db);
        const queuePendingAtStart = queue.pendingCount;
        const queueBusyAtStart = queue.isBusy;
        await timeline.delay(options.tickWorkDelayMs);
        const result = await runSchedulerTick(schedulerDeps);
        ticks.push({
          firedAtMs,
          atMs: timeline.now(),
          repaired: result.repaired,
          enqueued: result.enqueued,
          syncStatuses: result.syncStatuses,
          outstandingAtStart,
          queuePendingAtStart,
          queueBusyAtStart,
        });
      } catch (error) {
        errors.push(errorText(error));
      }
    })();
    const nextTickAt = tickAt + MS_PER_MINUTE;
    if (nextTickAt <= options.startMs + options.maxSimulatedMs) {
      timeline.at(nextTickAt, () => runTick(nextTickAt));
    }
  };
  timeline.at(firstTickAt, () => runTick(firstTickAt));

  const countJobs = (where: string): number =>
    Number(
      (
        db.database
          .prepare(`SELECT COUNT(*) AS n FROM outbound_jobs ${where}`)
          .get() as { n: number } | undefined
      )?.n ?? 0,
    );
  const poll = (): void => {
    const settled =
      countJobs('') >= expectedJobs &&
      countJobs("WHERE status NOT IN ('sent', 'cancelled', 'failed')") === 0 &&
      queue.idle();
    if (!doneResolved && settled) {
      doneResolved = true;
      resolveDone();
      return;
    }
    timeline.at(timeline.now() + COMPLETION_POLL_MS, poll);
  };
  timeline.at(firstTickAt + 1, poll);
  timeline.at(options.startMs + options.maxSimulatedMs, () => {
    timedOut = true;
    resolveDone();
  });

  await driveSimulation(timeline, done);

  return collectMetrics(
    db,
    options,
    timeline,
    telegram.state,
    queue,
    ticks,
    errors,
    timedOut,
    expectedJobs,
    probe,
  );
}
