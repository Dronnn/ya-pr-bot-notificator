/**
 * Shared, dependency-free helpers. No Node-only APIs are used here so the same
 * code runs in Workers, in tests and in plain Node.
 */

// Time units.
export const MS_PER_MINUTE = 60_000;
export const MS_PER_HOUR = 3_600_000;
export const MS_PER_DAY = 86_400_000;

// Send pacing.
export const BOT_SEND_PACE_PER_SECOND = 20;
export const PACE_MAX_WAIT_MS = 2_000;
export const PACE_RETRY_DELAY_MS = 200;

// Job lease, retry backoff and batch bounds.
export const MAX_SEND_BATCH = 100;
export const MAX_JOB_ATTEMPTS = 6;
export const JOB_LEASE_MS = 60_000;
/**
 * Scheduler enqueue reservation: deliberately longer than JOB_LEASE_MS so a
 * queue backlog that is still draining is not re-enqueued by overlapping ticks.
 * A truly lost queue message is repaired when this reservation expires.
 */
export const JOB_ENQUEUE_LEASE_MS = 10 * MS_PER_MINUTE;
export const JOB_BACKOFF_BASE_MS = 5_000;
const JOB_BACKOFF_CAP_MS = 15 * MS_PER_MINUTE;

// Source sync leases and freshness.
export const SOURCE_LEASE_MS = 5 * MS_PER_MINUTE;
/**
 * A Monday snapshot remains eligible through the next Monday plus one day of
 * retry room. This matches the weekly source-download requirement without
 * letting a failed refresh look fresh indefinitely.
 */
export const STALE_SOURCE_CUTOFF_MS = 8 * MS_PER_DAY;

// Webhook intake.
export const WEBHOOK_LEASE_MS = 2 * MS_PER_MINUTE;
export const MAX_WEBHOOK_BODY_BYTES = 64 * 1024;
/** Finite deadline for reading a webhook body; a stalled pull must not hold the invocation. */
export const WEBHOOK_BODY_READ_TIMEOUT_MS = 10_000;

// Calendar snapshot and expansion bounds.
export const MAX_ICAL_BYTES = 8 * 1024 * 1024;
export const EXPANSION_HORIZON_MS = 30 * MS_PER_DAY;
export const MAX_EXPANSION_ITERATIONS = 20_000;
/** A 304 re-expands once the materialized horizon is this close to running out. */
export const HORIZON_REFRESH_MARGIN_MS = 7 * MS_PER_DAY;
/** Lower bound between two 304-driven unconditional refreshes. */
export const HORIZON_REFRESH_MIN_INTERVAL_MS = MS_PER_DAY;

// Telegram payload and request limits.
export const TELEGRAM_TEXT_LIMIT = 4096;
export const TELEGRAM_TIMEOUT_MS = 10_000;

// Per-invocation D1 statement budgets and chunking.
export const MAX_D1_STATEMENTS_PER_CONSUMER = 50;
export const MAX_D1_STATEMENTS_PER_SCHEDULER = 50;
/** Statements each source sync must leave for the tick's planning, enqueue and cleanup tail. */
export const SYNC_TAIL_RESERVE = 8;
/**
 * Worst-case repository statements one queue message may execute, measured on
 * the final code (single-statement `acquireSendSlot`, claim-time join for
 * command ordering, no extra pre-read): claim (1), a paced slot reservation
 * with one wait (failed acquire 1 + pace read 1 + successful acquire 1), the
 * first reservation (1 when reserved), the persisted cooldown (1), the
 * in-place rate-limit retry (second slot 1 plus second reservation 1) and the
 * sent outcome with its delivery-ledger insert (2): 1 + 3 + 1 + 1 + 1 + 1 + 2
 * = 10, verified by driving the waited short-429 retry path and observing
 * `statementsUsed() === 10`. It is the admission unit: a message (or a chunk
 * of messages) starts only while this many statements per message still fit
 * the invocation budget, so a started send always has room to persist its
 * outcome - the budget can never be exhausted mid-flight.
 *
 * Per-method costs: claimJobContext 1; acquireSendSlot 1 (conditional INSERT;
 * old-row GC is once-per-batch via `pruneRateStarts`, not per send);
 * pruneRateStarts 1 per batch; getPaceState 1; beginSendAttempt 1 when
 * reserved, 2 otherwise (UPDATE + classification probe); setSendCooldown 1;
 * finishJobSent 2 (UPDATE + ledger INSERT in one batch); rescheduleJob,
 * finishJobTerminal, finishJobUnusable (cancel + refund in one statement),
 * deactivateUser 1 each. Cheaper paths (screened command
 * supersede 2; unwaited paced defer 4; plain success 5; command success 5;
 * success after one pace wait 7; waited in-place retry 10) all fit inside the
 * same reserve.
 */
export const MESSAGE_BUDGET_FLOOR = 10;
/** Queue redelivery delay for a message deferred because the invocation budget was spent. */
export const BUDGET_DEFER_RETRY_DELAY_SECONDS = 1;
export const MAX_D1_BATCH_STATEMENTS = 50;
/** Keeps bound parameters per statement under D1's 100-parameter limit. */
export const MAX_SQL_PARAMS = 90;

// Retention and cleanup.
export const RETENTION_MS = 24 * MS_PER_HOUR;
export const CLEANUP_LIMIT = 500;
/**
 * Sent reminder jobs are also the durable dedup tombstone: they must outlive
 * the 30-day materialized horizon so a moved occurrence that is re-materialized
 * inside it can never re-send. Occurrence rows keep the short retention; only
 * sent reminders hold the long one.
 */
export const SENT_REMINDER_RETENTION_MS =
  EXPANSION_HORIZON_MS + RETENTION_MS + 14 * MS_PER_DAY;

/** Europe/Moscow has been a fixed UTC+3 offset since 2014 (no DST). */
export const MOSCOW_OFFSET_MINUTES = 180;

/** Injected clock in epoch milliseconds; tests substitute a controllable fake. */
export type Clock = () => number;

// Input guards.
export function assertSafeInteger(value: unknown, name: string): asserts value is number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
    throw new RangeError(`${name} must be a finite safe integer, got ${String(value)}`);
  }
}

/** Longest accepted IANA timezone identifier; longer input is rejected unparsed. */
export const MAX_TIME_ZONE_LENGTH = 64;

/**
 * Whether a timezone identifier may be stored as a user preference: non-empty,
 * within `MAX_TIME_ZONE_LENGTH`, not a raw numeric offset (newer engines
 * accept `+03:00`, which is not DST-safe) and not the non-geographic tzdb
 * sentinel `Factory` (accepted by some runtimes and rejected by others, so
 * accepting it would make validation runtime-dependent). Applied to the raw
 * input to bound parsing and again to the canonical id resolved by `Intl`.
 */
export function isUsableTimeZoneId(id: string): boolean {
  return (
    id.length > 0 &&
    id.length <= MAX_TIME_ZONE_LENGTH &&
    !id.startsWith('+') &&
    !id.startsWith('-') &&
    id.toLowerCase() !== 'factory'
  );
}

/**
 * Validates and canonicalizes a user-supplied timezone with the runtime's
 * native `Intl.DateTimeFormat` support. Returns the canonical identifier
 * (`resolvedOptions().timeZone`, so aliases settle deterministically per
 * runtime) or null for empty, whitespace-only, overlong or unusable input.
 * Nothing is logged here, so free-form input never reaches a log.
 */
export function normalizeTimeZone(value: string): string | null {
  const trimmed = value.trim();
  if (!isUsableTimeZoneId(trimmed)) {
    return null;
  }
  try {
    const resolved = new Intl.DateTimeFormat('en-US', {
      timeZone: trimmed,
    }).resolvedOptions().timeZone;
    if (typeof resolved !== 'string' || !isUsableTimeZoneId(resolved)) {
      return null;
    }
    return resolved;
  } catch {
    return null;
  }
}

/**
 * Explicit, unambiguous local rendering in the stored user timezone: local
 * date, local time, the IANA identifier and the offset in effect at that
 * instant (`GMT+N`, DST-aware), e.g.
 * `2024-07-01 12:00 Europe/Berlin (GMT+2)`.
 */
export function formatUserTime(epochMs: number, timeZone: string): string {
  assertSafeInteger(epochMs, 'epochMs');
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
    timeZoneName: 'shortOffset',
  });
  const parts = formatter.formatToParts(new Date(epochMs));
  const pick = (type: Intl.DateTimeFormatPartTypes): string =>
    parts.find((part) => part.type === type)?.value ?? '';
  const offset = pick('timeZoneName');
  const rendered = `${pick('year')}-${pick('month')}-${pick('day')} ${pick('hour')}:${pick('minute')} ${timeZone}`;
  return offset.length === 0 ? rendered : `${rendered} (${offset})`;
}

/**
 * Clamps text to Telegram's limit on code-point boundaries so surrogate pairs
 * are never split. Returns the original string when it already fits.
 */
export function clampTelegramText(text: string, limit: number = TELEGRAM_TEXT_LIMIT): string {
  if (text.length <= limit) {
    return text;
  }
  const slice = text.slice(0, limit - 1);
  const trimmed = /[\uD800-\uDBFF]$/.test(slice) ? slice.slice(0, -1) : slice;
  return `${trimmed}\u2026`;
}

const TOKEN_PATTERN = /\b\d{6,}:[A-Za-z0-9_-]{20,}\b/g;
const URL_SECRET_PATTERN = /https?:\/\/[^\s"']+/g;

/**
 * Best-effort scrub of arbitrary text. Known secrets of at least four
 * characters are replaced first, then bot-token-shaped fragments and finally
 * every http(s) URL. Shorter secrets are left untouched.
 */
export function redactText(text: string, secrets: readonly string[] = []): string {
  let output = text;
  for (const secret of secrets) {
    if (secret.length >= 4) {
      output = output.split(secret).join('[redacted]');
    }
  }
  output = output.replace(TOKEN_PATTERN, '[redacted-token]');
  output = output.replace(URL_SECRET_PATTERN, '[redacted-url]');
  return output;
}

/** Compares two secrets in constant time, without an early exit on the first mismatch. */
export function constantTimeEqual(a: string, b: string): boolean {
  const encoder = new TextEncoder();
  const left = encoder.encode(a);
  const right = encoder.encode(b);
  let diff = left.length ^ right.length;
  const length = Math.max(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    diff |= (left[index] ?? 0) ^ (right[index] ?? 0);
  }
  return diff === 0;
}

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface Logger {
  debug(event: string, fields?: Record<string, unknown>): void;
  info(event: string, fields?: Record<string, unknown>): void;
  warn(event: string, fields?: Record<string, unknown>): void;
  error(event: string, fields?: Record<string, unknown>): void;
}

export type LogSink = (line: string) => void;

/**
 * Returns a logger that drops null/undefined fields, redacts secret strings
 * through `redactText` (recursively for nested arrays/objects, bounded depth)
 * and emits one JSON line per call. Field names and non-string scalars are
 * emitted unchanged.
 */
export function createLogger(sink: LogSink, secrets: readonly string[] = []): Logger {
  const redactValue = (value: unknown, depth: number): unknown => {
    if (typeof value === 'string') {
      return redactText(value, secrets);
    }
    if (depth >= 4) {
      return value;
    }
    if (Array.isArray(value)) {
      return value.map((item) => redactValue(item, depth + 1));
    }
    if (value !== null && typeof value === 'object') {
      const nested: Record<string, unknown> = {};
      for (const [key, item] of Object.entries(value)) {
        nested[key] = redactValue(item, depth + 1);
      }
      return nested;
    }
    return value;
  };
  const emit = (level: LogLevel, event: string, fields: Record<string, unknown> = {}): void => {
    const safe: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(fields)) {
      if (value === undefined || value === null) {
        continue;
      }
      safe[key] = redactValue(value, 0);
    }
    sink(JSON.stringify({ level, event, ...safe }));
  };
  return {
    debug: (event, fields) => emit('debug', event, fields),
    info: (event, fields) => emit('info', event, fields),
    warn: (event, fields) => emit('warn', event, fields),
    error: (event, fields) => emit('error', event, fields),
  };
}

/**
 * Deterministic exponential backoff capped at JOB_BACKOFF_CAP_MS, with up to
 * 25% jitter. Attempts below one behave like the first attempt.
 */
export function backoffMs(attempt: number, random: () => number): number {
  const exponent = Math.max(0, attempt - 1);
  const base = Math.min(JOB_BACKOFF_CAP_MS, JOB_BACKOFF_BASE_MS * 2 ** exponent);
  const jitter = Math.floor(base * 0.25 * random());
  return base + jitter;
}
