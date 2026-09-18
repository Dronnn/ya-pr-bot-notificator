/**
 * Pure domain rules for course calendar reminders.
 *
 * All times are UTC epoch milliseconds; valid negative values (before 1970)
 * are accepted. Every function validates its input at runtime and throws
 * RangeError on anything invalid; no coercion is performed.
 *
 * evaluateDelivery is a pure decision only: it does NOT ensure idempotency,
 * does NOT claim events and does NOT deliver anything. Deduplication,
 * claiming and sending are the responsibility of later layers.
 */

export type Course = 'basic' | 'extended';

/** A single reminder lead time in minutes before the occurrence start. */
export type ReminderOffsetMinutes = number;

/**
 * The at-start notification is stored as the special offset 0: it fires when
 * the occurrence starts, is seeded by default and is toggled independently of
 * the lead-time rules. It is not accepted by the `add`/`edit` commands.
 */
export const START_REMINDER_OFFSET_MINUTES = 0;
/**
 * Delivery slack for the at-start notification: if the exact start instant
 * cannot be sent (pacing, 429, a retry), it may still go out this much later.
 * Lead-time reminders keep expiring at the start.
 */
export const START_REMINDER_GRACE_MS = 5 * 60_000;

/** Smallest accepted lead time: one minute. Zero means the event start itself. */
export const MIN_REMINDER_OFFSET_MINUTES = 1;
/**
 * Largest accepted lead time: thirty days. This is deliberately the same
 * 30-day span the calendar horizon covers, so the planner's due window never
 * exceeds the materialized occurrences and no rule can point outside them.
 */
export const MAX_REMINDER_OFFSET_MINUTES = 43_200;
/** Upper bound on the number of rules one user may keep enabled at once. */
export const MAX_REMINDER_RULES_PER_USER = 100;
/**
 * Standard rule set every user starts with (and every pre-rules user was
 * migrated to): one day, one hour and five minutes before the start.
 */
export const DEFAULT_REMINDER_OFFSETS: readonly number[] = [1440, 60, 5];

export type SkipReason =
  | 'inactive'
  | 'course-mismatch'
  | 'cancelled'
  | 'revision-mismatch';

export type DeliveryDecision =
  | { kind: 'send' }
  | { kind: 'wait'; retryAtMs: number }
  | { kind: 'skip'; reason: SkipReason }
  | { kind: 'expired' };

export interface DeliveryInput {
  nowMs: number;
  startsAtMs: number;
  /**
   * Instant the delivery window closes. Equals `startsAtMs` for lead-time
   * rules; the at-start notification passes `startsAtMs + grace`.
   */
  expiresAtMs: number;
  sendAtMs: number;
  nextAttemptAtMs: number;
  active: boolean;
  userCourse: Course;
  eventCourse: Course;
  cancelled: boolean;
  expectedRevision: number;
  currentRevision: number;
}

const MINUTE_MS = 60_000;

/**
 * Whether `value` is a valid reminder lead time: an integer in
 * [MIN_REMINDER_OFFSET_MINUTES, MAX_REMINDER_OFFSET_MINUTES].
 */
export function isValidReminderOffset(value: unknown): value is ReminderOffsetMinutes {
  return (
    typeof value === 'number' &&
    Number.isSafeInteger(value) &&
    value >= MIN_REMINDER_OFFSET_MINUTES &&
    value <= MAX_REMINDER_OFFSET_MINUTES
  );
}

/**
 * Whether `value` may be stored as a rule offset: the user-facing lead times
 * plus the special at-start offset 0.
 */
export function isStoredReminderOffset(value: unknown): value is ReminderOffsetMinutes {
  return value === START_REMINDER_OFFSET_MINUTES || isValidReminderOffset(value);
}

/**
 * Validates, de-duplicates and orders a candidate rule set (largest lead time
 * first). The at-start offset does not count toward
 * `MAX_REMINDER_RULES_PER_USER`. Returns null when any value is invalid or the
 * set exceeds the rule budget; never coerces.
 */
export function normalizeReminderOffsets(
  values: readonly number[],
): number[] | null {
  if (!Array.isArray(values)) {
    return null;
  }
  const unique = new Set<number>();
  for (const value of values) {
    if (!isStoredReminderOffset(value)) {
      return null;
    }
    unique.add(value);
  }
  const rules = [...unique].filter((value) => value !== START_REMINDER_OFFSET_MINUTES);
  if (rules.length > MAX_REMINDER_RULES_PER_USER) {
    return null;
  }
  return [...unique].sort((left, right) => right - left);
}

/**
 * Returns startsAtMs - offsetMinutes, in UTC epoch milliseconds.
 * Throws RangeError for invalid offsets, non-safe-integer timestamps and
 * subtraction results that leave the safe integer range.
 */
export function calculateReminderAt(
  startsAtMs: number,
  offsetMinutes: ReminderOffsetMinutes,
): number {
  assertTimeMs(startsAtMs, 'startsAtMs');
  if (!isValidReminderOffset(offsetMinutes)) {
    throw new RangeError(
      `offsetMinutes must be an integer in [${MIN_REMINDER_OFFSET_MINUTES}, ${MAX_REMINDER_OFFSET_MINUTES}], got ${String(offsetMinutes)}`,
    );
  }
  const reminderAtMs = startsAtMs - offsetMinutes * MINUTE_MS;
  if (!Number.isSafeInteger(reminderAtMs)) {
    throw new RangeError(
      'reminder time overflows the safe integer range for the given inputs',
    );
  }
  return reminderAtMs;
}

/**
 * Decides what to do with one reminder candidate right now.
 *
 * The decision ladder is total and strictly ordered:
 * 1. skip: the first applicable reason from `skipReasonFor`;
 * 2. expired: the delivery window has closed (`expiresAtMs`);
 * 3. wait: nowMs is before `dueAtMs`, the max of sendAtMs and the retry time;
 * 4. send: otherwise.
 * Skip outranks expiry, expiry outranks wait, and a send can never happen
 * before sendAtMs or after the window closes.
 */
export function evaluateDelivery(input: DeliveryInput): DeliveryDecision {
  assertDeliveryInput(input);

  const skipReason = skipReasonFor(input);
  if (skipReason !== null) {
    return { kind: 'skip', reason: skipReason };
  }
  if (windowClosed(input)) {
    return { kind: 'expired' };
  }

  const retryAtMs = dueAtMs(input);
  if (input.nowMs < retryAtMs) {
    return { kind: 'wait', retryAtMs };
  }
  return { kind: 'send' };
}

/** Checks every field in a fixed order; the first violated rule decides the error. */
function assertDeliveryInput(input: DeliveryInput): void {
  assertTimeMs(input.nowMs, 'nowMs');
  assertTimeMs(input.startsAtMs, 'startsAtMs');
  assertTimeMs(input.expiresAtMs, 'expiresAtMs');
  assertTimeMs(input.sendAtMs, 'sendAtMs');
  assertTimeMs(input.nextAttemptAtMs, 'nextAttemptAtMs');
  assertRevision(input.expectedRevision, 'expectedRevision');
  assertRevision(input.currentRevision, 'currentRevision');
  assertBoolean(input.active, 'active');
  assertBoolean(input.cancelled, 'cancelled');
  assertCourse(input.userCourse, 'userCourse');
  assertCourse(input.eventCourse, 'eventCourse');
  if (input.sendAtMs > input.startsAtMs) {
    throw new RangeError('sendAtMs must not be after startsAtMs');
  }
  if (input.startsAtMs > input.expiresAtMs) {
    throw new RangeError('expiresAtMs must not be before startsAtMs');
  }
}

function assertTimeMs(value: unknown, name: string): void {
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) {
    throw new RangeError(
      `${name} must be a finite safe integer of epoch milliseconds, got ${String(value)}`,
    );
  }
}

function assertRevision(value: unknown, name: string): void {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(
      `${name} must be a nonnegative safe integer, got ${String(value)}`,
    );
  }
}

function assertBoolean(value: unknown, name: string): void {
  if (typeof value !== 'boolean') {
    throw new RangeError(`${name} must be a boolean, got ${String(value)}`);
  }
}

function assertCourse(value: unknown, name: string): asserts value is Course {
  if (value !== 'basic' && value !== 'extended') {
    throw new RangeError(`${name} must be 'basic' or 'extended', got ${String(value)}`);
  }
}

/** First skip reason that applies, in priority order. */
function skipReasonFor(input: DeliveryInput): SkipReason | null {
  if (!input.active) {
    return 'inactive';
  }
  if (input.userCourse !== input.eventCourse) {
    return 'course-mismatch';
  }
  if (input.cancelled) {
    return 'cancelled';
  }
  if (input.expectedRevision !== input.currentRevision) {
    return 'revision-mismatch';
  }
  return null;
}

/**
 * A lead-time reminder expires at its own start; only the at-start
 * notification passes a later `expiresAtMs` (start plus its grace window).
 */
function windowClosed(input: DeliveryInput): boolean {
  return input.nowMs >= input.expiresAtMs;
}

/** Earliest instant the candidate may be sent: sendAtMs unless a later retry is due. */
function dueAtMs(input: DeliveryInput): number {
  return Math.max(input.sendAtMs, input.nextAttemptAtMs);
}
