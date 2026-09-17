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

export type ReminderOffsetMinutes = 30 | 1440;

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

/** Every value ReminderOffsetMinutes may hold. */
const VALID_OFFSET_MINUTES: readonly ReminderOffsetMinutes[] = [30, 1440];

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
  if (!VALID_OFFSET_MINUTES.includes(offsetMinutes)) {
    throw new RangeError(
      `offsetMinutes must be 30 or 1440, got ${String(offsetMinutes)}`,
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
 * 2. expired: the event has already started (`hasStarted`);
 * 3. wait: nowMs is before `dueAtMs`, the max of sendAtMs and the retry time;
 * 4. send: otherwise.
 * Skip outranks expiry, expiry outranks wait, and a send can never happen
 * before sendAtMs or after the event has started.
 */
export function evaluateDelivery(input: DeliveryInput): DeliveryDecision {
  assertDeliveryInput(input);

  const skipReason = skipReasonFor(input);
  if (skipReason !== null) {
    return { kind: 'skip', reason: skipReason };
  }
  if (hasStarted(input)) {
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

/** An event expires at its own start; a reminder is never sent late. */
function hasStarted(input: DeliveryInput): boolean {
  return input.nowMs >= input.startsAtMs;
}

/** Earliest instant the candidate may be sent: sendAtMs unless a later retry is due. */
function dueAtMs(input: DeliveryInput): number {
  return Math.max(input.sendAtMs, input.nextAttemptAtMs);
}
