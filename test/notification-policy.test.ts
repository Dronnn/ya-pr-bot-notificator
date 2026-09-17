import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  calculateReminderAt,
  evaluateDelivery,
  type Course,
  type DeliveryInput,
  type ReminderOffsetMinutes,
} from '../src/domain/notification-policy.ts';

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;

const baseInput = (overrides: Partial<DeliveryInput> = {}): DeliveryInput => ({
  nowMs: 0,
  startsAtMs: 30 * MINUTE,
  sendAtMs: 0,
  nextAttemptAtMs: 0,
  active: true,
  userCourse: 'basic',
  eventCourse: 'basic',
  cancelled: false,
  expectedRevision: 1,
  currentRevision: 1,
  ...overrides,
});

describe('calculateReminderAt', () => {
  it('subtracts the 30 minute offset', () => {
    assert.equal(calculateReminderAt(HOUR, 30), HOUR - 30 * MINUTE);
  });

  it('subtracts the 1440 minute (one day) offset', () => {
    assert.equal(calculateReminderAt(DAY, 1440), 0);
  });

  it('accepts valid negative epoch milliseconds', () => {
    assert.equal(calculateReminderAt(-30 * MINUTE, 30), -HOUR);
    assert.equal(calculateReminderAt(-DAY, 1440), -2 * DAY);
  });

  it('returns exactly zero at the offset boundary', () => {
    assert.equal(calculateReminderAt(30 * MINUTE, 30), 0);
    assert.equal(calculateReminderAt(DAY, 1440), 0);
  });

  it('rejects offsets that are not 30 or 1440 via runtime cast', () => {
    for (const offset of [0, 15, 60, 1441, 30.5, -30]) {
      assert.throws(
        () => calculateReminderAt(0, offset as unknown as ReminderOffsetMinutes),
        RangeError,
      );
    }
  });

  it('rejects NaN and infinite offsets', () => {
    assert.throws(
      () => calculateReminderAt(0, NaN as unknown as ReminderOffsetMinutes),
      RangeError,
    );
    assert.throws(
      () => calculateReminderAt(0, Infinity as unknown as ReminderOffsetMinutes),
      RangeError,
    );
    assert.throws(
      () => calculateReminderAt(0, -Infinity as unknown as ReminderOffsetMinutes),
      RangeError,
    );
  });

  it('rejects non-numeric and non-integer timestamps', () => {
    for (const value of [NaN, Infinity, -Infinity, 1.5, '0', null, undefined]) {
      assert.throws(
        () => calculateReminderAt(value as unknown as number, 30),
        RangeError,
      );
    }
  });

  it('rejects unsafe integer timestamps', () => {
    assert.throws(
      () => calculateReminderAt(Number.MAX_SAFE_INTEGER + 1, 30),
      RangeError,
    );
  });

  it('rejects subtraction results outside the safe integer range', () => {
    assert.throws(
      () => calculateReminderAt(Number.MIN_SAFE_INTEGER, 1440),
      RangeError,
    );
  });
});

describe('evaluateDelivery input validation', () => {
  it('rejects invalid timestamps', () => {
    for (const value of [NaN, Infinity, -Infinity, 1.5, '0']) {
      assert.throws(
        () => evaluateDelivery(baseInput({ nowMs: value as unknown as number })),
        RangeError,
      );
    }
  });

  it('rejects unsafe timestamps', () => {
    assert.throws(
      () =>
        evaluateDelivery(
          baseInput({ startsAtMs: Number.MAX_SAFE_INTEGER + 1 }),
        ),
      RangeError,
    );
  });

  it('rejects negative and non-integer revisions', () => {
    assert.throws(
      () => evaluateDelivery(baseInput({ currentRevision: -1 })),
      RangeError,
    );
    assert.throws(
      () => evaluateDelivery(baseInput({ expectedRevision: 1.5 })),
      RangeError,
    );
  });

  it('rejects unsafe revisions', () => {
    assert.throws(
      () => evaluateDelivery(baseInput({ currentRevision: Number.MAX_SAFE_INTEGER + 1 })),
      RangeError,
    );
  });

  it('rejects invalid course values', () => {
    assert.throws(
      () => evaluateDelivery(baseInput({ userCourse: 'free' as unknown as Course })),
      RangeError,
    );
    assert.throws(
      () => evaluateDelivery(baseInput({ eventCourse: 'pro' as unknown as Course })),
      RangeError,
    );
  });

  it('rejects non-boolean flags', () => {
    assert.throws(
      () => evaluateDelivery(baseInput({ active: 1 as unknown as boolean })),
      RangeError,
    );
    assert.throws(
      () => evaluateDelivery(baseInput({ cancelled: 'no' as unknown as boolean })),
      RangeError,
    );
  });

  it('rejects sendAtMs after startsAtMs but allows equality', () => {
    assert.throws(
      () => evaluateDelivery(baseInput({ sendAtMs: 30 * MINUTE + 1 })),
      RangeError,
    );
    assert.doesNotThrow(() =>
      evaluateDelivery(baseInput({ sendAtMs: 30 * MINUTE })),
    );
  });
});

describe('evaluateDelivery decisions', () => {
  it('sends when due, active, matching and revision is current', () => {
    assert.deepEqual(evaluateDelivery(baseInput({ nowMs: 0 })), { kind: 'send' });
  });

  it('sends exactly at sendAtMs when no later retry is scheduled', () => {
    assert.deepEqual(
      evaluateDelivery(baseInput({ sendAtMs: 10 * MINUTE, nowMs: 10 * MINUTE })),
      { kind: 'send' },
    );
  });

  it('waits one millisecond before sendAtMs', () => {
    assert.deepEqual(
      evaluateDelivery(baseInput({ sendAtMs: 10 * MINUTE, nowMs: 10 * MINUTE - 1 })),
      { kind: 'wait', retryAtMs: 10 * MINUTE },
    );
  });

  it('sends a due catch-up well after sendAtMs but before start', () => {
    assert.deepEqual(
      evaluateDelivery(
        baseInput({ sendAtMs: 1 * MINUTE, nextAttemptAtMs: 2 * MINUTE, nowMs: 20 * MINUTE }),
      ),
      { kind: 'send' },
    );
  });

  it('expires exactly at startsAtMs', () => {
    assert.deepEqual(
      evaluateDelivery(baseInput({ nowMs: 30 * MINUTE })),
      { kind: 'expired' },
    );
  });

  it('expires after the event started even when otherwise due', () => {
    assert.deepEqual(
      evaluateDelivery(baseInput({ nowMs: 30 * MINUTE + 1 })),
      { kind: 'expired' },
    );
  });

  it('skips inactive events with an explicit reason', () => {
    assert.deepEqual(
      evaluateDelivery(baseInput({ active: false, nowMs: 0 })),
      { kind: 'skip', reason: 'inactive' },
    );
  });

  it('skips course mismatches with an explicit reason', () => {
    assert.deepEqual(
      evaluateDelivery(baseInput({ userCourse: 'extended', eventCourse: 'basic' })),
      { kind: 'skip', reason: 'course-mismatch' },
    );
  });

  it('skips cancelled events with an explicit reason', () => {
    assert.deepEqual(
      evaluateDelivery(baseInput({ cancelled: true })),
      { kind: 'skip', reason: 'cancelled' },
    );
  });

  it('skips stale revisions even when the reminder is due', () => {
    assert.deepEqual(
      evaluateDelivery(
        baseInput({ nowMs: 10 * MINUTE, expectedRevision: 1, currentRevision: 2 }),
      ),
      { kind: 'skip', reason: 'revision-mismatch' },
    );
  });

  it('gives skip priority over expiry', () => {
    assert.deepEqual(
      evaluateDelivery(
        baseInput({ active: false, nowMs: 30 * MINUTE + 1 }),
      ),
      { kind: 'skip', reason: 'inactive' },
    );
  });

  it('delays a retry that is later than sendAtMs', () => {
    assert.deepEqual(
      evaluateDelivery(
        baseInput({ sendAtMs: 1 * MINUTE, nextAttemptAtMs: 5 * MINUTE, nowMs: 2 * MINUTE }),
      ),
      { kind: 'wait', retryAtMs: 5 * MINUTE },
    );
  });

  it('never sends an early retry before sendAtMs', () => {
    assert.deepEqual(
      evaluateDelivery(
        baseInput({ sendAtMs: 5 * MINUTE, nextAttemptAtMs: 1 * MINUTE, nowMs: 2 * MINUTE }),
      ),
      { kind: 'wait', retryAtMs: 5 * MINUTE },
    );
    assert.deepEqual(
      evaluateDelivery(
        baseInput({ sendAtMs: 5 * MINUTE, nextAttemptAtMs: 1 * MINUTE, nowMs: 5 * MINUTE }),
      ),
      { kind: 'send' },
    );
  });

  it('accepts negative epoch milliseconds consistently', () => {
    assert.deepEqual(
      evaluateDelivery(
        baseInput({
          nowMs: -2 * HOUR,
          startsAtMs: -HOUR,
          sendAtMs: -3 * HOUR,
          nextAttemptAtMs: -3 * HOUR,
        }),
      ),
      { kind: 'send' },
    );
  });

  it('decides 1000 reminders independently without truncation', () => {
    const count = 1000;
    const decisions = Array.from({ length: count }, (_, index) =>
      evaluateDelivery(
        baseInput({
          nowMs: index,
          startsAtMs: 10 * HOUR,
          sendAtMs: 0,
          nextAttemptAtMs: 0,
          expectedRevision: index,
          currentRevision: index,
        }),
      ),
    );

    assert.equal(decisions.length, count);
    for (const decision of decisions) {
      assert.deepEqual(decision, { kind: 'send' });
    }
  });
});
