import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  buildOccurrences,
  CalendarIntegrityError,
  type BuildOptions,
  type ParsedEvent,
} from '../src/domain/calendar.ts';
import { MS_PER_DAY, MS_PER_HOUR } from '../src/util.ts';

const MONDAY = Date.UTC(2026, 0, 5, 9, 0, 0);

function event(
  overrides: Partial<ParsedEvent> & { uid: string; startsAtMs: number },
): ParsedEvent {
  return {
    summary: 'Lesson',
    description: null,
    url: null,
    endsAtMs: null,
    status: 'confirmed',
    isAllDay: false,
    recurrenceIdMs: null,
    expandedStartsMs: null,
    timezone: 'Europe/Moscow',
    ...overrides,
  };
}

function options(horizonEndMs: number, horizonStartMs = 0): BuildOptions {
  return { horizonStartMs, horizonEndMs };
}

describe('occurrence building', () => {
  it('emits a single instance for a non-recurring event', () => {
    const occurrences = buildOccurrences(
      { events: [event({ uid: 'a', startsAtMs: MONDAY })] },
      options(MONDAY + 30 * MS_PER_DAY),
    );
    assert.deepEqual(
      occurrences.map((occurrence) => occurrence.startsAtMs),
      [MONDAY],
    );
  });

  it('emits every parser-expanded start', () => {
    const occurrences = buildOccurrences(
      {
        events: [
          event({
            uid: 'a',
            startsAtMs: MONDAY,
            expandedStartsMs: [MONDAY, MONDAY + 7 * MS_PER_DAY, MONDAY + 14 * MS_PER_DAY],
          }),
        ],
      },
      options(MONDAY + 30 * MS_PER_DAY),
    );
    assert.deepEqual(
      occurrences.map((occurrence) => occurrence.startsAtMs),
      [MONDAY, MONDAY + 7 * MS_PER_DAY, MONDAY + 14 * MS_PER_DAY],
    );
  });

  it('derives each occurrence end from its own start (duration preserved)', () => {
    const duration = 90 * 60_000;
    const occurrences = buildOccurrences(
      {
        events: [
          event({
            uid: 'a',
            startsAtMs: MONDAY,
            endsAtMs: MONDAY + duration,
            expandedStartsMs: [MONDAY, MONDAY + 7 * MS_PER_DAY],
          }),
        ],
      },
      options(MONDAY + 30 * MS_PER_DAY),
    );
    for (const occurrence of occurrences) {
      assert.equal(occurrence.endsAtMs, occurrence.startsAtMs + duration);
    }
    assert.notEqual(occurrences[0]?.endsAtMs, occurrences[1]?.endsAtMs);
  });

  it('deduplicates repeated expanded starts', () => {
    const occurrences = buildOccurrences(
      {
        events: [
          event({
            uid: 'a',
            startsAtMs: MONDAY,
            expandedStartsMs: [MONDAY, MONDAY, MONDAY + 7 * MS_PER_DAY],
          }),
        ],
      },
      options(MONDAY + 30 * MS_PER_DAY),
    );
    assert.deepEqual(
      occurrences.map((occurrence) => occurrence.startsAtMs),
      [MONDAY, MONDAY + 7 * MS_PER_DAY],
    );
  });

  it('filters instances outside the horizon', () => {
    const occurrences = buildOccurrences(
      {
        events: [
          event({
            uid: 'a',
            startsAtMs: MONDAY,
            expandedStartsMs: [MONDAY - 2 * MS_PER_DAY, MONDAY, MONDAY + 40 * MS_PER_DAY],
          }),
        ],
      },
      options(MONDAY + 30 * MS_PER_DAY, MONDAY - MS_PER_DAY),
    );
    assert.deepEqual(
      occurrences.map((occurrence) => occurrence.startsAtMs),
      [MONDAY],
    );
  });

  it('keeps a stable identity for a moved RECURRENCE-ID instance', () => {
    const origin = MONDAY + 7 * MS_PER_DAY;
    const movedTo = origin + 2 * MS_PER_HOUR;
    const occurrences = buildOccurrences(
      {
        events: [
          event({ uid: 'a', startsAtMs: MONDAY, expandedStartsMs: [MONDAY, origin] }),
          event({ uid: 'a', startsAtMs: movedTo, recurrenceIdMs: origin }),
        ],
      },
      options(MONDAY + 30 * MS_PER_DAY),
    );
    const moved = occurrences.find((occurrence) => occurrence.occurrenceKey === `a#${origin}`);
    assert.ok(moved !== undefined);
    assert.equal(moved.startsAtMs, movedTo);
    assert.equal(moved.occurrenceKey, `a#${origin}`);
  });

  it('applies an explicit cancellation override', () => {
    const origin = MONDAY + MS_PER_DAY;
    const occurrences = buildOccurrences(
      {
        events: [
          event({ uid: 'a', startsAtMs: MONDAY, expandedStartsMs: [MONDAY, origin] }),
          event({ uid: 'a', startsAtMs: origin, recurrenceIdMs: origin, status: 'cancelled' }),
        ],
      },
      options(MONDAY + 30 * MS_PER_DAY),
    );
    const cancelled = occurrences.find((occurrence) => occurrence.occurrenceKey === `a#${origin}`);
    assert.equal(cancelled?.status, 'cancelled');
  });

  it('includes an override moved into the horizon from an unexpanded origin', () => {
    const origin = MONDAY + 60 * MS_PER_DAY;
    const movedTo = MONDAY + 2 * MS_PER_DAY;
    const occurrences = buildOccurrences(
      {
        events: [
          event({ uid: 'a', startsAtMs: MONDAY, expandedStartsMs: [MONDAY] }),
          event({ uid: 'a', startsAtMs: movedTo, recurrenceIdMs: origin }),
        ],
      },
      options(MONDAY + 30 * MS_PER_DAY),
    );
    const moved = occurrences.find((occurrence) => occurrence.occurrenceKey === `a#${origin}`);
    assert.ok(moved !== undefined);
    assert.equal(moved.startsAtMs, movedTo);
  });

  it('drops an override moved beyond the horizon', () => {
    const origin = MONDAY + 2 * MS_PER_DAY;
    const occurrences = buildOccurrences(
      {
        events: [
          event({ uid: 'a', startsAtMs: MONDAY, expandedStartsMs: [MONDAY, origin] }),
          event({ uid: 'a', startsAtMs: MONDAY + 100 * MS_PER_DAY, recurrenceIdMs: origin }),
        ],
      },
      options(MONDAY + 30 * MS_PER_DAY),
    );
    assert.equal(
      occurrences.some((occurrence) => occurrence.occurrenceKey === `a#${origin}`),
      false,
    );
  });

  it('rejects floating times without an explicit zone', () => {
    assert.throws(
      () =>
        buildOccurrences(
          { events: [event({ uid: 'a', startsAtMs: MONDAY, timezone: null })] },
          options(MONDAY + MS_PER_DAY),
        ),
      CalendarIntegrityError,
    );
  });

  it('rejects a non-integer expanded start', () => {
    assert.throws(
      () =>
        buildOccurrences(
          {
            events: [
              event({ uid: 'a', startsAtMs: MONDAY, expandedStartsMs: [MONDAY, 1.5] }),
            ],
          },
          options(MONDAY + MS_PER_DAY),
        ),
      RangeError,
    );
  });

  it('skips all-day events by explicit policy', () => {
    const occurrences = buildOccurrences(
      {
        events: [
          event({ uid: 'a', startsAtMs: MONDAY, isAllDay: true, timezone: null }),
        ],
      },
      options(MONDAY + MS_PER_DAY),
    );
    assert.equal(occurrences.length, 0);
  });
});
