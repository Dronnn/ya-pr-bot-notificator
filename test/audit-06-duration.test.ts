/**
 * Finding 6 regressions: RFC 5545 duration semantics across DST.
 *
 * All fixtures use the real ical.js adapter with a Europe/Berlin VTIMEZONE so
 * the offset transitions are exercised end to end. 2026 transitions:
 * spring-forward 2026-03-29 02:00-03:00 CET -> CEST, fall-back 2026-10-25
 * 03:00-02:00 CEST -> CET. Every 10:00 Berlin wall clock is 09:00 UTC in CET
 * (winter) and 08:00 UTC in CEST (summer).
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { IcalJsCalendarParser } from '../src/calendar/icaljs-parser.ts';
import type { ParseOptions } from '../src/calendar/parser.ts';
import { buildOccurrences, type Occurrence } from '../src/domain/calendar.ts';
import { MS_PER_HOUR } from '../src/util.ts';

const parser = new IcalJsCalendarParser();

const VTIMEZONE_BERLIN = `BEGIN:VTIMEZONE
TZID:Europe/Berlin
BEGIN:DAYLIGHT
TZNAME:CEST
DTSTART:19700329T020000
TZOFFSETFROM:+0100
TZOFFSETTO:+0200
RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU
END:DAYLIGHT
BEGIN:STANDARD
TZNAME:CET
DTSTART:19701025T030000
TZOFFSETFROM:+0200
TZOFFSETTO:+0100
RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU
END:STANDARD
END:VTIMEZONE`;

function calendar(events: string): string {
  return `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//test//EN
${VTIMEZONE_BERLIN}
${events}
END:VCALENDAR`;
}

const OPTIONS: ParseOptions = {
  sourceTimeZone: 'Europe/Moscow',
  horizonStartMs: Date.UTC(2026, 0, 1),
  horizonEndMs: Date.UTC(2027, 0, 1),
  maxIterations: 20_000,
};

function utc(year: number, month: number, day: number, hour: number, minute = 0): number {
  return Date.UTC(year, month - 1, day, hour, minute);
}

async function occurrencesFor(events: string): Promise<Occurrence[]> {
  const parsed = await parser.parse(calendar(events), OPTIONS);
  return buildOccurrences(parsed, {
    horizonStartMs: OPTIONS.horizonStartMs,
    horizonEndMs: OPTIONS.horizonEndMs,
  });
}

function endOf(occurrence: Occurrence): number {
  const end = occurrence.endsAtMs;
  assert.ok(end !== null, 'occurrence should carry an end');
  return end;
}

function startsOf(occurrences: readonly Occurrence[]): number[] {
  return occurrences.map((occurrence) => occurrence.startsAtMs);
}

function endsOf(occurrences: readonly Occurrence[]): number[] {
  return occurrences.map(endOf);
}

function elapsedHours(occurrences: readonly Occurrence[]): number[] {
  return occurrences.map((occurrence) => (endOf(occurrence) - occurrence.startsAtMs) / MS_PER_HOUR);
}

const SPRING_DAILY = `BEGIN:VEVENT
UID:spring-p1d
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Berlin:20260327T100000
DURATION:P1D
RRULE:FREQ=DAILY;COUNT=4
SUMMARY:Spring P1D
END:VEVENT`;

const FALL_DAILY = `BEGIN:VEVENT
UID:fall-p1d
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Berlin:20261023T100000
DURATION:P1D
RRULE:FREQ=DAILY;COUNT=4
SUMMARY:Fall P1D
END:VEVENT`;

describe('finding 6: nominal DURATION components follow wall-clock time', () => {
  it('keeps 10:00 local ends across spring-forward with a 23-hour day', async () => {
    const occurrences = await occurrencesFor(SPRING_DAILY);

    assert.deepEqual(startsOf(occurrences), [
      utc(2026, 3, 27, 9), // 10:00 CET  (UTC+1)
      utc(2026, 3, 28, 9), // 10:00 CET
      utc(2026, 3, 29, 8), // 10:00 CEST (UTC+2) - transition day
      utc(2026, 3, 30, 8), // 10:00 CEST
    ]);
    assert.deepEqual(endsOf(occurrences), [
      utc(2026, 3, 28, 9), // 10:00 local the next day
      utc(2026, 3, 29, 8),
      utc(2026, 3, 30, 8),
      utc(2026, 3, 31, 8),
    ]);
    // Every occurrence ends at 10:00 Berlin wall time; the calendar day across
    // spring-forward is 23 elapsed hours, all other days are 24.
    assert.deepEqual(elapsedHours(occurrences), [24, 23, 24, 24]);
  });

  it('keeps 10:00 local ends across fall-back with a 25-hour day', async () => {
    const occurrences = await occurrencesFor(FALL_DAILY);

    assert.deepEqual(startsOf(occurrences), [
      utc(2026, 10, 23, 8), // 10:00 CEST (UTC+2)
      utc(2026, 10, 24, 8), // 10:00 CEST
      utc(2026, 10, 25, 9), // 10:00 CET  (UTC+1) - transition day
      utc(2026, 10, 26, 9), // 10:00 CET
    ]);
    assert.deepEqual(endsOf(occurrences), [
      utc(2026, 10, 24, 8),
      utc(2026, 10, 25, 9),
      utc(2026, 10, 26, 9),
      utc(2026, 10, 27, 9),
    ]);
    assert.deepEqual(elapsedHours(occurrences), [24, 25, 24, 24]);
  });

  it('keeps DURATION:PT24H exactly 24 elapsed hours across the transition', async () => {
    const occurrences = await occurrencesFor(`BEGIN:VEVENT
UID:spring-pt24h
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Berlin:20260327T100000
DURATION:PT24H
RRULE:FREQ=DAILY;COUNT=4
SUMMARY:Spring PT24H
END:VEVENT`);

    assert.deepEqual(startsOf(occurrences), [
      utc(2026, 3, 27, 9),
      utc(2026, 3, 28, 9),
      utc(2026, 3, 29, 8),
      utc(2026, 3, 30, 8),
    ]);
    // Exact time never shifts with the wall clock: the transition-day instance
    // still ends exactly 24 elapsed hours later (11:00 local, not 10:00).
    assert.deepEqual(elapsedHours(occurrences), [24, 24, 24, 24]);
    assert.deepEqual(endsOf(occurrences), [
      utc(2026, 3, 28, 9),
      utc(2026, 3, 29, 9),
      utc(2026, 3, 30, 8),
      utc(2026, 3, 31, 8),
    ]);
  });

  it('adds nominal day/week and exact time components independently for mixed DURATIONs', async () => {
    // P1DT2H: the nominal day lands on the same 10:00 wall clock (23h across
    // spring-forward), then two exact hours are added on top.
    const spring = await occurrencesFor(`BEGIN:VEVENT
UID:mixed-spring
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Berlin:20260328T100000
DURATION:P1DT2H
RRULE:FREQ=DAILY;COUNT=2
SUMMARY:Mixed spring
END:VEVENT`);
    assert.deepEqual(endsOf(spring), [utc(2026, 3, 29, 10), utc(2026, 3, 30, 10)]);
    assert.deepEqual(elapsedHours(spring), [25, 26]);

    // The same mix across fall-back is 25 nominal hours plus 2 exact hours.
    const fall = await occurrencesFor(`BEGIN:VEVENT
UID:mixed-fall
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Berlin:20261024T100000
DURATION:P1DT2H
RRULE:FREQ=DAILY;COUNT=2
SUMMARY:Mixed fall
END:VEVENT`);
    assert.deepEqual(endsOf(fall), [utc(2026, 10, 25, 11), utc(2026, 10, 26, 11)]);
    assert.deepEqual(elapsedHours(fall), [27, 26]);

    // P1W is nominal too: one calendar week minus the spring-forward hour.
    const weeks = await occurrencesFor(`BEGIN:VEVENT
UID:weeks-spring
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Berlin:20260325T100000
DURATION:P1W
RRULE:FREQ=WEEKLY;COUNT=3
SUMMARY:Nominal week
END:VEVENT`);
    assert.deepEqual(endsOf(weeks), [
      utc(2026, 4, 1, 8),
      utc(2026, 4, 8, 8),
      utc(2026, 4, 15, 8),
    ]);
    assert.deepEqual(elapsedHours(weeks), [167, 168, 168]);
  });

  it('keeps the documented exact master delta for DTEND-derived recurrences', async () => {
    const occurrences = await occurrencesFor(`BEGIN:VEVENT
UID:spring-dtend
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Berlin:20260327T100000
DTEND;TZID=Europe/Berlin:20260327T113000
RRULE:FREQ=DAILY;COUNT=3
SUMMARY:Spring DTEND
END:VEVENT`);

    assert.deepEqual(startsOf(occurrences), [
      utc(2026, 3, 27, 9),
      utc(2026, 3, 28, 9),
      utc(2026, 3, 29, 8),
    ]);
    // DTEND-derived durations keep the master's exact 90-minute span on every
    // instance, including the one starting before the offset transition.
    for (const occurrence of occurrences) {
      assert.equal(endOf(occurrence) - occurrence.startsAtMs, 90 * 60_000);
    }
    assert.deepEqual(endsOf(occurrences), [
      utc(2026, 3, 27, 10, 30),
      utc(2026, 3, 28, 10, 30),
      utc(2026, 3, 29, 9, 30),
    ]);
  });

  it('keeps override-specific starts and ends on their stable origin keys', async () => {
    const occurrences = await occurrencesFor(`BEGIN:VEVENT
UID:override-dst
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Berlin:20260327T100000
DURATION:P1D
RRULE:FREQ=DAILY;COUNT=3
SUMMARY:Master
END:VEVENT
BEGIN:VEVENT
UID:override-dst
RECURRENCE-ID;TZID=Europe/Berlin:20260328T100000
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Berlin:20260328T120000
DURATION:PT2H
SUMMARY:Moved
END:VEVENT
BEGIN:VEVENT
UID:override-dst
RECURRENCE-ID;TZID=Europe/Berlin:20260329T100000
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Berlin:20260329T120000
DTEND;TZID=Europe/Berlin:20260329T133000
SUMMARY:Ended
END:VEVENT`);

    assert.equal(occurrences.length, 3);
    const byKey = new Map(occurrences.map((occurrence) => [occurrence.occurrenceKey, occurrence]));

    const firstOrigin = utc(2026, 3, 27, 9);
    const first = byKey.get(`override-dst#${firstOrigin}`);
    assert.equal(first?.startsAtMs, firstOrigin);
    assert.equal(first?.endsAtMs, utc(2026, 3, 28, 9));

    const movedOrigin = utc(2026, 3, 28, 9); // 10:00 CET
    const moved = byKey.get(`override-dst#${movedOrigin}`);
    assert.equal(moved?.startsAtMs, utc(2026, 3, 28, 11)); // 12:00 CET
    assert.equal(moved?.endsAtMs, utc(2026, 3, 28, 13)); // + PT2H exact

    const endedOrigin = utc(2026, 3, 29, 8); // 10:00 CEST
    const ended = byKey.get(`override-dst#${endedOrigin}`);
    assert.equal(ended?.startsAtMs, utc(2026, 3, 29, 10)); // 12:00 CEST
    assert.equal(ended?.endsAtMs, utc(2026, 3, 29, 11, 30)); // 13:30 CEST
  });
});
