/**
 * Tests for the real ical.js parser adapter. These import the runtime
 * dependency directly (declared in package.json) so recurrence expansion,
 * timezones and normalization are exercised together against synthetic ICS
 * fixtures instead of only the injected fake.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { IcalJsCalendarParser } from '../src/calendar/icaljs-parser.ts';
import type { ParseOptions } from '../src/calendar/parser.ts';
import {
  buildOccurrences,
  CalendarIntegrityError,
  RecurrenceLimitError,
  type Occurrence,
  type ParsedCalendar,
} from '../src/domain/calendar.ts';
import { MS_PER_DAY } from '../src/util.ts';

const parser = new IcalJsCalendarParser();

const VTIMEZONE_MOSCOW = `BEGIN:VTIMEZONE
TZID:Europe/Moscow
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETTO:+0300
TZNAME:MSK
END:STANDARD
END:VTIMEZONE`;

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

function calendar(events: string, vtimezone = VTIMEZONE_MOSCOW): string {
  return `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//test//EN
${vtimezone}
${events}
END:VCALENDAR`;
}

/** Europe/Moscow wall clock to UTC epoch ms (fixed UTC+3). */
function msk(year: number, month: number, day: number, hour = 0, minute = 0): number {
  return Date.UTC(year, month - 1, day, hour - 3, minute);
}

const HORIZON_2024: Pick<ParseOptions, 'horizonStartMs' | 'horizonEndMs'> = {
  horizonStartMs: Date.UTC(2024, 4, 1),
  horizonEndMs: Date.UTC(2024, 6, 1),
};

const HORIZON_2026: Pick<ParseOptions, 'horizonStartMs' | 'horizonEndMs'> = {
  horizonStartMs: Date.UTC(2026, 0, 1),
  horizonEndMs: Date.UTC(2027, 0, 1),
};

function parseOptions(overrides: Partial<ParseOptions> = {}): ParseOptions {
  return {
    sourceTimeZone: 'Europe/Moscow',
    horizonStartMs: HORIZON_2024.horizonStartMs,
    horizonEndMs: HORIZON_2024.horizonEndMs,
    maxIterations: 20_000,
    ...overrides,
  };
}

async function parseWith(ics: string, overrides: Partial<ParseOptions> = {}): Promise<ParsedCalendar> {
  return parser.parse(ics, parseOptions(overrides));
}

async function occurrencesFor(
  ics: string,
  overrides: Partial<ParseOptions> = {},
  vtimezone = VTIMEZONE_MOSCOW,
): Promise<Occurrence[]> {
  const options = parseOptions(overrides);
  const parsed = await parser.parse(calendar(ics, vtimezone), options);
  return buildOccurrences(parsed, {
    horizonStartMs: options.horizonStartMs,
    horizonEndMs: options.horizonEndMs,
  });
}

describe('ical.js parser adapter', () => {
  it('resolves a zoned event through VTIMEZONE and keeps description and url', async () => {
    const parsed = await parseWith(
      calendar(`BEGIN:VEVENT
UID:zoned-1
DTSTAMP:20240101T000000Z
DTSTART;TZID=Europe/Moscow:20240603T100000
DTEND;TZID=Europe/Moscow:20240603T113000
SUMMARY:Zoned lesson
DESCRIPTION:Room 5
URL:https://example.test/lesson/1
END:VEVENT`),
    );

    assert.equal(parsed.events.length, 1);
    const event = parsed.events[0];
    assert.ok(event);
    assert.equal(event.uid, 'zoned-1');
    assert.equal(event.summary, 'Zoned lesson');
    assert.equal(event.description, 'Room 5');
    assert.equal(event.url, 'https://example.test/lesson/1');
    assert.equal(event.startsAtMs, msk(2024, 6, 3, 10, 0));
    assert.equal(event.endsAtMs, msk(2024, 6, 3, 11, 30));
    assert.equal(event.timezone, 'Europe/Moscow');
    assert.equal(event.isAllDay, false);
    assert.equal(event.status, 'confirmed');
  });

  it('derives the end from DURATION', async () => {
    const parsed = await parseWith(
      calendar(`BEGIN:VEVENT
UID:dur-1
DTSTAMP:20240101T000000Z
DTSTART;TZID=Europe/Moscow:20240603T100000
DURATION:PT1H30M
SUMMARY:Duration lesson
END:VEVENT`),
    );

    const event = parsed.events[0];
    assert.ok(event);
    assert.equal(event.endsAtMs, msk(2024, 6, 3, 11, 30));
  });

  it('parses UTC (Z) timestamps without shifting them', async () => {
    const parsed = await parseWith(
      calendar(`BEGIN:VEVENT
UID:utc-1
DTSTAMP:20240101T000000Z
DTSTART:20240603T070000Z
DTEND:20240603T083000Z
SUMMARY:UTC lesson
END:VEVENT`),
    );
    const event = parsed.events[0];
    assert.ok(event);
    assert.equal(event.startsAtMs, Date.UTC(2024, 5, 3, 7, 0));
    assert.equal(event.endsAtMs, Date.UTC(2024, 5, 3, 8, 30));
    assert.equal(event.timezone, 'UTC');
  });

  it('rejects an unknown TZID instead of silently treating it as floating', async () => {
    await assert.rejects(
      parseWith(
        calendar(`BEGIN:VEVENT
UID:bad-tz
DTSTAMP:20240101T000000Z
DTSTART;TZID=Nowhere/Unknown:20240603T100000
SUMMARY:Unknown zone
END:VEVENT`),
      ),
      CalendarIntegrityError,
    );
  });

  it('interprets floating time in the explicit source timezone', async () => {
    const parsed = await parseWith(
      calendar(`BEGIN:VEVENT
UID:floating-1
DTSTAMP:20240101T000000Z
DTSTART:20240603T100000
SUMMARY:Floating lesson
END:VEVENT`),
    );
    const event = parsed.events[0];
    assert.ok(event);
    assert.equal(event.startsAtMs, msk(2024, 6, 3, 10, 0));
    assert.equal(event.timezone, 'Europe/Moscow');
  });

  it('rejects floating time when the source timezone has no known offset', async () => {
    await assert.rejects(
      parseWith(
        calendar(`BEGIN:VEVENT
UID:floating-2
DTSTAMP:20240101T000000Z
DTSTART:20240603T100000
SUMMARY:Floating lesson
END:VEVENT`),
        { sourceTimeZone: 'America/New_York' },
      ),
      CalendarIntegrityError,
    );
  });

  it('rejects malformed input', async () => {
    await assert.rejects(parser.parse('this is not an icalendar payload', parseOptions()));
  });

  it('flags all-day events and skips them by explicit expansion policy', async () => {
    const parsed = await parseWith(
      calendar(`BEGIN:VEVENT
UID:allday-1
DTSTAMP:20240101T000000Z
DTSTART;VALUE=DATE:20240604
DTEND;VALUE=DATE:20240605
SUMMARY:Holiday
END:VEVENT`),
    );
    assert.equal(parsed.events[0]?.isAllDay, true);
    assert.deepEqual(buildOccurrences(parsed, HORIZON_2024), []);
  });
});

describe('recurrence expansion through ical.js', () => {
  it('applies BYDAY as an RFC filter on DAILY frequency (regression)', async () => {
    const occurrences = await occurrencesFor(
      `BEGIN:VEVENT
UID:repro
DTSTAMP:20240101T000000Z
DTSTART:20260914T100000Z
RRULE:FREQ=DAILY;BYDAY=MO;COUNT=3
SUMMARY:Weekly-ish
END:VEVENT`,
      HORIZON_2026,
    );
    assert.deepEqual(
      occurrences.map((occurrence) => occurrence.startsAtMs),
      [Date.UTC(2026, 8, 14, 10), Date.UTC(2026, 8, 21, 10), Date.UTC(2026, 8, 28, 10)],
    );
  });

  it('honors WKST with multi-week intervals', async () => {
    const occurrences = await occurrencesFor(
      `BEGIN:VEVENT
UID:wkst
DTSTAMP:20240101T000000Z
DTSTART:20260907T100000Z
RRULE:FREQ=WEEKLY;INTERVAL=2;WKST=SU;BYDAY=MO,FR;COUNT=6
SUMMARY:Alternating weeks
END:VEVENT`,
      HORIZON_2026,
    );
    assert.deepEqual(
      occurrences.map((occurrence) => occurrence.startsAtMs),
      [
        Date.UTC(2026, 8, 7, 10),
        Date.UTC(2026, 8, 11, 10),
        Date.UTC(2026, 8, 21, 10),
        Date.UTC(2026, 8, 25, 10),
        Date.UTC(2026, 9, 5, 10),
        Date.UTC(2026, 9, 9, 10),
      ],
    );
  });

  it('combines monthly ordinal BYDAY filters and skips missing month days', async () => {
    const firstAndLast = await occurrencesFor(
      `BEGIN:VEVENT
UID:monthly
DTSTAMP:20240101T000000Z
DTSTART:20260105T100000Z
RRULE:FREQ=MONTHLY;BYDAY=1MO,-1FR;COUNT=6
SUMMARY:Monthly
END:VEVENT`,
      HORIZON_2026,
    );
    assert.deepEqual(
      firstAndLast.map((occurrence) => occurrence.startsAtMs),
      [
        Date.UTC(2026, 0, 5, 10),
        Date.UTC(2026, 0, 30, 10),
        Date.UTC(2026, 1, 2, 10),
        Date.UTC(2026, 1, 27, 10),
        Date.UTC(2026, 2, 2, 10),
        Date.UTC(2026, 2, 27, 10),
      ],
    );

    const day31 = await occurrencesFor(
      `BEGIN:VEVENT
UID:monthly31
DTSTAMP:20240101T000000Z
DTSTART:20260131T100000Z
RRULE:FREQ=MONTHLY;BYMONTHDAY=31;COUNT=4
SUMMARY:Month end
END:VEVENT`,
      HORIZON_2026,
    );
    assert.deepEqual(
      day31.map((occurrence) => occurrence.startsAtMs),
      [
        Date.UTC(2026, 0, 31, 10),
        Date.UTC(2026, 2, 31, 10),
        Date.UTC(2026, 4, 31, 10),
        Date.UTC(2026, 6, 31, 10),
      ],
    );
  });

  it('keeps the wall clock across a DST transition', async () => {
    const occurrences = await occurrencesFor(
      `BEGIN:VEVENT
UID:dst
DTSTAMP:20240101T000000Z
DTSTART;TZID=Europe/Berlin:20260316T100000
RRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=4
SUMMARY:Berlin weekly
END:VEVENT`,
      HORIZON_2026,
      VTIMEZONE_BERLIN,
    );
    assert.deepEqual(
      occurrences.map((occurrence) => occurrence.startsAtMs),
      [
        Date.UTC(2026, 2, 16, 9),
        Date.UTC(2026, 2, 23, 9),
        Date.UTC(2026, 2, 30, 8),
        Date.UTC(2026, 3, 6, 8),
      ],
    );
  });

  it('applies RDATE and EXDATE and keeps moved/cancelled overrides', async () => {
    const parsed = await parseWith(
      calendar(`BEGIN:VEVENT
UID:rec-1
DTSTAMP:20240101T000000Z
DTSTART;TZID=Europe/Moscow:20240603T100000
DTEND;TZID=Europe/Moscow:20240603T113000
RRULE:FREQ=WEEKLY;BYDAY=MO
EXDATE;TZID=Europe/Moscow:20240610T100000
RDATE;TZID=Europe/Moscow:20240615T100000
SUMMARY:Weekly lesson
END:VEVENT
BEGIN:VEVENT
UID:rec-1
RECURRENCE-ID;TZID=Europe/Moscow:20240617T100000
DTSTAMP:20240101T000000Z
DTSTART;TZID=Europe/Moscow:20240617T120000
DTEND;TZID=Europe/Moscow:20240617T133000
SUMMARY:Moved lesson
END:VEVENT
BEGIN:VEVENT
UID:rec-1
RECURRENCE-ID;TZID=Europe/Moscow:20240624T100000
DTSTAMP:20240101T000000Z
DTSTART;TZID=Europe/Moscow:20240624T100000
STATUS:CANCELLED
SUMMARY:Cancelled lesson
END:VEVENT`),
    );

    const occurrences = buildOccurrences(parsed, HORIZON_2024);
    const byKey = new Map(occurrences.map((occurrence) => [occurrence.occurrenceKey, occurrence]));

    const firstOrigin = msk(2024, 6, 3, 10, 0);
    assert.equal(byKey.get(`rec-1#${firstOrigin}`)?.startsAtMs, firstOrigin);

    const rdate = byKey.get(`rec-1#${msk(2024, 6, 15, 10, 0)}`);
    assert.ok(rdate, 'RDATE occurrence should be present');

    assert.equal(byKey.has(`rec-1#${msk(2024, 6, 10, 10, 0)}`), false, 'EXDATE should be removed');

    const movedOrigin = msk(2024, 6, 17, 10, 0);
    const moved = byKey.get(`rec-1#${movedOrigin}`);
    assert.ok(moved, 'moved instance keeps its original identity');
    assert.equal(moved.startsAtMs, msk(2024, 6, 17, 12, 0));
    assert.equal(moved.summary, 'Moved lesson');

    const cancelled = byKey.get(`rec-1#${msk(2024, 6, 24, 10, 0)}`);
    assert.ok(cancelled, 'cancelled instance is still tracked');
    assert.equal(cancelled.status, 'cancelled');
  });

  it('preserves the event duration for every generated occurrence', async () => {
    const occurrences = await occurrencesFor(
      `BEGIN:VEVENT
UID:duration
DTSTAMP:20240101T000000Z
DTSTART:20240603T100000Z
DTEND:20240603T113000Z
RRULE:FREQ=WEEKLY;BYDAY=MO;COUNT=3
SUMMARY:90 minute lesson
END:VEVENT`,
    );
    assert.equal(occurrences.length, 3);
    for (const occurrence of occurrences) {
      assert.equal(occurrence.endsAtMs, occurrence.startsAtMs + 90 * 60_000);
    }
    assert.notEqual(occurrences[0]?.endsAtMs, occurrences[2]?.endsAtMs);
  });

  it('bounds expansion by the horizon', async () => {
    const occurrences = await occurrencesFor(
      `BEGIN:VEVENT
UID:daily
DTSTAMP:20240101T000000Z
DTSTART:20240501T100000Z
RRULE:FREQ=DAILY
SUMMARY:Daily
END:VEVENT`,
      HORIZON_2024,
    );
    assert.ok(occurrences.length > 0);
    const last = occurrences[occurrences.length - 1];
    assert.ok(last !== undefined);
    assert.ok(last.startsAtMs <= HORIZON_2024.horizonEndMs);
    assert.ok(last.startsAtMs > HORIZON_2024.horizonEndMs - MS_PER_DAY);
  });

  it('rejects a recurrence that exceeds the iteration budget', async () => {
    await assert.rejects(
      occurrencesFor(
        `BEGIN:VEVENT
UID:too-frequent
DTSTAMP:20240101T000000Z
DTSTART:20240603T100000Z
RRULE:FREQ=SECONDLY
SUMMARY:Too frequent
END:VEVENT`,
        { maxIterations: 500 },
      ),
      RecurrenceLimitError,
    );
  });
});
