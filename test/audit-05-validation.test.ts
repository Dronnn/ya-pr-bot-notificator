/**
 * Finding 5 regressions: semantically invalid or ambiguous snapshots reject as
 * a whole, and the sync pipeline preserves the previously stored occurrences.
 *
 * Every rejection is asserted through the full parse -> buildOccurrences path:
 * a rejected fixture never yields a partial occurrence set. Rejection names are
 * stable (`error.name`) because sync logs exactly that via `errorName`.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { IcalJsCalendarParser } from '../src/calendar/icaljs-parser.ts';
import { runSourceSync } from '../src/calendar/sync.ts';
import type { ParseOptions } from '../src/calendar/parser.ts';
import {
  buildOccurrences,
  type Occurrence,
  type ParsedCalendar,
} from '../src/domain/calendar.ts';
import { MS_PER_HOUR } from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { textResponse } from './helpers/fakes.ts';
import { createHarness, syncDeps } from './helpers/harness.ts';
import { occurrence, seedSource, TEST_SOURCE } from './helpers/seed.ts';

const parser = new IcalJsCalendarParser();

const HORIZON = {
  horizonStartMs: Date.UTC(2026, 4, 1),
  horizonEndMs: Date.UTC(2026, 5, 1),
};

const PARSE_OPTIONS: ParseOptions = {
  sourceTimeZone: 'Europe/Moscow',
  ...HORIZON,
  maxIterations: 20_000,
};

function calendar(events: string, vtimezone = ''): string {
  return `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//test//EN
${vtimezone}
${events}
END:VCALENDAR`;
}

/** Full production path: parse first, normalize second; a throw skips building. */
async function parseAndBuild(ics: string): Promise<Occurrence[]> {
  const parsed = await parser.parse(ics, PARSE_OPTIONS);
  return buildOccurrences(parsed, {
    horizonStartMs: HORIZON.horizonStartMs,
    horizonEndMs: HORIZON.horizonEndMs,
  });
}

async function parse(ics: string): Promise<ParsedCalendar> {
  return parser.parse(ics, PARSE_OPTIONS);
}

const DTSTART_VALID = 'DTSTART:20260502T100000Z';

const DUPLICATE_MASTERS_CONFLICT = calendar(`BEGIN:VEVENT
UID:dup-conflict
DTSTAMP:20260101T000000Z
${DTSTART_VALID}
DTEND:20260502T110000Z
SUMMARY:First copy
END:VEVENT
BEGIN:VEVENT
UID:dup-conflict
DTSTAMP:20260101T000000Z
${DTSTART_VALID}
DTEND:20260502T113000Z
SUMMARY:Second copy
END:VEVENT`);

describe('finding 5: snapshot semantic validation', () => {
  it('rejects two non-identical masters with the same UID', async () => {
    await assert.rejects(parseAndBuild(DUPLICATE_MASTERS_CONFLICT), {
      name: 'AmbiguousCalendarError',
      message: /dup-conflict/,
    });
  });

  it('keeps exactly one copy of semantically identical duplicate masters', async () => {
    // The copies differ only in properties the domain never consumes
    // (DTSTAMP, LOCATION); the canonical comparison is over domain properties,
    // and the first copy in input order wins deterministically.
    const ics = calendar(`BEGIN:VEVENT
UID:dup-identical
DTSTAMP:20260101T000000Z
${DTSTART_VALID}
DTEND:20260502T110000Z
SUMMARY:Identical
END:VEVENT
BEGIN:VEVENT
UID:dup-identical
DTSTAMP:20260202T000000Z
${DTSTART_VALID}
DTEND:20260502T110000Z
LOCATION:Room 7
SUMMARY:Identical
END:VEVENT`);

    const first = await parse(ics);
    assert.equal(first.events.length, 1);

    const occurrences = await parseAndBuild(ics);
    assert.equal(occurrences.length, 1);
    assert.equal(occurrences[0]?.occurrenceKey, `dup-identical#${Date.UTC(2026, 4, 2, 10)}`);
    assert.equal(occurrences[0]?.endsAtMs, Date.UTC(2026, 4, 2, 11));

    // Same input, same result: selection is deterministic, not order-of-map luck.
    const again = await parseAndBuild(ics);
    assert.deepEqual(again, occurrences);
  });

  it('rejects a component whose DTEND is before its DTSTART', async () => {
    await assert.rejects(
      parseAndBuild(calendar(`BEGIN:VEVENT
UID:end-before-start
DTSTAMP:20260101T000000Z
DTSTART:20260502T100000Z
DTEND:20260502T090000Z
SUMMARY:Inverted
END:VEVENT`)),
      { name: 'CalendarIntegrityError', message: /ends before it starts/ },
    );
  });

  it('rejects a component that declares both DTEND and DURATION', async () => {
    await assert.rejects(
      parseAndBuild(calendar(`BEGIN:VEVENT
UID:end-and-duration
DTSTAMP:20260101T000000Z
${DTSTART_VALID}
DTEND:20260502T110000Z
DURATION:PT1H
SUMMARY:Ambiguous end
END:VEVENT`)),
      { name: 'CalendarIntegrityError', message: /both DTEND and DURATION/ },
    );
  });

  it('rejects February 30 instead of normalizing it to March', async () => {
    await assert.rejects(
      parseAndBuild(calendar(`BEGIN:VEVENT
UID:invalid-day
DTSTAMP:20260101T000000Z
DTSTART:20260230T100000Z
SUMMARY:Invalid day
END:VEVENT`)),
      { name: 'CalendarIntegrityError' },
    );
  });

  it('rejects month 13', async () => {
    await assert.rejects(
      parseAndBuild(calendar(`BEGIN:VEVENT
UID:invalid-month
DTSTAMP:20260101T000000Z
DTSTART:20261301T100000Z
SUMMARY:Invalid month
END:VEVENT`)),
      { name: 'CalendarIntegrityError' },
    );
  });

  it('rejects hour 25', async () => {
    await assert.rejects(
      parseAndBuild(calendar(`BEGIN:VEVENT
UID:invalid-hour
DTSTAMP:20260101T000000Z
DTSTART:20260502T250000Z
SUMMARY:Invalid hour
END:VEVENT`)),
      { name: 'CalendarIntegrityError' },
    );
  });

  it('rejects minute 60 and leap second 60 strictly', async () => {
    await assert.rejects(
      parseAndBuild(calendar(`BEGIN:VEVENT
UID:invalid-minute
DTSTAMP:20260101T000000Z
DTSTART:20260502T106000Z
SUMMARY:Invalid minute
END:VEVENT`)),
      { name: 'CalendarIntegrityError' },
    );
    await assert.rejects(
      parseAndBuild(calendar(`BEGIN:VEVENT
UID:invalid-second
DTSTAMP:20260101T000000Z
DTSTART:20260502T100060Z
SUMMARY:Leap second
END:VEVENT`)),
      { name: 'CalendarIntegrityError' },
    );
  });

  it('still accepts February 29 in a leap year', async () => {
    const leapYearOptions: ParseOptions = {
      sourceTimeZone: 'Europe/Moscow',
      horizonStartMs: Date.UTC(2028, 1, 1),
      horizonEndMs: Date.UTC(2028, 2, 1),
      maxIterations: 20_000,
    };
    const parsed = await parser.parse(
      calendar(`BEGIN:VEVENT
UID:leap-day
DTSTAMP:20260101T000000Z
DTSTART:20280229T100000Z
SUMMARY:Leap day
END:VEVENT`),
      leapYearOptions,
    );
    assert.equal(parsed.events[0]?.startsAtMs, Date.UTC(2028, 1, 29, 10));
  });

  it('rejects a negative or unparseable DURATION instead of a generic error', async () => {
    await assert.rejects(
      parseAndBuild(calendar(`BEGIN:VEVENT
UID:negative-duration
DTSTAMP:20260101T000000Z
${DTSTART_VALID}
DURATION:-PT1H
SUMMARY:Negative
END:VEVENT`)),
      { name: 'CalendarIntegrityError', message: /negative DURATION/ },
    );
    await assert.rejects(
      parseAndBuild(calendar(`BEGIN:VEVENT
UID:garbage-duration
DTSTAMP:20260101T000000Z
${DTSTART_VALID}
DURATION:garbage
SUMMARY:Garbage
END:VEVENT`)),
      { name: 'CalendarIntegrityError', message: /invalid DURATION/ },
    );
  });

  it('rejects malformed DURATION grammar that ical.js silently repairs', async () => {
    // ical.js accepts each of these and silently rewrites them (`P1H` to one
    // hour, `PT1D` to one day, `+-PT1H` to +PT1H, fractional seconds
    // truncated), which is exactly the normalization the raw layer must not
    // trust. The RFC 5545 grammar allows only one optional sign.
    for (const duration of ['+-PT1H', '--PT1H', 'P1H', 'PT1D', 'P1DT', 'PT1.5H', 'P', 'PT']) {
      await assert.rejects(
        parseAndBuild(calendar(`BEGIN:VEVENT
UID:malformed-duration
DTSTAMP:20260101T000000Z
${DTSTART_VALID}
DURATION:${duration}
SUMMARY:Malformed
END:VEVENT`)),
        { name: 'CalendarIntegrityError', message: /invalid DURATION/ },
        `DURATION:${duration} must be rejected`,
      );
    }
  });

  it('keeps every valid DURATION form ical.js supports', async () => {
    const expectedEnd = Date.UTC(2026, 4, 9, 10);
    // `P0D` is intentionally absent: a zero VEVENT duration is rejected as a
    // non-positive duration (finding 6c, test/audit-18-rfc-validation.test.ts).
    const valid = [
      { duration: 'P1W', endsAtMs: expectedEnd },
      { duration: '+P1D', endsAtMs: Date.UTC(2026, 4, 3, 10) },
      { duration: 'P1DT2H3M4S', endsAtMs: Date.UTC(2026, 4, 3, 12, 3, 4) },
    ];
    for (const { duration, endsAtMs } of valid) {
      const parsed = await parse(calendar(`BEGIN:VEVENT
UID:valid-duration
DTSTAMP:20260101T000000Z
${DTSTART_VALID}
DURATION:${duration}
SUMMARY:Valid
END:VEVENT`));
      assert.equal(parsed.events[0]?.endsAtMs, endsAtMs, `DURATION:${duration}`);
    }
  });

  it('rejects a referenced declared-but-empty VTIMEZONE', async () => {
    const vtimezone = `BEGIN:VTIMEZONE
TZID:Empty/Zone
END:VTIMEZONE`;
    await assert.rejects(
      parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:empty-zone
DTSTAMP:20260101T000000Z
DTSTART;TZID=Empty/Zone:20260502T100000
SUMMARY:Empty zone
END:VEVENT`,
          vtimezone,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /without a usable observance/ },
    );
  });

  it('rejects a referenced VTIMEZONE whose only observance is future-only', async () => {
    const vtimezone = `BEGIN:VTIMEZONE
TZID:Future/Zone
BEGIN:STANDARD
DTSTART:20280101T000000
TZOFFSETFROM:+0000
TZOFFSETTO:+0500
TZNAME:FUT
END:STANDARD
END:VTIMEZONE`;
    await assert.rejects(
      parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:future-zone
DTSTAMP:20260101T000000Z
DTSTART;TZID=Future/Zone:20260502T100000
SUMMARY:Future zone
END:VEVENT`,
          vtimezone,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /without a usable observance/ },
    );
  });

  it('rejects a referenced VTIMEZONE whose first observance starts after the wall clock', async () => {
    // ical.js resolves a time before the first observance at offset zero, so
    // accepting this zone would publish 10:00 local as 10:00Z.
    const vtimezone = `BEGIN:VTIMEZONE
TZID:Late/Onset
BEGIN:STANDARD
DTSTART:20260515T020000
TZOFFSETFROM:+0300
TZOFFSETTO:+0300
TZNAME:LATE
END:STANDARD
END:VTIMEZONE`;
    await assert.rejects(
      parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:late-onset
DTSTAMP:20260101T000000Z
DTSTART;TZID=Late/Onset:20260502T100000
SUMMARY:Late onset
END:VEVENT`,
          vtimezone,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /without a usable observance/ },
    );
  });

  it('keeps accepting a zone with an earlier applicable observance when a later onset is inside the horizon', async () => {
    // The DAYLIGHT onset on 2026-05-15 is inside the horizon, but the 2025
    // STANDARD observance already applies on 2026-05-02 (+0100), so the event
    // must resolve as 09:00Z rather than be rejected.
    const vtimezone = `BEGIN:VTIMEZONE
TZID:Span/Zone
BEGIN:STANDARD
DTSTART:20251026T030000
TZOFFSETFROM:+0200
TZOFFSETTO:+0100
TZNAME:CET
END:STANDARD
BEGIN:DAYLIGHT
DTSTART:20260515T020000
TZOFFSETFROM:+0100
TZOFFSETTO:+0200
TZNAME:CEST
END:DAYLIGHT
END:VTIMEZONE`;
    const occurrences = await parseAndBuild(
      calendar(
        `BEGIN:VEVENT
UID:span-zone
DTSTAMP:20260101T000000Z
DTSTART;TZID=Span/Zone:20260502T100000
SUMMARY:Span zone
END:VEVENT`,
        vtimezone,
      ),
    );
    assert.equal(occurrences.length, 1);
    assert.equal(occurrences[0]?.startsAtMs, Date.UTC(2026, 4, 2, 9));
  });

  it('keeps accepting a valid custom VTIMEZONE', async () => {
    const vtimezone = `BEGIN:VTIMEZONE
TZID:Fixed/Zone
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETTO:+0300
TZNAME:FIX
END:STANDARD
END:VTIMEZONE`;
    const occurrences = await parseAndBuild(
      calendar(
        `BEGIN:VEVENT
UID:fixed-zone
DTSTAMP:20260101T000000Z
DTSTART;TZID=Fixed/Zone:20260502T100000
DTEND;TZID=Fixed/Zone:20260502T110000
SUMMARY:Fixed zone
END:VEVENT`,
        vtimezone,
      ),
    );
    assert.equal(occurrences.length, 1);
    assert.equal(occurrences[0]?.startsAtMs, Date.UTC(2026, 4, 2, 7));
  });
});

describe('finding 5: raw-text date/time validation around ical.js', () => {
  // ical.js repairs every form below before jCal exists, so the raw line is the
  // only layer that can reject it; the fixtures use a valid master plus the
  // form under test so exactly one line can fail.
  const VTIMEZONE_MOSCOW = `BEGIN:VTIMEZONE
TZID:Europe/Moscow
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETTO:+0300
TZNAME:MSK
END:STANDARD
END:VTIMEZONE`;

  function rawTextEvent(line: string): string {
    return calendar(
      `BEGIN:VEVENT
UID:raw-text
DTSTAMP:20260101T000000Z
DTSTART:20260502T100000Z
${line}
SUMMARY:Raw text
END:VEVENT`,
      VTIMEZONE_MOSCOW,
    );
  }

  const REJECTED_FORMS: Array<{ name: string; line: string; message: RegExp }> = [
    { name: 'lowercase z date-time', line: 'DTSTART:20260502T100000z', message: /invalid date-time/ },
    {
      name: 'fractional seconds',
      line: 'DTSTART:20260502T100000.5Z',
      message: /invalid date-time/,
    },
    {
      name: 'numeric UTC offset (+0530)',
      line: 'DTSTART:20260502T100000+0530',
      message: /invalid date-time/,
    },
    {
      name: 'numeric UTC offset (+05:30)',
      line: 'DTSTART:20260502T100000+05:30',
      message: /invalid date-time/,
    },
    {
      name: 'VALUE=DATE with a time part',
      line: 'DTSTART;VALUE=DATE:20260502T100000',
      message: /invalid date/,
    },
    {
      name: 'VALUE=DATE-TIME with a date only',
      line: 'DTSTART;VALUE=DATE-TIME:20260502',
      message: /invalid date-time/,
    },
    { name: 'default DATE-TIME with a date only', line: 'DTSTART:20260502', message: /invalid date-time/ },
    { name: 'date-time without seconds', line: 'DTSTART:20260502T1000', message: /invalid date-time/ },
    {
      name: 'extended (punctuated) date-time',
      line: 'DTSTART:2026-05-02T10:00:00Z',
      message: /invalid date-time/,
    },
    { name: 'space before the UTC marker', line: 'DTSTART:20260502T100000 Z', message: /invalid date-time/ },
    {
      name: 'comma list on a single-value property',
      line: 'DTSTART:20260502T100000Z,20260503T100000Z',
      message: /invalid date-time/,
    },
    { name: 'seven-digit date', line: 'DTSTART:2026052T100000Z', message: /invalid date-time/ },
    {
      name: 'unknown VALUE type',
      line: 'DTSTART;VALUE=TEXT:20260502T100000Z',
      message: /unsupported VALUE type/,
    },
    { name: 'DTEND with lowercase z', line: 'DTEND:20260502T110000z', message: /invalid date-time/ },
    {
      name: 'RECURRENCE-ID with fractional seconds',
      line: 'RECURRENCE-ID:20260502T100000.5Z',
      message: /invalid date-time/,
    },
    { name: 'DUE with a numeric offset', line: 'DUE:20260505T100000+0530', message: /invalid date-time/ },
    {
      name: 'EXDATE with a malformed list token',
      line: 'EXDATE:20260502T100000Z,2026-05-03T10:00:00Z',
      message: /invalid date-time/,
    },
    { name: 'RDATE with a numeric offset', line: 'RDATE:20260503T100000+05:30', message: /invalid date-time/ },
    {
      name: 'RRULE UNTIL with lowercase z',
      line: 'RRULE:FREQ=DAILY;UNTIL=20260502T100000z',
      message: /UNTIL/,
    },
    {
      name: 'RRULE UNTIL with fractional seconds',
      line: 'RRULE:FREQ=DAILY;UNTIL=20260502T100000.5Z',
      message: /UNTIL/,
    },
    {
      name: 'RRULE UNTIL with a numeric offset',
      line: 'RRULE:FREQ=DAILY;UNTIL=20260502T100000+0530',
      message: /UNTIL/,
    },
    {
      name: 'RRULE UNTIL without seconds',
      line: 'RRULE:FREQ=DAILY;UNTIL=20260502T1000',
      message: /UNTIL/,
    },
    { name: 'malformed DURATION grammar', line: 'DURATION:+-PT1H', message: /invalid DURATION/ },
  ];

  it('rejects every RFC-forbidden or ical.js-repaired raw form', async () => {
    for (const testCase of REJECTED_FORMS) {
      await assert.rejects(
        parse(rawTextEvent(testCase.line)),
        { name: 'CalendarIntegrityError', message: testCase.message },
        testCase.name,
      );
    }
  });

  const ACCEPTED_DTSTART_FORMS: readonly string[] = [
    'DTSTART:20260502T100000Z',
    'DTSTART;TZID=Europe/Moscow:20260502T100000',
    'DTSTART;TZID="Europe/Moscow":20260502T100000',
    'DTSTART;VALUE=DATE-TIME:20260502T100000Z',
    'DTSTART;VALUE=DATE:20260502',
    'dtstart:20260502T100000Z',
    'DTSTART;value=date:20260502',
  ];

  it('accepts every canonical raw date/time/duration form', async () => {
    for (const line of ACCEPTED_DTSTART_FORMS) {
      const parsed = await parse(
        calendar(
          `BEGIN:VEVENT
UID:canonical
DTSTAMP:20260101T000000Z
${line}
SUMMARY:Canonical
END:VEVENT`,
          VTIMEZONE_MOSCOW,
        ),
      );
      assert.equal(parsed.events.length, 1, line);
    }

    const ACCEPTED_EXTRA_FORMS: readonly string[] = [
      'DTEND;VALUE=DATE:20260503',
      'EXDATE;TZID=Europe/Moscow:20260502T100000,20260503T100000',
      'RDATE;VALUE=DATE:20260502,20260503',
      'RRULE:FREQ=DAILY;UNTIL=20260502T100000Z',
      'RRULE:FREQ=DAILY;UNTIL=20260602',
      'DURATION:P1DT2H',
      'DUE:20260505T100000Z',
      'RECURRENCE-ID;TZID=Europe/Moscow:20260502T100000',
    ];
    for (const line of ACCEPTED_EXTRA_FORMS) {
      const parsed = await parse(rawTextEvent(line));
      assert.equal(parsed.events.length, 1, line);
    }
  });

  it('reads a value split across an RFC 5545 fold boundary', async () => {
    const parsed = await parse(
      calendar(
        `BEGIN:VEVENT
UID:folded
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Mos
 cow:20260502T100000
SUMMARY:Folded
END:VEVENT`,
        VTIMEZONE_MOSCOW,
      ),
    );
    assert.equal(parsed.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 7));
  });

  it('still parses a feed whose VTODO DUE is canonical but rejects a malformed one', async () => {
    const canonical = `BEGIN:VEVENT
UID:keep
DTSTAMP:20260101T000000Z
DTSTART:20260502T100000Z
SUMMARY:Keep
END:VEVENT
BEGIN:VTODO
UID:todo-1
DTSTAMP:20260101T000000Z
DUE:20260505T100000Z
END:VTODO`;
    const parsed = await parse(calendar(canonical));
    assert.equal(parsed.events.length, 1);

    await assert.rejects(
      parse(calendar(canonical.replace('DUE:20260505T100000Z', 'DUE:20260505T100000+0530'))),
      { name: 'CalendarIntegrityError', message: /invalid date-time/ },
    );
  });
});

describe('finding 5: sync preserves the previous snapshot on rejection', () => {
  it('a rejected snapshot leaves the stored occurrences untouched', async () => {
    const now = Date.UTC(2026, 4, 1, 12, 0, 0);
    const harness = createHarness({ now });
    await seedSource(harness.repository, 'basic', now);
    // A previously stored snapshot, seeded through the real repository API.
    const startsAtMs = now + MS_PER_HOUR;
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: `valid-1#${startsAtMs}`, uid: 'valid-1', startsAtMs })],
      now,
    );

    harness.setHandler(() => textResponse(DUPLICATE_MASTERS_CONFLICT));
    const rejected = await runSourceSync(
      syncDeps(harness, { parser: new IcalJsCalendarParser() }),
      TEST_SOURCE,
      'sync-owner-1',
    );

    assert.equal(rejected.status, 'rejected');
    assert.equal(rejected.reason, 'AmbiguousCalendarError');
    assert.equal(rejected.upserted, 0);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrences WHERE uid = 'valid-1'"),
      1,
    );
  });

  it('every rejection class preserves the prior snapshot and logs only the error name', async () => {
    const leakedUid = 'payload-uid-5';
    const fixtures: Array<{ name: string; reason: string; ics: string }> = [
      { name: 'duplicate conflicting masters', reason: 'AmbiguousCalendarError',
        ics: DUPLICATE_MASTERS_CONFLICT },
      { name: 'DTEND before DTSTART', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART:20260502T100000Z
DTEND:20260502T090000Z
END:VEVENT`) },
      { name: 'DTEND with DURATION', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART:20260502T100000Z
DTEND:20260502T110000Z
DURATION:PT1H
END:VEVENT`) },
      { name: 'February 30', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART:20260230T100000Z
END:VEVENT`) },
      { name: 'month 13', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART:20261302T100000Z
END:VEVENT`) },
      { name: 'minute 60', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART:20260502T106000Z
END:VEVENT`) },
      { name: 'malformed DURATION grammar', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART:20260502T100000Z
DURATION:+-PT1H
END:VEVENT`) },
      { name: 'repaired lowercase z date-time', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART:20260502T100000z
END:VEVENT`) },
      { name: 'repaired numeric UTC offset', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART:20260502T100000+0530
END:VEVENT`) },
      { name: 'empty VTIMEZONE', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART;TZID=Probe/Empty:20260502T100000
END:VEVENT`, 'BEGIN:VTIMEZONE\nTZID:Probe/Empty\nEND:VTIMEZONE') },
      { name: 'future-only VTIMEZONE', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART;TZID=Probe/Future:20260502T100000
END:VEVENT`, `BEGIN:VTIMEZONE
TZID:Probe/Future
BEGIN:STANDARD
DTSTART:20280101T000000
TZOFFSETFROM:+0000
TZOFFSETTO:+0500
END:STANDARD
END:VTIMEZONE`) },
      { name: 'timezone onset after the wall clock', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART;TZID=Probe/Late:20260502T100000
END:VEVENT`, `BEGIN:VTIMEZONE
TZID:Probe/Late
BEGIN:STANDARD
DTSTART:20260515T020000
TZOFFSETFROM:+0300
TZOFFSETTO:+0300
END:STANDARD
END:VTIMEZONE`) },
      { name: 'unknown TZID', reason: 'CalendarIntegrityError',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART;TZID=Probe/Unknown:20260502T100000
END:VEVENT`) },
    ];

    for (const fixture of fixtures) {
      const now = Date.UTC(2026, 4, 1, 12, 0, 0);
      const harness = createHarness({ now });
      await seedSource(harness.repository, 'basic', now);
      const startsAtMs = now + MS_PER_HOUR;
      await harness.repository.upsertOccurrences(
        [occurrence({ occurrenceKey: `keep-1#${startsAtMs}`, uid: 'keep-1', startsAtMs })],
        now,
      );
      harness.setHandler(() => textResponse(fixture.ics));

      const rejected = await runSourceSync(
        syncDeps(harness, { parser: new IcalJsCalendarParser() }),
        TEST_SOURCE,
        'sync-owner-1',
      );

      assert.equal(rejected.status, 'rejected', fixture.name);
      assert.equal(rejected.reason, fixture.reason, fixture.name);
      assert.equal(rejected.upserted, 0, fixture.name);
      assert.equal(rejected.deleted, 0, fixture.name);
      assert.equal(
        countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'),
        1,
        fixture.name,
      );
      assert.ok(
        !harness.logger.lines.some((line) => line.includes(leakedUid)),
        `${fixture.name}: the rejected payload must not reach the logs`,
      );
    }
  });
});
