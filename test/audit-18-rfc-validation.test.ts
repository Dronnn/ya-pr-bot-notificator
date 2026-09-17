/**
 * Finding 6 (08): RFC 5545 semantic validation around ical.js.
 *
 * Guards:
 *  (a) referenced STANDARD/DAYLIGHT observance requires DTSTART + TZOFFSETFROM
 *      + TZOFFSETTO (present + range-checked).
 *  (b) UTC offsets: full grammar + semantic ranges (reject +9999, bad
 *      minute/second, negative zero; keep valid incl. non-hour).
 *  (c) VEVENT DURATION must be positive (reject P0D and every zero form).
 *  (d) reject TZID on VALUE=DATE, TZID + trailing-Z, floating-vs-UTC mixing.
 *
 * Assumptions verified 2026-09-15:
 *  - raw-text + jCal layers exist: TRUE.
 *  - referenced-VTIMEZONE applicability check exists: TRUE.
 *  - UTC-offset shape check exists but range/FROM missing: PARTIAL.
 *  - DURATION grammar check exists: TRUE (zero still accepted).
 *  - Berlin/Lord Howe/Santiago/Dublin fixtures pass: FALSE - only Berlin
 *    fixtures exist in the repo; Lord Howe/Santiago/Dublin valid fixtures are
 *    added here to lock non-hour/negative/negative-DST behavior.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { IcalJsCalendarParser } from '../src/calendar/icaljs-parser.ts';
import { runSourceSync } from '../src/calendar/sync.ts';
import type { ParseOptions } from '../src/calendar/parser.ts';
import { buildOccurrences } from '../src/domain/calendar.ts';
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

async function parseAndBuild(ics: string): Promise<ReturnType<typeof buildOccurrences>> {
  const parsed = await parser.parse(ics, PARSE_OPTIONS);
  return buildOccurrences(parsed, { ...HORIZON });
}

async function parse(ics: string) {
  return parser.parse(ics, PARSE_OPTIONS);
}

const DTSTART_VALID = 'DTSTART:20260502T100000Z';

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

describe('audit-18 (a): observance requires TZOFFSETFROM', () => {
  it('rejects a referenced observance missing TZOFFSETFROM', async () => {
    const vtimezone = `BEGIN:VTIMEZONE
TZID:Missing/From
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETTO:+0300
END:STANDARD
END:VTIMEZONE`;
    await assert.rejects(
      parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:missing-from
DTSTAMP:20260101T000000Z
DTSTART;TZID=Missing/From:20260502T100000
SUMMARY:X
END:VEVENT`,
          vtimezone,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /TZOFFSETFROM/ },
    );
  });

  it('rejects a referenced observance with range-invalid TZOFFSETFROM', async () => {
    const vtimezone = `BEGIN:VTIMEZONE
TZID:Bad/From
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+9999
TZOFFSETTO:+0300
END:STANDARD
END:VTIMEZONE`;
    await assert.rejects(
      parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:bad-from
DTSTAMP:20260101T000000Z
DTSTART;TZID=Bad/From:20260502T100000
SUMMARY:X
END:VEVENT`,
          vtimezone,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /TZOFFSETFROM/ },
    );
  });
});

describe('audit-18 (b): UTC-offset grammar and ranges', () => {
  it('rejects +9999-style and out-of-range offsets', async () => {
    for (const offset of ['+9999', '+2400', '+2360', '+0399']) {
      const vtimezone = `BEGIN:VTIMEZONE
TZID:Range/Zone
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETTO:${offset}
END:STANDARD
END:VTIMEZONE`;
      await assert.rejects(
        parseAndBuild(
          calendar(
            `BEGIN:VEVENT
UID:range-bad
DTSTAMP:20260101T000000Z
DTSTART;TZID=Range/Zone:20260502T100000
SUMMARY:X
END:VEVENT`,
            vtimezone,
          ),
        ),
        { name: 'CalendarIntegrityError', message: /TZOFFSETTO/ },
        `TZOFFSETTO:${offset} must be rejected`,
      );
    }
  });

  it('rejects invalid minute/second fields', async () => {
    for (const offset of ['+0361', '+030061', '+03', '+030', '+03000']) {
      const vtimezone = `BEGIN:VTIMEZONE
TZID:MinSec/Zone
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETTO:${offset}
END:STANDARD
END:VTIMEZONE`;
      await assert.rejects(
        parseAndBuild(
          calendar(
            `BEGIN:VEVENT
UID:minsec-bad
DTSTAMP:20260101T000000Z
DTSTART;TZID=MinSec/Zone:20260502T100000
SUMMARY:X
END:VEVENT`,
            vtimezone,
          ),
        ),
        { name: 'CalendarIntegrityError' },
        `TZOFFSETTO:${offset} must be rejected`,
      );
    }
  });

  it('rejects negative zero offsets', async () => {
    for (const offset of ['-0000', '-000000']) {
      const vtimezone = `BEGIN:VTIMEZONE
TZID:NegZero/Zone
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:${offset}
TZOFFSETTO:+0000
END:STANDARD
END:VTIMEZONE`;
      await assert.rejects(
        parseAndBuild(
          calendar(
            `BEGIN:VEVENT
UID:negzero
DTSTAMP:20260101T000000Z
DTSTART;TZID=NegZero/Zone:20260502T100000
SUMMARY:X
END:VEVENT`,
            vtimezone,
          ),
        ),
        { name: 'CalendarIntegrityError' },
        `TZOFFSETFROM:${offset} must be rejected`,
      );
    }
  });

  it('keeps valid positive/negative and non-hour offsets', async () => {
    // Single-observance zones keep the offset selection deterministic
    // (ical.js picks the latest DTSTART when several observances share 1970
    // onsets). Each case locks that a valid offset value is accepted,
    // including non-hour, negative and negative-DST deltas.
    const single = (tzid: string, from: string, to: string) => `BEGIN:VTIMEZONE
TZID:${tzid}
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:${from}
TZOFFSETTO:${to}
END:STANDARD
END:VTIMEZONE`;
    const cases: Array<{ vtimezone: string; wall: string; expectedMs: number }> = [
      {
        vtimezone: VTIMEZONE_BERLIN,
        wall: 'DTSTART;TZID=Europe/Berlin:20260502T100000',
        expectedMs: Date.UTC(2026, 4, 2, 8),
      },
      {
        vtimezone: single('Australia/Lord_Howe', '+1100', '+1030'),
        wall: 'DTSTART;TZID=Australia/Lord_Howe:20260502T100000',
        expectedMs: Date.UTC(2026, 4, 1, 23, 30),
      },
      {
        vtimezone: single('America/Santiago', '-0300', '-0400'),
        wall: 'DTSTART;TZID=America/Santiago:20260502T100000',
        expectedMs: Date.UTC(2026, 4, 2, 14),
      },
      {
        vtimezone: single('Europe/Dublin', '+0100', '+0000'),
        wall: 'DTSTART;TZID=Europe/Dublin:20260502T100000',
        expectedMs: Date.UTC(2026, 4, 2, 10),
      },
    ];
    for (const { wall, vtimezone, expectedMs } of cases) {
      const occurrences = await parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:valid-offset
DTSTAMP:20260101T000000Z
${wall}
SUMMARY:X
END:VEVENT`,
          vtimezone,
        ),
      );
      assert.equal(occurrences.length, 1, wall);
      assert.equal(occurrences[0]?.startsAtMs, expectedMs, wall);
    }
  });

  it('keeps boundary-valid offsets incl. seconds and folded lines', async () => {
    for (const offset of ['+0000', '+1400', '-1200', '+0545', '+1030', '+2359']) {
      const vtimezone = `BEGIN:VTIMEZONE
TZID:Bound/Zone
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:${offset}
TZOFFSETTO:${offset}
END:STANDARD
END:VTIMEZONE`;
      const occurrences = await parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:bound-ok
DTSTAMP:20260101T000000Z
DTSTART;TZID=Bound/Zone:20260502T100000
SUMMARY:X
END:VEVENT`,
          vtimezone,
        ),
      );
      assert.equal(occurrences.length, 1, offset);
    }
    const folded = await parse(
      calendar(
        `BEGIN:VEVENT
UID:folded
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Mos
 cow:20260502T100000
SUMMARY:X
END:VEVENT`,
        VTIMEZONE_MOSCOW,
      ),
    );
    assert.equal(folded.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 7));
  });
});

describe('audit-18 (c): VEVENT DURATION must be positive', () => {
  it('rejects P0D and every zero duration form', async () => {
    for (const duration of ['P0D', 'P0W', 'PT0H', 'PT0M', 'PT0S', 'P0DT0H0M0S', '+P0D']) {
      await assert.rejects(
        parseAndBuild(calendar(`BEGIN:VEVENT
UID:zero-dur
DTSTAMP:20260101T000000Z
${DTSTART_VALID}
DURATION:${duration}
SUMMARY:X
END:VEVENT`)),
        { name: 'CalendarIntegrityError', message: /DURATION/ },
        `DURATION:${duration} must be rejected`,
      );
    }
  });

  it('keeps every valid duration form', async () => {
    const valid: Array<{ duration: string; endsAtMs: number }> = [
      { duration: 'P1W', endsAtMs: Date.UTC(2026, 4, 9, 10) },
      { duration: '+P1D', endsAtMs: Date.UTC(2026, 4, 3, 10) },
      { duration: 'P1D', endsAtMs: Date.UTC(2026, 4, 3, 10) },
      { duration: 'PT1H30M', endsAtMs: Date.UTC(2026, 4, 2, 11, 30) },
      { duration: 'P1DT2H3M4S', endsAtMs: Date.UTC(2026, 4, 3, 12, 3, 4) },
    ];
    for (const { duration, endsAtMs } of valid) {
      const parsed = await parse(calendar(`BEGIN:VEVENT
UID:valid-dur
DTSTAMP:20260101T000000Z
${DTSTART_VALID}
DURATION:${duration}
SUMMARY:X
END:VEVENT`));
      assert.equal(parsed.events[0]?.endsAtMs, endsAtMs, duration);
    }
  });
});

describe('audit-18 (d): TZID / UTC / floating combinations', () => {
  it('rejects TZID on VALUE=DATE', async () => {
    await assert.rejects(
      parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:tzid-date
DTSTAMP:20260101T000000Z
DTSTART;VALUE=DATE;TZID=Europe/Moscow:20260502
SUMMARY:X
END:VEVENT`,
          VTIMEZONE_MOSCOW,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /TZID/ },
    );
  });

  it('rejects TZID combined with trailing-Z UTC values', async () => {
    await assert.rejects(
      parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:tzid-z
DTSTAMP:20260101T000000Z
DTSTART;TZID=Europe/Moscow:20260502T100000Z
SUMMARY:X
END:VEVENT`,
          VTIMEZONE_MOSCOW,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /TZID/ },
    );
    await assert.rejects(
      parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:rdate-tzid-z
DTSTAMP:20260101T000000Z
DTSTART:20260502T100000Z
RDATE;TZID=Europe/Moscow:20260503T100000Z
SUMMARY:X
END:VEVENT`,
          VTIMEZONE_MOSCOW,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /TZID/ },
    );
  });

  it('rejects floating-vs-UTC inconsistencies in one VEVENT', async () => {
    await assert.rejects(
      parseAndBuild(
        calendar(`BEGIN:VEVENT
UID:float-utc
DTSTAMP:20260101T000000Z
DTSTART:20260502T100000
DTEND:20260502T110000Z
SUMMARY:X
END:VEVENT`),
      ),
      { name: 'CalendarIntegrityError', message: /UTC|floating|inconsistent/i },
    );
    await assert.rejects(
      parseAndBuild(
        calendar(`BEGIN:VEVENT
UID:utc-float
DTSTAMP:20260101T000000Z
DTSTART:20260502T100000Z
DTEND:20260502T110000
SUMMARY:X
END:VEVENT`),
      ),
      { name: 'CalendarIntegrityError' },
    );
  });

  it('keeps per-feed timezone isolation', async () => {
    function feed(offset: string): string {
      return calendar(
        `BEGIN:VEVENT
UID:iso-1
DTSTAMP:20240101T000000Z
DTSTART;TZID=Iso/Zone:20260502T100000
SUMMARY:X
END:VEVENT`,
        `BEGIN:VTIMEZONE
TZID:Iso/Zone
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:${offset}
TZOFFSETTO:${offset}
END:STANDARD
END:VTIMEZONE`,
      );
    }
    const plus3 = await parse(feed('+0300'));
    const plus5 = await parse(feed('+0500'));
    assert.equal(plus3.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 7));
    assert.equal(plus5.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 5));
  });
});

describe('audit-18: sync preserves the prior snapshot on each rejection', () => {
  const leakedUid = 'audit18-payload-uid';
  function invalidSnapshots(): Array<{ name: string; ics: string }> {
    return [
      {
        name: 'missing TZOFFSETFROM',
        ics: calendar(
          `BEGIN:VEVENT
UID:${leakedUid}
DTSTART;TZID=Probe/Missing:20260502T100000
END:VEVENT`,
          'BEGIN:VTIMEZONE\nTZID:Probe/Missing\nBEGIN:STANDARD\nDTSTART:19700101T000000\nTZOFFSETTO:+0300\nEND:STANDARD\nEND:VTIMEZONE',
        ),
      },
      {
        name: '+9999 offset',
        ics: calendar(
          `BEGIN:VEVENT
UID:${leakedUid}
DTSTART;TZID=Probe/Range:20260502T100000
END:VEVENT`,
          'BEGIN:VTIMEZONE\nTZID:Probe/Range\nBEGIN:STANDARD\nDTSTART:19700101T000000\nTZOFFSETFROM:+0300\nTZOFFSETTO:+9999\nEND:STANDARD\nEND:VTIMEZONE',
        ),
      },
      {
        name: 'negative zero offset',
        ics: calendar(
          `BEGIN:VEVENT
UID:${leakedUid}
DTSTART;TZID=Probe/NegZero:20260502T100000
END:VEVENT`,
          'BEGIN:VTIMEZONE\nTZID:Probe/NegZero\nBEGIN:STANDARD\nDTSTART:19700101T000000\nTZOFFSETFROM:-0000\nTZOFFSETTO:+0000\nEND:STANDARD\nEND:VTIMEZONE',
        ),
      },
      {
        name: 'zero duration P0D',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART:20260502T100000Z
DURATION:P0D
END:VEVENT`),
      },
      {
        name: 'TZID on DATE',
        ics: calendar(
          `BEGIN:VEVENT
UID:${leakedUid}
DTSTART;VALUE=DATE;TZID=Europe/Moscow:20260502
END:VEVENT`,
          VTIMEZONE_MOSCOW,
        ),
      },
      {
        name: 'TZID with trailing Z',
        ics: calendar(
          `BEGIN:VEVENT
UID:${leakedUid}
DTSTART;TZID=Europe/Moscow:20260502T100000Z
END:VEVENT`,
          VTIMEZONE_MOSCOW,
        ),
      },
      {
        name: 'floating vs UTC',
        ics: calendar(`BEGIN:VEVENT
UID:${leakedUid}
DTSTART:20260502T100000
DTEND:20260502T110000Z
END:VEVENT`),
      },
    ];
  }

  it('each invalid snapshot is rejected and leaves stored occurrences untouched', async () => {
    for (const fixture of invalidSnapshots()) {
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
      assert.equal(rejected.reason, 'CalendarIntegrityError', fixture.name);
      assert.equal(rejected.upserted, 0, fixture.name);
      assert.equal(rejected.deleted, 0, fixture.name);
      assert.equal(
        countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'),
        1,
        fixture.name,
      );
      assert.ok(
        !harness.logger.lines.some((line) => line.includes(leakedUid)),
        `${fixture.name}: payload must not reach logs`,
      );
    }
  });
});

describe('audit-18 D1: duplicate observance properties reject', () => {
  const dupToSecondBad = `BEGIN:VTIMEZONE
TZID:D/Z
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETTO:+0300
TZOFFSETTO:+9999
END:STANDARD
END:VTIMEZONE`;
  const dupToValid = `BEGIN:VTIMEZONE
TZID:Dup/Valid
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETTO:+0300
TZOFFSETTO:+0300
END:STANDARD
END:VTIMEZONE`;
  const dupFrom = `BEGIN:VTIMEZONE
TZID:Dup/From
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETFROM:+0300
TZOFFSETTO:+0300
END:STANDARD
END:VTIMEZONE`;
  const dupStart = `BEGIN:VTIMEZONE
TZID:Dup/Start
BEGIN:STANDARD
DTSTART:19700101T000000
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETTO:+0300
END:STANDARD
END:VTIMEZONE`;

  it('rejects duplicate TZOFFSETTO where the second is range-invalid', async () => {
    await assert.rejects(
      parseAndBuild(
        calendar(
          `BEGIN:VEVENT
UID:d1-dup-to-bad
DTSTAMP:20260101T000000Z
DTSTART;TZID=D/Z:20260502T100000
SUMMARY:X
END:VEVENT`,
          dupToSecondBad,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /TZOFFSETTO/ },
    );
  });

  it('rejects duplicate singleton observance properties even when all values are valid', async () => {
    const cases: Array<{ tzid: string; vtimezone: string; match: RegExp }> = [
      { tzid: 'Dup/Valid', vtimezone: dupToValid, match: /TZOFFSETTO/ },
      { tzid: 'Dup/From', vtimezone: dupFrom, match: /TZOFFSETFROM/ },
      { tzid: 'Dup/Start', vtimezone: dupStart, match: /DTSTART/ },
    ];
    for (const { tzid, vtimezone, match } of cases) {
      await assert.rejects(
        parseAndBuild(
          calendar(
            `BEGIN:VEVENT
UID:d1-dup
DTSTAMP:20260101T000000Z
DTSTART;TZID=${tzid}:20260502T100000
SUMMARY:X
END:VEVENT`,
            vtimezone,
          ),
        ),
        { name: 'CalendarIntegrityError', message: match },
        tzid,
      );
    }
  });

  it('sync preserves the prior snapshot on duplicate-observance rejection', async () => {
    const leaked = 'd1-dup-payload';
    const ics = calendar(
      `BEGIN:VEVENT
UID:${leaked}
DTSTART;TZID=D/Z:20260502T100000
END:VEVENT`,
      dupToSecondBad,
    );
    const now = Date.UTC(2026, 4, 1, 12, 0, 0);
    const harness = createHarness({ now });
    await seedSource(harness.repository, 'basic', now);
    const startsAtMs = now + MS_PER_HOUR;
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: `keep-1#${startsAtMs}`, uid: 'keep-1', startsAtMs })],
      now,
    );
    harness.setHandler(() => textResponse(ics));
    const rejected = await runSourceSync(
      syncDeps(harness, { parser: new IcalJsCalendarParser() }),
      TEST_SOURCE,
      'sync-owner-1',
    );
    assert.equal(rejected.status, 'rejected');
    assert.equal(rejected.reason, 'CalendarIntegrityError');
    assert.equal(rejected.upserted, 0);
    assert.equal(rejected.deleted, 0);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
    assert.ok(!harness.logger.lines.some((line) => line.includes(leaked)));
  });
});

describe('audit-18 D2: only referenced zones can reject', () => {
  const BROKEN_UNUSED = `BEGIN:VTIMEZONE
TZID:Unused/Bad
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETTO:+9999
END:STANDARD
END:VTIMEZONE`;

  it('parses a valid UTC event despite an unused broken VTIMEZONE', async () => {
    const occurrences = await parseAndBuild(
      calendar(
        `BEGIN:VEVENT
UID:d2-unused-ok
DTSTAMP:20260101T000000Z
DTSTART:20260502T100000Z
SUMMARY:X
END:VEVENT`,
        BROKEN_UNUSED,
      ),
    );
    assert.equal(occurrences.length, 1);
    assert.equal(occurrences[0]?.startsAtMs, Date.UTC(2026, 4, 2, 10));
  });

  it('still rejects a referenced broken zone and preserves the snapshot', async () => {
    const leaked = 'd2-ref-payload';
    const ics = calendar(
      `BEGIN:VEVENT
UID:${leaked}
DTSTART;TZID=Unused/Bad:20260502T100000
END:VEVENT`,
      BROKEN_UNUSED,
    );
    await assert.rejects(parseAndBuild(ics), { name: 'CalendarIntegrityError' });
    const now = Date.UTC(2026, 4, 1, 12, 0, 0);
    const harness = createHarness({ now });
    await seedSource(harness.repository, 'basic', now);
    const startsAtMs = now + MS_PER_HOUR;
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: `keep-1#${startsAtMs}`, uid: 'keep-1', startsAtMs })],
      now,
    );
    harness.setHandler(() => textResponse(ics));
    const rejected = await runSourceSync(
      syncDeps(harness, { parser: new IcalJsCalendarParser() }),
      TEST_SOURCE,
      'sync-owner-1',
    );
    assert.equal(rejected.status, 'rejected');
    assert.equal(rejected.reason, 'CalendarIntegrityError');
    assert.equal(rejected.upserted, 0);
    assert.equal(rejected.deleted, 0);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
    assert.ok(!harness.logger.lines.some((line) => line.includes(leaked)));
  });
});

/*
 * Spot-check falsification: a VTIMEZONE whose TZID is a reserved UTC alias
 * (`Z`/`UTC`/`GMT`) was dropped from the referenced set as a built-in, so a
 * broken declaration was never validated while ical.js resolved `TZID=GMT`
 * through the declared zone (verified: `Component.getTimeZoneByID` prefers an
 * in-feed VTIMEZONE over `TimezoneService`, with an exact case- and
 * whitespace-sensitive name comparison and first-declaration-wins order).
 * The regressions below lock the corrected behavior for every alias.
 */
describe('audit-18 (e): declared UTC-alias zones are custom referenced zones', () => {
  const ALIASES = ['GMT', 'UTC', 'Z'] as const;

  function zone(
    tzid: string,
    offsetFrom: string,
    offsetTo: string,
    dtstart = '19700101T000000',
  ): string {
    return `BEGIN:VTIMEZONE
TZID:${tzid}
BEGIN:STANDARD
DTSTART:${dtstart}
TZOFFSETFROM:${offsetFrom}
TZOFFSETTO:${offsetTo}
END:STANDARD
END:VTIMEZONE`;
  }

  function aliasEvent(tzid: string): string {
    return `BEGIN:VEVENT
UID:alias-evt
DTSTAMP:20260101T000000Z
DTSTART;TZID=${tzid}:20260502T100000
SUMMARY:X
END:VEVENT`;
  }

  it('rejects a referenced broken declared alias zone (GMT, UTC, Z)', async () => {
    for (const alias of ALIASES) {
      await assert.rejects(
        parseAndBuild(calendar(aliasEvent(alias), zone(alias, '+0300', '+9999'))),
        { name: 'CalendarIntegrityError', message: /TZOFFSETTO/ },
        alias,
      );
    }
    await assert.rejects(
      parseAndBuild(
        calendar(
          aliasEvent('GMT'),
          `BEGIN:VTIMEZONE
TZID:GMT
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETTO:+0300
END:STANDARD
END:VTIMEZONE`,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /TZOFFSETFROM/ },
    );
  });

  it('resolves a valid declared alias zone through the declaration, not built-in UTC', async () => {
    const cases: Array<{ alias: string; offset: string; expectedMs: number }> = [
      { alias: 'GMT', offset: '+0300', expectedMs: Date.UTC(2026, 4, 2, 7) },
      { alias: 'UTC', offset: '-0400', expectedMs: Date.UTC(2026, 4, 2, 14) },
      { alias: 'Z', offset: '+0530', expectedMs: Date.UTC(2026, 4, 2, 4, 30) },
    ];
    for (const { alias, offset, expectedMs } of cases) {
      const parsed = await parse(calendar(aliasEvent(alias), zone(alias, offset, offset)));
      assert.equal(parsed.events[0]?.startsAtMs, expectedMs, alias);
      assert.equal(parsed.events[0]?.timezone, alias, alias);
    }
  });

  it('validates a declared alias zone like any custom zone, first declaration wins', async () => {
    await assert.rejects(
      parseAndBuild(calendar(aliasEvent('GMT'), zone('GMT', '+0300', '+0300', '20300101T000000'))),
      { name: 'CalendarIntegrityError', message: /without a usable observance/ },
    );
    // ical.js resolves through the first declaration, so a usable second
    // declaration cannot rescue a future-only first one.
    await assert.rejects(
      parseAndBuild(
        calendar(
          aliasEvent('GMT'),
          `${zone('GMT', '+0300', '+0300', '20300101T000000')}\n${zone('GMT', '+0300', '+0300')}`,
        ),
      ),
      { name: 'CalendarIntegrityError', message: /without a usable observance/ },
    );
    const usableFirst = await parse(
      calendar(
        aliasEvent('GMT'),
        `${zone('GMT', '+0300', '+0300')}\n${zone('GMT', '+0300', '+0300', '20300101T000000')}`,
      ),
    );
    assert.equal(usableFirst.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 7));
  });

  it('keeps plain Z and undeclared aliases on built-in UTC', async () => {
    const plain = await parse(
      calendar(`BEGIN:VEVENT
UID:plain-z
DTSTAMP:20260101T000000Z
DTSTART:20260502T100000Z
SUMMARY:X
END:VEVENT`),
    );
    assert.equal(plain.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 10));
    for (const alias of ALIASES) {
      const parsed = await parse(calendar(aliasEvent(alias)));
      assert.equal(parsed.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 10), alias);
      // All three built-ins alias the same ICAL.Timezone, whose tzid is UTC.
      assert.equal(parsed.events[0]?.timezone, 'UTC', alias);
    }
  });

  it('mirrors exact TZID matching: case, whitespace and quoting', async () => {
    // A case difference means the declaration does not match the reference and
    // is unreferenced, so the built-in alias stays in effect even when broken.
    const caseMismatch = await parse(calendar(aliasEvent('GMT'), zone('gmt', '+0300', '+9999')));
    assert.equal(caseMismatch.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 10));
    // An exact lowercase match is a custom zone and is validated.
    const lower = await parse(calendar(aliasEvent('gmt'), zone('gmt', '+0300', '+0300')));
    assert.equal(lower.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 7));
    // Trailing whitespace is part of the TZID in ical.js.
    const spaceDecl = await parse(calendar(aliasEvent('GMT'), zone('GMT ', '+0300', '+0300')));
    assert.equal(spaceDecl.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 10));
    const spaceBoth = await parse(calendar(aliasEvent('GMT '), zone('GMT ', '+0300', '+0300')));
    assert.equal(spaceBoth.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 7));
    // Quotes are parameter syntax, not part of the value.
    await assert.rejects(
      parseAndBuild(calendar(aliasEvent('"GMT"'), zone('GMT', '+0300', '+9999'))),
      { name: 'CalendarIntegrityError', message: /TZOFFSETTO/ },
    );
    const spaced = await parse(
      calendar(
        aliasEvent('"Custom/Zone With Space"'),
        zone('Custom/Zone With Space', '+0300', '+0300'),
      ),
    );
    assert.equal(spaced.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 7));
  });

  it('ignores an unreferenced broken alias zone', async () => {
    for (const alias of ALIASES) {
      const parsed = await parse(
        calendar(
          `BEGIN:VEVENT
UID:alias-unused
DTSTAMP:20260101T000000Z
DTSTART:20260502T100000Z
SUMMARY:X
END:VEVENT`,
          zone(alias, '+0300', '+9999'),
        ),
      );
      assert.equal(parsed.events[0]?.startsAtMs, Date.UTC(2026, 4, 2, 10), alias);
    }
  });

  it('sync preserves the prior snapshot on a referenced broken alias zone', async () => {
    for (const alias of ALIASES) {
      const leaked = `alias-payload-${alias}`;
      const ics = calendar(
        `BEGIN:VEVENT
UID:${leaked}
DTSTART;TZID=${alias}:20260502T100000
END:VEVENT`,
        zone(alias, '+0300', '+9999'),
      );
      await assert.rejects(parse(ics), { name: 'CalendarIntegrityError' }, alias);
      const now = Date.UTC(2026, 4, 1, 12, 0, 0);
      const harness = createHarness({ now });
      await seedSource(harness.repository, 'basic', now);
      const startsAtMs = now + MS_PER_HOUR;
      await harness.repository.upsertOccurrences(
        [occurrence({ occurrenceKey: `keep-1#${startsAtMs}`, uid: 'keep-1', startsAtMs })],
        now,
      );
      harness.setHandler(() => textResponse(ics));
      const rejected = await runSourceSync(
        syncDeps(harness, { parser: new IcalJsCalendarParser() }),
        TEST_SOURCE,
        'sync-owner-1',
      );
      assert.equal(rejected.status, 'rejected', alias);
      assert.equal(rejected.reason, 'CalendarIntegrityError', alias);
      assert.equal(rejected.upserted, 0, alias);
      assert.equal(rejected.deleted, 0, alias);
      assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1, alias);
      assert.ok(!harness.logger.lines.some((line) => line.includes(leaked)), alias);
    }
  });
});
