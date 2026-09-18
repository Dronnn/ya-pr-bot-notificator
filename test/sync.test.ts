import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { runSourceSync, type SourceDefinition } from '../src/calendar/sync.ts';
import { IcalJsCalendarParser } from '../src/calendar/icaljs-parser.ts';
import { RecurrenceLimitError, type ParsedEvent } from '../src/domain/calendar.ts';
import { currentMoscowMondayStartMs, runSchedulerTick } from '../src/scheduler/tick.ts';
import {
  EXPANSION_HORIZON_MS,
  JOB_LEASE_MS,
  MAX_ICAL_BYTES,
  MS_PER_DAY,
  MS_PER_HOUR,
  MS_PER_MINUTE,
} from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { createThrowingParser, textResponse } from './helpers/fakes.ts';
import { createHarness, schedulerDeps, syncDeps } from './helpers/harness.ts';
import { EMPTY_ICS, occurrence, onboardUser, seedReminderOffsets } from './helpers/seed.ts';

function event(startsAtMs: number, overrides: Partial<ParsedEvent> = {}): ParsedEvent {
  return {
    uid: 'course-1',
    summary: 'Lesson',
    description: null,
    url: null,
    startsAtMs,
    endsAtMs: startsAtMs + MS_PER_HOUR,
    status: 'confirmed',
    isAllDay: false,
    recurrenceIdMs: null,
    expandedStartsMs: [startsAtMs],
    timezone: 'Europe/Moscow',
    ...overrides,
  };
}

const SOURCE: SourceDefinition = { id: 'basic', kind: 'basic', url: 'https://example.test/basic.ics' };

/** Europe/Moscow wall clock to UTC epoch ms (fixed UTC+3). */
function msk(year: number, month: number, day: number, hour = 0): number {
  return Date.UTC(year, month - 1, day, hour - 3);
}

const WEEKLY_ICS = `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//test//EN
BEGIN:VEVENT
UID:horizon-1
DTSTAMP:20240101T000000Z
DTSTART:20240603T100000Z
DTEND:20240603T113000Z
RRULE:FREQ=WEEKLY;BYDAY=MO
SUMMARY:Weekly lesson
END:VEVENT
END:VCALENDAR`;

const SINGLE_ICS = `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//test//EN
BEGIN:VEVENT
UID:single-1
DTSTAMP:20240101T000000Z
DTSTART:20240603T100000Z
DTEND:20240603T113000Z
SUMMARY:Single lesson
END:VEVENT
END:VCALENDAR`;

describe('calendar synchronization', () => {
  it('applies a valid snapshot and refreshes source freshness', async () => {
    const harness = createHarness();
    const start = harness.clock.now() + MS_PER_HOUR;
    harness.setParser({ events: [event(start)] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'v1', 'last-modified': 'lm1' }));

    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');
    assert.equal(result.status, 'applied');
    assert.equal(result.upserted, 1);

    const upcoming = await harness.repository.listUpcomingOccurrences(
      'basic',
      harness.clock.now(),
      10,
    );
    assert.equal(upcoming.length, 1);
    const source = await harness.repository.getSource('basic');
    assert.equal(source?.fetchedAtMs, harness.clock.now());
  });

  it('sends conditional headers and treats 304 as a freshness refresh', async () => {
    const harness = createHarness();
    const start = harness.clock.now() + MS_PER_HOUR;
    harness.setParser({ events: [event(start)] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'v1', 'last-modified': 'lm1' }));
    await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');

    harness.setHandler(() => new Response(null, { status: 304 }));
    harness.clock.advance(5 * MS_PER_MINUTE);
    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-2');
    assert.equal(result.status, 'not-modified');
    const source = await harness.repository.getSource('basic');
    assert.equal(source?.fetchedAtMs, harness.clock.now());

    const conditional = harness.fetchSpy.calls[0]?.init?.headers as
      | Record<string, string>
      | undefined;
    assert.equal(conditional?.['if-none-match'], 'v1');
    assert.equal(conditional?.['if-modified-since'], 'lm1');
  });

  it('rejects a truncated snapshot and preserves existing data', async () => {
    const harness = createHarness();
    const start = harness.clock.now() + MS_PER_HOUR;
    harness.setParser({ events: [event(start)] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));
    await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');

    harness.setHandler(() => textResponse('BEGIN:VCALENDAR\nVERSION:2.0', 200));
    harness.clock.advance(6 * MS_PER_MINUTE);
    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-2');
    assert.equal(result.status, 'rejected');
    assert.equal(result.reason, 'truncated');

    const upcoming = await harness.repository.listUpcomingOccurrences('basic', harness.clock.now(), 10);
    assert.equal(upcoming.length, 1);
  });

  it('rejects a snapshot whose recurrence exceeds the expansion budget', async () => {
    const harness = createHarness();
    const start = harness.clock.now() + MS_PER_HOUR;
    harness.setParser({ events: [event(start)] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));
    await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');

    const tooFrequent = `BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:bad-rule
DTSTAMP:20231101T000000Z
DTSTART:20231115T100000Z
RRULE:FREQ=SECONDLY
SUMMARY:Too frequent
END:VEVENT
END:VCALENDAR`;
    harness.setHandler(() => textResponse(tooFrequent, 200));
    harness.clock.advance(6 * MS_PER_MINUTE);
    const result = await runSourceSync(
      syncDeps(harness, { parser: new IcalJsCalendarParser() }),
      SOURCE,
      'owner-2',
    );
    assert.equal(result.status, 'rejected');
    assert.equal(result.reason, 'RecurrenceLimitError');
    assert.ok(new RecurrenceLimitError('x') instanceof Error);

    const upcoming = await harness.repository.listUpcomingOccurrences('basic', harness.clock.now(), 10);
    assert.equal(upcoming.length, 1);
  });

  it('rejects redirects', async () => {
    const harness = createHarness();
    harness.setHandler(() => textResponse('', 302));
    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');
    assert.equal(result.status, 'rejected');
    assert.equal(result.reason, 'redirect');
  });

  it('ingests a real ICS snapshot with recurrence through the ical.js parser', async () => {
    const harness = createHarness({ now: Date.UTC(2024, 5, 1) });
    const now = harness.clock.now();
    const ics = `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//test//EN
BEGIN:VTIMEZONE
TZID:Europe/Moscow
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:+0300
TZOFFSETTO:+0300
TZNAME:MSK
END:STANDARD
END:VTIMEZONE
BEGIN:VEVENT
UID:rec-1
DTSTAMP:20240101T000000Z
DTSTART;TZID=Europe/Moscow:20240603T100000
DTEND;TZID=Europe/Moscow:20240603T113000
RRULE:FREQ=WEEKLY;BYDAY=MO
EXDATE;TZID=Europe/Moscow:20240610T100000
SUMMARY:Weekly lesson
END:VEVENT
END:VCALENDAR`;
    harness.setHandler(() => textResponse(ics, 200, { etag: 'v1' }));
    const result = await runSourceSync(
      syncDeps(harness, { parser: new IcalJsCalendarParser() }),
      SOURCE,
      'owner-ics',
    );

    assert.equal(result.status, 'applied');
    assert.equal(result.upserted, 3);
    const upcoming = await harness.repository.listUpcomingOccurrences('basic', now, 10);
    assert.deepEqual(
      upcoming.map((row) => row.startsAtMs),
      [msk(2024, 6, 3, 10), msk(2024, 6, 17, 10), msk(2024, 6, 24, 10)],
    );
  });

  it('keeps the previous snapshot when the parser crashes', async () => {
    const harness = createHarness();
    const start = harness.clock.now() + MS_PER_HOUR;
    harness.setParser({ events: [event(start)] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));
    await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');

    harness.clock.advance(6 * MS_PER_MINUTE);
    const result = await runSourceSync(
      syncDeps(harness, { parser: createThrowingParser(new Error('parser exploded')) }),
      SOURCE,
      'owner-2',
    );
    assert.equal(result.status, 'rejected');
    assert.equal(result.reason, 'Error');
    const upcoming = await harness.repository.listUpcomingOccurrences(
      'basic',
      harness.clock.now(),
      10,
    );
    assert.equal(upcoming.length, 1);
  });

  it('keeps the previous snapshot when the fetch fails', async () => {
    const harness = createHarness();
    const start = harness.clock.now() + MS_PER_HOUR;
    harness.setParser({ events: [event(start)] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));
    await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');

    harness.setHandler(() => {
      throw new Error('network down');
    });
    harness.clock.advance(6 * MS_PER_MINUTE);
    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-2');
    assert.equal(result.status, 'error');
    const upcoming = await harness.repository.listUpcomingOccurrences(
      'basic',
      harness.clock.now(),
      10,
    );
    assert.equal(upcoming.length, 1);
  });

  it('rejects a snapshot larger than the size cap', async () => {
    const harness = createHarness();
    harness.setHandler(() =>
      textResponse(`${'x'.repeat(MAX_ICAL_BYTES + 1)}END:VCALENDAR`, 200),
    );
    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');
    assert.equal(result.status, 'rejected');
    assert.equal(result.reason, 'truncated');
  });

  it('deletes occurrences that disappeared from a new valid snapshot', async () => {
    const harness = createHarness();
    const start = harness.clock.now() + MS_PER_HOUR;
    harness.setParser({
      events: [
        event(start, { uid: 'a' }),
        event(start + 24 * MS_PER_HOUR, { uid: 'b' }),
      ],
    });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));
    await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');
    assert.equal(
      (await harness.repository.listUpcomingOccurrences('basic', harness.clock.now(), 10)).length,
      2,
    );

    harness.setParser({ events: [event(start, { uid: 'a' })] });
    harness.clock.advance(6 * MS_PER_MINUTE);
    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-2');
    assert.equal(result.status, 'applied');
    assert.equal(result.deleted, 1);
    assert.equal(
      (await harness.repository.listUpcomingOccurrences('basic', harness.clock.now(), 10)).length,
      1,
    );
  });

  it('reports the committed deletion count and preserves rows beyond the horizon', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const start = now + MS_PER_HOUR;
    harness.setParser({ events: [event(start, { uid: 'a' })] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));
    await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');

    // A row the 30-day expansion never compared sits beyond the horizon end.
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'far', startsAtMs: now + 40 * MS_PER_DAY })],
      now,
    );

    harness.setParser({ events: [] });
    harness.clock.advance(6 * MS_PER_MINUTE);
    const before = countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences');
    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-2');

    assert.equal(result.status, 'applied');
    assert.equal(result.deleted, 1, 'only the in-horizon disappearance is deleted');
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'),
      before - 1,
      'the reported deleted count equals the committed row delta',
    );
    const remaining = harness.db.database
      .prepare('SELECT id FROM occurrences ORDER BY id')
      .all() as { id: string }[];
    assert.deepEqual(
      remaining.map((row) => row.id),
      ['basic:far'],
      'the row beyond the horizon end survives',
    );
  });

  it('isolates a failing source from a healthy one in the same tick', async () => {
    const harness = createHarness({ now: Date.UTC(2023, 10, 12, 21) });
    const start = harness.clock.now() + MS_PER_HOUR;
    harness.setParser({ events: [event(start)] });
    harness.setHandler((url) =>
      url.includes('extended') ? textResponse('down', 503) : textResponse(EMPTY_ICS, 200),
    );
    const sources: SourceDefinition[] = [
      { id: 'basic', kind: 'basic', url: 'https://example.test/basic.ics' },
      { id: 'extended', kind: 'extended', url: 'https://example.test/extended.ics' },
    ];

    const result = await runSchedulerTick(schedulerDeps(harness, { sources }));

    assert.deepEqual(result.syncStatuses, ['applied', 'error']);
    assert.equal((await harness.repository.getSource('basic'))?.status, 'ok');
    assert.equal((await harness.repository.getSource('extended'))?.status, 'error');
  });

  it('materializes only events whose start is strictly in the future', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    harness.setParser({
      events: [
        event(now - 1, { uid: 'past' }),
        event(now, { uid: 'started' }),
        event(now + 1, { uid: 'future' }),
      ],
    });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));

    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');

    assert.equal(result.status, 'applied');
    assert.equal(result.upserted, 1);
    const rows = harness.db.database
      .prepare('SELECT uid, starts_at_ms FROM occurrences ORDER BY starts_at_ms')
      .all() as Array<{ uid: string; starts_at_ms: number }>;
    assert.deepEqual(
      rows.map((row) => ({ uid: row.uid, starts_at_ms: row.starts_at_ms })),
      [{ uid: 'future', starts_at_ms: now + 1 }],
    );
  });

  it('applies a large snapshot in bounded chunks', async () => {
    const harness = createHarness();
    const start = harness.clock.now() + MS_PER_HOUR;
    const events = Array.from({ length: 60 }, (_, index) =>
      event(start + index * MS_PER_HOUR, { uid: `course-${index}` }),
    );
    harness.setParser({ events });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));

    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');

    assert.equal(result.status, 'applied');
    assert.equal(result.upserted, 60);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 60);
  });

  it('skips when another owner holds the source lease', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    assert.notEqual(
      await harness.repository.acquireSourceLease('basic', 'other-owner', now, 5 * 60_000),
      null,
    );
    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-2');
    assert.equal(result.status, 'skipped');
    assert.equal(result.reason, 'lease-held');
  });
});

describe('weekly Monday scheduler refresh', () => {
  const mondayStartMs = Date.UTC(2024, 5, 2, 21); // 2024-06-03 00:00 Europe/Moscow

  it('downloads once per successful Moscow Monday and again the following Monday', async () => {
    const harness = createHarness({ now: mondayStartMs + 10 * MS_PER_MINUTE });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));

    const first = await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }));
    assert.deepEqual(first.syncStatuses, ['applied']);
    assert.equal(harness.fetchSpy.calls.length, 1);

    harness.clock.advance(5 * MS_PER_MINUTE);
    const sameMonday = await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }));
    assert.deepEqual(sameMonday.syncStatuses, ['skipped']);
    assert.equal(harness.fetchSpy.calls.length, 1);

    harness.clock.advance(MS_PER_DAY);
    const tuesday = await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }));
    assert.deepEqual(tuesday.syncStatuses, ['skipped']);
    assert.equal(harness.fetchSpy.calls.length, 1);

    harness.clock.advance(6 * MS_PER_DAY);
    const nextMonday = await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }));
    assert.deepEqual(nextMonday.syncStatuses, ['applied']);
    assert.equal(harness.fetchSpy.calls.length, 2);
  });

  it('runs the first fetch outside Monday when the source was never attempted', async () => {
    const harness = createHarness({ now: mondayStartMs - MS_PER_DAY });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));

    const first = await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }));
    assert.deepEqual(first.syncStatuses, ['applied']);
    assert.equal(harness.fetchSpy.calls.length, 1);

    harness.clock.advance(5 * MS_PER_MINUTE);
    const sameSunday = await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }));
    assert.deepEqual(sameSunday.syncStatuses, ['skipped']);
    assert.equal(harness.fetchSpy.calls.length, 1);

    harness.clock.advance(MS_PER_DAY - 5 * MS_PER_MINUTE);
    const monday = await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }));
    assert.deepEqual(monday.syncStatuses, ['applied']);
    assert.equal(harness.fetchSpy.calls.length, 2);
  });

  it('does not repeat a failed initial fetch outside Monday', async () => {
    const harness = createHarness({ now: mondayStartMs - MS_PER_DAY });
    let attempts = 0;
    harness.setHandler(() => {
      attempts += 1;
      return attempts === 1 ? textResponse('down', 503) : textResponse(EMPTY_ICS, 200);
    });

    const failed = await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }));
    assert.deepEqual(failed.syncStatuses, ['error']);
    assert.equal(harness.fetchSpy.calls.length, 1);

    harness.clock.advance(5 * MS_PER_MINUTE);
    const sameSunday = await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }));
    assert.deepEqual(sameSunday.syncStatuses, ['skipped']);
    assert.equal(harness.fetchSpy.calls.length, 1);

    harness.clock.advance(MS_PER_DAY - 5 * MS_PER_MINUTE);
    const monday = await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }));
    assert.deepEqual(monday.syncStatuses, ['applied']);
    assert.equal(harness.fetchSpy.calls.length, 2);
    assert.equal(attempts, 2);
  });

  it('retries a failed download during Monday, then stops after success', async () => {
    const harness = createHarness({ now: mondayStartMs });
    let attempts = 0;
    harness.setHandler(() => {
      attempts += 1;
      return attempts === 1 ? textResponse('down', 503) : textResponse(EMPTY_ICS, 200);
    });

    assert.deepEqual(
      (await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }))).syncStatuses,
      ['error'],
    );
    harness.clock.advance(5 * MS_PER_MINUTE);
    assert.deepEqual(
      (await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }))).syncStatuses,
      ['applied'],
    );
    harness.clock.advance(5 * MS_PER_MINUTE);
    assert.deepEqual(
      (await runSchedulerTick(schedulerDeps(harness, { sources: [SOURCE] }))).syncStatuses,
      ['skipped'],
    );
    assert.equal(attempts, 2);
  });

  it('uses the Europe/Moscow Monday boundary', () => {
    assert.equal(currentMoscowMondayStartMs(mondayStartMs - 1), null);
    assert.equal(currentMoscowMondayStartMs(mondayStartMs), mondayStartMs);
    assert.equal(currentMoscowMondayStartMs(mondayStartMs + MS_PER_DAY - 1), mondayStartMs);
    assert.equal(currentMoscowMondayStartMs(mondayStartMs + MS_PER_DAY), null);
  });
});

describe('horizon advancement after 304', () => {
  it('re-expands an unchanged recurring feed so future occurrences keep existing', async () => {
    const start = Date.UTC(2024, 5, 1); // 2024-06-01
    const harness = createHarness({ now: start });
    await harness.repository.activateUser(111, 111, start);
    onboardUser(harness, 111);

    harness.setHandler((_url, init) => {
      const headers = init?.headers as Record<string, string> | undefined;
      if (headers?.['if-none-match'] === 'v1') {
        return new Response(null, { status: 304 });
      }
      return textResponse(WEEKLY_ICS, 200, { etag: 'v1' });
    });
    const deps = syncDeps(harness, { parser: new IcalJsCalendarParser() });

    const first = await runSourceSync(deps, SOURCE, 'owner-1');
    assert.equal(first.status, 'applied');
    const firstMax = await harness.repository.getMaxOccurrenceStart('basic');
    assert.ok(firstMax !== null);

    // More than a full horizon later the cached snapshot has run out of coverage.
    harness.clock.advance(31 * MS_PER_DAY);
    harness.fetchSpy.calls.length = 0;
    const second = await runSourceSync(deps, SOURCE, 'owner-2');
    assert.equal(second.status, 'applied', 'a lagging 304 must trigger a bounded refresh');

    const secondMax = await harness.repository.getMaxOccurrenceStart('basic');
    assert.ok(secondMax !== null && firstMax !== null);
    assert.ok(secondMax > firstMax, 'the materialized horizon must advance');

    const upcoming = await harness.repository.listUpcomingOccurrences(
      'basic',
      harness.clock.now(),
      10,
    );
    assert.ok(upcoming.length >= 4, `expected future occurrences, got ${upcoming.length}`);

    const refreshes = harness.fetchSpy.calls.filter(
      (call) => ((call.init?.headers ?? {}) as Record<string, string>)['if-none-match'] === undefined,
    );
    assert.equal(refreshes.length, 1, 'only the refresh bypasses the conditional headers');
    assert.equal(harness.fetchSpy.calls.length, 2, 'one conditional 304 then one refresh');
  });

  it('bounds how often a lagging 304 triggers an unconditional refresh', async () => {
    const start = Date.UTC(2024, 5, 1);
    const harness = createHarness({ now: start });
    harness.setHandler((_url, init) => {
      const headers = init?.headers as Record<string, string> | undefined;
      if (headers?.['if-none-match'] === 'v1') {
        return new Response(null, { status: 304 });
      }
      return textResponse(SINGLE_ICS, 200, { etag: 'v1' });
    });
    const deps = syncDeps(harness, { parser: new IcalJsCalendarParser() });

    // A single near-term event means coverage always looks lagging.
    await runSourceSync(deps, SOURCE, 'owner-1');
    const baseline = harness.fetchSpy.calls.length;

    harness.clock.advance(6 * MS_PER_MINUTE);
    const withinInterval = await runSourceSync(deps, SOURCE, 'owner-2');
    assert.equal(withinInterval.status, 'not-modified');
    assert.equal(
      harness.fetchSpy.calls.length - baseline,
      1,
      'no refresh is allowed inside the interval',
    );

    harness.clock.advance(MS_PER_DAY);
    const afterInterval = await runSourceSync(deps, SOURCE, 'owner-3');
    assert.equal(afterInterval.status, 'applied');
    assert.equal(
      harness.fetchSpy.calls.length - baseline,
      3,
      'conditional 304 plus one bounded refresh after the interval',
    );
  });

  it('does not recreate a reminder that was already sent', async () => {
    const start = Date.UTC(2024, 5, 1);
    const harness = createHarness({ now: start });
    await harness.repository.activateUser(111, 111, start);
    onboardUser(harness, 111);
    seedReminderOffsets(harness, 111, [30]);

    harness.setHandler((_url, init) => {
      const headers = init?.headers as Record<string, string> | undefined;
      if (headers?.['if-none-match'] === 'v1') {
        return new Response(null, { status: 304 });
      }
      return textResponse(WEEKLY_ICS, 200, { etag: 'v1' });
    });
    const deps = syncDeps(harness, { parser: new IcalJsCalendarParser() });

    await runSourceSync(deps, SOURCE, 'owner-1');
    // Move just past the first reminder (Mon 10:00Z, 30 minute offset).
    harness.clock.advance(2 * MS_PER_DAY + 9 * MS_PER_HOUR + 31 * MS_PER_MINUTE);
    await harness.repository.planDueReminders(
      harness.clock.now(),
      harness.clock.now() + EXPANSION_HORIZON_MS,
    );
    const claimed = await harness.repository.claimDueJobs(
      'scheduler',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.ok(claimed.length >= 1);
    const jobId = claimed[0]?.jobId ?? '';
    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'scheduler', harness.clock.now(), JOB_LEASE_MS),
      null,
    );
    assert.equal(
      await harness.repository.finishJobSent(jobId, 'scheduler', harness.clock.now()),
      true,
    );

    // Advance past the horizon and re-sync; the sent job must survive re-planning.
    harness.clock.advance(31 * MS_PER_DAY);
    await runSourceSync(deps, SOURCE, 'owner-2');
    await harness.repository.planDueReminders(
      harness.clock.now(),
      harness.clock.now() + EXPANSION_HORIZON_MS,
    );
    assert.equal((await harness.repository.getJob(jobId))?.status, 'sent');
    const duplicates = harness.db.database
      .prepare('SELECT COUNT(*) AS n FROM outbound_jobs WHERE dedup_key = ?')
      .get(`rem:111:basic:horizon-1#${Date.UTC(2024, 5, 3, 10)}:30`) as { n: number };
    assert.equal(Number(duplicates.n), 1, 'the sent reminder must not be recreated');
  });
});

describe('fetch deadline covers the body', () => {
  it('times out a response whose body stalls after the headers arrive', async () => {
    const harness = createHarness();
    harness.setHandler(() => {
      const stream = new ReadableStream<Uint8Array>({
        start() {
          // Headers arrive, the body never produces a chunk.
        },
      });
      return new Response(stream, { status: 200, headers: { etag: 'v1' } });
    });

    const startedAt = Date.now();
    const result = await runSourceSync(
      syncDeps(harness, { fetchTimeoutMs: 50 }),
      SOURCE,
      'owner-1',
    );
    const elapsed = Date.now() - startedAt;

    assert.equal(result.status, 'error');
    assert.ok(elapsed < 2_000, `stalled body must be bounded, took ${elapsed}ms`);
    assert.equal((await harness.repository.getSource('basic'))?.status, 'error');
  });

  it('surfaces a mid-body stream failure without applying a snapshot', async () => {
    const harness = createHarness();
    const start = harness.clock.now() + MS_PER_HOUR;
    harness.setParser({ events: [event(start)] });
    harness.setHandler(() =>
      textResponse(WEEKLY_ICS, 200, { etag: 'v1' }),
    );
    await runSourceSync(syncDeps(harness), SOURCE, 'owner-1');

    harness.setHandler(() => {
      const encoder = new TextEncoder();
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(encoder.encode('BEGIN:VCALENDAR\n'));
          controller.error(new Error('stream broke'));
        },
      });
      return new Response(stream, { status: 200 });
    });
    harness.clock.advance(6 * MS_PER_MINUTE);
    const result = await runSourceSync(syncDeps(harness), SOURCE, 'owner-2');

    assert.equal(result.status, 'error');
    assert.equal((await harness.repository.getSource('basic'))?.status, 'error');
    const upcoming = await harness.repository.listUpcomingOccurrences(
      'basic',
      harness.clock.now(),
      10,
    );
    assert.equal(upcoming.length, 1, 'the previous snapshot must be preserved');
  });

  it('applies a valid chunked stream', async () => {
    const harness = createHarness({ now: Date.UTC(2024, 5, 1) });
    const encoder = new TextEncoder();
    harness.setHandler(() => {
      const chunks = WEEKLY_ICS.match(/.{1,40}/gs) ?? [];
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          for (const chunk of chunks) {
            controller.enqueue(encoder.encode(chunk));
          }
          controller.close();
        },
      });
      return new Response(stream, { status: 200 });
    });

    const result = await runSourceSync(
      syncDeps(harness, { parser: new IcalJsCalendarParser() }),
      SOURCE,
      'owner-1',
    );
    assert.equal(result.status, 'applied');
    assert.ok(result.upserted > 0);
  });
});
