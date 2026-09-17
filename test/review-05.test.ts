/**
 * Regression tests for the four defects confirmed by follow-up review 05:
 * source-lease custody during snapshot publication, per-feed timezone
 * resolution, pacing that must not outlive the event start, and cooldowns
 * timestamped when the Telegram response arrives.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { runSourceSync } from '../src/calendar/sync.ts';
import { IcalJsCalendarParser } from '../src/calendar/icaljs-parser.ts';
import { CalendarIntegrityError } from '../src/domain/calendar.ts';
import { SEND_PACE_WINDOW_MS } from '../src/data/repository.ts';
import { processQueueBatch } from '../src/queue/consumer.ts';
import {
  BOT_SEND_PACE_PER_SECOND,
  JOB_BACKOFF_BASE_MS,
  MS_PER_HOUR,
  MS_PER_MINUTE,
  PACE_RETRY_DELAY_MS,
} from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { jsonResponse, textResponse } from './helpers/fakes.ts';
import { consumerDeps, createHarness, syncDeps, type Harness } from './helpers/harness.ts';
import {
  EMPTY_ICS,
  expireSourceLease,
  makeQueueMessage,
  occurrence,
  parsedEvent,
  seedDueJobs,
  seedSource,
  TEST_SOURCE,
} from './helpers/seed.ts';
import { deferred, pausingRepository } from './helpers/streams.ts';
import { findPacingViolations } from './helpers/simulation.ts';

function occurrenceRows(harness: Harness): { summary: string; revision: number }[] {
  const rows = harness.db.database
    .prepare('SELECT summary, revision FROM occurrences ORDER BY id')
    .all() as { summary: unknown; revision: unknown }[];
  return rows.map((row) => ({ summary: String(row.summary), revision: Number(row.revision) }));
}

describe('source custody during snapshot publication', () => {
  it('never lets an expired owner erase a newer snapshot', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);
    // A shared event both attempts publish (same occurrence key), plus a row
    // only the newer attempt knows about.
    const sharedMs = start + 3 * MS_PER_HOUR;
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: `shared#${sharedMs}`, startsAtMs: sharedMs, summary: 'Seed' })],
      start,
    );

    const staged = deferred();
    const release = deferred();
    const oldRepository = pausingRepository(harness.db, async () => {
      staged.resolve();
      await release.promise;
    });

    // The old attempt stages a full snapshot (one staging batch) and is then
    // paused immediately before its publication.
    harness.setParser({
      events: [
        parsedEvent(start + 3 * MS_PER_HOUR, { uid: 'shared', summary: 'Old' }),
        parsedEvent(start + MS_PER_HOUR, { uid: 'old-only', summary: 'Old only' }),
      ],
    });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'old' }));
    const oldSync = runSourceSync(
      syncDeps(harness, { repository: oldRepository }),
      TEST_SOURCE,
      'old-owner',
    );
    await staged.promise;

    // Its lease expires and a newer attempt publishes different content.
    expireSourceLease(harness);
    const newer = harness.clock.now();
    harness.setParser({
      events: [
        parsedEvent(start + 3 * MS_PER_HOUR, { uid: 'shared', summary: 'Changed' }),
        parsedEvent(newer + 4 * MS_PER_HOUR, { uid: 'new-only', summary: 'New only' }),
      ],
    });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'new' }));
    const newResult = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'new-owner');
    assert.equal(newResult.status, 'applied');

    release.resolve();
    const oldResult = await oldSync;

    assert.equal(oldResult.status, 'skipped', 'a stale attempt must not report applied');
    assert.equal(oldResult.reason, 'lost-ownership');
    assert.equal(oldResult.upserted, 0);

    assert.deepEqual(
      occurrenceRows(harness),
      [
        { summary: 'New only', revision: 1 },
        { summary: 'Changed', revision: 2 },
      ],
      'the newer snapshot, its revision and its new row survive the stale publish',
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrences WHERE summary = 'Old only'"),
      0,
      'the stale attempt must not insert its rows',
    );
    const source = await harness.repository.getSource('basic');
    assert.equal(source?.etag, 'new', 'newer source metadata survives');
    assert.equal(source?.fetchedAtMs, newer);
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrence_staging'),
      0,
      'abandoned staging rows are swept by the newer publication',
    );
  });

  it('refuses publication when ownership is lost during staging', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);

    const staged = deferred();
    const release = deferred();
    const oldRepository = pausingRepository(harness.db, async () => {
      staged.resolve();
      await release.promise;
    });

    // 360 writes need 60 staging statements, so the first batch call pauses the
    // old attempt halfway through its own staging.
    harness.setParser({
      events: Array.from({ length: 360 }, (_, index) =>
        parsedEvent(start + MS_PER_MINUTE + index, { uid: `bulk-${index}`, summary: 'Bulk' }),
      ),
    });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'old' }));
    const oldSync = runSourceSync(
      syncDeps(harness, { repository: oldRepository }),
      TEST_SOURCE,
      'old-owner',
    );
    await staged.promise;

    expireSourceLease(harness);
    const newer = harness.clock.now();
    harness.setParser({
      events: [parsedEvent(newer + MS_PER_HOUR, { uid: 'new-only', summary: 'New only' })],
    });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'new' }));
    assert.equal((await runSourceSync(syncDeps(harness), TEST_SOURCE, 'new-owner')).status, 'applied');

    release.resolve();
    const oldResult = await oldSync;

    assert.equal(oldResult.status, 'skipped');
    assert.equal(oldResult.reason, 'lost-ownership');
    assert.deepEqual(occurrenceRows(harness), [{ summary: 'New only', revision: 1 }]);
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrences WHERE summary = 'Bulk'"),
      0,
      'a snapshot staged by an obsolete attempt is never published',
    );
  });

  it('refuses to publish when the lease lapsed first', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);

    const requested = deferred();
    const release = deferred();
    const slowFetch: typeof fetch = async () => {
      requested.resolve();
      await release.promise;
      return textResponse(EMPTY_ICS, 200, { etag: 'late' });
    };
    harness.setParser({ events: [parsedEvent(start + MS_PER_HOUR, { uid: 'late-1' })] });
    const slowSync = runSourceSync(syncDeps(harness, { fetch: slowFetch }), TEST_SOURCE, 'owner-1');
    await requested.promise;

    // Nobody takes the lease over, but this attempt's lease has lapsed.
    expireSourceLease(harness);
    release.resolve();
    const result = await slowSync;

    assert.equal(result.status, 'skipped');
    assert.equal(result.reason, 'lost-ownership');
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 0);
    const source = await harness.repository.getSource('basic');
    assert.equal(source?.etag, 'etag-basic', 'the lapsed attempt must not relabel the source');
    assert.equal(source?.fetchedAtMs, start);
  });

  it('refuses obsolete error and freshness writes', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);

    // The old attempt is paused inside its fetch, after it acquired the lease.
    const requested = deferred();
    const release = deferred();
    const failingFetch: typeof fetch = async () => {
      requested.resolve();
      await release.promise;
      return textResponse('upstream unavailable', 503);
    };
    const oldSync = runSourceSync(
      syncDeps(harness, { fetch: failingFetch }),
      TEST_SOURCE,
      'old-owner',
    );
    await requested.promise;

    expireSourceLease(harness);
    const newer = harness.clock.now();
    harness.setParser({ events: [parsedEvent(newer + MS_PER_HOUR, { uid: 'new-only' })] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'new' }));
    assert.equal((await runSourceSync(syncDeps(harness), TEST_SOURCE, 'new-owner')).status, 'applied');

    release.resolve();
    const oldResult = await oldSync;

    assert.equal(oldResult.status, 'skipped');
    assert.equal(oldResult.reason, 'lost-ownership');
    const source = await harness.repository.getSource('basic');
    assert.equal(source?.status, 'ok', 'an obsolete failure must not overwrite the newer status');
    assert.equal(source?.etag, 'new');
    assert.equal(source?.fetchedAtMs, newer);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
  });

  it('refuses obsolete not-modified freshness writes', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);
    // A recent refresh means a 304 takes the freshness-only path.
    harness.db.exec(`UPDATE sources SET last_refresh_at_ms = ${start}`);

    const requested = deferred();
    const release = deferred();
    const staleFetch: typeof fetch = async () => {
      requested.resolve();
      await release.promise;
      return new Response(null, { status: 304 });
    };
    const oldSync = runSourceSync(syncDeps(harness, { fetch: staleFetch }), TEST_SOURCE, 'old-owner');
    await requested.promise;

    expireSourceLease(harness);
    const newer = harness.clock.now();
    harness.setParser({ events: [parsedEvent(newer + MS_PER_HOUR, { uid: 'new-only' })] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'new' }));
    assert.equal((await runSourceSync(syncDeps(harness), TEST_SOURCE, 'new-owner')).status, 'applied');

    release.resolve();
    const oldResult = await oldSync;

    assert.equal(oldResult.status, 'skipped');
    assert.equal(oldResult.reason, 'lost-ownership');
    const source = await harness.repository.getSource('basic');
    assert.equal(source?.etag, 'new', 'a stale 304 must not roll the cursor back');
    assert.equal(source?.fetchedAtMs, newer);
    assert.equal(source?.status, 'ok');
  });
});

describe('per-feed timezone resolution', () => {
  const opts = {
    sourceTimeZone: 'Europe/Moscow',
    horizonStartMs: Date.UTC(2026, 8, 1),
    horizonEndMs: Date.UTC(2026, 9, 1),
    maxIterations: 100,
  };

  function vtimezone(tzid: string, offset: string): string {
    return `BEGIN:VTIMEZONE
TZID:${tzid}
BEGIN:STANDARD
DTSTART:19700101T000000
TZOFFSETFROM:${offset}
TZOFFSETTO:${offset}
END:STANDARD
END:VTIMEZONE`;
  }

  interface FeedOptions {
    define?: boolean;
    offset?: string;
    wallClock?: string;
  }

  function zonedFeed(tzid: string, options: FeedOptions = {}): string {
    const definition =
      options.define === false ? '' : `${vtimezone(tzid, options.offset ?? '+0300')}\n`;
    return `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//test//EN
${definition}BEGIN:VEVENT
UID:zone-event
DTSTAMP:20240101T000000Z
DTSTART;TZID=${tzid}:${options.wallClock ?? '20260915T100000'}
END:VEVENT
END:VCALENDAR`;
  }

  async function firstStart(ics: string, parser = new IcalJsCalendarParser()): Promise<number> {
    const parsed = await parser.parse(ics, opts);
    const start = parsed.events[0]?.startsAtMs;
    assert.ok(start !== undefined, 'expected a parsed event');
    return start;
  }

  it('rejects a TZID that earlier feeds happened to register', async () => {
    await assert.rejects(
      new IcalJsCalendarParser().parse(zonedFeed('Review05/Zone', { define: false }), opts),
      CalendarIntegrityError,
      'a cold parser rejects an undefined TZID',
    );

    await firstStart(zonedFeed('Review05/Zone', { offset: '+0300' }));

    await assert.rejects(
      new IcalJsCalendarParser().parse(zonedFeed('Review05/Zone', { define: false }), opts),
      CalendarIntegrityError,
      'acceptance must not depend on the isolate history',
    );
  });

  it('uses the definition of the calendar being parsed', async () => {
    const plus3 = await firstStart(zonedFeed('Review05/Zone', { offset: '+0300' }));
    const plus5 = await firstStart(zonedFeed('Review05/Zone', { offset: '+0500' }));
    const plus3Again = await firstStart(zonedFeed('Review05/Zone', { offset: '+0300' }));

    assert.equal(plus3, Date.UTC(2026, 8, 15, 7), '+0300 resolves 10:00 to 07:00Z');
    assert.equal(plus5, Date.UTC(2026, 8, 15, 5), 'a conflicting +0500 definition wins locally');
    assert.equal(plus3Again, Date.UTC(2026, 8, 15, 7), 'the next parse is independent again');
  });

  it('resolves built-in UTC without leaking a feed definition', async () => {
    // ical.js prefers the timezone defined in the calendar being parsed, so a
    // (non-compliant) feed definition only affects that feed...
    const withOwnDefinition = await firstStart(zonedFeed('UTC', { offset: '+0500' }));
    assert.equal(withOwnDefinition, Date.UTC(2026, 8, 15, 5));

    // ...and the built-in UTC stays authoritative for every other feed.
    const builtin = await firstStart(zonedFeed('UTC', { define: false }));
    assert.equal(builtin, Date.UTC(2026, 8, 15, 10));
  });

  it('does not let a rejected feed seed the registry', async () => {
    const rejected = `BEGIN:VCALENDAR
VERSION:2.0
${vtimezone('Review05/Zone', '+0300')}
BEGIN:VEVENT
UID:zone-event
DTSTAMP:20240101T000000Z
DTSTART;TZID=Review05/Other:20260915T100000
END:VEVENT
END:VCALENDAR`;

    await assert.rejects(new IcalJsCalendarParser().parse(rejected, opts), CalendarIntegrityError);
    await assert.rejects(
      new IcalJsCalendarParser().parse(zonedFeed('Review05/Zone', { define: false }), opts),
      CalendarIntegrityError,
    );
  });

  it('is stable across parser instances, repetition and concurrent parses', async () => {
    const parserA = new IcalJsCalendarParser();
    const parserB = new IcalJsCalendarParser();
    const plus3 = zonedFeed('Review05/Zone', { offset: '+0300' });
    const plus5 = zonedFeed('Review05/Zone', { offset: '+0500' });

    const starts = await Promise.all([
      firstStart(plus3, parserA),
      firstStart(plus5, parserA),
      firstStart(plus3, parserB),
      firstStart(plus5, parserB),
    ]);

    assert.deepEqual(starts, [
      Date.UTC(2026, 8, 15, 7),
      Date.UTC(2026, 8, 15, 5),
      Date.UTC(2026, 8, 15, 7),
      Date.UTC(2026, 8, 15, 5),
    ]);
  });

  it('rejects an undefined zone during sync and keeps the previous snapshot', async () => {
    const harness = createHarness({ now: Date.UTC(2026, 8, 1) });
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    const parser = new IcalJsCalendarParser();

    harness.setHandler(() => textResponse(zonedFeed('Review05/Zone', { offset: '+0300' }), 200));
    const applied = await runSourceSync(syncDeps(harness, { parser }), TEST_SOURCE, 'owner-1');
    assert.equal(applied.status, 'applied');
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);

    harness.setHandler(() => textResponse(zonedFeed('Review05/Zone', { define: false }), 200));
    expireSourceLease(harness);
    const rejected = await runSourceSync(syncDeps(harness, { parser }), TEST_SOURCE, 'owner-2');
    assert.equal(rejected.status, 'rejected');
    assert.equal(rejected.reason, 'CalendarIntegrityError');
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'),
      1,
      'the previous snapshot survives an invalid-zone rejection',
    );
  });
});

describe('pacing never sends after the event starts', () => {
  async function fillPaceWindow(harness: Harness, now: number): Promise<void> {
    for (let index = 0; index < BOT_SEND_PACE_PER_SECOND; index += 1) {
      await harness.repository.acquireSendSlot(now, BOT_SEND_PACE_PER_SECOND);
    }
  }

  it('expires instead of sending when the pace wait outlives the start', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [111], {
      occurrenceKey: 'pace#1',
      startsAtMs: now + 500,
    });
    await fillPaceWindow(harness, now);
    harness.db.stats.reset();

    const summary = await processQueueBatch([makeQueueMessage(jobId)], consumerDeps(harness));

    assert.equal(harness.clock.now(), now + 1_000, 'the consumer waited out the pace window');
    assert.equal(summary.sent, 0);
    assert.equal(summary.terminal, 1);
    assert.equal(harness.fetchSpy.calls.length, 0, 'no Telegram send may happen after the start');
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.status, 'failed');
    assert.equal(job?.last_error_code, 'expired');
  });

  it('expires when the pace wait lands exactly on the start', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [111], {
      occurrenceKey: 'pace#1',
      startsAtMs: now + 1_000,
    });
    await fillPaceWindow(harness, now);

    const summary = await processQueueBatch([makeQueueMessage(jobId)], consumerDeps(harness));

    assert.equal(harness.clock.now(), now + 1_000);
    assert.equal(summary.terminal, 1);
    assert.equal(harness.fetchSpy.calls.length, 0);
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'expired');
  });

  it('still sends when the pace wait leaves the reminder eligible', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [111], {
      occurrenceKey: 'pace#1',
      startsAtMs: now + 1_500,
    });
    await fillPaceWindow(harness, now);

    const summary = await processQueueBatch([makeQueueMessage(jobId)], consumerDeps(harness));

    assert.equal(harness.clock.now(), now + 1_000);
    assert.equal(summary.sent, 1);
    assert.equal(harness.fetchSpy.calls.length, 1);
    assert.equal((await harness.repository.getJob(jobId))?.status, 'sent');
  });
});

describe('outcome timestamps start when the response arrives', () => {
  it('starts the cooldown when the slow 429 response arrives', async () => {
    const harness = createHarness();
    const startedAt = harness.clock.now();
    // Seven jobs run concurrently, so a start can begin before the slow 429
    // response is durable; the cooldown must block every later start.
    const jobIds = await seedDueJobs(harness, 7, {
      occurrenceKey: 'slow#1',
      startsAtMs: startedAt + 10 * MS_PER_MINUTE,
    });
    let calls = 0;
    const starts: number[] = [];
    harness.setHandler(() => {
      calls += 1;
      starts.push(harness.clock.now());
      if (calls === 1) {
        harness.clock.advance(2_000);
        return jsonResponse({ ok: false, parameters: { retry_after: 60 } }, 429);
      }
      return jsonResponse({ ok: true, result: { message_id: calls } });
    });

    const messages = jobIds.map((jobId) => makeQueueMessage(jobId));
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    // Measured: only the starts that beat the slow 429 to the cooldown reached
    // the API; every later message was rescheduled without a request. The
    // exact split is a deterministic race between the slow response and the
    // waiting tasks, so the test pins the current interleave while the
    // invariants below carry the specified behavior.
    assert.equal(calls, 4, 'the recorded cooldown blocks every later start');
    assert.equal(summary.sent, 3);
    assert.equal(summary.retried, 4);
    assert.equal(summary.deferred, 0);
    assert.ok(messages.every((message) => message.acked));

    const pace = await harness.repository.getPaceState();
    assert.equal(
      pace?.cooldownUntilMs,
      startedAt + 2_000 + 60_000,
      'retry_after counts from the response, not from the request start',
    );
    assert.equal(
      findPacingViolations(
        starts.map((atMs) => ({ atMs })),
        BOT_SEND_PACE_PER_SECOND,
        SEND_PACE_WINDOW_MS,
      ),
      0,
      'the starts that beat the cooldown still hold the rolling pace',
    );
    assert.ok(
      starts.every((startedMs) => startedMs < (pace?.cooldownUntilMs ?? 0)),
      'no request starts once the cooldown is active',
    );

    const rateLimited = harness.db.database
      .prepare("SELECT next_attempt_at_ms FROM outbound_jobs WHERE last_error_code = 'rate_limited'")
      .all() as { next_attempt_at_ms: unknown }[];
    assert.equal(rateLimited.length, 1);
    assert.equal(
      Number(rateLimited[0]?.next_attempt_at_ms),
      startedAt + 2_000 + 60_000,
      'the slow 429 reschedules its own job to the response-time cooldown',
    );

    const paced = harness.db.database
      .prepare("SELECT status, next_attempt_at_ms FROM outbound_jobs WHERE last_error_code = 'paced'")
      .all() as { status: string; next_attempt_at_ms: unknown }[];
    assert.equal(paced.length, 3, 'the messages behind the cooldown are rescheduled');
    for (const row of paced) {
      assert.equal(row.status, 'pending');
      assert.equal(Number(row.next_attempt_at_ms), startedAt + 2_000 + PACE_RETRY_DELAY_MS);
    }
  });

  it('schedules transient backoff from the response time', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, 1, {
      occurrenceKey: 'slow#1',
      startsAtMs: harness.clock.now() + 10 * MS_PER_MINUTE,
    });
    const startedAt = harness.clock.now();
    harness.setHandler(() => {
      harness.clock.advance(3_000);
      return jsonResponse({ ok: false }, 500);
    });

    const summary = await processQueueBatch([makeQueueMessage(jobId)], consumerDeps(harness));

    assert.equal(summary.retried, 1);
    const job = await harness.repository.getJob(jobId);
    assert.equal(Number(job?.updated_at_ms), startedAt + 3_000);
    assert.ok(
      Number(job?.next_attempt_at_ms) >= startedAt + 3_000 + JOB_BACKOFF_BASE_MS,
      'backoff is measured from the response',
    );
  });

  it('persists a successful completion at the response time', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, 1, {
      occurrenceKey: 'slow#1',
      startsAtMs: harness.clock.now() + 10 * MS_PER_MINUTE,
    });
    const startedAt = harness.clock.now();
    harness.setHandler(() => {
      harness.clock.advance(2_500);
      return jsonResponse({ ok: true, result: { message_id: 1 } });
    });

    const summary = await processQueueBatch([makeQueueMessage(jobId)], consumerDeps(harness));

    assert.equal(summary.sent, 1);
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.status, 'sent');
    assert.equal(Number(job?.updated_at_ms), startedAt + 2_500);
  });

  it('uses the retry response time for a slow command rate-limit as well', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.activateUser(5, 5, now);
    await harness.repository.insertCommandJob({
      id: 'cmd-1',
      telegramUserId: 5,
      chatId: 5,
      payloadJson: JSON.stringify({ text: 'menu' }),
      dedupKey: 'cmd-1',
      sendAtMs: now,
      now,
      expectedRevision: 1,
    });
    let calls = 0;
    harness.setHandler(() => {
      calls += 1;
      harness.clock.advance(2_000);
      return jsonResponse({ ok: false, parameters: { retry_after: 2 } }, 429);
    });

    const summary = await processQueueBatch([makeQueueMessage('cmd-1')], consumerDeps(harness));

    assert.equal(summary.retried, 1);
    assert.equal(calls, 2, 'a short rate limit gets one in-place retried call');
    const job = await harness.repository.getJob('cmd-1');
    assert.equal(
      Number(job?.next_attempt_at_ms),
      now + 8_000,
      'retry_after counts from the second response (now+6000), not the first',
    );
    assert.equal(Number(job?.updated_at_ms), now + 6_000, 'the second response time is persisted');
    assert.equal(Number(job?.attempt_count), 2, 'both started calls are counted');
    assert.equal((await harness.repository.getPaceState())?.cooldownUntilMs, now + 8_000);
  });

  it('keeps a paced command reschedule inside the retry delay', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.activateUser(6, 6, now);
    await harness.repository.insertCommandJob({
      id: 'cmd-2',
      telegramUserId: 6,
      chatId: 6,
      payloadJson: JSON.stringify({ text: 'menu' }),
      dedupKey: 'cmd-2',
      sendAtMs: now,
      now,
      expectedRevision: 1,
    });
    await harness.repository.setSendCooldown(now + 5_000, now);

    const summary = await processQueueBatch([makeQueueMessage('cmd-2')], consumerDeps(harness));

    assert.equal(summary.retried, 1);
    assert.equal(harness.fetchSpy.calls.length, 0);
    const job = await harness.repository.getJob('cmd-2');
    assert.equal(job?.last_error_code, 'paced');
    assert.equal(Number(job?.next_attempt_at_ms), now + PACE_RETRY_DELAY_MS);
  });
});
