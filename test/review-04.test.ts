/**
 * Regression tests for the four defects confirmed by the follow-up review:
 * atomic snapshot publication, pacing plus 403 budget, bounded fetch
 * cancellation, and RANGE=THISANDFUTURE misinterpretation.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  StatementBudgetError,
  type DeletionHorizon,
  type OccurrenceWrite,
  type SnapshotPublication,
  type SourceLease,
} from '../src/data/repository.ts';
import { IcalJsCalendarParser } from '../src/calendar/icaljs-parser.ts';
import { runSourceSync } from '../src/calendar/sync.ts';
import { runSchedulerTick } from '../src/scheduler/tick.ts';
import { processQueueBatch, processQueueMessage } from '../src/queue/consumer.ts';
import {
  buildOccurrences,
  UnsupportedRecurrenceError,
  type Occurrence,
} from '../src/domain/calendar.ts';
import {
  BUDGET_DEFER_RETRY_DELAY_SECONDS,
  EXPANSION_HORIZON_MS,
  JOB_LEASE_MS,
  MAX_D1_STATEMENTS_PER_CONSUMER,
  MESSAGE_BUDGET_FLOOR,
  MS_PER_MINUTE,
  SOURCE_LEASE_MS,
} from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { jsonResponse, textResponse } from './helpers/fakes.ts';
import {
  consumerDeps,
  createHarness,
  schedulerDeps,
  syncDeps,
  type Harness,
} from './helpers/harness.ts';
import {
  EMPTY_ICS,
  expireSourceLease,
  makeQueueMessage,
  occurrence,
  onboardUser,
  parsedEvent,
  seedDueJobs,
  seedReminderOffsets,
  seedSource,
  TEST_SOURCE,
} from './helpers/seed.ts';
import { stalledResponse } from './helpers/streams.ts';

const MAX_BOUND_PARAMS = 100;

function writesFor(harness: Harness, count: number): OccurrenceWrite[] {
  const now = harness.clock.now();
  return Array.from({ length: count }, (_, index) =>
    occurrence({ occurrenceKey: `w-${index}`, startsAtMs: now + MS_PER_MINUTE + index }),
  );
}

const NO_CURSOR = { etag: null, lastModified: null };

/** Number of outbound jobs in the given durable status. */
function countJobStatus(harness: Harness, status: string): number {
  const row = harness.db.database
    .prepare('SELECT COUNT(*) AS n FROM outbound_jobs WHERE status = ?')
    .get(status) as { n: number };
  return Number(row.n);
}

/** Publishes through the real custody-checked repository API with a fixture lease. */
async function publish(
  harness: Harness,
  sourceId: string,
  writes: readonly OccurrenceWrite[],
  now: number,
  horizon: DeletionHorizon = { startMs: now, endMs: now + EXPANSION_HORIZON_MS },
): Promise<SnapshotPublication> {
  const lease = await harness.repository.acquireSourceLease(
    sourceId,
    `fixture-${sourceId}`,
    now,
    SOURCE_LEASE_MS,
  );
  assert.notEqual(lease, null);
  const published = await harness.repository.applySnapshot(
    sourceId,
    writes,
    lease as SourceLease,
    NO_CURSOR,
    () => now,
    horizon,
  );
  assert.notEqual(published, null, 'the fixture lease must still be live');
  return published as SnapshotPublication;
}

describe('atomic snapshot publication', () => {
  it('rejects an oversized publication without touching the visible snapshot', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await publish(
      harness,
      'basic',
      [occurrence({ occurrenceKey: 'seed', startsAtMs: now + MS_PER_MINUTE })],
      now,
    );

    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    await assert.rejects(
      publish(harness, 'basic', writesFor(harness, 360), now),
      StatementBudgetError,
    );

    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrence_staging'), 0);
  });

  it('publishes a snapshot atomically even when staging crosses a batch boundary', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    harness.repository.beginInvocation(400);

    // 360 writes need 60 staging statements, which span two internal batches.
    await publish(harness, 'basic', writesFor(harness, 360), now);

    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 360);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrence_staging'), 0);
  });

  it('keeps the previous snapshot when the publication SQL fails', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await publish(
      harness,
      'basic',
      [
        occurrence({ occurrenceKey: 'keep', startsAtMs: now + MS_PER_MINUTE, summary: 'Same' }),
        occurrence({ occurrenceKey: 'drop', startsAtMs: now + 2 * MS_PER_MINUTE }),
      ],
      now,
    );

    // Staging succeeds; the atomic publish (upsert + delete + clear) fails
    // halfway and must roll back as a whole.
    harness.db.exec(
      `CREATE TRIGGER review_publish_failure BEFORE INSERT ON occurrences
       WHEN NEW.summary = 'Boom'
       BEGIN SELECT RAISE(ABORT, 'publish failure'); END`,
    );
    const replacement = [
      occurrence({ occurrenceKey: 'keep', startsAtMs: now + MS_PER_MINUTE, summary: 'Same' }),
      occurrence({ occurrenceKey: 'boom', startsAtMs: now + 3 * MS_PER_MINUTE, summary: 'Boom' }),
    ];
    await assert.rejects(publish(harness, 'basic', replacement, now));

    const surviving = harness.db.database
      .prepare('SELECT id, revision FROM occurrences ORDER BY id')
      .all() as { id: string; revision: number }[];
    assert.deepEqual(
      surviving.map((row) => [row.id, Number(row.revision)]),
      [
        ['basic:drop', 1],
        ['basic:keep', 1],
      ],
      'a failed publish must apply neither its upserts nor its deletions',
    );

    harness.db.exec('DROP TRIGGER review_publish_failure');
    await publish(harness, 'basic', replacement, now);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 2);
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrences WHERE id = 'basic:drop'"),
      0,
      'the retried publication applies the deletion',
    );
  });

  it('preserves revisions for unchanged rows and removes disappeared ones', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await publish(
      harness,
      'basic',
      [
        occurrence({ occurrenceKey: 'keep', startsAtMs: now + MS_PER_MINUTE, summary: 'Same' }),
        occurrence({ occurrenceKey: 'change', startsAtMs: now + 2 * MS_PER_MINUTE, summary: 'Before' }),
        occurrence({ occurrenceKey: 'drop', startsAtMs: now + 3 * MS_PER_MINUTE }),
      ],
      now,
    );

    await publish(
      harness,
      'basic',
      [
        occurrence({ occurrenceKey: 'keep', startsAtMs: now + MS_PER_MINUTE, summary: 'Same' }),
        occurrence({ occurrenceKey: 'change', startsAtMs: now + 2 * MS_PER_MINUTE, summary: 'After' }),
        occurrence({ occurrenceKey: 'new', startsAtMs: now + 4 * MS_PER_MINUTE }),
      ],
      now,
    );

    const revisions = harness.db.database
      .prepare('SELECT id, revision FROM occurrences ORDER BY id')
      .all() as { id: string; revision: number }[];
    const byId = new Map(revisions.map((row) => [row.id, Number(row.revision)]));
    assert.equal(byId.get('basic:keep'), 1, 'unchanged rows keep their revision');
    assert.equal(byId.get('basic:change'), 2, 'materially changed rows increment');
    assert.equal(byId.get('basic:new'), 1, 'new rows start at revision 1');
    assert.equal(byId.has('basic:drop'), false, 'disappeared rows are removed');
  });

  it('defers a sync that cannot afford its publication', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await publish(
      harness,
      'basic',
      [occurrence({ occurrenceKey: 'keep', startsAtMs: now + MS_PER_MINUTE })],
      now,
    );
    expireSourceLease(harness);

    const events = Array.from({ length: 60 }, (_, index) =>
      parsedEvent(now + (index + 1) * MS_PER_MINUTE, { uid: `c-${index}` }),
    );
    harness.setParser({ events });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));

    harness.repository.beginInvocation(12);
    const result = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-1');

    assert.equal(result.status, 'skipped');
    assert.equal(result.reason, 'budget');
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
  });

  it('still plans and enqueues when a sync defers on the budget', async () => {
    const harness = createHarness({ now: Date.UTC(2023, 10, 12, 21) });
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.activateUser(7, 7, now);
    onboardUser(harness, 7);
    seedReminderOffsets(harness, 7, [30]);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'due', startsAtMs: now + MS_PER_MINUTE })],
      now,
    );

    const events = Array.from({ length: 200 }, (_, index) =>
      parsedEvent(now + (index + 1) * MS_PER_MINUTE, { uid: `bulk-${index}` }),
    );
    harness.setParser({ events });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));

    const result = await runSchedulerTick(schedulerDeps(harness, { sources: [TEST_SOURCE] }));

    assert.deepEqual(result.syncStatuses, ['skipped']);
    assert.equal(result.enqueued, 1, 'the tail still plans and enqueues');
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'),
      1,
      'the deferred snapshot is not partially applied',
    );
    assert.ok(
      harness.repository.statementsUsed() <= 50,
      `tick used ${harness.repository.statementsUsed()} statements`,
    );
  });
});

describe('consumer budget with a non-empty rate state', () => {
  it('completes a paced 403 batch without a budget exception', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 10);
    const now = harness.clock.now();
    for (let index = 0; index < 20; index += 1) {
      await harness.repository.acquireSendSlot(now, 20);
    }
    harness.setHandler(() => jsonResponse({ ok: false, description: 'Forbidden' }, 403));

    const messages = jobIds.map(makeQueueMessage);
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    // Rolling admission with the ten-statement worst-case floor: seven
    // waited terminals complete (each waits out the prefilled window, then
    // spends claim + waited slot triple + reservation + deactivation +
    // terminal) and the three-message remainder defers (measured).
    assert.equal(summary.terminal, 7);
    assert.equal(summary.retried, 0);
    assert.equal(summary.deferred, 3);
    assert.equal(harness.repository.statementsUsed(), 44, 'prune plus seven waited terminals');
    assert.equal(harness.fetchSpy.calls.length, 7);
    assert.ok(
      messages.slice(0, 7).every((message) => message.acked),
      'every admitted task is acknowledged',
    );
    assert.ok(
      messages.slice(7).every(
        (message) =>
          message.retried && message.retryDelaySeconds === BUDGET_DEFER_RETRY_DELAY_SECONDS,
      ),
      'the unaffordable remainder is retried, never acknowledged as handled',
    );
    assert.ok(harness.db.stats.maxParamsPerStatement <= MAX_BOUND_PARAMS);

    // The deferred tail was never claimed, so it is still the scheduler's
    // reservation and is repaired, not lost.
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM outbound_jobs WHERE status = 'enqueued'"),
      3,
    );
    harness.clock.advance(JOB_LEASE_MS + 1);
    assert.equal(await harness.repository.repairExpiredLeases(harness.clock.now()), 3);
    const reclaimed = await harness.repository.claimDueJobs(
      'scheduler',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.equal(reclaimed.length, 3, 'deferred work is re-enqueued by lease repair');
  });

  it('keeps a mixed paced batch with every outcome inside the budget', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 10);
    const now = harness.clock.now();
    for (let index = 0; index < 18; index += 1) {
      await harness.repository.acquireSendSlot(now, 20);
    }
    let batch = 0;
    harness.setHandler(() => {
      batch += 1;
      if (batch <= 4) {
        return jsonResponse({ ok: false }, 403);
      }
      if (batch === 5) {
        return jsonResponse({ ok: false, parameters: { retry_after: 5 } }, 429);
      }
      return jsonResponse({ ok: true, result: { message_id: batch } });
    });

    const messages = jobIds.map(makeQueueMessage);
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    // Measured outcome with the ten-statement floor: four 403 terminals, one
    // 429 and one paced success are admitted (the recorded 5s cooldown blocks
    // further starts), and the two-message tail no remaining reserve covers is
    // deferred. The rest of the success outcome is proven by the second wave
    // below, once the cooldown has expired.
    assert.equal(summary.sent, 1);
    assert.equal(summary.terminal, 4);
    assert.equal(summary.retried, 3);
    assert.equal(summary.deferred, 2);
    assert.equal(summary.processed + summary.deferred, 10, 'every message is accounted for');
    assert.equal(harness.repository.statementsUsed(), 43);
    assert.equal(harness.fetchSpy.calls.length, 6, 'the recorded cooldown blocks later starts');
    assert.equal(countJobStatus(harness, 'cancelled'), 4);
    assert.equal(countJobStatus(harness, 'pending'), 3);
    assert.equal(countJobStatus(harness, 'sent'), 1);
    assert.equal(countJobStatus(harness, 'enqueued'), 2);
    assert.equal(messages.filter((message) => message.acked).length, 8);
    const deferred = messages.filter((message) => message.retried);
    assert.equal(deferred.length, 2);
    assert.ok(
      deferred.every(
        (message) => message.retryDelaySeconds === BUDGET_DEFER_RETRY_DELAY_SECONDS,
      ),
      'only the unaffordable tail is Queue-retried with the deferral delay',
    );
    assert.ok(
      harness.repository.statementsUsed() <= MAX_D1_STATEMENTS_PER_CONSUMER,
      `used ${harness.repository.statementsUsed()} statements`,
    );

    // Second wave: the cooldown has expired, so the deferred tail sends.
    harness.clock.advance(6_000);
    harness.setHandler(() => jsonResponse({ ok: true, result: { message_id: 7 } }));
    const deferredIndices = messages.flatMap((message, index) => (message.retried ? [index] : []));
    const redelivery = deferredIndices.map((index) => makeQueueMessage(jobIds[index] ?? ''));
    const second = await processQueueBatch(redelivery, consumerDeps(harness));
    assert.equal(second.sent, 2, 'the deferred tail sends once the cooldown expired');
    assert.ok(redelivery.every((message) => message.acked));
    assert.equal(countJobStatus(harness, 'sent'), 3);
  });

  it('defers safely when the remaining budget cannot start a message', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 1);
    const jobId = jobIds[0] ?? '';

    harness.repository.beginInvocation(MESSAGE_BUDGET_FLOOR - 1);
    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'deferred');
    assert.equal(harness.repository.statementsUsed(), 0, 'a deferral executes no statements');
    assert.equal(
      (await harness.repository.getJob(jobId))?.status,
      'enqueued',
      'a deferred job stays recoverable for the next tick',
    );

    // The scheduler repairs the reservation and re-enqueues it.
    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    harness.clock.advance(JOB_LEASE_MS + 1);
    assert.equal(await harness.repository.repairExpiredLeases(harness.clock.now()), 1);
    const reclaimed = await harness.repository.claimDueJobs(
      'scheduler',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.equal(reclaimed.length, 1);
  });

  it('defers only the remainder no reserve covers and leaves its jobs recoverable', async () => {
    const harness = createHarness();
    // Thirteen unpaced 403s, five statements each: rolling admission with the
    // ten-statement floor runs eight of them (41 statements with the batch
    // prune) while the remaining budget cannot reserve a ninth task, so
    // exactly the tail defers.
    const jobIds = await seedDueJobs(harness, 13);
    harness.setHandler(() => jsonResponse({ ok: false, description: 'Forbidden' }, 403));
    harness.db.stats.reset();

    const messages = jobIds.map(makeQueueMessage);
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    assert.equal(summary.terminal, 8);
    assert.equal(summary.deferred, 5);
    assert.equal(
      harness.repository.statementsUsed(),
      41,
      'prune plus eight five-statement terminals; the ninth reserve would pass the limit',
    );
    assert.equal(harness.fetchSpy.calls.length, 8);
    assert.equal(messages.filter((message) => message.acked).length, 8);
    const deferred = messages.filter((message) => message.retried);
    assert.equal(deferred.length, 5);
    assert.ok(
      deferred.every(
        (message) => message.retryDelaySeconds === BUDGET_DEFER_RETRY_DELAY_SECONDS,
      ),
      'the unaffordable tail is retried, never acknowledged as handled',
    );
    assert.ok(harness.db.stats.maxParamsPerStatement <= MAX_BOUND_PARAMS);

    // The next invocation (tick or redelivery) has its own budget.
    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    const deferredIndices = messages.flatMap((message, index) => (message.retried ? [index] : []));
    for (const index of deferredIndices) {
      assert.equal(
        (await harness.repository.getJob(jobIds[index] ?? ''))?.status,
        'enqueued',
        'a deferred job stays recoverable for the next tick',
      );
    }

    // Lease repair returns the untouched tail to the pending pool.
    harness.clock.advance(JOB_LEASE_MS + 1);
    assert.equal(await harness.repository.repairExpiredLeases(harness.clock.now()), 5);
    const reclaimed = await harness.repository.claimDueJobs(
      'scheduler',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.equal(reclaimed.length, 5, 'the deferred tail is re-enqueued, not lost');
  });
});

describe('bounded fetch cancellation', () => {
  it('invokes cancellation when a stalled read hits the deadline', async () => {
    const harness = createHarness();
    let cancelled = false;
    harness.setHandler(() =>
      stalledResponse({
        onCancel: () => {
          cancelled = true;
        },
      }),
    );

    const result = await runSourceSync(syncDeps(harness, { fetchTimeoutMs: 20 }), TEST_SOURCE, 'owner-1');

    assert.equal(result.status, 'error');
    assert.equal(result.reason, 'AbortError');
    assert.equal(cancelled, true, 'cancellation must be initiated');
  });

  it('settles when the cancellation promise never resolves', async () => {
    const harness = createHarness();
    harness.setHandler(() =>
      new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new Uint8Array(9 * 1024 * 1024));
          },
          cancel() {
            return new Promise(() => {});
          },
        }),
      ),
    );

    const settled = await Promise.race([
      runSourceSync(syncDeps(harness, { fetchTimeoutMs: 20 }), TEST_SOURCE, 'owner-1').then(
        (result) => `settled:${result.status}:${result.reason}`,
      ),
      new Promise((resolve) => setTimeout(() => resolve('pending'), 250)),
    ]);

    assert.equal(settled, 'settled:rejected:truncated');
    assert.equal((await harness.repository.getSource('basic'))?.status, 'error');
  });

  it('keeps the previous snapshot and marks the source failed', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await publish(
      harness,
      'basic',
      [occurrence({ occurrenceKey: 'keep', startsAtMs: now + MS_PER_MINUTE })],
      now,
    );
    expireSourceLease(harness);

    let fail = false;
    harness.setHandler(() =>
      fail
        ? new Response(
            new ReadableStream<Uint8Array>({
              pull() {
                return new Promise(() => {});
              },
              cancel() {
                /* never settles */
              },
            }),
          )
        : textResponse(EMPTY_ICS, 200),
    );
    fail = true;

    const result = await runSourceSync(syncDeps(harness, { fetchTimeoutMs: 20 }), TEST_SOURCE, 'owner-1');
    assert.equal(result.status, 'error');
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
    assert.equal((await harness.repository.getSource('basic'))?.status, 'error');
  });

  it('lets the scheduler finish its other work when one source never cancels', async () => {
    const harness = createHarness({ now: Date.UTC(2023, 10, 12, 21) });
    const now = harness.clock.now();
    const events = [parsedEvent(now + MS_PER_MINUTE, { uid: 'healthy-1' })];
    harness.setParser({ events });
    harness.setHandler((url) =>
      url.includes('broken')
        ? new Response(
            new ReadableStream<Uint8Array>({
              pull() {
                return new Promise(() => {});
              },
              cancel() {
                return new Promise(() => {});
              },
            }),
          )
        : textResponse(EMPTY_ICS, 200),
    );

    const result = await runSchedulerTick(
      schedulerDeps(harness, {
        fetchTimeoutMs: 20,
        sources: [
          { id: 'broken', kind: 'basic', url: 'https://example.test/broken.ics' },
          { id: 'basic', kind: 'extended', url: 'https://example.test/basic.ics' },
        ],
      }),
    );

    assert.equal(result.syncStatuses[0], 'error');
    assert.equal(result.syncStatuses[1], 'applied');
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
    assert.ok(
      harness.repository.statementsUsed() <= 50,
      `tick used ${harness.repository.statementsUsed()} statements`,
    );
  });
});

describe('this-and-future recurrence rejection', () => {
  const opts = {
    sourceTimeZone: 'Europe/Moscow',
    horizonStartMs: Date.UTC(2026, 8, 14),
    horizonEndMs: Date.UTC(2026, 8, 30),
    maxIterations: 100,
  };

  function icsWith(body: string): string {
    return `BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//test//EN
${body}
END:VCALENDAR`;
  }

  async function parseAndNormalize(ics: string): Promise<Occurrence[]> {
    const parsed = await new IcalJsCalendarParser().parse(ics, opts);
    return buildOccurrences(parsed, {
      horizonStartMs: opts.horizonStartMs,
      horizonEndMs: opts.horizonEndMs,
    });
  }

  it('rejects RANGE=THISANDFUTURE instead of silently keeping old dates', async () => {
    const ics = icsWith(`BEGIN:VEVENT
UID:range-test
DTSTAMP:20240101T000000Z
DTSTART:20260914T100000Z
RRULE:FREQ=DAILY;COUNT=3
SUMMARY:Master
END:VEVENT
BEGIN:VEVENT
UID:range-test
RECURRENCE-ID;RANGE=THISANDFUTURE:20260915T100000Z
DTSTAMP:20240101T000000Z
DTSTART:20260915T120000Z
SUMMARY:Shifted
END:VEVENT`);

    await assert.rejects(parseAndNormalize(ics), UnsupportedRecurrenceError);
  });

  it('preserves the previous snapshot when a range override is rejected', async () => {
    const harness = createHarness({ now: Date.UTC(2026, 8, 1) });
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await publish(
      harness,
      'basic',
      [occurrence({ occurrenceKey: 'keep', startsAtMs: now + MS_PER_MINUTE })],
      now,
    );
    expireSourceLease(harness);

    harness.setHandler(() =>
      textResponse(
        icsWith(`BEGIN:VEVENT
UID:range-test
DTSTAMP:20240101T000000Z
DTSTART:20260914T100000Z
RRULE:FREQ=DAILY;COUNT=3
END:VEVENT
BEGIN:VEVENT
UID:range-test
RECURRENCE-ID;RANGE=THISANDFUTURE:20260915T100000Z
DTSTAMP:20240101T000000Z
DTSTART:20260915T120000Z
END:VEVENT`),
        200,
      ),
    );

    const result = await runSourceSync(
      syncDeps(harness, { parser: new IcalJsCalendarParser() }),
      TEST_SOURCE,
      'owner-1',
    );

    assert.equal(result.status, 'rejected');
    assert.equal(result.reason, 'UnsupportedRecurrenceError');
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
  });

  it('rejects EXRULE and RRULE RSCALE/SKIP rather than ignoring them', async () => {
    const event = (body: string): string => icsWith(`BEGIN:VEVENT
UID:unsupported
DTSTAMP:20240101T000000Z
DTSTART:20260914T100000Z
${body}
END:VEVENT`);

    await assert.rejects(
      parseAndNormalize(event('RRULE:FREQ=DAILY;COUNT=3\nEXRULE:FREQ=WEEKLY;BYDAY=MO')),
      UnsupportedRecurrenceError,
    );
    await assert.rejects(
      parseAndNormalize(event('RRULE:FREQ=DAILY;COUNT=3;RSCALE=CHINESE')),
      UnsupportedRecurrenceError,
    );
    await assert.rejects(
      parseAndNormalize(event('RRULE:FREQ=DAILY;COUNT=3;RSCALE=CHINESE;SKIP=OMIT')),
      UnsupportedRecurrenceError,
    );
  });

  it('still expands an ordinary override with a default or explicit THIS range', async () => {
    for (const range of ['', ';RANGE=THIS']) {
      const occurrences = await parseAndNormalize(
        icsWith(`BEGIN:VEVENT
UID:plain
DTSTAMP:20240101T000000Z
DTSTART:20260914T100000Z
RRULE:FREQ=DAILY;COUNT=3
SUMMARY:Master
END:VEVENT
BEGIN:VEVENT
UID:plain
RECURRENCE-ID${range}:20260915T100000Z
DTSTAMP:20240101T000000Z
DTSTART:20260915T120000Z
SUMMARY:Moved
END:VEVENT`),
      );
      assert.deepEqual(
        occurrences.map((occurrence) => [
          occurrence.occurrenceKey,
          occurrence.startsAtMs,
          occurrence.summary,
        ]),
        [
          [`plain#${Date.UTC(2026, 8, 14, 10)}`, Date.UTC(2026, 8, 14, 10), 'Master'],
          [`plain#${Date.UTC(2026, 8, 15, 10)}`, Date.UTC(2026, 8, 15, 12), 'Moved'],
          [`plain#${Date.UTC(2026, 8, 16, 10)}`, Date.UTC(2026, 8, 16, 10), 'Master'],
        ],
        `range "${range || 'default'}" keeps stable origins and applies the move in place`,
      );
    }
  });
});
