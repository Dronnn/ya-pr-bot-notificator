/**
 * Finding 4 (handlers): durable per-user/chat ordering for command replies,
 * independent of subscription existence.
 *
 * Uses the real repository + real consumer. Queue delivery order is never
 * assumed: /stop then /start and /start then /stop are each processed in BOTH
 * queue orders and only the current guidance may survive. Covers missing-user
 * commands, stale callbacks, duplicate updates, concurrent processing,
 * claimed-old-job races and NULL-revision first-contact ordering via update_id.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { Repository } from '../src/data/repository.ts';
import { processQueueMessage } from '../src/queue/consumer.ts';
import { handleUpdate, type HandlerDeps } from '../src/telegram/handlers.ts';
import { START_TEXT, STOPPED_TEXT } from '../src/telegram/replies.ts';
import {
  parseUpdate,
  type ParsedUpdate,
  type PrivateCallbackUpdate,
  type PrivateMessageUpdate,
} from '../src/telegram/updates.ts';
import { JOB_LEASE_MS } from '../src/util.ts';
import { consumerDeps, createHarness, type Harness } from './helpers/harness.ts';
import { createStatementBarrier, makeQueueMessage } from './helpers/seed.ts';

const USER_ID = 4242;

function handlerDeps(harness: Harness): HandlerDeps {
  return {
    repository: harness.repository,
    telegram: harness.telegram,
    queue: harness.queue.producer,
    now: harness.now,
    logger: harness.logger.logger,
    idFactory: harness.idFactory,
    eventsLimit: 10,
  };
}

function parseOrFail(payload: unknown): ParsedUpdate {
  const parsed = parseUpdate(payload);
  if (!parsed.ok) {
    throw new Error(`expected parsed update, got ${parsed.reason}`);
  }
  return parsed.update;
}

function messageUpdate(updateId: number, text: string): PrivateMessageUpdate {
  const update = parseOrFail({
    update_id: updateId,
    message: {
      message_id: updateId,
      chat: { id: USER_ID, type: 'private' },
      from: { id: USER_ID, username: 'andrew' },
      text,
    },
  });
  if (update.kind !== 'message') {
    throw new Error('expected message');
  }
  return update;
}

function callbackUpdate(updateId: number, data: string): PrivateCallbackUpdate {
  const update = parseOrFail({
    update_id: updateId,
    callback_query: {
      id: `callback-${updateId}`,
      from: { id: USER_ID, username: 'andrew' },
      message: { message_id: updateId, chat: { id: USER_ID, type: 'private' } },
      data,
    },
  });
  if (update.kind !== 'callback') {
    throw new Error('expected callback');
  }
  return update;
}

function sentTexts(harness: Harness): string[] {
  return harness.fetchSpy.calls
    .filter((call) => call.url.includes('/sendMessage'))
    .map((call) => (JSON.parse(String(call.init?.body)) as { text: string }).text);
}

function queuedJobIds(harness: Harness): string[] {
  return harness.queue.batches.flat().map((message) => message.jobId);
}

function jobRow(harness: Harness, jobId: string): Record<string, unknown> {
  return harness.db.database
    .prepare('SELECT * FROM outbound_jobs WHERE id = ?')
    .get(jobId) as Record<string, unknown>;
}

function allJobs(harness: Harness): Record<string, unknown>[] {
  return harness.db.database
    .prepare('SELECT * FROM outbound_jobs ORDER BY created_at_ms ASC, id ASC')
    .all() as Record<string, unknown>[];
}

function callbackAnswerCalls(harness: Harness): number {
  return harness.fetchSpy.calls.filter((call) => call.url.includes('/answerCallbackQuery')).length;
}

/**
 * `getLastCommandUpdate` returns the durable ordering object
 * `{chatId,lastUpdateId}|null`; the consumer screens from the claim-time join
 * (`sourceUpdateId` vs `commandLastSeenUpdateId`) without an extra statement.
 */
async function lastSeenUpdateId(harness: Harness, userId: number): Promise<number | null> {
  const seen = await harness.repository.getLastCommandUpdate(userId);
  if (seen === null) {
    return null;
  }
  assert.equal(typeof seen.lastUpdateId, 'number');
  return seen.lastUpdateId;
}

describe('Audit 16 (F4): command reply ordering survives queue reordering', () => {
  it('/stop then /start: only start guidance survives in both queue orders', async () => {
    for (const order of ['fifo', 'lifo'] as const) {
      const harness = createHarness();
      const deps = handlerDeps(harness);
      await handleUpdate(messageUpdate(1, '/stop'), deps);
      await handleUpdate(messageUpdate(2, '/start'), deps);
      const ids = queuedJobIds(harness);
      assert.equal(ids.length, 2);
      const sequence = order === 'fifo' ? ids : [...ids].reverse();
      for (const jobId of sequence) {
        await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
      }
      const sent = sentTexts(harness);
      assert.equal(sent.length, 1, `order=${order}: exactly one reply may be sent, got ${JSON.stringify(sent)}`);
      assert.match(sent[0] ?? '', /Привет|Настройки|курс/i);
      assert.equal((await harness.repository.getUser(USER_ID))?.active, true);
    }
  });

  it('/start then /stop: only stop confirmation survives in both queue orders', async () => {
    for (const order of ['fifo', 'lifo'] as const) {
      const harness = createHarness();
      const deps = handlerDeps(harness);
      await handleUpdate(messageUpdate(1, '/start'), deps);
      await handleUpdate(messageUpdate(2, '/stop'), deps);
      const ids = queuedJobIds(harness);
      assert.equal(ids.length, 2);
      const sequence = order === 'fifo' ? ids : [...ids].reverse();
      for (const jobId of sequence) {
        await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
      }
      const sent = sentTexts(harness);
      assert.deepEqual(sent, ['Напоминания отключены. Вернуться можно командой /start.'], `order=${order}`);
      assert.equal((await harness.repository.getUser(USER_ID))?.active, false);
    }
  });

  it('missing-user commands are deliverable, ordered and create no subscription', async () => {
    for (const text of ['/events', '/nonsense', '/settings', '/stop']) {
      const harness = createHarness();
      await handleUpdate(messageUpdate(5, text), handlerDeps(harness));
      assert.equal(await harness.repository.getUser(USER_ID), null);
      const ids = queuedJobIds(harness);
      assert.equal(ids.length, 1);
      const row = jobRow(harness, ids[0] ?? '');
      assert.equal(row.kind, 'command');
      assert.ok(
        row.source_update_id === 5,
        `${text}: source_update_id must be 5, got ${String(row.source_update_id)}`,
      );
      const outcome = await processQueueMessage(
        makeQueueMessage(ids[0] ?? ''),
        consumerDeps(harness),
      );
      assert.equal(outcome, 'sent', text);
      assert.equal(await harness.repository.getUser(USER_ID), null);
    }
    // Ordering state advances even without a user row (monotonic max).
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(6, '/events'), deps);
    await handleUpdate(messageUpdate(7, '/stop'), deps);
    assert.equal(await lastSeenUpdateId(harness, USER_ID), 7);
    await handleUpdate(messageUpdate(6, '/events'), deps);
    assert.equal(await lastSeenUpdateId(harness, USER_ID), 7);
  });

  it('stale callbacks never reactivate, create no job and leave ordering alone', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/stop'), deps);
    const before = await harness.repository.getUser(USER_ID);
    const jobsBefore = queuedJobIds(harness).length;
    const lastBefore = await harness.repository.getLastCommandUpdate(USER_ID);

    await handleUpdate(callbackUpdate(3, 'course:basic'), deps);
    assert.deepEqual(await harness.repository.getUser(USER_ID), before);
    assert.equal(queuedJobIds(harness).length, jobsBefore);
    assert.deepEqual(await harness.repository.getLastCommandUpdate(USER_ID), lastBefore);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, false);
  });

  it('duplicate updates: second handling creates no job and bumps no revision', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(10, '/start'), deps);
    const revisionAfterFirst = (await harness.repository.getUser(USER_ID))?.revision;
    const jobsAfterFirst = queuedJobIds(harness).length;
    await handleUpdate(messageUpdate(10, '/start'), deps);
    assert.equal(queuedJobIds(harness).length, jobsAfterFirst);
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, revisionAfterFirst);

    // Duplicate callback: same update_id twice → single job.
    await handleUpdate(callbackUpdate(11, 'rm:menu'), deps);
    const jobsAfterCallback = queuedJobIds(harness).length;
    await handleUpdate(callbackUpdate(11, 'rm:menu'), deps);
    assert.equal(queuedJobIds(harness).length, jobsAfterCallback);
  });

  it('concurrent duplicate processing: one logical job only', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await Promise.all([
      handleUpdate(messageUpdate(20, '/start'), deps),
      handleUpdate(messageUpdate(20, '/start'), deps),
    ]);
    const jobs = harness.db.database
      .prepare("SELECT COUNT(*) AS n FROM outbound_jobs WHERE dedup_key = 'cmd:20'")
      .get() as { n: number };
    assert.equal(jobs.n, 1);
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 1);
  });

  it('claimed-old-job race: leased /start superseded by later /stop', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    const startJobId = queuedJobIds(harness)[0] ?? '';
    const raceOwner = 'race-consumer';
    assert.notEqual(
      await harness.repository.claimJobContext(startJobId, raceOwner, harness.clock.now(), JOB_LEASE_MS),
      null,
    );
    await handleUpdate(messageUpdate(2, '/stop'), deps);
    const stopJobId = queuedJobIds(harness)[1] ?? '';
    const staleOutcome = await processQueueMessage(
      makeQueueMessage(startJobId),
      consumerDeps(harness, { ownerFactory: () => raceOwner }),
    );
    assert.equal(staleOutcome, 'terminal');
    assert.equal(await processQueueMessage(makeQueueMessage(stopJobId), consumerDeps(harness)), 'sent');
    assert.deepEqual(sentTexts(harness), ['Напоминания отключены. Вернуться можно командой /start.']);
  });

  it('NULL-revision first-contact /stop is suppressed by a later /start via update_id', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/stop'), deps);
    const stopJobId = queuedJobIds(harness)[0] ?? '';
    assert.equal(jobRow(harness, stopJobId).expected_revision, null);
    await handleUpdate(messageUpdate(2, '/start'), deps);
    // Later /start cancels the older pending first-contact stop.
    assert.equal(jobRow(harness, stopJobId).status, 'cancelled');
    const startJobId = queuedJobIds(harness)[1] ?? '';
    // Deliver in the adversarial order: old stop first, then start.
    assert.equal(await processQueueMessage(makeQueueMessage(stopJobId), consumerDeps(harness)), 'skipped');
    assert.equal(await processQueueMessage(makeQueueMessage(startJobId), consumerDeps(harness)), 'sent');
    assert.equal(sentTexts(harness).length, 1);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, true);
    // Ordering state reflects the newest update.
    assert.equal(await lastSeenUpdateId(harness, USER_ID), 2);
    // Every command job carries its source update_id.
    assert.equal(jobRow(harness, stopJobId).source_update_id, 1);
    assert.equal(jobRow(harness, startJobId).source_update_id, 2);
  });

  it('leased NULL-revision first-contact /stop is terminal via update_id when /start lands later', async () => {
    // Order-lease scenario: the first-contact /stop has expected_revision NULL
    // so the revision guard passes; handler cancellation only touches
    // pending/enqueued rows, so the leased row survives it. Only the
    // update_id guards (claim-time screening + atomic reservation) can stop
    // the stale send. Real repository + real consumer.
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/stop'), deps);
    const stopJobId = queuedJobIds(harness)[0] ?? '';
    assert.equal(jobRow(harness, stopJobId).expected_revision, null);
    const raceOwner = 'race-consumer';
    assert.notEqual(
      await harness.repository.claimJobContext(stopJobId, raceOwner, harness.clock.now(), JOB_LEASE_MS),
      null,
    );
    await handleUpdate(messageUpdate(2, '/start'), deps);
    const startJobId = queuedJobIds(harness)[1] ?? '';
    // The leased stale row survives handler cancellation.
    assert.equal(jobRow(harness, stopJobId).status, 'leased');
    const staleOutcome = await processQueueMessage(
      makeQueueMessage(stopJobId),
      consumerDeps(harness, { ownerFactory: () => raceOwner }),
    );
    assert.equal(staleOutcome, 'terminal');
    assert.equal(jobRow(harness, stopJobId).status, 'cancelled');
    assert.equal(jobRow(harness, stopJobId).last_error_code, 'superseded');
    assert.equal(sentTexts(harness).length, 0, 'stale leased first-contact reply must never send');
    assert.equal(await processQueueMessage(makeQueueMessage(startJobId), consumerDeps(harness)), 'sent');
    assert.equal(sentTexts(harness).length, 1);
    assert.ok(!sentTexts(harness)[0]?.includes('отключены'));
    assert.equal((await harness.repository.getUser(USER_ID))?.active, true);
  });

  it('recordCommandUpdate never moves the watermark backwards', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.recordCommandUpdate(USER_ID, USER_ID, 10, now);
    assert.equal((await harness.repository.getLastCommandUpdate(USER_ID))?.lastUpdateId, 10);
    // Out-of-order older update: the max stays 10.
    await harness.repository.recordCommandUpdate(USER_ID, USER_ID, 5, now);
    assert.equal((await harness.repository.getLastCommandUpdate(USER_ID))?.lastUpdateId, 10);
    // Duplicate: the max stays 10.
    await harness.repository.recordCommandUpdate(USER_ID, USER_ID, 10, now);
    assert.equal((await harness.repository.getLastCommandUpdate(USER_ID))?.lastUpdateId, 10);
  });

  it('recordCommandUpdate interleavings converge to the maximum seen', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    for (const updateId of [3, 7, 5, 9, 8, 9, 4]) {
      await harness.repository.recordCommandUpdate(USER_ID, USER_ID, updateId, now);
    }
    assert.equal((await harness.repository.getLastCommandUpdate(USER_ID))?.lastUpdateId, 9);
  });
});

describe('Audit 16 (C1): stale update_id never mutates', () => {
  function callbackAnswers(harness: Harness): number {
    return harness.fetchSpy.calls.filter((call) => call.url.includes('/answerCallbackQuery')).length;
  }

  it('msg(10,/start) then cb(5,course:extended): older callback applies nothing', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(10, '/start'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.course, 'basic');
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 1);
    assert.equal(queuedJobIds(harness).length, 1);

    await handleUpdate(callbackUpdate(5, 'course:extended'), deps);

    assert.equal((await harness.repository.getUser(USER_ID))?.course, 'basic');
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 1);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, true);
    assert.equal(queuedJobIds(harness).length, 1);
    assert.equal(jobRow(harness, queuedJobIds(harness)[0] ?? '').status, 'pending');
    assert.equal(callbackAnswers(harness), 0);
    assert.equal(sentTexts(harness).length, 0);
    assert.equal(await lastSeenUpdateId(harness, USER_ID), 10);
  });

  it('msg(10,/start) then msg(5,/stop): older stop deactivates nothing', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(10, '/start'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, true);

    await handleUpdate(messageUpdate(5, '/stop'), deps);

    assert.equal((await harness.repository.getUser(USER_ID))?.active, true);
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 1);
    assert.equal(queuedJobIds(harness).length, 1);
    assert.equal(await lastSeenUpdateId(harness, USER_ID), 10);
  });

  it('same-update retry after a failure still delivers', async () => {
    const harness = createHarness();
    // Fail once between the ordering claim and the job insert: the first
    // attempt records max=10 but leaves no job, so the retry must pass the
    // same-update (>=) claim and still deliver.
    const originalCancel = harness.repository.cancelPendingJobsForUser.bind(harness.repository);
    let failOnce = true;
    harness.repository.cancelPendingJobsForUser = async (
      userId: number,
      now: number,
      commandUpdateId?: number,
    ): Promise<number> => {
      if (failOnce) {
        failOnce = false;
        throw new Error('boom before enqueue');
      }
      return originalCancel(userId, now, commandUpdateId);
    };
    await assert.rejects(handleUpdate(messageUpdate(10, '/start'), handlerDeps(harness)));
    harness.repository.cancelPendingJobsForUser = originalCancel;
    assert.equal(await harness.repository.getUser(USER_ID), null);

    await handleUpdate(messageUpdate(10, '/start'), handlerDeps(harness));
    const row = harness.db.database
      .prepare("SELECT id FROM outbound_jobs WHERE dedup_key = 'cmd:10'")
      .get() as { id: string };
    assert.ok(row?.id);
    assert.equal(await processQueueMessage(makeQueueMessage(row.id), consumerDeps(harness)), 'sent');
    assert.equal(sentTexts(harness).length, 1);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, true);
  });

  it('newer wins regardless of arrival order, both pairs both orders', async () => {
    // Pair A: newer /start(10) vs older /stop(5) → active in both arrival orders.
    for (const order of ['older-first', 'newer-first'] as const) {
      const harness = createHarness();
      const deps = handlerDeps(harness);
      if (order === 'older-first') {
        await handleUpdate(messageUpdate(5, '/stop'), deps);
        await handleUpdate(messageUpdate(10, '/start'), deps);
      } else {
        await handleUpdate(messageUpdate(10, '/start'), deps);
        await handleUpdate(messageUpdate(5, '/stop'), deps);
      }
      assert.equal((await harness.repository.getUser(USER_ID))?.active, true, order);
      assert.equal(await lastSeenUpdateId(harness, USER_ID), 10, order);
    }
    // Pair B: newer /stop(10) vs older /start(5) → inactive in both arrival orders.
    for (const order of ['older-first', 'newer-first'] as const) {
      const harness = createHarness();
      const deps = handlerDeps(harness);
      if (order === 'older-first') {
        await handleUpdate(messageUpdate(5, '/start'), deps);
        await handleUpdate(messageUpdate(10, '/stop'), deps);
        assert.equal((await harness.repository.getUser(USER_ID))?.active, false, order);
      } else {
        await handleUpdate(messageUpdate(10, '/stop'), deps);
        await handleUpdate(messageUpdate(5, '/start'), deps);
        assert.equal(await harness.repository.getUser(USER_ID), null, `${order}: stale /start must not create a user`);
      }
      assert.equal(await lastSeenUpdateId(harness, USER_ID), 10, order);
    }
  });

  it('claimCommandUpdate is monotonic: first contact, retry and stale', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    assert.equal(await harness.repository.claimCommandUpdate(USER_ID, USER_ID, 10, now), true);
    assert.equal(await harness.repository.claimCommandUpdate(USER_ID, USER_ID, 10, now), true);
    assert.equal(await harness.repository.claimCommandUpdate(USER_ID, USER_ID, 5, now), false);
    assert.equal(await harness.repository.claimCommandUpdate(USER_ID, USER_ID, 11, now), true);
    assert.equal((await harness.repository.getLastCommandUpdate(USER_ID))?.lastUpdateId, 11);
  });
});

/**
 * FIX-ORDER: the claim-time freshness gate alone is not enough. A suspended
 * older invocation resumes after a newer command already completed and used to
 * cancel the newer reply, overwrite the newer subscription state and enqueue a
 * reply that the consumer then supersedes - zero replies and the wrong final
 * state. These tests hold the older invocation inside a chosen D1 statement
 * with a shim, complete the newer invocation, then release: the SQL guards and
 * the pre-enqueue re-check must keep the newer command authoritative.
 */
describe('Audit 16 (FIX-ORDER): suspended older invocation cannot overwrite the newer command', () => {
  const CANCEL_MATCH = (sql: string): boolean =>
    sql.includes('UPDATE outbound_jobs') && sql.includes("status IN ('pending', 'enqueued')");

  /** Runs `older` on a statement-barrier repository, `newer` on the real one. */
  async function interleave(
    harness: Harness,
    olderUpdate: PrivateMessageUpdate,
    newerUpdate: PrivateMessageUpdate,
  ): Promise<void> {
    const barrier = createStatementBarrier(harness.db, CANCEL_MATCH);
    const olderDeps: HandlerDeps = {
      ...handlerDeps(harness),
      repository: new Repository(barrier.db),
    };
    const older = handleUpdate(olderUpdate, olderDeps);
    await barrier.reached;
    await handleUpdate(newerUpdate, handlerDeps(harness));
    barrier.release();
    await older;
  }

  it('first contact: /start(10) suspended, /stop(11) completes -> only stop guidance', async () => {
    const harness = createHarness();
    await interleave(harness, messageUpdate(10, '/start'), messageUpdate(11, '/stop'));

    assert.equal(await harness.repository.getUser(USER_ID), null, 'stale /start must not activate');
    const jobs = allJobs(harness);
    assert.equal(jobs.length, 1, 'the stale /start must not enqueue a reply');
    assert.equal(jobs[0]?.dedup_key, 'cmd:11');
    assert.equal(jobs[0]?.source_update_id, 11);
    assert.equal(jobs[0]?.status, 'pending');
    assert.equal(await lastSeenUpdateId(harness, USER_ID), 11);

    assert.equal(await processQueueMessage(makeQueueMessage(String(jobs[0]?.id)), consumerDeps(harness)), 'sent');
    assert.deepEqual(sentTexts(harness), [STOPPED_TEXT], 'exactly the newest guidance is delivered');
  });

  it('active user: /start(10) suspended, /stop(11) completes -> no stale reactivation', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await processQueueMessage(makeQueueMessage(queuedJobIds(harness)[0] ?? ''), consumerDeps(harness));
    assert.equal((await harness.repository.getUser(USER_ID))?.active, true);

    await interleave(harness, messageUpdate(10, '/start'), messageUpdate(11, '/stop'));

    const user = await harness.repository.getUser(USER_ID);
    assert.equal(user?.active, false, 'the newest /stop must win');
    assert.equal(user?.revision, 2, 'the stale /start must not bump the revision');
    const newerJobs = allJobs(harness).filter((job) => Number(job.source_update_id) >= 10);
    assert.deepEqual(
      newerJobs.map((job) => [job.source_update_id, job.status]),
      [[11, 'pending']],
    );
    assert.equal(
      await processQueueMessage(makeQueueMessage(String(newerJobs[0]?.id)), consumerDeps(harness)),
      'sent',
    );
    assert.equal(sentTexts(harness).length, 2);
    assert.equal(sentTexts(harness).at(-1), STOPPED_TEXT);
  });

  it('first contact: /stop(10) suspended, /start(11) completes -> only start guidance', async () => {
    const harness = createHarness();
    await interleave(harness, messageUpdate(10, '/stop'), messageUpdate(11, '/start'));

    const user = await harness.repository.getUser(USER_ID);
    assert.equal(user?.active, true, 'the newest /start must win');
    assert.equal(user?.revision, 1);
    const jobs = allJobs(harness);
    assert.equal(jobs.length, 1, 'the stale /stop must not enqueue a confirmation');
    assert.equal(jobs[0]?.dedup_key, 'cmd:11');
    assert.equal(jobs[0]?.source_update_id, 11);
    assert.equal(jobs[0]?.expected_revision, 1);
    assert.equal(await lastSeenUpdateId(harness, USER_ID), 11);

    assert.equal(await processQueueMessage(makeQueueMessage(String(jobs[0]?.id)), consumerDeps(harness)), 'sent');
    assert.equal(sentTexts(harness).length, 1);
    assert.ok(sentTexts(harness)[0]?.startsWith(START_TEXT));
  });

  it('inactive user: /stop(10) suspended, /start(11) completes -> no stale deactivation', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await processQueueMessage(makeQueueMessage(queuedJobIds(harness)[0] ?? ''), consumerDeps(harness));
    await handleUpdate(messageUpdate(2, '/stop'), deps);
    await processQueueMessage(makeQueueMessage(queuedJobIds(harness)[1] ?? ''), consumerDeps(harness));
    assert.equal((await harness.repository.getUser(USER_ID))?.active, false);

    await interleave(harness, messageUpdate(10, '/stop'), messageUpdate(11, '/start'));

    const user = await harness.repository.getUser(USER_ID);
    assert.equal(user?.active, true, 'the newest /start must reactivate');
    assert.equal(user?.revision, 3, 'only the reactivation may bump the revision');
    const newerJobs = allJobs(harness).filter((job) => Number(job.source_update_id) >= 10);
    assert.deepEqual(
      newerJobs.map((job) => [job.source_update_id, job.status]),
      [[11, 'pending']],
    );
    assert.equal(
      await processQueueMessage(makeQueueMessage(String(newerJobs[0]?.id)), consumerDeps(harness)),
      'sent',
    );
    assert.equal(sentTexts(harness).length, 3);
    assert.ok(sentTexts(harness).at(-1)?.startsWith(START_TEXT));
  });

  it('allowed callback suspended in its mutation, newer /stop wins: answer once, no stale state or reply', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await processQueueMessage(makeQueueMessage(queuedJobIds(harness)[0] ?? ''), consumerDeps(harness));

    const barrier = createStatementBarrier(harness.db, (sql) =>
      sql.includes('UPDATE users SET course'),
    );
    const callbackDeps: HandlerDeps = {
      ...deps,
      repository: new Repository(barrier.db),
    };
    const older = handleUpdate(callbackUpdate(10, 'course:extended'), callbackDeps);
    await barrier.reached;
    await handleUpdate(messageUpdate(11, '/stop'), deps);
    barrier.release();
    await older;

    const user = await harness.repository.getUser(USER_ID);
    assert.equal(user?.course, 'basic', 'the stale callback must not change the course');
    assert.equal(user?.active, false, 'the newer /stop stays authoritative');
    assert.deepEqual(
      allJobs(harness).map((job) => job.source_update_id),
      [1, 11],
      'no stale callback reply job',
    );
    assert.equal(callbackAnswerCalls(harness), 1, 'the ephemeral acknowledgement is sent exactly once');
    assert.equal(sentTexts(harness).length, 1, 'no stale prompt may be sent');
  });
});

describe('Audit 16 (FIX-ORDER): SQL guards make stale mutations observably no-ops', () => {
  it('a stale update id applies no mutation and leaves every row untouched', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.claimCommandUpdate(USER_ID, USER_ID, 11, now);
    const created = await harness.repository.activateUser(USER_ID, USER_ID, now, 11);
    assert.equal(created?.revision, 1, 'same-update guarded activation applies');
    await harness.repository.setUserCourse(USER_ID, 'extended', now, 11);
    await harness.repository.setUserReminderOffsets(USER_ID, [1440], now, 11);
    await harness.repository.insertCommandJob({
      id: 'job-11',
      telegramUserId: USER_ID,
      chatId: USER_ID,
      payloadJson: JSON.stringify({ text: 'current guidance' }),
      dedupKey: 'cmd:11',
      sendAtMs: now,
      now,
      expectedRevision: created?.revision ?? null,
      sourceUpdateId: 11,
    });
    const userBefore = await harness.repository.getUser(USER_ID);
    const jobsBefore = allJobs(harness).map((job) => [job.id, job.status]);

    assert.equal(await harness.repository.setUserCourse(USER_ID, 'basic', now, 10), false);
    assert.equal(await harness.repository.setUserReminderOffsets(USER_ID, [30], now, 10), false);
    assert.equal(await harness.repository.deactivateUser(USER_ID, now, 10), false);
    assert.equal(await harness.repository.cancelPendingJobsForUser(USER_ID, now, 10), 0);
    assert.deepEqual(
      await harness.repository.activateUser(USER_ID, USER_ID, now, 10),
      userBefore,
      'a rejected activation reports the untouched row',
    );

    assert.deepEqual(await harness.repository.getUser(USER_ID), userBefore);
    assert.deepEqual(allJobs(harness).map((job) => [job.id, job.status]), jobsBefore);
    assert.equal(await harness.repository.isCommandUpdateCurrent(USER_ID, 9), false);
    assert.equal(await harness.repository.isCommandUpdateCurrent(USER_ID, 10), false);
    assert.equal(await harness.repository.isCommandUpdateCurrent(USER_ID, 11), true);
    assert.equal(await harness.repository.isCommandUpdateCurrent(USER_ID, 12), true);
  });

  it('a stale first-contact activation creates no user row and reports rejection', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    assert.equal(await harness.repository.claimCommandUpdate(999, 999, 11, now), true);
    assert.equal(await harness.repository.activateUser(999, 999, now, 10), null);
    assert.equal(await harness.repository.getUser(999), null);
    assert.equal(await harness.repository.cancelPendingJobsForUser(999, now, 10), 0);
    assert.equal(await harness.repository.isCommandUpdateCurrent(999, 10), false);
  });

  it('unguarded callers and users without recorded state keep the old semantics', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const unguarded = await harness.repository.activateUser(USER_ID, USER_ID, now);
    assert.equal(unguarded.active, true);

    const unrecorded = 555;
    await harness.repository.activateUser(unrecorded, unrecorded, now);
    assert.equal(await harness.repository.isCommandUpdateCurrent(unrecorded, 10), true);
    assert.equal(await harness.repository.setUserCourse(unrecorded, 'extended', now, 10), true);
    assert.equal(await harness.repository.deactivateUser(unrecorded, now, 10), true);
  });

  it('same-update retry after a failed activation keeps one revision and one job', async () => {
    const harness = createHarness();
    const originalActivate = harness.repository.activateUser.bind(harness.repository) as (
      telegramUserId: number,
      chatId: number,
      now: number,
      commandUpdateId?: number,
    ) => Promise<unknown>;
    let failOnce = true;
    harness.repository.activateUser = (async (
      userId: number,
      chatId: number,
      now: number,
      commandUpdateId?: number,
    ) => {
      if (failOnce) {
        failOnce = false;
        throw new Error('boom during activation');
      }
      return originalActivate(userId, chatId, now, commandUpdateId);
    }) as typeof harness.repository.activateUser;

    await assert.rejects(handleUpdate(messageUpdate(10, '/start'), handlerDeps(harness)));
    assert.equal(await harness.repository.getUser(USER_ID), null);
    assert.equal(harness.queue.batches.flat().length, 0);

    await handleUpdate(messageUpdate(10, '/start'), handlerDeps(harness));
    const rows = allJobs(harness);
    assert.equal(rows.length, 1, 'the retry must not create a second job');
    assert.equal(rows[0]?.dedup_key, 'cmd:10');
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 1);
    assert.equal(await processQueueMessage(makeQueueMessage(String(rows[0]?.id)), consumerDeps(harness)), 'sent');
    assert.equal(sentTexts(harness).length, 1);
  });

  it('same-update retry after a failed enqueue keeps one job, one revision and one reply', async () => {
    const harness = createHarness();
    const originalSendBatch = harness.queue.producer.sendBatch;
    let failOnce = true;
    harness.queue.producer.sendBatch = async (messages) => {
      if (failOnce) {
        failOnce = false;
        throw new Error('queue unavailable');
      }
      return originalSendBatch(messages);
    };

    await assert.rejects(handleUpdate(messageUpdate(10, '/start'), handlerDeps(harness)));
    const rows = allJobs(harness);
    assert.equal(rows.length, 1, 'the reply job is persisted before the enqueue');
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 1);
    assert.equal(harness.queue.batches.flat().length, 0);

    await handleUpdate(messageUpdate(10, '/start'), handlerDeps(harness));
    assert.equal(allJobs(harness).length, 1, 'the retry sees the dedup key and adds nothing');
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 1);
    assert.equal(harness.queue.batches.flat().length, 0);

    assert.equal(await processQueueMessage(makeQueueMessage(String(rows[0]?.id)), consumerDeps(harness)), 'sent');
    assert.equal(sentTexts(harness).length, 1);
  });

  it('duplicate delivery of the same command job yields one logical effect', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await processQueueMessage(makeQueueMessage(queuedJobIds(harness)[0] ?? ''), consumerDeps(harness));
    const revisionBefore = (await harness.repository.getUser(USER_ID))?.revision ?? 0;

    await Promise.all([
      handleUpdate(messageUpdate(5, '/stop'), deps),
      handleUpdate(messageUpdate(5, '/stop'), deps),
    ]);

    const user = await harness.repository.getUser(USER_ID);
    assert.equal(user?.active, false);
    assert.equal(user?.revision, revisionBefore + 1, 'the duplicate pair deactivates once');
    const stopJobs = allJobs(harness).filter((job) => job.dedup_key === 'cmd:5');
    assert.equal(stopJobs.length, 1, 'the duplicate pair converges to one job');
    assert.equal(await lastSeenUpdateId(harness, USER_ID), 5);

    const jobId = String(stopJobs[0]?.id);
    assert.equal(await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness)), 'sent');
    assert.equal(await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness)), 'skipped');
    assert.equal(sentTexts(harness).filter((text) => text === STOPPED_TEXT).length, 1);
  });
});
