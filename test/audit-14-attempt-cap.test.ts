/**
 * F2 regression (repo part): the atomic pre-send reservation enforces the real
 * attempt cap before every Telegram call and distinguishes `reserved`,
 * `exhausted`, `lost` and `superseded` without collapsing states that demand
 * different durable outcomes.
 *
 * These tests fail against the old `beginSendAttempt` (unconditional increment
 * without an `attempt_count < maxAttempts` predicate): the crash-after-final
 * test would start a seventh request, and removing the cap predicate (mutation
 * probe) reopens it.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueMessage } from '../src/queue/consumer.ts';
import { JOB_LEASE_MS, MAX_JOB_ATTEMPTS } from '../src/util.ts';
import { consumerDeps, createHarness, type Harness } from './helpers/harness.ts';
import { makeQueueMessage, seedDueJobs } from './helpers/seed.ts';
import { jsonResponse } from './helpers/fakes.ts';

function attemptCount(harness: Harness, jobId: string): number {
  const row = harness.db.database
    .prepare('SELECT attempt_count FROM outbound_jobs WHERE id = ?')
    .get(jobId) as { attempt_count: unknown };
  return Number(row.attempt_count);
}

function telegramCalls(harness: Harness): number {
  return harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage')).length;
}

/** Claims a seeded job for `owner` and returns its id. */
async function claimedJob(harness: Harness, userId: number, owner: string): Promise<string> {
  const [jobId = ''] = await seedDueJobs(harness, [userId]);
  const now = harness.clock.now();
  assert.notEqual(
    await harness.repository.claimJobContext(jobId, owner, now, JOB_LEASE_MS),
    null,
  );
  return jobId;
}

describe('atomic attempt cap', () => {
  it('crash after the final increment: repair reserves nothing and sends nothing', async () => {
    const harness = createHarness();
    const jobId = await claimedJob(harness, 1, 'consumer-a');
    const now = harness.clock.now();

    // Six real calls start; the sixth outcome is never persisted (crash).
    for (let attempt = 1; attempt <= MAX_JOB_ATTEMPTS; attempt += 1) {
      assert.deepEqual(await harness.repository.beginSendAttempt(jobId, 'consumer-a', now, MAX_JOB_ATTEMPTS), {
        status: 'reserved',
        attempt,
        userTimeZone: 'Europe/Moscow',
      });
    }
    assert.equal(attemptCount(harness, jobId), MAX_JOB_ATTEMPTS);

    // Lease repair preserves the count; the recovered consumer must not start
    // another request and must settle terminal at exactly the cap.
    harness.clock.advance(JOB_LEASE_MS + 1);
    assert.equal(await harness.repository.repairExpiredLeases(harness.clock.now()), 1);
    const reclaimed = await harness.repository.claimDueJobs(
      'scheduler',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.equal(reclaimed.length, 1);
    const outcome = await processQueueMessage(
      makeQueueMessage(jobId),
      consumerDeps(harness, { ownerFactory: () => 'consumer-b' }),
    );
    assert.equal(outcome, 'terminal');
    assert.equal(telegramCalls(harness), 0, 'zero additional HTTP calls after the cap');
    assert.equal(attemptCount(harness, jobId), MAX_JOB_ATTEMPTS, 'terminal count equals the cap');
    assert.equal((await harness.repository.getJob(jobId))?.status, 'failed');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'attempts-exhausted');
  });

  it('cap 1 allows exactly one start, then exhausts without sending', async () => {
    const harness = createHarness();
    const jobId = await claimedJob(harness, 2, 'owner');
    const now = harness.clock.now();

    assert.deepEqual(await harness.repository.beginSendAttempt(jobId, 'owner', now, 1), {
      status: 'reserved',
      attempt: 1,
      userTimeZone: 'Europe/Moscow',
    });
    assert.deepEqual(await harness.repository.beginSendAttempt(jobId, 'owner', now, 1), {
      status: 'exhausted',
    });
    assert.equal(attemptCount(harness, jobId), 1, 'the refused reservation increments nothing');
  });

  it('a short-429 retry at the cap sends once and terminates', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, [3]);
    let calls = 0;
    harness.setHandler(() => {
      calls += 1;
      return jsonResponse({ ok: false, parameters: { retry_after: 1 } }, 429);
    });
    const outcome = await processQueueMessage(
      makeQueueMessage(jobId),
      consumerDeps(harness, { maxAttempts: 1 }),
    );
    assert.equal(outcome, 'terminal');
    assert.equal(calls, 1, 'the exhausted allowance forbids the in-place retry');
    assert.equal(attemptCount(harness, jobId), 1);
    assert.equal((await harness.repository.getJob(jobId))?.status, 'failed');
  });

  it('a short-429 retry below the cap counts both real calls', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, [4]);
    let calls = 0;
    harness.setHandler(() => {
      calls += 1;
      if (calls === 1) {
        return jsonResponse({ ok: false, parameters: { retry_after: 1 } }, 429);
      }
      return jsonResponse({ ok: true, result: { message_id: 1 } });
    });
    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
    assert.equal(outcome, 'sent');
    assert.equal(calls, 2, 'first call plus exactly one in-place retry');
    assert.equal(attemptCount(harness, jobId), 2, 'exactly one increment per started call');
  });

  it('concurrent owners and expired leases classify as lost without incrementing', async () => {
    const harness = createHarness();
    const jobId = await claimedJob(harness, 5, 'owner-a');
    const now = harness.clock.now();

    assert.deepEqual(await harness.repository.beginSendAttempt(jobId, 'owner-b', now, 6), {
      status: 'lost',
    });
    assert.deepEqual(await harness.repository.beginSendAttempt('missing-job', 'owner-a', now, 6), {
      status: 'lost',
    });
    assert.equal(attemptCount(harness, jobId), 0);

    harness.clock.advance(JOB_LEASE_MS + 1);
    assert.deepEqual(
      await harness.repository.beginSendAttempt(jobId, 'owner-a', harness.clock.now(), 6),
      { status: 'lost' },
      'an expired lease cannot start a call even for the owning consumer',
    );
    assert.equal(attemptCount(harness, jobId), 0);
  });

  it('command revision supersession never increments', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(7, 7, now);
    await harness.repository.insertCommandJob({
      id: 'cmd-cap',
      telegramUserId: 7,
      chatId: 7,
      payloadJson: JSON.stringify({ text: 'menu' }),
      dedupKey: 'cmd-cap',
      sendAtMs: now,
      now,
      expectedRevision: 1,
    });
    assert.notEqual(
      await harness.repository.claimJobContext('cmd-cap', 'owner', now, JOB_LEASE_MS),
      null,
    );
    assert.deepEqual(await harness.repository.beginSendAttempt('cmd-cap', 'owner', now, 6), {
      status: 'reserved',
      attempt: 1,
      userTimeZone: null,
    });

    await harness.repository.setUserCourse(7, 'extended', now);
    assert.deepEqual(await harness.repository.beginSendAttempt('cmd-cap', 'owner', now, 6), {
      status: 'superseded',
    });
    assert.equal(attemptCount(harness, 'cmd-cap'), 1);
  });

  it('command update ordering supersedes older updates only', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(8, 8, now);
    await harness.repository.recordCommandUpdate(8, 8, 10, now);

    for (const [id, sourceUpdateId, expected] of [
      ['cmd-old', 5, 'superseded'],
      ['cmd-current', 10, 'reserved'],
      ['cmd-new', 11, 'reserved'],
      ['cmd-legacy-null', null, 'reserved'],
      ['cmd-legacy-zero', 0, 'reserved'],
    ] as const) {
      await harness.repository.insertCommandJob({
        id,
        telegramUserId: 8,
        chatId: 8,
        payloadJson: JSON.stringify({ text: id }),
        dedupKey: id,
        sendAtMs: now,
        now,
        expectedRevision: null,
        sourceUpdateId,
      });
      assert.notEqual(
        await harness.repository.claimJobContext(id, 'owner', now, JOB_LEASE_MS),
        null,
      );
      const reservation = await harness.repository.beginSendAttempt(id, 'owner', now, 6);
      assert.equal(reservation.status, expected, `${id} classifies as ${expected}`);
    }
    assert.equal(attemptCount(harness, 'cmd-old'), 0, 'superseded work increments nothing');

    // No ordering evidence anywhere: deliverable.
    await harness.repository.activateUser(9, 9, now);
    await harness.repository.insertCommandJob({
      id: 'cmd-no-state',
      telegramUserId: 9,
      chatId: 9,
      payloadJson: JSON.stringify({ text: 'x' }),
      dedupKey: 'cmd-no-state',
      sendAtMs: now,
      now,
      expectedRevision: null,
      sourceUpdateId: 5,
    });
    assert.notEqual(
      await harness.repository.claimJobContext('cmd-no-state', 'owner', now, JOB_LEASE_MS),
      null,
    );
    assert.deepEqual(await harness.repository.beginSendAttempt('cmd-no-state', 'owner', now, 6), {
      status: 'reserved',
      attempt: 1,
      userTimeZone: null,
    });
  });

  it('command ordering state is monotonic under out-of-order updates', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.recordCommandUpdate(11, 11, 7, now);
    await harness.repository.recordCommandUpdate(11, 11, 5, now);
    assert.deepEqual(await harness.repository.getLastCommandUpdate(11), {
      chatId: 11,
      lastUpdateId: 7,
    });
    await harness.repository.recordCommandUpdate(11, 11, 7, now);
    await harness.repository.recordCommandUpdate(11, 12, 9, now);
    assert.deepEqual(await harness.repository.getLastCommandUpdate(11), {
      chatId: 12,
      lastUpdateId: 9,
    });
    assert.equal(await harness.repository.getLastCommandUpdate(12), null);
  });

  it('hasCommandJob reflects enqueued command identity', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    assert.equal(await harness.repository.hasCommandJob('cmd:42'), false);
    await harness.repository.insertCommandJob({
      id: 'cmd-42',
      telegramUserId: 42,
      chatId: 42,
      payloadJson: JSON.stringify({ text: 'hi' }),
      dedupKey: 'cmd:42',
      sendAtMs: now,
      now,
      expectedRevision: null,
    });
    assert.equal(await harness.repository.hasCommandJob('cmd:42'), true);
  });

  it('activateUser bumps revision only on activation or chat change', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const first = await harness.repository.activateUser(13, 100, now);
    assert.equal(first.revision, 1);
    const repeat = await harness.repository.activateUser(13, 100, now + 1);
    assert.equal(repeat.revision, 1, 'a repeat /start must not bump the revision');
    const moved = await harness.repository.activateUser(13, 200, now + 2);
    assert.equal(moved.revision, 2, 'a chat change bumps once');
    await harness.repository.deactivateUser(13, now + 3);
    const reactivated = await harness.repository.activateUser(13, 200, now + 4);
    assert.equal(
      reactivated.revision,
      (await harness.repository.getUser(13))?.revision,
      'reactivation after /stop bumps exactly once',
    );
    assert.equal(reactivated.revision, 4);
  });
});
