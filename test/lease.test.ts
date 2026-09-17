/**
 * Exclusive consumer lease ownership. These tests run against the real
 * SQLite-backed repository so the atomic eligibility checks are exercised with
 * actual SQL.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueMessage, type ConsumerDeps } from '../src/queue/consumer.ts';
import { runSchedulerTick } from '../src/scheduler/tick.ts';
import { JOB_ENQUEUE_LEASE_MS, JOB_LEASE_MS } from '../src/util.ts';
import { consumerDeps, createHarness, schedulerDeps, type Harness } from './helpers/harness.ts';
import { makeQueueMessage, seedDueJobs, seedDueReminders } from './helpers/seed.ts';

function deps(harness: Harness, owner: string): ConsumerDeps {
  return consumerDeps(harness, { ownerFactory: () => owner });
}

/** Reserves the only due job for enqueue (the scheduler's `enqueued` state). */
async function reservedJob(harness: Harness): Promise<string> {
  const [jobId = ''] = await seedDueJobs(harness, [111]);
  return jobId;
}

describe('exclusive consumer leases', () => {
  it('does not let a second consumer steal an active processing lease', async () => {
    const harness = createHarness();
    const jobId = await reservedJob(harness);
    const now = harness.clock.now();

    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'consumer-a', now, JOB_LEASE_MS),
      null,
    );
    assert.equal(
      await harness.repository.claimJobContext(jobId, 'consumer-b', now, JOB_LEASE_MS),
      null,
    );
    assert.equal(await harness.repository.finishJobSent(jobId, 'consumer-b', now), false);
    assert.equal(await harness.repository.finishJobSent(jobId, 'consumer-a', now), true);
  });

  it('recovers an expired processing lease for a new consumer', async () => {
    const harness = createHarness();
    const jobId = await reservedJob(harness);
    const now = harness.clock.now();

    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'consumer-a', now, JOB_LEASE_MS),
      null,
    );
    harness.clock.advance(JOB_LEASE_MS + 1);
    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'consumer-b', harness.clock.now(), JOB_LEASE_MS),
      null,
    );
  });

  it('rejects an obsolete owner once the lease is reclaimed', async () => {
    const harness = createHarness();
    const jobId = await reservedJob(harness);
    const now = harness.clock.now();

    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'consumer-a', now, JOB_LEASE_MS),
      null,
    );
    harness.clock.advance(JOB_LEASE_MS + 1);
    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'consumer-b', harness.clock.now(), JOB_LEASE_MS),
      null,
    );
    assert.equal(
      await harness.repository.finishJobSent(jobId, 'consumer-a', harness.clock.now()),
      false,
      'the obsolete owner must not report a persisted success',
    );
    assert.equal(
      await harness.repository.finishJobSent(jobId, 'consumer-b', harness.clock.now()),
      true,
    );
  });

  it('keeps scheduler overlap from double-reserving a job', async () => {
    const harness = createHarness();
    const jobId = await reservedJob(harness);
    assert.notEqual(jobId, '');
    const again = await harness.repository.claimDueJobs(
      'scheduler-2',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.equal(again.length, 0);
  });

  it('repairs an enqueue reservation whose sender crashed', async () => {
    const harness = createHarness();
    const jobId = await reservedJob(harness);

    const notReserved = await harness.repository.claimDueJobs(
      'scheduler-2',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.equal(notReserved.length, 0, 'an enqueued reservation is not re-claimable');

    harness.clock.advance(JOB_LEASE_MS + 1);
    assert.equal(await harness.repository.repairExpiredLeases(harness.clock.now()), 1);
    assert.equal((await harness.repository.getJob(jobId))?.status, 'pending');
    const reclaimed = await harness.repository.claimDueJobs(
      'scheduler-2',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.equal(reclaimed.length, 1);
  });

  it('leaves a job recoverable when the queue send fails', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    await seedDueReminders(harness, [111]);

    const failingQueue = {
      async sendBatch(): Promise<unknown> {
        throw new Error('queue down');
      },
    };
    await assert.rejects(
      runSchedulerTick(schedulerDeps(harness, { queue: failingQueue })),
    );

    const stillReserved = await harness.repository.claimDueJobs(
      'scheduler-2',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.equal(stillReserved.length, 0);

    // The tick reserves enqueues with JOB_ENQUEUE_LEASE_MS, so recovery waits
    // out that reservation instead of the shorter processing lease.
    harness.clock.advance(JOB_ENQUEUE_LEASE_MS + 1);
    assert.equal(await harness.repository.repairExpiredLeases(harness.clock.now()), 1);
    const reclaimed = await harness.repository.claimDueJobs(
      'scheduler-2',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.equal(reclaimed.length, 1);
  });

  it('acknowledges a duplicate delivery without sending twice', async () => {
    const harness = createHarness();
    const jobId = await reservedJob(harness);

    const first = await processQueueMessage(makeQueueMessage(jobId), deps(harness, 'consumer-a'));
    const second = await processQueueMessage(makeQueueMessage(jobId), deps(harness, 'consumer-b'));

    assert.equal(first, 'sent');
    assert.equal(second, 'skipped');
    const sends = harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage'));
    assert.equal(sends.length, 1, 'only one normal send may happen');
  });

  it('serializes two consumers racing the same delivery into one send', async () => {
    const harness = createHarness();
    const jobId = await reservedJob(harness);

    const [a, b] = await Promise.all([
      processQueueMessage(makeQueueMessage(jobId), deps(harness, 'consumer-a')),
      processQueueMessage(makeQueueMessage(jobId), deps(harness, 'consumer-b')),
    ]);

    assert.deepEqual([a, b].sort(), ['sent', 'skipped']);
    const sends = harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage'));
    assert.equal(sends.length, 1, 'only one normal send may happen');
  });

  it('delivers a command job enqueued directly by the webhook', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(222, 222, now);
    await harness.repository.insertCommandJob({
      id: 'cmd-1',
      telegramUserId: 222,
      chatId: 222,
      payloadJson: JSON.stringify({ text: 'hello' }),
      dedupKey: 'cmd:1',
      sendAtMs: now,
      now,
      expectedRevision: 1,
    });

    // The webhook sends the queue message itself: the job is still `pending`.
    const outcome = await processQueueMessage(makeQueueMessage('cmd-1'), deps(harness, 'consumer'));
    assert.equal(outcome, 'sent');
    assert.equal((await harness.repository.getJob('cmd-1'))?.status, 'sent');
  });
});
