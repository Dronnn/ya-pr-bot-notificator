/**
 * F2 regression (consumer side): `attempt_count` counts started Telegram calls
 * only. Pacing, cooldown, budget and lease-repair deferrals make zero HTTP
 * calls and leave the counter untouched; every started call - success, network
 * error or timeout - increments it exactly once; and the configured maximum
 * terminates on real HTTP attempts, not on Queue deliveries.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueBatch, processQueueMessage } from '../src/queue/consumer.ts';
import { runSchedulerTick } from '../src/scheduler/tick.ts';
import { TelegramClient } from '../src/telegram/adapter.ts';
import {
  JOB_ENQUEUE_LEASE_MS,
  JOB_LEASE_MS,
  MAX_D1_STATEMENTS_PER_CONSUMER,
  MS_PER_MINUTE,
  PACE_MAX_WAIT_MS,
  PACE_RETRY_DELAY_MS,
} from '../src/util.ts';
import { hangingFetch, jsonResponse } from './helpers/fakes.ts';
import {
  consumerDeps,
  createHarness,
  schedulerDeps,
  TEST_BOT_TOKEN,
  type Harness,
} from './helpers/harness.ts';
import { makeQueueMessage, seedDueJobs, seedDueReminders } from './helpers/seed.ts';

function attemptCount(harness: Harness, jobId: string): number {
  const row = harness.db.database
    .prepare('SELECT attempt_count FROM outbound_jobs WHERE id = ?')
    .get(jobId) as { attempt_count: unknown } | undefined;
  assert.notEqual(row, undefined, `job ${jobId} must exist`);
  return Number(row?.attempt_count);
}

function sendCallCount(harness: Harness): number {
  return harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage')).length;
}

/** Re-enqueues a pending job exactly as the scheduler does for a due retry. */
async function reclaim(harness: Harness): Promise<void> {
  const claimed = await harness.repository.claimDueJobs(
    'scheduler',
    harness.clock.now(),
    JOB_LEASE_MS,
    10,
  );
  assert.equal(claimed.length, 1, 'the job must be due for this delivery round');
}

describe('attempt counting follows real Telegram calls', () => {
  it('leaves attempts at zero through repeated global-cooldown deferrals', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, [111]);
    await harness.repository.setSendCooldown(
      harness.clock.now() + 10 * MS_PER_MINUTE,
      harness.clock.now(),
    );

    // `seedDueJobs` already enqueued the job; every retry after the first is
    // re-enqueued by the scheduler exactly as in production.
    for (let round = 0; round < 3; round += 1) {
      if (round > 0) {
        harness.clock.advance(PACE_RETRY_DELAY_MS + 1);
        await reclaim(harness);
      }
      const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
      assert.equal(outcome, 'retried', `round ${round} defers instead of sending`);
      assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'paced');
      assert.equal(attemptCount(harness, jobId), 0);
    }

    assert.equal(sendCallCount(harness), 0, 'no HTTP call may start during a cooldown');
  });

  it('leaves attempts at zero when the pace row makes a slot impossible', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, [222]);
    // Without the singleton pace row no slot can ever be reserved; the job
    // must defer without counting a send.
    harness.db.exec("DELETE FROM rate_state WHERE name = 'send'");

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'retried');
    assert.equal(attemptCount(harness, jobId), 0);
    assert.equal(sendCallCount(harness), 0);
  });

  it('increments exactly once per started call on network errors and timeouts', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, [333]);

    harness.setHandler(() => {
      throw new TypeError('network down');
    });
    const first = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
    assert.equal(first, 'retried');
    assert.equal(attemptCount(harness, jobId), 1, 'a started call counts even when it fails');
    assert.equal(sendCallCount(harness), 1);

    // A delivery that defers during a cooldown makes no call, so it must not
    // move the counter - a claim alone is not an attempt.
    await harness.repository.setSendCooldown(
      harness.clock.now() + 10 * MS_PER_MINUTE,
      harness.clock.now(),
    );
    harness.clock.advance(MS_PER_MINUTE);
    await reclaim(harness);
    const deferred = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
    assert.equal(deferred, 'retried');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'paced');
    assert.equal(attemptCount(harness, jobId), 1, 'the deferral claim must not count');

    // A timeout is still a started call: the request left the Worker.
    harness.db.exec('UPDATE rate_state SET cooldown_until_ms = NULL');
    harness.clock.advance(2 * MS_PER_MINUTE);
    await reclaim(harness);
    const timeoutClient = new TelegramClient({
      botToken: TEST_BOT_TOKEN,
      fetch: hangingFetch,
      logger: harness.logger.logger,
      timeoutMs: 20,
    });
    const second = await processQueueMessage(
      makeQueueMessage(jobId),
      consumerDeps(harness, { telegram: timeoutClient }),
    );
    assert.equal(second, 'retried');
    assert.equal(attemptCount(harness, jobId), 2);
  });

  it('terminates on real HTTP attempts, not on Queue deliveries', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, [444]);
    const capped = consumerDeps(harness, { maxAttempts: 2 });
    await harness.repository.setSendCooldown(
      harness.clock.now() + MS_PER_MINUTE,
      harness.clock.now(),
    );

    // Three deliveries that never start a call must not consume the allowance.
    for (let round = 0; round < 3; round += 1) {
      if (round > 0) {
        harness.clock.advance(PACE_RETRY_DELAY_MS + 1);
        await reclaim(harness);
      }
      assert.equal(await processQueueMessage(makeQueueMessage(jobId), capped), 'retried');
      assert.equal(attemptCount(harness, jobId), 0);
    }

    // Two real attempts exhaust `maxAttempts` even though the job was
    // delivered five times in total.
    harness.db.exec('UPDATE rate_state SET cooldown_until_ms = NULL');
    harness.setHandler(() => jsonResponse({ ok: false }, 500));

    harness.clock.advance(PACE_RETRY_DELAY_MS + 1);
    await reclaim(harness);
    assert.equal(await processQueueMessage(makeQueueMessage(jobId), capped), 'retried');
    assert.equal(attemptCount(harness, jobId), 1);

    harness.clock.advance(2 * MS_PER_MINUTE);
    await reclaim(harness);
    assert.equal(await processQueueMessage(makeQueueMessage(jobId), capped), 'terminal');
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.status, 'failed');
    assert.equal(job?.last_error_code, 'transient_server');
    assert.equal(attemptCount(harness, jobId), 2);
    assert.equal(sendCallCount(harness), 2, 'only the two real calls were counted');
  });

  it('does not count scheduler enqueue, re-enqueue or expired-lease repair', async () => {
    const harness = createHarness();
    await seedDueReminders(harness, [555]);
    const first = await runSchedulerTick(schedulerDeps(harness));
    assert.equal(first.enqueued, 1);
    const jobId = harness.queue.batches.flat()[0]?.jobId ?? '';
    assert.equal(attemptCount(harness, jobId), 0);

    // An overlapping tick must not re-enqueue the draining job either.
    const second = await runSchedulerTick(schedulerDeps(harness));
    assert.equal(second.enqueued, 0);
    assert.equal(attemptCount(harness, jobId), 0);

    // After the enqueue reservation expires the job is repaired and
    // re-enqueued - still without counting a send.
    harness.clock.advance(JOB_ENQUEUE_LEASE_MS + 1);
    const third = await runSchedulerTick(schedulerDeps(harness));
    assert.equal(third.enqueued, 1, 'an expired reservation is repaired and re-enqueued');
    assert.equal(attemptCount(harness, jobId), 0);
  });
});

describe('bounded cooldown waiting and the in-place rate-limit retry', () => {
  it('waits out a short cooldown inside the batch instead of parking the wave', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 5);
    const now = harness.clock.now();
    // A ~1s cooldown is already recorded: every message of the wave must wait
    // it out and send instead of being parked until the next tick.
    await harness.repository.setSendCooldown(now + 1_000, now);
    const cooldownUntil = now + 1_000;

    const sendTimes: number[] = [];
    harness.setHandler((url) => {
      if (url.includes('/sendMessage')) {
        sendTimes.push(harness.clock.now());
      }
      return jsonResponse({ ok: true, result: { message_id: 1 } });
    });
    const messages = jobIds.map(makeQueueMessage);
    harness.db.stats.reset();
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    assert.equal(summary.sent, jobIds.length);
    assert.equal(summary.deferred, 0);
    assert.ok(messages.every((message) => message.acked));
    assert.equal(sendTimes.length, jobIds.length);
    for (const at of sendTimes) {
      assert.ok(
        at >= cooldownUntil,
        `send at ${at} must not start before the cooldown ends at ${cooldownUntil}`,
      );
    }
    assert.ok(
      harness.db.stats.statements <= MAX_D1_STATEMENTS_PER_CONSUMER,
      `used ${harness.db.stats.statements} statements`,
    );
  });

  it('retries a short 429 in place and persists both real calls', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 3);

    const calls: { chatId: number; at: number }[] = [];
    harness.setHandler((url, init) => {
      if (!url.includes('/sendMessage')) {
        return jsonResponse({ ok: true, result: { message_id: 1 } });
      }
      const chatId = (JSON.parse(String(init?.body)) as { chat_id: number }).chat_id;
      calls.push({ chatId, at: harness.clock.now() });
      return calls.length === 1
        ? jsonResponse({ ok: false, parameters: { retry_after: 1 } }, 429)
        : jsonResponse({ ok: true, result: { message_id: calls.length } });
    });

    const messages = jobIds.map(makeQueueMessage);
    harness.db.stats.reset();
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    // Same invocation, no parking: the 429 job waits the cooldown out and
    // sends again, so every job is sent and exactly one job made two calls.
    assert.equal(summary.sent, jobIds.length);
    assert.equal(summary.retried, 0);
    assert.equal(summary.deferred, 0);
    assert.ok(messages.every((message) => message.acked));
    assert.equal(calls.length, 4, 'three jobs, one of them retried once');

    const firstAt = calls[0]?.at ?? 0;
    const firstChatId = calls[0]?.chatId ?? 0;
    const retryCalls = calls.filter((call) => call.chatId === firstChatId);
    assert.equal(retryCalls.length, 2, 'the 429 job starts a second real call');
    assert.ok(
      (retryCalls[1]?.at ?? 0) >= firstAt + 1_000,
      'the retry must start at or after the recorded cooldown boundary',
    );

    const retried = harness.db.database
      .prepare('SELECT id, status, attempt_count FROM outbound_jobs WHERE attempt_count = 2')
      .all() as { id: string; status: string; attempt_count: unknown }[];
    assert.deepEqual(
      retried.map((row) => [row.status, Number(row.attempt_count)]),
      [['sent', 2]],
      'exactly the retried job counts both started calls and is persisted as sent',
    );
    const firstAttempts = harness.db.database
      .prepare('SELECT COUNT(*) AS n FROM outbound_jobs WHERE attempt_count = 1')
      .get() as { n: number };
    assert.equal(Number(firstAttempts.n), jobIds.length - 1);
    assert.ok(
      harness.db.stats.statements <= MAX_D1_STATEMENTS_PER_CONSUMER,
      `used ${harness.db.stats.statements} statements`,
    );
  });

  it('keeps a long 429 on the reschedule path and never sends early', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, [666]);
    const now = harness.clock.now();
    harness.setHandler(() =>
      jsonResponse({ ok: false, parameters: { retry_after: PACE_MAX_WAIT_MS / 1_000 + 1 } }, 429),
    );

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'retried');
    assert.equal(sendCallCount(harness), 1, 'a long 429 is not retried in place');
    assert.equal(attemptCount(harness, jobId), 1);
    const pace = await harness.repository.getPaceState();
    assert.ok((pace?.cooldownUntilMs ?? 0) > now);
    assert.equal(harness.clock.now(), now, 'no bounded wait was taken');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'rate_limited');
  });
});
