import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueBatch, processQueueMessage } from '../src/queue/consumer.ts';
import type { Repository, SendReservation } from '../src/data/repository.ts';
import {
  JOB_LEASE_MS,
  MAX_D1_STATEMENTS_PER_CONSUMER,
  MAX_JOB_ATTEMPTS,
  MS_PER_MINUTE,
  PACE_MAX_WAIT_MS,
  STALE_SOURCE_CUTOFF_MS,
} from '../src/util.ts';
import { jsonResponse, textResponse } from './helpers/fakes.ts';
import { consumerDeps, createHarness, TEST_BOT_TOKEN, type Harness } from './helpers/harness.ts';
import { makeQueueMessage, occurrence, seedDueJobs } from './helpers/seed.ts';

async function dueReminderJob(
  harness: Harness,
  startOffsetMs: number = 10 * MS_PER_MINUTE,
  offset: 30 | 1440 = 30,
): Promise<string> {
  const [jobId = ''] = await seedDueJobs(harness, [111], {
    startsAtMs: harness.clock.now() + startOffsetMs,
    reminderOffsetMinutes: offset,
  });
  return jobId;
}

describe('queue consumer', () => {
  it('sends a due reminder, persists the send before acknowledging', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    const message = makeQueueMessage(jobId);

    const summary = await processQueueBatch([message], consumerDeps(harness));

    assert.equal(summary.sent, 1);
    assert.equal(message.acked, true);
    assert.equal((await harness.repository.getJob(jobId))?.status, 'sent');
    const sendCalls = harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage'));
    assert.equal(sendCalls.length, 1);
    const body = JSON.parse(String(sendCalls[0]?.init?.body)) as {
      allow_paid_broadcast?: boolean;
      text?: string;
    };
    assert.equal(body.allow_paid_broadcast, false);
    assert.match(body.text ?? '', /Europe\/Moscow/);
  });

  it('only the lease owner can complete a job', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    const now = harness.clock.now();
    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'owner-a', now, JOB_LEASE_MS),
      null,
    );
    assert.equal(await harness.repository.finishJobSent(jobId, 'owner-b', now), false);
    assert.equal(await harness.repository.finishJobSent(jobId, 'owner-a', now), true);
  });

  it('retries transient server errors with backoff', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    harness.setHandler(() => jsonResponse({ ok: false }, 500));

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'retried');
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.status, 'pending');
    assert.equal(job?.last_error_code, 'transient_server');
    assert.ok(Number(job?.next_attempt_at_ms) > harness.clock.now());
  });

  it('disables the recipient on 403', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    harness.setHandler(() => jsonResponse({ ok: false, description: 'Forbidden' }, 403));

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'terminal');
    const user = await harness.repository.getUser(111);
    assert.equal(user?.active, false);
    assert.equal((await harness.repository.getJob(jobId))?.status, 'cancelled');
  });

  it('applies a global cooldown and retries on 429', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    harness.setHandler(() =>
      jsonResponse({ ok: false, parameters: { retry_after: 5 } }, 429),
    );

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'retried');
    const pace = await harness.repository.getPaceState();
    assert.ok((pace?.cooldownUntilMs ?? 0) > harness.clock.now());
    assert.equal((await harness.repository.getJob(jobId))?.status, 'pending');
  });

  it('defers instead of sending when the cooldown exceeds the bounded wait', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, [111]);
    const now = harness.clock.now();
    await harness.repository.setSendCooldown(now + PACE_MAX_WAIT_MS + 1, now);

    let calls = 0;
    harness.setHandler(() => {
      calls += 1;
      return jsonResponse({ ok: true, result: { message_id: 1 } });
    });
    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
    const statements = harness.repository.statementsUsed();

    assert.equal(outcome, 'retried');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'paced');
    assert.equal(calls, 0, 'a long cooldown must never start a request');
    assert.equal(harness.clock.now(), now, 'no bounded wait was taken');
    assert.equal(statements, 4, 'claim, failed slot (1), pace read, reschedule');
  });

  it('cancels a reminder whose recipient stopped after enqueue', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    await harness.repository.deactivateUser(111, harness.clock.now());

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'terminal');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'inactive');
  });

  it('expires a reminder once the event started', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness, 10 * MS_PER_MINUTE);
    harness.clock.advance(11 * MS_PER_MINUTE);

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'terminal');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'expired');
  });

  it('defers and then fails on a stale source', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness, 300 * MS_PER_MINUTE, 1440);
    // Simulate a missed weekly refresh: the stored source is older than the
    // 8-day staleness cutoff while the event has not started yet.
    harness.db.database
      .prepare('UPDATE sources SET fetched_at_ms = ?')
      .run(harness.clock.now() - STALE_SOURCE_CUTOFF_MS - 1);

    const deferred = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
    assert.equal(deferred, 'retried');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'stale-source');

    harness.clock.advance(300 * MS_PER_MINUTE);
    const reclaimed = await harness.repository.claimDueJobs(
      'scheduler',
      harness.clock.now(),
      JOB_LEASE_MS,
      100,
    );
    assert.equal(reclaimed.length, 1);
    const failed = await processQueueMessage(
      makeQueueMessage(reclaimed[0]?.jobId ?? ''),
      consumerDeps(harness),
    );
    assert.equal(failed, 'terminal');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'stale-source');
  });

  it('never logs the bot token', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    harness.setHandler(() => jsonResponse({ ok: false }, 500));
    await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
    for (const line of harness.logger.lines) {
      assert.equal(line.includes(TEST_BOT_TOKEN), false);
    }
  });

  it('fails permanently on a non-retryable 4xx without retrying', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    harness.setHandler(() => textResponse('bad request', 400));

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'terminal');
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.status, 'failed');
    assert.equal(job?.last_error_code, 'http_400');
  });

  it('skips a reminder whose user changed course after enqueue', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    await harness.repository.setUserCourse(111, 'extended', harness.clock.now());

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'terminal');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'skip_course-mismatch');
  });

  it('skips a reminder whose user changed the offset after enqueue', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    await harness.repository.setUserReminderOffsets(111, [1440], harness.clock.now());

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'terminal');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'skip_offset-mismatch');
  });

  it('skips a reminder whose occurrence revision changed after enqueue', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const jobId = await dueReminderJob(harness);
    await harness.repository.upsertOccurrences(
      [
        occurrence({
          occurrenceKey: 'a#1',
          startsAtMs: now + 20 * MS_PER_MINUTE,
          summary: 'Moved lesson',
        }),
      ],
      now,
    );

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'terminal');
    assert.equal(
      (await harness.repository.getJob(jobId))?.last_error_code,
      'skip_revision-mismatch',
    );
  });

  it('acks poison queue payloads without crashing or retrying forever', async () => {
    const harness = createHarness();
    const poison = makeQueueMessage('');
    const summary = await processQueueBatch([poison], consumerDeps(harness));
    assert.equal(summary.terminal, 1);
    assert.equal(poison.acked, true);
    assert.equal(poison.retried, false);

    const unknown = makeQueueMessage('job-that-does-not-exist');
    const second = await processQueueBatch([unknown], consumerDeps(harness));
    assert.equal(second.skipped, 1);
    assert.equal(unknown.acked, true);
    assert.equal(unknown.retried, false);
  });

  it('delivers a stop confirmation to a now-inactive user', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(333, 333, now);
    await harness.repository.deactivateUser(333, now);
    await harness.repository.insertCommandJob({
      id: 'cmd-stop',
      telegramUserId: 333,
      chatId: 333,
      payloadJson: JSON.stringify({ text: 'Остановлено' }),
      dedupKey: 'cmd:stop',
      sendAtMs: now,
      now,
      // The /stop confirmation carries the post-deactivation revision (2).
      expectedRevision: (await harness.repository.getUser(333))?.revision ?? null,
    });

    // The stale-work sweep only cancels reminders, never user-visible replies.
    await harness.repository.cancelStaleJobs(now);
    assert.equal((await harness.repository.getJob('cmd-stop'))?.status, 'pending');

    const outcome = await processQueueMessage(makeQueueMessage('cmd-stop'), consumerDeps(harness));
    assert.equal(outcome, 'sent');
  });

  it('terminates a command job whose payload cannot be parsed', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(222, 222, now);
    await harness.repository.insertCommandJob({
      id: 'cmd-bad',
      telegramUserId: 222,
      chatId: 222,
      payloadJson: '{not json',
      dedupKey: 'cmd:bad',
      sendAtMs: now,
      now,
      expectedRevision: (await harness.repository.getUser(222))?.revision ?? null,
    });
    const claimed = await harness.repository.claimDueJobs('scheduler', now, JOB_LEASE_MS, 100);
    assert.equal(claimed.length, 1);

    const outcome = await processQueueMessage(
      makeQueueMessage(claimed[0]?.jobId ?? ''),
      consumerDeps(harness),
    );

    assert.equal(outcome, 'terminal');
    assert.equal((await harness.repository.getJob('cmd-bad'))?.last_error_code, 'bad-payload');
  });

  it('stops retrying after the attempt cap', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    harness.setHandler(() => jsonResponse({ ok: false }, 500));
    const capped = consumerDeps(harness, { maxAttempts: 3 });

    const first = await processQueueMessage(makeQueueMessage(jobId), capped);
    assert.equal(first, 'retried');
    harness.clock.advance(MS_PER_MINUTE);

    for (let attempt = 2; attempt <= 3; attempt += 1) {
      const claimed = await harness.repository.claimDueJobs(
        'scheduler',
        harness.clock.now(),
        JOB_LEASE_MS,
        100,
      );
      assert.equal(claimed.length, 1, `attempt ${attempt} should re-claim the job`);
      const outcome = await processQueueMessage(makeQueueMessage(jobId), capped);
      if (attempt < 3) {
        assert.equal(outcome, 'retried');
        harness.clock.advance(MS_PER_MINUTE);
      } else {
        assert.equal(outcome, 'terminal');
      }
    }

    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.status, 'failed');
    assert.equal(job?.attempt_count, 3);
    assert.equal(
      harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage')).length,
      3,
      'the cap counts real HTTP attempts, one per delivery here',
    );
  });
});

/**
 * Scripts only the atomic reservation verdict; every other repository call
 * runs against the real D1 shim. Pins the consumer-side mapping of each
 * union branch independently of repository internals.
 */
function stubReservationRepository(
  harness: Harness,
  script: (call: number) => SendReservation,
): Repository {
  let calls = 0;
  const inner = harness.repository;
  return new Proxy(inner, {
    get(target, property, _receiver): unknown {
      if (property === 'beginSendAttempt') {
        return async (
          _jobId: string,
          _owner: string,
          _now: number,
          _maxAttempts: number,
        ): Promise<SendReservation> => {
          calls += 1;
          return script(calls);
        };
      }
      const value = Reflect.get(target, property, target) as unknown;
      if (typeof value === 'function') {
        return (value as (...args: never[]) => unknown).bind(target);
      }
      return value;
    },
  }) as Repository;
}

function sendCallCount(harness: Harness): number {
  return harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage')).length;
}

/**
 * Drives a job to exactly `maxAttempts` reserved attempts, then crashes. The
 * expected live timezone of the job's recipient defaults to the seeded reminder
 * fixture; command fixtures without an onboarded user pass `null`.
 */
async function crashAtAttemptCap(
  harness: Harness,
  jobId: string,
  maxAttempts: number,
  userTimeZone: string | null = 'Europe/Moscow',
): Promise<void> {
  const owner = 'crashed-consumer';
  const now = harness.clock.now();
  assert.notEqual(
    await harness.repository.claimJobContext(jobId, owner, now, JOB_LEASE_MS),
    null,
  );
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    assert.deepEqual(
      await harness.repository.beginSendAttempt(jobId, owner, harness.clock.now(), maxAttempts),
      { status: 'reserved', attempt, userTimeZone },
    );
  }
  // The invocation dies before persisting the final outcome; lease repair
  // preserves the count exactly at the cap.
  harness.clock.advance(JOB_LEASE_MS + 1);
  assert.equal(await harness.repository.repairExpiredLeases(harness.clock.now()), 1);
}

describe('atomic attempt-cap reservation (finding 2)', () => {
  it('never sends past the cap after a crash on the final attempt (cap 6)', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    await crashAtAttemptCap(harness, jobId, MAX_JOB_ATTEMPTS);

    let calls = 0;
    harness.setHandler(() => {
      calls += 1;
      return jsonResponse({ ok: true, result: { message_id: 1 } });
    });
    const message = makeQueueMessage(jobId);
    const summary = await processQueueBatch(
      [message],
      consumerDeps(harness, { maxAttempts: MAX_JOB_ATTEMPTS }),
    );

    assert.equal(calls, 0, 'no Telegram call may start at the persisted cap');
    assert.equal(summary.terminal, 1);
    assert.equal(message.acked, true, 'the exhausted job is acknowledged, not retried');
    assert.equal(message.retried, false);
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.status, 'failed');
    assert.equal(job?.last_error_code, 'attempts-exhausted');
    assert.equal(Number(job?.attempt_count), MAX_JOB_ATTEMPTS);
  });

  it('never sends past the cap after a crash on the final attempt (cap 1)', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    await crashAtAttemptCap(harness, jobId, 1);

    let calls = 0;
    harness.setHandler(() => {
      calls += 1;
      return jsonResponse({ ok: true, result: { message_id: 1 } });
    });
    const summary = await processQueueBatch([makeQueueMessage(jobId)], consumerDeps(harness, { maxAttempts: 1 }));

    assert.equal(calls, 0);
    assert.equal(summary.terminal, 1);
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.status, 'failed');
    assert.equal(job?.last_error_code, 'attempts-exhausted');
    assert.equal(Number(job?.attempt_count), 1);
  });

  it('never sends a command past the cap after a crash on the final attempt', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(777, 777, now);
    await harness.repository.insertCommandJob({
      id: 'cmd-cap',
      telegramUserId: 777,
      chatId: 777,
      payloadJson: JSON.stringify({ text: 'menu' }),
      dedupKey: 'cmd-cap',
      sendAtMs: now,
      now,
      expectedRevision: 1,
      sourceUpdateId: 9,
    });
    await crashAtAttemptCap(harness, 'cmd-cap', 1, null);

    let calls = 0;
    harness.setHandler(() => {
      calls += 1;
      return jsonResponse({ ok: true, result: { message_id: 1 } });
    });
    const summary = await processQueueBatch([makeQueueMessage('cmd-cap')], consumerDeps(harness, { maxAttempts: 1 }));

    assert.equal(calls, 0);
    assert.equal(summary.terminal, 1);
    const job = await harness.repository.getJob('cmd-cap');
    assert.equal(job?.status, 'failed');
    assert.equal(job?.last_error_code, 'attempts-exhausted');
    assert.equal(Number(job?.attempt_count), 1);
  });

  it('never starts the in-place 429 retry once the cap is reached', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    await crashAtAttemptCap(harness, jobId, MAX_JOB_ATTEMPTS);

    let calls = 0;
    harness.setHandler(() => {
      calls += 1;
      return jsonResponse({ ok: false, parameters: { retry_after: 1 } }, 429);
    });
    const summary = await processQueueBatch(
      [makeQueueMessage(jobId)],
      consumerDeps(harness, { maxAttempts: MAX_JOB_ATTEMPTS }),
    );

    assert.equal(calls, 0, 'the exhausted first reservation must not send at all');
    assert.equal(summary.terminal, 1);
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'attempts-exhausted');
  });

  it('counts the in-place retry as an attempt and terminates at the cap', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    // Four attempts already started: the in-place retry consumes the 6th.
    const owner = 'crashed-consumer';
    const now = harness.clock.now();
    assert.notEqual(
      await harness.repository.claimJobContext(jobId, owner, now, JOB_LEASE_MS),
      null,
    );
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      await harness.repository.beginSendAttempt(jobId, owner, now, MAX_JOB_ATTEMPTS);
    }
    harness.clock.advance(JOB_LEASE_MS + 1);
    await harness.repository.repairExpiredLeases(harness.clock.now());

    harness.setHandler(() => jsonResponse({ ok: false, parameters: { retry_after: 1 } }, 429));
    const summary = await processQueueBatch(
      [makeQueueMessage(jobId)],
      consumerDeps(harness, { maxAttempts: MAX_JOB_ATTEMPTS }),
    );

    assert.equal(summary.terminal, 1);
    assert.equal(sendCallCount(harness), 2, 'initial send plus exactly one in-place retry');
    const job = await harness.repository.getJob(jobId);
    assert.equal(Number(job?.attempt_count), MAX_JOB_ATTEMPTS);
    assert.equal(job?.status, 'failed');
    assert.equal(job?.last_error_code, 'rate_limited');
  });

  it('maps an exhausted reservation to terminal without sending', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    const deps = consumerDeps(harness, {
      repository: stubReservationRepository(harness, () => ({ status: 'exhausted' })),
    });
    const message = makeQueueMessage(jobId);
    const summary = await processQueueBatch([message], deps);

    assert.equal(sendCallCount(harness), 0);
    assert.equal(summary.terminal, 1);
    assert.equal(message.acked, true);
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.status, 'failed');
    assert.equal(job?.last_error_code, 'attempts-exhausted');
  });

  it('retries the message when the reservation reports lost custody', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    const deps = consumerDeps(harness, {
      repository: stubReservationRepository(harness, () => ({ status: 'lost' })),
    });
    const message = makeQueueMessage(jobId);
    const summary = await processQueueBatch([message], deps);

    assert.equal(sendCallCount(harness), 0);
    assert.equal(message.acked, false, 'lost work must not be acknowledged');
    assert.equal(message.retried, true, 'lost work is redelivered');
    assert.equal(summary.sent, 0);
    assert.equal(summary.terminal, 0);
    // The job keeps its lease; the redelivery can still complete it.
    assert.equal((await harness.repository.getJob(jobId))?.status, 'leased');
  });

  it('maps a superseded reservation to terminal without sending', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(778, 778, now);
    await harness.repository.insertCommandJob({
      id: 'cmd-super',
      telegramUserId: 778,
      chatId: 778,
      payloadJson: JSON.stringify({ text: 'menu' }),
      dedupKey: 'cmd-super',
      sendAtMs: now,
      now,
      expectedRevision: 1,
      sourceUpdateId: 3,
    });
    const deps = consumerDeps(harness, {
      repository: stubReservationRepository(harness, () => ({ status: 'superseded' })),
    });
    const message = makeQueueMessage('cmd-super');
    const summary = await processQueueBatch([message], deps);

    assert.equal(sendCallCount(harness), 0);
    assert.equal(summary.terminal, 1);
    assert.equal(message.acked, true);
    const job = await harness.repository.getJob('cmd-super');
    assert.equal(job?.status, 'cancelled');
    assert.equal(job?.last_error_code, 'superseded');
  });

  it('checks the reservation again before the in-place retry send', async () => {
    const harness = createHarness();
    const jobId = await dueReminderJob(harness);
    const deps = consumerDeps(harness, {
      repository: stubReservationRepository(harness, (call) =>
        call === 1
          ? { status: 'reserved', attempt: 1, userTimeZone: 'Europe/Moscow' }
          : { status: 'exhausted' },
      ),
    });
    harness.setHandler(() => jsonResponse({ ok: false, parameters: { retry_after: 1 } }, 429));
    const summary = await processQueueBatch([makeQueueMessage(jobId)], deps);

    assert.equal(sendCallCount(harness), 1, 'the denied retry must not send again');
    assert.equal(summary.terminal, 1);
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'attempts-exhausted');
  });
});

async function seedCommandJob(
  harness: Harness,
  id: string,
  userId: number,
  text: string,
  updateId: number | null,
  expectedRevision: number | null,
): Promise<string> {
  const now = harness.clock.now();
  await harness.repository.insertCommandJob({
    id,
    telegramUserId: userId,
    chatId: userId,
    payloadJson: JSON.stringify({ text }),
    dedupKey: `cmd:${id}`,
    sendAtMs: now,
    now,
    expectedRevision,
    sourceUpdateId: updateId,
  });
  return id;
}

function sentTexts(harness: Harness): string[] {
  return harness.fetchSpy.calls
    .filter((call) => call.url.includes('/sendMessage'))
    .map((call) => (JSON.parse(String(call.init?.body)) as { text: string }).text);
}

describe('command update ordering at the consumer (finding 4)', () => {
  it('supersedes a pending older command without reserving a send', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(901, 901, now);
    await seedCommandJob(harness, 'cmd-old', 901, 'old guidance', 11, 1);
    await harness.repository.recordCommandUpdate(901, 901, 12, now);

    harness.db.stats.reset();
    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    const outcome = await processQueueMessage(makeQueueMessage('cmd-old'), consumerDeps(harness));
    const statements = harness.repository.statementsUsed();

    assert.equal(outcome, 'terminal');
    assert.equal(sendCallCount(harness), 0);
    const job = await harness.repository.getJob('cmd-old');
    assert.equal(job?.status, 'cancelled');
    assert.equal(job?.last_error_code, 'superseded');
    assert.equal(
      statements,
      2,
      'claim plus terminal supersede; screening reads the claim-time join',
    );
  });

  it('supersedes an enqueued older command in the same way', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(902, 902, now);
    await seedCommandJob(harness, 'cmd-old-q', 902, 'old guidance', 11, 1);
    await harness.repository.recordCommandUpdate(902, 902, 12, now);
    const claimed = await harness.repository.claimDueJobs('scheduler', now, JOB_LEASE_MS, 100);
    assert.equal(claimed.length, 1);

    const outcome = await processQueueMessage(makeQueueMessage('cmd-old-q'), consumerDeps(harness));

    assert.equal(outcome, 'terminal');
    assert.equal(sendCallCount(harness), 0);
    assert.equal((await harness.repository.getJob('cmd-old-q'))?.last_error_code, 'superseded');
  });

  for (const order of ['fifo', 'lifo'] as const) {
    it(`delivers only the newest guidance in ${order} queue order`, async () => {
      const harness = createHarness();
      const now = harness.clock.now();
      await harness.repository.activateUser(903, 903, now);
      await seedCommandJob(harness, 'cmd-old-o', 903, 'old guidance', 11, 1);
      await seedCommandJob(harness, 'cmd-new-o', 903, 'new guidance', 12, 1);
      await harness.repository.recordCommandUpdate(903, 903, 11, now);
      await harness.repository.recordCommandUpdate(903, 903, 12, now);

      const sequence = order === 'fifo' ? ['cmd-old-o', 'cmd-new-o'] : ['cmd-new-o', 'cmd-old-o'];
      const outcomes: string[] = [];
      for (const jobId of sequence) {
        outcomes.push(await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness)));
      }

      assert.deepEqual(outcomes.sort(), ['sent', 'terminal']);
      assert.deepEqual(sentTexts(harness), ['new guidance']);
      assert.equal((await harness.repository.getJob('cmd-old-o'))?.last_error_code, 'superseded');
      assert.equal((await harness.repository.getJob('cmd-new-o'))?.status, 'sent');
    });
  }

  it('acknowledges a duplicate queue delivery without a second send', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(904, 904, now);
    await seedCommandJob(harness, 'cmd-dup', 904, 'guidance', 21, 1);
    await harness.repository.recordCommandUpdate(904, 904, 21, now);

    const first = await processQueueMessage(makeQueueMessage('cmd-dup'), consumerDeps(harness));
    const message = makeQueueMessage('cmd-dup');
    const summary = await processQueueBatch([message], consumerDeps(harness));

    assert.equal(first, 'sent');
    assert.equal(summary.skipped, 1);
    assert.equal(message.acked, true);
    assert.deepEqual(sentTexts(harness), ['guidance']);
  });

  it('supersedes a leased old job when a newer update lands mid-race', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(905, 905, now);
    await seedCommandJob(harness, 'cmd-race-old', 905, 'old guidance', 31, 1);
    await harness.repository.recordCommandUpdate(905, 905, 31, now);
    const raceOwner = 'race-consumer';
    assert.notEqual(
      await harness.repository.claimJobContext('cmd-race-old', raceOwner, now, JOB_LEASE_MS),
      null,
    );
    // A newer update lands (and is recorded) while the old job is leased.
    await seedCommandJob(harness, 'cmd-race-new', 905, 'new guidance', 32, 1);
    await harness.repository.recordCommandUpdate(905, 905, 32, now);

    const staleOutcome = await processQueueMessage(
      makeQueueMessage('cmd-race-old'),
      consumerDeps(harness, { ownerFactory: () => raceOwner }),
    );
    const freshOutcome = await processQueueMessage(
      makeQueueMessage('cmd-race-new'),
      consumerDeps(harness),
    );

    assert.equal(staleOutcome, 'terminal');
    assert.equal(freshOutcome, 'sent');
    assert.deepEqual(sentTexts(harness), ['new guidance']);
    assert.equal((await harness.repository.getJob('cmd-race-old'))?.last_error_code, 'superseded');
  });

  it('supersedes the in-place retry when a newer update lands during the cooldown wait', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(906, 906, now);
    await seedCommandJob(harness, 'cmd-retry', 906, 'guidance', 41, 1);
    await harness.repository.recordCommandUpdate(906, 906, 41, now);

    harness.setHandler(() => {
      // A newer update is recorded while the first send is in flight; the
      // short 429 then forces the in-place retry path to re-reserve.
      void harness.repository.recordCommandUpdate(906, 906, 42, harness.clock.now());
      return jsonResponse({ ok: false, parameters: { retry_after: 1 } }, 429);
    });
    const summary = await processQueueBatch([makeQueueMessage('cmd-retry')], consumerDeps(harness));

    assert.equal(sendCallCount(harness), 1, 'the superseded retry must not send again');
    assert.equal(summary.terminal, 1);
    const job = await harness.repository.getJob('cmd-retry');
    assert.equal(job?.status, 'cancelled');
    assert.equal(job?.last_error_code, 'superseded');
    assert.equal(Number(job?.attempt_count), 1, 'the denied retry increments nothing');
  });

  it('still delivers a legacy command without ordering evidence', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(907, 907, now);
    await seedCommandJob(harness, 'cmd-legacy', 907, 'legacy guidance', null, 1);
    await harness.repository.recordCommandUpdate(907, 907, 99, now);

    const outcome = await processQueueMessage(makeQueueMessage('cmd-legacy'), consumerDeps(harness));

    assert.equal(outcome, 'sent', 'NULL source_update_id stays drainable/deliverable');
    assert.deepEqual(sentTexts(harness), ['legacy guidance']);
  });

  it('sends a current command in five statements', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(908, 908, now);
    await seedCommandJob(harness, 'cmd-cost', 908, 'guidance', 51, 1);
    await harness.repository.recordCommandUpdate(908, 908, 51, now);

    harness.db.stats.reset();
    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    const outcome = await processQueueMessage(makeQueueMessage('cmd-cost'), consumerDeps(harness));
    const statements = harness.repository.statementsUsed();

    assert.equal(outcome, 'sent');
    assert.equal(
      statements,
      5,
      'claim, sliding slot (1), reserve, sent (2 with ledger); screening reads the claim-time join',
    );
  });
});
