import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  MAX_INFLIGHT_SENDS,
  processQueueBatch,
  processQueueMessage,
} from '../src/queue/consumer.ts';
import { runSchedulerTick } from '../src/scheduler/tick.ts';
import {
  MAX_D1_STATEMENTS_PER_CONSUMER,
  MAX_D1_STATEMENTS_PER_SCHEDULER,
  MESSAGE_BUDGET_FLOOR,
  MS_PER_MINUTE,
} from '../src/util.ts';
import { SEND_PACE_WINDOW_MS } from '../src/data/repository.ts';
import {
  findPacingViolations,
  runPacingBoundaryProbe,
  runTargetSimulation,
} from './helpers/simulation.ts';
import { jsonResponse } from './helpers/fakes.ts';
import { consumerDeps, createHarness, schedulerDeps, type Harness } from './helpers/harness.ts';
import {
  makeQueueMessage,
  occurrence,
  onboardUser,
  seedDueJobs,
  seedReminderOffsets,
  seedSource,
} from './helpers/seed.ts';

const MAX_BOUND_PARAMS = 100;

async function enqueueDueReminders(harness: Harness): Promise<string[]> {
  harness.queue.batches.length = 0;
  await runSchedulerTick(schedulerDeps(harness));
  return harness.queue.batches.flat().map((message) => message.jobId);
}

describe('end-to-end delivery', () => {
  it('processes a 10-message typical batch across one invocation plus its redelivery', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const ids = await seedDueJobs(harness, 10);
    const messages = ids.map(makeQueueMessage);

    harness.db.stats.reset();
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    // Measured rolling admission with the ten-statement floor: the batch prune
    // plus eight five-statement sends fit (41 statements), the reserve for a
    // ninth would pass the limit, so two defer as Queue retries, not losses.
    assert.equal(ids.length, 10);
    assert.equal(summary.sent, 8, 'rolling admission releases reserves as tasks complete');
    assert.equal(summary.deferred, 2);
    assert.equal(messages.filter((message) => message.acked).length, 8);
    assert.ok(
      harness.db.stats.statements <= MAX_D1_STATEMENTS_PER_CONSUMER,
      `used ${harness.db.stats.statements} statements`,
    );
    assert.ok(harness.db.stats.maxParamsPerStatement <= MAX_BOUND_PARAMS);
    const redelivery = messages
      .flatMap((message, index) => (message.retried ? [ids[index] ?? ''] : []))
      .map(makeQueueMessage);
    const second = await processQueueBatch(redelivery, consumerDeps(harness));
    assert.equal(second.sent, 2);
    for (const id of ids) {
      assert.equal((await harness.repository.getJob(id))?.status, 'sent');
    }
  });

  it('never runs more than five Telegram sends concurrently', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const jobIds = await seedDueJobs(harness, 10);
    assert.equal(jobIds.length, 10);

    let inFlight = 0;
    let maxInFlight = 0;
    let sends = 0;
    // Every send parks for one macrotask, so an unbounded batch would let all
    // ten handlers pile up before any of them settles.
    harness.setHandler(async () => {
      sends += 1;
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await new Promise((resolve) => setTimeout(resolve, 0));
      inFlight -= 1;
      return jsonResponse({ ok: true, result: { message_id: sends } });
    });

    const messages = jobIds.map(makeQueueMessage);
    harness.db.stats.reset();
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    assert.ok(
      maxInFlight <= MAX_INFLIGHT_SENDS,
      `an unbounded batch would have piled up more than ${MAX_INFLIGHT_SENDS} sends`,
    );
    assert.ok(maxInFlight >= 2, 'the batch does run sends in parallel');
    assert.equal(sends, 8);
    assert.equal(summary.sent, 8);
    assert.equal(summary.deferred, 2);
    assert.equal(messages.filter((message) => message.acked).length, 8);
    assert.ok(
      harness.db.stats.statements <= MAX_D1_STATEMENTS_PER_CONSUMER,
      `used ${harness.db.stats.statements} statements`,
    );
    const tail = messages
      .flatMap((message, index) => (message.retried ? [jobIds[index] ?? ''] : []))
      .map(makeQueueMessage);
    const second = await processQueueBatch(tail, consumerDeps(harness));
    assert.equal(second.sent, 2);
    assert.equal(sends, 10);
    assert.ok(tail.every((message) => message.acked));
  });

  it('applies a 429 cooldown to every later message without starting another send', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const jobIds = await seedDueJobs(harness, 10);

    let arrived = 0;
    harness.setHandler(() => {
      arrived += 1;
      return jsonResponse({ ok: false, parameters: { retry_after: 300 } }, 429);
    });

    const messages = jobIds.map(makeQueueMessage);
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    // The first wave of sends learns the cooldown; every later message defers
    // on it without another HTTP call, all inside the same invocation budget.
    // Measured with the ten-statement floor: nine admit as paced reschedules,
    // the tenth no reserve covers defers as a Queue retry, not a loss.
    assert.ok(
      arrived > 0 && arrived <= MAX_INFLIGHT_SENDS,
      `only the pre-cooldown wave may send, saw ${arrived}`,
    );
    assert.equal(summary.retried, 9, 'every admitted message is rescheduled, not lost');
    assert.equal(summary.deferred, 1);
    assert.equal(summary.sent, 0);
    assert.equal(messages.filter((message) => message.acked).length, 9);
    const pace = await harness.repository.getPaceState();
    assert.ok((pace?.cooldownUntilMs ?? 0) > harness.clock.now());
    const waveSends = arrived;

    // The deferred tail is a Queue retry: the next invocation reschedules it
    // the same paced way without an HTTP call.
    const tail = messages.flatMap((message, index) =>
      message.retried && !message.acked ? [jobIds[index] ?? ''] : [],
    );
    assert.equal(tail.length, 1);
    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    assert.equal(
      await processQueueMessage(makeQueueMessage(tail[0] ?? ''), consumerDeps(harness)),
      'retried',
    );
    assert.equal(arrived, waveSends, 'no further HTTP call may start during the cooldown');

    // A fresh delivery while the cooldown is live defers without an HTTP call
    // and without consuming an attempt.
    const cooledJobId = jobIds[waveSends] ?? '';
    harness.clock.advance(MS_PER_MINUTE);
    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    assert.equal(
      await processQueueMessage(makeQueueMessage(cooledJobId), consumerDeps(harness)),
      'retried',
    );
    assert.equal(arrived, waveSends, 'no further HTTP call may start during the cooldown');
    assert.equal((await harness.repository.getJob(cooledJobId))?.last_error_code, 'paced');
    assert.equal(Number((await harness.repository.getJob(cooledJobId))?.attempt_count), 0);
  });

  it('never duplicates sends when a paced forbidden tail would exhaust the budget', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const jobIds = await seedDueJobs(harness, 10);
    const now = harness.clock.now();
    // Fifteen slots consumed: the first five successes fill the pace window, so
    // the remaining sends must pace-wait and cost the seven-statement maximum.
    for (let index = 0; index < 15; index += 1) {
      await harness.repository.acquireSendSlot(now, 20);
    }

    let sends = 0;
    let minRemainingAtSend = Number.POSITIVE_INFINITY;
    const callsPerChat = new Map<number, number>();
    harness.setHandler((url, init) => {
      if (!url.includes('/sendMessage')) {
        return jsonResponse({ ok: true, result: { message_id: 1 } });
      }
      sends += 1;
      minRemainingAtSend = Math.min(minRemainingAtSend, harness.repository.remainingBudget() ?? 0);
      const chatId = (JSON.parse(String(init?.body)) as { chat_id: number }).chat_id;
      callsPerChat.set(chatId, (callsPerChat.get(chatId) ?? 0) + 1);
      return sends <= 5
        ? jsonResponse({ ok: true, result: { message_id: sends } })
        : jsonResponse({ ok: false, description: 'Forbidden' }, 403);
    });

    const firstMessages = jobIds.map(makeQueueMessage);
    harness.db.stats.reset();
    const first = await processQueueBatch(firstMessages, consumerDeps(harness));
    assert.ok(
      harness.repository.statementsUsed() <= MAX_D1_STATEMENTS_PER_CONSUMER,
      `first invocation used ${harness.repository.statementsUsed()} statements`,
    );
    assert.equal(first.sent, 5);
    assert.equal(first.terminal, 3);
    assert.equal(first.deferred, 2, 'only the messages no reserve can cover are deferred');
    assert.ok(
      minRemainingAtSend >= 2,
      `a started send must keep room to persist its outcome, saw ${minRemainingAtSend}`,
    );

    // The deferred message is a Queue retry, not lost: the next invocation has
    // a fresh budget and completes it with its outcome persisted.
    const leftover = jobIds.filter((_, index) => {
      const message = firstMessages[index];
      return message !== undefined && message.retried && !message.acked;
    });
    assert.equal(leftover.length, 2);
    const secondMessages = leftover.map(makeQueueMessage);
    harness.db.stats.reset();
    const second = await processQueueBatch(secondMessages, consumerDeps(harness));
    assert.equal(second.terminal, 2);
    assert.equal(second.deferred, 0);
    assert.ok(secondMessages.every((message) => message.acked));
    assert.ok(
      harness.repository.statementsUsed() <= MAX_D1_STATEMENTS_PER_CONSUMER,
      `second invocation used ${harness.repository.statementsUsed()} statements`,
    );

    assert.equal(sends, 10);
    for (const jobId of jobIds) {
      const job = await harness.repository.getJob(jobId);
      assert.ok(
        job?.status === 'sent' || job?.status === 'cancelled',
        `job ${jobId} must persist its send outcome, got ${String(job?.status)}`,
      );
      const chatId = Number(job?.telegram_user_id);
      assert.equal(
        Number(job?.attempt_count),
        callsPerChat.get(chatId) ?? 0,
        'attempt_count must equal the started calls for the job',
      );
    }
    for (const [chatId, count] of callsPerChat) {
      assert.equal(count, 1, `chat ${chatId} must receive exactly one sendMessage`);
    }
  });

  it('admits as much forbidden work as the rolling reserve allows', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const jobIds = await seedDueJobs(harness, 10);

    let arrived = 0;
    harness.setHandler(() => {
      arrived += 1;
      return jsonResponse({ ok: false, description: 'Forbidden' }, 403);
    });

    const messages = jobIds.map(makeQueueMessage);
    harness.db.stats.reset();
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    // Each 403 costs five statements (prune included once per batch) while
    // the roll reserves ten, so the invocation admits eight of ten and defers
    // the last two untouched.
    assert.equal(summary.terminal, 8);
    assert.equal(summary.deferred, 2);
    assert.equal(arrived, 8, 'the deferred messages must start no HTTP call');
    assert.equal(harness.repository.statementsUsed(), 41);
    assert.ok(harness.repository.statementsUsed() <= MAX_D1_STATEMENTS_PER_CONSUMER);
    const deferred = messages.filter((message) => message.retried && !message.acked);
    assert.equal(deferred.length, 2);

    // The deferred messages complete in the next invocation with persisted
    // outcomes and no duplicate call anywhere.
    const tailMessages = deferred.map((message) => {
      const index = messages.indexOf(message);
      return makeQueueMessage(jobIds[index] ?? '');
    });
    const second = await processQueueBatch(tailMessages, consumerDeps(harness));
    assert.equal(second.terminal, 2);
    assert.equal(arrived, 10);
    assert.ok(tailMessages.every((message) => message.acked));
  });

  it('starts a paced send only with budget left to persist its outcome', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const jobIds = await seedDueJobs(harness, 1);
    const jobId = jobIds[0] ?? '';
    const now = harness.clock.now();
    for (let index = 0; index < 20; index += 1) {
      await harness.repository.acquireSendSlot(now, 20);
    }

    let remainingAtSend: number | null = null;
    harness.setHandler((url) => {
      if (url.includes('/sendMessage')) {
        remainingAtSend = harness.repository.remainingBudget();
      }
      return jsonResponse({ ok: false, description: 'Forbidden' }, 403);
    });

    // The admission-unit budget covers the paced worst case; at send time the
    // deactivate plus terminal outcome must still be affordable from what is
    // left.
    harness.repository.beginInvocation(MESSAGE_BUDGET_FLOOR);
    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
    const used = harness.repository.statementsUsed();

    assert.equal(outcome, 'terminal');
    assert.ok(
      (remainingAtSend ?? 0) >= 2,
      `deactivate plus terminal outcome must still be affordable at send time, left ${String(remainingAtSend)}`,
    );
    assert.ok(used <= MESSAGE_BUDGET_FLOOR, `used ${used} of ${MESSAGE_BUDGET_FLOOR}`);
    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    assert.equal((await harness.repository.getJob(jobId))?.status, 'cancelled');
  });

  it('sustains 1000 deliveries with latency, duplicates and controlled failures', async () => {
    const start = 1_700_000_000_000;
    const harness = createHarness({ now: start });
    await seedSource(harness.repository, 'basic', start);

    const recipients = 1000;
    for (let index = 1; index <= recipients; index += 1) {
      await harness.repository.activateUser(index, index, start);
      onboardUser(harness, index);
      seedReminderOffsets(harness, index, [30]);
    }
    // A 30-minute reminder for an event starting in 31 minutes is not due at the
    // first cron tick; it becomes due exactly one tick (60s) later.
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'shared#1', startsAtMs: start + 31 * MS_PER_MINUTE })],
      start,
    );

    const networkLatencyMs = 25;
    const failOnce = new Set<number>();
    for (let index = 1; index <= recipients; index += 40) {
      failOnce.add(index);
    }
    const attempts = new Map<number, number>();
    const deliveredAt = new Map<number, number>();
    const deliveredCount = new Map<number, number>();

    harness.setHandler((url, init) => {
      if (!url.includes('/sendMessage')) {
        return jsonResponse({ ok: true, result: { message_id: 1 } });
      }
      harness.clock.advance(networkLatencyMs);
      const body = JSON.parse(String(init?.body)) as { chat_id: number };
      const chatId = body.chat_id;
      const attempt = (attempts.get(chatId) ?? 0) + 1;
      attempts.set(chatId, attempt);
      if (failOnce.has(chatId) && attempt === 1) {
        return jsonResponse({ ok: false }, 500);
      }
      deliveredAt.set(chatId, harness.clock.now());
      deliveredCount.set(chatId, (deliveredCount.get(chatId) ?? 0) + 1);
      return jsonResponse({ ok: true, result: { message_id: attempt } });
    });

    const processMessages = async (
      ids: string[],
    ): Promise<{ sent: number; retried: number; terminal: number; skipped: number }> => {
      let sent = 0;
      let retried = 0;
      let terminal = 0;
      let skipped = 0;
      // Five-message invocations keep the pacing deterministic at this scale;
      // the ten-message rolling admission is covered by its own regressions.
      for (let cursor = 0; cursor < ids.length; cursor += 5) {
        const batch = ids.slice(cursor, cursor + 5).map(makeQueueMessage);
        harness.db.stats.reset();
        const summary = await processQueueBatch(batch, consumerDeps(harness));
        assert.ok(
          harness.db.stats.statements <= MAX_D1_STATEMENTS_PER_CONSUMER,
          `batch used ${harness.db.stats.statements} statements`,
        );
        assert.ok(harness.db.stats.maxParamsPerStatement <= MAX_BOUND_PARAMS);
        assert.equal(summary.deferred, 0, 'a five-message invocation must fit the budget');
        assert.ok(batch.every((message) => message.acked));
        sent += summary.sent;
        retried += summary.retried;
        terminal += summary.terminal;
        skipped += summary.skipped;
      }
      return { sent, retried, terminal, skipped };
    };

    // Cron tick at start: nothing is due yet.
    const nothingDue = await enqueueDueReminders(harness);
    assert.equal(nothingDue.length, 0, 'no reminders should be due yet');
    assert.ok(
      harness.repository.statementsUsed() <= MAX_D1_STATEMENTS_PER_SCHEDULER,
      `scheduler tick used ${harness.repository.statementsUsed()} statements`,
    );

    // Cron tick one minute later: every reminder becomes due and is enqueued.
    harness.clock.advance(MS_PER_MINUTE);
    const pending = await enqueueDueReminders(harness);
    assert.equal(pending.length, recipients);
    assert.ok(
      harness.repository.statementsUsed() <= MAX_D1_STATEMENTS_PER_SCHEDULER,
      `scheduler tick used ${harness.repository.statementsUsed()} statements`,
    );
    assert.ok(
      harness.queue.batches.every((batch) => batch.length > 0 && batch.length <= 100),
      'queue.sendBatch must never exceed 100 ID payloads',
    );
    const dueAt = harness.clock.now();

    // Duplicate every tenth delivery to exercise lease overlap at scale.
    const withDuplicates: string[] = [];
    pending.forEach((jobId, index) => {
      withDuplicates.push(jobId);
      if (index % 10 === 0) {
        withDuplicates.push(jobId);
      }
    });

    const firstPass = await processMessages(withDuplicates);
    // Every message is accounted: sent, job-level retried or a skipped
    // duplicate. A rate-limited-by-pacing job may also be retried, so the
    // transient count is a lower bound, not an exact split.
    assert.equal(
      firstPass.sent + firstPass.retried + firstPass.terminal,
      recipients,
      'every delivery must reach a persisted outcome',
    );
    assert.ok(firstPass.retried >= failOnce.size, 'the controlled failures are retried');
    assert.equal(firstPass.skipped, 100, 'duplicate deliveries must be acknowledged without sending');
    assert.equal(firstPass.terminal, 0);

    // Normal delivery is bounded by the 180s window from the due tick.
    const normalDeliveries = [...deliveredAt.entries()]
      .filter(([chatId]) => !failOnce.has(chatId))
      .map(([, at]) => at);
    assert.ok(
      Math.max(...normalDeliveries) - dueAt <= 180_000,
      `normal delivery exceeded the window by ${Math.max(...normalDeliveries) - dueAt}ms`,
    );

    // The controlled failures (and any paced deferral) back off and are
    // delivered on later ticks; each tick has its own invocation budget.
    const retryTick = harness.clock.now() + MS_PER_MINUTE;
    let pendingCount = firstPass.retried;
    for (let tick = 0; deliveredAt.size < recipients && pendingCount > 0; tick += 1) {
      assert.ok(tick < 5, 'paced deferrals must drain across later ticks');
      harness.clock.advance(MS_PER_MINUTE);
      const reenqueued = await enqueueDueReminders(harness);
      assert.equal(reenqueued.length, pendingCount, 'every retried job is re-enqueued');
      const pass = await processMessages(reenqueued);
      pendingCount = pass.retried;
    }

    // Each recipient was delivered exactly once, and after its own backoff.
    assert.equal(deliveredAt.size, recipients, 'no job may be lost');
    for (const count of deliveredCount.values()) {
      assert.equal(count, 1, 'no recipient may be delivered twice');
    }
    const retriedDeliveries = [...deliveredAt.entries()]
      .filter(([chatId]) => failOnce.has(chatId))
      .map(([, at]) => at);
    assert.equal(retriedDeliveries.length, failOnce.size);
    assert.ok(
      Math.min(...retriedDeliveries) >= retryTick - networkLatencyMs,
      'retried deliveries must wait for their backoff, not the normal path',
    );
    assert.ok(
      Math.max(...retriedDeliveries) - dueAt <= 180_000,
      'even the controlled retries land inside the simulated window',
    );
  });
});

describe('rolling one-second pacer under reset-boundary traffic (finding 5)', () => {
  it('detects the old fixed-window burst shape, so the detector is not vacuous', () => {
    // 1 start at 0 ms, 19 at 900 ms, 20 at 1000 ms: the fixed-window gate
    // grants all 40, putting 39 starts inside one rolling second.
    const starts = [{ atMs: 0 }];
    for (let index = 0; index < 19; index += 1) {
      starts.push({ atMs: 900 });
    }
    for (let index = 0; index < 20; index += 1) {
      starts.push({ atMs: 1_000 });
    }
    assert.ok(
      findPacingViolations(starts, 20, SEND_PACE_WINDOW_MS) > 0,
      '39 starts in a rolling second must be flagged',
    );
    const compliant = Array.from({ length: 40 }, (_, index) => ({
      atMs: Math.floor(index / 20) * 1_000 + (index % 20),
    }));
    assert.equal(findPacingViolations(compliant, 20, SEND_PACE_WINDOW_MS), 0);
  });

  it('grants the boundary bursts without a rolling violation', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const probe = await runPacingBoundaryProbe(harness.repository, harness.clock.now());

    assert.equal(
      probe.grantTimesMs.length,
      21,
      `the T+1000 burst must almost entirely deny, granted ${probe.grantTimesMs.length}`,
    );
    assert.equal(probe.violations, 0, 'granted boundary starts hold the rolling pace');
  });

  it('holds zero rolling violations with boundary traffic on the 1,000-user run', { timeout: 280_000 }, async () => {
    const metrics = await runTargetSimulation({
      recipients: 1_000,
      cronAlignmentMs: MS_PER_MINUTE,
      pacingBoundaryProbe: true,
    });

    assert.equal(metrics.timedOut, false);
    assert.equal(metrics.unprocessedJobs, 0);
    assert.equal(metrics.lostJobs, 0);
    assert.deepEqual(metrics.duplicateSuccessChatIds, []);
    assert.deepEqual(metrics.attemptMismatchJobIds, []);
    assert.equal(metrics.pacingProbeViolations, 0, 'adversarial boundary grants hold the pace');
    assert.equal(metrics.pacingViolations, 0, 'the 1,000-user run holds the rolling pace');
    assert.equal(metrics.cooldownViolations, 0);
    assert.equal(metrics.completionSamples, 1_000);
    assert.ok(
      metrics.maxCompletionFromSendAtMs !== null && metrics.maxCompletionFromSendAtMs <= 180_000,
      `1,000-user bound ${String(metrics.maxCompletionFromSendAtMs)}ms exceeds 180 s`,
    );
  });
});
