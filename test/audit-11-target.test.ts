/**
 * F11 acceptance: the 1,000-recipient delivery target under a deterministic
 * virtual-time simulation (test/helpers/simulation.ts).
 *
 * Scenario: 1,000 subscribed recipients across the basic (30-minute offset) and
 * extended (1-day offset) courses; every reminder's logical `send_at` is the
 * simulation start. One cron tick fires with the worst allowed 60 s alignment;
 * later minute ticks overlap the draining backlog. The Queue delivers 10-message
 * batches with a 1 s batch timeout and concurrency 1. Telegram answers after a
 * deterministic 200 ms mean latency, one request gets a 429 with retry_after,
 * and every 37th queue delivery is duplicated (at-least-once). Nothing sleeps
 * for real and no network request is made.
 *
 * All measurements are taken from each reminder's logical `send_at`, never from
 * the tick that discovered it. The assertions below pin the modeled conditions
 * themselves (tick alignment, handoff, latency, batch shape, overlapping ticks,
 * delivered duplicates) so the target cannot pass on a vacuous scenario, and
 * the negative controls prove the pacing/cooldown/attempt detectors fail on a
 * fabricated violation. Lease expiry/recovery is a separate scenario: a 60 s
 * lease plus 60 s tick alignment mathematically cannot recover before the next
 * tick, so the 180 s normal-delivery bound cannot include a crash.
 */

import { before, describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { SEND_PACE_WINDOW_MS } from '../src/data/repository.ts';
import {
  BOT_SEND_PACE_PER_SECOND,
  MESSAGE_BUDGET_FLOOR,
  MS_PER_MINUTE,
} from '../src/util.ts';
import {
  findCooldownViolations,
  findPacingViolations,
  QUEUE_BATCH_SIZE,
  QUEUE_BATCH_TIMEOUT_MS,
  QUEUE_CONSUMER_CONCURRENCY,
  runTargetSimulation,
  type TargetSimulationMetrics,
} from './helpers/simulation.ts';

const RECIPIENTS = 1000;
const DELIVERY_WINDOW_MS = 180_000;
const WORST_CRON_ALIGNMENT_MS = MS_PER_MINUTE;
/**
 * Envelope for deliberately harsher-than-defined diagnostic variants. The
 * 180 s bound is asserted only for the defined scenario; a diagnostic asserts
 * its own envelope and reports its measured bound through the returned metrics.
 */
/** Measured envelope for the early-5s-429 diagnostic (currently ~225 s). */
const EARLY_429_DIAGNOSTIC_BOUND_MS = 300_000;
/** Measured envelope for the extended-latency overlap diagnostic (currently ~303 s). */
const OVERLAP_DIAGNOSTIC_BOUND_MS = 420_000;

/**
 * Invariants every healthy modeled run must hold: no lost or duplicated
 * deliveries, no attempt inflation, no pacing/cooldown breach, no scheduler
 * backlog duplication, no Queue retry exhaustion and no harness errors.
 *
 * Budget deferral is now an expected path: a chunk is redelivered with
 * `retry({delaySeconds})`, so `consumerSummary.deferred` must be matched
 * one-for-one by redeliveries and must never silently fall back to the
 * 10-minute enqueue lease.
 */
function assertHealthy(metrics: TargetSimulationMetrics, label: string): void {
  assert.equal(metrics.timedOut, false, `${label}: the simulation finished before its deadline`);
  assert.equal(metrics.totalJobs, metrics.expectedJobs, `${label}: exactly one job per recipient`);
  assert.equal(
    metrics.unprocessedJobs,
    0,
    `${label}: ${metrics.unprocessedJobs} job(s) never reached sent/terminal (status ${JSON.stringify(metrics.jobsByStatus)})`,
  );
  assert.equal(
    metrics.lostJobs,
    0,
    `${label}: jobs by status ${JSON.stringify(metrics.jobsByStatus)}`,
  );
  assert.equal(metrics.jobsWithoutSuccess, 0, `${label}: every sent job has a recorded response`);
  assert.deepEqual(metrics.duplicateSuccessChatIds, [], `${label}: no duplicate successful sends`);
  assert.deepEqual(
    metrics.attemptMismatchJobIds,
    [],
    `${label}: attempt_count must equal started Telegram calls`,
  );
  assert.equal(metrics.pacingViolations, 0, `${label}: durable 20/s pacing`);
  assert.equal(metrics.cooldownViolations, 0, `${label}: recorded 429 cooldown`);
  assert.equal(
    metrics.schedulerBacklogDuplicates,
    0,
    `${label}: overlapping ticks must not duplicate queue backlog`,
  );
  assert.equal(
    metrics.exhaustedQueueMessages,
    0,
    `${label}: ${metrics.exhaustedQueueMessages} queue message(s) exhausted Cloudflare's default 3 retries and fell back to the 10-minute lease repair`,
  );
  assert.equal(
    metrics.budgetDeferralRetries,
    metrics.consumerSummary.deferred,
    `${label}: every budget-deferred chunk must be redelivered as a Queue retry`,
  );
  assert.equal(
    metrics.consumerSummary.sent,
    metrics.expectedJobs,
    `${label}: every recipient send was processed successfully`,
  );
  assert.equal(metrics.consumerSummary.terminal, 0, `${label}: no terminal consumer outcome`);
  assert.equal(metrics.errors.length, 0, `${label}: ${metrics.errors.join('; ')}`);
}

describe('F11: 1,000-recipient delivery target', () => {
  let metrics: TargetSimulationMetrics;

  before(async () => {
    metrics = await runTargetSimulation({
      recipients: RECIPIENTS,
      cronAlignmentMs: WORST_CRON_ALIGNMENT_MS,
    });
  });

  it('keeps at most five simultaneous Telegram fetches, measured at the transport', () => {
    // Literal audit bound, not the consumer's own exported constant: mutating
    // MAX_INFLIGHT_SENDS must not be able to move the asserted ceiling.
    assert.ok(
      metrics.maxInFlight <= 5,
      `max simultaneous Telegram fetches was ${metrics.maxInFlight}, above the audit's literal bound of 5`,
    );
    // Meaningful dynamic check: the run must exercise real parallel sending.
    // The consumer's current 10-statement reserve blocks the fifth admission,
    // so the observed saturation is 4; the literal ceiling above is the pin.
    assert.ok(
      metrics.maxInFlight >= 3,
      `the defined scenario must exercise parallel sending, measured ${metrics.maxInFlight}`,
    );
  });

  it('respects the durable 20/s pace and the recorded 429 cooldown', () => {
    assert.equal(
      metrics.pacingViolations,
      0,
      `durable 20/s pacing violated ${metrics.pacingViolations} time(s)`,
    );
    assert.equal(
      metrics.cooldownViolations,
      0,
      `${metrics.cooldownViolations} request(s) started inside a recorded 429 cooldown`,
    );
    assert.equal(metrics.cooldownWindows.length, 1, 'exactly one modeled 429 cooldown');
    assert.equal(metrics.requestEnds, metrics.requestStarts, 'every started request has a response');
    assert.ok(
      metrics.sustainedStartsPerSecond <= 20,
      `sustained start rate was ${metrics.sustainedStartsPerSecond.toFixed(2)}/s`,
    );
  });

  it('delivers every normal reminder within 180 s of its logical send_at', () => {
    assert.equal(metrics.timedOut, false, 'the simulation finished before its deadline');
    assert.equal(
      metrics.requestStarts,
      RECIPIENTS + 1,
      'every reminder must get a first attempt plus the single 429 retry',
    );
    assert.equal(
      metrics.completionSamples,
      RECIPIENTS,
      'every recipient has a measured successful delivery',
    );
    const bound = metrics.maxCompletionFromSendAtMs;
    assert.notEqual(bound, null, 'a completion bound was measured');
    assert.ok(
      bound !== null && bound <= DELIVERY_WINDOW_MS,
      `measured completion bound ${String(bound)}ms exceeds ${DELIVERY_WINDOW_MS}ms`,
    );
  });

  it('has no duplicate deliveries, no lost jobs and no attempt inflation', () => {
    assertHealthy(metrics, 'target');
    assert.ok(
      metrics.duplicateQueueDeliveries > 0,
      'the at-least-once duplicate fraction produced duplicate deliveries',
    );
    assert.ok(
      metrics.duplicateDeliveriesProcessed > 0,
      'duplicate deliveries reached the consumer during the run',
    );
    assert.equal(
      metrics.consumerSummary.retried,
      0,
      'the short 429 cooldown must be slept out in place, not parked as a job retry',
    );
    assert.ok(
      metrics.consumerSummary.skipped > 0,
      'at-least-once duplicate deliveries were acknowledged as already handled',
    );
  });

  it('really models each required condition (alignment, handoff, batch, ticks, duplicates)', () => {
    const firstTick = metrics.ticks[0];
    assert.notEqual(firstTick, undefined, 'the first tick was recorded');
    assert.equal(
      metrics.firstTickFiredAtMs,
      metrics.startMs + WORST_CRON_ALIGNMENT_MS,
      'the first tick fired at the worst allowed 60 s alignment',
    );
    assert.equal(
      (firstTick?.atMs ?? 0) - (firstTick?.firedAtMs ?? 0),
      metrics.assumptions.tickWorkDelayMs,
      'modeled scheduler D1 work delayed the tick body',
    );
    assert.ok(
      (metrics.firstRequestAtMs ?? 0) - (firstTick?.atMs ?? 0) >= metrics.assumptions.queueHandoffMs,
      'the first request cannot start before the modeled D1/Queue handoff',
    );

    assert.equal(metrics.ticks.length >= 2, true, 'minute ticks overlapped the run');
    assert.equal(
      metrics.ticksWithOutstandingJobs >= 1,
      true,
      'a later tick fired while jobs from the first backlog were still outstanding',
    );

    assert.equal(metrics.maxConcurrentQueueBatches, QUEUE_CONSUMER_CONCURRENCY);
    assert.ok(
      metrics.queueBatchSizes.every((size) => size <= QUEUE_BATCH_SIZE),
      `a batch exceeded the queue's ${QUEUE_BATCH_SIZE}-message cap`,
    );
    assert.equal(Math.max(...metrics.queueBatchSizes), QUEUE_BATCH_SIZE, 'full batches were delivered');
    assert.ok(
      Math.min(...metrics.queueBatchSizes) < QUEUE_BATCH_SIZE,
      'a partial batch was flushed by the 1 s batch timeout',
    );
    assert.ok(metrics.queueTimeoutFlushes > 0, 'the queue batch timeout was exercised');
    assert.equal(metrics.assumptions.queueBatchTimeoutMs, QUEUE_BATCH_TIMEOUT_MS);

    assert.equal(
      metrics.telegramLatencySamples,
      metrics.requestStarts,
      'every request has a measured response latency',
    );
    assert.ok(
      metrics.meanTelegramLatencyMs !== null &&
        Math.abs(metrics.meanTelegramLatencyMs - metrics.assumptions.telegramLatencyMs) <= 5,
      `mean Telegram latency was ${String(metrics.meanTelegramLatencyMs)}ms`,
    );
    assert.equal(
      (metrics.maxTelegramLatencyMs ?? 0) - (metrics.minTelegramLatencyMs ?? 0),
      100,
      'the deterministic latency cycle spans the configured 200 ms mean',
    );
  });

  it('reports the measured bound, modeled assumptions, concurrency and start rate', () => {
    assert.equal(metrics.assumptions.cronAlignmentMs, WORST_CRON_ALIGNMENT_MS);
    assert.equal(metrics.assumptions.tickWorkDelayMs, 100);
    assert.equal(metrics.assumptions.queueHandoffMs, 500);
    assert.equal(metrics.assumptions.telegramLatencyMs, 200);
    assert.equal(metrics.assumptions.queueBatchSize, 10);
    assert.equal(metrics.assumptions.queueBatchTimeoutMs, 1_000);
    assert.equal(metrics.assumptions.consumerConcurrency, 1);
    assert.equal(metrics.assumptions.duplicateEvery, 37);
    assert.equal(metrics.assumptions.rateLimitOrdinal, 137);
    assert.equal(metrics.assumptions.crashMessageIndex, null);
    assert.equal(metrics.assumptions.messageBudgetFloor, MESSAGE_BUDGET_FLOOR);
    assert.equal(metrics.assumptions.budgetDeferRetryDelaySeconds, 1);
    assert.equal(metrics.assumptions.maxQueueDeliveries, 4);
    assert.equal(metrics.assumptions.forbiddenEvery, 0);
    // Measured with the ten-statement floor: a full 10-message batch costs one
    // prune plus ten five-statement sends (51 statements), so full batches
    // defer their tail as Queue retries; every deferred chunk is redelivered
    // (pinned one-for-one by assertHealthy) and the run still settles inside
    // the bound with zero stranded jobs.
    assert.equal(
      metrics.budgetDeferralRetries,
      metrics.consumerSummary.deferred,
      'every budget-deferred chunk was redelivered as a Queue retry',
    );
    assert.ok(
      metrics.consumerSummary.deferred > 0,
      'full 10-message batches exercise the budget-deferral path',
    );
    assert.equal(metrics.exhaustedQueueMessages, 0, 'no Queue message exhausted its retries');
    assert.ok(metrics.duplicateQueueDeliveries > 0, 'duplicate queue deliveries were exercised');
    assert.equal(metrics.cooldownWindows.length, 1, 'the modeled 429 retry was exercised');
    assert.ok(metrics.maxInFlight >= 1);
    assert.equal(metrics.errors.length, 0, metrics.errors.join('; '));
    assert.ok(metrics.elapsedVirtualMs > 0, 'virtual time advanced');
    assert.equal(metrics.ticks.length >= 1, true, 'minute ticks ran');
  });
});

describe('F11: measurement is from logical send_at, not the discovering tick', () => {
  it('moves the measured bound by ~60 s when only the first tick is 60 s later', async () => {
    const early = await runTargetSimulation({ recipients: 100, cronAlignmentMs: 0 });
    const late = await runTargetSimulation({
      recipients: 100,
      cronAlignmentMs: WORST_CRON_ALIGNMENT_MS,
    });
    const earlyBound = early.maxCompletionFromSendAtMs;
    const lateBound = late.maxCompletionFromSendAtMs;
    assert.notEqual(earlyBound, null);
    assert.notEqual(lateBound, null);
    assert.equal(
      late.firstRequestAtMs !== null && late.firstRequestAtMs >= late.startMs + 60_000,
      true,
      'the late tick really delayed the first request',
    );
    assert.ok(
      (lateBound ?? 0) - (earlyBound ?? 0) >= 55_000,
      `a 60 s later tick must move the send_at bound by ~60 s (early ${String(earlyBound)}ms, late ${String(lateBound)}ms)`,
    );
    assertHealthy(early, 'early-alignment');
    assertHealthy(late, 'late-alignment');
  });
});

describe('F11: metric sensitivity (negative controls)', () => {
  it('attempt inflation: a second increment for one started call is detected', async () => {
    const metrics = await runTargetSimulation({
      recipients: 6,
      cronAlignmentMs: 0,
      rateLimitOrdinal: null,
      duplicateEvery: 0,
      attemptInflationProbe: true,
    });
    assert.equal(metrics.requestStarts, 6, 'each job made exactly one Telegram call');
    assert.equal(metrics.sentJobs, 6, 'the probe still delivered every job');
    assert.equal(
      metrics.attemptMismatchJobIds.length,
      6,
      'every inflated job must be reported as attempt_count != started calls',
    );
  });

  it('pacing detector flags 21 starts inside one pace window and clears compliant spacing', () => {
    const violating = Array.from({ length: 21 }, (_, index) => ({ atMs: index * 49 }));
    assert.equal(
      findPacingViolations(violating, BOT_SEND_PACE_PER_SECOND, SEND_PACE_WINDOW_MS),
      1,
      'a 21st start 980 ms after the first must be detected',
    );
    const compliant = Array.from({ length: 21 }, (_, index) => ({ atMs: index * 1_000 }));
    assert.equal(
      findPacingViolations(compliant, BOT_SEND_PACE_PER_SECOND, SEND_PACE_WINDOW_MS),
      0,
      'starts exactly one window apart are compliant',
    );
  });

  it('cooldown detector flags a start faked inside a recorded 429 window', () => {
    const fabricated = [{ recordedAtMs: 10_000, untilMs: 20_000, retryAfterMs: 10_000 }];
    assert.equal(
      findCooldownViolations([{ atMs: 15_000 }], fabricated),
      1,
      'a start strictly inside the recorded window must be detected',
    );
    assert.equal(
      findCooldownViolations([{ atMs: 10_000 }, { atMs: 20_000 }], fabricated),
      0,
      'the window boundaries themselves are not violations',
    );
  });
});

describe('F11: adversarial variants', () => {
  it('diagnostic: an early 429 defers a wave to the next tick and may exceed the bound', async () => {
    // A 5 s cooldown fires on the very first request, so the paced send path
    // defers the whole first wave to the next minute tick. Cooldown compliance
    // is the asserted invariant here; the 180 s defined-scenario bound is not
    // applied because a cooldown intentionally parks work until the next tick.
    const metrics = await runTargetSimulation({
      recipients: RECIPIENTS,
      cronAlignmentMs: WORST_CRON_ALIGNMENT_MS,
      rateLimitOrdinal: 1,
      rateLimitRetryAfterMs: 5_000,
    });
    assertHealthy(metrics, 'early-429-diagnostic');
    assert.equal(metrics.cooldownWindows.length, 1, 'the early 429 recorded one cooldown');
    assert.equal(metrics.cooldownViolations, 0);
    assert.ok(
      metrics.consumerSummary.retried > 0,
      'a cooldown longer than PACE_MAX_WAIT_MS still uses the durable job retry path',
    );
    assert.equal(
      metrics.requestStarts,
      RECIPIENTS + 1,
      'every reminder got a first attempt plus the single 429 retry',
    );
    assert.ok(
      metrics.maxCompletionFromSendAtMs !== null &&
        metrics.maxCompletionFromSendAtMs <= EARLY_429_DIAGNOSTIC_BOUND_MS,
      `early-429 diagnostic bound ${String(metrics.maxCompletionFromSendAtMs)}ms exceeds the diagnostic envelope`,
    );
  });

  it('holds the 180 s bound when the 429 lands late in the wave', async () => {
    // Defined configuration (1000 users, 60 s alignment, 200 ms latency, 1 s
    // retry_after) with the single 429 at request ordinal 950. The in-place
    // bounded retry sleeps out the cooldown and re-sends from the same task,
    // so the rate-limited job no longer parks until the next minute tick
    // (measured ~166.8 s, inside the 180 s bound).
    const metrics = await runTargetSimulation({
      recipients: RECIPIENTS,
      cronAlignmentMs: WORST_CRON_ALIGNMENT_MS,
      rateLimitOrdinal: 950,
    });
    assertHealthy(metrics, 'late-429');
    assert.equal(metrics.cooldownWindows.length, 1, 'the late 429 recorded one cooldown');
    assert.equal(metrics.cooldownViolations, 0);
    assert.equal(
      metrics.requestStarts,
      RECIPIENTS + 1,
      'every reminder got a first attempt plus the single 429 retry',
    );
    assert.equal(metrics.completionSamples, RECIPIENTS);
    assert.ok(
      metrics.maxInFlight <= 5,
      `late-429 max in-flight was ${metrics.maxInFlight}, above the literal 5`,
    );
    assert.ok(metrics.maxInFlight >= 3, 'the late-429 run exercises parallel sending');
    assert.ok(
      metrics.maxCompletionFromSendAtMs !== null &&
        metrics.maxCompletionFromSendAtMs <= DELIVERY_WINDOW_MS,
      `late-429 bound ${String(metrics.maxCompletionFromSendAtMs)}ms exceeds ${DELIVERY_WINDOW_MS}ms`,
    );
  });

  it('diagnostic: keeps ticking while an extended-latency backlog is still draining', async () => {
    // Deliberately harsher than the defined scenario (1500 ms latency, 1 s
    // handoff) to force overlap; its diagnostic envelope applies instead of
    // the 180 s defined-scenario bound. The defined scenario's own overlap is
    // asserted by the primary run's outstanding-jobs tick check.
    const metrics = await runTargetSimulation({
      recipients: 400,
      cronAlignmentMs: 0,
      telegramLatencyMs: 1_500,
      queueHandoffMs: 1_000,
      rateLimitOrdinal: null,
      duplicateEvery: 13,
      maxSimulatedMs: 10 * MS_PER_MINUTE,
    });
    assertHealthy(metrics, 'overlap-diagnostic');
    assert.equal(metrics.ticks.length >= 2, true, 'at least two ticks fired');
    assert.equal(
      metrics.ticksWithQueueBacklog >= 1,
      true,
      'a tick fired while queue messages were still buffered or in flight',
    );
    assert.ok(
      metrics.maxCompletionFromSendAtMs !== null &&
        metrics.maxCompletionFromSendAtMs <= OVERLAP_DIAGNOSTIC_BOUND_MS,
      `overlap diagnostic bound ${String(metrics.maxCompletionFromSendAtMs)}ms exceeds the diagnostic envelope`,
    );
  });

  it('recovers a job when a consumer invocation crashes mid-batch', async () => {
    const metrics = await runTargetSimulation({
      recipients: 32,
      cronAlignmentMs: 0,
      crashMessageIndex: 15,
      rateLimitOrdinal: null,
      duplicateEvery: 0,
      maxSimulatedMs: 5 * MS_PER_MINUTE,
    });
    assertHealthy(metrics, 'crash-mid-batch');
    assert.equal(metrics.crashedJobIds.length, 1, 'one delivery crashed after claiming its lease');
    assert.ok(metrics.repairedLeases >= 1, 'the expired lease was repaired');
    assert.equal(metrics.requestStarts, 32, 'the crashed attempt made no Telegram call');
    assert.equal(
      metrics.ticksWithOutstandingJobs >= 1,
      true,
      'recovery happened across a later tick',
    );
    assert.equal(
      metrics.maxConcurrentQueueBatches,
      QUEUE_CONSUMER_CONCURRENCY,
      'the crashed invocation was replaced, never overlapped',
    );
    assert.ok(
      metrics.maxCompletionFromSendAtMs !== null &&
        metrics.maxCompletionFromSendAtMs <= DELIVERY_WINDOW_MS,
      `crash recovery bound ${String(metrics.maxCompletionFromSendAtMs)}ms exceeds ${DELIVERY_WINDOW_MS}ms`,
    );
  });

  it('rolls a 10x403 batch to 8 processed plus two Queue-retried remainders', async () => {
    // Rolling admission admits while the 10-statement worst-case reserve fits;
    // ten 403 messages exceed one invocation's budget, so exactly two messages
    // roll over and must come back through `retry({delaySeconds: 1})`.
    const metrics = await runTargetSimulation({
      recipients: 10,
      cronAlignmentMs: 0,
      rateLimitOrdinal: null,
      duplicateEvery: 0,
      forbiddenEvery: 1,
      maxSimulatedMs: 5 * MS_PER_MINUTE,
    });
    assert.equal(metrics.timedOut, false);
    assert.equal(metrics.totalJobs, 10);
    assert.equal(metrics.unprocessedJobs, 0, 'every job reached sent or terminal');
    assert.equal(metrics.terminalJobs, 10);
    assert.equal(metrics.requestStarts, 10, 'exactly one Telegram call per recipient');
    assert.equal(metrics.requestEnds, 10);
    assert.equal(metrics.pacingViolations, 0);
    assert.equal(metrics.cooldownViolations, 0);
    assert.deepEqual(metrics.attemptMismatchJobIds, []);
    assert.deepEqual(metrics.duplicateSuccessChatIds, []);
    assert.equal(metrics.consumerSummary.processed, 10, 'the batch ran in one invocation plus retries');
    assert.equal(metrics.consumerSummary.deferred, 2, 'only the budget remainder rolled over');
    assert.equal(
      metrics.budgetDeferralRetries,
      metrics.consumerSummary.deferred,
      'the rolled-over message was redelivered as a Queue retry',
    );
    assert.equal(metrics.exhaustedQueueMessages, 0, 'the remainder stayed inside the retry cap');
    for (const calls of metrics.callCountsByChatId.values()) {
      assert.equal(calls, 1, 'no duplicate Telegram calls');
    }
  });

  it('recovers jobs whose rolled-over messages the Queue cannot redeliver', async () => {
    // Lease-repair fallback, pinned directly: a Queue with no retry budget
    // (maxQueueDeliveries 1) drops the rolled-over messages, so the 10-minute
    // enqueue lease returns their jobs to the pending pool. Nothing is lost
    // and every recipient still receives exactly one Telegram call.
    const metrics = await runTargetSimulation({
      recipients: 40,
      cronAlignmentMs: 0,
      rateLimitOrdinal: null,
      duplicateEvery: 0,
      forbiddenEvery: 2,
      maxQueueDeliveries: 1,
      maxSimulatedMs: 15 * MS_PER_MINUTE,
    });
    assert.equal(metrics.timedOut, false);
    assert.ok(metrics.exhaustedQueueMessages > 0, 'the rolled-over messages exhausted the retry budget');
    assert.ok(metrics.repairedLeases > 0, 'lease repair recovered the dropped messages');
    assert.equal(metrics.unprocessedJobs, 0, 'no job is permanently lost');
    assert.equal(metrics.sentJobs, 20);
    assert.equal(metrics.terminalJobs, 20);
    assert.equal(metrics.requestStarts, 40);
    assert.deepEqual(metrics.attemptMismatchJobIds, []);
    assert.deepEqual(metrics.duplicateSuccessChatIds, []);
    for (const calls of metrics.callCountsByChatId.values()) {
      assert.equal(calls, 1, 'no duplicate Telegram calls');
    }
  });
});

describe('F11: lease expiry recovery', () => {
  it('repairs an expired processing lease and delivers once without attempt inflation', async () => {
    const metrics = await runTargetSimulation({
      recipients: 4,
      cronAlignmentMs: 0,
      crashMessageIndex: 0,
      rateLimitOrdinal: null,
      duplicateEvery: 0,
      maxSimulatedMs: 5 * MS_PER_MINUTE,
    });

    assertHealthy(metrics, 'lease-recovery');
    assert.equal(metrics.crashedJobIds.length, 1, 'one delivery crashed after claiming its lease');
    assert.ok(metrics.repairedLeases >= 1, 'the expired lease was repaired');
    assert.ok(
      metrics.ticks.length >= 3,
      'repeated minute ticks overlapped the recovery window',
    );
    assert.equal(metrics.requestStarts, 4, 'the crashed attempt made no Telegram call');
    assert.equal(metrics.sentJobs, 4, 'all jobs were delivered after recovery');
    for (const successes of metrics.successCountsByChatId.values()) {
      assert.equal(successes.length, 1, 'recovered delivery must be exactly once');
    }
    const bound = metrics.maxCompletionFromSendAtMs;
    assert.ok(
      bound !== null && bound <= DELIVERY_WINDOW_MS,
      `recovered delivery bound ${String(bound)}ms exceeds ${DELIVERY_WINDOW_MS}ms`,
    );
  });
});
