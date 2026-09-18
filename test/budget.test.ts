/**
 * Whole-invocation D1 statement budget. The repository arms a hard limit for
 * one consumer batch or one scheduler tick and refuses to exceed it. Every
 * branch is exercised against the real SQLite-backed repository.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { StatementBudgetError } from '../src/data/repository.ts';
import { processQueueBatch } from '../src/queue/consumer.ts';
import { runSchedulerTick } from '../src/scheduler/tick.ts';
import type { SourceDefinition } from '../src/calendar/sync.ts';
import {
  BUDGET_DEFER_RETRY_DELAY_SECONDS,
  MAX_D1_STATEMENTS_PER_CONSUMER,
  MAX_D1_STATEMENTS_PER_SCHEDULER,
  MS_PER_HOUR,
} from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { jsonResponse, textResponse } from './helpers/fakes.ts';
import { consumerDeps, createHarness, schedulerDeps, type Harness } from './helpers/harness.ts';
import { EMPTY_ICS, makeQueueMessage, parsedEvent, seedDueJobs } from './helpers/seed.ts';

/** D1's hard bound parameters per statement. */
const MAX_BOUND_PARAMS = 100;

function assertWithinBudget(harness: Harness, limit: number): void {
  const used = harness.repository.statementsUsed();
  assert.ok(used <= limit, `used ${used} of ${limit} D1 statements`);
  assert.ok(
    harness.db.stats.maxParamsPerStatement <= MAX_BOUND_PARAMS,
    `bound ${harness.db.stats.maxParamsPerStatement} parameters in one statement`,
  );
}

/** All job statuses, for asserting the durable outcome of a batch. */
function jobStatuses(harness: Harness): string[] {
  return (
    harness.db.database.prepare('SELECT status FROM outbound_jobs ORDER BY id').all() as {
      status: string;
    }[]
  ).map((row) => row.status);
}

function countStatus(harness: Harness, status: string): number {
  return jobStatuses(harness).filter((value) => value === status).length;
}

describe('per-invocation statement budget', () => {
  it('throws once an invocation exceeds its limit', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    harness.repository.beginInvocation(2);
    await harness.repository.activateUser(1, 1, now);
    assert.equal(harness.repository.statementsUsed(), 2, 'user insert with atomic defaults, read-back');
    await assert.rejects(harness.repository.activateUser(2, 2, now), StatementBudgetError);
  });

  // Consumer batches: rolling admission reserves the worst case per in-flight
  // task, so a batch does as much as the measured actual cost leaves room for
  // and only the remainder no reserve can cover defers. Each deferred message
  // is Queue-retried (never acknowledged) and its job keeps the recoverable
  // `enqueued` state for the next invocation or lease repair.
  it('sends what fits of a 10-message success batch inside the budget', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 10);
    assert.equal(jobIds.length, 10);
    const messages = jobIds.map(makeQueueMessage);

    const first = await processQueueBatch(messages, consumerDeps(harness));

    // Measured rolling admission with the ten-statement worst-case floor: one
    // batch-start prune plus eight five-statement sends fit (41 statements:
    // prune + claim + single-statement slot + reservation + two-statement
    // sent-with-ledger each), the reserve for a ninth would pass the limit.
    assert.equal(first.sent, 8);
    assert.equal(first.deferred, 2);
    assert.equal(harness.repository.statementsUsed(), 41, 'prune plus eight five-statement sends');
    assert.equal(messages.filter((message) => message.acked).length, 8);
    const deferred = messages.filter((message) => message.retried);
    assert.equal(deferred.length, 2);
    assert.ok(
      deferred.every(
        (message) => message.retryDelaySeconds === BUDGET_DEFER_RETRY_DELAY_SECONDS,
      ),
      'deferred messages are Queue-retried after the documented delay, never acknowledged',
    );
    assert.equal(countStatus(harness, 'enqueued'), 2, 'deferred jobs stay recoverable');
    assertWithinBudget(harness, MAX_D1_STATEMENTS_PER_CONSUMER);

    // The next redelivery has its own budget and finishes the deferred tail.
    const deferredIndices = messages.flatMap((message, index) => (message.retried ? [index] : []));
    const redelivery = deferredIndices.map((index) => makeQueueMessage(jobIds[index] ?? ''));
    const second = await processQueueBatch(redelivery, consumerDeps(harness));
    assert.equal(second.sent, 2);
    assert.ok(redelivery.every((message) => message.acked));
    assert.deepEqual(jobStatuses(harness), Array.from({ length: 10 }, () => 'sent'));
  });

  it('keeps a 10-recipient 403 batch inside the budget', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 10);
    harness.setHandler(() => jsonResponse({ ok: false, description: 'Forbidden' }, 403));
    const messages = jobIds.map(makeQueueMessage);

    const first = await processQueueBatch(messages, consumerDeps(harness));

    // Measured rolling admission with the ten-statement worst-case floor:
    // eight five-statement terminals fit (41 statements with the batch prune:
    // claim + single-statement slot + reservation + deactivation + terminal),
    // the reserve for a ninth would pass the limit.
    assert.equal(first.terminal, 8);
    assert.equal(first.deferred, 2);
    assert.equal(harness.repository.statementsUsed(), 41, 'prune plus eight five-statement terminals');
    assert.equal(countStatus(harness, 'enqueued'), 2, 'the deferred tail is untouched');
    assert.equal(messages.filter((message) => message.acked).length, 8);
    const deferred = messages.filter((message) => message.retried);
    assert.equal(deferred.length, 2);
    assert.ok(
      deferred.every(
        (message) => message.retryDelaySeconds === BUDGET_DEFER_RETRY_DELAY_SECONDS,
      ),
      'deferred messages are Queue-retried after the documented delay, never acknowledged',
    );
    assertWithinBudget(harness, MAX_D1_STATEMENTS_PER_CONSUMER);

    // The next redelivery has its own budget and finishes the deferred tail.
    const deferredIndices = messages.flatMap((message, index) => (message.retried ? [index] : []));
    const redelivery = deferredIndices.map((index) => makeQueueMessage(jobIds[index] ?? ''));
    const second = await processQueueBatch(redelivery, consumerDeps(harness));
    assert.equal(second.terminal, 2);
    assert.ok(redelivery.every((message) => message.acked));
    assert.deepEqual(jobStatuses(harness), Array.from({ length: 10 }, () => 'cancelled'));
  });

  it('keeps a 10-recipient 429 batch inside the budget', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 10);
    harness.setHandler(() => jsonResponse({ ok: false, parameters: { retry_after: 5 } }, 429));
    const messages = jobIds.map(makeQueueMessage);

    const summary = await processQueueBatch(messages, consumerDeps(harness));

    // Measured outcome with the ten-statement floor: the batch never
    // reaches the in-place retry (retry_after 5s exceeds the bounded wait),
    // the recorded cooldown turns the next jobs into cheaper paced outcomes,
    // and nine are admitted before the reserve for a tenth would pass the
    // limit; the tail defers with its job untouched.
    assert.equal(summary.retried, 9);
    assert.equal(summary.deferred, 1);
    assert.equal(harness.repository.statementsUsed(), 41, 'nine admitted 429s, one deferred');
    assert.equal(harness.fetchSpy.calls.length, 4, 'the recorded cooldown stops later starts');
    assert.equal(countStatus(harness, 'pending'), 9);
    assert.equal(countStatus(harness, 'enqueued'), 1, 'the deferred tail stays recoverable');
    assert.equal(messages.filter((message) => message.acked).length, 9);
    const deferred429 = messages.filter((message) => message.retried);
    assert.equal(deferred429.length, 1);
    assert.ok(
      deferred429.every(
        (message) => message.retryDelaySeconds === BUDGET_DEFER_RETRY_DELAY_SECONDS,
      ),
      'the unaffordable tail is Queue-retried with the deferral delay, never acknowledged',
    );
    assertWithinBudget(harness, MAX_D1_STATEMENTS_PER_CONSUMER);
  });

  it('keeps a 10-recipient transient-failure batch inside the budget', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 10);
    harness.setHandler(() => jsonResponse({ ok: false }, 500));
    const messages = jobIds.map(makeQueueMessage);

    const summary = await processQueueBatch(messages, consumerDeps(harness));

    // Measured rolling admission with the ten-statement floor: ten
    // four-statement retries fit (41 statements with the batch prune: claim +
    // single-statement slot + reservation + reschedule), so nothing defers.
    assert.equal(summary.retried, 10);
    assert.equal(summary.deferred, 0);
    assert.equal(harness.repository.statementsUsed(), 41, 'prune plus ten four-statement retries');
    assert.equal(harness.fetchSpy.calls.length, 10, 'every admitted job got exactly one HTTP attempt');
    assert.equal(messages.filter((message) => message.acked).length, 10);
    assert.equal(countStatus(harness, 'pending'), 10);
    assert.equal(countStatus(harness, 'enqueued'), 0, 'nothing defers');
    assertWithinBudget(harness, MAX_D1_STATEMENTS_PER_CONSUMER);
  });

  it('keeps a 10-recipient skipped batch inside the budget', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 10);
    for (let index = 1; index <= 10; index += 1) {
      await harness.repository.deactivateUser(index, harness.clock.now());
    }
    const messages = jobIds.map(makeQueueMessage);

    const summary = await processQueueBatch(messages, consumerDeps(harness));

    assert.equal(summary.terminal, 10);
    assert.ok(messages.every((message) => message.acked));
    assertWithinBudget(harness, MAX_D1_STATEMENTS_PER_CONSUMER);
  });

  // A scheduler tick plans and syncs both sources under the same hard limit.
  it('keeps a two-source tick with 60 occurrences each inside the budget', async () => {
    const harness = createHarness({ now: Date.UTC(2024, 5, 2, 21) });
    const now = harness.clock.now();
    const events = Array.from({ length: 60 }, (_, index) => {
      const startsAtMs = now + (index + 1) * MS_PER_HOUR;
      return parsedEvent(startsAtMs, {
        uid: `course-${index}`,
        endsAtMs: startsAtMs + MS_PER_HOUR,
      });
    });
    harness.setParser({ events });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'v1' }));
    const sources: SourceDefinition[] = [
      { id: 'basic', kind: 'basic', url: 'https://example.test/basic.ics' },
      { id: 'extended', kind: 'extended', url: 'https://example.test/extended.ics' },
    ];

    const result = await runSchedulerTick(schedulerDeps(harness, { sources }));

    assert.deepEqual(result.syncStatuses, ['applied', 'applied']);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 120);
    assertWithinBudget(harness, MAX_D1_STATEMENTS_PER_SCHEDULER);
  });
});
