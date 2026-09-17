/**
 * F1 regression (repo part): a delivered reminder creates durable identity in
 * `delivery_ledger` that survives any later movement of the same stable
 * occurrence, retention cleanup, re-materialization and revision changes.
 *
 * These tests fail against the pre-ledger implementation: once the `sent` job
 * row ages past `SENT_REMINDER_RETENTION_MS`, cleanup deletes it and the next
 * `planDueReminders` for the moved occurrence re-inserts the same logical
 * reminder. With the ledger, planning excludes delivered dedup keys and
 * cleanup never touches the ledger table.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueMessage } from '../src/queue/consumer.ts';
import { MAX_JOB_ATTEMPTS } from '../src/util.ts';
import {
  CLEANUP_LIMIT,
  EXPANSION_HORIZON_MS,
  JOB_LEASE_MS,
  MS_PER_DAY,
  MS_PER_MINUTE,
  RETENTION_MS,
} from '../src/util.ts';
import { applyMigrations, countRows, createSqliteD1 } from './helpers/d1-sqlite.ts';
import { consumerDeps, createHarness, type Harness } from './helpers/harness.ts';
import { makeQueueMessage, occurrence, seedDueJobs } from './helpers/seed.ts';

const DAY = MS_PER_DAY;

function jobCount(harness: Harness): number {
  return countRows(harness.db, 'SELECT COUNT(*) AS n FROM outbound_jobs');
}

function ledgerCount(harness: Harness): number {
  return countRows(harness.db, 'SELECT COUNT(*) AS n FROM delivery_ledger');
}

function telegramCalls(harness: Harness): number {
  return harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage')).length;
}

/** Sends the single seeded reminder through the real consumer. */
async function sendSeededReminder(harness: Harness, jobId: string): Promise<void> {
  assert.equal(await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness)), 'sent');
  assert.equal(telegramCalls(harness), 1);
  assert.equal(ledgerCount(harness), 1, 'the send records the delivery identity');
}

describe('delivery ledger survives occurrence movement', () => {
  it('day-44 move to day-74: cleanup on day-46 cannot reopen the send', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [1], {
      occurrenceKey: 'long-move#1',
      startsAtMs: start + 10 * MS_PER_MINUTE,
    });
    await sendSeededReminder(harness, jobId);

    // Day 44: the same stable occurrence moves to day 74.
    const movedStartMs = start + 74 * DAY;
    harness.clock.set(start + 44 * DAY);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'long-move#1', startsAtMs: movedStartMs })],
      harness.clock.now(),
    );

    // Day 46: cleanup collects the 45-day-old `sent` job row...
    harness.clock.set(start + 46 * DAY);
    await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, CLEANUP_LIMIT);
    assert.equal(jobCount(harness), 0, 'the transient sent row is collected');
    assert.equal(ledgerCount(harness), 1, 'the delivery identity is never deleted');

    // ...but when the moved occurrence becomes due, planning must stay silent.
    const dueNow = movedStartMs - 10 * MS_PER_MINUTE;
    harness.clock.set(dueNow);
    assert.equal(
      await harness.repository.planDueReminders(dueNow, dueNow + EXPANSION_HORIZON_MS),
      0,
      'the ledger suppresses the already-delivered reminder',
    );
    assert.equal(jobCount(harness), 0, 'zero new jobs');
    assert.equal(telegramCalls(harness), 1, 'zero second Telegram calls');

    // Negative control: without the ledger row the moved occurrence is a
    // genuine candidate, so this test cannot pass vacuously.
    harness.db.database.prepare('DELETE FROM delivery_ledger').run();
    assert.equal(
      await harness.repository.planDueReminders(dueNow, dueNow + EXPANSION_HORIZON_MS),
      1,
      'without the ledger the moved occurrence would re-plan',
    );
  });

  it('repeated moves through more than one horizon stay suppressed', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [1], {
      occurrenceKey: 'hop#1',
      startsAtMs: start + 10 * MS_PER_MINUTE,
    });
    await sendSeededReminder(harness, jobId);

    // Hop the same stable occurrence across three horizons, cleaning between.
    for (const day of [74, 130, 200]) {
      const movedStartMs = start + day * DAY;
      harness.clock.set(movedStartMs - 30 * DAY);
      await harness.repository.upsertOccurrences(
        [occurrence({ occurrenceKey: 'hop#1', startsAtMs: movedStartMs })],
        harness.clock.now(),
      );
      await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, CLEANUP_LIMIT);
      const dueNow = movedStartMs - 10 * MS_PER_MINUTE;
      harness.clock.set(dueNow);
      await harness.repository.cleanup(dueNow, RETENTION_MS, CLEANUP_LIMIT);
      assert.equal(
        await harness.repository.planDueReminders(dueNow, dueNow + EXPANSION_HORIZON_MS),
        0,
        `move to day ${day} must not re-plan`,
      );
      assert.equal(jobCount(harness), 0, 'no job row may reappear');
    }
    assert.equal(telegramCalls(harness), 1, 'exactly one delivery across all horizons');
    assert.equal(ledgerCount(harness), 1);
  });

  it('interleaved planner/cleanup overlap cannot reopen delivered identity', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [1], {
      occurrenceKey: 'overlap#1',
      startsAtMs: start + 10 * MS_PER_MINUTE,
    });
    await sendSeededReminder(harness, jobId);

    const movedStartMs = start + 74 * DAY;
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'overlap#1', startsAtMs: movedStartMs })],
      harness.clock.now(),
    );
    const dueNow = movedStartMs - 10 * MS_PER_MINUTE;
    harness.clock.set(dueNow);
    for (let pass = 0; pass < 5; pass += 1) {
      assert.equal(
        await harness.repository.planDueReminders(dueNow, dueNow + EXPANSION_HORIZON_MS),
        0,
        `planner pass ${pass} stays silent`,
      );
      await harness.repository.cleanup(dueNow, RETENTION_MS, CLEANUP_LIMIT);
      assert.equal(jobCount(harness), 0, `cleanup pass ${pass} creates nothing`);
    }
    assert.equal(telegramCalls(harness), 1);
  });

  it('pending and cancelled reminders still reschedule on material changes', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [1], {
      occurrenceKey: 'live#1',
      startsAtMs: now + 10 * MS_PER_MINUTE,
    });

    // Cancel (e.g. `/stop` sweep), then replan: the unsent row revives because
    // no ledger row exists for it.
    assert.equal(await harness.repository.cancelPendingJobsForUser(1, now), 1);
    assert.equal(
      await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS),
      1,
      'a cancelled unsent reminder is revived',
    );
    assert.equal((await harness.repository.getJob(jobId))?.status, 'pending');

    // A material revision change reschedules and resets attempts.
    await harness.repository.upsertOccurrences(
      [
        occurrence({
          occurrenceKey: 'live#1',
          startsAtMs: now + 10 * MS_PER_MINUTE,
          summary: 'Renamed',
        }),
      ],
      now,
    );
    harness.db.database
      .prepare("UPDATE outbound_jobs SET attempt_count = 4 WHERE id = ?")
      .run(jobId);
    assert.equal(
      await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS),
      1,
    );
    const job = await harness.repository.getJob(jobId);
    assert.equal(Number(job?.attempt_count), 0);
    assert.equal(ledgerCount(harness), 0, 'unsent work never touches the ledger');
  });

  it('upgrade rehearsal: v3 rows backfill the ledger via migration 0004', async () => {
    const db = createSqliteD1();
    applyMigrations(db, ['0001_init.sql', '0002_attempt_semantics.sql', '0003_occurrence_and_job_indexes.sql']);
    db.exec(
      `INSERT INTO outbound_jobs (
         id, kind, telegram_user_id, chat_id, occurrence_id, reminder_offset_minutes,
         send_at_ms, next_attempt_at_ms, status, attempt_count, expected_revision,
         payload_json, dedup_key, created_at_ms, updated_at_ms
       ) VALUES
         ('j-sent', 'reminder', 1, 1, 'basic:a#1', 30, 1, 1, 'sent', 1, 1, NULL, 'rem:1:basic:a#1:30', 1, 1),
         ('j-pending', 'reminder', 2, 2, 'basic:a#1', 30, 1, 1, 'pending', 0, 1, NULL, 'rem:2:basic:a#1:30', 1, 1),
         ('j-leased', 'reminder', 3, 3, 'basic:a#1', 30, 1, 1, 'leased', 2, 1, NULL, 'rem:3:basic:a#1:30', 1, 1),
         ('j-failed', 'reminder', 4, 4, 'basic:a#1', 30, 1, 1, 'failed', ${MAX_JOB_ATTEMPTS}, 1, NULL, 'rem:4:basic:a#1:30', 1, 1),
         ('j-cmd', 'command', 5, 5, NULL, NULL, 1, 1, 'sent', 1, NULL, '{}', 'cmd:9', 1, 1)`,
    );
    applyMigrations(db, ['0004_ledger_and_command_state.sql']);

    const ledger = db.database
      .prepare('SELECT dedup_key FROM delivery_ledger ORDER BY dedup_key')
      .all() as { dedup_key: string }[];
    assert.deepEqual(
      ledger.map((row) => row.dedup_key),
      ['rem:1:basic:a#1:30'],
      'only sent reminders backfill; pending/leased/terminal/command rows do not',
    );
    assert.equal(
      countRows(db, 'SELECT COUNT(*) AS n FROM user_command_state'),
      0,
      'the command-ordering table starts empty',
    );
    const columns = db.database
      .prepare('PRAGMA table_info(outbound_jobs)')
      .all() as { name: string }[];
    assert.ok(
      columns.some((column) => column.name === 'source_update_id'),
      'legacy job rows gain a nullable source_update_id',
    );
    db.close();
  });

  it('clean-build rehearsal: every migration applies and the ledger works', async () => {
    const harness = createHarness();
    const tables = harness.db.database
      .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
      .all() as { name: string }[];
    const names = tables.map((row) => row.name);
    assert.ok(names.includes('delivery_ledger'), 'clean builds create the ledger');
    assert.ok(names.includes('user_command_state'), 'clean builds create command state');
    assert.ok(names.includes('rate_starts'), 'clean builds create the pacer table');

    const start = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [1], {
      occurrenceKey: 'clean#1',
      startsAtMs: start + 10 * MS_PER_MINUTE,
    });
    await sendSeededReminder(harness, jobId);
    harness.clock.set(start + 46 * DAY);
    await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, CLEANUP_LIMIT);
    const dueNow = start + 74 * DAY - 10 * MS_PER_MINUTE;
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'clean#1', startsAtMs: start + 74 * DAY })],
      harness.clock.now(),
    );
    harness.clock.set(dueNow);
    assert.equal(
      await harness.repository.planDueReminders(dueNow, dueNow + EXPANSION_HORIZON_MS),
      0,
    );
    assert.equal(telegramCalls(harness), 1);
    assert.deepEqual(
      await harness.repository.claimDueJobs('scheduler', dueNow, JOB_LEASE_MS, 100),
      [],
      'nothing is enqueued for delivered identity',
    );
  });
});
