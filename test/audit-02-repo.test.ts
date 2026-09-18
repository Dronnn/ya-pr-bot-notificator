/**
 * F2 regression: `attempt_count` counts started Telegram calls only.
 *
 * Scheduler reservation (`claimDueJobs`), direct pending claim
 * (`claimJobContext`), queue redelivery and lease repair must not consume the
 * retry allowance. `beginSendAttempt` is the only increment and it only applies
 * under a live lease owned by the caller (plus the command-revision guard).
 * Migration 0002 resets in-flight rows produced by the old accounting.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueMessage } from '../src/queue/consumer.ts';
import { CLEANUP_LIMIT, JOB_LEASE_MS, MAX_JOB_ATTEMPTS, RETENTION_MS } from '../src/util.ts';
import { applyMigrations, createSqliteD1, countRows, migrationFiles } from './helpers/d1-sqlite.ts';
import { consumerDeps, createHarness, type Harness } from './helpers/harness.ts';
import { makeQueueMessage, seedDueJobs, seedDueReminders } from './helpers/seed.ts';

function attemptCount(harness: Harness, jobId: string): number {
  const row = harness.db.database
    .prepare('SELECT attempt_count FROM outbound_jobs WHERE id = ?')
    .get(jobId) as { attempt_count: unknown } | undefined;
  assert.notEqual(row, undefined, `job ${jobId} must exist`);
  return Number(row?.attempt_count);
}

function firstJobId(harness: Harness): string {
  const row = harness.db.database
    .prepare('SELECT id FROM outbound_jobs LIMIT 1')
    .get() as { id: string } | undefined;
  assert.notEqual(row, undefined, 'expected a planned job');
  return row?.id ?? '';
}

describe('send attempts count started Telegram calls', () => {
  it('does not consume attempts on scheduler reservation or repair', async () => {
    const harness = createHarness();
    await seedDueReminders(harness, [111]);
    const now = harness.clock.now();

    const claimed = await harness.repository.claimDueJobs('scheduler', now, JOB_LEASE_MS, 10);
    assert.equal(claimed.length, 1);
    const jobId = claimed[0]?.jobId ?? '';
    assert.equal(attemptCount(harness, jobId), 0, 'enqueue reservation is not a send attempt');
    assert.equal((await harness.repository.getJob(jobId))?.status, 'enqueued');

    harness.clock.advance(JOB_LEASE_MS + 1);
    assert.equal(await harness.repository.repairExpiredLeases(harness.clock.now()), 1);
    assert.equal(attemptCount(harness, jobId), 0, 'lease repair is not a send attempt');
    const reclaimed = await harness.repository.claimDueJobs('scheduler-2', harness.clock.now(), JOB_LEASE_MS, 10);
    assert.equal(reclaimed.length, 1);
    assert.equal(attemptCount(harness, jobId), 0, 're-enqueue is not a send attempt');
  });

  it('does not consume attempts when a pending job is claimed directly', async () => {
    const harness = createHarness();
    await seedDueReminders(harness, [222]);
    const jobId = firstJobId(harness);

    const context = await harness.repository.claimJobContext(
      jobId,
      'consumer-a',
      harness.clock.now(),
      JOB_LEASE_MS,
    );
    assert.notEqual(context, null);
    assert.equal(context?.attemptCount, 0, 'claim must report a clean attempt count');
    assert.equal(attemptCount(harness, jobId), 0, 'claiming pending work is not a send attempt');
  });

  it('increments exactly once per started call under a live lease', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, [333]);
    const jobId = jobIds[0] ?? '';
    const now = harness.clock.now();

    const context = await harness.repository.claimJobContext(jobId, 'consumer-a', now, JOB_LEASE_MS);
    assert.equal(context?.attemptCount, 0);

    harness.db.stats.reset();
    assert.deepEqual(
      await harness.repository.beginSendAttempt(jobId, 'consumer-a', now, MAX_JOB_ATTEMPTS),
      { status: 'reserved', attempt: 1, userTimeZone: 'Europe/Moscow' },
    );
    assert.equal(harness.db.stats.statements, 1, 'beginSendAttempt is one atomic statement');
    assert.deepEqual(
      await harness.repository.beginSendAttempt(jobId, 'consumer-a', now, MAX_JOB_ATTEMPTS),
      { status: 'reserved', attempt: 2, userTimeZone: 'Europe/Moscow' },
    );
    assert.equal(attemptCount(harness, jobId), 2);
  });

  it('returns null for an expired or foreign lease', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, [444]);
    const jobId = jobIds[0] ?? '';
    const now = harness.clock.now();

    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'consumer-a', now, 1_000),
      null,
    );
    assert.deepEqual(
      await harness.repository.beginSendAttempt(jobId, 'consumer-a', now + 1_001, MAX_JOB_ATTEMPTS),
      { status: 'lost' },
      'an expired lease cannot start a call',
    );
    assert.deepEqual(
      await harness.repository.beginSendAttempt(jobId, 'consumer-b', now, MAX_JOB_ATTEMPTS),
      { status: 'lost' },
      'a foreign owner cannot start a call',
    );
    assert.equal(attemptCount(harness, jobId), 0);
    assert.deepEqual(
      await harness.repository.beginSendAttempt(jobId, 'consumer-a', now, MAX_JOB_ATTEMPTS),
      { status: 'reserved', attempt: 1, userTimeZone: 'Europe/Moscow' },
    );
    assert.equal(attemptCount(harness, jobId), 1, 'only the live owner counted one call');
  });

  it('refuses a command attempt whose expected revision moved on', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(7, 7, now);

    await harness.repository.insertCommandJob({
      id: 'cmd-rev',
      telegramUserId: 7,
      chatId: 7,
      payloadJson: JSON.stringify({ text: 'menu' }),
      dedupKey: 'cmd-rev',
      sendAtMs: now,
      now,
      expectedRevision: 1,
    });
    const context = await harness.repository.claimJobContext('cmd-rev', 'consumer', now, JOB_LEASE_MS);
    assert.equal(context?.expectedRevision, 1);
    assert.equal(context?.userRevision, 1, 'claimJobContext surfaces the user revision');
    assert.deepEqual(
      await harness.repository.beginSendAttempt('cmd-rev', 'consumer', now, MAX_JOB_ATTEMPTS),
      { status: 'reserved', attempt: 1, userTimeZone: null },
    );

    await harness.repository.setUserCourse(7, 'extended', now);
    assert.deepEqual(
      await harness.repository.beginSendAttempt('cmd-rev', 'consumer', now, MAX_JOB_ATTEMPTS),
      { status: 'superseded' },
      'a superseded command reply must not start a call',
    );
    assert.equal(attemptCount(harness, 'cmd-rev'), 1);
  });

  it('treats a NULL expected revision as unconditional delivery', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(8, 8, now);

    await harness.repository.insertCommandJob({
      id: 'cmd-first-contact',
      telegramUserId: 900,
      chatId: 900,
      payloadJson: JSON.stringify({ text: 'help' }),
      dedupKey: 'cmd-first-contact',
      sendAtMs: now,
      now,
      expectedRevision: null,
    });
    const anonymous = await harness.repository.claimJobContext(
      'cmd-first-contact',
      'consumer',
      now,
      JOB_LEASE_MS,
    );
    assert.equal(anonymous?.userFound, false);
    assert.equal(anonymous?.userRevision, null);
    assert.deepEqual(
      await harness.repository.beginSendAttempt(
        'cmd-first-contact',
        'consumer',
        now,
        MAX_JOB_ATTEMPTS,
      ),
      { status: 'reserved', attempt: 1, userTimeZone: null },
      'a never-subscribed recipient can still be answered',
    );

    await harness.repository.insertCommandJob({
      id: 'cmd-null-with-user',
      telegramUserId: 8,
      chatId: 8,
      payloadJson: JSON.stringify({ text: 'menu' }),
      dedupKey: 'cmd-null-with-user',
      sendAtMs: now,
      now,
      expectedRevision: null,
    });
    assert.notEqual(
      await harness.repository.claimJobContext('cmd-null-with-user', 'consumer', now, JOB_LEASE_MS),
      null,
    );
    assert.deepEqual(
      await harness.repository.beginSendAttempt(
        'cmd-null-with-user',
        'consumer',
        now,
        MAX_JOB_ATTEMPTS,
      ),
      { status: 'reserved', attempt: 1, userTimeZone: null },
      'NULL stays deliverable when the user row appears after the reply was queued',
    );
    assert.equal(attemptCount(harness, 'cmd-null-with-user'), 1);
  });

  it('delivers a queued first-contact reply after the user row appears', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.insertCommandJob({
      id: 'cmd-late-user',
      telegramUserId: 901,
      chatId: 901,
      payloadJson: JSON.stringify({ text: 'events' }),
      dedupKey: 'cmd-late-user',
      sendAtMs: now,
      now,
      expectedRevision: null,
    });
    // The row appears only after the reply was queued (e.g. a later /start).
    await harness.repository.activateUser(901, 901, now);

    const outcome = await processQueueMessage(
      makeQueueMessage('cmd-late-user'),
      consumerDeps(harness),
    );

    assert.equal(outcome, 'sent', 'first-contact replies must terminate, not spin on the guard');
    const job = await harness.repository.getJob('cmd-late-user');
    assert.equal(job?.status, 'sent');
    assert.equal(Number(job?.attempt_count), 1, 'exactly one real call was counted');
    const sends = harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage'));
    assert.equal(sends.length, 1, 'the reply is delivered exactly once');
  });

  it('changes attempt_count only through beginSendAttempt', async () => {
    const harness = createHarness();
    await seedDueReminders(harness, [555, 556]);
    harness.db.exec(
      `CREATE TABLE attempt_audit (
         seq INTEGER PRIMARY KEY AUTOINCREMENT,
         job_id TEXT NOT NULL,
         old_count INTEGER,
         new_count INTEGER
       );
       CREATE TRIGGER audit_attempt_count
       AFTER UPDATE OF attempt_count ON outbound_jobs
       WHEN OLD.attempt_count IS NOT NEW.attempt_count
       BEGIN
         INSERT INTO attempt_audit (job_id, old_count, new_count)
         VALUES (NEW.id, OLD.attempt_count, NEW.attempt_count);
       END`,
    );
    const audit = (): unknown[] =>
      (
        harness.db.database
          .prepare('SELECT job_id, old_count, new_count FROM attempt_audit ORDER BY seq')
          .all() as { job_id: unknown; old_count: unknown; new_count: unknown }[]
      ).map((row) => ({
        job_id: String(row.job_id),
        old_count: Number(row.old_count),
        new_count: Number(row.new_count),
      }));
    const now = harness.clock.now();

    const claimed = await harness.repository.claimDueJobs('scheduler', now, JOB_LEASE_MS, 10);
    assert.equal(claimed.length, 2);
    const jobId = claimed[0]?.jobId ?? '';
    assert.notEqual(await harness.repository.claimJobContext(jobId, 'consumer', now, JOB_LEASE_MS), null);
    assert.deepEqual(audit(), [], 'claiming never touches the counter');

    assert.deepEqual(
      await harness.repository.beginSendAttempt(jobId, 'consumer', now, MAX_JOB_ATTEMPTS),
      { status: 'reserved', attempt: 1, userTimeZone: 'Europe/Moscow' },
    );
    assert.deepEqual(audit(), [{ job_id: jobId, old_count: 0, new_count: 1 }]);

    assert.equal(
      await harness.repository.rescheduleJob(jobId, 'consumer', now + 1_000, 'transient_500', now),
      true,
    );
    assert.deepEqual(audit(), [{ job_id: jobId, old_count: 0, new_count: 1 }], 'reschedule resets nothing');
    harness.clock.advance(1_001);
    assert.equal(await harness.repository.repairExpiredLeases(harness.clock.now()), 0);
    assert.equal(
      await harness.repository.planDueReminders(
        harness.clock.now(),
        harness.clock.now() + RETENTION_MS,
      ),
      0,
      'the unrevised pending row is a no-op',
    );
    assert.equal(await harness.repository.cancelPendingJobsForUser(556, harness.clock.now()), 1);
    assert.equal(await harness.repository.cancelStaleJobs(harness.clock.now()), 0);
    await harness.repository.cleanup(
      harness.clock.now() + RETENTION_MS,
      RETENTION_MS,
      CLEANUP_LIMIT,
    );
    assert.deepEqual(audit(), [{ job_id: jobId, old_count: 0, new_count: 1 }]);
  });

  it('resets only in-flight attempt counts in migration 0002', () => {
    const db = createSqliteD1();
    assert.deepEqual(
      migrationFiles(),
      [
        '0001_init.sql',
        '0002_attempt_semantics.sql',
        '0003_occurrence_and_job_indexes.sql',
        '0004_ledger_and_command_state.sql',
        '0005_pacer_and_cleanup_indexes.sql',
        '0006_callback_answers.sql',
        '0007_user_time_zone.sql',
        '0008_user_reminder_rules.sql',
      ],
      'every migration is applied in filename order by default',
    );
    applyMigrations(db, ['0001_init.sql']);
    db.exec(
      `INSERT INTO outbound_jobs (
         id, kind, telegram_user_id, chat_id, occurrence_id, reminder_offset_minutes,
         send_at_ms, next_attempt_at_ms, status, attempt_count, expected_revision,
         payload_json, dedup_key, created_at_ms, updated_at_ms
       ) VALUES
         ('j-pending', 'reminder', 1, 1, NULL, 30, 1, 1, 'pending', 7, NULL, NULL, 'k1', 1, 1),
         ('j-enqueued', 'reminder', 1, 1, NULL, 30, 1, 1, 'enqueued', 7, NULL, NULL, 'k2', 1, 1),
         ('j-leased', 'reminder', 1, 1, NULL, 30, 1, 1, 'leased', 7, NULL, NULL, 'k3', 1, 1),
         ('j-sent', 'reminder', 1, 1, NULL, 30, 1, 1, 'sent', 7, NULL, NULL, 'k4', 1, 1),
         ('j-cancelled', 'reminder', 1, 1, NULL, 30, 1, 1, 'cancelled', 7, NULL, NULL, 'k5', 1, 1),
         ('j-failed', 'reminder', 1, 1, NULL, 30, 1, 1, 'failed', 7, NULL, NULL, 'k6', 1, 1)`,
    );

    applyMigrations(db, ['0002_attempt_semantics.sql']);

    const rows = db.database
      .prepare('SELECT status, attempt_count FROM outbound_jobs ORDER BY status')
      .all() as { status: string; attempt_count: unknown }[];
    assert.deepEqual(
      rows.map((row) => [row.status, Number(row.attempt_count)]),
      [
        ['cancelled', 7],
        ['enqueued', 0],
        ['failed', 7],
        ['leased', 0],
        ['pending', 0],
        ['sent', 7],
      ],
      'in-flight rows restart, terminal rows keep their history',
    );
    assert.equal(countRows(db, 'SELECT COUNT(*) AS n FROM outbound_jobs'), 6);
    db.close();
  });
});
