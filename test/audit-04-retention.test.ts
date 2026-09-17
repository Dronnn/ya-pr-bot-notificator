/**
 * F4 regression: the durable sent-reminder identity survives retention.
 *
 * A delivered reminder creates permanent identity in `delivery_ledger` (plus a
 * transient `sent` job row). Occurrence rows keep the short retention and can
 * be deleted (and later re-materialized, moved or revised by a sync) while the
 * ledger row must stay - otherwise the next planner run would create and send
 * the reminder again. The transient sent job row is collected after
 * `SENT_REMINDER_RETENTION_MS`, but the ledger row is never deleted, so a
 * moved occurrence that is re-materialized inside any later horizon can never
 * re-send. Command replies and non-sent terminal reminders keep the flat
 * 24-hour policy.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueMessage } from '../src/queue/consumer.ts';
import {
  CLEANUP_LIMIT,
  EXPANSION_HORIZON_MS,
  JOB_LEASE_MS,
  MAX_JOB_ATTEMPTS,
  MS_PER_DAY,
  MS_PER_MINUTE,
  RETENTION_MS,
  SENT_REMINDER_RETENTION_MS,
  SOURCE_LEASE_MS,
} from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { consumerDeps, createHarness, type Harness } from './helpers/harness.ts';
import { makeQueueMessage, occurrence, seedDueJobs } from './helpers/seed.ts';

/**
 * Claims the job and persists one successful send. The expected live timezone
 * defaults to the seeded reminder fixture; command fixtures without an
 * onboarded user pass `null`.
 */
async function sendReminder(
  harness: Harness,
  jobId: string,
  owner = 'consumer',
  userTimeZone: string | null = 'Europe/Moscow',
): Promise<void> {
  const now = harness.clock.now();
  const context = await harness.repository.claimJobContext(jobId, owner, now, JOB_LEASE_MS);
  assert.notEqual(context, null, `job ${jobId} must be claimable`);
  assert.deepEqual(
    await harness.repository.beginSendAttempt(jobId, owner, now, MAX_JOB_ATTEMPTS),
    { status: 'reserved', attempt: 1, userTimeZone },
  );
  assert.equal(await harness.repository.finishJobSent(jobId, owner, now), true);
}

function ledgerCount(harness: Harness): number {
  return countRows(harness.db, 'SELECT COUNT(*) AS n FROM delivery_ledger');
}

function jobCount(harness: Harness, jobId?: string): number {
  if (jobId === undefined) {
    return countRows(harness.db, 'SELECT COUNT(*) AS n FROM outbound_jobs');
  }
  const row = harness.db.database
    .prepare('SELECT COUNT(*) AS n FROM outbound_jobs WHERE id = ?')
    .get(jobId) as { n: number };
  return Number(row.n);
}

function sentCount(harness: Harness): number {
  return countRows(harness.db, "SELECT COUNT(*) AS n FROM outbound_jobs WHERE status = 'sent'");
}

function telegramCalls(harness: Harness): number {
  return harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage')).length;
}

/** Counts the due (occurrence, user) pairs planning would consider right now. */
function dueCandidates(harness: Harness, now: number): number {
  return countRows(
    harness.db,
    `SELECT COUNT(*) AS n FROM occurrences o
     JOIN users u ON u.course = o.course
     WHERE u.active = 1 AND o.status = 'confirmed'
       AND o.starts_at_ms > ${now}
       AND o.starts_at_ms <= ${now + EXPANSION_HORIZON_MS}
       AND o.starts_at_ms - u.reminder_offset_minutes * ${MS_PER_MINUTE} <= ${now}`,
  );
}

describe('sent reminder identity through retention', () => {
  it('survives the occurrence cleanup and a later re-materialization, then persists in the ledger', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    const jobIds = await seedDueJobs(harness, [1], {
      occurrenceKey: 'audit#1',
      startsAtMs: start + 10 * MS_PER_MINUTE,
    });
    const jobId = jobIds[0] ?? '';
    assert.equal(
      await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness)),
      'sent',
    );
    assert.equal(telegramCalls(harness), 1);
    assert.equal(ledgerCount(harness), 1, 'the send records the permanent delivery identity');

    // The audit ordering: retention passes, cleanup removes the occurrence row
    // first; the sent job row and the ledger row must survive it.
    harness.clock.set(start + RETENTION_MS + 11 * MS_PER_MINUTE);
    harness.db.stats.reset();
    await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, CLEANUP_LIMIT);
    assert.equal(harness.db.stats.statements, 7, 'cleanup is seven bounded statements');
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'),
      0,
      'the old occurrence row is collected',
    );
    assert.equal(jobCount(harness, jobId), 1, 'the sent job row survives');
    assert.equal(ledgerCount(harness), 1, 'the ledger row survives');
    assert.equal((await harness.repository.getJob(jobId))?.status, 'sent');

    // A later sync re-materializes the same stable occurrence moved +3 days.
    const movedStartMs = start + 3 * MS_PER_DAY;
    harness.clock.set(start + RETENTION_MS + 12 * MS_PER_MINUTE);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'audit#1', startsAtMs: movedStartMs })],
      harness.clock.now(),
    );
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);

    // It becomes due again: planning must not create a second reminder.
    const dueNow = movedStartMs - 10 * MS_PER_MINUTE;
    harness.clock.set(dueNow);
    assert.equal(dueCandidates(harness, dueNow), 1, 'the moved occurrence is a candidate');
    assert.equal(
      await harness.repository.planDueReminders(dueNow, dueNow + EXPANSION_HORIZON_MS),
      0,
      'the retained tombstone suppresses the reminder',
    );
    assert.equal(jobCount(harness), 1, 'no new job row exists');
    assert.equal(telegramCalls(harness), 1, 'Telegram is never called a second time');

    // Past the long retention the transient sent row is collected, but the
    // ledger identity is permanent: storage of the job row stays bounded while
    // the reminder can never re-send.
    harness.clock.set(start + SENT_REMINDER_RETENTION_MS + MS_PER_MINUTE);
    await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, CLEANUP_LIMIT);
    assert.equal(jobCount(harness, jobId), 0, 'the transient sent row is collected');
    assert.equal(ledgerCount(harness), 1, 'the delivery identity is never deleted');
    assert.equal(
      await harness.repository.planDueReminders(
        harness.clock.now(),
        harness.clock.now() + EXPANSION_HORIZON_MS,
      ),
      0,
    );
  });

  it('does not recreate a reminder for an unchanged occurrence that survives cleanup', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const jobIds = await seedDueJobs(harness, [1], {
      occurrenceKey: 'same#1',
      startsAtMs: now + 10 * MS_PER_MINUTE,
    });
    const jobId = jobIds[0] ?? '';
    await sendReminder(harness, jobId);

    // Cleanup a minute after the send: the 24h occurrence cutoff is still
    // behind the future occurrence, so it survives unchanged.
    const cleanupAt = now + MS_PER_MINUTE;
    harness.clock.set(cleanupAt);
    await harness.repository.cleanup(cleanupAt, RETENTION_MS, CLEANUP_LIMIT);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
    assert.equal(jobCount(harness, jobId), 1);

    // The unchanged occurrence is a genuine planning candidate and the
    // retained sent row keeps planning a no-op.
    const replanAt = now + 5 * MS_PER_MINUTE;
    harness.clock.set(replanAt);
    assert.equal(dueCandidates(harness, replanAt), 1);
    assert.equal(
      await harness.repository.planDueReminders(replanAt, replanAt + EXPANSION_HORIZON_MS),
      0,
      'no duplicate reminder may be planned after cleanup',
    );
    assert.equal(jobCount(harness), 1);
  });

  it('collects transient sent rows after the long retention while the ledger persists', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const jobIds = await seedDueJobs(harness, [1, 2, 3], {
      occurrenceKey: 'bound#1',
      startsAtMs: now + 10 * MS_PER_MINUTE,
    });
    for (const jobId of jobIds) {
      await sendReminder(harness, jobId);
    }
    assert.equal(sentCount(harness), 3);

    // The short retention window has passed; the transient rows must still exist.
    harness.clock.set(now + RETENTION_MS + MS_PER_MINUTE);
    await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, CLEANUP_LIMIT);
    assert.equal(sentCount(harness), 3, 'sent job rows outlive the 24h policy');
    assert.equal(ledgerCount(harness), 3, 'every send recorded its ledger identity');

    // Age them past the transient bound: each pass collects at most `limit`
    // job rows while the ledger rows are never touched.
    harness.db.database
      .prepare('UPDATE outbound_jobs SET updated_at_ms = ?, send_at_ms = ? WHERE status = ?')
      .run(now - SENT_REMINDER_RETENTION_MS - 1, now - SENT_REMINDER_RETENTION_MS - 1, 'sent');
    harness.clock.set(now + SENT_REMINDER_RETENTION_MS + MS_PER_MINUTE);
    await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, 2);
    assert.equal(sentCount(harness), 1, 'the LIMIT bounds one cleanup pass');
    assert.equal(ledgerCount(harness), 3, 'cleanup never deletes delivery identities');
    await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, 2);
    assert.equal(sentCount(harness), 0, 'transient storage is bounded');
    assert.equal(ledgerCount(harness), 3, 'the ledger persists after its job rows drain');
  });

  it('does not recreate a sent reminder when the same occurrence moves later', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    const jobIds = await seedDueJobs(harness, [1], {
      occurrenceKey: 'move#1',
      startsAtMs: start + 10 * MS_PER_MINUTE,
    });
    const jobId = jobIds[0] ?? '';
    await sendReminder(harness, jobId);

    // The same stable occurrence id is moved three days later before cleanup.
    const movedStartMs = start + 3 * MS_PER_DAY;
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'move#1', startsAtMs: movedStartMs })],
      start,
    );

    harness.clock.set(start + RETENTION_MS + MS_PER_MINUTE);
    await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, CLEANUP_LIMIT);
    assert.equal(jobCount(harness), 1, 'the moved occurrence keeps the sent identity alive');
    assert.equal(ledgerCount(harness), 1, 'the ledger keeps the permanent identity');
    assert.equal((await harness.repository.getJob(jobId))?.status, 'sent');

    // The moved occurrence becomes due again and is a real planning candidate.
    const dueNow = movedStartMs - 10 * MS_PER_MINUTE;
    harness.clock.set(dueNow);
    const candidates = countRows(
      harness.db,
      `SELECT COUNT(*) AS n FROM occurrences o
       JOIN users u ON u.course = o.course
       WHERE u.active = 1 AND o.status = 'confirmed'
         AND o.starts_at_ms > ${dueNow}
         AND o.starts_at_ms <= ${dueNow + EXPANSION_HORIZON_MS}
         AND o.starts_at_ms - u.reminder_offset_minutes * ${MS_PER_MINUTE} <= ${dueNow}`,
    );
    assert.equal(candidates, 1, 'the fixture is genuinely due before the replan');

    const replanned = await harness.repository.planDueReminders(dueNow, dueNow + EXPANSION_HORIZON_MS);
    assert.equal(replanned, 0, 'the sent row suppresses a second reminder');
    assert.equal(jobCount(harness), 1);
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.status, 'sent');
    assert.equal(Number(job?.attempt_count), 1);
  });

  it('never lets a negative retention reopen a future sent identity', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const jobIds = await seedDueJobs(harness, [1], {
      occurrenceKey: 'future#1',
      startsAtMs: now + 10 * MS_PER_MINUTE,
    });
    const jobId = jobIds[0] ?? '';
    await sendReminder(harness, jobId);

    // A negative retention computes a cutoff in the future. The occurrence
    // delete is clamped to `now`, so a future-planable occurrence, its sent job
    // row and its ledger row survive and the planner cannot create a duplicate
    // reminder.
    await harness.repository.cleanup(now, -RETENTION_MS, CLEANUP_LIMIT);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 1);
    assert.equal(jobCount(harness, jobId), 1);
    assert.equal(ledgerCount(harness), 1);
    assert.equal((await harness.repository.getJob(jobId))?.status, 'sent');
    assert.equal(
      await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS),
      0,
      'no duplicate reminder may be planned after cleanup',
    );
  });

  it('collects staging rows of dead attempts without touching live ones', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const cutoff = now - SOURCE_LEASE_MS;
    const insertStaging = harness.db.database.prepare(
      `INSERT INTO occurrence_staging (
         attempt_id, id, source_id, uid, occurrence_key, course, starts_at_ms, ends_at_ms,
         summary, description, url, status, is_all_day, updated_at_ms
       ) VALUES (?, ?, 'basic', 'u', ?, 'basic', 1, NULL, ?, NULL, NULL, 'confirmed', 0, ?)`,
    );
    insertStaging.run('dead#7', 'basic:dead', 'dead', 'Dead', cutoff - 1);
    insertStaging.run('live#8', 'basic:live', 'live', 'Live', cutoff + 1);

    harness.db.stats.reset();
    await harness.repository.cleanup(now, RETENTION_MS, CLEANUP_LIMIT);
    assert.equal(harness.db.stats.statements, 7, 'cleanup is seven bounded statements');
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrence_staging WHERE attempt_id = 'dead#7'"),
      0,
      'stage-then-crash leftovers are collected',
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrence_staging WHERE attempt_id = 'live#8'"),
      1,
      "a live attempt's staging rows are never swept",
    );
  });

  it('deletes terminal command replies on the flat 24-hour policy', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(222, 222, now);
    await harness.repository.insertCommandJob({
      id: 'cmd-1',
      telegramUserId: 222,
      chatId: 222,
      payloadJson: JSON.stringify({ text: 'hello' }),
      dedupKey: 'cmd-1',
      sendAtMs: now,
      now,
      expectedRevision: 1,
    });
    await sendReminder(harness, 'cmd-1', 'consumer', null);

    harness.clock.set(now + RETENTION_MS + 1);
    await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, CLEANUP_LIMIT);
    assert.equal(jobCount(harness, 'cmd-1'), 0, 'command replies keep the 24h retention');
  });

  it('still revives cancelled rows and reschedules material revision changes', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const jobIds = await seedDueJobs(harness, [1], {
      occurrenceKey: 'rev#1',
      startsAtMs: now + 10 * MS_PER_MINUTE,
    });
    const jobId = jobIds[0] ?? '';

    // `/stop` (and the stale sweep) cancel enqueued reservations too.
    assert.equal(await harness.repository.cancelPendingJobsForUser(1, now), 1);
    assert.equal((await harness.repository.getJob(jobId))?.status, 'cancelled');
    assert.equal(
      await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS),
      1,
      'a cancelled row is revived by planning',
    );
    assert.equal((await harness.repository.getJob(jobId))?.status, 'pending');

    // A material revision change reschedules from send_at and resets attempts.
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'rev#1', startsAtMs: now + 10 * MS_PER_MINUTE, summary: 'Moved' })],
      now,
    );
    harness.db.database
      .prepare("UPDATE outbound_jobs SET attempt_count = 4, last_error_code = 'transient_500' WHERE id = ?")
      .run(jobId);
    assert.equal(
      await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS),
      1,
      'a material revision change rewrites the row',
    );
    const job = await harness.repository.getJob(jobId);
    assert.equal(Number(job?.expected_revision), 2);
    assert.equal(Number(job?.attempt_count), 0, 'a rescheduled reminder restarts its attempts');
    assert.equal(job?.last_error_code, null);
    assert.equal(Number(job?.next_attempt_at_ms), Number(job?.send_at_ms));
  });
});
