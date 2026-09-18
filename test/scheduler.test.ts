import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { runSchedulerTick } from '../src/scheduler/tick.ts';
import {
  CLEANUP_LIMIT,
  EXPANSION_HORIZON_MS,
  JOB_ENQUEUE_LEASE_MS,
  MAX_JOB_ATTEMPTS,
  MS_PER_MINUTE,
  RETENTION_MS,
  SENT_REMINDER_RETENTION_MS,
} from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { createHarness, schedulerDeps } from './helpers/harness.ts';
import {
  occurrence,
  onboardUser,
  seedDueReminders,
  seedReminderOffsets,
  seedSource,
} from './helpers/seed.ts';

describe('scheduler planning and claiming', () => {
  it('plans a due reminder once and claims it for exactly one owner', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedDueReminders(harness, [111]);

    const first = await harness.repository.claimDueJobs('owner-a', now, 60_000, 100);
    assert.equal(first.length, 1);
    const second = await harness.repository.claimDueJobs('owner-b', now, 60_000, 100);
    assert.equal(second.length, 0);
  });

  it('never recreates a reminder that was already sent', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedDueReminders(harness, [111]);
    const claimed = await harness.repository.claimDueJobs('owner-a', now, 60_000, 100);
    const jobId = claimed[0]?.jobId ?? '';
    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'owner-a', now, 60_000),
      null,
    );
    assert.equal(await harness.repository.finishJobSent(jobId, 'owner-a', now), true);

    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    const again = await harness.repository.claimDueJobs('owner-c', now, 60_000, 100);
    assert.equal(again.length, 0);
    assert.equal((await harness.repository.getJob(jobId))?.status, 'sent');
  });

  it('does not plan past or far-future occurrences', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.activateUser(111, 111, now);
    onboardUser(harness, 111);
    await harness.repository.upsertOccurrences(
      [
        occurrence({ occurrenceKey: 'past', startsAtMs: now - 10 * MS_PER_MINUTE }),
        occurrence({ occurrenceKey: 'far', startsAtMs: now + 40 * 24 * 60 * MS_PER_MINUTE }),
      ],
      now,
    );
    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    const claimed = await harness.repository.claimDueJobs('owner', now, 60_000, 100);
    assert.equal(claimed.length, 0);
  });

  it('plans only the at-start job for an occurrence inside its grace window', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.activateUser(111, 111, now);
    onboardUser(harness, 111);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'late', startsAtMs: now - MS_PER_MINUTE })],
      now,
    );
    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    const claimed = await harness.repository.claimDueJobs('owner', now, 60_000, 100);
    assert.equal(claimed.length, 1, 'the at-start job covers a just-started occurrence');
    const job = await harness.repository.getJob(claimed[0]?.jobId ?? '');
    assert.equal(Number(job?.reminder_offset_minutes), 0);
  });

  it('updates the pending job on a material occurrence change without duplicating', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.activateUser(111, 111, now);
    onboardUser(harness, 111);
    seedReminderOffsets(harness, 111, [30]);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'a#1', startsAtMs: now + 10 * MS_PER_MINUTE, summary: 'Old' })],
      now,
    );
    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'a#1', startsAtMs: now + 12 * MS_PER_MINUTE, summary: 'New' })],
      now,
    );
    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);

    const claimed = await harness.repository.claimDueJobs('owner', now, 60_000, 100);
    assert.equal(claimed.length, 1);
    const jobId = claimed[0]?.jobId ?? '';
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.expected_revision, 2);
    assert.equal(job?.send_at_ms, now + 12 * MS_PER_MINUTE - 30 * MS_PER_MINUTE);
  });

  it('cancels pending reminders when the user stops', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedDueReminders(harness, [111]);
    await harness.repository.deactivateUser(111, now);
    await harness.repository.cancelStaleJobs(now);
    const claimed = await harness.repository.claimDueJobs('owner', now, 60_000, 100);
    assert.equal(claimed.length, 0);
  });

  it('keeps a stopped reminder cancelled until the user restarts', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedDueReminders(harness, [111]);

    // /stop cancels the pending reminder and deactivates the user.
    await harness.repository.cancelPendingJobsForUser(111, now);
    await harness.repository.deactivateUser(111, now);
    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    assert.equal((await harness.repository.claimDueJobs('o', now, 60_000, 100)).length, 0);

    // /start re-activates the user and the reminder is planned again.
    await harness.repository.activateUser(111, 111, now);
    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    assert.equal((await harness.repository.claimDueJobs('o', now, 60_000, 100)).length, 1);
  });

  it('preserves retry backoff and attempts across re-planning until a material change', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedDueReminders(harness, [111]);
    const claimed = await harness.repository.claimDueJobs('o1', now, 60_000, 100);
    const jobId = claimed[0]?.jobId ?? '';
    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'o1', now, 60_000),
      null,
    );
    // One started Telegram call: only then may the job carry an attempt.
    assert.deepEqual(await harness.repository.beginSendAttempt(jobId, 'o1', now, MAX_JOB_ATTEMPTS), {
      status: 'reserved',
      attempt: 1,
      userTimeZone: 'Europe/Moscow',
    });
    await harness.repository.rescheduleJob(
      jobId,
      'o1',
      now + 5_000,
      'transient_server',
      now,
    );

    // The next cron tick re-plans the same occurrence: the pending retry keeps
    // its backoff deadline and attempt counter.
    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    const pending = await harness.repository.getJob(jobId);
    assert.equal(pending?.last_error_code, 'transient_server');
    assert.equal(Number(pending?.next_attempt_at_ms), now + 5_000);
    assert.equal(Number(pending?.attempt_count), 1);

    // A material occurrence change resets the retry state to the new schedule.
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'a#1', startsAtMs: now + 12 * MS_PER_MINUTE })],
      now,
    );
    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    const reset = await harness.repository.getJob(jobId);
    assert.equal(Number(reset?.attempt_count), 0);
    assert.equal(reset?.last_error_code, null);
    assert.equal(Number(reset?.next_attempt_at_ms), now + 12 * MS_PER_MINUTE - 30 * MS_PER_MINUTE);
  });

  it('keeps the occurrence revision stable when nothing material changed', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    const row = occurrence({ occurrenceKey: 'a#1', startsAtMs: now + 10 * MS_PER_MINUTE });
    await harness.repository.upsertOccurrences([row], now);
    await harness.repository.upsertOccurrences([row], now + 1_000);

    const stored = harness.db.database
      .prepare('SELECT revision FROM occurrences WHERE id = ?')
      .get(row.id) as { revision: number };
    assert.equal(Number(stored.revision), 1);
  });

  it('does not duplicate the Queue backlog on overlapping ticks', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const startedAt = harness.clock.now();
    await seedDueReminders(harness, [111]);

    const first = await runSchedulerTick(schedulerDeps(harness));
    assert.equal(first.enqueued, 1);
    const jobId = harness.queue.batches.flat()[0]?.jobId ?? '';
    const enqueued = await harness.repository.getJob(jobId);
    assert.equal(
      Number(enqueued?.lease_expires_at_ms),
      startedAt + JOB_ENQUEUE_LEASE_MS,
      'the enqueue reservation uses the longer backlog lease',
    );

    // An overlapping tick sees the still-live reservation and adds nothing.
    const second = await runSchedulerTick(schedulerDeps(harness));
    assert.equal(second.enqueued, 0);
    assert.equal(harness.queue.batches.flat().length, 1);

    // Only after that reservation expires is the job repaired and re-enqueued.
    harness.clock.advance(JOB_ENQUEUE_LEASE_MS + 1);
    const third = await runSchedulerTick(schedulerDeps(harness));
    assert.equal(third.repaired, 1);
    assert.equal(third.enqueued, 1);
    assert.equal(harness.queue.batches.flat().length, 2);
    const repaired = await harness.repository.getJob(jobId);
    assert.equal(Number(repaired?.attempt_count), 0, 'reservation and repair consume no attempts');
  });

  it('returns expired leases to the pending pool so an enqueue crash is repaired', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedDueReminders(harness, [111]);
    const claimed = await harness.repository.claimDueJobs('crashed-owner', now, 60_000, 100);
    const jobId = claimed[0]?.jobId ?? '';

    harness.clock.advance(61_000);
    assert.equal(await harness.repository.repairExpiredLeases(harness.clock.now()), 1);
    assert.equal((await harness.repository.getJob(jobId))?.status, 'pending');

    const reclaimed = await harness.repository.claimDueJobs(
      'next-owner',
      harness.clock.now(),
      60_000,
      100,
    );
    assert.equal(reclaimed.length, 1);
  });

  it('does not collide reminders when different sources share an occurrence key', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await seedSource(harness.repository, 'extended', now);
    await harness.repository.activateUser(111, 111, now);
    onboardUser(harness, 111);
    seedReminderOffsets(harness, 111, [30]);
    await harness.repository.upsertOccurrences(
      [
        occurrence({
          id: 'basic:shared#1',
          sourceId: 'basic',
          course: 'basic',
          occurrenceKey: 'shared#1',
          startsAtMs: now + 10 * MS_PER_MINUTE,
        }),
        occurrence({
          id: 'extended:shared#1',
          sourceId: 'extended',
          course: 'extended',
          occurrenceKey: 'shared#1',
          startsAtMs: now + 20 * MS_PER_MINUTE,
        }),
      ],
      now,
    );

    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    await harness.repository.setUserCourse(111, 'extended', now);
    await harness.repository.cancelPendingJobsForUser(111, now);
    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);

    const claimed = await harness.repository.claimDueJobs('owner', now, 60_000, 100);
    assert.equal(claimed.length, 1);
    const job = await harness.repository.getJob(claimed[0]?.jobId ?? '');
    assert.equal(job?.occurrence_id, 'extended:shared#1');
    assert.equal(Number(job?.send_at_ms), now + 20 * MS_PER_MINUTE - 30 * MS_PER_MINUTE);
  });

  it('cleans up only terminal work older than the retention window', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedDueReminders(harness, [111]);
    const claimed = await harness.repository.claimDueJobs('o', now, 60_000, 100);
    const jobId = claimed[0]?.jobId ?? '';
    assert.notEqual(
      await harness.repository.claimJobContext(jobId, 'o', now, 60_000),
      null,
    );
    await harness.repository.finishJobSent(jobId, 'o', now);
    await harness.repository.tryBeginUpdate(1, 'o', now, 60_000);
    await harness.repository.completeUpdate(1, 'o', now);

    await harness.repository.cleanup(now, RETENTION_MS, CLEANUP_LIMIT);
    assert.notEqual(
      await harness.repository.getJob(jobId),
      null,
      'a sent reminder is the durable dedup identity while its occurrence exists',
    );

    // The occurrence started ten minutes in and the sent reminder is the
    // durable dedup identity: both the occurrence retention and the sent
    // reminder retention must pass before the pair is collectible.
    harness.clock.advance(SENT_REMINDER_RETENTION_MS + 20 * MS_PER_MINUTE);
    await harness.repository.cleanup(harness.clock.now(), RETENTION_MS, CLEANUP_LIMIT);
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrences WHERE occurrence_key = 'a#1'"),
      0,
      'the occurrence is deleted first',
    );
    assert.equal(await harness.repository.getJob(jobId), null);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM processed_updates'), 0);
  });

  it('keeps planning and claiming within a small statement budget', async () => {
    const harness = createHarness({ now: 1_700_000_000_000 });
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.activateUser(111, 111, now);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'a#1', startsAtMs: now + 10 * MS_PER_MINUTE })],
      now,
    );
    harness.db.stats.reset();
    await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    await harness.repository.claimDueJobs('owner', now, 60_000, 100);
    assert.ok(harness.db.stats.statements <= 4, `used ${harness.db.stats.statements} statements`);
    assert.ok(harness.db.stats.maxParamsPerStatement <= 90);
  });
});
