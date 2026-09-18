/**
 * User-tunable reminder rules: every user keeps an arbitrary set of lead times.
 * These regressions drive the real handler, repository, queue spy and consumer:
 * defaults, zero rules, custom offsets, add/edit/delete/toggle/clear, invalid
 * input, replanning without duplicates and the consumer-side rule guard.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueMessage } from '../src/queue/consumer.ts';
import { handleUpdate, type HandlerDeps } from '../src/telegram/handlers.ts';
import {
  DEFAULT_REMINDER_OFFSETS,
  MAX_REMINDER_OFFSET_MINUTES,
  MAX_REMINDER_RULES_PER_USER,
} from '../src/domain/notification-policy.ts';
import { REMINDERS_INVALID_TEXT } from '../src/telegram/replies.ts';
import {
  parseUpdate,
  type ParsedUpdate,
  type PrivateCallbackUpdate,
  type PrivateMessageUpdate,
} from '../src/telegram/updates.ts';
import { EXPANSION_HORIZON_MS, JOB_LEASE_MS, MS_PER_MINUTE } from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { consumerDeps, createHarness, type Harness } from './helpers/harness.ts';
import {
  makeQueueMessage,
  occurrence,
  onboardUser,
  seedDueJobs,
  seedSource,
} from './helpers/seed.ts';

const USER_ID = 8800;

function handlerDeps(harness: Harness): HandlerDeps {
  return {
    repository: harness.repository,
    telegram: harness.telegram,
    queue: harness.queue.producer,
    now: harness.now,
    logger: harness.logger.logger,
    idFactory: harness.idFactory,
    eventsLimit: 10,
  };
}

function parseOrFail(payload: unknown): ParsedUpdate {
  const parsed = parseUpdate(payload);
  if (!parsed.ok) {
    throw new Error(`expected a parsed update, got ${parsed.reason}`);
  }
  return parsed.update;
}

function messageUpdate(updateId: number, text: string): PrivateMessageUpdate {
  const update = parseOrFail({
    update_id: updateId,
    message: {
      message_id: updateId,
      chat: { id: USER_ID, type: 'private' },
      from: { id: USER_ID, username: 'andrew' },
      text,
    },
  });
  if (update.kind !== 'message') {
    throw new Error('expected a private message update');
  }
  return update;
}

function callbackUpdate(updateId: number, data: string): PrivateCallbackUpdate {
  const update = parseOrFail({
    update_id: updateId,
    callback_query: {
      id: `callback-${updateId}`,
      from: { id: USER_ID, username: 'andrew' },
      message: { message_id: updateId, chat: { id: USER_ID, type: 'private' } },
      data,
    },
  });
  if (update.kind !== 'callback') {
    throw new Error('expected a private callback update');
  }
  return update;
}

function lastJobPayload(harness: Harness): { text?: string } {
  const id = harness.queue.batches.flat().at(-1)?.jobId ?? '';
  const raw = harness.db.database
    .prepare('SELECT payload_json FROM outbound_jobs WHERE id = ?')
    .get(id) as { payload_json: unknown } | undefined;
  return JSON.parse(String(raw?.payload_json ?? '{}')) as { text?: string };
}

function reminderJobs(harness: Harness, userId: number = USER_ID): Record<string, unknown>[] {
  return harness.db.database
    .prepare(
      "SELECT * FROM outbound_jobs WHERE kind = 'reminder' AND telegram_user_id = ? ORDER BY reminder_offset_minutes ASC, id",
    )
    .all(userId) as Record<string, unknown>[];
}

/** Activates and onboards a fixture user, returning the harness. */
async function activeUser(harness: Harness): Promise<void> {
  const now = harness.clock.now();
  await seedSource(harness.repository, 'basic', now);
  await harness.repository.activateUser(USER_ID, USER_ID, now);
  onboardUser(harness, USER_ID);
}

describe('reminder rule defaults', () => {
  it('a new user gets exactly the three standard rules', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(USER_ID, USER_ID, now);

    assert.deepEqual(
      await harness.repository.listReminderOffsets(USER_ID),
      [...DEFAULT_REMINDER_OFFSETS],
      '1440/60/5 are enabled by default',
    );
    assert.deepEqual(DEFAULT_REMINDER_OFFSETS, [1440, 60, 5]);
  });

  it('plans one job per due standard rule with a stable offset dedup key', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await activeUser(harness);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'due', startsAtMs: now + MS_PER_MINUTE })],
      now,
    );

    assert.equal(
      await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS),
      3,
      'all three standard rules are due for an event one minute away',
    );
    assert.deepEqual(
      reminderJobs(harness).map((job) => [job.dedup_key, job.reminder_offset_minutes, job.send_at_ms]),
      [
        [`rem:${USER_ID}:basic:due:5`, 5, now + MS_PER_MINUTE - 5 * MS_PER_MINUTE],
        [`rem:${USER_ID}:basic:due:60`, 60, now + MS_PER_MINUTE - 60 * MS_PER_MINUTE],
        [`rem:${USER_ID}:basic:due:1440`, 1440, now + MS_PER_MINUTE - 1440 * MS_PER_MINUTE],
      ],
    );
  });

  it('replanning the same rules does not duplicate jobs', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await activeUser(harness);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'due', startsAtMs: now + MS_PER_MINUTE })],
      now,
    );

    assert.equal(await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS), 3);
    assert.equal(
      await harness.repository.planDueReminders(now + 1_000, now + 1_000 + EXPANSION_HORIZON_MS),
      0,
      'an unchanged replan is a no-op',
    );
    assert.equal(reminderJobs(harness).length, 3);
  });
});

describe('reminder rule sets', () => {
  it('zero rules produce no reminder jobs', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await activeUser(harness);
    assert.equal(await harness.repository.setUserReminderOffsets(USER_ID, [], now), true);
    assert.deepEqual(await harness.repository.listReminderOffsets(USER_ID), []);

    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'due', startsAtMs: now + MS_PER_MINUTE })],
      now,
    );
    assert.equal(await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS), 0);
    assert.equal(reminderJobs(harness).length, 0);
  });

  it('a custom offset is planned with the exact send time', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await activeUser(harness);
    await harness.repository.setUserReminderOffsets(USER_ID, [90], now);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'custom', startsAtMs: now + 60 * MS_PER_MINUTE })],
      now,
    );

    assert.equal(await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS), 1);
    const [job] = reminderJobs(harness);
    assert.equal(Number(job?.reminder_offset_minutes), 90);
    assert.equal(Number(job?.send_at_ms), now + 60 * MS_PER_MINUTE - 90 * MS_PER_MINUTE);
  });

  it('changing the set cancels old pending jobs and plans only the new set', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await activeUser(harness);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'due', startsAtMs: now + MS_PER_MINUTE })],
      now,
    );
    assert.equal(await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS), 3);

    await harness.repository.setUserReminderOffsets(USER_ID, [90], now);
    assert.equal(await harness.repository.cancelStaleJobs(now), 3, 'stale rule jobs are cancelled');
    assert.equal(
      (await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS)),
      1,
      'only the new rule is planned',
    );

    const byOffset = reminderJobs(harness).map((job) => [
      Number(job.reminder_offset_minutes),
      String(job.status),
    ]);
    assert.deepEqual(byOffset, [
      [5, 'cancelled'],
      [60, 'cancelled'],
      [90, 'pending'],
      [1440, 'cancelled'],
    ]);
    assert.equal(
      reminderJobs(harness).filter((job) => String(job.status) === 'pending').length,
      1,
      'exactly one live job, no duplicates',
    );
  });

  it('a refused edit keeps the old rule instead of dropping it', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(USER_ID, USER_ID, now);
    await harness.repository.setUserReminderOffsets(USER_ID, [60, 30], now);

    assert.equal(
      await harness.repository.editReminderOffset(USER_ID, 60, 30, now),
      false,
      'renaming onto an already enabled rule is refused',
    );
    assert.deepEqual(
      await harness.repository.listReminderOffsets(USER_ID),
      [60, 30],
      'the source rule is never removed before the target is written',
    );
  });

  it('enforces the per-user cap and validates offsets before writing', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(USER_ID, USER_ID, now);
    await harness.repository.setUserReminderOffsets(USER_ID, [], now);

    assert.equal(await harness.repository.addReminderOffset(USER_ID, 0, now), false);
    assert.equal(
      await harness.repository.addReminderOffset(USER_ID, MAX_REMINDER_OFFSET_MINUTES + 1, now),
      false,
      'one minute past the 30-day maximum is rejected',
    );
    assert.equal(
      await harness.repository.addReminderOffset(USER_ID, MAX_REMINDER_OFFSET_MINUTES, now),
      true,
      'the 30-day maximum is accepted',
    );
    assert.deepEqual(
      await harness.repository.listReminderOffsets(USER_ID),
      [MAX_REMINDER_OFFSET_MINUTES],
    );

    await harness.repository.setUserReminderOffsets(USER_ID, [], now);
    for (let offset = 1; offset <= MAX_REMINDER_RULES_PER_USER; offset += 1) {
      assert.equal(
        await harness.repository.addReminderOffset(USER_ID, offset, now),
        true,
        `rule ${offset} is accepted below the cap`,
      );
    }
    assert.equal(
      (await harness.repository.listReminderOffsets(USER_ID)).length,
      MAX_REMINDER_RULES_PER_USER,
      'exactly the cap is stored',
    );
    assert.equal(
      await harness.repository.addReminderOffset(USER_ID, MAX_REMINDER_RULES_PER_USER + 1, now),
      false,
      'the rule past the cap is rejected',
    );
    assert.equal(
      (await harness.repository.listReminderOffsets(USER_ID)).length,
      MAX_REMINDER_RULES_PER_USER,
      'the rejected rule did not extend the set',
    );
  });
});

describe('reminder write statement budget', () => {
  it('keeps add, edit, delete, clear and a fresh /start at their fixed statement cost', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const repository = harness.repository;

    repository.beginInvocation(3);
    await repository.activateUser(USER_ID, USER_ID, now);
    assert.equal(
      repository.statementsUsed(),
      3,
      'fresh /start: user upsert, read-back and one bulk default-rule insert',
    );

    repository.beginInvocation(2);
    assert.equal(await repository.addReminderOffset(USER_ID, 30, now), true);
    assert.equal(repository.statementsUsed(), 2, 'add: revision bump + insert');

    repository.beginInvocation(2);
    assert.equal(await repository.editReminderOffset(USER_ID, 30, 45, now), true);
    assert.equal(repository.statementsUsed(), 2, 'edit: revision bump + rename');

    repository.beginInvocation(2);
    assert.equal(await repository.removeReminderOffset(USER_ID, 45, now), true);
    assert.equal(repository.statementsUsed(), 2, 'delete: revision bump + delete');

    repository.beginInvocation(2);
    assert.equal(await repository.setUserReminderOffsets(USER_ID, [], now), true);
    assert.equal(repository.statementsUsed(), 2, 'clear: revision bump + delete all');

    repository.beginInvocation(1);
    assert.equal(
      (await repository.listReminderOffsets(USER_ID)).length,
      0,
      'the whole sequence left the set empty',
    );
  });
});

describe('reminder settings command', () => {
  it('adds, edits, deletes and clears rules through /reminders', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    assert.deepEqual(await harness.repository.listReminderOffsets(USER_ID), [1440, 60, 5]);

    await handleUpdate(messageUpdate(2, '/reminders add 90'), deps);
    assert.deepEqual(await harness.repository.listReminderOffsets(USER_ID), [1440, 90, 60, 5]);

    await handleUpdate(messageUpdate(3, '/reminders edit 90 120'), deps);
    assert.deepEqual(await harness.repository.listReminderOffsets(USER_ID), [1440, 120, 60, 5]);

    await handleUpdate(messageUpdate(4, '/reminders del 120'), deps);
    assert.deepEqual(await harness.repository.listReminderOffsets(USER_ID), [1440, 60, 5]);

    await handleUpdate(messageUpdate(5, '/reminders clear'), deps);
    assert.deepEqual(await harness.repository.listReminderOffsets(USER_ID), []);
    assert.match(lastJobPayload(harness).text ?? '', /Напоминания/);
  });

  it('keeps the set empty when /start follows /reminders clear', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);

    await handleUpdate(messageUpdate(1, '/start'), deps);
    assert.deepEqual(
      await harness.repository.listReminderOffsets(USER_ID),
      [1440, 60, 5],
      'a first /start seeds the standard rules',
    );

    await handleUpdate(messageUpdate(2, '/reminders clear'), deps);
    assert.deepEqual(await harness.repository.listReminderOffsets(USER_ID), []);

    await handleUpdate(messageUpdate(3, '/start'), deps);
    assert.deepEqual(
      await harness.repository.listReminderOffsets(USER_ID),
      [],
      'reactivating an existing user must not restore defaults',
    );
  });

  it('rejects invalid /reminders input without mutating the set', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    const before = await harness.repository.listReminderOffsets(USER_ID);

    const invalid = [
      '/reminders add 0',
      '/reminders add 43201',
      '/reminders add abc',
      '/reminders add',
      '/reminders edit 60',
      '/reminders edit 60 x',
      '/reminders del nope',
      '/reminders nonsense',
      '/reminders clear extra',
    ];
    for (const [index, text] of invalid.entries()) {
      await handleUpdate(messageUpdate(10 + index, text), deps);
      assert.deepEqual(
        await harness.repository.listReminderOffsets(USER_ID),
        before,
        `${text} must not mutate`,
      );
    }
    assert.match(lastJobPayload(harness).text ?? '', new RegExp(REMINDERS_INVALID_TEXT.slice(0, 20)));
  });

  it('accepts the 30-day offset boundary and rejects one minute past it', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);

    await handleUpdate(messageUpdate(2, '/reminders add 43200'), deps);
    assert.deepEqual(
      await harness.repository.listReminderOffsets(USER_ID),
      [43200, 1440, 60, 5],
      'the 30-day maximum is accepted through the command',
    );

    const before = await harness.repository.listReminderOffsets(USER_ID);
    await handleUpdate(messageUpdate(3, '/reminders add 43201'), deps);
    assert.deepEqual(
      await harness.repository.listReminderOffsets(USER_ID),
      before,
      'one minute past the maximum does not mutate the set',
    );
  });

  it('toggles standard rules and clears through callbacks', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);

    await handleUpdate(callbackUpdate(2, 'rm:t:5'), deps);
    assert.deepEqual(await harness.repository.listReminderOffsets(USER_ID), [1440, 60]);

    await handleUpdate(callbackUpdate(3, 'rm:t:5'), deps);
    assert.deepEqual(await harness.repository.listReminderOffsets(USER_ID), [1440, 60, 5]);

    await handleUpdate(callbackUpdate(4, 'rm:clear'), deps);
    assert.deepEqual(await harness.repository.listReminderOffsets(USER_ID), []);
  });

  it('a stale callback cannot mutate a set a newer command already replaced', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await harness.repository.recordCommandUpdate(USER_ID, USER_ID, 50, now);

    await handleUpdate(callbackUpdate(10, 'rm:t:5'), deps);
    assert.deepEqual(
      await harness.repository.listReminderOffsets(USER_ID),
      [1440, 60, 5],
      'the older callback is rejected by the ordering guard',
    );
  });
});

describe('reminder rule consumer guard', () => {
  it('does not deliver a reminder whose rule was removed after planning', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [USER_ID], { reminderOffsets: [30] });
    assert.notEqual(jobId, '');

    await harness.repository.setUserReminderOffsets(USER_ID, [], now);
    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));

    assert.equal(outcome, 'terminal');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'skip_offset-mismatch');
    assert.equal(
      harness.fetchSpy.calls.filter((call) => call.url.includes('/sendMessage')).length,
      0,
      'no Telegram request for a removed rule',
    );
  });

  it('does not resend a delivered rule that was removed and re-added', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [USER_ID], { reminderOffsets: [30] });
    assert.equal(await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness)), 'sent');
    assert.equal(
      await harness.repository.claimJobContext(jobId, 'recheck', now, JOB_LEASE_MS),
      null,
      'the sent job cannot be re-claimed',
    );

    await harness.repository.setUserReminderOffsets(USER_ID, [], now);
    await harness.repository.setUserReminderOffsets(USER_ID, [30], now);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'a#1', startsAtMs: now + 10 * MS_PER_MINUTE })],
      now,
    );
    assert.equal(
      await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS),
      0,
      'the ledger suppresses the re-added rule',
    );
    assert.equal(countRows(harness.db, "SELECT COUNT(*) AS n FROM outbound_jobs WHERE kind = 'reminder'"), 1);
  });
});
