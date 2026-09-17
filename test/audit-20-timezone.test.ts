/**
 * Per-user timezone onboarding and settings (prompts/09).
 *
 * Drives the real handler, repository, webhook-equivalent command pipeline and
 * consumer: onboarding before any reminder exists, timezone buttons and
 * `/timezone`, validation and logging safety, ordering guards, `/events` and
 * reminder rendering per recipient, defensive screening, `/stop` authority and
 * migration rehearsal for `0007_user_time_zone.sql`.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { Repository } from '../src/data/repository.ts';
import { processQueueMessage } from '../src/queue/consumer.ts';
import { handleUpdate, type HandlerDeps } from '../src/telegram/handlers.ts';
import {
  EVENTS_GUIDANCE_TEXT,
  INACTIVE_GUIDANCE_TEXT,
  START_TEXT,
  TIMEZONE_CHANGE_HINT,
  TIMEZONE_INVALID_TEXT,
  TIMEZONE_PROMPT_TEXT,
} from '../src/telegram/replies.ts';
import {
  parseUpdate,
  type ParsedUpdate,
  type PrivateCallbackUpdate,
  type PrivateMessageUpdate,
} from '../src/telegram/updates.ts';
import {
  BOT_SEND_PACE_PER_SECOND,
  EXPANSION_HORIZON_MS,
  JOB_LEASE_MS,
  MAX_D1_STATEMENTS_PER_CONSUMER,
  MAX_JOB_ATTEMPTS,
  MS_PER_MINUTE,
} from '../src/util.ts';
import {
  applyMigrations,
  countRows,
  createSqliteD1,
  migrationFiles,
} from './helpers/d1-sqlite.ts';
import { jsonResponse } from './helpers/fakes.ts';
import { consumerDeps, createHarness, type Harness } from './helpers/harness.ts';
import {
  createStatementBarrier,
  makeQueueMessage,
  occurrence,
  onboardUser,
  seedDueJobs,
  seedSource,
} from './helpers/seed.ts';

const USER_ID = 9100;

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
    throw new Error(`expected parsed update, got ${parsed.reason}`);
  }
  return parsed.update;
}

function messageUpdate(
  updateId: number,
  text: string,
  userId: number = USER_ID,
): PrivateMessageUpdate {
  const update = parseOrFail({
    update_id: updateId,
    message: {
      message_id: updateId,
      chat: { id: userId, type: 'private' },
      from: { id: userId, username: 'andrew' },
      text,
    },
  });
  if (update.kind !== 'message') {
    throw new Error('expected a private message update');
  }
  return update;
}

function callbackUpdate(
  updateId: number,
  data: string,
  userId: number = USER_ID,
): PrivateCallbackUpdate {
  const update = parseOrFail({
    update_id: updateId,
    callback_query: {
      id: `callback-${updateId}`,
      from: { id: userId, username: 'andrew' },
      message: { message_id: updateId, chat: { id: userId, type: 'private' } },
      data,
    },
  });
  if (update.kind !== 'callback') {
    throw new Error('expected a private callback update');
  }
  return update;
}

function queuedJobId(harness: Harness, index: number): string {
  const id = harness.queue.batches.flat()[index]?.jobId;
  if (id === undefined) {
    throw new Error(`no queued job at index ${index}`);
  }
  return id;
}

function jobPayload(harness: Harness, jobId: string): string {
  const raw = harness.db.database
    .prepare('SELECT payload_json FROM outbound_jobs WHERE id = ?')
    .get(jobId) as { payload_json: unknown } | undefined;
  return String(raw?.payload_json ?? '');
}

function sentTexts(harness: Harness): string[] {
  return harness.fetchSpy.calls
    .filter((call) => call.url.includes('/sendMessage'))
    .map((call) => (JSON.parse(String(call.init?.body)) as { text: string }).text);
}

function callbackAnswers(harness: Harness): string[] {
  return harness.fetchSpy.calls
    .filter((call) => call.url.includes('/answerCallbackQuery'))
    .map((call) => (JSON.parse(String(call.init?.body)) as { text?: string }).text ?? '');
}

function allJobs(harness: Harness): Record<string, unknown>[] {
  return harness.db.database
    .prepare('SELECT * FROM outbound_jobs ORDER BY created_at_ms ASC, id ASC')
    .all() as Record<string, unknown>[];
}

async function deliver(harness: Harness, jobId: string): Promise<string> {
  return processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
}

describe('Timezone migration 0007', () => {
  it('clean install keeps new users incomplete until a zone is chosen', async () => {
    const db = createSqliteD1();
    applyMigrations(db);
    const columns = db.database.prepare('PRAGMA table_info(users)').all() as {
      name: string;
    }[];
    assert.ok(
      columns.some((column) => column.name === 'time_zone'),
      'clean install creates the time_zone column',
    );

    const repository = new Repository(db);
    const now = 1_700_000_000_000;
    const created = await repository.activateUser(1, 1, now);
    assert.equal(created.timeZone, null, 'a new user starts un-onboarded');
    assert.equal(await repository.setUserTimeZone(1, 'Asia/Yerevan', now), true);
    assert.equal((await repository.getUser(1))?.timeZone, 'Asia/Yerevan');
    db.close();
  });

  it('validates and canonicalizes timezones at the repository boundary', async () => {
    const db = createSqliteD1();
    applyMigrations(db);
    const repository = new Repository(db);
    const now = 1_700_000_000_000;
    await repository.activateUser(1, 1, now);
    const before = await repository.getUser(1);

    for (const invalid of ['', '   ', 'Mars/Phobos', '+03:00', 'A'.repeat(100)]) {
      assert.equal(await repository.setUserTimeZone(1, invalid, now), false, invalid);
      assert.deepEqual(await repository.getUser(1), before, invalid);
    }

    assert.equal(await repository.setUserTimeZone(1, ' Asia/Yerevan ', now), true);
    assert.equal((await repository.getUser(1))?.timeZone, 'Asia/Yerevan');
    assert.equal(await repository.setUserTimeZone(1, 'Asia/Yerevan', now), false);
    db.close();
  });

  it('rejects empty and whitespace-only values at the schema level', () => {
    const db = createSqliteD1();
    applyMigrations(db);
    db.exec(
      `INSERT INTO users (
         telegram_user_id, chat_id, course, reminder_offset_minutes,
         active, revision, created_at_ms, updated_at_ms
       ) VALUES (1, 1, 'basic', 30, 1, 1, 0, 0)`,
    );
    for (const bad of ['', '   ', '\t', '\n', ' \t\n ']) {
      assert.throws(
        () =>
          db.database
            .prepare('UPDATE users SET time_zone = ? WHERE telegram_user_id = 1')
            .run(bad),
        /CHECK constraint failed/,
        bad,
      );
    }
    db.close();
  });

  it('upgrade maps every existing user to Moscow and preserves all other state', async () => {
    const db = createSqliteD1();
    const pre0007 = migrationFiles().filter((name) => name !== '0007_user_time_zone.sql');
    applyMigrations(db, pre0007);

    const repository = new Repository(db);
    const now = 1_700_000_000_000;
    await repository.recordCommandUpdate(11, 11, 5, now);
    await repository.activateUser(11, 11, now, 5);
    await repository.setUserCourse(11, 'extended', now, 5);
    await repository.setUserReminderOffset(11, 1440, now, 5);
    await repository.insertCommandJob({
      id: 'job-11',
      telegramUserId: 11,
      chatId: 11,
      payloadJson: JSON.stringify({ text: 'queued guidance' }),
      dedupKey: 'cmd:5',
      sendAtMs: now,
      now,
      expectedRevision: 3,
      sourceUpdateId: 5,
    });
    await repository.activateUser(12, 12, now);
    db.exec(
      `INSERT INTO delivery_ledger (dedup_key, occurrence_id, telegram_user_id, sent_at_ms)
       VALUES ('rem:12:basic:a#1:30', 'basic:a#1', 12, ${now})`,
    );
    // Attempts already spent on the queued job must survive the upgrade.
    db.exec("UPDATE outbound_jobs SET attempt_count = 3, last_error_code = 'transient_500' WHERE id = 'job-11'");

    interface RawSnapshot {
      users: unknown[];
      jobs: unknown[];
      ledger: unknown[];
      state: unknown[];
    }
    const snapshot = (): RawSnapshot => ({
      users: db.database.prepare('SELECT * FROM users ORDER BY telegram_user_id').all() as unknown[],
      jobs: db.database.prepare('SELECT * FROM outbound_jobs ORDER BY id').all() as unknown[],
      ledger: db.database.prepare('SELECT * FROM delivery_ledger ORDER BY dedup_key').all() as unknown[],
      state: db.database.prepare('SELECT * FROM user_command_state ORDER BY telegram_user_id').all() as unknown[],
    });
    const withoutTimeZone = (rows: unknown[]): unknown[] =>
      rows.map((row) => {
        const copy: Record<string, unknown> = { ...(row as Record<string, unknown>) };
        delete copy.time_zone;
        return copy;
      });
    const before = snapshot();

    applyMigrations(db, ['0007_user_time_zone.sql']);

    const after = snapshot();
    assert.deepEqual(withoutTimeZone(after.users), withoutTimeZone(before.users));
    assert.deepEqual(after.jobs, before.jobs, 'queued jobs and delivery identities survive');
    assert.deepEqual(after.ledger, before.ledger, 'the delivery ledger survives');
    assert.deepEqual(after.state, before.state, 'command ordering state survives');
    const zones = (
      db.database.prepare('SELECT time_zone FROM users ORDER BY telegram_user_id').all() as {
        time_zone: unknown;
      }[]
    ).map((row) => String(row.time_zone));
    assert.deepEqual(zones, ['Europe/Moscow', 'Europe/Moscow'], 'every existing user maps to Moscow');
    db.close();
  });
});

describe('Timezone onboarding', () => {
  it('prompts a new /start user and plans no reminder before the choice', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'due', startsAtMs: now + 10 * MS_PER_MINUTE })],
      now,
    );

    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);

    const user = await harness.repository.getUser(USER_ID);
    assert.equal(user?.active, true, '/start activates the subscription');
    assert.equal(user?.timeZone, null, 'no timezone is silently assumed');

    const promptJobId = queuedJobId(harness, 0);
    assert.match(jobPayload(harness, promptJobId), /Europe\/Moscow/);
    assert.match(jobPayload(harness, promptJobId), /tz:Asia\/Yerevan/);
    assert.equal(await deliver(harness, promptJobId), 'sent');
    assert.match(sentTexts(harness)[0] ?? '', /часовой пояс/i);

    assert.equal(
      await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS),
      0,
      'no reminder may be planned before the timezone choice',
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM outbound_jobs WHERE kind = 'reminder'"),
      0,
    );
  });

  it('persists the Moscow and Yerevan buttons and completes onboarding', async () => {
    for (const [updateId, data, expected] of [
      [10, 'tz:Europe/Moscow', 'Europe/Moscow'],
      [11, 'tz:Asia/Yerevan', 'Asia/Yerevan'],
    ] as const) {
      const harness = createHarness();
      const now = harness.clock.now();
      await seedSource(harness.repository, 'basic', now);
      await harness.repository.upsertOccurrences(
        [occurrence({ occurrenceKey: 'due', startsAtMs: now + 10 * MS_PER_MINUTE })],
        now,
      );
      const deps = handlerDeps(harness);
      await handleUpdate(messageUpdate(1, '/start'), deps);

      await handleUpdate(callbackUpdate(updateId, data), deps);

      const user = await harness.repository.getUser(USER_ID);
      assert.equal(user?.timeZone, expected);
      assert.equal(
        await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS),
        1,
        `${data}: onboarding completion enables reminder planning`,
      );
      const planned = allJobs(harness).find((job) => job.kind === 'reminder');
      assert.equal(planned?.status, 'pending');
    }
  });

  it('sets, changes and canonicalizes the zone; identical retries are idempotent', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);

    await handleUpdate(messageUpdate(2, '/timezone Asia/Yerevan'), deps);
    const afterSet = await harness.repository.getUser(USER_ID);
    assert.equal(afterSet?.timeZone, 'Asia/Yerevan');
    assert.equal(afterSet?.revision, 2, 'completing onboarding bumps the revision once');

    // Webhook retry of the same update: dedup key exists, nothing is replayed.
    await handleUpdate(messageUpdate(2, '/timezone Asia/Yerevan'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 2);
    assert.equal(
      allJobs(harness).filter((job) => job.dedup_key === 'cmd:2').length,
      1,
      'one reply job per update',
    );

    await handleUpdate(messageUpdate(3, '/timezone Europe/Berlin'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.timeZone, 'Europe/Berlin');
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 3);

    // An alias is stored in its canonical form.
    await handleUpdate(messageUpdate(4, '/timezone US/Eastern'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.timeZone, 'America/New_York');
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 4);

    // Re-selecting the same zone changes nothing, even with a new update id.
    await handleUpdate(messageUpdate(5, '/timezone America/New_York'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 4);
    assert.equal(await harness.repository.setUserTimeZone(USER_ID, 'America/New_York', harness.clock.now()), false);
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 4);
  });

  it('rejects empty, whitespace, unsupported, offset and overlong zones without mutation', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/timezone Asia/Yerevan'), deps);
    const before = await harness.repository.getUser(USER_ID);

    const currentZoneGuidance = `Часовой пояс: Asia/Yerevan\n${TIMEZONE_CHANGE_HINT}`;
    const cases: readonly (readonly [string, string])[] = [
      ['/timezone Mars/Phobos', TIMEZONE_INVALID_TEXT],
      ['/timezone +03:00', TIMEZONE_INVALID_TEXT],
      ['/timezone UTC+3', TIMEZONE_INVALID_TEXT],
      ['/timezone Europe/ Moscow', TIMEZONE_INVALID_TEXT],
      [`/timezone ${'A'.repeat(100)}`, TIMEZONE_INVALID_TEXT],
      ['/timezone', currentZoneGuidance], // empty argument: current zone + how to change
      ['/timezone   ', currentZoneGuidance], // whitespace-only argument
    ];
    // Jobs 0 and 1 are the /start prompt and the successful /timezone reply.
    let nextJob = 2;
    for (const [index, [text, expected]] of cases.entries()) {
      await handleUpdate(messageUpdate(10 + index, text), deps);
      const jobId = queuedJobId(harness, nextJob);
      nextJob += 1;
      assert.equal(await deliver(harness, jobId), 'sent');
      assert.equal(sentTexts(harness).at(-1), expected, text);
      // Every reply keeps both timezone choices reachable.
      assert.match(jobPayload(harness, jobId), /tz:Europe\/Moscow/, text);
      assert.match(jobPayload(harness, jobId), /tz:Asia\/Yerevan/, text);
      assert.deepEqual(
        await harness.repository.getUser(USER_ID),
        before,
        `${text}: rejected input must not mutate the user`,
      );
    }

    assert.equal((await harness.repository.getUser(USER_ID))?.timeZone, 'Asia/Yerevan');
    assert.equal(sentTexts(harness).some((text) => text.includes('Mars/Phobos')), false);
    assert.equal(sentTexts(harness).some((text) => text.includes('UTC+3')), false);
    assert.equal(
      harness.logger.lines.some((line) => line.includes('Mars/Phobos') || line.includes('UTC+3')),
      false,
      'free-form timezone input never reaches a log line',
    );
  });

  it('guides an un-onboarded /events sender instead of rendering an event list', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'due', startsAtMs: now + 10 * MS_PER_MINUTE, summary: 'Lesson' })],
      now,
    );
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/events'), deps);

    const jobId = queuedJobId(harness, 1);
    assert.match(jobPayload(harness, jobId), /tz:Europe\/Moscow/);
    await deliver(harness, jobId);
    assert.equal(sentTexts(harness).at(-1), TIMEZONE_PROMPT_TEXT);
    assert.equal(sentTexts(harness).at(-1)?.includes('Lesson'), false);
  });

  it('guides a never-subscribed /events sender to /start and the timezone choice', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/events'), handlerDeps(harness));

    const jobId = queuedJobId(harness, 0);
    await deliver(harness, jobId);
    const reply = sentTexts(harness).at(-1) ?? '';
    assert.equal(reply, EVENTS_GUIDANCE_TEXT);
    assert.match(reply, /\/start/);
    assert.match(reply, /часовой пояс/i);
    assert.equal(
      jobPayload(harness, jobId).includes('tz:'),
      false,
      'timezone buttons cannot work before /start, so none are attached',
    );
    assert.equal(await harness.repository.getUser(USER_ID), null);
  });
});

describe('Timezone settings and rendering', () => {
  it('/settings shows the zone and offers all controls', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/timezone Asia/Yerevan'), deps);
    await handleUpdate(messageUpdate(3, '/settings'), deps);

    const jobId = queuedJobId(harness, 2);
    const payload = jobPayload(harness, jobId);
    assert.match(payload, /Часовой пояс: Asia\/Yerevan/);
    assert.match(payload, /tz:Europe\/Moscow/);
    assert.match(payload, /tz:Asia\/Yerevan/);
    assert.match(payload, /course:basic/);
    assert.match(payload, /reminder:1440/);
    await deliver(harness, jobId);
    assert.match(sentTexts(harness).at(-1) ?? '', /Настройки:/);
    assert.match(sentTexts(harness).at(-1) ?? '', /Часовой пояс: Asia\/Yerevan/);
  });

  it('/events renders the same instant differently per stored zone', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const startsAtMs = now + 10 * MS_PER_MINUTE;
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'a#1', startsAtMs, summary: 'Lesson' })],
      now,
    );
    // The default clock is 2023-11-14T22:13:20Z; the lesson starts ten minutes
    // later. Berlin is on winter time (GMT+1), the other two are fixed offsets.
    const zones: readonly (readonly [number, string, string])[] = [
      [21, 'Europe/Moscow', '2023-11-15 01:23 Europe/Moscow (GMT+3)'],
      [22, 'Asia/Yerevan', '2023-11-15 02:23 Asia/Yerevan (GMT+4)'],
      [23, 'Europe/Berlin', '2023-11-14 23:23 Europe/Berlin (GMT+1)'],
    ];
    for (const [userId, zone] of zones) {
      await harness.repository.activateUser(userId, userId, now);
      onboardUser(harness, userId, zone);
    }

    const deps = handlerDeps(harness);
    for (const [index, [userId, , expected]] of zones.entries()) {
      await handleUpdate(messageUpdate(30 + index, '/events', userId), deps);
      await deliver(harness, queuedJobId(harness, index));
      const text = sentTexts(harness).at(-1) ?? '';
      assert.ok(text.includes(expected), `user ${userId}: expected ${expected} in ${text}`);
      for (const [, otherZone, otherRendering] of zones) {
        if (otherRendering !== expected) {
          assert.equal(
            text.includes(otherRendering),
            false,
            `user ${userId}: must not render the instant as ${otherZone}`,
          );
        }
      }
    }
  });

  it('keeps /events informational for a stopped sender with a stored zone', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const startsAtMs = now + 10 * MS_PER_MINUTE;
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'a#1', startsAtMs, summary: 'Lesson' })],
      now,
    );
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/timezone Europe/Berlin'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.timeZone, 'Europe/Berlin');
    await handleUpdate(messageUpdate(3, '/stop'), deps);
    await handleUpdate(messageUpdate(4, '/events'), deps);

    await deliver(harness, queuedJobId(harness, 3));
    const text = sentTexts(harness).at(-1) ?? '';
    assert.equal((await harness.repository.getUser(USER_ID))?.active, false);
    assert.ok(text.includes('2023-11-14 23:23 Europe/Berlin (GMT+1)'), text);
    assert.match(text, /Lesson/);
  });

  it('renders with the zone live at the reservation after a pace wait', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const jobId = (await seedDueJobs(harness, [USER_ID], { timeZone: 'Europe/Moscow' }))[0] ?? '';
    for (let slot = 0; slot < BOT_SEND_PACE_PER_SECOND; slot += 1) {
      assert.equal(await harness.repository.acquireSendSlot(now, BOT_SEND_PACE_PER_SECOND), true);
    }
    let waits = 0;
    const deps = consumerDeps(harness, {
      sleep: async (ms) => {
        waits += 1;
        // The zone changes while the consumer sleeps out the full pace window.
        await harness.repository.setUserTimeZone(USER_ID, 'Europe/Berlin', harness.clock.now());
        await harness.clock.sleep(ms);
      },
    });

    assert.equal(await processQueueMessage(makeQueueMessage(jobId), deps), 'sent');
    assert.equal(waits, 1, 'the pace wait was taken');
    assert.ok(
      sentTexts(harness).at(-1)?.includes('2023-11-14 23:23 Europe/Berlin (GMT+1)'),
      'the reservation after the wait reads the changed zone',
    );
  });

  it('renders the 429 retry from the retry reservation, not the first attempt', async () => {
    const harness = createHarness();
    const jobId = (await seedDueJobs(harness, [USER_ID], { timeZone: 'Europe/Moscow' }))[0] ?? '';
    let sends = 0;
    harness.setHandler((url) => {
      if (!url.includes('/sendMessage')) {
        return jsonResponse({ ok: true, result: { message_id: 1 } });
      }
      sends += 1;
      if (sends === 1) {
        return jsonResponse({ ok: false, parameters: { retry_after: 1 } }, 429);
      }
      return jsonResponse({ ok: true, result: { message_id: 2 } });
    });
    const deps = consumerDeps(harness, {
      sleep: async (ms) => {
        // The zone changes while the consumer waits out the short 429.
        await harness.repository.setUserTimeZone(USER_ID, 'Europe/Berlin', harness.clock.now());
        await harness.clock.sleep(ms);
      },
    });

    assert.equal(await processQueueMessage(makeQueueMessage(jobId), deps), 'sent');
    const texts = sentTexts(harness);
    assert.equal(texts.length, 2, 'first call plus exactly one in-place retry');
    assert.ok(texts[0]?.includes('Europe/Moscow (GMT+3)'), 'first attempt uses its own reservation');
    assert.ok(texts[1]?.includes('Europe/Berlin (GMT+1)'), 'the retry uses the fresh reservation');
  });

  it('terminally cancels a reminder whose durable zone is malformed', async () => {
    const harness = createHarness();
    const jobId = (await seedDueJobs(harness, [USER_ID], { timeZone: 'Europe/Moscow' }))[0] ?? '';
    // Corruption simulation: a non-null value the schema accepts but Intl
    // rejects (the repository boundary prevents this through application code).
    harness.db.database
      .prepare('UPDATE users SET time_zone = ? WHERE telegram_user_id = ?')
      .run('Mars/Phobos', USER_ID);

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
    assert.equal(outcome, 'terminal');
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.last_error_code, 'invalid-time-zone');
    assert.equal(Number(job?.attempt_count), 0, 'the reserved attempt is refunded');
    assert.equal(
      sentTexts(harness).length,
      0,
      'a malformed zone must never be formatted or sent',
    );
  });

  it('treats a malformed durable zone as un-onboarded for /events without throwing', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'a#1', startsAtMs: now + 10 * MS_PER_MINUTE, summary: 'Lesson' })],
      now,
    );
    await harness.repository.activateUser(USER_ID, USER_ID, now);
    harness.db.database
      .prepare('UPDATE users SET time_zone = ? WHERE telegram_user_id = ?')
      .run('Mars/Phobos', USER_ID);

    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/events'), deps);
    const jobId = queuedJobId(harness, 0);
    await deliver(harness, jobId);
    assert.equal(sentTexts(harness).at(-1), TIMEZONE_PROMPT_TEXT);
    assert.match(jobPayload(harness, jobId), /tz:Europe\/Moscow/, 'the buttons can repair the row');
    assert.equal(sentTexts(harness).some((text) => text.includes('Lesson')), false);
  });

  it('refunds the reservation when the zone disappears during a pace wait', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const jobId = (await seedDueJobs(harness, [USER_ID], { timeZone: 'Europe/Moscow' }))[0] ?? '';
    for (let slot = 0; slot < BOT_SEND_PACE_PER_SECOND; slot += 1) {
      assert.equal(await harness.repository.acquireSendSlot(now, BOT_SEND_PACE_PER_SECOND), true);
    }
    const deps = consumerDeps(harness, {
      sleep: async (ms) => {
        harness.db.database
          .prepare('UPDATE users SET time_zone = NULL WHERE telegram_user_id = ?')
          .run(USER_ID);
        await harness.clock.sleep(ms);
      },
    });

    assert.equal(await processQueueMessage(makeQueueMessage(jobId), deps), 'terminal');
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.last_error_code, 'no-time-zone');
    assert.equal(Number(job?.attempt_count), 0, 'no call started, so no attempt is kept');
    assert.equal(sentTexts(harness).length, 0);
  });

  it('refunds only the retry reservation when the zone is corrupted during the 429 wait', async () => {
    const harness = createHarness();
    const jobId = (await seedDueJobs(harness, [USER_ID], { timeZone: 'Europe/Moscow' }))[0] ?? '';
    let sends = 0;
    harness.setHandler((url) => {
      if (!url.includes('/sendMessage')) {
        return jsonResponse({ ok: true, result: { message_id: 1 } });
      }
      sends += 1;
      return jsonResponse({ ok: false, parameters: { retry_after: 1 } }, 429);
    });
    const deps = consumerDeps(harness, {
      sleep: async (ms) => {
        harness.db.database
          .prepare('UPDATE users SET time_zone = ? WHERE telegram_user_id = ?')
          .run('Mars/Phobos', USER_ID);
        await harness.clock.sleep(ms);
      },
    });

    assert.equal(await processQueueMessage(makeQueueMessage(jobId), deps), 'terminal');
    const job = await harness.repository.getJob(jobId);
    assert.equal(job?.last_error_code, 'invalid-time-zone');
    assert.equal(
      Number(job?.attempt_count),
      1,
      'the real first call stays counted, only the retry is refunded',
    );
    assert.equal(sends, 1, 'no request is made with the malformed zone');
  });

  it('refuses an unusable-zone refund outside the live lease or with a moved counter', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [USER_ID], { timeZone: 'Europe/Moscow' });
    assert.notEqual(await harness.repository.claimJobContext(jobId, 'owner-a', now, JOB_LEASE_MS), null);
    const reservation = await harness.repository.beginSendAttempt(
      jobId,
      'owner-a',
      now,
      MAX_JOB_ATTEMPTS,
    );
    assert.equal(reservation.status, 'reserved');

    assert.equal(
      await harness.repository.finishJobUnusable(jobId, 'owner-b', 1, 'invalid-time-zone', now),
      false,
      'a foreign owner cannot refund',
    );
    assert.equal(
      await harness.repository.finishJobUnusable(jobId, 'owner-a', 2, 'invalid-time-zone', now),
      false,
      'a guard for a different attempt cannot refund',
    );
    assert.equal(Number((await harness.repository.getJob(jobId))?.attempt_count), 1);

    assert.equal(
      await harness.repository.finishJobUnusable(jobId, 'owner-a', 1, 'invalid-time-zone', now),
      true,
    );
    const job = await harness.repository.getJob(jobId);
    assert.equal(Number(job?.attempt_count), 0);
    assert.equal(job?.status, 'cancelled');
    assert.equal(job?.lease_owner, null);

    const [expiredId = ''] = await seedDueJobs(harness, [USER_ID + 1], { timeZone: 'Europe/Moscow' });
    assert.notEqual(await harness.repository.claimJobContext(expiredId, 'owner-a', now, JOB_LEASE_MS), null);
    assert.equal(
      (await harness.repository.beginSendAttempt(expiredId, 'owner-a', now, MAX_JOB_ATTEMPTS)).status,
      'reserved',
    );
    harness.clock.advance(JOB_LEASE_MS + 1);
    assert.equal(
      await harness.repository.finishJobUnusable(
        expiredId,
        'owner-a',
        1,
        'invalid-time-zone',
        harness.clock.now(),
      ),
      false,
      'an expired lease cannot refund',
    );
  });

  it('rejects non-positive or unsafe refund attempts without touching state', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const [jobId = ''] = await seedDueJobs(harness, [USER_ID], { timeZone: 'Europe/Moscow' });
    assert.notEqual(await harness.repository.claimJobContext(jobId, 'owner-a', now, JOB_LEASE_MS), null);
    const before = await harness.repository.getJob(jobId);
    assert.equal(Number(before?.attempt_count), 0, 'a freshly leased job has no attempt to refund');

    for (const invalid of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1]) {
      await assert.rejects(
        harness.repository.finishJobUnusable(jobId, 'owner-a', invalid, 'invalid-time-zone', now),
        RangeError,
        String(invalid),
      );
    }
    assert.deepEqual(
      await harness.repository.getJob(jobId),
      before,
      'no invalid refund attempt mutates the job',
    );
  });

  it('treats a malformed durable zone as incomplete onboarding everywhere', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await harness.repository.activateUser(USER_ID, USER_ID, now);
    harness.db.database
      .prepare('UPDATE users SET time_zone = ? WHERE telegram_user_id = ?')
      .run('Mars/Phobos', USER_ID);
    const deps = handlerDeps(harness);

    await handleUpdate(messageUpdate(1, '/start'), deps);
    const startJobId = queuedJobId(harness, 0);
    await deliver(harness, startJobId);
    const startReply = sentTexts(harness).at(-1) ?? '';
    assert.ok(startReply.startsWith(START_TEXT));
    assert.match(startReply, /часовой пояс/i);
    assert.match(jobPayload(harness, startJobId), /tz:Europe\/Moscow/);

    await handleUpdate(messageUpdate(2, '/settings'), deps);
    const settingsJobId = queuedJobId(harness, 1);
    await deliver(harness, settingsJobId);
    const settingsReply = sentTexts(harness).at(-1) ?? '';
    assert.match(settingsReply, /Часовой пояс: не выбран/);
    assert.match(settingsReply, /часовой пояс/i);
    assert.match(jobPayload(harness, settingsJobId), /tz:Asia\/Yerevan/);

    await handleUpdate(messageUpdate(3, '/timezone'), deps);
    const timezoneJobId = queuedJobId(harness, 2);
    await deliver(harness, timezoneJobId);
    assert.match(sentTexts(harness).at(-1) ?? '', /Часовой пояс не выбран/);
    assert.match(sentTexts(harness).at(-1) ?? '', /часовой пояс/i);
    assert.match(jobPayload(harness, timezoneJobId), /tz:Europe\/Moscow/);

    // The repair controls fix the corrupted row.
    await handleUpdate(callbackUpdate(4, 'tz:Asia/Yerevan'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.timeZone, 'Asia/Yerevan');
  });

  it('reminder delivery formats in the live recipient zone at the same instant', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    const jobId = (await seedDueJobs(harness, [USER_ID], { timeZone: 'Europe/Moscow' }))[0] ?? '';
    const startsAtMs = now + 10 * MS_PER_MINUTE;
    const planned = await harness.repository.getJob(jobId);
    const plannedSendAtMs = Number(planned?.send_at_ms);

    await harness.repository.setUserTimeZone(USER_ID, 'Europe/Berlin', now);

    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
    const statements = harness.repository.statementsUsed();
    assert.equal(outcome, 'sent');
    const text = sentTexts(harness).at(-1) ?? '';
    assert.ok(text.includes('2023-11-14 23:23 Europe/Berlin (GMT+1)'), text);
    assert.equal(text.includes('Europe/Moscow'), false);

    const delivered = await harness.repository.getJob(jobId);
    assert.equal(Number(delivered?.send_at_ms), plannedSendAtMs, 'the absolute instant never moves');
    assert.equal(
      Number(
        (
          harness.db.database
            .prepare('SELECT starts_at_ms FROM occurrences WHERE id = ?')
            .get('basic:a#1') as { starts_at_ms: unknown }
        ).starts_at_ms,
      ),
      startsAtMs,
    );
    assert.equal(
      statements,
      5,
      'timezone-aware reminder rendering costs no extra statements',
    );
  });

  it('cancels a reminder defensively when the recipient has no zone, and replans after onboarding', async () => {
    const harness = createHarness();
    const jobId = (await seedDueJobs(harness, [USER_ID], { timeZone: 'Europe/Moscow' }))[0] ?? '';
    // Stale-data simulation: a reminder exists while the stored zone is gone.
    harness.db.database
      .prepare('UPDATE users SET time_zone = NULL WHERE telegram_user_id = ?')
      .run(USER_ID);

    const outcome = await processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
    assert.equal(outcome, 'terminal');
    assert.equal((await harness.repository.getJob(jobId))?.last_error_code, 'no-time-zone');
    assert.equal(sentTexts(harness).length, 0, 'no Moscow fallback may be sent');

    // After onboarding the cancelled reminder becomes plannable again.
    const at = harness.clock.now();
    await harness.repository.setUserTimeZone(USER_ID, 'Europe/Berlin', at);
    assert.equal(await harness.repository.planDueReminders(at, at + EXPANSION_HORIZON_MS), 1);
    assert.equal(allJobs(harness).filter((job) => job.kind === 'reminder').length, 1);
  });
});

describe('Timezone command ordering and /stop authority', () => {
  it('a suspended older /timezone cannot overwrite the newer choice or queue stale guidance', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/timezone Europe/Berlin'), deps);

    const barrier = createStatementBarrier(harness.db, (sql) =>
      sql.includes('UPDATE users SET time_zone'),
    );
    const olderDeps: HandlerDeps = {
      ...deps,
      repository: new Repository(barrier.db),
    };
    const older = handleUpdate(messageUpdate(10, '/timezone Asia/Yerevan'), olderDeps);
    await barrier.reached;
    await handleUpdate(messageUpdate(11, '/timezone Europe/Berlin'), deps);
    barrier.release();
    await older;

    assert.equal(
      (await harness.repository.getUser(USER_ID))?.timeZone,
      'Europe/Berlin',
      'the newer choice stays authoritative',
    );
    const newerJobs = allJobs(harness).filter((job) => Number(job.source_update_id) >= 10);
    assert.deepEqual(
      newerJobs.map((job) => [Number(job.source_update_id), String(job.status)]),
      [[11, 'pending']],
      'the suspended older command queues no stale guidance',
    );
    assert.equal(
      Number(
        (
          harness.db.database
            .prepare('SELECT last_update_id FROM user_command_state WHERE telegram_user_id = ?')
            .get(USER_ID) as { last_update_id: unknown }
        ).last_update_id,
      ),
      11,
    );
  });

  it('a strictly older /timezone is rejected before any mutation', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(20, '/timezone Asia/Yerevan'), deps);
    const revision = (await harness.repository.getUser(USER_ID))?.revision;

    await handleUpdate(messageUpdate(19, '/timezone Europe/Berlin'), deps);

    assert.equal((await harness.repository.getUser(USER_ID))?.timeZone, 'Asia/Yerevan');
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, revision);
    assert.equal(
      allJobs(harness).filter((job) => job.dedup_key === 'cmd:19').length,
      0,
      'a stale command queues nothing',
    );
  });

  it('/stop stays authoritative over timezone commands until a new /start', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/stop'), deps);
    const stopped = await harness.repository.getUser(USER_ID);
    assert.equal(stopped?.active, false);
    assert.equal(stopped?.timeZone, null);

    await handleUpdate(messageUpdate(3, '/timezone Europe/Berlin'), deps);
    assert.deepEqual(await harness.repository.getUser(USER_ID), stopped, 'no mutation after /stop');
    await deliver(harness, queuedJobId(harness, 2));
    assert.equal(sentTexts(harness).at(-1), INACTIVE_GUIDANCE_TEXT);

    await handleUpdate(callbackUpdate(4, 'tz:Europe/Moscow'), deps);
    assert.deepEqual(await harness.repository.getUser(USER_ID), stopped);
    assert.deepEqual(callbackAnswers(harness), [INACTIVE_GUIDANCE_TEXT]);
    assert.equal(
      allJobs(harness).filter((job) => job.dedup_key === 'cmd:4').length,
      0,
      'a stopped user gets no timezone reply job',
    );

    // Only an explicit /start resumes onboarding; the zone is still not assumed.
    await handleUpdate(messageUpdate(5, '/start'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, true);
    assert.equal((await harness.repository.getUser(USER_ID))?.timeZone, null);
    await handleUpdate(messageUpdate(6, '/timezone Europe/Berlin'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.timeZone, 'Europe/Berlin');
  });
});
