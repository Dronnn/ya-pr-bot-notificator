import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { createApp } from '../src/worker/app.ts';
import { MS_PER_MINUTE } from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import {
  buildTestApp,
  createHarness,
  TEST_WEBHOOK_SECRET,
  webhookRequest,
  type Harness,
} from './helpers/harness.ts';
import { occurrence, onboardUser, seedSource } from './helpers/seed.ts';

function messageUpdate(updateId: number, text: string, chatType = 'private'): unknown {
  return {
    update_id: updateId,
    message: {
      message_id: updateId,
      chat: { id: 111, type: chatType },
      from: { id: 111, username: 'andrew' },
      text,
    },
  };
}

function callbackUpdate(updateId: number, data: string): unknown {
  return {
    update_id: updateId,
    callback_query: {
      id: `callback-${updateId}`,
      from: { id: 111, username: 'andrew' },
      message: { message_id: 5, chat: { id: 111, type: 'private' } },
      data,
    },
  };
}

function allJobs(harness: Harness): Record<string, unknown>[] {
  return harness.db.database
    .prepare('SELECT * FROM outbound_jobs ORDER BY created_at_ms ASC, id ASC')
    .all() as Record<string, unknown>[];
}

function updateStatus(harness: Harness, updateId: number): string | null {
  const row = harness.db.database
    .prepare('SELECT status FROM processed_updates WHERE update_id = ?')
    .get(updateId) as { status: string } | undefined;
  return row?.status ?? null;
}

function updateLeaseOwner(harness: Harness, updateId: number): string | null {
  const row = harness.db.database
    .prepare('SELECT lease_owner FROM processed_updates WHERE update_id = ?')
    .get(updateId) as { lease_owner: string | null } | undefined;
  return row?.lease_owner ?? null;
}

describe('webhook endpoint', () => {
  it('reports unavailable without config and leaks no values', async () => {
    const app = createApp({
      config: { ok: false, missing: ['TELEGRAM_BOT_TOKEN'] },
      deps: null,
    });
    const health = await app.fetch(new Request('https://bot.test/health'));
    assert.equal(health.status, 503);
    const body = (await health.json()) as { missing: string[] };
    assert.deepEqual(body.missing, ['TELEGRAM_BOT_TOKEN']);
  });

  it('rejects a wrong secret before touching the database', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const response = await app.fetch(
      webhookRequest(messageUpdate(1, '/start'), 'wrong-secret'),
    );
    assert.equal(response.status, 401);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM processed_updates'), 0);
  });

  it('handles /start, stores the user and enqueues a paced reply', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const response = await app.fetch(webhookRequest(messageUpdate(1, '/start')));
    assert.equal(response.status, 200);
    const user = await harness.repository.getUser(111);
    assert.equal(user?.active, true);
    assert.equal(harness.queue.batches.flat().length, 1);
    assert.equal(allJobs(harness).length, 1);
  });

  it('ignores group chats', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const response = await app.fetch(webhookRequest(messageUpdate(2, '/start', 'group')));
    assert.equal(response.status, 200);
    assert.equal((await harness.repository.getUser(111)), null);
  });

  it('rejects malformed JSON', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const request = new Request('https://bot.test/telegram/webhook', {
      method: 'POST',
      headers: { 'x-telegram-bot-api-secret-token': TEST_WEBHOOK_SECRET },
      body: '{not json',
    });
    const response = await app.fetch(request);
    assert.equal(response.status, 400);
  });

  it('marks a completed duplicate as duplicate and repeats no effects', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const update = messageUpdate(3, '/start');
    await app.fetch(webhookRequest(update));
    const second = await app.fetch(webhookRequest(update));
    assert.equal(second.status, 200);
    assert.deepEqual(await second.json(), { status: 'duplicate' });
    assert.equal(allJobs(harness).length, 1);
    assert.equal(updateStatus(harness, 3), 'done');
  });

  it('returns a retryable 503 while another owner holds a live update lease', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const started = await harness.repository.tryBeginUpdate(
      20,
      'other-owner',
      harness.clock.now(),
      60_000,
    );
    assert.equal(started, 'acquired');

    const response = await app.fetch(webhookRequest(messageUpdate(20, '/start')));
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), { status: 'busy' });
    assert.equal(await harness.repository.getUser(111), null);
    assert.equal(allJobs(harness).length, 0);
    assert.equal(updateLeaseOwner(harness, 20), 'other-owner');
  });

  it('recovers an update whose processing lease expired', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const started = await harness.repository.tryBeginUpdate(
      4,
      'stale-owner',
      harness.clock.now() - 60_000,
      1,
    );
    assert.equal(started, 'acquired');

    const response = await app.fetch(webhookRequest(messageUpdate(4, '/start')));
    assert.equal(response.status, 200);
    assert.equal((await harness.repository.getUser(111))?.active, true);
    assert.equal(updateStatus(harness, 4), 'done');
    assert.equal(allJobs(harness).length, 1);
  });

  it('releases ownership on a processing failure so a later delivery succeeds', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const originalSendBatch = harness.queue.producer.sendBatch;
    harness.queue.producer.sendBatch = async (): Promise<never> => {
      throw new Error('queue unavailable');
    };

    const failed = await app.fetch(webhookRequest(messageUpdate(21, '/start')));
    assert.equal(failed.status, 500);
    assert.equal(updateStatus(harness, 21), 'processing');
    assert.equal(updateLeaseOwner(harness, 21), null);

    harness.queue.producer.sendBatch = originalSendBatch;
    const retried = await app.fetch(webhookRequest(messageUpdate(21, '/start')));
    assert.equal(retried.status, 200);
    assert.equal((await harness.repository.getUser(111))?.active, true);
    assert.equal(updateStatus(harness, 21), 'done');
  });

  it('applies a course callback and acknowledges it', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    await app.fetch(webhookRequest(messageUpdate(5, '/start')));
    await app.fetch(webhookRequest(callbackUpdate(6, 'course:basic')));

    const ackCalls = harness.fetchSpy.calls.filter((call) =>
      call.url.includes('/answerCallbackQuery'),
    );
    assert.equal(ackCalls.length, 1);
    const jobs = allJobs(harness);
    const reply = jobs.find((job) => job.status === 'pending');
    assert.ok(reply !== undefined);
    assert.match(String(reply?.payload_json), /reminder:30/);
  });

  it('acknowledges an unknown callback without creating a job', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    await app.fetch(webhookRequest(callbackUpdate(7, 'hack:everything')));
    assert.equal(allJobs(harness).length, 0);
    const ackCalls = harness.fetchSpy.calls.filter((call) =>
      call.url.includes('/answerCallbackQuery'),
    );
    assert.equal(ackCalls.length, 1);
  });

  it('cancels pending work on /stop', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    await app.fetch(webhookRequest(messageUpdate(8, '/start')));
    await app.fetch(webhookRequest(messageUpdate(9, '/stop')));

    assert.equal((await harness.repository.getUser(111))?.active, false);
    const statuses = allJobs(harness).map((job) => String(job.status));
    assert.ok(statuses.includes('cancelled'));
    assert.ok(statuses.includes('pending'));
  });

  it('answers unknown commands with help', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    await app.fetch(webhookRequest(messageUpdate(10, '/nonsense')));
    const jobs = allJobs(harness);
    assert.equal(jobs.length, 1);
    assert.match(String(jobs[0]?.payload_json), /Доступные команды/);
  });

  it('accepts a /start command addressed to the bot username', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const response = await app.fetch(webhookRequest(messageUpdate(11, '/start@my_notifier_bot')));
    assert.equal(response.status, 200);
    assert.equal((await harness.repository.getUser(111))?.active, true);
  });

  it('answers /settings with the current settings and course keyboard', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    await app.fetch(webhookRequest(messageUpdate(12, '/start')));
    await app.fetch(webhookRequest(messageUpdate(13, '/settings')));

    const jobs = allJobs(harness);
    const settingsJob = jobs[jobs.length - 1];
    const payload = String(settingsJob?.payload_json);
    assert.match(payload, /Настройки:/);
    assert.match(payload, /course:basic/);
    assert.match(payload, /course:extended/);
  });

  it('answers /events with explicit times in the stored timezone', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'a#1', startsAtMs: now + 10 * MS_PER_MINUTE, summary: 'Lesson 1' })],
      now,
    );
    await harness.repository.activateUser(111, 111, now);
    onboardUser(harness, 111, 'Europe/Moscow');
    const app = buildTestApp(harness);
    await app.fetch(webhookRequest(messageUpdate(14, '/events')));

    const payload = String(allJobs(harness)[0]?.payload_json);
    assert.match(payload, /Lesson 1/);
    assert.match(payload, /Europe\/Moscow \(GMT\+3\)/);
  });

  it('applies a reminder callback and stores the offset', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    await app.fetch(webhookRequest(messageUpdate(15, '/start')));
    await app.fetch(webhookRequest(callbackUpdate(16, 'reminder:1440')));

    const user = await harness.repository.getUser(111);
    assert.equal(user?.reminderOffsetMinutes, 1440);
    const jobs = allJobs(harness);
    assert.match(String(jobs[jobs.length - 1]?.payload_json), /за сутки/);
  });

  it('rejects an oversized request body', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const request = new Request('https://bot.test/telegram/webhook', {
      method: 'POST',
      headers: { 'x-telegram-bot-api-secret-token': TEST_WEBHOOK_SECRET },
      body: JSON.stringify({ update_id: 17, padding: 'x'.repeat(70_000) }),
    });
    const response = await app.fetch(request);
    assert.equal(response.status, 413);
    assert.equal(allJobs(harness).length, 0);
  });

  it('rejects an update without a valid update id and ignores non-text messages', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const missingId = new Request('https://bot.test/telegram/webhook', {
      method: 'POST',
      headers: { 'x-telegram-bot-api-secret-token': TEST_WEBHOOK_SECRET },
      body: JSON.stringify({ message: { chat: { id: 111, type: 'private' }, from: { id: 111 } } }),
    });
    assert.equal((await app.fetch(missingId)).status, 400);

    const sticker = messageUpdate(18, '/start');
    (sticker as { message: { text?: unknown } }).message.text = undefined;
    assert.equal((await app.fetch(webhookRequest(sticker))).status, 200);
    assert.equal(allJobs(harness).length, 0);
    assert.equal(updateStatus(harness, 18), 'done');
  });

  it('reports unavailable runtime bindings by name and defaults to runtime', async () => {
    const config = {
      ok: true as const,
      config: {
        telegramBotToken: 't',
        telegramWebhookSecret: 's',
        basicIcalUrl: 'https://example.test/basic.ics',
        extendedIcalUrl: 'https://example.test/extended.ics',
      },
    };
    const named = createApp({ config, deps: null, missingRuntime: ['DB', 'NOTIFICATIONS'] });
    const namedHealth = await named.fetch(new Request('https://bot.test/health'));
    assert.equal(namedHealth.status, 503);
    assert.deepEqual(await namedHealth.json(), {
      status: 'unavailable',
      missing: ['DB', 'NOTIFICATIONS'],
    });

    const unnamed = createApp({ config, deps: null });
    const unnamedHealth = await unnamed.fetch(new Request('https://bot.test/health'));
    assert.equal(unnamedHealth.status, 503);
    assert.deepEqual(await unnamedHealth.json(), { status: 'unavailable', missing: ['runtime'] });
  });
});
