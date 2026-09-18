import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { createAdminNotifier, type AdminAlertDeps } from '../src/worker/admin-alerts.ts';
import { jsonResponse, textResponse } from './helpers/fakes.ts';
import { buildTestApp, createHarness, TEST_CALENDAR_REFRESH_SECRET } from './helpers/harness.ts';
import { makeQueueMessage } from './helpers/seed.ts';
import type { Harness } from './helpers/harness.ts';

interface SentAlert {
  chatId: number;
  text: string;
}

/** Records Telegram sends as alerts and answers with a successful send. */
function recordAlerts(harness: Harness, sent: SentAlert[]): void {
  harness.setHandler((_url, init) => {
    const body = JSON.parse(String(init?.body)) as { chat_id: number; text: string };
    sent.push({ chatId: body.chat_id, text: body.text });
    return jsonResponse({ ok: true, result: { message_id: 1 } });
  });
}

function buildNotifier(harness: Harness, overrides: Partial<AdminAlertDeps> = {}) {
  return createAdminNotifier({
    telegram: harness.telegram,
    repository: harness.repository,
    now: harness.now,
    logger: harness.logger.logger,
    adminUserId: 42,
    redact: (text) => text,
    cooldownMs: 60_000,
    ...overrides,
  });
}

describe('admin alerts', () => {
  it('sends one alert per event per cooldown and redacts the error text', async () => {
    const harness = createHarness();
    const sent: SentAlert[] = [];
    recordAlerts(harness, sent);
    const notify = buildNotifier(harness, {
      redact: (text) => text.replaceAll('https://private.test/feed.ics', '[url]'),
    });

    await notify('scheduled_tick_failed', new Error('boom https://private.test/feed.ics'));
    await notify('scheduled_tick_failed', new Error('boom again'));
    await notify('queue_batch_failed', new Error('other'));

    assert.equal(sent.length, 2, 'the same event is suppressed during the cooldown');
    assert.equal(sent[0]?.chatId, 42);
    assert.match(sent[0]?.text ?? '', /cron-тик упал/);
    assert.match(sent[0]?.text ?? '', /\[url\]/);
    assert.equal(sent[0]?.text.includes('private.test'), false);
    assert.match(sent[1]?.text ?? '', /обработка очереди упала/);

    await harness.clock.sleep(60_001);
    await notify('scheduled_tick_failed', new Error('later'));
    assert.equal(sent.length, 3, 'the cooldown expiry releases the next alert');
  });

  it('swallows a failed alert delivery and logs it', async () => {
    const harness = createHarness();
    harness.setHandler(() => textResponse('nope', 500));
    const notify = buildNotifier(harness);

    await notify('scheduled_tick_failed', new Error('boom'));

    assert.ok(
      harness.logger.lines.some((line) => line.includes('admin_alert_send_failed')),
      'the delivery failure is logged, the original failure is not replaced',
    );
  });

  it('alerts and rethrows when a scheduled tick fails', async () => {
    const harness = createHarness();
    const events: string[] = [];
    const app = buildTestApp(harness, [], TEST_CALENDAR_REFRESH_SECRET, async (event) => {
      events.push(event);
    });
    harness.repository.repairExpiredLeases = async () => {
      throw new Error('tick exploded');
    };

    await assert.rejects(app.scheduled(), /tick exploded/);
    assert.deepEqual(events, ['scheduled_tick_failed']);
  });

  it('alerts and rethrows when queue processing fails', async () => {
    const harness = createHarness();
    const events: string[] = [];
    const app = buildTestApp(harness, [], TEST_CALENDAR_REFRESH_SECRET, async (event) => {
      events.push(event);
    });
    harness.repository.pruneRateStarts = async () => {
      throw new Error('queue exploded');
    };

    await assert.rejects(
      app.queue({ queue: 'test', messages: [makeQueueMessage('missing-job')] }),
      /queue exploded/,
    );
    assert.deepEqual(events, ['queue_batch_failed']);
  });
});
