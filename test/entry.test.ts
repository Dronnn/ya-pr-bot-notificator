/**
 * Exercises the real Workers entry point (src/index.ts) with synthetic
 * bindings and a stubbed global fetch. No network, no Cloudflare account.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import worker from '../src/index.ts';
import type { OutboundJobMessage } from '../src/platform.ts';
import { jsonResponse } from './helpers/fakes.ts';
import {
  createHarness,
  TEST_BOT_TOKEN,
  TEST_WEBHOOK_SECRET,
  webhookRequest,
} from './helpers/harness.ts';
import { makeQueueMessage } from './helpers/seed.ts';

/** Config-only environment: the bindings are deliberately absent. */
function configOnlyEnv(): Env {
  return {
    TELEGRAM_BOT_TOKEN: TEST_BOT_TOKEN,
    TELEGRAM_WEBHOOK_SECRET: TEST_WEBHOOK_SECRET,
    YANDEX_BASIC_ICAL_URL: 'https://example.test/basic.ics',
    YANDEX_EXTENDED_ICAL_URL: 'https://example.test/extended.ics',
  } as unknown as Env;
}

/** The runtime `MessageBatch` surface is only checked in production wiring. */
function queueBatch(messages: readonly unknown[]): MessageBatch<OutboundJobMessage> {
  return { queue: 'notifications', messages } as unknown as MessageBatch<OutboundJobMessage>;
}

/** Minimal execution context; `waitUntil` must not throw on a dropped promise. */
function executionContext(): ExecutionContext {
  return {
    waitUntil(): void {},
    passThroughOnException(): void {},
  } as unknown as ExecutionContext;
}

/** Entry fetch with the runtime execution context attached. */
function entryFetch(request: Request, env: Env): Promise<Response> {
  return worker.fetch(request, env, executionContext());
}

/** Config plus both bindings, with one replaced by a throwing accessor. */
function envWithThrowingBinding(name: 'DB' | 'NOTIFICATIONS'): Env {
  const harness = createHarness();
  const env: Record<string, unknown> = {
    ...configOnlyEnv(),
    DB: harness.db,
    NOTIFICATIONS: harness.queue.producer,
  };
  Object.defineProperty(env, name, {
    get(): never {
      throw new Error(`binding accessor failed: ${name}`);
    },
    configurable: true,
  });
  return env as unknown as Env;
}

function trackedMessage(): {
  message: { body: { jobId: string }; ack(): void; retry(options?: { delaySeconds?: number }): void };
  state: { acked: boolean; retryOptions: { delaySeconds?: number } | undefined };
} {
  const state: { acked: boolean; retryOptions: { delaySeconds?: number } | undefined } = {
    acked: false,
    retryOptions: undefined,
  };
  return {
    message: {
      body: { jobId: 'unprocessed-job' },
      ack(): void {
        state.acked = true;
      },
      retry(options?: { delaySeconds?: number }): void {
        state.retryOptions = options;
      },
    },
    state,
  };
}

describe('worker entry point', () => {
  it('reports unavailable without configuration and does not leak values', async () => {
    const health = await entryFetch(new Request('https://bot.test/health'), {} as Env);
    assert.equal(health.status, 503);
    const body = (await health.json()) as { missing: string[] };
    assert.ok(body.missing.length > 0);

    const webhookResponse = await entryFetch(
      new Request('https://bot.test/telegram/webhook', { method: 'POST', body: '{}' }),
      {} as Env,
    );
    assert.equal(webhookResponse.status, 503);
    await worker.scheduled({} as ScheduledController, {} as Env);
  });

  it('wires config, webhook, scheduler and queue together', async () => {
    const harness = createHarness();
    const originalFetch = globalThis.fetch;
    globalThis.fetch = harness.fetchSpy.fetch;
    try {
      harness.setHandler(() => jsonResponse({ ok: true, result: { message_id: 1 } }));
      const env = {
        DB: harness.db,
        NOTIFICATIONS: harness.queue.producer,
        TELEGRAM_BOT_TOKEN: TEST_BOT_TOKEN,
        TELEGRAM_WEBHOOK_SECRET: TEST_WEBHOOK_SECRET,
        YANDEX_BASIC_ICAL_URL: 'https://example.test/basic.ics',
        YANDEX_EXTENDED_ICAL_URL: 'https://example.test/extended.ics',
      } as unknown as Env;

      const health = await entryFetch(new Request('https://bot.test/health'), env);
      assert.equal(health.status, 200);

      const start = await entryFetch(
        webhookRequest({
          update_id: 1,
          message: {
            message_id: 1,
            chat: { id: 111, type: 'private' },
            from: { id: 111, username: 'andrew' },
            text: '/start',
          },
        }),
        env,
      );
      assert.equal(start.status, 200);
      assert.equal((await harness.repository.getUser(111))?.active, true);

      await worker.scheduled({} as ScheduledController, env);
      const jobIds = harness.queue.batches.flat().map((message) => message.jobId);
      assert.ok(jobIds.length >= 1, 'scheduler should enqueue the pending reply');
      const jobId = jobIds[jobIds.length - 1] ?? '';
      const batch = [makeQueueMessage(jobId)];

      await worker.queue(
        queueBatch(batch),
        env,
      );

      assert.equal(batch[0]?.acked, true);
      assert.equal((await harness.repository.getJob(jobId))?.status, 'sent');
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it('reports unavailable when bindings are absent while configuration is complete', async () => {
    const env = configOnlyEnv();
    const health = await entryFetch(new Request('https://bot.test/health'), env);
    assert.equal(health.status, 503);
    assert.deepEqual(await health.json(), {
      status: 'unavailable',
      missing: ['DB', 'NOTIFICATIONS'],
    });

    await worker.scheduled({} as ScheduledController, env);

    const webhook = await entryFetch(
      new Request('https://bot.test/telegram/webhook', { method: 'POST', body: '{}' }),
      env,
    );
    assert.equal(webhook.status, 503);
  });

  it('reports unavailable when a binding is malformed', async () => {
    const env = {
      ...configOnlyEnv(),
      DB: { prepare: () => undefined, batch: 42 },
      NOTIFICATIONS: {},
    } as unknown as Env;
    const health = await entryFetch(new Request('https://bot.test/health'), env);
    assert.equal(health.status, 503);
    assert.deepEqual(await health.json(), {
      status: 'unavailable',
      missing: ['DB', 'NOTIFICATIONS'],
    });
  });

  it('degrades safely when the DB binding accessor throws', async () => {
    const env = envWithThrowingBinding('DB');
    const health = await entryFetch(new Request('https://bot.test/health'), env);
    assert.equal(health.status, 503);
    assert.deepEqual(await health.json(), { status: 'unavailable', missing: ['DB'] });

    await worker.scheduled({} as ScheduledController, env);

    const { message, state } = trackedMessage();
    await worker.queue(queueBatch([message]), env);
    assert.equal(state.acked, false);
    assert.deepEqual(state.retryOptions, { delaySeconds: 60 });
  });

  it('degrades safely when the NOTIFICATIONS binding accessor throws', async () => {
    const env = envWithThrowingBinding('NOTIFICATIONS');
    const health = await entryFetch(new Request('https://bot.test/health'), env);
    assert.equal(health.status, 503);
    assert.deepEqual(await health.json(), { status: 'unavailable', missing: ['NOTIFICATIONS'] });

    await worker.scheduled({} as ScheduledController, env);

    const { message, state } = trackedMessage();
    await worker.queue(queueBatch([message]), env);
    assert.equal(state.acked, false);
    assert.deepEqual(state.retryOptions, { delaySeconds: 60 });
  });

  it('retries every message and never acks while the queue runtime is unavailable', async () => {
    const env = configOnlyEnv();
    let acked = false;
    let retryOptions: { delaySeconds?: number } | undefined;
    const message = {
      body: { jobId: 'unprocessed-job' },
      ack(): void {
        acked = true;
      },
      retry(options?: { delaySeconds?: number }): void {
        retryOptions = options;
      },
    };

    await worker.queue(queueBatch([message]), env);

    assert.equal(acked, false);
    assert.deepEqual(retryOptions, { delaySeconds: 60 });
  });

  it('acks a malformed poison message in ready mode without touching the job', async () => {
    const harness = createHarness();
    const env = {
      DB: harness.db,
      NOTIFICATIONS: harness.queue.producer,
      TELEGRAM_BOT_TOKEN: TEST_BOT_TOKEN,
      TELEGRAM_WEBHOOK_SECRET: TEST_WEBHOOK_SECRET,
      YANDEX_BASIC_ICAL_URL: 'https://example.test/basic.ics',
      YANDEX_EXTENDED_ICAL_URL: 'https://example.test/extended.ics',
    } as unknown as Env;
    let acked = false;
    let retried = false;
    const message = {
      body: { bogus: true },
      ack(): void {
        acked = true;
      },
      retry(): void {
        retried = true;
      },
    };

    await worker.queue(queueBatch([message]), env);

    assert.equal(acked, true);
    assert.equal(retried, false);
    assert.equal(harness.fetchSpy.calls.length, 0);
  });
});
