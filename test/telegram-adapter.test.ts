import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { TelegramClient } from '../src/telegram/adapter.ts';
import { createCapturedLogger, createFetchSpy, hangingHandler, jsonResponse, textResponse } from './helpers/fakes.ts';
import { createHarness, TEST_BOT_TOKEN } from './helpers/harness.ts';

describe('telegram adapter', () => {
  it('returns the message id on success and never uses parse_mode', async () => {
    const harness = createHarness();
    const outcome = await harness.telegram.sendMessage(42, 'hello');
    assert.deepEqual(outcome, { ok: true, messageId: 1 });
    const body = JSON.parse(String(harness.fetchSpy.calls[0]?.init?.body)) as Record<string, unknown>;
    assert.equal(body.parse_mode, undefined);
    assert.equal(body.allow_paid_broadcast, false);
    assert.equal(body.chat_id, 42);
  });

  it('classifies 429 with retry_after', async () => {
    const harness = createHarness();
    harness.setHandler(() => jsonResponse({ ok: false, parameters: { retry_after: 7 } }, 429));
    const outcome = await harness.telegram.sendMessage(1, 'x');
    assert.deepEqual(outcome, { ok: false, kind: 'rate-limit', retryAfterMs: 7_000 });
  });

  it('classifies 403 as forbidden', async () => {
    const harness = createHarness();
    harness.setHandler(() => textResponse('nope', 403));
    assert.deepEqual(await harness.telegram.sendMessage(1, 'x'), { ok: false, kind: 'forbidden' });
  });

  it('classifies 5xx and other 4xx outcomes', async () => {
    const harness = createHarness();
    harness.setHandler(() => textResponse('boom', 503));
    assert.deepEqual(await harness.telegram.sendMessage(1, 'x'), {
      ok: false,
      kind: 'transient',
      code: 'server',
    });
    harness.setHandler(() => textResponse('bad', 400));
    assert.deepEqual(await harness.telegram.sendMessage(1, 'x'), {
      ok: false,
      kind: 'permanent',
      status: 400,
    });
  });

  it('classifies a malformed 200 body as transient', async () => {
    const harness = createHarness();
    harness.setHandler(() => jsonResponse({ ok: true, result: {} }));
    assert.deepEqual(await harness.telegram.sendMessage(1, 'x'), {
      ok: false,
      kind: 'transient',
      code: 'malformed',
    });
  });

  it('aborts on timeout and reports it without leaking the token', async () => {
    const captured = createCapturedLogger();
    const spy = createFetchSpy(hangingHandler);
    const client = new TelegramClient({
      botToken: TEST_BOT_TOKEN,
      fetch: spy.fetch,
      logger: captured.logger,
      timeoutMs: 5,
    });
    const outcome = await client.sendMessage(1, 'x');
    assert.deepEqual(outcome, { ok: false, kind: 'transient', code: 'timeout' });
    for (const line of captured.lines) {
      assert.equal(line.includes(TEST_BOT_TOKEN), false);
    }
  });

  it('clamps overlong text on code-point boundaries', async () => {
    const harness = createHarness();
    await harness.telegram.sendMessage(1, 'a'.repeat(6000));
    const body = JSON.parse(String(harness.fetchSpy.calls[0]?.init?.body)) as { text: string };
    assert.equal(body.text.length <= 4096, true);
  });
});
