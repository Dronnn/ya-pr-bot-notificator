/**
 * Finding 12: webhook body reads are bounded, deadline-aware and always cancel
 * the reader on overflow, timeout and error. Secret validation stays before any
 * body read, and no request contents ever reach logs or responses.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { WEBHOOK_BODY_READ_TIMEOUT_MS } from '../src/util.ts';
import { buildTestApp, createHarness, TEST_WEBHOOK_SECRET } from './helpers/harness.ts';
import { stalledRequestBody } from './helpers/streams.ts';

const WEBHOOK_URL = 'https://bot.example.test/telegram/webhook';

function streamRequest(
  body: ReadableStream<Uint8Array>,
  secret: string = TEST_WEBHOOK_SECRET,
): Request {
  return new Request(WEBHOOK_URL, {
    method: 'POST',
    headers: { 'x-telegram-bot-api-secret-token': secret },
    body,
    duplex: 'half',
  } as RequestInit & { duplex: 'half' });
}

function sixteenKibChunk(): Uint8Array {
  return new Uint8Array(16 * 1024);
}

describe('webhook bounded body reads', () => {
  it('returns 413 and cancels the stream when the byte cap is exceeded', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    let cancelled = false;
    let pulls = 0;
    const chunk = sixteenKibChunk();
    const request = streamRequest(
      stalledRequestBody({
        chunks: [chunk, chunk, chunk, chunk, chunk],
        onCancel: () => {
          cancelled = true;
        },
        onPull: () => {
          pulls += 1;
        },
      }),
    );

    const response = await app.fetch(request);

    assert.equal(response.status, 413);
    assert.deepEqual(await response.json(), { status: 'too_large' });
    assert.equal(cancelled, true);
    assert.ok(pulls >= 5);
    assert.equal(harness.fetchSpy.calls.length, 0);
    assert.equal(harness.logger.lines.length, 0);
  });

  it('terminates a stalled stream with 408 and cancels the stream', async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const harness = createHarness();
    const app = buildTestApp(harness);
    let cancelled = false;
    const request = streamRequest(
      stalledRequestBody({
        onCancel: () => {
          cancelled = true;
        },
      }),
    );

    const pending = app.fetch(request);
    t.mock.timers.tick(WEBHOOK_BODY_READ_TIMEOUT_MS);
    const response = await pending;

    assert.equal(response.status, 408);
    assert.deepEqual(await response.json(), { status: 'timeout' });
    assert.equal(cancelled, true);
    assert.equal(harness.fetchSpy.calls.length, 0);
  });

  it('answers a bounded 400 on a reader failure without logging request contents', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    const marker = 'private-payload-marker';
    const request = streamRequest(
      stalledRequestBody({
        failWith: new Error(`reader failed on ${marker}`),
      }),
    );

    const response = await app.fetch(request);
    const bodyText = await response.text();

    assert.equal(response.status, 400);
    assert.deepEqual(JSON.parse(bodyText), { status: 'bad_request' });
    assert.ok(!bodyText.includes(marker));
    // The reader was acquired and torn down (an errored stream is already
    // cancelled by the platform, so only the disturbance is observable).
    assert.equal(request.bodyUsed, true);
    const logs = harness.logger.lines.join('\n');
    assert.ok(logs.includes('webhook_body_read_failed'));
    assert.ok(!logs.includes(marker));
  });

  it('rejects a wrong secret before reading a single body byte', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    let cancelled = false;
    const request = streamRequest(
      stalledRequestBody({
        onCancel: () => {
          cancelled = true;
        },
      }),
      'wrong-secret',
    );

    const response = await app.fetch(request);

    assert.equal(response.status, 401);
    // Disturbing the body is what `getReader()` does: bodyUsed proves no read
    // was ever attempted.
    assert.equal(request.bodyUsed, false);
    assert.equal(cancelled, false);
    assert.equal(harness.logger.lines.length, 0);
  });
});
