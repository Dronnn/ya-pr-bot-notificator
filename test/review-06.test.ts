/**
 * Regression tests for the two defects confirmed by follow-up review 06:
 * lease expiry evaluated against a timestamp captured before asynchronous
 * staging, and Telegram deadlines that ended at the response headers instead of
 * covering the body.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { runSourceSync } from '../src/calendar/sync.ts';
import { TelegramClient, type SendOutcome } from '../src/telegram/adapter.ts';
import { processQueueBatch } from '../src/queue/consumer.ts';
import { CalendarIntegrityError } from '../src/domain/calendar.ts';
import type { CalendarParser } from '../src/calendar/parser.ts';
import {
  JOB_BACKOFF_BASE_MS,
  MAX_D1_STATEMENTS_PER_CONSUMER,
  MS_PER_HOUR,
  MS_PER_MINUTE,
} from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { jsonResponse, textResponse } from './helpers/fakes.ts';
import {
  consumerDeps,
  createHarness,
  syncDeps,
  TEST_BOT_TOKEN,
  type Harness,
} from './helpers/harness.ts';
import {
  EMPTY_ICS,
  expireSourceLease,
  makeQueueMessage,
  occurrence,
  parsedEvent,
  seedDueJobs,
  seedSource,
  TEST_SOURCE,
} from './helpers/seed.ts';
import { gatedRepository, stalledResponse } from './helpers/streams.ts';

const MAX_BOUND_PARAMS = 100;

interface SourceSnapshot {
  status: string;
  etag: string | null;
  fetchedAtMs: number | null;
  lastRefreshAtMs: number | null;
}

async function sourceSnapshot(harness: Harness): Promise<SourceSnapshot> {
  const source = await harness.repository.getSource('basic');
  return {
    status: source?.status ?? 'missing',
    etag: source?.etag ?? null,
    fetchedAtMs: source?.fetchedAtMs ?? null,
    lastRefreshAtMs: source?.lastRefreshAtMs ?? null,
  };
}

/** A fetch that advances the clock past the source lease, then answers. */
function expiringFetch(harness: Harness, response: () => Response): typeof fetch {
  return async () => {
    expireSourceLease(harness);
    return response();
  };
}

describe('lease expiry at the final custody check', () => {
  it('refuses to publish when the lease expires immediately before publication', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);
    // A snapshot with a revision the lapsed attempt must not touch.
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'visible', startsAtMs: start + 2 * MS_PER_HOUR, summary: 'Keep' })],
      start,
    );
    const before = await sourceSnapshot(harness);

    const slowRepository = gatedRepository(harness.db, 1, () => {
      expireSourceLease(harness);
    });
    harness.setParser({
      events: [
        parsedEvent(start + 2 * MS_PER_HOUR, { uid: 'visible', summary: 'Replace' }),
        parsedEvent(start + MS_PER_HOUR, { uid: 'late-stage', summary: 'Late' }),
      ],
    });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'late' }));

    const result = await runSourceSync(
      syncDeps(harness, { repository: slowRepository }),
      TEST_SOURCE,
      'late-owner',
    );

    assert.equal(result.status, 'skipped', 'an attempt whose lease lapsed cannot publish');
    assert.equal(result.reason, 'lost-ownership');
    const visible = harness.db.database
      .prepare('SELECT summary, revision FROM occurrences ORDER BY id')
      .all() as { summary: unknown; revision: unknown }[];
    assert.deepEqual(
      visible.map((row) => ({ summary: String(row.summary), revision: Number(row.revision) })),
      [{ summary: 'Keep', revision: 1 }],
      'the visible snapshot and its revisions survive the lapsed attempt',
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrences WHERE summary = 'Late'"),
      0,
    );
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrence_staging'),
      0,
      'the lapsed attempt cleans up its own staging rows',
    );
    assert.deepEqual(await sourceSnapshot(harness), before, 'the source is not relabelled');
  });

  it('refuses to publish when the lease expires during a later staging batch', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);
    const before = await sourceSnapshot(harness);

    // 360 writes stage in two D1 batches; the clock moves past the lease during
    // the second one, after the first batch was already committed to staging.
    const slowRepository = gatedRepository(harness.db, 2, () => {
      expireSourceLease(harness);
    });
    harness.setParser({
      events: Array.from({ length: 360 }, (_, index) =>
        parsedEvent(start + MS_PER_MINUTE + index, { uid: `bulk-${index}` }),
      ),
    });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'late' }));

    const result = await runSourceSync(
      syncDeps(harness, { repository: slowRepository }),
      TEST_SOURCE,
      'late-owner',
    );

    assert.equal(result.status, 'skipped');
    assert.equal(result.reason, 'lost-ownership');
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 0);
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrence_staging'),
      0,
      'a partially staged snapshot is never published and is swept',
    );
    assert.deepEqual(await sourceSnapshot(harness), before);
  });

  it('refuses stale 304, redirect, truncation and parser metadata writes', async () => {
    const cases: { name: string; response: () => Response }[] = [
      { name: '304', response: () => new Response(null, { status: 304 }) },
      { name: 'redirect', response: () => new Response(null, { status: 302 }) },
      { name: 'truncated', response: () => textResponse('BEGIN:VCALENDAR', 200) },
      { name: 'http-error', response: () => textResponse('unavailable', 503) },
    ];
    for (const testCase of cases) {
      const harness = createHarness();
      const start = harness.clock.now();
      await seedSource(harness.repository, 'basic', start);
      // A recent refresh keeps the 304 on the freshness-only path.
      harness.db.exec(`UPDATE sources SET last_refresh_at_ms = ${start}`);
      const before = await sourceSnapshot(harness);

      const result = await runSourceSync(
        syncDeps(harness, { fetch: expiringFetch(harness, testCase.response) }),
        TEST_SOURCE,
        'late-owner',
      );

      assert.equal(result.status, 'skipped', `${testCase.name}: must not report a stale outcome`);
      assert.equal(result.reason, 'lost-ownership', testCase.name);
      assert.deepEqual(
        await sourceSnapshot(harness),
        before,
        `${testCase.name}: an expired attempt must not relabel the source`,
      );
      assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), 0);
    }
  });

  it('refuses a stale parser-error metadata write', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);
    const before = await sourceSnapshot(harness);

    const throwingParser: CalendarParser = {
      async parse(): Promise<never> {
        throw new CalendarIntegrityError('synthetic parse failure');
      },
    };
    const result = await runSourceSync(
      syncDeps(harness, {
        parser: throwingParser,
        fetch: expiringFetch(harness, () => textResponse(EMPTY_ICS, 200)),
      }),
      TEST_SOURCE,
      'late-owner',
    );

    assert.equal(result.status, 'skipped');
    assert.equal(result.reason, 'lost-ownership');
    assert.deepEqual(await sourceSnapshot(harness), before);
  });

  it('keeps the attempt inside the statement and parameter budgets', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);
    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
    harness.db.stats.reset();
    harness.setParser({ events: [parsedEvent(start + MS_PER_HOUR, { uid: 'late-stage' })] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'late' }));

    const result = await runSourceSync(
      syncDeps(harness, { repository: gatedRepository(harness.db, 1, () => expireSourceLease(harness)) }),
      TEST_SOURCE,
      'late-owner',
    );

    assert.equal(result.reason, 'lost-ownership');
    assert.ok(
      harness.repository.statementsUsed() <= MAX_D1_STATEMENTS_PER_CONSUMER,
      `used ${harness.repository.statementsUsed()} statements`,
    );
    assert.ok(harness.db.stats.maxParamsPerStatement <= MAX_BOUND_PARAMS);
  });
});

describe('telegram response-body deadlines', () => {
  function clientFor(harness: Harness, timeoutMs = 20): TelegramClient {
    return new TelegramClient({
      botToken: TEST_BOT_TOKEN,
      fetch: harness.fetchSpy.fetch,
      logger: harness.logger.logger,
      timeoutMs,
    });
  }

  async function bounded<T>(promise: Promise<T>, label: string): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        setTimeout(() => reject(new Error(`${label}: still pending after 400ms`)), 400);
      }),
    ]);
  }

  function assertNoSecrets(harness: Harness, text: string): void {
    for (const line of harness.logger.lines) {
      assert.equal(line.includes(TEST_BOT_TOKEN), false, 'the token must never be logged');
      assert.equal(line.includes(text), false, 'message text must never be logged');
    }
  }

  it('settles a stalled 200 body as a transient timeout and cancels it', async () => {
    const harness = createHarness();
    let cancelled = false;
    harness.setHandler(() => stalledResponse({ onCancel: () => (cancelled = true) }));
    const client = clientFor(harness);

    const outcome = await bounded(client.sendMessage(1, 'secret text'), 'stalled 200');

    assert.deepEqual(outcome, { ok: false, kind: 'transient', code: 'timeout' });
    assert.equal(cancelled, true, 'cancellation must be initiated');
    assertNoSecrets(harness, 'secret text');
  });

  it('settles a stalled 429 body whose cancellation never settles', async () => {
    const harness = createHarness();
    let cancelled = false;
    harness.setHandler(() =>
      stalledResponse({
        status: 429,
        headers: { 'retry-after': '1' },
        cancelNeverSettles: true,
        onCancel: () => (cancelled = true),
      }),
    );
    const client = clientFor(harness);

    const outcome = await bounded(client.sendMessage(1, 'text'), 'stalled 429');

    assert.deepEqual(outcome, { ok: false, kind: 'transient', code: 'timeout' });
    assert.equal(cancelled, true);
  });

  it('settles a partial body that stalls as a transient timeout', async () => {
    const harness = createHarness();
    const encoder = new TextEncoder();
    harness.setHandler(() =>
      stalledResponse({
        chunks: [encoder.encode('{"ok":true,"res')],
        onCancel: () => undefined,
      }),
    );
    const client = clientFor(harness);

    const outcome = await bounded(client.sendMessage(1, 'text'), 'partial body');

    assert.deepEqual(outcome, { ok: false, kind: 'transient', code: 'timeout' });
  });

  it('caps an oversized body and classifies it as malformed', async () => {
    const harness = createHarness();
    let cancelled = false;
    harness.setHandler(() =>
      stalledResponse({
        chunks: [new Uint8Array(80 * 1024)],
        cancelNeverSettles: true,
        onCancel: () => (cancelled = true),
      }),
    );
    const client = clientFor(harness);

    const outcome = await bounded(client.sendMessage(1, 'text'), 'oversized body');

    assert.deepEqual(outcome, { ok: false, kind: 'transient', code: 'malformed' });
    assert.equal(cancelled, true, 'an overflow must cancel the stream without awaiting it');
  });

  it('classifies a body read error as transient instead of throwing', async () => {
    const harness = createHarness();
    harness.setHandler(() => stalledResponse({ failAfterMs: 1 }));
    const client = clientFor(harness);

    const outcome = await bounded(client.sendMessage(1, 'text'), 'failing body');

    assert.deepEqual(outcome, { ok: false, kind: 'transient', code: 'network' });
  });

  it('does not await a non-2xx cancellation that never settles', async () => {
    const harness = createHarness();
    let cancelled = false;
    harness.setHandler(() =>
      stalledResponse({
        status: 403,
        cancelNeverSettles: true,
        onCancel: () => (cancelled = true),
      }),
    );
    const client = clientFor(harness);

    const outcome = await bounded(client.sendMessage(1, 'text'), '403 body');

    assert.deepEqual(outcome, { ok: false, kind: 'forbidden' });
    assert.equal(cancelled, true);
  });

  it('bounds a callback acknowledgement with a stalled body', async () => {
    const harness = createHarness();
    let cancelled = false;
    harness.setHandler(() => stalledResponse({ onCancel: () => (cancelled = true) }));
    const client = clientFor(harness);

    const acknowledged = await bounded(
      client.answerCallbackQuery('callback-1'),
      'callback ack',
    );

    assert.equal(acknowledged, false);
    assert.equal(cancelled, true);
  });

  it('retries the stalled message and delivers the next one', async () => {
    const harness = createHarness();
    const jobIds = await seedDueJobs(harness, 2, { occurrenceKey: 'slow#1' });
    assert.equal(jobIds.length, 2);

    let calls = 0;
    const client = clientFor(harness);
    harness.setHandler(() => {
      calls += 1;
      if (calls === 1) {
        harness.clock.advance(1_500);
        return stalledResponse({});
      }
      return jsonResponse({ ok: true, result: { message_id: 2 } });
    });
    const deps = consumerDeps(harness, { telegram: client });
    const messages = jobIds.map((jobId) => makeQueueMessage(jobId));
    harness.db.stats.reset();
    harness.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);

    const summary = await bounded(processQueueBatch(messages, deps), 'consumer batch');

    assert.equal(summary.retried, 1, 'the stalled send is rescheduled, not lost');
    assert.equal(summary.sent, 1, 'the next message is still delivered');
    assert.ok(messages.every((message) => message.acked));
    assert.ok(harness.repository.statementsUsed() <= MAX_D1_STATEMENTS_PER_CONSUMER);
    assert.ok(harness.db.stats.maxParamsPerStatement <= MAX_BOUND_PARAMS);
    const stalled = await harness.repository.getJob(jobIds[0] ?? '');
    assert.equal(stalled?.status, 'pending');
    assert.ok(
      Number(stalled?.next_attempt_at_ms) >= harness.clock.now() + JOB_BACKOFF_BASE_MS - 1_500,
      'backoff is scheduled from the response deadline, not the request start',
    );
    assertNoSecrets(harness, 'text');
  });

  it('persists a cooldown measured when a slow 429 body arrives', async () => {
    const harness = createHarness();
    const [jobId = ''] = await seedDueJobs(harness, 1, { occurrenceKey: 'slow#1' });
    const startedAt = harness.clock.now();

    harness.setHandler(
      () =>
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              setTimeout(() => {
                harness.clock.advance(2_000);
                controller.enqueue(
                  new TextEncoder().encode(
                    JSON.stringify({ ok: false, parameters: { retry_after: 3 } }),
                  ),
                );
                controller.close();
              }, 5);
            },
          }),
          { status: 429 },
        ),
    );
    const client = new TelegramClient({
      botToken: TEST_BOT_TOKEN,
      fetch: harness.fetchSpy.fetch,
      logger: harness.logger.logger,
      timeoutMs: 200,
    });

    const summary = await bounded(
      processQueueBatch([makeQueueMessage(jobId)], consumerDeps(harness, { telegram: client })),
      'slow 429',
    );

    assert.equal(summary.retried, 1);
    const pace = await harness.repository.getPaceState();
    assert.equal(pace?.cooldownUntilMs, startedAt + 2_000 + 3_000);
    const job = await harness.repository.getJob(jobId);
    assert.equal(Number(job?.next_attempt_at_ms), startedAt + 2_000 + 3_000);
    assert.equal(Number(job?.updated_at_ms), startedAt + 2_000);
  });

  it('keeps a healthy send outcome unchanged', async () => {
    const harness = createHarness();
    harness.setHandler(() => jsonResponse({ ok: true, result: { message_id: 7 } }));
    const client = clientFor(harness, 500);

    const outcome: SendOutcome = await client.sendMessage(1, 'text');

    assert.deepEqual(outcome, { ok: true, messageId: 7 });
  });

  it('reports a network failure without leaking the url', async () => {
    const harness = createHarness();
    const client = new TelegramClient({
      botToken: TEST_BOT_TOKEN,
      fetch: () => Promise.reject(new TypeError('network down')),
      logger: harness.logger.logger,
      timeoutMs: 20,
    });

    const outcome = await client.sendMessage(1, 'text');

    assert.deepEqual(outcome, { ok: false, kind: 'transient', code: 'network' });
    assertNoSecrets(harness, 'text');
  });
});
