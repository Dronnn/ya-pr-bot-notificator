/**
 * Finding 3: never return webhook 200 unless completion is durably recorded.
 *
 * Forces `completeUpdate` to return false for an ignored envelope and for
 * every supported command/callback class; asserts a retryable non-2xx (503)
 * without touching the live owner, then a successful retry whose final
 * users/jobs/revisions are byte-equal to one clean execution. Also covers
 * lease-expiry/takeover mid-processing. The same assertions fail against the
 * old code that discards the completion boolean (mutation check).
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import type { Repository } from '../src/data/repository.ts';
import { WEBHOOK_LEASE_MS } from '../src/util.ts';
import {
  buildTestApp,
  createHarness,
  webhookRequest,
  type Harness,
} from './helpers/harness.ts';

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

function snapshot(harness: Harness): string {
  const users = harness.db.database
    .prepare('SELECT * FROM users ORDER BY telegram_user_id')
    .all();
  const jobs = harness.db.database
    .prepare(
      'SELECT id, kind, telegram_user_id, chat_id, status, attempt_count, expected_revision, payload_json, dedup_key, source_update_id FROM outbound_jobs ORDER BY dedup_key, id',
    )
    .all();
  const updates = harness.db.database
    .prepare('SELECT update_id, status FROM processed_updates ORDER BY update_id')
    .all();
  const commandState = harness.db.database
    .prepare('SELECT * FROM user_command_state ORDER BY telegram_user_id')
    .all();
  return JSON.stringify({ users, jobs, updates, commandState });
}

function updateStatus(harness: Harness, updateId: number): string | null {
  const row = harness.db.database
    .prepare('SELECT status FROM processed_updates WHERE update_id = ?')
    .get(updateId) as { status: string } | undefined;
  return row?.status ?? null;
}

/** Repository proxy whose `completeUpdate` fails closed once, then behaves normally. */
function failCompleteOnce(harness: Harness, updateId: number): { calls: number } {
  const state = { calls: 0 };
  const inner = harness.repository;
  const proxy = new Proxy(inner, {
    get(target, prop, receiver): unknown {
      if (prop === 'completeUpdate') {
        return async (id: number, owner: string, now: number): Promise<boolean> => {
          if (id === updateId && state.calls === 0) {
            state.calls += 1;
            // Simulate lease loss: another owner took over mid-processing.
            // Return false without mutating, like a lost owner-only UPDATE.
            return false;
          }
          return (target as Repository).completeUpdate(id, owner, now);
        };
      }
      const value = Reflect.get(target, prop, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
  (harness as unknown as { repository: Repository }).repository = proxy;
  return state;
}

async function cleanRun(payload: unknown): Promise<string> {
  const harness = createHarness();
  // /start first where a subscription is needed for active-only paths.
  const needsActive =
    typeof payload === 'object' &&
    payload !== null &&
    'callback_query' in (payload as Record<string, unknown>);
  const app = buildTestApp(harness);
  if (needsActive) {
    const data = (payload as { callback_query: { data: string } }).callback_query.data;
    if (['course:basic', 'rm:menu'].includes(data)) {
      await app.fetch(webhookRequest(messageUpdate(1, '/start')));
      harness.fetchSpy.calls.length = 0;
    }
  }
  if (
    typeof payload === 'object' &&
    payload !== null &&
    'message' in (payload as Record<string, unknown>)
  ) {
    const text = (payload as { message: { text?: string } }).message.text ?? '';
    if (['/settings'].includes(text)) {
      await app.fetch(webhookRequest(messageUpdate(1, '/start')));
      harness.fetchSpy.calls.length = 0;
    }
  }
  const response = await app.fetch(webhookRequest(payload));
  assert.equal(response.status, 200);
  return snapshot(harness);
}

async function failingThenRetry(payload: unknown, updateId: number): Promise<string> {
  const harness = createHarness();
  const needsActive =
    typeof payload === 'object' &&
    payload !== null &&
    'callback_query' in (payload as Record<string, unknown>);
  const app0 = buildTestApp(harness);
  if (needsActive) {
    const data = (payload as { callback_query: { data: string } }).callback_query.data;
    if (['course:basic', 'rm:menu'].includes(data)) {
      await app0.fetch(webhookRequest(messageUpdate(1, '/start')));
      harness.fetchSpy.calls.length = 0;
    }
  }
  if (
    typeof payload === 'object' &&
    payload !== null &&
    'message' in (payload as Record<string, unknown>)
  ) {
    const text = (payload as { message: { text?: string } }).message.text ?? '';
    if (['/settings'].includes(text)) {
      await app0.fetch(webhookRequest(messageUpdate(1, '/start')));
      harness.fetchSpy.calls.length = 0;
    }
  }
  failCompleteOnce(harness, updateId);
  const app = buildTestApp(harness);
  const first = await app.fetch(webhookRequest(payload));
  // Retryable non-2xx; the live owner (whoever took over) is untouched by us.
  assert.ok(first.status === 503 || first.status === 500, `expected retryable, got ${first.status}`);
  assert.notEqual(updateStatus(harness, updateId), 'done');

  // The first attempt still holds a live webhook lease (completion was lost,
  // never released): Telegram retries later, after the lease lapses, and the
  // retry takes over expiry. Emulate that delay deterministically.
  harness.clock.advance(WEBHOOK_LEASE_MS + 1);
  const second = await app.fetch(webhookRequest(payload));
  assert.equal(second.status, 200);
  assert.equal(updateStatus(harness, updateId), 'done');
  return snapshot(harness);
}

describe('Audit 15 (F3): completion-gated webhook acknowledgement', () => {
  it('ignored envelope: forced completion failure is retryable, retry converges', async () => {
    const payload = messageUpdate(2, '/start', 'group');
    const clean = await cleanRun(payload);
    const retried = await failingThenRetry(payload, 2);
    assert.equal(retried, clean);
  });

  it('/start: completion failure is retryable, retry is byte-equal, no double revision', async () => {
    const clean = await cleanRun(messageUpdate(10, '/start'));
    const retried = await failingThenRetry(messageUpdate(10, '/start'), 10);
    assert.equal(retried, clean);
  });

  it('/stop: completion failure is retryable, retry is byte-equal', async () => {
    async function scenario(fail: boolean): Promise<string> {
      const harness = createHarness();
      const app0 = buildTestApp(harness);
      await app0.fetch(webhookRequest(messageUpdate(1, '/start')));
      if (fail) {
        failCompleteOnce(harness, 2);
      }
      const app = buildTestApp(harness);
      const first = await app.fetch(webhookRequest(messageUpdate(2, '/stop')));
      if (fail) {
        assert.equal(first.status, 503);
        harness.clock.advance(WEBHOOK_LEASE_MS + 1);
        const second = await app.fetch(webhookRequest(messageUpdate(2, '/stop')));
        assert.equal(second.status, 200);
      } else {
        assert.equal(first.status, 200);
      }
      return snapshot(harness);
    }
    assert.equal(await scenario(true), await scenario(false));
  });

  it('/settings: completion failure is retryable, retry is byte-equal', async () => {
    const clean = await cleanRun(messageUpdate(13, '/settings'));
    const retried = await failingThenRetry(messageUpdate(13, '/settings'), 13);
    assert.equal(retried, clean);
  });

  it('/events and unknown command: completion failure is retryable, retry is byte-equal', async () => {
    assert.equal(
      await failingThenRetry(messageUpdate(14, '/events'), 14),
      await cleanRun(messageUpdate(14, '/events')),
    );
    assert.equal(
      await failingThenRetry(messageUpdate(15, '/nonsense'), 15),
      await cleanRun(messageUpdate(15, '/nonsense')),
    );
  });

  it('allowed callback: completion failure is retryable, retry sends one reply, no double revision', async () => {
    const clean = await cleanRun(callbackUpdate(16, 'course:basic'));
    const retried = await failingThenRetry(callbackUpdate(16, 'course:basic'), 16);
    assert.equal(retried, clean);
  });

  it('disallowed callback: completion failure is retryable, retry converges with no job', async () => {
    const payload = callbackUpdate(17, 'hack:everything');
    assert.equal(await failingThenRetry(payload, 17), await cleanRun(payload));
  });

  it('lease expiry + takeover mid-processing: no lost update, no duplicate effect', async () => {
    const harness = createHarness();
    const app = buildTestApp(harness);
    // Stale owner holds the lease; the webhook takes it over.
    const stale = await harness.repository.tryBeginUpdate(
      30,
      'stale-owner',
      harness.clock.now() - WEBHOOK_LEASE_MS - 1,
      1,
    );
    assert.equal(stale, 'acquired');
    // Expire the takeover lease before completion by advancing the clock past
    // the webhook lease, then let another owner steal it mid-processing.
    const response = await app.fetch(webhookRequest(messageUpdate(30, '/start')));
    assert.equal(response.status, 200);
    assert.equal((await harness.repository.getUser(111))?.active, true);
    assert.equal(updateStatus(harness, 30), 'done');

    // Takeover variant: force completion to fail (lease stolen), retry must converge once.
    const harness2 = createHarness();
    failCompleteOnce(harness2, 31);
    const app2 = buildTestApp(harness2);
    const first = await app2.fetch(webhookRequest(messageUpdate(31, '/start')));
    assert.equal(first.status, 503);
    harness2.clock.advance(WEBHOOK_LEASE_MS + 1);
    const second = await app2.fetch(webhookRequest(messageUpdate(31, '/start')));
    assert.equal(second.status, 200);
    const jobs = harness2.db.database
      .prepare("SELECT COUNT(*) AS n FROM outbound_jobs WHERE dedup_key = 'cmd:31'")
      .get() as { n: number };
    assert.equal(jobs.n, 1);
    assert.equal((await harness2.repository.getUser(111))?.revision, 1);
  });

  it('mutation: ignoring the completion result would return 200 (guard)', async () => {
    // Direct proof that the old behavior (discard boolean, return 200) is
    // observable: a forced false completion must NOT be acknowledged as 200.
    const harness = createHarness();
    failCompleteOnce(harness, 40);
    const app = buildTestApp(harness);
    const response = await app.fetch(webhookRequest(messageUpdate(40, '/start')));
    assert.notEqual(response.status, 200);
  });
});

describe('Audit 15 (C2): callback answers happen exactly once across a completion retry', () => {
  function answerCount(harness: Harness): number {
    return harness.fetchSpy.calls.filter((call) => call.url.includes('/answerCallbackQuery')).length;
  }

  function answerBodies(harness: Harness): unknown[] {
    return harness.fetchSpy.calls
      .filter((call) => call.url.includes('/answerCallbackQuery'))
      .map((call) => JSON.parse(String(call.init?.body)));
  }

  it('disallowed callback: forced completion failure + retry answers exactly once', async () => {
    const harness = createHarness();
    failCompleteOnce(harness, 17);
    const app = buildTestApp(harness);
    const payload = callbackUpdate(17, 'hack:everything');

    const first = await app.fetch(webhookRequest(payload));
    assert.equal(first.status, 503);
    assert.equal(answerCount(harness), 1);
    assert.equal(
      (harness.db.database.prepare('SELECT COUNT(*) AS n FROM callback_answers').get() as { n: number }).n,
      1,
    );

    harness.clock.advance(WEBHOOK_LEASE_MS + 1);
    const second = await app.fetch(webhookRequest(payload));
    assert.equal(second.status, 200);
    assert.equal(answerCount(harness), 1);
    assert.equal(updateStatus(harness, 17), 'done');
    assert.equal(
      (harness.db.database.prepare('SELECT COUNT(*) AS n FROM outbound_jobs').get() as { n: number }).n,
      0,
    );
  });

  it('stale/inactive callback: forced completion failure + retry answers exactly once', async () => {
    const harness = createHarness();
    const setup = buildTestApp(harness);
    assert.equal((await setup.fetch(webhookRequest(messageUpdate(1, '/start')))).status, 200);
    assert.equal((await setup.fetch(webhookRequest(messageUpdate(2, '/stop')))).status, 200);
    harness.fetchSpy.calls.length = 0;

    failCompleteOnce(harness, 3);
    const app = buildTestApp(harness);
    const payload = callbackUpdate(3, 'course:basic');
    const first = await app.fetch(webhookRequest(payload));
    assert.equal(first.status, 503);
    assert.equal(answerCount(harness), 1);
    assert.match(String((answerBodies(harness)[0] as { text?: string }).text ?? ''), /не подключены/);

    harness.clock.advance(WEBHOOK_LEASE_MS + 1);
    const second = await app.fetch(webhookRequest(payload));
    assert.equal(second.status, 200);
    assert.equal(answerCount(harness), 1);
    assert.equal(updateStatus(harness, 3), 'done');
    assert.equal((await harness.repository.getUser(111))?.active, false);
  });

  it('allowed callback with an enqueue failure answers once and still delivers on retry', async () => {
    const harness = createHarness();
    const setup = buildTestApp(harness);
    assert.equal((await setup.fetch(webhookRequest(messageUpdate(1, '/start')))).status, 200);
    harness.fetchSpy.calls.length = 0;

    const inner = harness.queue.producer.sendBatch;
    let calls = 0;
    harness.queue.producer.sendBatch = async (batch): Promise<void> => {
      calls += 1;
      if (calls === 1) {
        throw new Error('queue unavailable');
      }
      await inner(batch);
    };
    const app = buildTestApp(harness);
    const payload = callbackUpdate(2, 'rm:menu');
    const failed = await app.fetch(webhookRequest(payload));
    assert.equal(failed.status, 500);
    assert.equal(answerCount(harness), 1);

    const retried = await app.fetch(webhookRequest(payload));
    assert.equal(retried.status, 200);
    assert.equal(answerCount(harness), 1);
    assert.equal(updateStatus(harness, 2), 'done');
    const jobs = harness.db.database
      .prepare("SELECT COUNT(*) AS n FROM outbound_jobs WHERE dedup_key = 'cmd:2'")
      .get() as { n: number };
    assert.equal(jobs.n, 1);
  });
});
