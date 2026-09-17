/**
 * Finding 8: command delivery identity is separate from subscription
 * activation. These regressions drive the real handler, repository, queue spy
 * and consumer, so every claim is a wire-level or database-level fact.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueMessage } from '../src/queue/consumer.ts';
import { handleUpdate, type HandlerDeps } from '../src/telegram/handlers.ts';
import {
  EVENTS_GUIDANCE_TEXT,
  HELP_TEXT,
  INACTIVE_GUIDANCE_TEXT,
  STOPPED_TEXT,
} from '../src/telegram/replies.ts';
import {
  parseUpdate,
  type ParsedUpdate,
  type PrivateCallbackUpdate,
  type PrivateMessageUpdate,
} from '../src/telegram/updates.ts';
import { JOB_LEASE_MS } from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { consumerDeps, createHarness, type Harness } from './helpers/harness.ts';
import { makeQueueMessage } from './helpers/seed.ts';

const USER_ID = 777;

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

interface SentMessage {
  chatId: number;
  text: string;
}

function sentMessages(harness: Harness): SentMessage[] {
  return harness.fetchSpy.calls
    .filter((call) => call.url.includes('/sendMessage'))
    .map((call) => {
      const body = JSON.parse(String(call.init?.body)) as { chat_id: number; text: string };
      return { chatId: body.chat_id, text: body.text };
    });
}

function callbackAnswers(harness: Harness): string[] {
  return harness.fetchSpy.calls
    .filter((call) => call.url.includes('/answerCallbackQuery'))
    .map((call) => {
      const body = JSON.parse(String(call.init?.body)) as { text?: string };
      return body.text ?? '';
    });
}

function allJobs(harness: Harness): Record<string, unknown>[] {
  return harness.db.database
    .prepare('SELECT * FROM outbound_jobs ORDER BY created_at_ms ASC, id ASC')
    .all() as Record<string, unknown>[];
}

function queuedJobId(harness: Harness, index: number): string {
  const id = harness.queue.batches.flat()[index]?.jobId;
  if (id === undefined) {
    throw new Error(`no queued job at index ${index}`);
  }
  return id;
}

async function deliver(harness: Harness, jobId: string): Promise<string> {
  return processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
}

describe('Audit 8: first-contact commands reach the wire without subscribing', () => {
  it('answers a never-subscribed /events with guidance exactly once and creates no user row', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/events'), handlerDeps(harness));

    assert.equal(await harness.repository.getUser(USER_ID), null);
    assert.equal((await harness.repository.getJob(queuedJobId(harness, 0)))?.expected_revision, null);

    const outcome = await deliver(harness, queuedJobId(harness, 0));
    assert.equal(outcome, 'sent');

    const sent = sentMessages(harness);
    assert.equal(sent.length, 1);
    // No stored timezone → no event list; the guidance names both required
    // steps (/start, then the timezone choice) without unusable buttons.
    assert.equal(sent[0]?.text, EVENTS_GUIDANCE_TEXT);
    assert.match(sent[0]?.text ?? '', /\/start/);
    assert.match(sent[0]?.text ?? '', /часовой пояс/);
    assert.equal(await harness.repository.getUser(USER_ID), null);
  });

  it('answers a never-subscribed unknown command exactly once and creates no user row', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/nonsense'), handlerDeps(harness));

    assert.equal(await harness.repository.getUser(USER_ID), null);
    assert.equal((await harness.repository.getJob(queuedJobId(harness, 0)))?.expected_revision, null);

    const outcome = await deliver(harness, queuedJobId(harness, 0));
    assert.equal(outcome, 'sent');

    const sent = sentMessages(harness);
    assert.equal(sent.length, 1);
    assert.equal(sent[0]?.text, HELP_TEXT);
    assert.equal(await harness.repository.getUser(USER_ID), null);
  });

  it('answers a never-subscribed /stop exactly once and leaves the sender unsubscribed', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/stop'), handlerDeps(harness));

    assert.equal(await harness.repository.getUser(USER_ID), null);
    const stopJobId = queuedJobId(harness, 0);
    assert.equal((await harness.repository.getJob(stopJobId))?.expected_revision, null);

    const outcome = await deliver(harness, stopJobId);
    assert.equal(outcome, 'sent');

    const sent = sentMessages(harness);
    assert.equal(sent.length, 1);
    assert.equal(sent[0]?.text, STOPPED_TEXT);
    assert.equal(await harness.repository.getUser(USER_ID), null);
  });
});

describe('Audit 8: /stop cancels, deactivates, then confirms', () => {
  it('cancels pending and enqueued work, deactivates, and confirms with the post-stop revision', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    const startJobId = queuedJobId(harness, 0);

    // The scheduler reserves the /start reply; /stop must reach `enqueued` too.
    const claimed = await harness.repository.claimDueJobs(
      'scheduler',
      harness.clock.now(),
      JOB_LEASE_MS,
      10,
    );
    assert.deepEqual(claimed, [{ jobId: startJobId }]);

    await handleUpdate(messageUpdate(2, '/stop'), deps);

    const user = await harness.repository.getUser(USER_ID);
    assert.equal(user?.active, false);
    assert.equal((await harness.repository.getJob(startJobId))?.status, 'cancelled');

    const stopJobId = queuedJobId(harness, 1);
    const stopJob = await harness.repository.getJob(stopJobId);
    assert.equal(stopJob?.status, 'pending');
    assert.equal(stopJob?.expected_revision, user?.revision);

    assert.equal(await deliver(harness, stopJobId), 'sent');
    const sent = sentMessages(harness);
    assert.equal(sent.length, 1);
    assert.equal(sent[0]?.text, STOPPED_TEXT);

    // Only an explicit /start reactivates the subscription.
    await handleUpdate(messageUpdate(3, '/start'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, true);
  });

  it('delivers only the stop confirmation when /stop follows a queued /start', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    const startJobId = queuedJobId(harness, 0);
    await handleUpdate(messageUpdate(2, '/stop'), deps);
    const stopJobId = queuedJobId(harness, 1);

    assert.equal(await deliver(harness, startJobId), 'skipped');
    assert.equal(await deliver(harness, stopJobId), 'sent');

    const sent = sentMessages(harness);
    assert.equal(sent.length, 1);
    assert.equal(sent[0]?.text, STOPPED_TEXT);
    assert.equal((await harness.repository.getJob(startJobId))?.status, 'cancelled');
  });

  it('supersedes an in-flight /start reply that a consumer already claimed', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    const startJobId = queuedJobId(harness, 0);

    // A consumer claims the /start reply just before /stop is handled.
    const raceOwner = 'race-consumer';
    const claimed = await harness.repository.claimJobContext(
      startJobId,
      raceOwner,
      harness.clock.now(),
      JOB_LEASE_MS,
    );
    assert.equal(claimed?.expectedRevision, 1);

    await handleUpdate(messageUpdate(2, '/stop'), deps);
    const stopJobId = queuedJobId(harness, 1);

    const staleOutcome = await processQueueMessage(
      makeQueueMessage(startJobId),
      consumerDeps(harness, { ownerFactory: () => raceOwner }),
    );
    assert.equal(staleOutcome, 'terminal');
    assert.equal((await harness.repository.getJob(startJobId))?.last_error_code, 'superseded');

    assert.equal(await deliver(harness, stopJobId), 'sent');
    const sent = sentMessages(harness);
    assert.equal(sent.length, 1);
    assert.equal(sent[0]?.text, STOPPED_TEXT);
  });
});

describe('Audit 8: inactive senders never reactivate', () => {
  it('answers /settings for a missing sender with guidance and no user row', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/settings'), handlerDeps(harness));

    assert.equal(await harness.repository.getUser(USER_ID), null);
    const jobId = queuedJobId(harness, 0);
    assert.equal((await harness.repository.getJob(jobId))?.expected_revision, null);

    assert.equal(await deliver(harness, jobId), 'sent');
    const sent = sentMessages(harness);
    assert.equal(sent.length, 1);
    assert.equal(sent[0]?.text, INACTIVE_GUIDANCE_TEXT);
    assert.equal(await harness.repository.getUser(USER_ID), null);
  });

  it('/settings after /stop stays inactive and changes no existing work', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/stop'), deps);

    const before = await harness.repository.getUser(USER_ID);
    assert.equal(before?.active, false);
    const jobsBefore = allJobs(harness).map((job) => [job.id, job.status]);

    await handleUpdate(messageUpdate(3, '/settings'), deps);

    assert.deepEqual(await harness.repository.getUser(USER_ID), before);
    const jobsAfter = allJobs(harness);
    assert.equal(jobsAfter.length, jobsBefore.length + 1);
    assert.deepEqual(
      jobsAfter.slice(0, jobsBefore.length).map((job) => [job.id, job.status]),
      jobsBefore,
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM outbound_jobs WHERE kind = 'reminder'"),
      0,
    );

    const guidanceJob = jobsAfter[jobsAfter.length - 1];
    assert.equal(guidanceJob?.status, 'pending');
    assert.equal(guidanceJob?.expected_revision, before?.revision);
    assert.equal(await deliver(harness, String(guidanceJob?.id)), 'sent');
    assert.equal(sentMessages(harness).at(-1)?.text, INACTIVE_GUIDANCE_TEXT);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, false);
  });

  it('answers every allowed stale callback after /stop without a job or reactivation', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/stop'), deps);

    const before = await harness.repository.getUser(USER_ID);
    const jobsBefore = allJobs(harness).length;

    const allowed = [
      'course:basic',
      'course:extended',
      'reminder:30',
      'reminder:1440',
      'tz:Europe/Moscow',
      'tz:Asia/Yerevan',
    ];
    for (const [index, data] of allowed.entries()) {
      await handleUpdate(callbackUpdate(10 + index, data), deps);
    }

    assert.deepEqual(await harness.repository.getUser(USER_ID), before);
    assert.equal(allJobs(harness).length, jobsBefore);
    assert.deepEqual(callbackAnswers(harness), allowed.map(() => INACTIVE_GUIDANCE_TEXT));
    assert.equal(sentMessages(harness).length, 0);
  });

  it('keeps the unknown-action answer for a disallowed callback after /stop', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/stop'), deps);
    const jobsBefore = allJobs(harness).length;

    await handleUpdate(callbackUpdate(3, 'course:admin'), deps);

    assert.equal(allJobs(harness).length, jobsBefore);
    assert.deepEqual(callbackAnswers(harness), ['Неизвестное действие']);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, false);
  });
});

describe('Audit 8: active-user behavior is unchanged', () => {
  it('delivers /settings and applies course and reminder callbacks', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    assert.equal(await deliver(harness, queuedJobId(harness, 0)), 'sent');

    await handleUpdate(messageUpdate(2, '/settings'), deps);
    assert.equal(await deliver(harness, queuedJobId(harness, 1)), 'sent');
    assert.match(sentMessages(harness)[1]?.text ?? '', /Настройки:/);

    await handleUpdate(callbackUpdate(3, 'reminder:1440'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.reminderOffsetMinutes, 1440);
    assert.equal(await deliver(harness, queuedJobId(harness, 2)), 'sent');
    assert.match(sentMessages(harness)[2]?.text ?? '', /за сутки/);

    await handleUpdate(callbackUpdate(4, 'course:extended'), deps);
    assert.equal((await harness.repository.getUser(USER_ID))?.course, 'extended');
    assert.equal(await deliver(harness, queuedJobId(harness, 3)), 'sent');
    assert.match(sentMessages(harness)[3]?.text ?? '', /Расширенный/);
  });
});
