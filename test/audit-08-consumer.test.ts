/**
 * F8 regression (consumer side): command delivery identity is separate from
 * subscription activation. A command reply is delivered from its persisted
 * payload even when the sender has no user row, a reply composed for a
 * superseded subscription revision is finished terminally as `superseded`, and
 * the atomic `beginSendAttempt` guard refuses to send when the revision moves
 * between claim and send. Reminder screening is unchanged.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueBatch, processQueueMessage } from '../src/queue/consumer.ts';
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
import { consumerDeps, createHarness, type Harness } from './helpers/harness.ts';
import { makeQueueMessage, type TrackedMessage } from './helpers/seed.ts';

const USER_ID = 909;

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

/** Every message the handler enqueued since `from`, as ackable Queue messages. */
function queued(harness: Harness, from: number): TrackedMessage[] {
  return harness.queue.batches
    .slice(from)
    .flat()
    .map((message) => makeQueueMessage(message.jobId));
}

function queuedJobId(harness: Harness, index: number): string {
  const id = harness.queue.batches.flat()[index]?.jobId;
  if (id === undefined) {
    throw new Error(`no queued job at index ${index}`);
  }
  return id;
}

async function confirmUserMissing(harness: Harness): Promise<void> {
  assert.equal(await harness.repository.getUser(USER_ID), null);
}

describe('Audit 8: command delivery without a subscription row', () => {
  it('delivers first-contact /events guidance through the Queue without subscribing', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/events'), handlerDeps(harness));

    const messages = queued(harness, 0);
    const summary = await processQueueBatch(messages, consumerDeps(harness));

    assert.equal(summary.sent, 1);
    assert.ok(messages.every((message) => message.acked));
    // Without a subscription there is no stored timezone, so no event list is
    // rendered; the guidance names /start and the timezone choice that follows.
    assert.equal(sentTexts(harness)[0], EVENTS_GUIDANCE_TEXT);
    await confirmUserMissing(harness);
  });

  it('delivers a first-contact unknown command through the Queue without subscribing', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/nonsense'), handlerDeps(harness));

    const summary = await processQueueBatch(queued(harness, 0), consumerDeps(harness));

    assert.equal(summary.sent, 1);
    assert.equal(sentTexts(harness)[0], HELP_TEXT);
    await confirmUserMissing(harness);
  });

  it('delivers a first-contact /stop through the Queue without subscribing', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/stop'), handlerDeps(harness));

    const summary = await processQueueBatch(queued(harness, 0), consumerDeps(harness));

    assert.equal(summary.sent, 1);
    assert.equal(sentTexts(harness)[0], STOPPED_TEXT);
    await confirmUserMissing(harness);
  });
});

describe('Audit 8: supersession on the consumer side', () => {
  it('suppresses an in-flight settings reply after /stop and still confirms the stop', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/settings'), deps);
    const settingsJobId = queuedJobId(harness, 1);
    assert.equal(
      (await harness.repository.getJob(settingsJobId))?.expected_revision,
      1,
    );

    // A consumer claimed the settings reply just before /stop landed.
    const raceOwner = 'race-consumer';
    assert.notEqual(
      await harness.repository.claimJobContext(
        settingsJobId,
        raceOwner,
        harness.clock.now(),
        JOB_LEASE_MS,
      ),
      null,
    );

    await handleUpdate(messageUpdate(3, '/stop'), deps);
    const stopJobId = queuedJobId(harness, 2);

    const staleOutcome = await processQueueMessage(
      makeQueueMessage(settingsJobId),
      consumerDeps(harness, { ownerFactory: () => raceOwner }),
    );
    assert.equal(staleOutcome, 'terminal');
    assert.equal((await harness.repository.getJob(settingsJobId))?.last_error_code, 'superseded');

    assert.equal(await processQueueMessage(makeQueueMessage(stopJobId), consumerDeps(harness)), 'sent');
    assert.deepEqual(sentTexts(harness), [STOPPED_TEXT], 'only the stop confirmation may be sent');
    assert.equal((await harness.repository.getUser(USER_ID))?.active, false);
  });

  it('suppresses the older /start reply and delivers only the stop confirmation', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    const startJobId = queuedJobId(harness, 0);

    // The /start reply is leased when /stop arrives, so cancellation cannot
    // reach it: the consumer itself must declare it superseded.
    const raceOwner = 'race-consumer';
    assert.notEqual(
      await harness.repository.claimJobContext(startJobId, raceOwner, harness.clock.now(), JOB_LEASE_MS),
      null,
    );
    await handleUpdate(messageUpdate(2, '/stop'), deps);
    const stopJobId = queuedJobId(harness, 1);
    assert.equal((await harness.repository.getJob(stopJobId))?.expected_revision, 1 + 1);

    assert.equal(
      await processQueueMessage(
        makeQueueMessage(startJobId),
        consumerDeps(harness, { ownerFactory: () => raceOwner }),
      ),
      'terminal',
    );
    assert.equal((await harness.repository.getJob(startJobId))?.last_error_code, 'superseded');
    assert.equal(await processQueueMessage(makeQueueMessage(stopJobId), consumerDeps(harness)), 'sent');

    assert.deepEqual(sentTexts(harness), [STOPPED_TEXT]);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, false);
  });

  it('settles superseded when the revision moves after the claim (atomic guard)', async () => {
    const harness = createHarness();
    const handler = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), handler);
    const startJobId = queuedJobId(harness, 0);
    assert.equal((await harness.repository.getUser(USER_ID))?.revision, 1);

    // The pace window is full, so the consumer sleeps before sending. The
    // injected sleep moves the subscription revision after the claim but
    // before the send: only the atomic guard can see it.
    const now = harness.clock.now();
    for (let index = 0; index < 20; index += 1) {
      await harness.repository.acquireSendSlot(now, 20);
    }
    const racingDeps = consumerDeps(harness, {
      sleep: async (ms) => {
        await harness.repository.setUserCourse(USER_ID, 'extended', harness.clock.now());
        harness.clock.advance(ms);
      },
    });

    // The atomic `beginSendAttempt` reservation observes the moved revision and
    // reports `superseded`; the consumer persists a terminal superseded state
    // without sending instead of throwing.
    assert.equal(
      await processQueueMessage(makeQueueMessage(startJobId), racingDeps),
      'terminal',
    );

    assert.equal(sentTexts(harness).length, 0, 'no stale call may leave the Worker');
    const row = harness.db.database
      .prepare('SELECT status, attempt_count, last_error_code FROM outbound_jobs WHERE id = ?')
      .get(startJobId) as { status: string; attempt_count: unknown; last_error_code: unknown };
    assert.equal(row.status, 'cancelled', 'the superseded reply settles terminally');
    assert.equal(row.last_error_code, 'superseded');
    assert.equal(Number(row.attempt_count), 0, 'a refused send counts no attempt');
  });
});

describe('Audit 8: inactive senders stay inactive', () => {
  it('answers /settings after /stop with guidance and leaves no stale UI reply queued', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    assert.equal(await processQueueMessage(makeQueueMessage(queuedJobId(harness, 0)), consumerDeps(harness)), 'sent');
    await handleUpdate(messageUpdate(2, '/stop'), deps);
    assert.equal(await processQueueMessage(makeQueueMessage(queuedJobId(harness, 1)), consumerDeps(harness)), 'sent');
    const before = await harness.repository.getUser(USER_ID);
    assert.equal(before?.active, false);

    await handleUpdate(messageUpdate(3, '/settings'), deps);

    assert.deepEqual(await harness.repository.getUser(USER_ID), before, '/settings must not reactivate');
    const guidanceJobId = queuedJobId(harness, 2);
    assert.equal((await harness.repository.getJob(guidanceJobId))?.expected_revision, before?.revision);
    assert.equal(
      await processQueueMessage(makeQueueMessage(guidanceJobId), consumerDeps(harness)),
      'sent',
    );
    assert.equal(sentTexts(harness).at(-1), INACTIVE_GUIDANCE_TEXT);
    assert.equal((await harness.repository.getUser(USER_ID))?.active, false);
  });

  it('answers a stale keyboard callback after /stop without a job or a send', async () => {
    const harness = createHarness();
    const deps = handlerDeps(harness);
    await handleUpdate(messageUpdate(1, '/start'), deps);
    await handleUpdate(messageUpdate(2, '/stop'), deps);

    const before = await harness.repository.getUser(USER_ID);
    const jobsBefore = harness.queue.batches.flat().length;

    await handleUpdate(callbackUpdate(3, 'rm:t:1440'), deps);

    assert.deepEqual(await harness.repository.getUser(USER_ID), before);
    assert.equal(harness.queue.batches.flat().length, jobsBefore, 'no reply job may be created');
    assert.deepEqual(callbackAnswers(harness), [INACTIVE_GUIDANCE_TEXT]);
    assert.equal(sentTexts(harness).length, 0, 'a stale callback never reaches sendMessage');
    assert.equal((await harness.repository.getUser(USER_ID))?.active, false);
  });
});
