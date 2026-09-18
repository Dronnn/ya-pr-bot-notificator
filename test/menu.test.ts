/**
 * Reply-keyboard menu: the persistent bottom keyboard, its label-to-command
 * mapping, and the text-only replies that carry it. A tap must behave exactly
 * like the matching slash command, and the stored payload must survive the
 * consumer's payload validation.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { processQueueMessage } from '../src/queue/consumer.ts';
import { handleUpdate, type HandlerDeps } from '../src/telegram/handlers.ts';
import {
  buildRemindersKeyboard,
  HELP_TEXT,
  MENU_KEYBOARD,
  resolveMenuCommand,
  SETTINGS_KEYBOARD,
  STOPPED_TEXT,
  TIMEZONE_KEYBOARD,
} from '../src/telegram/replies.ts';
import {
  parseUpdate,
  type ParsedUpdate,
  type PrivateMessageUpdate,
} from '../src/telegram/updates.ts';
import { consumerDeps, createHarness, type Harness } from './helpers/harness.ts';
import { makeQueueMessage, occurrence, onboardUser, seedSource } from './helpers/seed.ts';

const USER_ID = 9300;

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

function messageUpdate(updateId: number, text: string): PrivateMessageUpdate {
  const parsed = parseUpdate({
    update_id: updateId,
    message: {
      message_id: updateId,
      chat: { id: USER_ID, type: 'private' },
      from: { id: USER_ID, username: 'andrew' },
      text,
    },
  });
  const update: ParsedUpdate = parsed.ok ? parsed.update : (() => {
    throw new Error('expected a private message update');
  })();
  if (update.kind !== 'message') {
    throw new Error('expected a private message update');
  }
  return update;
}

function lastJobId(harness: Harness): string {
  const id = harness.queue.batches.flat().at(-1)?.jobId;
  if (id === undefined) {
    throw new Error('no queued job');
  }
  return id;
}

function jobPayload(harness: Harness, jobId: string): Record<string, unknown> {
  const raw = harness.db.database
    .prepare('SELECT payload_json FROM outbound_jobs WHERE id = ?')
    .get(jobId) as { payload_json: unknown } | undefined;
  return JSON.parse(String(raw?.payload_json ?? '{}')) as Record<string, unknown>;
}

async function deliver(harness: Harness, jobId: string): Promise<string> {
  return processQueueMessage(makeQueueMessage(jobId), consumerDeps(harness));
}

function sentBodies(harness: Harness): Record<string, unknown>[] {
  return harness.fetchSpy.calls
    .filter((call) => call.url.includes('/sendMessage'))
    .map((call) => JSON.parse(String(call.init?.body)) as Record<string, unknown>);
}

describe('Menu label mapping', () => {
  it('resolves every menu label to its command and passes other text through', () => {
    assert.equal(resolveMenuCommand('Настройки'), '/settings');
    assert.equal(resolveMenuCommand('Напоминания'), '/reminders');
    assert.equal(resolveMenuCommand('  ЧАСОВОЙ ПОЯС '), '/timezone');
    assert.equal(resolveMenuCommand('Ближайшие занятия'), '/events');
    assert.equal(resolveMenuCommand('помощь'), '/help');
    assert.equal(resolveMenuCommand('/settings'), '/settings');
    assert.equal(resolveMenuCommand('привет'), 'привет');
  });
});

describe('Menu keyboard on replies', () => {
  it('a Помощь tap answers with the help text and the persistent menu, and delivers it', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(901, 'Помощь'), handlerDeps(harness));

    const jobId = lastJobId(harness);
    const payload = jobPayload(harness, jobId);
    assert.equal(payload.text, HELP_TEXT);
    assert.deepEqual(payload.replyMarkup, MENU_KEYBOARD);

    assert.equal(await deliver(harness, jobId), 'sent');
    const body = sentBodies(harness).at(-1);
    assert.equal(body?.text, HELP_TEXT);
    assert.deepEqual(body?.reply_markup, MENU_KEYBOARD);
  });

  it('a Настройки tap renders the settings view with the inline controls', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/start'), handlerDeps(harness));
    onboardUser(harness, USER_ID);

    await handleUpdate(messageUpdate(902, 'Настройки'), handlerDeps(harness));
    const jobId = lastJobId(harness);
    const payload = jobPayload(harness, jobId);
    assert.match(String(payload.text), /^Настройки:/);
    assert.deepEqual(payload.replyMarkup, SETTINGS_KEYBOARD);

    assert.equal(await deliver(harness, jobId), 'sent');
  });

  it('a Напоминания tap renders the rule set with the start state', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/start'), handlerDeps(harness));

    await handleUpdate(messageUpdate(906, 'Напоминания'), handlerDeps(harness));
    const payload = jobPayload(harness, lastJobId(harness));
    assert.match(String(payload.text), /Напоминания \(правил: 3\):/);
    assert.match(String(payload.text), /В момент начала: включено/);
    assert.deepEqual(payload.replyMarkup, buildRemindersKeyboard([1440, 60, 5, 0]));

    assert.equal(await deliver(harness, lastJobId(harness)), 'sent');
  });

  it('a Часовой пояс tap prompts the timezone choice', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/start'), handlerDeps(harness));

    await handleUpdate(messageUpdate(903, 'Часовой пояс'), handlerDeps(harness));
    const payload = jobPayload(harness, lastJobId(harness));
    assert.match(String(payload.text), /Часовой пояс/);
    assert.deepEqual(payload.replyMarkup, TIMEZONE_KEYBOARD);
  });

  it('a Ближайшие занятия tap renders the upcoming list and keeps the menu', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'soon', startsAtMs: now + 60 * 60 * 1000, summary: 'Вебинар' })],
      now,
    );
    await handleUpdate(messageUpdate(1, '/start'), handlerDeps(harness));
    onboardUser(harness, USER_ID);

    await handleUpdate(messageUpdate(904, 'Ближайшие занятия'), handlerDeps(harness));
    const payload = jobPayload(harness, lastJobId(harness));
    assert.match(String(payload.text), /Ближайшие занятия \(Базовый\):/);
    assert.match(String(payload.text), /Вебинар/);
    assert.deepEqual(payload.replyMarkup, MENU_KEYBOARD);

    assert.equal(await deliver(harness, lastJobId(harness)), 'sent');
  });

  it('a /stop confirmation carries the menu so the way back stays one tap away', async () => {
    const harness = createHarness();
    await handleUpdate(messageUpdate(1, '/start'), handlerDeps(harness));
    onboardUser(harness, USER_ID);

    await handleUpdate(messageUpdate(905, '/stop'), handlerDeps(harness));
    const payload = jobPayload(harness, lastJobId(harness));
    assert.equal(payload.text, STOPPED_TEXT);
    assert.deepEqual(payload.replyMarkup, MENU_KEYBOARD);

    assert.equal(await deliver(harness, lastJobId(harness)), 'sent');
  });
});
