import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  CALLBACK_ACTIONS,
  isAllowedCallback,
  parseUpdate,
} from '../src/telegram/updates.ts';

function message(overrides: Record<string, unknown> = {}): unknown {
  return {
    update_id: 1,
    message: {
      message_id: 10,
      chat: { id: 111, type: 'private' },
      from: { id: 111, username: 'andrew' },
      text: '/start',
      ...overrides,
    },
  };
}

function callback(overrides: Record<string, unknown> = {}): unknown {
  return {
    update_id: 2,
    callback_query: {
      id: 'cb-1',
      from: { id: 111, username: 'andrew' },
      message: { message_id: 5, chat: { id: 111, type: 'private' } },
      data: 'course:basic',
      ...overrides,
    },
  };
}

describe('update parsing', () => {
  it('rejects non-objects and updates without a safe integer id', () => {
    assert.deepEqual(parseUpdate(null), { ok: false, reason: 'shape' });
    assert.deepEqual(parseUpdate('nope'), { ok: false, reason: 'shape' });
    assert.deepEqual(parseUpdate({ update_id: 1.5 }), { ok: false, reason: 'shape' });
    assert.deepEqual(parseUpdate({ message: {} }), { ok: false, reason: 'shape' });
  });

  it('accepts a private text message', () => {
    const result = parseUpdate(message());
    assert.equal(result.ok, true);
    if (result.ok && result.update.kind === 'message') {
      assert.equal(result.update.updateId, 1);
      assert.equal(result.update.chatId, 111);
      assert.equal(result.update.userId, 111);
      assert.equal(result.update.username, 'andrew');
      assert.equal(result.update.text, '/start');
    } else {
      assert.fail('expected a message update');
    }
  });

  it('rejects group chats and non-text messages', () => {
    assert.deepEqual(parseUpdate(message({ chat: { id: 1, type: 'group' } })), {
      ok: false,
      reason: 'not-private',
    });
    assert.deepEqual(parseUpdate(message({ text: undefined })), {
      ok: false,
      reason: 'unsupported',
    });
  });

  it('accepts a private callback query', () => {
    const result = parseUpdate(callback());
    assert.equal(result.ok, true);
    if (result.ok && result.update.kind === 'callback') {
      assert.equal(result.update.callbackQueryId, 'cb-1');
      assert.equal(result.update.data, 'course:basic');
    } else {
      assert.fail('expected a callback update');
    }
  });

  it('rejects callbacks without a message or from a group', () => {
    assert.deepEqual(parseUpdate(callback({ message: undefined })), {
      ok: false,
      reason: 'unsupported',
    });
    assert.deepEqual(
      parseUpdate(callback({ message: { message_id: 5, chat: { id: 1, type: 'supergroup' } } })),
      { ok: false, reason: 'not-private' },
    );
  });

  it('ignores a callback from a different update type', () => {
    assert.deepEqual(parseUpdate({ update_id: 3, edited_message: {} }), {
      ok: false,
      reason: 'unsupported',
    });
  });
});

describe('callback allowlist', () => {
  it('accepts only the finite set of known actions', () => {
    for (const action of CALLBACK_ACTIONS) {
      assert.equal(isAllowedCallback(action), true, action);
    }
    assert.equal(isAllowedCallback('course:admin'), false);
    assert.equal(isAllowedCallback('reminder:60'), false);
    assert.equal(isAllowedCallback('course:basic; DROP TABLE users'), false);
    assert.equal(isAllowedCallback(''), false);
  });

  it('accepts the at-start offset only for the toggle action', () => {
    assert.equal(isAllowedCallback('rm:t:0'), true, 'the start toggle is allowed');
    assert.equal(isAllowedCallback('rm:t:5'), true);
    assert.equal(isAllowedCallback('rm:del:5'), true);
    assert.equal(isAllowedCallback('rm:edit:5'), true);
    assert.equal(isAllowedCallback('rm:del:0'), false, 'delete stays lead-time only');
    assert.equal(isAllowedCallback('rm:edit:0'), false, 'edit stays lead-time only');
  });
});
