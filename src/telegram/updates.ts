/**
 * Runtime validation for incoming Telegram updates.
 *
 * The parser accepts exactly two shapes: a private-chat text message and a
 * private-chat callback query. Any other input is rejected with a single
 * reason and acknowledged by the caller without side effects.
 */

export interface PrivateMessageUpdate {
  kind: 'message';
  updateId: number;
  messageId: number;
  chatId: number;
  userId: number;
  username: string | null;
  text: string;
}

export interface PrivateCallbackUpdate {
  kind: 'callback';
  updateId: number;
  chatId: number;
  userId: number;
  username: string | null;
  callbackQueryId: string;
  data: string;
}

export type ParsedUpdate = PrivateMessageUpdate | PrivateCallbackUpdate;

/**
 * Either the parsed private-chat update or the one reason it was rejected:
 * `shape` for malformed fields, `not-private` for a well-formed update from a
 * non-private chat, and `unsupported` for a private-chat shape this bot does
 * not handle.
 */
export type ParseUpdateResult =
  | { ok: true; update: ParsedUpdate }
  | { ok: false; reason: 'shape' | 'not-private' | 'unsupported' };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function asSafeInteger(value: unknown): number | null {
  return typeof value === 'number' && Number.isSafeInteger(value) ? value : null;
}

function asNonEmptyString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function readChat(record: Record<string, unknown>): Record<string, unknown> | null {
  const chat = record.chat;
  return isRecord(chat) ? chat : null;
}

function readFrom(record: Record<string, unknown>): Record<string, unknown> | null {
  const from = record.from;
  return isRecord(from) ? from : null;
}

function readUsername(from: Record<string, unknown>): string | null {
  return asNonEmptyString(from.username);
}

function parseMessage(updateId: number, message: Record<string, unknown>): ParseUpdateResult {
  const chat = readChat(message);
  const from = readFrom(message);
  if (chat === null || from === null) {
    return { ok: false, reason: 'shape' };
  }
  if (chat.type !== 'private') {
    return { ok: false, reason: 'not-private' };
  }
  const chatId = asSafeInteger(chat.id);
  const userId = asSafeInteger(from.id);
  if (chatId === null || userId === null) {
    return { ok: false, reason: 'shape' };
  }
  const text = asNonEmptyString(message.text);
  if (text === null) {
    return { ok: false, reason: 'unsupported' };
  }
  return {
    ok: true,
    update: {
      kind: 'message',
      updateId,
      messageId: asSafeInteger(message.message_id) ?? 0,
      chatId,
      userId,
      username: readUsername(from),
      text,
    },
  };
}

function parseCallback(updateId: number, query: Record<string, unknown>): ParseUpdateResult {
  const from = readFrom(query);
  if (from === null) {
    return { ok: false, reason: 'shape' };
  }
  const userId = asSafeInteger(from.id);
  const callbackQueryId = asNonEmptyString(query.id);
  const data = asNonEmptyString(query.data);
  if (userId === null || callbackQueryId === null || data === null) {
    return { ok: false, reason: 'shape' };
  }
  const chat = isRecord(query.message) ? readChat(query.message) : null;
  if (chat === null) {
    return { ok: false, reason: 'unsupported' };
  }
  if (chat.type !== 'private') {
    return { ok: false, reason: 'not-private' };
  }
  const chatId = asSafeInteger(chat.id);
  if (chatId === null) {
    return { ok: false, reason: 'shape' };
  }
  return {
    ok: true,
    update: {
      kind: 'callback',
      updateId,
      chatId,
      userId,
      username: readUsername(from),
      callbackQueryId,
      data,
    },
  };
}

export function parseUpdate(payload: unknown): ParseUpdateResult {
  if (!isRecord(payload)) {
    return { ok: false, reason: 'shape' };
  }
  const updateId = asSafeInteger(payload.update_id);
  if (updateId === null) {
    return { ok: false, reason: 'shape' };
  }

  if (isRecord(payload.message)) {
    return parseMessage(updateId, payload.message);
  }
  if (isRecord(payload.callback_query)) {
    return parseCallback(updateId, payload.callback_query);
  }
  return { ok: false, reason: 'unsupported' };
}

/**
 * The complete, finite set of callback payloads the bot acts on. Anything
 * outside this list is acknowledged as an unknown action.
 */
export const CALLBACK_ACTIONS: readonly string[] = [
  'course:basic',
  'course:extended',
  'reminder:30',
  'reminder:1440',
  'tz:Europe/Moscow',
  'tz:Asia/Yerevan',
];

const ALLOWED_CALLBACKS: ReadonlySet<string> = new Set(CALLBACK_ACTIONS);

/** Exact membership of the allowlist; no prefix or pattern matching. */
export function isAllowedCallback(data: string): boolean {
  return ALLOWED_CALLBACKS.has(data);
}
