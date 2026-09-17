/**
 * Minimal Telegram Bot API client.
 *
 * Everything is injectable: `fetch`, timeout and logger. The client never logs
 * the token-bearing URL, error bodies or message text; it only reports outcome
 * classes. `allow_paid_broadcast` is always false (plain text, no parse_mode).
 */

import { cancelBody, readBoundedBody } from '../http-body.ts';
import type { Logger } from '../util.ts';
import { clampTelegramText, TELEGRAM_TEXT_LIMIT, TELEGRAM_TIMEOUT_MS } from '../util.ts';

const API_BASE = 'https://api.telegram.org';
/** Hard cap on any Telegram response body; a larger body counts as malformed. */
const MAX_RESPONSE_BYTES = 64 * 1024;
const CALLBACK_TIMEOUT_MS = 3_000;
/** Telegram caps the callback-answer text at 200 characters. */
const CALLBACK_TEXT_LIMIT = 200;
/** Telegram's floor for a 429 backoff: a send is never retried in under a second. */
const MIN_RETRY_AFTER_MS = 1_000;

export interface InlineKeyboardButton {
  text: string;
  callback_data: string;
}

export interface ReplyMarkup {
  inline_keyboard: InlineKeyboardButton[][];
}

export type SendOutcome =
  | { ok: true; messageId: number }
  | { ok: false; kind: 'rate-limit'; retryAfterMs: number }
  | { ok: false; kind: 'forbidden' }
  | { ok: false; kind: 'transient'; code: 'timeout' | 'network' | 'server' | 'malformed' }
  | { ok: false; kind: 'permanent'; status: number };

export interface TelegramClientOptions {
  botToken: string;
  fetch: typeof fetch;
  timeoutMs?: number;
  logger?: Logger;
}

interface RawSuccess {
  kind: 'ok';
  json: unknown;
}

interface RawHttp {
  kind: 'http';
  status: number;
  retryAfterMs: number | null;
}

interface RawNetwork {
  kind: 'network';
  code: 'timeout' | 'network' | 'malformed';
}

/**
 * Client-internal transport vocabulary: every request settles into exactly one
 * of these raw states, and `#classifySendResult` is the only place that turns
 * them into the public `SendOutcome`.
 */
type RawResult = RawSuccess | RawHttp | RawNetwork;

function isAbort(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'name' in error &&
    (error as { name?: unknown }).name === 'AbortError'
  );
}

/** Reads `result.message_id` out of a successful API body, or null if unusable. */
function readMessageId(json: unknown): number | null {
  const body = json as { ok?: unknown; result?: { message_id?: unknown } } | null;
  if (body?.ok !== true) {
    return null;
  }
  const messageId = body.result?.message_id;
  if (typeof messageId !== 'number' || !Number.isSafeInteger(messageId)) {
    return null;
  }
  return messageId;
}

export class TelegramClient {
  readonly #botToken: string;
  readonly #fetch: typeof fetch;
  readonly #timeoutMs: number;
  readonly #logger: Logger | undefined;

  constructor(options: TelegramClientOptions) {
    this.#botToken = options.botToken;
    this.#fetch = options.fetch;
    this.#timeoutMs = options.timeoutMs ?? TELEGRAM_TIMEOUT_MS;
    this.#logger = options.logger;
  }

  async sendMessage(
    chatId: number,
    text: string,
    replyMarkup?: ReplyMarkup,
  ): Promise<SendOutcome> {
    const payload: Record<string, unknown> = {
      chat_id: chatId,
      text: clampTelegramText(text, TELEGRAM_TEXT_LIMIT),
      disable_web_page_preview: true,
      allow_paid_broadcast: false,
    };
    if (replyMarkup !== undefined) {
      payload.reply_markup = replyMarkup;
    }
    const result = await this.#call('sendMessage', payload, this.#timeoutMs);
    return this.#classifySendResult(result);
  }

  /** Callback acknowledgements are best-effort and never retried. */
  async answerCallbackQuery(callbackQueryId: string, text?: string): Promise<boolean> {
    const payload: Record<string, unknown> = { callback_query_id: callbackQueryId };
    if (text !== undefined) {
      payload.text = clampTelegramText(text, CALLBACK_TEXT_LIMIT);
    }
    // Acknowledgements never wait longer than the configured client deadline.
    const result = await this.#call(
      'answerCallbackQuery',
      payload,
      Math.min(CALLBACK_TIMEOUT_MS, this.#timeoutMs),
    );
    if (result.kind === 'ok') {
      const json = result.json as { ok?: unknown } | null;
      return json?.ok === true;
    }
    this.#logger?.debug('telegram_callback_ack_failed', {
      code: result.kind === 'network' ? result.code : 'http',
    });
    return false;
  }

  /**
   * One request under a single deadline that stays active after the headers
   * arrive: the body is read against the abort signal, cancellation is started
   * without awaiting it, and the timer is always cleared. A body-stream failure
   * or deadline is a transient outcome, never a thrown error.
   */
  async #call(method: string, payload: Record<string, unknown>, timeoutMs: number): Promise<RawResult> {
    const controller = new AbortController();
    const timer = setTimeout(() => {
      controller.abort();
    }, timeoutMs);
    try {
      const response = await this.#fetch(`${API_BASE}/bot${this.#botToken}/${method}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });

      if (response.status === 429) {
        const retryAfterMs = await this.#readRetryAfter(response, controller.signal);
        return { kind: 'http', status: 429, retryAfterMs };
      }
      if (!response.ok) {
        cancelBody(response);
        return { kind: 'http', status: response.status, retryAfterMs: null };
      }

      const text = await readBoundedBody(response, MAX_RESPONSE_BYTES, controller.signal);
      if (text === null) {
        return { kind: 'network', code: 'malformed' };
      }
      try {
        return { kind: 'ok', json: JSON.parse(text) as unknown };
      } catch {
        return { kind: 'network', code: 'malformed' };
      }
    } catch (error) {
      return this.#networkResult(method, error);
    } finally {
      clearTimeout(timer);
    }
  }

  async #readRetryAfter(response: Response, signal: AbortSignal): Promise<number> {
    const header = response.headers.get('retry-after');
    let seconds: number | null = null;
    if (header !== null && /^\d+$/.test(header)) {
      seconds = Number(header);
    }
    const text = await readBoundedBody(response, MAX_RESPONSE_BYTES, signal);
    if (text !== null) {
      try {
        const parsed = JSON.parse(text) as { parameters?: { retry_after?: unknown } };
        const retryAfter = parsed.parameters?.retry_after;
        if (typeof retryAfter === 'number' && Number.isFinite(retryAfter) && retryAfter >= 0) {
          seconds = retryAfter;
        }
      } catch {
        // Ignore an unparseable 429 body.
      }
    }
    return Math.max(MIN_RETRY_AFTER_MS, (seconds ?? 0) * 1_000);
  }

  #networkResult(method: string, error: unknown): RawNetwork {
    const code = isAbort(error) ? 'timeout' : 'network';
    this.#logger?.debug('telegram_request_failed', { method, code });
    return { kind: 'network', code };
  }

  /**
   * The single mapping from transport result to `SendOutcome`: network and
   * body failures are transient, HTTP statuses collapse into the rate-limit,
   * forbidden, server and permanent classes.
   */
  #classifySendResult(result: RawResult): SendOutcome {
    switch (result.kind) {
      case 'network':
        return { ok: false, kind: 'transient', code: result.code };
      case 'http': {
        const { status, retryAfterMs } = result;
        if (status === 429) {
          return { ok: false, kind: 'rate-limit', retryAfterMs: retryAfterMs ?? MIN_RETRY_AFTER_MS };
        }
        if (status === 403) {
          return { ok: false, kind: 'forbidden' };
        }
        if (status >= 500) {
          return { ok: false, kind: 'transient', code: 'server' };
        }
        return { ok: false, kind: 'permanent', status };
      }
      case 'ok': {
        const messageId = readMessageId(result.json);
        if (messageId === null) {
          return { ok: false, kind: 'transient', code: 'malformed' };
        }
        return { ok: true, messageId };
      }
    }
  }
}
