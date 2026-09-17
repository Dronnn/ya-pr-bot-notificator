/**
 * workerd requires the global `fetch` to be called with its own receiver:
 * storing the global and invoking it as a property (`obj.fetch(...)`,
 * `this.#fetch(...)`) throws `TypeError: Illegal invocation`. The Telegram
 * adapter surfaced that exception as `transient_network`, so every delivery
 * failed in production until the wiring passed `platformFetch` instead of the
 * global. These tests pin the runtime contract and the production wiring.
 */

import { afterEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { readConfig } from '../src/config.ts';
import { createDeps } from '../src/index.ts';
import { platformFetch } from '../src/platform.ts';

const CONFIG_ENV = {
  TELEGRAM_BOT_TOKEN: '123456789:AAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
  TELEGRAM_WEBHOOK_SECRET: 'secret',
  YANDEX_BASIC_ICAL_URL: 'https://secure.test/basic.ics',
  YANDEX_EXTENDED_ICAL_URL: 'https://secure.test/extended.ics',
};

let restoreFetch: (() => void) | null = null;

afterEach(() => {
  restoreFetch?.();
  restoreFetch = null;
});

/**
 * Replaces the global with a receiver-checking function that mirrors workerd:
 * a bare call resolves, a property call rejects with `Illegal invocation`.
 */
function installReceiverSensitiveFetch(body: unknown): void {
  const original = globalThis.fetch;
  const sensitive = function (this: unknown): Promise<Response> {
    if (this !== undefined) {
      return Promise.reject(new TypeError('Illegal invocation'));
    }
    return Promise.resolve(
      new Response(JSON.stringify(body), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    );
  };
  globalThis.fetch = sensitive as unknown as typeof fetch;
  restoreFetch = () => {
    globalThis.fetch = original;
  };
}

describe('platform fetch wrapper (workerd receiver contract)', () => {
  it('calls the global with the platform receiver, unlike a stored reference', async () => {
    installReceiverSensitiveFetch({ ok: true });

    const response = await platformFetch('https://api.telegram.org/bot0/getMe');
    assert.equal(response.status, 200);

    const stored = { fetch: globalThis.fetch };
    await assert.rejects(
      () => stored.fetch('https://api.telegram.org/bot0/getMe'),
      TypeError,
    );
  });

  it('wires a receiver-safe fetch into the app dependencies and the Telegram client', async () => {
    installReceiverSensitiveFetch({ ok: true, result: { message_id: 42 } });

    const config = readConfig(CONFIG_ENV);
    assert.equal(config.ok, true);
    if (!config.ok) {
      return;
    }

    const env = {
      DB: {
        prepare() {
          throw new Error('unused in this test');
        },
        batch() {
          throw new Error('unused in this test');
        },
      },
      NOTIFICATIONS: {
        sendBatch() {
          throw new Error('unused in this test');
        },
      },
      ...CONFIG_ENV,
    } as unknown as Env;

    const deps = createDeps(env, config.config);

    const response = await deps.fetch('https://secure.test/basic.ics');
    assert.equal(response.status, 200);

    const outcome = await deps.telegram.sendMessage(1, 'ping');
    assert.deepEqual(outcome, { ok: true, messageId: 42 });
  });
});
