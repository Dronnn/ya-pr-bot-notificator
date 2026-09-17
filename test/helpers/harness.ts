/**
 * End-to-end harness: real repository on node:sqlite, real scheduler/consumer,
 * deterministic clock, seeded randomness and a scripted fetch.
 *
 * `consumerDeps`, `syncDeps` and `schedulerDeps` wire that harness into the
 * production dependency shapes the suites invoke.
 */

import { TelegramClient } from '../../src/telegram/adapter.ts';
import { Repository } from '../../src/data/repository.ts';
import type { CalendarParser } from '../../src/calendar/parser.ts';
import type { SourceDefinition, SyncDeps } from '../../src/calendar/sync.ts';
import type { ConfigReady } from '../../src/config.ts';
import type { ParsedCalendar } from '../../src/domain/calendar.ts';
import type { OutboundJobMessage } from '../../src/platform.ts';
import type { ConsumerDeps } from '../../src/queue/consumer.ts';
import type { TickDeps } from '../../src/scheduler/tick.ts';
import type { Clock } from '../../src/util.ts';
import { createApp, type AppDeps, type WorkerApp } from '../../src/worker/app.ts';
import { applyMigrations, createSqliteD1, type SqliteD1 } from './d1-sqlite.ts';
import {
  createCapturedLogger,
  createFetchSpy,
  createParser,
  createQueueSpy,
  createSeededRandom,
  createSequenceIds,
  createTestClock,
  jsonResponse,
  type CapturedLogger,
  type FetchHandler,
  type FetchSpy,
  type QueueSpy,
  type TestClock,
} from './fakes.ts';

export const TEST_BOT_TOKEN = '123456789:AAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
export const TEST_WEBHOOK_SECRET = 'webhook-secret-value';

export interface Harness {
  db: SqliteD1;
  repository: Repository;
  telegram: TelegramClient;
  queue: QueueSpy<OutboundJobMessage>;
  clock: TestClock;
  now: Clock;
  logger: CapturedLogger;
  fetchSpy: FetchSpy;
  parser: CalendarParser;
  setParser(calendar: ParsedCalendar): void;
  setHandler(handler: FetchHandler): void;
  idFactory(): string;
  random(): number;
}

export interface HarnessOptions {
  /** Initial clock value; defaults to `DEFAULT_NOW_MS`. */
  now?: number;
  /** Initial fetch handler; defaults to a successful Telegram send. */
  handler?: FetchHandler;
}

/** 2023-11-14T22:13:20Z, the clock start shared by most suites. */
const DEFAULT_NOW_MS = 1_700_000_000_000;

/** A fresh isolated harness: migrated in-memory D1, real repository, spies. */
export function createHarness(options: HarnessOptions = {}): Harness {
  const db = createSqliteD1();
  applyMigrations(db);
  const repository = new Repository(db);
  const clock = createTestClock(options.now ?? DEFAULT_NOW_MS);
  const now: Clock = () => clock.now();
  const logger = createCapturedLogger();
  let currentHandler: FetchHandler =
    options.handler ?? (() => jsonResponse({ ok: true, result: { message_id: 1 } }));
  const fetchSpy = createFetchSpy((url, init) => currentHandler(url, init));
  const telegram = new TelegramClient({
    botToken: TEST_BOT_TOKEN,
    fetch: fetchSpy.fetch,
    logger: logger.logger,
    timeoutMs: 500,
  });
  const queue = createQueueSpy<OutboundJobMessage>();
  const ids = createSequenceIds('id');
  const random = createSeededRandom(1234);

  let parser: CalendarParser = createParser({ events: [] });

  return {
    db,
    repository,
    telegram,
    queue,
    clock,
    now,
    logger,
    fetchSpy,
    get parser(): CalendarParser {
      return parser;
    },
    setParser(calendar: ParsedCalendar): void {
      parser = createParser(calendar);
    },
    setHandler(handler: FetchHandler): void {
      currentHandler = handler;
      fetchSpy.calls.length = 0;
    },
    idFactory: () => ids(),
    random,
  };
}

/**
 * The real worker app wired to `harness` with fixed test config and the given
 * source definitions. Requests are built with `webhookRequest`.
 */
export function buildTestApp(
  harness: Harness,
  sources: SourceDefinition[] = [],
): WorkerApp {
  const config: ConfigReady = {
    ok: true,
    config: {
      telegramBotToken: TEST_BOT_TOKEN,
      telegramWebhookSecret: TEST_WEBHOOK_SECRET,
      basicIcalUrl: 'https://example.test/basic.ics',
      extendedIcalUrl: 'https://example.test/extended.ics',
    },
  };
  const deps: AppDeps = {
    repository: harness.repository,
    telegram: harness.telegram,
    queue: harness.queue.producer,
    parser: harness.parser,
    fetch: harness.fetchSpy.fetch,
    now: harness.now,
    logger: harness.logger.logger,
    sources,
    fetchTimeoutMs: 1_000,
    sourceTimeZone: 'Europe/Moscow',
    idFactory: harness.idFactory,
    random: harness.random,
    eventsLimit: 10,
    webhookSecret: TEST_WEBHOOK_SECRET,
  };
  return createApp({ config, deps });
}

/**
 * Production consumer dependencies wired to the harness; `overrides` replaces
 * individual fields such as the lease owner or the Telegram client.
 */
export function consumerDeps(
  harness: Harness,
  overrides: Partial<ConsumerDeps> = {},
): ConsumerDeps {
  return {
    repository: harness.repository,
    telegram: harness.telegram,
    now: harness.now,
    logger: harness.logger.logger,
    random: harness.random,
    ownerFactory: harness.idFactory,
    sleep: (ms) => harness.clock.sleep(ms),
    ...overrides,
  };
}

/**
 * Production sync dependencies wired to the harness; `overrides` replaces
 * individual fields such as the parser, fetch or timeout.
 */
export function syncDeps(harness: Harness, overrides: Partial<SyncDeps> = {}): SyncDeps {
  return {
    repository: harness.repository,
    parser: harness.parser,
    fetch: harness.fetchSpy.fetch,
    now: harness.now,
    logger: harness.logger.logger,
    sourceTimeZone: 'Europe/Moscow',
    fetchTimeoutMs: 500,
    ...overrides,
  };
}

/**
 * Production scheduler-tick dependencies wired to the harness; `overrides`
 * replaces individual fields such as the sources or the queue.
 */
export function schedulerDeps(harness: Harness, overrides: Partial<TickDeps> = {}): TickDeps {
  return {
    repository: harness.repository,
    queue: harness.queue.producer,
    now: harness.now,
    logger: harness.logger.logger,
    parser: harness.parser,
    fetch: harness.fetchSpy.fetch,
    sourceTimeZone: 'Europe/Moscow',
    fetchTimeoutMs: 500,
    sources: [],
    ownerFactory: harness.idFactory,
    ...overrides,
  };
}

/** A signed Telegram webhook POST carrying `payload`. */
export function webhookRequest(
  payload: unknown,
  secret: string = TEST_WEBHOOK_SECRET,
): Request {
  return new Request('https://bot.example.test/telegram/webhook', {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-telegram-bot-api-secret-token': secret,
    },
    body: JSON.stringify(payload),
  });
}
