/**
 * Workers entry point. Validates configuration and binding capabilities before
 * constructing the real dependencies, then delegates to the injectable
 * application.
 *
 * The handler contract is checked against the Wrangler-generated platform types
 * (`worker-configuration.d.ts`, produced by `npm run typegen`): the global
 * `Env` binding interface plus the `ExportedHandler` signature. Secrets are
 * merged in from `src/env.d.ts`.
 */

import { IcalJsCalendarParser } from './calendar/icaljs-parser.ts';
import type { SourceDefinition } from './calendar/sync.ts';
import { readConfig, type AppConfig } from './config.ts';
import { Repository } from './data/repository.ts';
import type { OutboundJobMessage } from './platform.ts';
import { platformFetch } from './platform.ts';
import { BOT_COMMANDS } from './telegram/replies.ts';
import { TelegramClient } from './telegram/adapter.ts';
import { createLogger, type Logger } from './util.ts';
import {
  createApp,
  DEFAULT_EVENTS_LIMIT,
  DEFAULT_FETCH_TIMEOUT_MS,
  DEFAULT_SOURCE_TIME_ZONE,
  type AppDeps,
  type WorkerApp,
} from './worker/app.ts';

/** Redacts every config value that carries a secret: tokens and calendar URLs. */
function createRedactingLogger(config: AppConfig, extraSecrets: readonly string[] = []): Logger {
  return createLogger((line) => console.log(line), [
    config.telegramBotToken,
    config.telegramWebhookSecret,
    config.basicIcalUrl,
    config.extendedIcalUrl,
    ...extraSecrets,
  ]);
}

function createCalendarSources(config: AppConfig): SourceDefinition[] {
  return [
    { id: 'basic', kind: 'basic', url: config.basicIcalUrl },
    { id: 'extended', kind: 'extended', url: config.extendedIcalUrl },
  ];
}

function createTelegramClient(config: AppConfig, logger: Logger): TelegramClient {
  return new TelegramClient({
    botToken: config.telegramBotToken,
    fetch: platformFetch,
    logger,
    timeoutMs: DEFAULT_FETCH_TIMEOUT_MS,
  });
}

function isDatabaseBinding(value: unknown): boolean {
  try {
    if (typeof value !== 'object' || value === null) {
      return false;
    }
    const candidate = value as { prepare?: unknown; batch?: unknown };
    return typeof candidate.prepare === 'function' && typeof candidate.batch === 'function';
  } catch {
    return false;
  }
}

function isQueueBinding(value: unknown): boolean {
  try {
    if (typeof value !== 'object' || value === null) {
      return false;
    }
    return typeof (value as { sendBatch?: unknown }).sendBatch === 'function';
  } catch {
    return false;
  }
}

/** A throwing accessor is treated exactly like an absent binding. */
function readBinding(env: Env, name: 'DB' | 'NOTIFICATIONS'): unknown {
  try {
    return env[name];
  } catch {
    return undefined;
  }
}

/**
 * Capability check, not a cast: a structural cast cannot prove a binding works,
 * and a hostile/throwing accessor must degrade to unavailable, never reject.
 */
function missingRuntimeBindings(env: Env): string[] {
  const missing: string[] = [];
  if (!isDatabaseBinding(readBinding(env, 'DB'))) {
    missing.push('DB');
  }
  if (!isQueueBinding(readBinding(env, 'NOTIFICATIONS'))) {
    missing.push('NOTIFICATIONS');
  }
  return missing;
}

export function createDeps(env: Env, config: AppConfig): AppDeps {
  const calendarRefreshSecret =
    typeof env.CALENDAR_REFRESH_SECRET === 'string' && env.CALENDAR_REFRESH_SECRET.length > 0
      ? env.CALENDAR_REFRESH_SECRET
      : null;
  const logger = createRedactingLogger(
    config,
    calendarRefreshSecret === null ? [] : [calendarRefreshSecret],
  );
  return {
    repository: new Repository(env.DB),
    telegram: createTelegramClient(config, logger),
    queue: env.NOTIFICATIONS,
    parser: new IcalJsCalendarParser(),
    fetch: platformFetch,
    now: () => Date.now(),
    logger,
    sources: createCalendarSources(config),
    fetchTimeoutMs: DEFAULT_FETCH_TIMEOUT_MS,
    sourceTimeZone: DEFAULT_SOURCE_TIME_ZONE,
    idFactory: () => crypto.randomUUID(),
    random: Math.random,
    eventsLimit: DEFAULT_EVENTS_LIMIT,
    webhookSecret: config.telegramWebhookSecret,
    calendarRefreshSecret,
  };
}

function createAppFromEnv(env: Env): WorkerApp {
  const config = readConfig(env);
  if (!config.ok) {
    return createApp({ config, deps: null });
  }
  const missingRuntime = missingRuntimeBindings(env);
  if (missingRuntime.length > 0) {
    return createApp({ config, deps: null, missingRuntime });
  }
  return createApp({ config, deps: createDeps(env, config.config) });
}

/**
 * Registers the Telegram command menu once per isolate. The command list only
 * changes with a deploy, so a module flag is enough state; a failed attempt
 * leaves the flag down and the next invocation retries.
 */
let commandsRegistered = false;

async function ensureBotCommands(env: Env): Promise<void> {
  if (commandsRegistered) {
    return;
  }
  const config = readConfig(env);
  if (!config.ok) {
    return;
  }
  try {
    const client = createTelegramClient(config.config, createRedactingLogger(config.config));
    commandsRegistered = await client.setMyCommands(BOT_COMMANDS);
  } catch {
    // Best-effort registration: never affect request or tick processing.
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    ctx.waitUntil(ensureBotCommands(env));
    return createAppFromEnv(env).fetch(request);
  },

  async scheduled(_controller: ScheduledController, env: Env): Promise<void> {
    await ensureBotCommands(env);
    await createAppFromEnv(env).scheduled();
  },

  async queue(batch: MessageBatch<OutboundJobMessage>, env: Env): Promise<void> {
    await createAppFromEnv(env).queue(batch);
  },
} satisfies ExportedHandler<Env, OutboundJobMessage>;
