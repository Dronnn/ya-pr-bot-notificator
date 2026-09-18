/**
 * Worker application assembled from injected dependencies so the whole HTTP,
 * cron and queue surface is testable without the Workers runtime.
 *
 * Webhook processing is ordered: shared-secret check before the body is read,
 * bounded body read, JSON and update-id validation, lease-based deduplication,
 * then the handler. The update lease is completed once the update is handled
 * (or ignored) and released after a processing failure.
 *
 * Missing configuration and missing runtime bindings both degrade to one
 * unavailable mode: health and webhook answer 503 with the missing names,
 * without exposing any values.
 */

import type { SourceDefinition } from '../calendar/sync.ts';
import type { CalendarParser } from '../calendar/parser.ts';
import type { ConfigResult } from '../config.ts';
import type { Repository } from '../data/repository.ts';
import { readBoundedStream } from '../http-body.ts';
import type { MessageBatchLike, OutboundJobMessage, QueueProducerLike } from '../platform.ts';
import { runSchedulerTick, type TickDeps } from '../scheduler/tick.ts';
import { processQueueBatch } from '../queue/consumer.ts';
import { TelegramClient } from '../telegram/adapter.ts';
import { handleUpdate } from '../telegram/handlers.ts';
import { parseUpdate, type ParsedUpdate } from '../telegram/updates.ts';
import {
  constantTimeEqual,
  MAX_WEBHOOK_BODY_BYTES,
  WEBHOOK_BODY_READ_TIMEOUT_MS,
  WEBHOOK_LEASE_MS,
  type Clock,
  type Logger,
} from '../util.ts';

export interface AppDeps {
  repository: Repository;
  telegram: TelegramClient;
  queue: QueueProducerLike<OutboundJobMessage>;
  parser: CalendarParser;
  fetch: typeof fetch;
  now: Clock;
  logger: Logger;
  sources: readonly SourceDefinition[];
  fetchTimeoutMs: number;
  sourceTimeZone: string;
  idFactory: () => string;
  random: () => number;
  eventsLimit: number;
  webhookSecret: string;
  /** Optional Worker Secret; absent means the manual refresh path is disabled. */
  calendarRefreshSecret: string | null;
}

export interface WorkerApp {
  fetch(request: Request): Promise<Response>;
  scheduled(): Promise<void>;
  queue(batch: MessageBatchLike<OutboundJobMessage>): Promise<void>;
}

const SECRET_HEADER = 'x-telegram-bot-api-secret-token';
const HEALTH_PATH = '/health';
const WEBHOOK_PATH = '/telegram/webhook';

/**
 * Retry delay for every message while the application dependencies are
 * unavailable. The platform must redeliver the work; an unavailable consumer
 * never acknowledges anything it did not process.
 */
const UNAVAILABLE_QUEUE_RETRY_SECONDS = 60;

function json(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function hasValidSecret(request: Request, expectedSecret: string): boolean {
  const providedSecret = request.headers.get(SECRET_HEADER) ?? '';
  return constantTimeEqual(providedSecret, expectedSecret);
}

/** Exact private text match for the optional operator-only calendar refresh. */
function isManualCalendarRefresh(
  update: ParsedUpdate,
  secret: string | null,
): boolean {
  return secret !== null && update.kind === 'message' && constantTimeEqual(update.text, secret);
}

function manualRefreshResultText(deps: AppDeps, statuses: readonly string[]): string {
  const lines = deps.sources.map((source, index) => {
    const status = statuses[index] ?? 'error';
    const label =
      status === 'applied'
        ? 'обновлён'
        : status === 'not-modified'
          ? 'без изменений'
          : status === 'skipped'
            ? 'пропущен'
            : 'ошибка';
    return `${source.id}: ${label}`;
  });
  const successful = statuses.length === deps.sources.length && statuses.every(
    (status) => status === 'applied' || status === 'not-modified',
  );
  return `${successful ? 'Обновление календарей завершено.' : 'Обновление календарей завершено с ошибками.'}\n${lines.join('\n')}`;
}

async function enqueueOperatorReply(
  deps: AppDeps,
  update: ParsedUpdate,
  text: string,
  dedupKey: string,
): Promise<void> {
  const now = deps.now();
  const jobId = deps.idFactory();
  await deps.repository.insertCommandJob({
    id: jobId,
    telegramUserId: update.userId,
    chatId: update.chatId,
    payloadJson: JSON.stringify({ text }),
    dedupKey,
    sendAtMs: now,
    now,
    expectedRevision: null,
    sourceUpdateId: null,
  });
  await deps.queue.sendBatch([{ body: { jobId } }]);
}

function extractUpdateId(payload: unknown): number | null {
  if (typeof payload !== 'object' || payload === null) {
    return null;
  }
  const value = (payload as { update_id?: unknown }).update_id;
  return typeof value === 'number' && Number.isSafeInteger(value) ? value : null;
}

type WebhookRead =
  | { ok: true; payload: unknown; updateId: number }
  | { ok: false; response: Response };

async function readWebhookUpdate(request: Request, logger: Logger): Promise<WebhookRead> {
  const deadline = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    deadline.abort();
  }, WEBHOOK_BODY_READ_TIMEOUT_MS);
  let text: string | null;
  try {
    text = await readBoundedStream(request.body, MAX_WEBHOOK_BODY_BYTES, deadline.signal);
  } catch (error) {
    if (timedOut) {
      return { ok: false, response: json({ status: 'timeout' }, 408) };
    }
    // Only the failure class is logged, never the body or the stream message.
    logger.warn('webhook_body_read_failed', {
      code: error instanceof Error ? error.name : 'unknown',
    });
    return { ok: false, response: json({ status: 'bad_request' }, 400) };
  } finally {
    clearTimeout(timer);
  }
  if (text === null) {
    return { ok: false, response: json({ status: 'too_large' }, 413) };
  }
  let payload: unknown;
  try {
    payload = JSON.parse(text) as unknown;
  } catch {
    return { ok: false, response: json({ status: 'bad_request' }, 400) };
  }
  const updateId = extractUpdateId(payload);
  if (updateId === null) {
    return { ok: false, response: json({ status: 'bad_request' }, 400) };
  }
  return { ok: true, payload, updateId };
}

async function processWebhookUpdate(
  payload: unknown,
  updateId: number,
  deps: AppDeps,
): Promise<Response> {
  const leaseOwner = deps.idFactory();
  const acquisition = await deps.repository.tryBeginUpdate(
    updateId,
    leaseOwner,
    deps.now(),
    WEBHOOK_LEASE_MS,
  );
  if (acquisition === 'done') {
    return json({ status: 'duplicate' }, 200);
  }
  if (acquisition === 'busy') {
    // Another invocation owns a live lease: retryable, and its lease is left
    // untouched. Answering 200 here would make Telegram drop the update.
    return json({ status: 'busy' }, 503);
  }

  const parsed = parseUpdate(payload);
  if (!parsed.ok) {
    let completed = false;
    try {
      completed = await deps.repository.completeUpdate(updateId, leaseOwner, deps.now());
    } catch {
      completed = false;
    }
    if (!completed) {
      // Ownership or durable completion was lost (expiry/takeover/crash):
      // retryable, and the possibly-live owner is left untouched.
      return json({ status: 'busy' }, 503);
    }
    deps.logger.warn('update_rejected', { reason: parsed.reason });
    return json({ status: 'ignored' }, 200);
  }

  try {
    if (isManualCalendarRefresh(parsed.update, deps.calendarRefreshSecret)) {
      await enqueueOperatorReply(
        deps,
        parsed.update,
        'Принял. Начал обновление календарей.',
        `calendar-refresh:${updateId}:accepted`,
      );
      try {
        const result = await runSchedulerTick(schedulerTickDeps(deps), { forceRefresh: true });
        await enqueueOperatorReply(
          deps,
          parsed.update,
          manualRefreshResultText(deps, result.syncStatuses),
          `calendar-refresh:${updateId}:result`,
        );
      } catch {
        await enqueueOperatorReply(
          deps,
          parsed.update,
          'Обновление календарей не удалось завершить.',
          `calendar-refresh:${updateId}:result`,
        );
      }
    } else {
      await handleUpdate(parsed.update, {
        repository: deps.repository,
        telegram: deps.telegram,
        queue: deps.queue,
        now: deps.now,
        logger: deps.logger,
        idFactory: deps.idFactory,
        eventsLimit: deps.eventsLimit,
      });
    }
  } catch {
    await deps.repository.releaseUpdate(updateId, leaseOwner);
    deps.logger.error('update_processing_failed', { updateId });
    return json({ status: 'error' }, 500);
  }
  let completed = false;
  try {
    completed = await deps.repository.completeUpdate(updateId, leaseOwner, deps.now());
  } catch {
    completed = false;
  }
  if (!completed) {
    // The handler effects may have partially completed; without durable
    // completion Telegram must retry. The live owner is never touched here:
    // no release, no second completion attempt.
    return json({ status: 'busy' }, 503);
  }
  return json({ status: 'ok' }, 200);
}

function schedulerTickDeps(deps: AppDeps): TickDeps {
  return {
    repository: deps.repository,
    queue: deps.queue,
    now: deps.now,
    logger: deps.logger,
    parser: deps.parser,
    fetch: deps.fetch,
    sourceTimeZone: deps.sourceTimeZone,
    fetchTimeoutMs: deps.fetchTimeoutMs,
    sources: deps.sources,
    ownerFactory: deps.idFactory,
  };
}

async function handleWebhook(request: Request, deps: AppDeps): Promise<Response> {
  if (!hasValidSecret(request, deps.webhookSecret)) {
    return json({ status: 'forbidden' }, 401);
  }
  const read = await readWebhookUpdate(request, deps.logger);
  if (!read.ok) {
    return read.response;
  }
  return processWebhookUpdate(read.payload, read.updateId, deps);
}

type AppMode =
  | { kind: 'ready'; deps: AppDeps }
  | { kind: 'unavailable'; missing: readonly string[] };

type Route = 'health' | 'webhook' | 'unknown';

function routeFor(request: Request, url: URL): Route {
  if (request.method === 'GET' && url.pathname === HEALTH_PATH) {
    return 'health';
  }
  if (request.method === 'POST' && url.pathname === WEBHOOK_PATH) {
    return 'webhook';
  }
  return 'unknown';
}

function resolveMode(
  config: ConfigResult,
  deps: AppDeps | null,
  missingRuntime: readonly string[],
): AppMode {
  if (!config.ok) {
    return { kind: 'unavailable', missing: config.missing };
  }
  if (deps === null) {
    // Configuration is complete but the runtime bindings are unavailable.
    return { kind: 'unavailable', missing: missingRuntime };
  }
  return { kind: 'ready', deps };
}

async function handleRequest(request: Request, mode: AppMode): Promise<Response> {
  const route = routeFor(request, new URL(request.url));
  if (route === 'unknown') {
    return json({ status: 'not_found' }, 404);
  }
  if (mode.kind === 'unavailable') {
    return json({ status: 'unavailable', missing: mode.missing }, 503);
  }
  if (route === 'health') {
    return json({ status: 'ok' }, 200);
  }
  return handleWebhook(request, mode.deps);
}

export interface CreateAppInput {
  config: ConfigResult;
  deps: AppDeps | null;
  /** Binding names reported by `/health` when `deps` is null. */
  missingRuntime?: readonly string[];
}

export function createApp(input: CreateAppInput): WorkerApp {
  const missingRuntime = input.missingRuntime ?? ['runtime'];
  const mode = resolveMode(input.config, input.deps, missingRuntime);

  return {
    async fetch(request: Request): Promise<Response> {
      return handleRequest(request, mode);
    },

    async scheduled(): Promise<void> {
      if (mode.kind === 'unavailable') {
        // Nothing can run without configuration or runtime bindings.
        return;
      }
      await runSchedulerTick(schedulerTickDeps(mode.deps));
    },

    async queue(batch: MessageBatchLike<OutboundJobMessage>): Promise<void> {
      if (mode.kind === 'unavailable') {
        // Never acknowledge work that was not processed: let the platform
        // redeliver it once the dependencies are available again.
        for (const message of batch.messages) {
          message.retry({ delaySeconds: UNAVAILABLE_QUEUE_RETRY_SECONDS });
        }
        return;
      }
      const deps = mode.deps;
      await processQueueBatch(batch, {
        repository: deps.repository,
        telegram: deps.telegram,
        now: deps.now,
        logger: deps.logger,
        random: deps.random,
        ownerFactory: deps.idFactory,
      });
    },
  };
}

export const DEFAULT_SOURCE_TIME_ZONE = 'Europe/Moscow';
export const DEFAULT_FETCH_TIMEOUT_MS = 15 * 1_000;
export const DEFAULT_EVENTS_LIMIT = 10;
