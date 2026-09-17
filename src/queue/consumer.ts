/**
 * Queue consumer. Bounded batch, bounded parallelism, job-lease ownership,
 * paced sends, classified retries. A successful send is persisted before the
 * message is acknowledged.
 *
 * A batch uses rolling admission: at most `MAX_INFLIGHT_SENDS` message tasks
 * run at once, each task owns its message's ack/retry exactly as a
 * single-message delivery does, and a new task starts only while the remaining
 * invocation budget can still absorb its worst case (see `processQueueBatch`).
 * One message is decided in a fixed order: budget admission, lease claim,
 * reminder eligibility, command-supersession, paced send, persisted
 * transition. No outcome is reported that was not persisted under the lease.
 *
 * `attempt_count` counts started Telegram calls only: the atomic
 * `beginSendAttempt` reservation runs immediately before every `sendMessage`
 * (initial send and the single in-place 429 retry alike) and returns a union
 * that the consumer maps exactly: `reserved` sends with the reserved count,
 * `exhausted` fails without sending, `lost` retries the message, and
 * `superseded` cancels as superseded. Pacing, cooldown, budget and
 * lease-repair paths never touch it.
 *
 * Command replies carry a durable per-user order: a reply composed for a
 * subscription revision that has since moved on, or for a Telegram update
 * older than the newest update seen for that user, is terminal `superseded`
 * before a send slot is reserved; the atomic reservation re-checks both
 * guards, so a newer update that lands during the pacing wait still wins.
 *
 * A slot reservation is bounded: a full pace window, or a running cooldown
 * with at most `PACE_MAX_WAIT_MS` left, is slept out while the lease is live
 * and the atomic reservation is re-attempted once, so a short 429 does not
 * park a whole wave until the next tick. A short rate limit additionally gets
 * at most one in-place retry inside the same message processing (see
 * `sendWithRateLimitRetry`): the second real call is counted and its outcome
 * is persisted normally. Only a longer cooldown is deferred to a later
 * delivery.
 *
 * Exactly-once delivery cannot be guaranteed after network uncertainty: a send
 * may succeed while the acknowledgement is lost. The job status in D1 is the
 * source of truth and prevents re-sending, but at-least-once is the honest
 * guarantee.
 */

import type { JobContext, Repository, SendReservation } from '../data/repository.ts';
import { SEND_PACE_WINDOW_MS } from '../data/repository.ts';
import { evaluateDelivery, type Course } from '../domain/notification-policy.ts';
import type { MessageBatchLike, OutboundJobMessage, QueueMessageLike } from '../platform.ts';
import type { TelegramClient, SendOutcome, ReplyMarkup } from '../telegram/adapter.ts';
import { buildReminderText } from '../telegram/replies.ts';
import type { ReplyPayload } from '../telegram/handlers.ts';
import {
  backoffMs,
  BOT_SEND_PACE_PER_SECOND,
  BUDGET_DEFER_RETRY_DELAY_SECONDS,
  JOB_LEASE_MS,
  MAX_D1_STATEMENTS_PER_CONSUMER,
  MAX_JOB_ATTEMPTS,
  MESSAGE_BUDGET_FLOOR,
  MS_PER_MINUTE,
  normalizeTimeZone,
  PACE_MAX_WAIT_MS,
  PACE_RETRY_DELAY_MS,
  STALE_SOURCE_CUTOFF_MS,
  type Clock,
  type Logger,
} from '../util.ts';

export interface ConsumerDeps {
  repository: Repository;
  telegram: TelegramClient;
  now: Clock;
  logger: Logger;
  random: () => number;
  maxAttempts?: number;
  ownerFactory: () => string;
  /** Injected so tests can advance a fake clock instead of sleeping for real. */
  sleep?: (ms: number) => Promise<void>;
}

const defaultSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

/** Maximum Telegram requests one invocation keeps in flight at any time. */
export const MAX_INFLIGHT_SENDS = 5;

export interface ConsumeSummary {
  processed: number;
  sent: number;
  retried: number;
  terminal: number;
  skipped: number;
  deferred: number;
}

/** Result of one delivery attempt; the job state is already persisted. */
type JobOutcome = 'sent' | 'retried' | 'terminal';

/** Batch disposition of one queue message: a job outcome or a non-attempt. */
type MessageOutcome = JobOutcome | 'skipped' | 'deferred';

/**
 * A live reservation: the incremented attempt count plus the recipient's
 * stored timezone read atomically with it. Reminders format immediately before
 * each request from this value, so a zone change during pacing or a 429 wait is
 * picked up by the next reservation instead of rendering a stale zone.
 */
interface ReservedAttempt {
  readonly attempt: number;
  readonly userTimeZone: string | null;
}

/** The reservation's live timezone cannot render a reminder. */
interface UnusableZone {
  readonly kind: 'unusable-zone';
  readonly reason: 'missing' | 'malformed';
}

/** What one prepared request reports: a real outcome or an unusable zone. */
type SendResult = SendOutcome | UnusableZone;

function unusableZone(reason: UnusableZone['reason']): UnusableZone {
  return { kind: 'unusable-zone', reason };
}

function isUnusableZone(value: SendResult): value is UnusableZone {
  return 'kind' in value && value.kind === 'unusable-zone';
}

function isJobOutcome(value: ReservedAttempt | JobOutcome): value is JobOutcome {
  return typeof value === 'string';
}

/**
 * Thrown when a job transition did not persist: the writer lost its lease or
 * the job was already terminal. The message is retried, and the redelivery
 * finds nothing to claim and is acknowledged as skipped. This is what keeps a
 * stale worker from reporting a success that never landed.
 */
export class JobCompletionError extends Error {
  constructor(jobId: string) {
    super(`job ${jobId} completion did not apply (lost ownership or already terminal)`);
    this.name = 'JobCompletionError';
  }
}

/**
 * Awaits a persistence call and throws JobCompletionError when it reports
 * false. Success is never returned for a transition that did not land.
 */
async function assertPersisted(context: JobContext, persisted: Promise<boolean>): Promise<void> {
  if (!(await persisted)) {
    throw new JobCompletionError(context.jobId);
  }
}

/** Persists a terminal (cancelled/failed) transition; throws if the job lost its lease. */
async function finishTerminal(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
  status: 'cancelled' | 'failed',
  code: string,
  now: number,
): Promise<'terminal'> {
  await assertPersisted(
    context,
    deps.repository.finishJobTerminal(context.jobId, owner, status, code, now),
  );
  return 'terminal';
}

/** Returns the job to the pending pool for a later attempt; throws if it lost its lease. */
async function finishRetried(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
  nextAttemptAtMs: number,
  code: string,
  now: number,
): Promise<'retried'> {
  await assertPersisted(
    context,
    deps.repository.rescheduleJob(context.jobId, owner, nextAttemptAtMs, code, now),
  );
  return 'retried';
}

/**
 * Reserves one started Telegram call under the live lease, enforcing the
 * attempt cap atomically with the increment. One statement on success, two
 * (reserve UPDATE plus the classifying probe SELECT) otherwise.
 */
async function reserveSendAttempt(
  deps: ConsumerDeps,
  jobId: string,
  owner: string,
): Promise<SendReservation> {
  const maxAttempts = deps.maxAttempts ?? MAX_JOB_ATTEMPTS;
  return deps.repository.beginSendAttempt(jobId, owner, deps.now(), maxAttempts);
}

/**
 * Maps one reservation to the live attempt (count plus recipient timezone) or
 * to its already-persisted terminal outcome. `exhausted` fails under ownership
 * without sending, `superseded` cancels under ownership without sending, and
 * `lost` throws so the Queue redelivers instead of acknowledging work that
 * never landed.
 */
async function useReservation(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
  reservation: SendReservation,
): Promise<ReservedAttempt | JobOutcome> {
  switch (reservation.status) {
    case 'reserved':
      return { attempt: reservation.attempt, userTimeZone: reservation.userTimeZone };
    case 'exhausted':
      return finishTerminal(deps, context, owner, 'failed', 'attempts-exhausted', deps.now());
    case 'superseded':
      return finishTerminal(deps, context, owner, 'cancelled', 'superseded', deps.now());
    case 'lost':
      throw new JobCompletionError(context.jobId);
  }
}

/**
 * Terminally cancels a reminder whose live stored zone cannot be rendered
 * (`missing` for NULL, `malformed` for a non-null value `Intl` rejects) and
 * refunds the attempt reserved for a request that is never made, so a
 * corrupted row can neither reach `formatUserTime` nor inflate
 * `attempt_count`. The refund is guarded by the exact reserved attempt under
 * the live lease; a lost guard throws so the Queue retries instead of
 * acknowledging a transition that did not land.
 */
async function finishUnusableZone(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
  reservedAttempt: number,
  reason: UnusableZone['reason'],
): Promise<JobOutcome> {
  await assertPersisted(
    context,
    deps.repository.finishJobUnusable(
      context.jobId,
      owner,
      reservedAttempt,
      reason === 'missing' ? 'no-time-zone' : 'invalid-time-zone',
      deps.now(),
    ),
  );
  return 'terminal';
}

/**
 * Per-user command ordering, screened from the claim-time join without an
 * extra statement: a later Telegram update supersedes an older pending,
 * enqueued or already-leased reply. A NULL/0 source id is drainable legacy
 * data and a NULL seen watermark means nothing newer exists, so both stay
 * deliverable; otherwise the job must be at least as new as the watermark.
 * The atomic reservation re-checks the same rule, so a newer update that
 * lands after the claim still wins before any send.
 */
function isSupersededByNewerUpdate(context: JobContext): boolean {
  const source = context.sourceUpdateId;
  const maxSeen = context.commandLastSeenUpdateId;
  return source !== null && source !== 0 && maxSeen !== null && source < maxSeen;
}

/** Name of a thrown error, used as the log code. */
function errorCode(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }
  return 'unknown';
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isReplyMarkup(value: unknown): value is ReplyMarkup {
  if (!isRecord(value) || !Array.isArray(value.inline_keyboard)) {
    return false;
  }
  return value.inline_keyboard.every(
    (row) =>
      Array.isArray(row) &&
      row.every(
        (button) =>
          isRecord(button) &&
          typeof button.text === 'string' &&
          typeof button.callback_data === 'string',
      ),
  );
}

/** Parses a stored reply payload; null when it is missing or malformed. */
function parsePayload(raw: string | null): ReplyPayload | null {
  if (raw === null) {
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw) as unknown;
  } catch {
    return null;
  }
  if (!isRecord(parsed) || typeof parsed.text !== 'string' || parsed.text.length === 0) {
    return null;
  }
  const markup = parsed.replyMarkup;
  if (markup !== undefined && !isReplyMarkup(markup)) {
    return null;
  }
  return markup === undefined ? { text: parsed.text } : { text: parsed.text, replyMarkup: markup };
}

function isJobMessage(body: unknown): body is OutboundJobMessage {
  return isRecord(body) && typeof body.jobId === 'string' && body.jobId.length > 0;
}

/**
 * Persists the transition a send outcome demands. `responseAtMs` is the
 * response-arrival time, never the request time: a slow 429 would otherwise
 * make its own retry_after look expired before it is recorded.
 *
 * A first short rate-limit outcome is handled by `sendWithRateLimitRetry`, so
 * this mapping sees either a non-rate-limit outcome or the outcome of the
 * single in-place retry.
 *
 * | send outcome          | persisted transition                     |
 * | --------------------- | ---------------------------------------- |
 * | ok                    | sent                                     |
 * | forbidden             | deactivate recipient, cancel (forbidden) |
 * | rate-limit, under cap | cooldown, retry (rate_limited)           |
 * | rate-limit, at cap    | cooldown, fail (rate_limited)            |
 * | transient, under cap  | retry with backoff (transient_*)         |
 * | transient, at cap     | fail (transient_*)                       |
 * | permanent             | fail (http_*)                            |
 */
async function persistSendOutcome(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
  outcome: SendOutcome,
  attemptCount: number,
  responseAtMs: number,
): Promise<JobOutcome> {
  if (outcome.ok) {
    await assertPersisted(
      context,
      deps.repository.finishJobSent(context.jobId, owner, responseAtMs),
    );
    return 'sent';
  }
  const maxAttempts = deps.maxAttempts ?? MAX_JOB_ATTEMPTS;
  switch (outcome.kind) {
    case 'forbidden': {
      // Deactivation is durable and the next tick's cancelStaleJobs sweeps the
      // recipient's remaining reservations, so this stays within the statement
      // budget instead of cancelling per message.
      await deps.repository.deactivateUser(context.telegramUserId, responseAtMs);
      await finishTerminal(deps, context, owner, 'cancelled', 'forbidden', responseAtMs);
      deps.logger.warn('recipient_forbidden', { userId: context.telegramUserId });
      return 'terminal';
    }
    case 'rate-limit': {
      const retryAtMs = responseAtMs + outcome.retryAfterMs;
      await deps.repository.setSendCooldown(retryAtMs, responseAtMs);
      if (attemptCount >= maxAttempts) {
        return finishTerminal(deps, context, owner, 'failed', 'rate_limited', responseAtMs);
      }
      return finishRetried(deps, context, owner, retryAtMs, 'rate_limited', responseAtMs);
    }
    case 'transient': {
      const code = `transient_${outcome.code}`;
      if (attemptCount >= maxAttempts) {
        return finishTerminal(deps, context, owner, 'failed', code, responseAtMs);
      }
      return finishRetried(
        deps,
        context,
        owner,
        responseAtMs + backoffMs(attemptCount, deps.random),
        code,
        responseAtMs,
      );
    }
    case 'permanent':
      return finishTerminal(
        deps,
        context,
        owner,
        'failed',
        `http_${outcome.status}`,
        responseAtMs,
      );
  }
}

/**
 * Tries to reserve one global send slot. If the reservation fails, the consumer
 * waits out the bound and re-attempts it exactly once, while it still owns the
 * live lease, so a batch keeps its work instead of parking until the next tick:
 *
 * - an active Telegram-wide cooldown with at most `PACE_MAX_WAIT_MS` left is
 *   slept out for exactly that remainder;
 * - otherwise exactly the remainder of the rolling window is slept out
 *   (`windowStartedAtMs + 1000 - now` from the pace read: the instant the
 *   oldest start in the window expires and one slot frees), because the
 *   sliding counter keeps no reset instant to aim at and a full-window sleep
 *   would idle up to a whole window past the free instant;
 * - a cooldown longer than `PACE_MAX_WAIT_MS` (or a missing pace row, or an
 *   incoherent remainder) is not waited out: the job is rescheduled instead.
 *
 * Costs one statement on a reserved slot, two (failed reservation plus the
 * pace read) on a deferral, three when the wait is taken and the re-attempt
 * runs. The re-attempt is the same atomic `acquireSendSlot` guard, so a wait
 * can never start a request before the cooldown ends or past the pace window.
 * Returns false when no slot was reserved and the job must be deferred.
 */
async function tryReserveSendSlot(deps: ConsumerDeps): Promise<boolean> {
  const startedAt = deps.now();
  if (await deps.repository.acquireSendSlot(startedAt, BOT_SEND_PACE_PER_SECOND)) {
    return true;
  }
  const pace = await deps.repository.getPaceState();
  if (pace === null) {
    return false;
  }
  const cooldownWaitMs = pace.cooldownUntilMs === null ? 0 : pace.cooldownUntilMs - startedAt;
  if (cooldownWaitMs > PACE_MAX_WAIT_MS) {
    return false;
  }
  const waitMs =
    cooldownWaitMs > 0 ? cooldownWaitMs : pace.windowStartedAtMs + SEND_PACE_WINDOW_MS - startedAt;
  if (waitMs <= 0 || waitMs > PACE_MAX_WAIT_MS) {
    return false;
  }
  await (deps.sleep ?? defaultSleep)(waitMs);
  return deps.repository.acquireSendSlot(deps.now(), BOT_SEND_PACE_PER_SECOND);
}

/** A job that cannot reserve a send slot is retried shortly after, not dropped. */
async function deferForPacing(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
): Promise<'retried'> {
  const nextAttemptAtMs = deps.now() + PACE_RETRY_DELAY_MS;
  return finishRetried(deps, context, owner, nextAttemptAtMs, 'paced', deps.now());
}

/**
 * One started call plus its attempt accounting, with at most one in-place
 * rate-limit retry. A 429 whose `retryAfterMs` fits the bounded wait
 * (`<= PACE_MAX_WAIT_MS`) and leaves attempts available is not handed to the
 * next tick: the cooldown is persisted first, the consumer sleeps it out while
 * it still owns the live lease, then the same atomic `acquireSendSlot` guard
 * reserves the retry start - it re-checks the cooldown, so the retry can never
 * start before the cooldown ends, and it counts the start inside the 20/s
 * window. The retry consumes attempts like any real call: a second atomic
 * reservation is checked before re-sending, so an exhausted allowance or a
 * newer command update that landed during the wait settles the job without a
 * second request. A longer cooldown, an exhausted allowance or a pace slot
 * that cannot be reserved immediately keeps the plain reschedule path, and at
 * most one in-place retry happens per delivery.
 *
 * `send` receives the live recipient timezone of the reservation that produced
 * the upcoming request, so a reminder formats immediately before each call
 * (including the retry) from current durable state. Commands ignore the value.
 */
async function sendWithRateLimitRetry(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
  send: (timeZone: string | null) => Promise<SendResult>,
): Promise<JobOutcome> {
  const first = await useReservation(
    deps,
    context,
    owner,
    await reserveSendAttempt(deps, context.jobId, owner),
  );
  if (isJobOutcome(first)) {
    return first;
  }
  return sendAndPersist(deps, context, owner, send, first);
}

/**
 * Sends with a reserved attempt and handles the single in-place rate-limit
 * retry. The retry path re-reserves atomically before re-sending, so it
 * settles (rather than sends) when the allowance ran out or the command was
 * superseded while waiting out the cooldown - and it formats the reminder from
 * the retry reservation's fresh timezone. A reservation whose live zone cannot
 * render a reminder terminates without a request.
 */
async function sendAndPersist(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
  send: (timeZone: string | null) => Promise<SendResult>,
  reserved: ReservedAttempt,
): Promise<JobOutcome> {
  let attempt = reserved;
  let outcome = await send(attempt.userTimeZone);
  let responseAtMs = deps.now();
  const maxAttempts = deps.maxAttempts ?? MAX_JOB_ATTEMPTS;

  if (isUnusableZone(outcome)) {
    return finishUnusableZone(deps, context, owner, attempt.attempt, outcome.reason);
  }
  if (
    !outcome.ok &&
    outcome.kind === 'rate-limit' &&
    outcome.retryAfterMs <= PACE_MAX_WAIT_MS &&
    attempt.attempt < maxAttempts
  ) {
    // Persist the cooldown before waiting so every path observes the same
    // boundary, then reserve the retry start atomically.
    await deps.repository.setSendCooldown(responseAtMs + outcome.retryAfterMs, responseAtMs);
    await (deps.sleep ?? defaultSleep)(outcome.retryAfterMs);
    const slot = await deps.repository.acquireSendSlot(deps.now(), BOT_SEND_PACE_PER_SECOND);
    if (!slot) {
      return finishRetried(
        deps,
        context,
        owner,
        deps.now() + PACE_RETRY_DELAY_MS,
        'rate_limited',
        deps.now(),
      );
    }
    const second = await useReservation(
      deps,
      context,
      owner,
      await reserveSendAttempt(deps, context.jobId, owner),
    );
    if (isJobOutcome(second)) {
      return second;
    }
    attempt = second;
    outcome = await send(attempt.userTimeZone);
    responseAtMs = deps.now();
    if (isUnusableZone(outcome)) {
      return finishUnusableZone(deps, context, owner, attempt.attempt, outcome.reason);
    }
  }
  return persistSendOutcome(deps, context, owner, outcome, attempt.attempt, responseAtMs);
}

/**
 * One command attempt. A command reply is delivery state, not subscription
 * state: it is sent from the persisted payload even when the recipient has no
 * user row (first contact). A reply composed for a subscription revision that
 * has since moved on, or for a Telegram update older than the newest update
 * seen for that user, is terminal `superseded` before a send slot is
 * reserved; the atomic reservation re-checks both guards, so a newer update
 * that lands during the pacing wait still wins.
 */
async function processCommand(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
  now: number,
): Promise<JobOutcome> {
  const payload = parsePayload(context.payloadJson);
  if (payload === null) {
    return finishTerminal(deps, context, owner, 'failed', 'bad-payload', now);
  }
  if (context.expectedRevision !== null && context.expectedRevision !== context.userRevision) {
    return finishTerminal(deps, context, owner, 'cancelled', 'superseded', now);
  }
  if (isSupersededByNewerUpdate(context)) {
    return finishTerminal(deps, context, owner, 'cancelled', 'superseded', now);
  }
  if (!(await tryReserveSendSlot(deps))) {
    return deferForPacing(deps, context, owner);
  }
  return sendWithRateLimitRetry(deps, context, owner, () =>
    deps.telegram.sendMessage(context.chatId, payload.text, payload.replyMarkup),
  );
}

/**
 * Screening result for a claimed reminder: either the job already has a
 * persisted outcome, or the reminder is deliverable from `startsAtMs`.
 */
type ReminderScreening =
  | { readonly kind: 'finished'; readonly outcome: 'retried' | 'terminal' }
  | { readonly kind: 'deliverable'; readonly startsAtMs: number };

/**
 * Reminder eligibility, screened in rule order: recipient active, timezone
 * chosen, offset unchanged, occurrence present and confirmed, source fresh,
 * then the pure delivery decision (course, revision, expiry). A failed rule
 * finishes the job and returns that persisted outcome; otherwise the reminder
 * is deliverable. The timezone rule is defensive: the planner never creates a
 * reminder for incomplete onboarding, so such a job can only exist as stale
 * data, and it is cancelled without a send.
 */
async function screenReminder(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
  now: number,
): Promise<ReminderScreening> {
  if (!context.userFound || !context.userActive) {
    return {
      kind: 'finished',
      outcome: await finishTerminal(deps, context, owner, 'cancelled', 'inactive', now),
    };
  }
  if (context.userTimeZone === null) {
    return {
      kind: 'finished',
      outcome: await finishTerminal(deps, context, owner, 'cancelled', 'no-time-zone', now),
    };
  }
  if (
    context.reminderOffsetMinutes !== null &&
    context.userReminderOffsetMinutes !== null &&
    context.reminderOffsetMinutes !== context.userReminderOffsetMinutes
  ) {
    return {
      kind: 'finished',
      outcome: await finishTerminal(deps, context, owner, 'cancelled', 'skip_offset-mismatch', now),
    };
  }
  if (!context.occurrenceFound || context.occurrenceStartsAtMs === null) {
    return {
      kind: 'finished',
      outcome: await finishTerminal(deps, context, owner, 'cancelled', 'occurrence-missing', now),
    };
  }
  if (context.occurrenceStatus === 'cancelled') {
    return {
      kind: 'finished',
      outcome: await finishTerminal(deps, context, owner, 'cancelled', 'cancelled', now),
    };
  }

  const startsAtMs = context.occurrenceStartsAtMs;
  const sourceIsStale =
    context.sourceFetchedAtMs === null || now - context.sourceFetchedAtMs > STALE_SOURCE_CUTOFF_MS;
  if (sourceIsStale) {
    if (now >= startsAtMs) {
      return {
        kind: 'finished',
        outcome: await finishTerminal(deps, context, owner, 'failed', 'stale-source', now),
      };
    }
    // Re-check the source soon, but never after the event has started.
    return {
      kind: 'finished',
      outcome: await finishRetried(
        deps,
        context,
        owner,
        Math.min(now + MS_PER_MINUTE, startsAtMs),
        'stale-source',
        now,
      ),
    };
  }

  const userCourse: Course = context.userCourse ?? 'basic';
  const eventCourse: Course = context.occurrenceCourse ?? 'basic';
  const decision = evaluateDelivery({
    nowMs: now,
    startsAtMs,
    sendAtMs: context.sendAtMs,
    nextAttemptAtMs: context.nextAttemptAtMs,
    // Inactive and cancelled recipients were screened above with
    // consumer-specific codes, so the policy decides only course, revision,
    // expiry and wait time here.
    active: true,
    userCourse,
    eventCourse,
    cancelled: false,
    expectedRevision: context.expectedRevision ?? 0,
    currentRevision: context.occurrenceRevision ?? 0,
  });

  if (decision.kind === 'wait') {
    return {
      kind: 'finished',
      outcome: await finishRetried(deps, context, owner, decision.retryAtMs, 'wait', now),
    };
  }
  if (decision.kind === 'skip') {
    return {
      kind: 'finished',
      outcome: await finishTerminal(
        deps,
        context,
        owner,
        'cancelled',
        `skip_${decision.reason}`,
        now,
      ),
    };
  }
  if (decision.kind === 'expired') {
    return {
      kind: 'finished',
      outcome: await finishTerminal(deps, context, owner, 'failed', 'expired', now),
    };
  }
  return { kind: 'deliverable', startsAtMs };
}

/**
 * One reminder attempt. Screening decides whether the reminder may be sent;
 * from there the order is fixed: reserve a send slot, re-read the clock, turn
 * a started event into a terminal expiry, then reserve, format and send. The
 * text is built inside the send callback from the timezone returned by that
 * attempt's reservation, so pacing, a 429 wait or a plan-time payload can never
 * render a stale zone; changing the zone never moves the absolute instant.
 */
async function processReminder(
  deps: ConsumerDeps,
  context: JobContext,
  owner: string,
  now: number,
): Promise<JobOutcome> {
  const screening = await screenReminder(deps, context, owner, now);
  if (screening.kind === 'finished') {
    return screening.outcome;
  }
  const { startsAtMs } = screening;

  if (!(await tryReserveSendSlot(deps))) {
    return deferForPacing(deps, context, owner);
  }
  // Reserving a send slot can sleep out a full pace window, so the clock is
  // re-read immediately before the send: at or after the start the reminder is
  // no longer deliverable and becomes a terminal expiry instead.
  const sendNow = deps.now();
  if (sendNow >= startsAtMs) {
    return finishTerminal(deps, context, owner, 'failed', 'expired', sendNow);
  }

  return sendWithRateLimitRetry(deps, context, owner, async (timeZone) => {
    // The reservation returns current durable state. Validate with the same
    // native Intl check as the write path so a corrupted row terminates
    // instead of throwing out of Queue processing.
    const canonical = timeZone === null ? null : normalizeTimeZone(timeZone);
    if (canonical === null) {
      return unusableZone(timeZone === null ? 'missing' : 'malformed');
    }
    return deps.telegram.sendMessage(
      context.chatId,
      buildReminderText(
        context.occurrenceSummary ?? '(no title)',
        startsAtMs,
        context.occurrenceUrl,
        canonical,
      ),
    );
  });
}

/**
 * One delivery attempt: defer when the invocation budget is nearly spent, claim
 * the job under a lease, then run the command or reminder path. The outcome
 * tells the batch loop whether the message can be acknowledged.
 */
export async function processQueueMessage(
  message: QueueMessageLike<OutboundJobMessage>,
  deps: ConsumerDeps,
): Promise<MessageOutcome> {
  const body: unknown = message.body;
  if (!isJobMessage(body)) {
    // A malformed body can never become processable: acknowledge it.
    return 'terminal';
  }
  if (!deps.repository.canAfford(MESSAGE_BUDGET_FLOOR)) {
    // The invocation budget cannot cover this message's worst case. Leave the
    // job untouched and let the batch loop request a short Queue redelivery
    // instead of starting work whose outcome could not be persisted.
    deps.logger.warn('job_deferred_budget', { jobId: body.jobId });
    return 'deferred';
  }
  const owner = deps.ownerFactory();
  const now = deps.now();
  const context = await deps.repository.claimJobContext(body.jobId, owner, now, JOB_LEASE_MS);
  if (context === null) {
    return 'skipped';
  }
  if (context.kind === 'command') {
    return processCommand(deps, context, owner, now);
  }
  return processReminder(deps, context, owner, now);
}

/**
 * Runs a batch under one invocation budget with rolling admission: at most
 * `MAX_INFLIGHT_SENDS` tasks run at once and a new task starts only while the
 * remaining budget can still absorb its worst case.
 *
 * Invariant: before a task starts, `used + reserved + MESSAGE_BUDGET_FLOOR <=
 * limit`, where `reserved` is the summed worst-case cost of the tasks already
 * in flight. Each in-flight task can still execute at most its own reserve, so
 * `used` can never grow past the limit while admitted tasks run: no started
 * task can hit `StatementBudgetError` mid-flight and every started send has
 * room to persist its outcome. A completion releases one full reserve while
 * adding the task's (no larger) actual cost, so later admission checks stay
 * valid and the reserve shrinks as work completes instead of blocking the
 * tail.
 *
 * When no task is in flight and the budget cannot admit the next message, the
 * remainder is deferred with
 * `message.retry({ delaySeconds: BUDGET_DEFER_RETRY_DELAY_SECONDS })`. Budget
 * deferrals are Queue retries, not acknowledgements: the job keeps its durable
 * `enqueued` state and is redelivered after the short delay instead of waiting
 * out the 10-minute scheduler reservation. If the Queue exhausts its retries,
 * lease repair still returns the job to the pending pool, so a deferred
 * message is never lost.
 */
export async function processQueueBatch(
  batch: MessageBatchLike<OutboundJobMessage> | readonly QueueMessageLike<OutboundJobMessage>[],
  deps: ConsumerDeps,
): Promise<ConsumeSummary> {
  deps.repository.beginInvocation(MAX_D1_STATEMENTS_PER_CONSUMER);
  // Once-per-batch GC for the rolling pacer (1 statement): replaces the old
  // per-send prune. Rows deleted here are <= now-1000 and could never satisfy
  // a later admission's `> now'-1000` predicate, so the sliding-window
  // decision stays exact while every send saves one statement.
  await deps.repository.pruneRateStarts(deps.now());
  const messages = Array.isArray(batch)
    ? (batch as readonly QueueMessageLike<OutboundJobMessage>[])
    : (batch as MessageBatchLike<OutboundJobMessage>).messages;
  const summary: ConsumeSummary = {
    processed: 0,
    sent: 0,
    retried: 0,
    terminal: 0,
    skipped: 0,
    deferred: 0,
  };

  const inflight = new Set<Promise<void>>();
  let reserved = 0;
  let next = 0;

  const startTask = (message: QueueMessageLike<OutboundJobMessage>): Promise<void> => {
    const task = (async (): Promise<void> => {
      try {
        const outcome = await processQueueMessage(message, deps);
        summary[outcome] += 1;
        if (outcome === 'deferred') {
          message.retry({ delaySeconds: BUDGET_DEFER_RETRY_DELAY_SECONDS });
        } else {
          message.ack();
        }
      } catch (error) {
        deps.logger.error('job_processing_failed', { code: errorCode(error) });
        message.retry();
      }
    })();
    // Completion releases the task's worst-case reserve. The task never
    // rejects (it handles its own errors), so the cleanup promise is safe.
    void task.finally(() => {
      reserved -= MESSAGE_BUDGET_FLOOR;
      inflight.delete(task);
    });
    return task;
  };

  while (next < messages.length) {
    if (inflight.size >= MAX_INFLIGHT_SENDS) {
      await Promise.race(inflight);
      continue;
    }
    if (!deps.repository.canAfford(reserved + MESSAGE_BUDGET_FLOOR)) {
      if (inflight.size === 0) {
        break;
      }
      // A completing task releases its reserve; re-check the budget then.
      await Promise.race(inflight);
      continue;
    }
    const message = messages[next];
    if (message === undefined) {
      break;
    }
    next += 1;
    summary.processed += 1;
    reserved += MESSAGE_BUDGET_FLOOR;
    inflight.add(startTask(message));
  }
  await Promise.all(inflight);

  for (let index = next; index < messages.length; index += 1) {
    const message = messages[index];
    if (message === undefined) {
      break;
    }
    deps.logger.warn('job_deferred_budget', {
      jobId: isJobMessage(message.body) ? message.body.jobId : undefined,
    });
    summary.deferred += 1;
    message.retry({ delaySeconds: BUDGET_DEFER_RETRY_DELAY_SECONDS });
  }
  return summary;
}
