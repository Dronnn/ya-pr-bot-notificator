/**
 * Command and callback handling.
 *
 * Every user-visible reply goes through `enqueueReply`: it is persisted as a
 * command job and delivered by the same paced pipeline as reminders.
 * `answerCallbackQuery` is the only direct Telegram call; acknowledgements are
 * finite and never retried.
 *
 * `/start` is the only command that activates or reactivates a subscription.
 * Every other command and every allowed callback answers a missing or inactive
 * sender with the inactive guidance reply and leaves the user row, the queued
 * work and the subscription state untouched. Each persisted reply carries the
 * revision it is valid for, so a later activation change supersedes it, plus
 * the source Telegram update_id so the consumer can order first-contact
 * (NULL-revision) guidance by update recency independently of any user row.
 *
 * Webhook-retry idempotency (finding 3): `handleUpdate` returns early when a
 * command job with dedup key `cmd:<updateId>` already exists, skipping every
 * mutation and the enqueue because the reply is already queued. The crash
 * window between mutation and enqueue relies on the revision-idempotent
 * `activateUser` (bumps only on 0→1 or chat change) and on the conditional
 * course/offset writers, so a retry replays safely.
 *
 * Command ordering (finding 4 + C1 + FIX-ORDER): every message command and
 * every allowed active callback claims `claimCommandUpdate` first. The claim is
 * the atomic conditional max-upsert: it returns true iff this update is now the
 * max (updateId >= previous max, first contact included) and only then may the
 * handler mutate, enqueue or answer. A stale (strictly older) update returns
 * silently with no mutation, no enqueue and no answer. Same-update retries
 * still pass (>=).
 *
 * The claim alone is not enough: two invocations may overlap and the older one
 * may be suspended while the newer one completes fully. Every user/job mutation
 * therefore carries the same update id, so its SQL guard (`COMMAND_CURRENT`)
 * rejects it once a newer command is recorded, and the handler re-checks
 * `isCommandUpdateCurrent` immediately before `enqueueReply`: a suspended older
 * invocation can neither overwrite the newer state nor queue stale guidance
 * (the newer command's reply is the authoritative one). Callbacks may still
 * answer the ephemeral `answerCallbackQuery` once through the
 * `callback_answers` dedup, but never mutate or enqueue a stale reply. A
 * concurrent overlap that outlives the newer command settles at the consumer's
 * `sourceUpdateId` guard, so at least the newest reply always survives.
 * Stale/inactive and disallowed callbacks create no job and claim no ordering
 * state, so they can neither reactivate nor suppress current guidance.
 *
 * Callback answers (C2): answer-only paths (disallowed data, stale/inactive
 * guidance) dedup via `callback_answers` (insert-or-ignore before answering,
 * skip when the row exists), so a retry after a lost completion answers once.
 * Allowed-callback answers that precede an enqueue use the same guard while
 * the job-dedup early return stays as-is for the post-enqueue retry.
 *
 * Timezone onboarding: a new `/start` user stays un-onboarded (`time_zone`
 * NULL) and is prompted with the two `tz:` buttons plus `/timezone`
 * instructions; the planner excludes such users, so no reminder exists before
 * the choice. `/timezone Region/City` and the buttons validate with native
 * `Intl` (`normalizeTimeZone`), apply the same guarded mutation and ordering
 * checks as every other command, and never log or echo rejected input. A
 * stopped user is only ever guided back to `/start`; no timezone path
 * reactivates a subscription.
 */

import type { Repository, UserRecord } from '../data/repository.ts';
import type { Course, ReminderOffsetMinutes } from '../domain/notification-policy.ts';
import type { OutboundJobMessage, QueueProducerLike } from '../platform.ts';
import { normalizeTimeZone, type Clock, type Logger } from '../util.ts';
import type { ReplyMarkup, TelegramClient } from './adapter.ts';
import {
  isAllowedCallback,
  type ParsedUpdate,
  type PrivateCallbackUpdate,
  type PrivateMessageUpdate,
} from './updates.ts';
import {
  buildCoursePromptText,
  buildEventsText,
  buildReminderPromptText,
  buildSettingsText,
  buildTimeZoneText,
  EVENTS_GUIDANCE_TEXT,
  HELP_TEXT,
  INACTIVE_GUIDANCE_TEXT,
  REMINDER_KEYBOARD,
  SETTINGS_KEYBOARD,
  START_TEXT,
  STOPPED_TEXT,
  TIMEZONE_CHANGE_HINT,
  TIMEZONE_INVALID_TEXT,
  TIMEZONE_KEYBOARD,
  TIMEZONE_PROMPT_TEXT,
} from './replies.ts';

export interface ReplyPayload {
  text: string;
  replyMarkup?: ReplyMarkup;
}

export interface HandlerDeps {
  repository: Repository;
  telegram: TelegramClient;
  queue: QueueProducerLike<OutboundJobMessage>;
  now: Clock;
  logger: Logger;
  idFactory: () => string;
  eventsLimit: number;
}

const UNKNOWN_ACTION_TEXT = 'Неизвестное действие';

type CallbackIntent =
  | { kind: 'set-course'; course: Course }
  | { kind: 'set-reminder-offset'; offset: ReminderOffsetMinutes }
  | { kind: 'set-time-zone'; timeZone: string };

/** The handled callback buttons, keyed by their exact `callback_data`. */
const CALLBACK_INTENTS: Readonly<Record<string, CallbackIntent>> = {
  'course:basic': { kind: 'set-course', course: 'basic' },
  'course:extended': { kind: 'set-course', course: 'extended' },
  'reminder:30': { kind: 'set-reminder-offset', offset: 30 },
  'reminder:1440': { kind: 'set-reminder-offset', offset: 1440 },
  'tz:Europe/Moscow': { kind: 'set-time-zone', timeZone: 'Europe/Moscow' },
  'tz:Asia/Yerevan': { kind: 'set-time-zone', timeZone: 'Asia/Yerevan' },
};

export async function handleUpdate(update: ParsedUpdate, deps: HandlerDeps): Promise<void> {
  // Finding 3: a retry after a lost completion must not replay effects. When
  // the reply job already exists the first attempt enqueued it (and recorded
  // ordering before that), so skip every mutation and the enqueue and let the
  // caller complete durably.
  if (await deps.repository.hasCommandJob(`cmd:${update.updateId}`)) {
    return;
  }
  if (update.kind === 'message') {
    await handleCommand(deps, update);
    return;
  }
  await handleCallback(deps, update);
  deps.logger.debug('update_handled', { kind: update.kind });
}

/**
 * FIX-ORDER: true while no newer command update is recorded for this user.
 * Every handler checks it immediately before `enqueueReply` so a suspended
 * older invocation never queues guidance the newer command already replaced.
 */
async function isCurrentCommand(deps: HandlerDeps, update: ParsedUpdate): Promise<boolean> {
  return deps.repository.isCommandUpdateCurrent(update.userId, update.updateId);
}

/**
 * The stored zone if it is usable, otherwise null. A missing or malformed
 * durable value is treated as incomplete onboarding everywhere it is shown, so
 * a corrupted row offers the repair controls instead of a zone that reminders
 * ignore. Validity uses the same native `Intl` check as the write path.
 */
function usableTimeZone(user: Pick<UserRecord, 'timeZone'>): string | null {
  return user.timeZone === null ? null : normalizeTimeZone(user.timeZone);
}

/**
 * Settings view shared by `/start`, `/settings` and successful timezone
 * changes: the stored course, offset and timezone plus the timezone choices on
 * the keyboard. An unfinished choice repeats how to complete it; an existing
 * one explains how to change it and continues to the course prompt.
 */
function settingsReplyText(user: Pick<UserRecord, 'course' | 'reminderOffsetMinutes' | 'timeZone'>): string {
  const header = buildSettingsText(user.course, user.reminderOffsetMinutes, user.timeZone);
  if (user.timeZone === null) {
    return `${header}\n${TIMEZONE_PROMPT_TEXT}`;
  }
  return `${header}\n${TIMEZONE_CHANGE_HINT}\n${buildCoursePromptText()}`;
}

async function handleCommand(deps: HandlerDeps, update: PrivateMessageUpdate): Promise<void> {
  const { command, argument } = parseCommand(update.text);
  switch (command) {
    case '/start': {
      // Later /start supersedes an older pending /stop: claim first so a stale
      // update leaves newer state, jobs and answers untouched. Only current
      // guidance survives regardless of queue delivery order. The guarded
      // mutations and the pre-enqueue re-check keep a suspended older /start
      // from reactivating the user or queueing its own superseded reply.
      if (!(await deps.repository.claimCommandUpdate(update.userId, update.chatId, update.updateId, deps.now()))) {
        return;
      }
      await deps.repository.cancelPendingJobsForUser(update.userId, deps.now(), update.updateId);
      const user = await deps.repository.activateUser(
        update.userId,
        update.chatId,
        deps.now(),
        update.updateId,
      );
      if (user === null || !(await isCurrentCommand(deps, update))) {
        return;
      }
      const timeZone = usableTimeZone(user);
      if (timeZone === null) {
        // New or corrupted: onboarding is incomplete until a zone is chosen.
        await enqueueReply(
          deps,
          update,
          {
            text: `${START_TEXT}\n${TIMEZONE_PROMPT_TEXT}`,
            replyMarkup: TIMEZONE_KEYBOARD,
          },
          user.revision,
        );
        return;
      }
      await enqueueReply(
        deps,
        update,
        {
          text: `${START_TEXT}\n${buildSettingsText(user.course, user.reminderOffsetMinutes, timeZone)}`,
          replyMarkup: SETTINGS_KEYBOARD,
        },
        user.revision,
      );
      return;
    }
    case '/settings': {
      if (!(await deps.repository.claimCommandUpdate(update.userId, update.chatId, update.updateId, deps.now()))) {
        return;
      }
      const user = await deps.repository.getUser(update.userId);
      if (!(await isCurrentCommand(deps, update))) {
        return;
      }
      if (user === null || !user.active) {
        // No activation, no job mutation: only point the sender back to /start.
        await enqueueReply(deps, update, { text: INACTIVE_GUIDANCE_TEXT }, user?.revision ?? null);
        return;
      }
      await enqueueReply(
        deps,
        update,
        {
          text: settingsReplyText({ ...user, timeZone: usableTimeZone(user) }),
          replyMarkup: SETTINGS_KEYBOARD,
        },
        user.revision,
      );
      return;
    }
    case '/timezone': {
      // Ordering guard first, exactly like every other command.
      if (!(await deps.repository.claimCommandUpdate(update.userId, update.chatId, update.updateId, deps.now()))) {
        return;
      }
      const user = await deps.repository.getUser(update.userId);
      if (!(await isCurrentCommand(deps, update))) {
        return;
      }
      if (user === null || !user.active) {
        // /stop stays authoritative: a timezone command never reactivates.
        await enqueueReply(deps, update, { text: INACTIVE_GUIDANCE_TEXT }, user?.revision ?? null);
        return;
      }
      if (argument.length === 0) {
        await enqueueReply(
          deps,
          update,
          { text: buildTimeZoneText(usableTimeZone(user)), replyMarkup: TIMEZONE_KEYBOARD },
          user.revision,
        );
        return;
      }
      const timeZone = normalizeTimeZone(argument);
      if (timeZone === null) {
        // Never log the free-form input; only the bounded outcome class.
        deps.logger.debug('time_zone_rejected');
        await enqueueReply(
          deps,
          update,
          { text: TIMEZONE_INVALID_TEXT, replyMarkup: TIMEZONE_KEYBOARD },
          user.revision,
        );
        return;
      }
      await deps.repository.setUserTimeZone(update.userId, timeZone, deps.now(), update.updateId);
      await deps.repository.cancelPendingJobsForUser(update.userId, deps.now(), update.updateId);
      if (!(await isCurrentCommand(deps, update))) {
        return;
      }
      const updated = await deps.repository.getUser(update.userId);
      await enqueueReply(
        deps,
        update,
        {
          text: settingsReplyText(updated ?? { ...user, timeZone }),
          replyMarkup: SETTINGS_KEYBOARD,
        },
        updated?.revision ?? user.revision,
      );
      return;
    }
    case '/events': {
      // Informational: no subscription state changes, but rendering needs the
      // sender's stored timezone, so a sender without one is guided instead of
      // receiving a Moscow-rendered list. A stopped sender with a stored zone
      // still gets the list, exactly as before.
      if (!(await deps.repository.claimCommandUpdate(update.userId, update.chatId, update.updateId, deps.now()))) {
        return;
      }
      const existingUser = await deps.repository.getUser(update.userId);
      if (!(await isCurrentCommand(deps, update))) {
        return;
      }
      if (existingUser === null) {
        // No subscription row: `/start` is required first and the timezone
        // choice follows there, so no buttons are attached to this guidance.
        await enqueueReply(deps, update, { text: EVENTS_GUIDANCE_TEXT }, null);
        return;
      }
      // A missing or malformed durable zone gets the same timezone guidance:
      // neither can render, and the buttons can repair a corrupted row. The
      // validation above is the only Intl use, so `formatUserTime` never sees
      // a corrupt value.
      const timeZone = usableTimeZone(existingUser);
      if (timeZone === null) {
        await enqueueReply(
          deps,
          update,
          { text: TIMEZONE_PROMPT_TEXT, replyMarkup: TIMEZONE_KEYBOARD },
          existingUser.revision,
        );
        return;
      }
      const occurrences = await deps.repository.listUpcomingOccurrences(
        existingUser.course,
        deps.now(),
        deps.eventsLimit,
      );
      if (!(await isCurrentCommand(deps, update))) {
        return;
      }
      await enqueueReply(
        deps,
        update,
        { text: buildEventsText(occurrences, existingUser.course, timeZone) },
        existingUser.revision,
      );
      return;
    }
    case '/stop': {
      // Cancel and deactivate first, then read the post-deactivation revision:
      // only that revision may still surface as current guidance. The guards
      // stop a suspended older /stop from killing the newer command's job or
      // deactivating the user it reactivated.
      if (!(await deps.repository.claimCommandUpdate(update.userId, update.chatId, update.updateId, deps.now()))) {
        return;
      }
      await deps.repository.cancelPendingJobsForUser(update.userId, deps.now(), update.updateId);
      await deps.repository.deactivateUser(update.userId, deps.now(), update.updateId);
      if (!(await isCurrentCommand(deps, update))) {
        return;
      }
      const user = await deps.repository.getUser(update.userId);
      await enqueueReply(deps, update, { text: STOPPED_TEXT }, user?.revision ?? null);
      return;
    }
    default: {
      if (!(await deps.repository.claimCommandUpdate(update.userId, update.chatId, update.updateId, deps.now()))) {
        return;
      }
      const user = await deps.repository.getUser(update.userId);
      if (!(await isCurrentCommand(deps, update))) {
        return;
      }
      await enqueueReply(deps, update, { text: HELP_TEXT }, user?.revision ?? null);
    }
  }
}

async function handleCallback(deps: HandlerDeps, update: PrivateCallbackUpdate): Promise<void> {
  if (!isAllowedCallback(update.data)) {
    if (!(await deps.repository.claimCallbackAnswer(update.callbackQueryId, update.updateId, deps.now()))) {
      return;
    }
    await deps.telegram.answerCallbackQuery(update.callbackQueryId, UNKNOWN_ACTION_TEXT);
    return;
  }

  const user = await deps.repository.getUser(update.userId);
  if (user === null || !user.active) {
    // A stale keyboard from before /stop must not reactivate the subscription.
    if (!(await deps.repository.claimCallbackAnswer(update.callbackQueryId, update.updateId, deps.now()))) {
      return;
    }
    await deps.telegram.answerCallbackQuery(update.callbackQueryId, INACTIVE_GUIDANCE_TEXT);
    return;
  }

  const intent = CALLBACK_INTENTS[update.data];

  if (intent?.kind === 'set-course') {
    const { course } = intent;
    if (!(await deps.repository.claimCommandUpdate(update.userId, update.chatId, update.updateId, deps.now()))) {
      return;
    }
    await deps.repository.setUserCourse(update.userId, course, deps.now(), update.updateId);
    await deps.repository.cancelPendingJobsForUser(update.userId, deps.now(), update.updateId);
    // FIX-ORDER: a callback suspended past a newer command must not apply its
    // setting nor queue its prompt. It may still answer the ephemeral
    // acknowledgement exactly once (the `callback_answers` dedup guarantees it).
    const current = await isCurrentCommand(deps, update);
    const updated = current ? await deps.repository.getUser(update.userId) : null;
    if (await deps.repository.claimCallbackAnswer(update.callbackQueryId, update.updateId, deps.now())) {
      await deps.telegram.answerCallbackQuery(update.callbackQueryId);
    }
    if (!current) {
      return;
    }
    await enqueueReply(
      deps,
      update,
      { text: buildReminderPromptText(course), replyMarkup: REMINDER_KEYBOARD },
      updated?.revision ?? null,
    );
    return;
  }

  if (intent?.kind === 'set-reminder-offset') {
    const { offset } = intent;
    if (!(await deps.repository.claimCommandUpdate(update.userId, update.chatId, update.updateId, deps.now()))) {
      return;
    }
    await deps.repository.setUserReminderOffset(update.userId, offset, deps.now(), update.updateId);
    await deps.repository.cancelPendingJobsForUser(update.userId, deps.now(), update.updateId);
    // FIX-ORDER: guarded exactly like the course callback above.
    const current = await isCurrentCommand(deps, update);
    const updated = current ? await deps.repository.getUser(update.userId) : null;
    if (await deps.repository.claimCallbackAnswer(update.callbackQueryId, update.updateId, deps.now())) {
      await deps.telegram.answerCallbackQuery(update.callbackQueryId);
    }
    if (!current) {
      return;
    }
    await enqueueReply(
      deps,
      update,
      {
        text: buildSettingsText(
          user.course,
          offset,
          updated?.timeZone ?? user.timeZone,
        ),
      },
      updated?.revision ?? null,
    );
    return;
  }

  if (intent?.kind === 'set-time-zone') {
    // Defense-in-depth: the two allowlisted buttons already carry canonical
    // IANA ids, but a timezone that reaches storage is always validated first.
    const timeZone = normalizeTimeZone(intent.timeZone);
    if (timeZone === null) {
      if (await deps.repository.claimCallbackAnswer(update.callbackQueryId, update.updateId, deps.now())) {
        await deps.telegram.answerCallbackQuery(update.callbackQueryId, UNKNOWN_ACTION_TEXT);
      }
      return;
    }
    if (!(await deps.repository.claimCommandUpdate(update.userId, update.chatId, update.updateId, deps.now()))) {
      return;
    }
    await deps.repository.setUserTimeZone(update.userId, timeZone, deps.now(), update.updateId);
    await deps.repository.cancelPendingJobsForUser(update.userId, deps.now(), update.updateId);
    // FIX-ORDER: guarded exactly like the course callback above.
    const current = await isCurrentCommand(deps, update);
    const updated = current ? await deps.repository.getUser(update.userId) : null;
    if (await deps.repository.claimCallbackAnswer(update.callbackQueryId, update.updateId, deps.now())) {
      await deps.telegram.answerCallbackQuery(update.callbackQueryId);
    }
    if (!current) {
      return;
    }
    await enqueueReply(
      deps,
      update,
      {
        text: settingsReplyText(updated ?? { ...user, timeZone }),
        replyMarkup: SETTINGS_KEYBOARD,
      },
      updated?.revision ?? null,
    );
    return;
  }

  if (!(await deps.repository.claimCallbackAnswer(update.callbackQueryId, update.updateId, deps.now()))) {
    return;
  }
  await deps.telegram.answerCallbackQuery(update.callbackQueryId, UNKNOWN_ACTION_TEXT);
}

/**
 * The only path for user-visible text: persist the reply as a command job with
 * the user revision it is valid for and the source Telegram update_id, then
 * wake the queue. Delivery stays with the paced consumer; a reply whose
 * revision is outdated, or whose source update is older than the newest seen
 * command update, is never sent as current guidance. A null revision means
 * "no subscription row yet" and is ordered by update_id instead.
 */
async function enqueueReply(
  deps: HandlerDeps,
  update: ParsedUpdate,
  payload: ReplyPayload,
  expectedRevision: number | null,
): Promise<void> {
  const jobId = deps.idFactory();
  const now = deps.now();
  await deps.repository.insertCommandJob({
    id: jobId,
    telegramUserId: update.userId,
    chatId: update.chatId,
    payloadJson: JSON.stringify(payload),
    dedupKey: `cmd:${update.updateId}`,
    sendAtMs: now,
    now,
    expectedRevision,
    sourceUpdateId: update.updateId,
  });
  await deps.queue.sendBatch([{ body: { jobId } }]);
}

/**
 * Splits a message into the normalized command token (trimmed, lowercased,
 * `@botname` suffix dropped) and the raw argument text (everything after the
 * token, trimmed). The argument stays verbatim: `/timezone` validation is the
 * only place that interprets it.
 */
function parseCommand(text: string): { command: string; argument: string } {
  const trimmed = text.trim();
  const separator = trimmed.search(/\s/);
  const token = separator === -1 ? trimmed : trimmed.slice(0, separator);
  const at = token.indexOf('@');
  return {
    command: (at === -1 ? token : token.slice(0, at)).toLowerCase(),
    argument: separator === -1 ? '' : trimmed.slice(separator).trim(),
  };
}
