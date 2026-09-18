/**
 * Operator alerting for silent pipeline failures (cron ticks, queue batches).
 *
 * Alerts are sent directly to the admin's Telegram chat instead of the durable
 * outbox, so a broken queue or a crashed tick cannot swallow them. The same
 * event is suppressed for a cooldown window through a budget-exempt D1 row:
 * a minute-cron that fails every minute must not become a minute-cron of
 * alerts. The text never contains raw configuration: the caller passes the
 * same redaction used by the logger.
 */

import type { Repository } from '../data/repository.ts';
import type { TelegramClient } from '../telegram/adapter.ts';
import { clampTelegramText, type Clock, type Logger } from '../util.ts';

export type AdminAlertEvent = 'scheduled_tick_failed' | 'queue_batch_failed';

export type AdminNotify = (event: AdminAlertEvent, error: unknown) => Promise<void>;

/** One alert per event per hour; the failure has to persist to repeat. */
export const ADMIN_ALERT_COOLDOWN_MS = 60 * 60 * 1000;

const ADMIN_ALERT_TEXT_LIMIT = 500;

const EVENT_TITLES: Record<AdminAlertEvent, string> = {
  scheduled_tick_failed: 'cron-тик упал',
  queue_batch_failed: 'обработка очереди упала',
};

export interface AdminAlertDeps {
  telegram: Pick<TelegramClient, 'sendMessage'>;
  repository: Repository;
  now: Clock;
  logger: Logger;
  adminUserId: number;
  /** Scrubs configured secrets and URLs before the text leaves the Worker. */
  redact: (text: string) => string;
  cooldownMs?: number;
}

function errorSummary(error: unknown): string {
  if (error instanceof Error && error.message.length > 0) {
    return error.message;
  }
  if (typeof error === 'string' && error.length > 0) {
    return error;
  }
  return 'неизвестная ошибка';
}

/**
 * A notifier that never throws: alerting must not mask or replace the original
 * failure, and its own delivery problems are logged and dropped.
 */
export function createAdminNotifier(deps: AdminAlertDeps): AdminNotify {
  const cooldownMs = deps.cooldownMs ?? ADMIN_ALERT_COOLDOWN_MS;
  return async (event, error) => {
    try {
      const allowed = await deps.repository.tryAcquireAdminAlertLease(
        `admin-alert:${event}`,
        deps.now(),
        cooldownMs,
      );
      if (!allowed) {
        deps.logger.debug('admin_alert_suppressed', { alertEvent: event });
        return;
      }
      const text = clampTelegramText(
        `\u26a0\ufe0f Сбой бота: ${EVENT_TITLES[event]}.\nОшибка: ${deps.redact(errorSummary(error))}`,
        ADMIN_ALERT_TEXT_LIMIT,
      );
      const outcome = await deps.telegram.sendMessage(deps.adminUserId, text);
      if (!outcome.ok) {
        deps.logger.warn('admin_alert_send_failed', {
          alertEvent: event,
          code: outcome.kind === 'permanent' ? `http_${outcome.status}` : outcome.kind,
        });
      }
    } catch (alertError) {
      deps.logger.warn('admin_alert_failed', {
        alertEvent: event,
        code: alertError instanceof Error ? alertError.name : 'unknown',
      });
    }
  };
}
