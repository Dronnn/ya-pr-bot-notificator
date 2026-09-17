/**
 * User-visible copy and inline keyboards. Every timestamp is rendered in the
 * stored user timezone (source events arrive in Europe/Moscow; the absolute
 * instants never change).
 */

import type { OccurrenceView } from '../data/repository.ts';
import type { Course, ReminderOffsetMinutes } from '../domain/notification-policy.ts';
import { formatUserTime } from '../util.ts';
import type { ReplyMarkup } from './adapter.ts';

export const TIMEZONE_KEYBOARD: ReplyMarkup = {
  inline_keyboard: [
    [
      { text: 'Europe/Moscow', callback_data: 'tz:Europe/Moscow' },
      { text: 'Asia/Yerevan', callback_data: 'tz:Asia/Yerevan' },
    ],
  ],
};

export const COURSE_KEYBOARD: ReplyMarkup = {
  inline_keyboard: [
    [
      { text: 'Базовый курс', callback_data: 'course:basic' },
      { text: 'Расширенный курс', callback_data: 'course:extended' },
    ],
  ],
};

export const REMINDER_KEYBOARD: ReplyMarkup = {
  inline_keyboard: [
    [{ text: 'За 30 минут', callback_data: 'reminder:30' }],
    [{ text: 'За сутки', callback_data: 'reminder:1440' }],
  ],
};

/** Settings view: timezone choices next to the existing course/reminder controls. */
export const SETTINGS_KEYBOARD: ReplyMarkup = {
  inline_keyboard: [
    ...TIMEZONE_KEYBOARD.inline_keyboard,
    ...COURSE_KEYBOARD.inline_keyboard,
    ...REMINDER_KEYBOARD.inline_keyboard,
  ],
};

export const START_TEXT = 'Привет! Я присылаю напоминания о занятиях.';

export const TIMEZONE_PROMPT_TEXT = [
  'Выберите часовой пояс, чтобы видеть время занятий по своим часам.',
  'Europe/Moscow или Asia/Yerevan, либо любой другой: /timezone Region/City (например, /timezone Asia/Yerevan)',
].join('\n');

export const TIMEZONE_INVALID_TEXT =
  'Не удалось распознать часовой пояс. Укажите IANA-идентификатор, например /timezone Asia/Yerevan.';

export const HELP_TEXT = `Доступные команды:
/start - начать и выбрать курс
/settings - изменить курс, время напоминания и часовой пояс
/timezone - выбрать или изменить часовой пояс, например /timezone Asia/Yerevan
/events - ближайшие занятия
/stop - отключить напоминания`;

export const STOPPED_TEXT = 'Напоминания отключены. Вернуться можно командой /start.';

/** Answer for a missing or inactive sender: no state change, only a way back. */
export const INACTIVE_GUIDANCE_TEXT =
  'Напоминания не подключены. Отправьте /start, чтобы включить их.';

/**
 * Answer for `/events` without a subscription row: there is no stored timezone
 * to render with and no row the timezone buttons could update, so the guidance
 * explains both required steps without attaching buttons that cannot work.
 */
export const EVENTS_GUIDANCE_TEXT =
  'Напоминания не подключены. Отправьте /start, чтобы подключить их, а затем выберите часовой пояс: без него время занятий не показывается.';

function hasUrl(url: string | null): url is string {
  return url !== null && url.length > 0;
}

export function courseLabel(course: Course): string {
  switch (course) {
    case 'basic':
      return 'Базовый';
    case 'extended':
      return 'Расширенный';
  }
}

export function offsetLabel(offset: ReminderOffsetMinutes): string {
  switch (offset) {
    case 30:
      return 'за 30 минут';
    case 1440:
      return 'за сутки';
  }
}

/** Instruction line shown wherever an existing choice can be changed. */
export const TIMEZONE_CHANGE_HINT =
  'Изменить часовой пояс: /timezone Region/City (например, /timezone Europe/Berlin)';

/**
 * Transport-independent rendering of the current choice; `null` says that
 * onboarding is incomplete and repeats how to finish it.
 */
export function buildTimeZoneText(timeZone: string | null): string {
  if (timeZone === null) {
    return `Часовой пояс не выбран.\n${TIMEZONE_PROMPT_TEXT}`;
  }
  return `Часовой пояс: ${timeZone}\n${TIMEZONE_CHANGE_HINT}`;
}

export function buildSettingsText(
  course: Course,
  offset: ReminderOffsetMinutes,
  timeZone: string | null,
): string {
  const zone = timeZone ?? 'не выбран';
  return `Настройки:\nКурс: ${courseLabel(course)}\nНапоминание: ${offsetLabel(offset)}\nЧасовой пояс: ${zone}`;
}

export function buildCoursePromptText(): string {
  return 'Выберите курс:';
}

export function buildReminderPromptText(course: Course): string {
  return `Курс: ${courseLabel(course)}\nКогда напомнить о занятии?`;
}

export function buildReminderText(
  summary: string,
  startsAtMs: number,
  url: string | null,
  timeZone: string,
): string {
  const lines = [`Напоминание: ${summary}`, `Начало: ${formatUserTime(startsAtMs, timeZone)}`];
  if (hasUrl(url)) {
    lines.push(url);
  }
  return lines.join('\n');
}

export function buildEventsText(
  occurrences: readonly OccurrenceView[],
  course: Course,
  timeZone: string,
): string {
  const label = courseLabel(course);
  if (occurrences.length === 0) {
    return `Ближайших занятий для курса "${label}" нет.`;
  }
  const lines = [`Ближайшие занятия (${label}):`];
  for (const occurrence of occurrences) {
    lines.push(`- ${formatUserTime(occurrence.startsAtMs, timeZone)} - ${occurrence.summary}`);
    if (hasUrl(occurrence.url)) {
      lines.push(`  ${occurrence.url}`);
    }
  }
  return lines.join('\n');
}
