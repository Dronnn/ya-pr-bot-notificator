/**
 * User-visible copy and inline keyboards. Every timestamp is rendered in the
 * stored user timezone (source events arrive in Europe/Moscow; the absolute
 * instants never change).
 */

import type { OccurrenceView } from '../data/repository.ts';
import {
  DEFAULT_REMINDER_OFFSETS,
  START_REMINDER_OFFSET_MINUTES,
  type Course,
  type ReminderOffsetMinutes,
} from '../domain/notification-policy.ts';
import { formatUserTime } from '../util.ts';
import type { InlineKeyboardMarkup, ReplyKeyboardMarkup } from './adapter.ts';

export const TIMEZONE_KEYBOARD: InlineKeyboardMarkup = {
  inline_keyboard: [
    [
      { text: 'Europe/Moscow', callback_data: 'tz:Europe/Moscow' },
      { text: 'Asia/Yerevan', callback_data: 'tz:Asia/Yerevan' },
    ],
  ],
};

export const COURSE_KEYBOARD: InlineKeyboardMarkup = {
  inline_keyboard: [
    [
      { text: 'Базовый курс', callback_data: 'course:basic' },
      { text: 'Расширенный курс', callback_data: 'course:extended' },
    ],
  ],
};

/** Human label of a lead time, for buttons and the settings summary. */
export function offsetLabel(offset: ReminderOffsetMinutes): string {
  if (offset === START_REMINDER_OFFSET_MINUTES) {
    return 'в момент начала';
  }
  if (offset === 1440) {
    return 'за сутки';
  }
  if (offset === 60) {
    return 'за 1 час';
  }
  if (offset === 5) {
    return 'за 5 минут';
  }
  if (offset % 1440 === 0) {
    return `за ${offset / 1440} дн.`;
  }
  if (offset % 60 === 0) {
    return `за ${offset / 60} ч.`;
  }
  return `за ${offset} мин.`;
}

/** Short one-line summary of the enabled rule set. */
export function formatReminderOffsets(offsets: readonly ReminderOffsetMinutes[]): string {
  if (offsets.length === 0) {
    return 'выключены';
  }
  return offsets
    .map((offset) =>
      offset === START_REMINDER_OFFSET_MINUTES
        ? offsetLabel(offset)
        : `${offsetLabel(offset)} (${offset} мин.)`,
    )
    .join(', ');
}

/**
 * Russian count form for a whole number: `1 час`, `3 часа`, `11 часов`. The
 * last-two-digits check is required because `11..14` take the many-form.
 */
function formatCount(value: number, one: string, few: string, many: string): string {
  const lastTwo = value % 100;
  const last = value % 10;
  if (last === 1 && lastTwo !== 11) {
    return `${value} ${one}`;
  }
  if (last >= 2 && last <= 4 && (lastTwo < 12 || lastTwo > 14)) {
    return `${value} ${few}`;
  }
  return `${value} ${many}`;
}

/**
 * A reminder's configured lead time as whole hours and minutes, e.g.
 * `24 часа`, `1 час 30 минут`, `5 минут`. Zero components are omitted; a rule
 * is at least one minute, so the result is never empty.
 */
export function formatReminderLeadTime(totalMinutes: number): string {
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  const parts: string[] = [];
  if (hours > 0) {
    parts.push(formatCount(hours, 'час', 'часа', 'часов'));
  }
  if (minutes > 0) {
    parts.push(formatCount(minutes, 'минута', 'минуты', 'минут'));
  }
  return parts.join(' ');
}

/**
 * Reminder control keyboard. The at-start notification and the three standard
 * rules are toggle buttons (enabled/disabled in place); custom rules get an
 * edit and a delete button. Callback payloads carry the offset directly and
 * are parsed back by the handler, so only one row is needed per rule.
 */
export function buildRemindersKeyboard(
  offsets: readonly ReminderOffsetMinutes[],
): InlineKeyboardMarkup {
  const enabled = new Set(offsets);
  const rows: { text: string; callback_data: string }[][] = [];
  const startState = enabled.has(START_REMINDER_OFFSET_MINUTES) ? 'вкл' : 'выкл';
  rows.push([{ text: `В момент начала [${startState}]`, callback_data: 'rm:t:0' }]);
  for (const offset of DEFAULT_REMINDER_OFFSETS) {
    const state = enabled.has(offset) ? 'вкл' : 'выкл';
    rows.push([{ text: `${offsetLabel(offset)} [${state}]`, callback_data: `rm:t:${offset}` }]);
  }
  for (const offset of offsets) {
    if (offset === START_REMINDER_OFFSET_MINUTES || DEFAULT_REMINDER_OFFSETS.includes(offset)) {
      continue;
    }
    rows.push([
      { text: `Изменить: ${offsetLabel(offset)}`, callback_data: `rm:edit:${offset}` },
      { text: `Удалить: ${offsetLabel(offset)}`, callback_data: `rm:del:${offset}` },
    ]);
  }
  rows.push([{ text: 'Добавить время', callback_data: 'rm:add' }]);
  if (offsets.some((offset) => offset !== START_REMINDER_OFFSET_MINUTES)) {
    rows.push([{ text: 'Убрать все', callback_data: 'rm:clear' }]);
  }
  return { inline_keyboard: rows };
}

/** Settings view: timezone choices, course controls and the reminder entry point. */
export const SETTINGS_KEYBOARD: InlineKeyboardMarkup = {
  inline_keyboard: [
    ...TIMEZONE_KEYBOARD.inline_keyboard,
    ...COURSE_KEYBOARD.inline_keyboard,
    [{ text: 'Напоминания', callback_data: 'rm:menu' }],
  ],
};

/**
 * Persistent bottom menu. A Telegram message carries either an inline keyboard
 * or a reply keyboard, never both, so this menu rides on the text-only replies
 * (`/help`, `/events`, `/stop`, guidance) and then stays visible for the chat.
 */
export const MENU_KEYBOARD: ReplyKeyboardMarkup = {
  keyboard: [
    [{ text: 'Настройки' }, { text: 'Напоминания' }],
    [{ text: 'Часовой пояс' }, { text: 'Ближайшие занятия' }],
    [{ text: 'Помощь' }],
  ],
  resize_keyboard: true,
  is_persistent: true,
};

/**
 * Reply-keyboard labels resolve to the same commands as their slash forms, so a
 * button tap and a typed command share one handler path.
 */
export const MENU_COMMANDS: ReadonlyMap<string, string> = new Map([
  ['настройки', '/settings'],
  ['напоминания', '/reminders'],
  ['часовой пояс', '/timezone'],
  ['ближайшие занятия', '/events'],
  ['помощь', '/help'],
]);

export function resolveMenuCommand(text: string): string {
  return MENU_COMMANDS.get(text.trim().toLowerCase()) ?? text;
}

/**
 * Bot command list shown in Telegram's command menu button. Registered by the
 * Worker itself via `setMyCommands`, so no manual BotFather step is needed.
 * Telegram requires lowercase `a-z0-9_` names (1-32 chars) and 1-256 char
 * descriptions.
 */
export const BOT_COMMANDS: readonly { command: string; description: string }[] = [
  { command: 'start', description: 'Начать и выбрать курс' },
  { command: 'settings', description: 'Курс, напоминания, часовой пояс' },
  { command: 'reminders', description: 'Настроить время напоминаний' },
  { command: 'timezone', description: 'Выбрать или изменить часовой пояс' },
  { command: 'events', description: 'Ближайшие занятия' },
  { command: 'stop', description: 'Отключить напоминания' },
  { command: 'help', description: 'Помощь' },
];

export const START_TEXT = 'Привет! Я присылаю напоминания о занятиях.';

export const TIMEZONE_PROMPT_TEXT = [
  'Выберите часовой пояс, чтобы видеть время занятий по своим часам.',
  'Europe/Moscow или Asia/Yerevan, либо любой другой: /timezone Region/City (например, /timezone Asia/Yerevan)',
].join('\n');

export const TIMEZONE_INVALID_TEXT =
  'Не удалось распознать часовой пояс. Укажите IANA-идентификатор, например /timezone Asia/Yerevan.';

export const HELP_TEXT = `Доступные команды:
/start - начать и выбрать курс
/settings - изменить курс, напоминания и часовой пояс
/reminders - настроить время напоминаний (add/del/edit/clear/start on|off)
/timezone - выбрать или изменить часовой пояс, например /timezone Asia/Yerevan
/events - ближайшие занятия
/stop - отключить напоминания

Кнопки меню внизу экрана повторяют эти команды.`;

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
  offsets: readonly ReminderOffsetMinutes[],
  timeZone: string | null,
): string {
  const zone = timeZone ?? 'не выбран';
  return `Настройки:\nКурс: ${courseLabel(course)}\nНапоминания: ${formatReminderOffsets(offsets)}\nЧасовой пояс: ${zone}`;
}

export function buildCoursePromptText(): string {
  return 'Выберите курс:';
}

/**
 * Reminder settings view: the at-start notification state, the enabled lead
 * times plus the exact command forms for adding, editing, deleting and
 * clearing rules. The at-start notification is stored as offset 0, is not a
 * lead time and is shown as its own line instead of a rule bullet. The buttons
 * below carry the per-rule actions.
 */
export function buildRemindersText(offsets: readonly ReminderOffsetMinutes[]): string {
  const rules = offsets.filter((offset) => offset !== START_REMINDER_OFFSET_MINUTES);
  const lines = [
    `Напоминания (правил: ${rules.length}):`,
    `В момент начала: ${offsets.includes(START_REMINDER_OFFSET_MINUTES) ? 'включено' : 'выключено'}`,
  ];
  if (offsets.length === 0) {
    lines.push('Сейчас напоминания выключены.');
  } else {
    for (const offset of rules) {
      lines.push(`- ${offsetLabel(offset)} (${offset} мин.)`);
    }
  }
  lines.push(
    'Добавить: /reminders add 90',
    'Изменить: /reminders edit 60 90',
    'Удалить: /reminders del 60',
    'Убрать все: /reminders clear',
    'Уведомление в момент начала: /reminders start on|off',
  );
  return lines.join('\n');
}

export function buildReminderPromptText(
  course: Course,
  offsets: readonly ReminderOffsetMinutes[],
): string {
  return `Курс: ${courseLabel(course)}\n${buildRemindersText(offsets)}`;
}

export const REMINDERS_INVALID_TEXT =
  'Не удалось распознать время. Укажите целое число минут от 1 до 43200, например /reminders add 90.';

export const REMINDERS_ADD_HINT_TEXT =
  'Отправьте /reminders add <минуты>, чтобы добавить правило, например /reminders add 90.';

export const REMINDERS_EDIT_HINT_TEXT =
  'Отправьте /reminders edit <старое> <новое>, чтобы изменить правило, например /reminders edit 60 90.';

export const REMINDERS_LIMIT_TEXT = 'Достигнут максимум числа правил напоминаний.';

export function buildReminderText(
  summary: string,
  startsAtMs: number,
  url: string | null,
  timeZone: string,
  offsetMinutes: number | null,
): string {
  // The at-start notification has no lead time and no "Напоминание:" prefix:
  // it opens with the upcoming start itself.
  const lines =
    offsetMinutes === START_REMINDER_OFFSET_MINUTES
      ? [`Занятие начинается: ${summary}`]
      : [`Напоминание: ${summary}`];
  if (offsetMinutes !== null && offsetMinutes !== START_REMINDER_OFFSET_MINUTES) {
    lines.push(`До начала события: ${formatReminderLeadTime(offsetMinutes)}`);
  }
  lines.push(`Начало: ${formatUserTime(startsAtMs, timeZone)}`);
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
