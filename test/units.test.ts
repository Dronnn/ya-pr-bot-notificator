import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { floatingWallClockToUtcMs } from '../src/calendar/floating-time.ts';
import { readConfig } from '../src/config.ts';
import {
  buildReminderText,
  buildRemindersKeyboard,
  buildRemindersText,
  formatReminderLeadTime,
  formatReminderOffsets,
  offsetLabel,
} from '../src/telegram/replies.ts';
import {
  MAX_TIME_ZONE_LENGTH,
  backoffMs,
  clampTelegramText,
  constantTimeEqual,
  formatUserTime,
  isUsableTimeZoneId,
  normalizeTimeZone,
  redactText,
} from '../src/util.ts';

const TOKEN = '123456789:AAaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

describe('config', () => {
  it('lists missing variables by name only', () => {
    const result = readConfig({ TELEGRAM_BOT_TOKEN: TOKEN });
    assert.equal(result.ok, false);
    if (!result.ok) {
      assert.deepEqual(result.missing, [
        'TELEGRAM_WEBHOOK_SECRET',
        'YANDEX_BASIC_ICAL_URL',
        'YANDEX_EXTENDED_ICAL_URL',
      ]);
    }
  });

  it('rejects non-https source urls', () => {
    const result = readConfig({
      TELEGRAM_BOT_TOKEN: TOKEN,
      TELEGRAM_WEBHOOK_SECRET: 'secret',
      YANDEX_BASIC_ICAL_URL: 'http://insecure.test/basic.ics',
      YANDEX_EXTENDED_ICAL_URL: 'https://secure.test/extended.ics',
    });
    assert.equal(result.ok, false);
    if (!result.ok) {
      assert.deepEqual(result.missing, ['YANDEX_BASIC_ICAL_URL']);
    }
  });

  it('accepts complete valid configuration', () => {
    const result = readConfig({
      TELEGRAM_BOT_TOKEN: TOKEN,
      TELEGRAM_WEBHOOK_SECRET: 'secret',
      YANDEX_BASIC_ICAL_URL: 'https://secure.test/basic.ics',
      YANDEX_EXTENDED_ICAL_URL: 'https://secure.test/extended.ics',
    });
    assert.equal(result.ok, true);
  });
});

describe('floating time resolution', () => {
  it('interprets a floating wall clock in Europe/Moscow', () => {
    const ms = floatingWallClockToUtcMs(
      { year: 2026, month: 9, day: 14, hour: 12, minute: 30, second: 0 },
      'Europe/Moscow',
    );
    assert.equal(ms, Date.UTC(2026, 8, 14, 9, 30, 0));
  });

  it('rejects an unsupported floating source timezone instead of assuming UTC', () => {
    assert.throws(
      () =>
        floatingWallClockToUtcMs(
          { year: 2026, month: 9, day: 14, hour: 12, minute: 30, second: 0 },
          'Asia/Tokyo',
        ),
      /floating time is not supported/,
    );
  });
});

describe('time and text helpers', () => {
  it('renders an explicit timestamp in the requested timezone', () => {
    assert.equal(formatUserTime(0, 'Europe/Moscow'), '01.01.1970 03:00 Europe/Moscow (GMT+3)');
    assert.equal(formatUserTime(0, 'Asia/Yerevan'), '01.01.1970 04:00 Asia/Yerevan (GMT+4)');
  });

  it('renders DST-aware Berlin offsets with fixed Moscow and Yerevan offsets', () => {
    const summer = Date.UTC(2024, 6, 1, 10, 0, 0);
    const winter = Date.UTC(2024, 0, 15, 10, 0, 0);
    assert.equal(formatUserTime(summer, 'Europe/Berlin'), '01.07.2024 12:00 Europe/Berlin (GMT+2)');
    assert.equal(formatUserTime(winter, 'Europe/Berlin'), '15.01.2024 11:00 Europe/Berlin (GMT+1)');
    assert.equal(formatUserTime(summer, 'Europe/Moscow'), '01.07.2024 13:00 Europe/Moscow (GMT+3)');
    assert.equal(formatUserTime(winter, 'Europe/Moscow'), '15.01.2024 13:00 Europe/Moscow (GMT+3)');
    assert.equal(formatUserTime(summer, 'Asia/Yerevan'), '01.07.2024 14:00 Asia/Yerevan (GMT+4)');
    assert.equal(formatUserTime(winter, 'Asia/Yerevan'), '15.01.2024 14:00 Asia/Yerevan (GMT+4)');
    // Western zones keep their negative offsets (README documents `GMT-4`).
    assert.equal(
      formatUserTime(summer, 'America/New_York'),
      '01.07.2024 06:00 America/New_York (GMT-4)',
    );
    assert.equal(
      formatUserTime(winter, 'America/New_York'),
      '15.01.2024 05:00 America/New_York (GMT-5)',
    );
    // Midnight renders as 00:00 (h23), never 24:00.
    assert.equal(
      formatUserTime(Date.UTC(2024, 0, 15, 21), 'Europe/Moscow'),
      '16.01.2024 00:00 Europe/Moscow (GMT+3)',
    );
  });

  it('validates and canonicalizes IANA timezones with native Intl', () => {
    assert.equal(normalizeTimeZone('Europe/Moscow'), 'Europe/Moscow');
    assert.equal(normalizeTimeZone(' Asia/Yerevan '), 'Asia/Yerevan');
    assert.equal(normalizeTimeZone('Europe/Berlin'), 'Europe/Berlin');
    assert.equal(normalizeTimeZone(''), null);
    assert.equal(normalizeTimeZone('   '), null);
    assert.equal(normalizeTimeZone('+03:00'), null);
    assert.equal(normalizeTimeZone('UTC+3'), null);
    assert.equal(normalizeTimeZone('Mars/Phobos'), null);
    assert.equal(normalizeTimeZone('Europe/ Moscow'), null);
    assert.equal(normalizeTimeZone('A'.repeat(MAX_TIME_ZONE_LENGTH + 1)), null);
    // The storable check is directly testable, including the runtime-dependent
    // `Factory` sentinel that Node rejects before the resolved check can run.
    assert.equal(isUsableTimeZoneId('Factory'), false);
    assert.equal(isUsableTimeZoneId('factory'), false);
    assert.equal(isUsableTimeZoneId('+03:00'), false);
    assert.equal(isUsableTimeZoneId('-03:00'), false);
    assert.equal(isUsableTimeZoneId(''), false);
    assert.equal(isUsableTimeZoneId('A'.repeat(MAX_TIME_ZONE_LENGTH + 1)), false);
    assert.equal(isUsableTimeZoneId('Europe/Berlin'), true);
    assert.equal(isUsableTimeZoneId('US/Eastern'), true);
    assert.equal(normalizeTimeZone('Factory'), null);
    // Aliases resolve deterministically to the runtime-canonical identifier.
    assert.equal(normalizeTimeZone('US/Eastern'), 'America/New_York');
    assert.equal(normalizeTimeZone('GMT'), 'UTC');
  });

  it('renders a reminder lead time as Russian hours and minutes', () => {
    assert.equal(formatReminderLeadTime(1), '1 минута');
    assert.equal(formatReminderLeadTime(5), '5 минут');
    assert.equal(formatReminderLeadTime(60), '1 час');
    assert.equal(formatReminderLeadTime(61), '1 час 1 минута');
    assert.equal(formatReminderLeadTime(90), '1 час 30 минут');
    assert.equal(formatReminderLeadTime(121), '2 часа 1 минута');
    assert.equal(formatReminderLeadTime(720), '12 часов');
    assert.equal(formatReminderLeadTime(1440), '24 часа');
    assert.equal(formatReminderLeadTime(11 * 60 + 11), '11 часов 11 минут');
    assert.equal(formatReminderLeadTime(22 * 60 + 22), '22 часа 22 минуты');
    assert.equal(formatReminderLeadTime(43200), '720 часов');
  });

  it('adds the configured lead time to the reminder text', () => {
    const startsAtMs = Date.UTC(2026, 8, 15, 16, 0);
    assert.equal(
      buildReminderText(
        'Работа с сетью',
        startsAtMs,
        'https://example.test/meeting',
        'Europe/Moscow',
        60,
      ),
      [
        'Напоминание: Работа с сетью',
        'До начала события: 1 час',
        'Начало: 15.09.2026 19:00 Europe/Moscow (GMT+3)',
        'https://example.test/meeting',
      ].join('\n'),
    );
    assert.equal(
      buildReminderText('X', startsAtMs, null, 'Europe/Moscow', null),
      ['Напоминание: X', 'Начало: 15.09.2026 19:00 Europe/Moscow (GMT+3)'].join('\n'),
    );
  });

  it('renders the at-start notification without a lead-time line', () => {
    const startsAtMs = Date.UTC(2026, 8, 15, 16, 0);
    assert.equal(
      buildReminderText(
        'Работа с сетью',
        startsAtMs,
        'https://example.test/meeting',
        'Europe/Moscow',
        0,
      ),
      [
        'Занятие начинается: Работа с сетью',
        'Начало: 15.09.2026 19:00 Europe/Moscow (GMT+3)',
        'https://example.test/meeting',
      ].join('\n'),
    );
    assert.equal(
      buildReminderText('X', startsAtMs, null, 'Europe/Moscow', 0),
      ['Занятие начинается: X', 'Начало: 15.09.2026 19:00 Europe/Moscow (GMT+3)'].join('\n'),
    );
  });

  it('labels the at-start offset and keeps the summary clear about it', () => {
    assert.equal(offsetLabel(0), 'в момент начала');
    assert.equal(offsetLabel(5), 'за 5 минут');
    assert.equal(formatReminderOffsets([]), 'выключены');
    assert.equal(formatReminderOffsets([0]), 'в момент начала');
    assert.equal(
      formatReminderOffsets([1440, 60, 5, 0]),
      'за сутки (1440 мин.), за 1 час (60 мин.), за 5 минут (5 мин.), в момент начала',
    );
  });

  it('renders the reminder settings view with the start state', () => {
    const withStart = buildRemindersText([1440, 60, 5, 0]);
    assert.match(withStart, /^Напоминания \(правил: 3\):/);
    assert.match(withStart, /В момент начала: включено/);
    assert.doesNotMatch(withStart, /- в момент начала/);
    assert.match(withStart, /\/reminders start on\|off/);
    assert.match(withStart, /\/reminders add 90/);
    assert.match(withStart, /\/reminders edit 60 90/);
    assert.match(withStart, /\/reminders del 60/);
    assert.match(withStart, /\/reminders clear/);

    assert.match(buildRemindersText([1440]), /В момент начала: выключено/);
    const cleared = buildRemindersText([0]);
    assert.match(cleared, /^Напоминания \(правил: 0\):/);
    assert.doesNotMatch(cleared, /выключены/);
    assert.match(buildRemindersText([]), /Сейчас напоминания выключены/);
  });

  it('puts the start toggle first and never renders it as a custom rule', () => {
    const keyboard = buildRemindersKeyboard([1440, 60, 5, 0]);
    assert.deepEqual(keyboard.inline_keyboard[0], [
      { text: 'В момент начала [вкл]', callback_data: 'rm:t:0' },
    ]);
    assert.equal(
      keyboard.inline_keyboard.some((row) =>
        row.some((button) => button.callback_data === 'rm:del:0' || button.callback_data === 'rm:edit:0'),
      ),
      false,
    );
    const offKeyboard = buildRemindersKeyboard([]);
    assert.deepEqual(offKeyboard.inline_keyboard[0], [
      { text: 'В момент начала [выкл]', callback_data: 'rm:t:0' },
    ]);
    assert.deepEqual(
      offKeyboard.inline_keyboard.map((row) => row.map((button) => button.text)),
      [
        ['В момент начала [выкл]'],
        ['за сутки [выкл]'],
        ['за 1 час [выкл]'],
        ['за 5 минут [выкл]'],
        ['Добавить время'],
      ],
    );
  });

  it('clamps text without splitting surrogate pairs', () => {
    const emoji = '\u{1F600}';
    const text = emoji.repeat(3000);
    const clamped = clampTelegramText(text, 4096);
    assert.ok(clamped.length <= 4096);
    assert.equal(clamped.charCodeAt(clamped.length - 2) >= 0xd800 && clamped.charCodeAt(clamped.length - 2) <= 0xdbff, false);
  });

  it('redacts secrets, tokens and urls', () => {
    const raw = `boom token=${TOKEN} at https://api.telegram.org/bot${TOKEN}/sendMessage`;
    const redacted = redactText(raw, [TOKEN]);
    assert.equal(redacted.includes(TOKEN), false);
    assert.equal(redacted.includes('api.telegram.org'), false);
  });

  it('compares strings in constant time', () => {
    assert.equal(constantTimeEqual('abc', 'abc'), true);
    assert.equal(constantTimeEqual('abc', 'abd'), false);
    assert.equal(constantTimeEqual('abc', 'abcd'), false);
  });

  it('bounds exponential backoff', () => {
    const value = backoffMs(20, () => 1);
    assert.ok(value >= 15 * 60_000);
    assert.ok(value <= 15 * 60_000 * 1.25);
  });
});
