import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { floatingWallClockToUtcMs } from '../src/calendar/floating-time.ts';
import { readConfig } from '../src/config.ts';
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
    assert.equal(formatUserTime(0, 'Europe/Moscow'), '1970-01-01 03:00 Europe/Moscow (GMT+3)');
    assert.equal(formatUserTime(0, 'Asia/Yerevan'), '1970-01-01 04:00 Asia/Yerevan (GMT+4)');
  });

  it('renders DST-aware Berlin offsets with fixed Moscow and Yerevan offsets', () => {
    const summer = Date.UTC(2024, 6, 1, 10, 0, 0);
    const winter = Date.UTC(2024, 0, 15, 10, 0, 0);
    assert.equal(formatUserTime(summer, 'Europe/Berlin'), '2024-07-01 12:00 Europe/Berlin (GMT+2)');
    assert.equal(formatUserTime(winter, 'Europe/Berlin'), '2024-01-15 11:00 Europe/Berlin (GMT+1)');
    assert.equal(formatUserTime(summer, 'Europe/Moscow'), '2024-07-01 13:00 Europe/Moscow (GMT+3)');
    assert.equal(formatUserTime(winter, 'Europe/Moscow'), '2024-01-15 13:00 Europe/Moscow (GMT+3)');
    assert.equal(formatUserTime(summer, 'Asia/Yerevan'), '2024-07-01 14:00 Asia/Yerevan (GMT+4)');
    assert.equal(formatUserTime(winter, 'Asia/Yerevan'), '2024-01-15 14:00 Asia/Yerevan (GMT+4)');
    // Western zones keep their negative offsets (README documents `GMT-4`).
    assert.equal(formatUserTime(summer, 'America/New_York'), '2024-07-01 06:00 America/New_York (GMT-4)');
    assert.equal(formatUserTime(winter, 'America/New_York'), '2024-01-15 05:00 America/New_York (GMT-5)');
    // Midnight renders as 00:00 (h23), never 24:00.
    assert.equal(
      formatUserTime(Date.UTC(2024, 0, 15, 21), 'Europe/Moscow'),
      '2024-01-16 00:00 Europe/Moscow (GMT+3)',
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
