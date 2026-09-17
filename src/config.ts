/**
 * Environment parsing. Missing or invalid configuration is reported as
 * variable names only, never values, so a misconfigured deployment cannot leak
 * a token or a private calendar URL into logs or responses.
 */

export interface AppConfig {
  telegramBotToken: string;
  telegramWebhookSecret: string;
  basicIcalUrl: string;
  extendedIcalUrl: string;
}

export interface ConfigReady {
  ok: true;
  config: AppConfig;
}

export interface ConfigMissing {
  ok: false;
  missing: readonly string[];
}

export type ConfigResult = ConfigReady | ConfigMissing;

function readString(source: Record<string, unknown>, key: string): string | null {
  const value = source[key];
  return typeof value === 'string' && value.trim().length > 0 ? value.trim() : null;
}

function isTrustedHttpsUrl(value: string): boolean {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && url.hostname.length > 0;
  } catch {
    return false;
  }
}

/** Tokens and secrets have no format rule beyond being non-empty strings. */
const acceptsAnyString = (): boolean => true;

/**
 * Reads a required variable and applies its validator. Only the variable name
 * is recorded in `missing` - never the value - so a rejection cannot leak a
 * token or a private calendar URL.
 */
function readRequired(
  source: Record<string, unknown>,
  key: string,
  isValid: (value: string) => boolean,
  missing: string[],
): string | null {
  const value = readString(source, key);
  if (value === null || !isValid(value)) {
    missing.push(key);
    return null;
  }
  return value;
}

export function readConfig(env: object | undefined): ConfigResult {
  const source = (env ?? {}) as Record<string, unknown>;
  const missing: string[] = [];

  const telegramBotToken = readRequired(source, 'TELEGRAM_BOT_TOKEN', acceptsAnyString, missing);
  const telegramWebhookSecret = readRequired(
    source,
    'TELEGRAM_WEBHOOK_SECRET',
    acceptsAnyString,
    missing,
  );
  const basicIcalUrl = readRequired(source, 'YANDEX_BASIC_ICAL_URL', isTrustedHttpsUrl, missing);
  const extendedIcalUrl = readRequired(
    source,
    'YANDEX_EXTENDED_ICAL_URL',
    isTrustedHttpsUrl,
    missing,
  );

  if (
    telegramBotToken === null ||
    telegramWebhookSecret === null ||
    basicIcalUrl === null ||
    extendedIcalUrl === null
  ) {
    return { ok: false, missing };
  }

  return {
    ok: true,
    config: { telegramBotToken, telegramWebhookSecret, basicIcalUrl, extendedIcalUrl },
  };
}
