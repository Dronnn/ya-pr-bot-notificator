/**
 * Declaration merge for the Wrangler-generated global `Env` (see
 * `worker-configuration.d.ts`, produced by `npm run typegen`). Secrets are not
 * part of the binding surface Wrangler can infer, so they are declared here.
 *
 * Every value stays optional: the runtime config reader in src/config.ts
 * validates presence, format and trust, and reports only variable names when a
 * value is missing or invalid.
 */
interface Env {
  TELEGRAM_BOT_TOKEN?: string;
  TELEGRAM_WEBHOOK_SECRET?: string;
  CALENDAR_REFRESH_SECRET?: string;
  YANDEX_BASIC_ICAL_URL?: string;
  YANDEX_EXTENDED_ICAL_URL?: string;
}
