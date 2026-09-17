-- 0004_ledger_and_command_state.sql
--
-- Finding 1 (delivery ledger): a successful reminder delivery creates durable
-- identity that time-based cleanup can never reopen. `delivery_ledger` stores
-- one row per delivered logical reminder (dedup_key), separate from the
-- transient `outbound_jobs` rows. `finishJobSent` inserts the ledger row in the
-- same atomic batch as the `sent` transition; `planDueReminders` excludes any
-- candidate whose dedup_key already exists in the ledger. Cleanup never touches
-- this table, so arbitrarily late/repeated movement of the same stable
-- occurrence cannot re-send. Storage tradeoff: exact permanent dedup requires
-- durable memory; one small row per delivered reminder (~<100 bytes + key).
--
-- Finding 4 (command ordering): `user_command_state` holds the highest seen
-- Telegram update_id per user/chat, including users with no subscription row.
-- `insertCommandJob` stores `source_update_id` (nullable for legacy rows);
-- `beginSendAttempt` treats a command as superseded when its stored update id
-- is below the max seen (NULL source + NULL max = deliverable; NULL/0 source
-- legacy = drainable/deliverable).
--
-- Forward-only: never rewrites 0001-0003. Backfills ledger from already-sent
-- reminder rows.

CREATE TABLE IF NOT EXISTS delivery_ledger (
  dedup_key TEXT PRIMARY KEY,
  occurrence_id TEXT,
  telegram_user_id INTEGER,
  sent_at_ms INTEGER NOT NULL
);

INSERT OR IGNORE INTO delivery_ledger (dedup_key, occurrence_id, telegram_user_id, sent_at_ms)
  SELECT dedup_key, occurrence_id, telegram_user_id, updated_at_ms
  FROM outbound_jobs
  WHERE kind = 'reminder' AND status = 'sent';

CREATE TABLE IF NOT EXISTS user_command_state (
  telegram_user_id INTEGER PRIMARY KEY,
  chat_id INTEGER,
  last_update_id INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

-- Nullable for legacy rows (pre-0004). NULL/0 means "no ordering info",
-- treated as drainable/deliverable by the claim/begin guards.
ALTER TABLE outbound_jobs ADD COLUMN source_update_id INTEGER;
