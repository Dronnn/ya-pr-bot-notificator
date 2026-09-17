-- 0006_callback_answers.sql
--
-- C2 (duplicate callback answers): answer-only paths (disallowed data,
-- stale/inactive guidance) create no job, so the webhook job-dedup guard
-- (`hasCommandJob`) can never suppress a retry. A forced completion failure
-- followed by a Telegram retry therefore answered twice.
--
-- `callback_answers` records one row per answered callback query. Answer paths
-- insert-or-ignore BEFORE answering and skip when the row already exists, so a
-- retry after a lost completion answers exactly once. Allowed-callback answers
-- that precede an enqueue use the same guard (the job-dedup early return stays
-- as-is for the post-enqueue retry). Cleanup prunes rows older than the caller
-- retention in one bounded statement (see `Repository.cleanup`); the scheduler
-- reserve must cover this 7th statement (see tick.ts CLEANUP_RESERVE_STATEMENTS).
--
-- Forward-only: never rewrites 0001-0005.

CREATE TABLE IF NOT EXISTS callback_answers (
  query_id TEXT PRIMARY KEY,
  update_id INTEGER NOT NULL,
  answered_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_callback_answers_age
  ON callback_answers (answered_at_ms, query_id);
