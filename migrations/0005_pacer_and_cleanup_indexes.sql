-- 0005_pacer_and_cleanup_indexes.sql
--
-- Finding 5 (rolling pacer): `rate_starts` records one row per reserved Telegram
-- request start. `acquireSendSlot` does a single conditional INSERT guarded by
-- the global 429 cooldown (via `rate_state`) and by the rolling-window count,
-- plus a prune DELETE in the same atomic batch. Window predicate is
-- (now-1000, now]: a start at `now` is admitted only when
-- COUNT(started_at_ms > now-1000 AND started_at_ms <= now) < max. The old
-- fixed-window `rate_state.consumed/window_started_at_ms` columns are retained
-- only for the cooldown + legacy reads; the window itself is sliding.
--
-- Finding 7 (indexed cleanup): every cleanup branch must begin with a selective
-- indexed range and a deterministic indexed LIMIT order. The old
-- `idx_staging_source_age (source_id, updated_at_ms)` forces a full
-- `occurrence_staging` scan for age-only deletes, and the single multi-branch
-- jobs OR-query plus the occurrence `ORDER BY starts_at_ms, id` need temp
-- sorts. New indexes below cover each split branch; the repository splits the
-- jobs OR into three selective statements.

CREATE TABLE IF NOT EXISTS rate_starts (
  started_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rate_starts_started
  ON rate_starts (started_at_ms);

-- Age-ordered staging sweep without a leading source_id: selective range on
-- updated_at_ms with the LIMIT order satisfied by the index.
CREATE INDEX IF NOT EXISTS idx_staging_age
  ON occurrence_staging (updated_at_ms, attempt_id, id);

-- Occurrence retention sweep: range on starts_at_ms with deterministic id
-- tiebreak from the index.
CREATE INDEX IF NOT EXISTS idx_occurrences_cleanup
  ON occurrences (starts_at_ms, id);

-- Per-branch job retention sweeps (the repository issues three separate selective
-- deletes instead of one OR): each branch filters by kind plus an
-- `updated_at_ms` age bound and orders by (updated_at_ms, id) under a LIMIT.
-- One covering index serves all three: the (kind, updated_at_ms) prefix gives
-- a selective range with the LIMIT order straight from the index (verified: no
-- SCAN, no temp b-tree on any branch), while `status` (+ the orphan EXISTS for
-- terminal reminders) filters rows inside that aged range.
CREATE INDEX IF NOT EXISTS idx_jobs_cleanup_kind_age
  ON outbound_jobs (kind, updated_at_ms, id);
