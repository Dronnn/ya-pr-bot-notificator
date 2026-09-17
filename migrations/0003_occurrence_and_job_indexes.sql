-- 0003_occurrence_and_job_indexes.sql
--
-- Query-plan hardening for the bounded retention and scheduling paths:
--
-- `idx_occurrences_start` gives the cleanup occurrence sweep a start-time
-- range instead of a full `occurrences` scan. The planner already uses the
-- `(course, starts_at_ms)` index, but cleanup cannot: it deletes by
-- `starts_at_ms` alone, across every course and source.
--
-- `idx_jobs_due` gains the `id` tiebreak so the scheduler's `ORDER BY
-- next_attempt_at_ms, id LIMIT` is satisfied by the index instead of a temp
-- b-tree. The old two-column index is replaced in place; `idx_jobs_due` is the
-- only index on that prefix, so no other statement changes plan shape.
CREATE INDEX IF NOT EXISTS idx_occurrences_start ON occurrences (starts_at_ms);

DROP INDEX IF EXISTS idx_jobs_due;
CREATE INDEX IF NOT EXISTS idx_jobs_due
  ON outbound_jobs (status, next_attempt_at_ms, id);
