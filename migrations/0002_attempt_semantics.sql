-- 0002_attempt_semantics.sql
--
-- Semantic change to `outbound_jobs.attempt_count`: it now counts Telegram
-- send attempts that were actually started. The counter is incremented
-- atomically by `Repository.beginSendAttempt` immediately before the outbound
-- request, while the consumer still holds the live processing lease.
--
-- Under the old semantics the scheduler reserved a pending job and the consumer
-- claim both incremented the column, so pacing deferrals, global cooldowns,
-- Queue redeliveries and lease repairs could exhaust the retry allowance
-- without a single HTTP request ever being made.
--
-- Forward-only rule: every row that is still going to be attempted
-- (`pending`, `enqueued`, `leased`) restarts with a clean attempt history,
-- because its stored value was produced by lease activity and cannot be
-- reinterpreted as started calls. Terminal rows (`sent`, `cancelled`, `failed`)
-- are left untouched: their counters are historical facts about the old
-- accounting and no retry decision will read them again.
UPDATE outbound_jobs
SET attempt_count = 0
WHERE status IN ('pending', 'enqueued', 'leased');
