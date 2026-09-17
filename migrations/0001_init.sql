-- Schema for the local Telegram calendar notifier.
-- D1 runs this migration once, in order, per database.

CREATE TABLE users (
  telegram_user_id INTEGER PRIMARY KEY,
  chat_id INTEGER NOT NULL,
  course TEXT NOT NULL DEFAULT 'basic' CHECK (course IN ('basic', 'extended')),
  reminder_offset_minutes INTEGER NOT NULL DEFAULT 30
    CHECK (reminder_offset_minutes IN (30, 1440)),
  active INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0, 1)),
  revision INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE INDEX idx_users_active_course ON users (active, course);

CREATE TABLE sources (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('basic', 'extended')),
  etag TEXT,
  last_modified TEXT,
  fetched_at_ms INTEGER,
  last_success_at_ms INTEGER,
  last_refresh_at_ms INTEGER,
  status TEXT NOT NULL DEFAULT 'unknown'
    CHECK (status IN ('unknown', 'ok', 'error')),
  last_error_code TEXT,
  revision INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE occurrences (
  id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
  uid TEXT NOT NULL,
  occurrence_key TEXT NOT NULL,
  course TEXT NOT NULL CHECK (course IN ('basic', 'extended')),
  starts_at_ms INTEGER NOT NULL,
  ends_at_ms INTEGER,
  summary TEXT NOT NULL,
  description TEXT,
  url TEXT,
  status TEXT NOT NULL DEFAULT 'confirmed'
    CHECK (status IN ('confirmed', 'cancelled')),
  is_all_day INTEGER NOT NULL DEFAULT 0 CHECK (is_all_day IN (0, 1)),
  revision INTEGER NOT NULL DEFAULT 0,
  updated_at_ms INTEGER NOT NULL,
  UNIQUE (source_id, occurrence_key)
);

CREATE INDEX idx_occurrences_source_start
  ON occurrences (source_id, starts_at_ms);
CREATE INDEX idx_occurrences_course_start
  ON occurrences (course, starts_at_ms);

-- Staging area for atomic snapshot publication. A sync writes the full
-- validated snapshot here (chunked, invisible to readers), then publishes it
-- with a single batch so a reader never observes a half-applied replacement.
-- Rows are scoped by `attempt_id` (source plus lease generation), so a stale
-- attempt can never publish, clear or leak into another attempt's snapshot.
CREATE TABLE occurrence_staging (
  attempt_id TEXT NOT NULL,
  id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  uid TEXT NOT NULL,
  occurrence_key TEXT NOT NULL,
  course TEXT NOT NULL,
  starts_at_ms INTEGER NOT NULL,
  ends_at_ms INTEGER,
  summary TEXT NOT NULL,
  description TEXT,
  url TEXT,
  status TEXT NOT NULL,
  is_all_day INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (attempt_id, id)
);

CREATE INDEX idx_staging_source_age ON occurrence_staging (source_id, updated_at_ms);

CREATE TABLE outbound_jobs (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('reminder', 'command')),
  telegram_user_id INTEGER NOT NULL,
  chat_id INTEGER NOT NULL,
  occurrence_id TEXT,
  reminder_offset_minutes INTEGER,
  send_at_ms INTEGER NOT NULL,
  next_attempt_at_ms INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'enqueued', 'leased', 'sent', 'cancelled', 'failed')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  lease_owner TEXT,
  lease_expires_at_ms INTEGER,
  expected_revision INTEGER,
  payload_json TEXT,
  dedup_key TEXT NOT NULL,
  last_error_code TEXT,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_jobs_dedup ON outbound_jobs (dedup_key);
CREATE INDEX idx_jobs_due ON outbound_jobs (status, next_attempt_at_ms);
CREATE INDEX idx_jobs_lease ON outbound_jobs (status, lease_expires_at_ms);
CREATE INDEX idx_jobs_user_status ON outbound_jobs (telegram_user_id, status);
CREATE INDEX idx_jobs_occurrence_status ON outbound_jobs (occurrence_id, status);
CREATE INDEX idx_jobs_cleanup ON outbound_jobs (status, updated_at_ms);

CREATE TABLE processed_updates (
  update_id INTEGER PRIMARY KEY,
  lease_owner TEXT,
  lease_expires_at_ms INTEGER,
  status TEXT NOT NULL DEFAULT 'processing'
    CHECK (status IN ('processing', 'done')),
  processed_at_ms INTEGER
);

CREATE INDEX idx_updates_lease ON processed_updates (status, lease_expires_at_ms);
CREATE INDEX idx_updates_cleanup ON processed_updates (status, processed_at_ms);

-- `generation` increments on every acquisition (including a same-owner renewal),
-- so a specific attempt (owner plus generation) can prove that it is still the
-- current holder instead of trusting a preflight read.
CREATE TABLE locks (
  name TEXT PRIMARY KEY,
  owner TEXT,
  generation INTEGER NOT NULL DEFAULT 0,
  expires_at_ms INTEGER,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE rate_state (
  name TEXT PRIMARY KEY,
  window_started_at_ms INTEGER NOT NULL DEFAULT 0,
  consumed INTEGER NOT NULL DEFAULT 0,
  cooldown_until_ms INTEGER,
  updated_at_ms INTEGER NOT NULL
);

INSERT INTO rate_state (name, window_started_at_ms, consumed, cooldown_until_ms, updated_at_ms)
VALUES ('send', 0, 0, NULL, 0);
