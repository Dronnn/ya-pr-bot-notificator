-- 0008_user_reminder_rules.sql
--
-- Replaces the single `users.reminder_offset_minutes` (CHECK 30/1440) with a
-- per-user set of reminder rules. A user may keep zero, one or many lead times;
-- planning produces one job per (user, occurrence, rule) with the offset in the
-- stable dedup key, so a delivered reminder is never re-sent when the set is
-- edited.
--
-- Default for new and existing users: exactly three rules - 24 hours (1440),
-- 1 hour (60) and 5 minutes (5) before the lesson. Existing settings are
-- replaced by this new standard; every user row (active or not) is preserved.
--
-- The offset is bounded at the schema level to [1, 43200] minutes (thirty days):
-- that is the calendar horizon itself, so the planner's due window (the same
-- maximum) never exceeds the materialized occurrences. Zero is rejected because
-- it would coincide with the occurrence start.
--
-- Forward-only: never rewrites 0001-0007. The legacy column is removed by
-- rebuilding `users` (a plain DROP COLUMN is not portable across the runtime's
-- SQLite versions). No table references `users`, so the rebuild is safe.

CREATE TABLE users_new (
  telegram_user_id INTEGER PRIMARY KEY,
  chat_id INTEGER NOT NULL,
  course TEXT NOT NULL DEFAULT 'basic' CHECK (course IN ('basic', 'extended')),
  active INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0, 1)),
  revision INTEGER NOT NULL DEFAULT 0,
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  time_zone TEXT
    CHECK (
      time_zone IS NULL OR (
        length(time_zone) <= 64
        AND length(trim(
          time_zone,
          char(9) || char(10) || char(11) || char(12) || char(13) || ' '
        )) > 0
      )
    )
);

INSERT INTO users_new (
  telegram_user_id, chat_id, course, active, revision, created_at_ms, updated_at_ms, time_zone
)
SELECT
  telegram_user_id, chat_id, course, active, revision, created_at_ms, updated_at_ms, time_zone
FROM users;

DROP TABLE users;
ALTER TABLE users_new RENAME TO users;

CREATE INDEX idx_users_active_course ON users (active, course);

CREATE TABLE user_reminder_offsets (
  telegram_user_id INTEGER NOT NULL REFERENCES users (telegram_user_id) ON DELETE CASCADE,
  offset_minutes INTEGER NOT NULL
    CHECK (offset_minutes >= 1 AND offset_minutes <= 43200),
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (telegram_user_id, offset_minutes)
);

-- The primary key already covers lookups by user (its leading column); no
-- secondary index is needed.
INSERT OR IGNORE INTO user_reminder_offsets (telegram_user_id, offset_minutes, created_at_ms)
  SELECT u.telegram_user_id, d.offset_minutes, 0
  FROM users u
  CROSS JOIN (
    SELECT 1440 AS offset_minutes
    UNION ALL SELECT 60
    UNION ALL SELECT 5
  ) AS d;
