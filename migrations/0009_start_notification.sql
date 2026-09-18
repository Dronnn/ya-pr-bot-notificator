-- 0009_start_notification.sql
--
-- Adds the at-start notification as the special stored offset 0: it fires when
-- the occurrence starts (with a five-minute delivery grace in the consumer),
-- is seeded for every user by default and is toggled independently of the
-- lead-time rules. The `add`/`edit` commands still accept only 1..43200; only
-- the dedicated toggle stores 0.
--
-- Forward-only: never rewrites 0001-0008. The offsets table is rebuilt because
-- SQLite cannot relax a CHECK constraint in place; every existing row (and the
-- user foreign key) is preserved. Existing users get offset 0 inserted, so the
-- notification is enabled by default; the AFTER INSERT trigger seeds it for
-- every new user together with the standard 1440/60/5 set.

DROP TRIGGER users_insert_default_reminder_offsets;

CREATE TABLE user_reminder_offsets_new (
  telegram_user_id INTEGER NOT NULL REFERENCES users (telegram_user_id) ON DELETE CASCADE,
  offset_minutes INTEGER NOT NULL
    CHECK (offset_minutes >= 0 AND offset_minutes <= 43200),
  created_at_ms INTEGER NOT NULL,
  PRIMARY KEY (telegram_user_id, offset_minutes)
);

INSERT INTO user_reminder_offsets_new (telegram_user_id, offset_minutes, created_at_ms)
SELECT telegram_user_id, offset_minutes, created_at_ms
FROM user_reminder_offsets;

DROP TABLE user_reminder_offsets;
ALTER TABLE user_reminder_offsets_new RENAME TO user_reminder_offsets;

-- New users receive the standard set plus the at-start notification in the
-- same SQLite transaction as the users INSERT.
CREATE TRIGGER users_insert_default_reminder_offsets
AFTER INSERT ON users
BEGIN
  INSERT INTO user_reminder_offsets (telegram_user_id, offset_minutes, created_at_ms)
  VALUES
    (NEW.telegram_user_id, 1440, NEW.created_at_ms),
    (NEW.telegram_user_id, 60, NEW.created_at_ms),
    (NEW.telegram_user_id, 5, NEW.created_at_ms),
    (NEW.telegram_user_id, 0, NEW.created_at_ms);
END;

INSERT OR IGNORE INTO user_reminder_offsets (telegram_user_id, offset_minutes, created_at_ms)
  SELECT telegram_user_id, 0, 0
  FROM users;
