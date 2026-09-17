-- 0007_user_time_zone.sql
--
-- Per-user IANA timezone for local rendering. `NULL` explicitly means
-- "onboarding incomplete: no timezone chosen yet"; reminder planning excludes
-- such users, so no reminder can be created before the choice. Existing users
-- were onboarded under Moscow-only rendering, so the upgrade maps every one of
-- them to Europe/Moscow without touching any other state.
--
-- Forward-only: never rewrites 0001-0006. New users start with NULL and must
-- select a zone (inline buttons or `/timezone Region/City`).

-- NULL = onboarding incomplete. The CHECK rejects empty and whitespace-only
-- values (space, tab, newline, vertical tab, form feed, carriage return) at the
-- schema level; the repository validates and canonicalizes with native Intl
-- before every write.
ALTER TABLE users ADD COLUMN time_zone TEXT
  CHECK (
    time_zone IS NULL OR (
      length(time_zone) <= 64
      AND length(trim(
        time_zone,
        char(9) || char(10) || char(11) || char(12) || char(13) || ' '
      )) > 0
    )
  );

-- Upgrade default: every user that existed before this migration. The
-- `IS NULL` guard keeps a manually repeated run from overwriting a zone a user
-- already chose (D1 applies each migration once anyway).
UPDATE users SET time_zone = 'Europe/Moscow' WHERE time_zone IS NULL;
