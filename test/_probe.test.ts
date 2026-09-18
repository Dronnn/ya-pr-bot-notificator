import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { applyMigrations, countRows, createSqliteD1 } from './helpers/d1-sqlite.ts';

describe('sqlite shim', () => {
  it('creates the schema and rolls back a failed batch atomically', async () => {
    const db = createSqliteD1();
    applyMigrations(db);

    const tables = (
      db.database
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
        .all() as { name: string }[]
    ).map((row) => row.name);
    for (const expected of [
      'users',
      'sources',
      'occurrences',
      'outbound_jobs',
      'processed_updates',
      'locks',
      'rate_state',
    ]) {
      assert.ok(tables.includes(expected), `missing table ${expected}`);
    }

    await assert.rejects(
      db.batch([
        db.prepare(
          `INSERT INTO users (telegram_user_id, chat_id, course, active, revision, created_at_ms, updated_at_ms)
           VALUES (1, 1, 'basic', 1, 1, 0, 0)`,
        ),
        db.prepare(
          `INSERT INTO users (telegram_user_id, chat_id, course, active, revision, created_at_ms, updated_at_ms)
           VALUES (1, 1, 'basic', 1, 1, 0, 0)`,
        ),
      ]),
    );

    assert.equal(countRows(db, 'SELECT COUNT(*) AS n FROM users'), 0);
  });
});
