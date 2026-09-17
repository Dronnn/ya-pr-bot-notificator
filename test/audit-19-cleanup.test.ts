/**
 * F7 regression (repo part): every cleanup branch begins with a selective
 * indexed range and a deterministic indexed LIMIT order. The old
 * `occurrence_staging` age sweep filtered/ordered by
 * (updated_at_ms, attempt_id, id) while its only age index began with
 * `source_id` (full SCAN + temp b-tree), and the single multi-branch jobs
 * OR-query could not use one index for all branches.
 *
 * Migration 0005 adds `idx_staging_age` (NOT starting with source_id),
 * `idx_occurrences_cleanup` and `idx_jobs_cleanup_kind_age`; the repository
 * splits the jobs OR into three selective deletes. The EXPLAIN assertions
 * capture the shipped cleanup SQL via a recording D1 shim and fail against
 * the old shapes; the convergence test proves each pass
 * touches at most its limit and repeated passes finish without harming live
 * rows or delivery identities.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { Repository } from '../src/data/repository.ts';
import type { D1DatabaseLike, D1StatementLike } from '../src/platform.ts';
import { runSchedulerTick } from '../src/scheduler/tick.ts';
import {
  MAX_D1_STATEMENTS_PER_SCHEDULER,
  MS_PER_HOUR,
  RETENTION_MS,
  SENT_REMINDER_RETENTION_MS,
  SOURCE_LEASE_MS,
} from '../src/util.ts';
import { countRows, type SqliteD1 } from './helpers/d1-sqlite.ts';
import { textResponse } from './helpers/fakes.ts';
import { createHarness, schedulerDeps, type Harness } from './helpers/harness.ts';
import { parsedEvent } from './helpers/seed.ts';
import type { SourceDefinition } from '../src/calendar/sync.ts';

/**
 * Recording D1 shim: delegates every statement to the harness SQLite database
 * while capturing the exact SQL texts `Repository.cleanup` issues. The EXPLAIN
 * assertions always exercise shipped SQL, so an OR-shape regression in
 * repository SQL cannot pass silently against a hardcoded copy.
 */
class CleanupSqlCapture implements D1DatabaseLike {
  readonly statements: { sql: string; params: readonly unknown[] }[] = [];
  private readonly inner: SqliteD1;
  constructor(inner: SqliteD1) {
    this.inner = inner;
  }
  prepare(query: string): D1StatementLike {
    const innerStatement = this.inner.prepare(query);
    const captured = this.statements;
    return {
      bind(...values: readonly unknown[]): D1StatementLike {
        captured.push({ sql: query, params: values });
        return innerStatement.bind(...values);
      },
      first<T = Record<string, unknown>>(): Promise<T | null> {
        return innerStatement.first<T>();
      },
      all<T = Record<string, unknown>>() {
        return innerStatement.all<T>();
      },
      run() {
        return innerStatement.run();
      },
    };
  }
  batch(statements: readonly D1StatementLike[]): Promise<readonly unknown[]> {
    return this.inner.batch(statements);
  }
}

function explainDetails(harness: Harness, sql: string, params: readonly unknown[]): string[] {
  const rows = harness.db.database.prepare(`EXPLAIN QUERY PLAN ${sql}`).all(...(params as never[])) as {
    detail: string;
  }[];
  return rows.map((row) => row.detail);
}

/** Seeds a large mixed table state: live rows plus collectible old rows. */
function seedMixedTables(harness: Harness, now: number): void {
  const db = harness.db.database;
  db.prepare(`INSERT INTO sources (id, kind, etag, last_modified, fetched_at_ms, last_success_at_ms, status, last_error_code, revision, updated_at_ms) VALUES ('basic', 'basic', NULL, NULL, ?, ?, 'ok', NULL, 1, ?)`)
    .run(now, now, now);
  const old = now - RETENTION_MS - 60_000;
  const ancient = now - SENT_REMINDER_RETENTION_MS - 60_000;

  const insertOccurrence = db.prepare(
    `INSERT INTO occurrences (id, source_id, uid, occurrence_key, course, starts_at_ms, ends_at_ms, summary, description, url, status, is_all_day, revision, updated_at_ms)
     VALUES (?, 'basic', 'u', ?, 'basic', ?, NULL, 'Lesson', NULL, NULL, 'confirmed', 0, 1, ?)`,
  );
  for (let index = 0; index < 300; index += 1) {
    insertOccurrence.run(`old-occ-${index}`, `old-${index}`, old - index, old);
  }
  for (let index = 0; index < 200; index += 1) {
    insertOccurrence.run(`live-occ-${index}`, `live-${index}`, now + (index + 1) * MS_PER_HOUR, now);
  }

  const insertJob = db.prepare(
    `INSERT INTO outbound_jobs (id, kind, telegram_user_id, chat_id, occurrence_id, reminder_offset_minutes, send_at_ms, next_attempt_at_ms, status, attempt_count, expected_revision, payload_json, dedup_key, created_at_ms, updated_at_ms)
     VALUES (?, ?, 1, 1, ?, 30, ?, ?, ?, 0, 1, NULL, ?, ?, ?)`,
  );
  for (let index = 0; index < 120; index += 1) {
    insertJob.run(`cmd-old-${index}`, 'command', null, old, old, 'sent', `cmd-old-${index}`, old, old);
  }
  for (let index = 0; index < 120; index += 1) {
    insertJob.run(
      `rem-sent-old-${index}`, 'reminder', `gone-${index}`, ancient, ancient,
      'sent', `rem-sent-old-${index}`, ancient, ancient,
    );
  }
  for (let index = 0; index < 60; index += 1) {
    insertJob.run(
      `rem-term-old-${index}`, 'reminder', `gone-t-${index}`, old, old,
      'cancelled', `rem-term-old-${index}`, old, old,
    );
  }
  for (let index = 0; index < 40; index += 1) {
    insertJob.run(
      `rem-live-${index}`, 'reminder', `live-occ-${index}`, now, now + MS_PER_HOUR,
      'pending', `rem-live-${index}`, now, now,
    );
  }

  const insertStaging = db.prepare(
    `INSERT INTO occurrence_staging (attempt_id, id, source_id, uid, occurrence_key, course, starts_at_ms, ends_at_ms, summary, description, url, status, is_all_day, updated_at_ms)
     VALUES (?, ?, 'basic', 'u', ?, 'basic', 1, NULL, 'S', NULL, NULL, 'confirmed', 0, ?)`,
  );
  for (let index = 0; index < 150; index += 1) {
    insertStaging.run('dead#1', `dead-${index}`, `dead-${index}`, now - SOURCE_LEASE_MS - 1000);
  }
  insertStaging.run('live#9', 'live-row', 'live-row', now);

  const insertUpdate = db.prepare(
    `INSERT INTO processed_updates (update_id, lease_owner, lease_expires_at_ms, status, processed_at_ms)
     VALUES (?, NULL, NULL, 'done', ?)`,
  );
  for (let index = 1; index <= 130; index += 1) {
    insertUpdate.run(index, old);
  }

  // One durable delivery identity for an already-collected sent row.
  db.prepare(
    `INSERT INTO delivery_ledger (dedup_key, occurrence_id, telegram_user_id, sent_at_ms)
     VALUES ('rem:1:gone-ledger:30', 'gone-ledger', 1, ?)`,
  ).run(ancient);
}

describe('indexed bounded cleanup', () => {
  it('every cleanup branch uses a selective index with no SCAN or temp sort', async () => {
    const harness = createHarness();
    const capture = new CleanupSqlCapture(harness.db);
    await new Repository(capture).cleanup(harness.clock.now(), RETENTION_MS, 50);
    assert.equal(capture.statements.length, 7, 'cleanup issues its seven shipped statements');
    for (const [index, statement] of capture.statements.entries()) {
      const name = `cleanup[${index}] ${statement.sql.replace(/\s+/g, ' ').slice(0, 90)}`;
      const details = explainDetails(harness, statement.sql, statement.params);
      assert.ok(details.length > 0, `${name}: EXPLAIN returns a plan`);
      for (const detail of details) {
        assert.ok(
          !/\bSCAN\b/.test(detail),
          `${name}: no table scan (got "${detail}")`,
        );
        assert.ok(
          !/TEMP B-TREE/.test(detail),
          `${name}: no temporary sort (got "${detail}")`,
        );
      }
      assert.ok(
        details.some((detail) => /SEARCH/.test(detail)),
        `${name}: a selective indexed range leads the plan (${details.join(' / ')})`,
      );
    }
  });

  it('seeded passes touch at most the limit and converge without harming live rows', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    seedMixedTables(harness, now);
    const limit = 50;

    const countJobs = (): number => countRows(harness.db, 'SELECT COUNT(*) AS n FROM outbound_jobs');
    const initialJobs = countJobs();
    assert.equal(initialJobs, 120 + 120 + 60 + 40);

    // One pass deletes at most `limit` rows per statement: the first pass
    // cannot drain any 120-row backlog table in full.
    await harness.repository.cleanup(now, RETENTION_MS, limit);
    const afterFirst = countJobs();
    assert.ok(
      initialJobs - afterFirst <= 3 * limit,
      `one pass deletes at most one limit per job branch (removed ${initialJobs - afterFirst})`,
    );
    assert.ok(afterFirst > 40, 'old rows remain after a single bounded pass');

    // Repeated passes converge: old rows drain, live rows and the ledger stay.
    for (let pass = 0; pass < 10; pass += 1) {
      await harness.repository.cleanup(now, RETENTION_MS, limit);
    }
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM outbound_jobs WHERE id LIKE 'rem-live-%'"),
      40,
      'live pending reminders are never collected',
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM outbound_jobs WHERE id LIKE 'cmd-old-%'"),
      0,
      'old command replies drain',
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM outbound_jobs WHERE id LIKE 'rem-sent-old-%'"),
      0,
      'aged sent rows drain',
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM outbound_jobs WHERE id LIKE 'rem-term-old-%'"),
      0,
      'orphaned terminal reminders drain',
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrences WHERE id LIKE 'live-occ-%'"),
      200,
      'future occurrences survive',
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrences WHERE id LIKE 'old-occ-%'"),
      0,
      'aged occurrences drain',
    );
    assert.equal(
      countRows(harness.db, "SELECT COUNT(*) AS n FROM occurrence_staging WHERE attempt_id = 'live#9'"),
      1,
      'live staging rows are never swept',
    );
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrence_staging'),
      1,
      'dead staging leftovers converge to just the live row',
    );
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM delivery_ledger'),
      1,
      'cleanup NEVER deletes delivery identities',
    );

    // A further pass is a no-op: convergence, not churn.
    const before = countJobs();
    await harness.repository.cleanup(now, RETENTION_MS, limit);
    assert.equal(countJobs(), before, 'a converged pass changes nothing');
  });

  it('scheduler total stays below 50 statements with both sources', async () => {
    const harness = createHarness({ now: Date.UTC(2024, 5, 2, 21) });
    const now = harness.clock.now();
    const events = Array.from({ length: 60 }, (_, index) => {
      const startsAtMs = now + (index + 1) * MS_PER_HOUR;
      return parsedEvent(startsAtMs, { uid: `course-${index}`, endsAtMs: startsAtMs + MS_PER_HOUR });
    });
    harness.setParser({ events });
    harness.setHandler(() => textResponse('BEGIN:VCALENDAR\nVERSION:2.0\nEND:VCALENDAR', 200, { etag: 'v1' }));
    const sources: SourceDefinition[] = [
      { id: 'basic', kind: 'basic', url: 'https://example.test/basic.ics' },
      { id: 'extended', kind: 'extended', url: 'https://example.test/extended.ics' },
    ];

    const result = await runSchedulerTick(schedulerDeps(harness, { sources }));

    assert.deepEqual(result.syncStatuses, ['applied', 'applied']);
    const used = harness.repository.statementsUsed();
    assert.ok(
      used <= MAX_D1_STATEMENTS_PER_SCHEDULER,
      `two-source tick with 60 occurrences each uses ${used} of ${MAX_D1_STATEMENTS_PER_SCHEDULER} statements`,
    );
    assert.ok(
      harness.db.stats.maxParamsPerStatement <= 100,
      `at most 100 bound parameters per statement (saw ${harness.db.stats.maxParamsPerStatement})`,
    );
    // Recorded for the handoff report (cleanup is seven statements: one
    // occurrence sweep, three split job sweeps, staging, updates, callback
    // answers).
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'),
      120,
      'both snapshots materialize',
    );
  });
});
