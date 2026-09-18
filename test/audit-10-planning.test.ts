/**
 * F10 regression: planning cost is proportional to the due time window.
 *
 * The old join started from every occurrence of the course within the full
 * 30-day horizon and only then evaluated `starts_at - offset <= now`, so 1,000
 * users and 60 far-future occurrences produced 60,000 candidate pairs each
 * minute. The planner bounds `starts_at_ms` to `(now, now +
 * MAX_REMINDER_OFFSET_MINUTES]` by the indexed `(course, starts_at_ms)` range
 * before the user join. Since the largest rule offset (30 days) is exactly the
 * calendar horizon, that window equals the horizon, and the per-candidate
 * `send_at_ms <= now` predicate is what removes not-yet-due work; occurrences
 * beyond the horizon are still never joined. The conflict update is a no-op for
 * unchanged pending rows.
 *
 * Local `node:sqlite` is a proxy for D1: `EXPLAIN QUERY PLAN`, the rewritten
 * candidate count, the join-fanout count and statement counts exercise the same
 * SQL, indexes and parameters, but they are not a billed-row measurement of
 * production D1.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import type { DatabaseSync } from 'node:sqlite';

import { Repository } from '../src/data/repository.ts';
import type { D1DatabaseLike, D1ResultLike, D1StatementLike } from '../src/platform.ts';
import { MAX_REMINDER_OFFSET_MINUTES } from '../src/domain/notification-policy.ts';
import { EXPANSION_HORIZON_MS, MS_PER_HOUR, MS_PER_MINUTE } from '../src/util.ts';
import { type SqliteD1 } from './helpers/d1-sqlite.ts';
import { createHarness } from './helpers/harness.ts';
import { occurrence, onboardUser, seedReminderOffsets, seedSource } from './helpers/seed.ts';

interface RecordedStatement {
  readonly sql: string;
  readonly params: readonly unknown[];
}

/**
 * D1 wrapper that records every statement the repository actually executes and
 * delegates to the in-process SQLite shim. The planning regression rewrites the
 * recorded INSERT into its equivalent COUNT to measure the candidate rows.
 */
class RecordingD1 implements D1DatabaseLike {
  readonly records: RecordedStatement[] = [];
  readonly shim: SqliteD1;

  constructor(shim: SqliteD1) {
    this.shim = shim;
  }

  get database(): DatabaseSync {
    return this.shim.database;
  }

  prepare(query: string): D1StatementLike {
    return new RecordingStatement(this, query, [], (params) =>
      this.shim.prepare(query).bind(...params),
    );
  }

  async batch(statements: readonly D1StatementLike[]): Promise<readonly unknown[]> {
    return this.shim.batch(
      statements.map((statement) => (statement as RecordingStatement).boundStatement),
    );
  }
}

class RecordingStatement implements D1StatementLike {
  readonly owner: RecordingD1;
  readonly sql: string;
  readonly params: readonly unknown[];
  readonly boundStatement: D1StatementLike;

  constructor(
    owner: RecordingD1,
    sql: string,
    params: readonly unknown[],
    bind: (params: readonly unknown[]) => D1StatementLike,
  ) {
    this.owner = owner;
    this.sql = sql;
    this.params = params;
    this.boundStatement = bind(params);
  }

  bind(...values: readonly unknown[]): D1StatementLike {
    return new RecordingStatement(this.owner, this.sql, values, (params) =>
      this.owner.shim.prepare(this.sql).bind(...params),
    );
  }

  #record(): void {
    this.owner.records.push({ sql: this.sql, params: this.params });
  }

  async first<T = Record<string, unknown>>(): Promise<T | null> {
    this.#record();
    return this.boundStatement.first<T>();
  }

  async all<T = Record<string, unknown>>(): Promise<D1ResultLike<T>> {
    this.#record();
    return this.boundStatement.all<T>();
  }

  async run(): Promise<D1ResultLike<never>> {
    this.#record();
    return this.boundStatement.run();
  }
}

/**
 * Rewrites a recorded planning INSERT into the equivalent COUNT over its
 * SELECT. The old query had no `starts_at_ms` upper bound, so this counts
 * occurrence × user fanout; the new one filters by index first. Returns the
 * number of leading `?` placeholders the removed select list held, so the
 * caller can bind the remaining WHERE parameters.
 */
function candidateCountQuery(insertSql: string): { sql: string; leadingParams: number } {
  const withoutConflict = insertSql.replace(/\s*ON CONFLICT[\s\S]*$/i, '');
  const selectAt = withoutConflict.search(/SELECT/i);
  const fromAt = withoutConflict.search(/FROM occurrences o/i);
  assert.ok(selectAt >= 0 && fromAt > selectAt, 'planning SQL must select from occurrences');
  const leadingParams = (withoutConflict.slice(selectAt, fromAt).match(/\?/g) ?? []).length;
  return { sql: `SELECT COUNT(*) AS n ${withoutConflict.slice(fromAt)}`, leadingParams };
}

function recordedPlan(recorder: RecordingD1): RecordedStatement {
  const planning = recorder.records.find(
    (record) => record.sql.includes('INSERT INTO outbound_jobs') && record.sql.includes('FROM occurrences o'),
  );
  assert.notEqual(planning, undefined, 'expected one planning statement');
  return planning as RecordedStatement;
}

function candidateRows(recorder: RecordingD1, planning: RecordedStatement): number {
  const candidate = candidateCountQuery(planning.sql);
  const row = recorder.database
    .prepare(candidate.sql)
    .get(...(planning.params.slice(candidate.leadingParams) as never[])) as { n: number };
  return Number(row.n);
}

function queryPlanDetails(recorder: RecordingD1, planning: RecordedStatement): string {
  const candidate = candidateCountQuery(planning.sql);
  const rows = recorder.database
    .prepare(`EXPLAIN QUERY PLAN ${candidate.sql}`)
    .all(...(planning.params.slice(candidate.leadingParams) as never[])) as { detail: string }[];
  return rows.map((row) => row.detail).join('\n');
}

/**
 * Join-fanout rows the planner's occurrence window would produce: every
 * confirmed occurrence in `(now, windowEndMs]` × every active user of the same
 * course. Since the largest rule offset equals the 30-day horizon, the
 * occurrence window no longer shrinks below it; this shape still proves that
 * beyond-horizon occurrences are excluded by the indexed range before the join.
 * Run through the recording wrapper's database.
 */
function fanoutRows(recorder: RecordingD1, now: number, windowEndMs: number): number {
  const row = recorder.database
    .prepare(
       `SELECT COUNT(*) AS n FROM occurrences o
        JOIN users u ON u.course = o.course
        WHERE u.active = 1 AND u.time_zone IS NOT NULL AND o.status = 'confirmed'
          AND o.starts_at_ms > ? AND o.starts_at_ms <= ?`,
    )
    .get(now, windowEndMs) as { n: number };
  return Number(row.n);
}

describe('planning proportional to the due window', () => {
  it('evaluates zero candidates for 1000 users and 60 out-of-window occurrences', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    for (let userId = 1; userId <= 1000; userId += 1) {
      await harness.repository.activateUser(userId, userId, now);
      onboardUser(harness, userId);
    }
    const outside = Array.from({ length: 60 }, (_, index) =>
      occurrence({
        occurrenceKey: `far-${index}`,
        // Beyond the 30-day horizon. The largest offset equals the horizon, so
        // nothing past `now + EXPANSION_HORIZON_MS` can ever be a candidate.
        startsAtMs:
          now +
          EXPANSION_HORIZON_MS +
          (index + 1) * 8 * MS_PER_HOUR,
      }),
    );
    await harness.repository.upsertOccurrences(outside, now);

    const recorder = new RecordingD1(harness.db);
    const repository = new Repository(recorder);
    const changes = await repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);

    assert.equal(changes, 0, 'no beyond-horizon occurrence may become a job');
    assert.equal(recorder.records.length, 1, 'planning is one set-based statement');
    const planning = recordedPlan(recorder);
    assert.equal(
      candidateRows(recorder, planning),
      0,
      '1000 users x 60 beyond-horizon occurrences must evaluate zero candidates',
    );

    const dueWindowEndMs = now + MAX_REMINDER_OFFSET_MINUTES * MS_PER_MINUTE;
    assert.equal(
      dueWindowEndMs,
      now + EXPANSION_HORIZON_MS,
      'the due window is exactly the 30-day horizon: the largest offset equals it',
    );
    assert.ok(
      planning.params.includes(dueWindowEndMs),
      'the recorded statement binds now + MAX_REMINDER_OFFSET_MINUTES * MS_PER_MINUTE',
    );
    assert.equal(
      fanoutRows(recorder, now, dueWindowEndMs),
      0,
      'the occurrence window evaluates no beyond-horizon occurrence',
    );

    // The occurrences search is an index range on (course, starts_at_ms) with
    // both window bounds pushed into the range, so far-future rows are never
    // joined against the user set.
    const details = queryPlanDetails(recorder, planning);
    assert.match(details, /SEARCH o USING INDEX idx_occurrences_course_start/);
    assert.match(details, /starts_at_ms>\?/);
    assert.match(details, /starts_at_ms<\?/);
    assert.match(details, /USING INDEX idx_users_active_course/);
  });

  it('plans due 30-minute and 1-day reminders exactly once and then no-ops', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await seedSource(harness.repository, 'extended', now);
    await harness.repository.activateUser(1, 1, now);
    onboardUser(harness, 1);
    seedReminderOffsets(harness, 1, [30]);
    await harness.repository.activateUser(2, 2, now);
    onboardUser(harness, 2);
    await harness.repository.setUserCourse(2, 'extended', now);
    seedReminderOffsets(harness, 2, [1440]);

    await harness.repository.upsertOccurrences(
      [
        occurrence({ occurrenceKey: 'due-30', startsAtMs: now + 10 * MS_PER_MINUTE }),
        occurrence({
          sourceId: 'extended',
          course: 'extended',
          occurrenceKey: 'due-1440',
          startsAtMs: now + 23 * 60 * MS_PER_MINUTE,
        }),
      ],
      now,
    );

    const first = await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
    assert.equal(first, 2, 'exactly one 30-minute and one 1-day reminder are planned');
    const planned = harness.db.database
      .prepare('SELECT id, status, attempt_count, expected_revision FROM outbound_jobs ORDER BY id')
      .all() as { id: string; status: string; attempt_count: unknown; expected_revision: unknown }[];
    assert.deepEqual(
      planned.map((row) => [row.id, row.status, Number(row.attempt_count)]),
      [
        ['rem:1:basic:due-30:30', 'pending', 0],
        ['rem:2:extended:due-1440:1440', 'pending', 0],
      ],
      'one job per recipient, planned exactly once at attempt zero',
    );

    // Simulate failed attempts; an unchanged replan must not rewrite or reset.
    harness.db.database
      .prepare("UPDATE outbound_jobs SET attempt_count = 3, last_error_code = 'transient_500', updated_at_ms = 999")
      .run();
    const second = await harness.repository.planDueReminders(
      now + MS_PER_MINUTE,
      now + MS_PER_MINUTE + EXPANSION_HORIZON_MS,
    );
    assert.equal(second, 0, 'repeated planning of unchanged data reports 0 changes');
    const after = harness.db.database
      .prepare('SELECT attempt_count, last_error_code, updated_at_ms FROM outbound_jobs ORDER BY id')
      .all() as { attempt_count: unknown; last_error_code: unknown; updated_at_ms: unknown }[];
    assert.deepEqual(
      after.map((row) => [Number(row.attempt_count), row.last_error_code, Number(row.updated_at_ms)]),
      [
        [3, 'transient_500', 999],
        [3, 'transient_500', 999],
      ],
      'attempts, error code and updated_at survive a no-op replan',
    );
  });

  it('derives the candidate bound from MAX_REMINDER_OFFSET_MINUTES inclusively', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.activateUser(1, 1, now);
    onboardUser(harness, 1);
    seedReminderOffsets(harness, 1, [MAX_REMINDER_OFFSET_MINUTES]);
    const windowEnd = now + MAX_REMINDER_OFFSET_MINUTES * MS_PER_MINUTE;
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'edge', startsAtMs: windowEnd })],
      now,
    );

    const recorder = new RecordingD1(harness.db);
    const repository = new Repository(recorder);
    assert.equal(await repository.planDueReminders(now, now + EXPANSION_HORIZON_MS), 1);
    const planning = recordedPlan(recorder);
    assert.equal(
      planning.params[planning.params.length - 1],
      windowEnd,
      'the bound is now + MAX_REMINDER_OFFSET_MINUTES * MS_PER_MINUTE',
    );
    assert.equal(candidateRows(recorder, planning), 1, 'the boundary occurrence is a candidate');
  });

  it('never scans an occurrence one millisecond past the derived bound', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.activateUser(1, 1, now);
    onboardUser(harness, 1);
    await harness.repository.upsertOccurrences(
      [
        occurrence({
          occurrenceKey: 'past-edge',
          startsAtMs: now + MAX_REMINDER_OFFSET_MINUTES * MS_PER_MINUTE + 1,
        }),
      ],
      now,
    );

    const recorder = new RecordingD1(harness.db);
    const repository = new Repository(recorder);
    assert.equal(await repository.planDueReminders(now, now + EXPANSION_HORIZON_MS), 0);
    assert.equal(candidateRows(recorder, recordedPlan(recorder)), 0);
  });

  it('plans overdue work after a missed tick while the send-time predicate bounds candidates', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);
    await harness.repository.activateUser(1, 1, start);
    onboardUser(harness, 1);
    seedReminderOffsets(harness, 1, [30]);
    await harness.repository.upsertOccurrences(
      [
        occurrence({ occurrenceKey: 'overdue', startsAtMs: start + 20 * MS_PER_MINUTE }),
        occurrence({ occurrenceKey: 'far', startsAtMs: start + 8 * 24 * 60 * MS_PER_MINUTE }),
      ],
      start,
    );

    // The tick is six minutes late: the reminder's send time has passed while
    // its occurrence is still in the future.
    const now = start + 6 * MS_PER_MINUTE;
    const recorder = new RecordingD1(harness.db);
    const repository = new Repository(recorder);
    const changes = await repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);

    assert.equal(changes, 1, 'the overdue reminder is recovered');
    const planning = recordedPlan(recorder);
    assert.equal(
      candidateRows(recorder, planning),
      1,
      'only the occurrence inside the due window is joined against users',
    );
    assert.equal(
      fanoutRows(recorder, now, now + EXPANSION_HORIZON_MS),
      2,
      'both occurrences are inside the 30-day occurrence window',
    );
    assert.equal(
      fanoutRows(recorder, now, now + MAX_REMINDER_OFFSET_MINUTES * MS_PER_MINUTE),
      2,
      'the due window equals the horizon, so the join sees both occurrences',
    );
    assert.equal(
      MAX_REMINDER_OFFSET_MINUTES * MS_PER_MINUTE,
      30 * 24 * 60 * MS_PER_MINUTE,
      'the due window is exactly the 30-day calendar horizon',
    );
  });
});
