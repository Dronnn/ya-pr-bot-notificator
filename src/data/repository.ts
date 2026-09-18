/**
 * Parameterized D1 repository. Every statement is bound; no SQL is built from
 * user input. Chunked writers keep each batch bounded (<= MAX_D1_BATCH_STATEMENTS
 * statements and <= MAX_SQL_PARAMS parameters per statement).
 *
 * A per-invocation statement budget can be armed with `beginInvocation`. Every
 * statement executed through the repository is counted and the invocation is
 * refused as soon as it would exceed the limit, so a Worker request can never
 * silently drift past the D1 statement allowance.
 */

import type { D1DatabaseLike, D1ResultLike, D1StatementLike } from '../platform.ts';
import type { OccurrenceStatus } from '../domain/calendar.ts';
import {
  MAX_REMINDER_RULES_PER_USER,
  MAX_REMINDER_OFFSET_MINUTES,
  START_REMINDER_GRACE_MS,
  START_REMINDER_OFFSET_MINUTES,
  isStoredReminderOffset,
  isValidReminderOffset,
  normalizeReminderOffsets,
  type Course,
  type ReminderOffsetMinutes,
} from '../domain/notification-policy.ts';
import {
  assertSafeInteger,
  MAX_D1_BATCH_STATEMENTS,
  MAX_SQL_PARAMS,
  MS_PER_MINUTE,
  SENT_REMINDER_RETENTION_MS,
  SOURCE_LEASE_MS,
  normalizeTimeZone,
  type Clock,
} from '../util.ts';

/** Raised when a statement would push the armed invocation past its budget. */
export class StatementBudgetError extends Error {
  constructor(limit: number) {
    super(`D1 statement budget of ${limit} exceeded for this invocation`);
    this.name = 'StatementBudgetError';
  }
}

/** Comma-separated `?` placeholders for `count` bound parameters. */
function placeholders(count: number): string {
  return Array.from({ length: count }, () => '?').join(', ');
}

/** Values `occurrenceRowValues` contributes to one occurrence row. */
const OCCURRENCE_VALUE_COUNT = 12;
/** Bound parameters per direct upsert row: the twelve values plus updated_at. */
const UPSERT_PARAMS_PER_ROW = OCCURRENCE_VALUE_COUNT + 1;
/** `revision` is the literal 1 between the values and updated_at. */
const UPSERT_TUPLE = `(${placeholders(OCCURRENCE_VALUE_COUNT)}, 1, ?)`;
/** Staging rows add attempt_id before the values and updated_at after them. */
const STAGING_PARAMS_PER_ROW = OCCURRENCE_VALUE_COUNT + 2;
const STAGING_TUPLE = `(${placeholders(STAGING_PARAMS_PER_ROW)})`;
/** Chunk size for wide occurrence writers, bounded by the parameter limit. */
const UPSERT_ROWS_PER_STATEMENT = Math.max(
  1,
  Math.floor(MAX_SQL_PARAMS / UPSERT_PARAMS_PER_ROW),
);
/** Same chunk size for staging: 14 parameters per row still fits the limit. */
const STAGING_ROWS_PER_STATEMENT = Math.max(
  1,
  Math.min(
    UPSERT_ROWS_PER_STATEMENT,
    Math.floor(MAX_SQL_PARAMS / STAGING_PARAMS_PER_ROW),
  ),
);

/**
 * Statements of the atomic publication, in execution order. The record built by
 * `#occurrencePublishStatements` is keyed by these names, so the sequence and
 * `PUBLISH_STATEMENT_COUNT` cannot drift apart.
 */
const PUBLISH_STEPS = [
  'custodyProbe',
  'stagedUpsert',
  'horizonDelete',
  'cursorUpdate',
  'refreshMarker',
  'stagingSweep',
] as const;
type PublishStep = (typeof PUBLISH_STEPS)[number];
/** Statements in one publication batch, charged by the `snapshotCost` preflight. */
const PUBLISH_STATEMENT_COUNT = PUBLISH_STEPS.length;
/** Position of `horizonDelete` in the publication results, for its `meta.changes`. */
const HORIZON_DELETE_INDEX = PUBLISH_STEPS.indexOf('horizonDelete');

/** Occurrence columns in write order; `revision` is inserted as the literal 1. */
const OCCURRENCE_COLUMNS =
  'id, source_id, uid, occurrence_key, course, starts_at_ms, ends_at_ms, ' +
  'summary, description, url, status, is_all_day, revision, updated_at_ms';

/** Source columns in write order, shared by the insert, fresh and error writers. */
const SOURCE_COLUMNS =
  'id, kind, etag, last_modified, fetched_at_ms, last_success_at_ms, ' +
  'status, last_error_code, revision, updated_at_ms';

/**
 * Shared tail of both occurrence upserts (direct and staged). The
 * material-change WHERE updates only rows whose content changed, so revisions
 * stay stable for unchanged rows.
 */
const OCCURRENCE_UPSERT_CONFLICT = `ON CONFLICT(id) DO UPDATE SET
  starts_at_ms = excluded.starts_at_ms,
  ends_at_ms = excluded.ends_at_ms,
  summary = excluded.summary,
  description = excluded.description,
  url = excluded.url,
  status = excluded.status,
  is_all_day = excluded.is_all_day,
  revision = occurrences.revision + 1,
  updated_at_ms = excluded.updated_at_ms
  WHERE occurrences.starts_at_ms IS NOT excluded.starts_at_ms
     OR occurrences.ends_at_ms IS NOT excluded.ends_at_ms
     OR occurrences.summary IS NOT excluded.summary
     OR occurrences.description IS NOT excluded.description
     OR occurrences.url IS NOT excluded.url
     OR occurrences.status IS NOT excluded.status
     OR occurrences.is_all_day IS NOT excluded.is_all_day`;

/**
 * Custody check for a source attempt: the exact owner, the exact generation and
 * a lease that is still live at the time of the write. Every attempt-bearing
 * statement uses it, so an attempt whose lease lapsed (with or without a newer
 * acquisition) can neither switch the snapshot nor relabel the source.
 */
const LOCKED_LIVE =
  'EXISTS (SELECT 1 FROM locks WHERE name = ? AND owner = ? AND generation = ? AND expires_at_ms > ?)';

/**
 * Staleness predicate for command-driven mutations. Its first parameter is the
 * optional `commandUpdateId` (NULL disables the guard and the whole predicate is
 * true); the remaining parameters are the user id, the fallback update id and
 * the update id again. A missing `user_command_state` row has no recorded newer
 * command, so the fallback (this update id) keeps first-contact work
 * deliverable; otherwise the statement applies only while the recorded maximum
 * is at most `commandUpdateId`. A suspended older invocation therefore cannot
 * overwrite state a newer command already completed.
 */
const COMMAND_CURRENT =
  '(? IS NULL OR COALESCE((SELECT last_update_id FROM user_command_state WHERE telegram_user_id = ?), ?) <= ?)';

export interface UserRecord {
  telegramUserId: number;
  chatId: number;
  course: Course;
  /** Stored IANA timezone; null means onboarding is incomplete. */
  timeZone: string | null;
  active: boolean;
  revision: number;
}

export interface SourceRecord {
  id: string;
  kind: Course;
  etag: string | null;
  lastModified: string | null;
  fetchedAtMs: number | null;
  /** Last full re-expansion, used to bound 304-driven unconditional refreshes. */
  lastRefreshAtMs: number | null;
  status: 'unknown' | 'ok' | 'error';
  revision: number;
}

/**
 * One acquired source lease. `generation` identifies the attempt: it changes on
 * every acquisition, so a caller can prove that it still is the current holder.
 */
export interface SourceLease {
  owner: string;
  generation: number;
}

/** HTTP validators persisted with a published snapshot. */
export interface SnapshotCursor {
  etag: string | null;
  lastModified: string | null;
}

/**
 * Validated deletion range of one snapshot publication. Both ends are
 * inclusive, exactly like the expansion range `buildOccurrences` filtered on,
 * and the start is additionally clamped to the publication clock so rows in the
 * past are never removed by a horizon rule.
 */
export interface DeletionHorizon {
  readonly startMs: number;
  readonly endMs: number;
}

/** Outcome of one successful snapshot publication. */
export interface SnapshotPublication {
  readonly upserted: number;
  readonly deleted: number;
}

/**
 * Result of trying to take ownership of a webhook update:
 * - `acquired`: this caller now holds the processing lease and must process it;
 * - `done`: the update was already completed; acknowledge the duplicate;
 * - `busy`: another owner holds a live processing lease; the caller must not
 *   process or acknowledge it, and should ask the sender to retry.
 */
export type UpdateAcquisition = 'acquired' | 'done' | 'busy';

/**
 * Atomic pre-send reservation (finding 2 + finding 4 ordering guard):
 * - `reserved` carries the new attempt_count; exactly one increment happened
 *   and the caller may start one Telegram request;
 * - `exhausted` means attempt_count is already at maxAttempts; the caller must
 *   persist a terminal state without sending and without incrementing;
 * - `lost` means custody is gone (missing job, wrong/expired lease, non-leased
 *   status); the caller must not send;
 * - `superseded` means a command reply is stale (expected_revision moved on,
 *   or source_update_id < max seen update_id); the caller must persist a
 *   terminal superseded state without sending.
 */
export type SendReservation =
  | {
      readonly status: 'reserved';
      readonly attempt: number;
      /**
       * Recipient's stored timezone read atomically with the increment. A
       * reminder renders immediately before this request from this value, so a
       * zone change during pacing or a 429 wait is picked up by the next
       * reservation instead of rendering a stale zone.
       */
      readonly userTimeZone: string | null;
    }
  | { readonly status: 'exhausted' }
  | { readonly status: 'lost' }
  | { readonly status: 'superseded' };

export interface OccurrenceWrite {
  id: string;
  sourceId: string;
  uid: string;
  occurrenceKey: string;
  course: Course;
  startsAtMs: number;
  endsAtMs: number | null;
  summary: string;
  description: string | null;
  url: string | null;
  status: OccurrenceStatus;
  isAllDay: boolean;
}

export interface OccurrenceView {
  id: string;
  occurrenceKey: string;
  course: Course;
  startsAtMs: number;
  endsAtMs: number | null;
  summary: string;
  description: string | null;
  url: string | null;
  status: OccurrenceStatus;
}

export interface JobContext {
  jobId: string;
  kind: 'reminder' | 'command';
  telegramUserId: number;
  chatId: number;
  occurrenceId: string | null;
  reminderOffsetMinutes: number | null;
  sendAtMs: number;
  nextAttemptAtMs: number;
  attemptCount: number;
  expectedRevision: number | null;
  payloadJson: string | null;
  /** Stored Telegram update_id that produced this command job (NULL/0 = legacy, drainable). */
  sourceUpdateId: number | null;
  /** Max seen update_id for this user/chat from user_command_state (NULL = none). */
  commandLastSeenUpdateId: number | null;
  userFound: boolean;
  userActive: boolean;
  userCourse: Course | null;
  /**
   * Whether the job's `reminderOffsetMinutes` is still one of the recipient's
   * current rules. A reminder is cancelled when its rule was removed after the
   * job was planned.
   */
  userHasReminderOffset: boolean;
  /** Live stored timezone of the recipient (null when none is chosen yet). */
  userTimeZone: string | null;
  /** Current `users.revision` (null when the recipient has no user row). */
  userRevision: number | null;
  occurrenceFound: boolean;
  occurrenceStatus: OccurrenceStatus | null;
  occurrenceStartsAtMs: number | null;
  occurrenceCourse: Course | null;
  occurrenceRevision: number | null;
  occurrenceKey: string | null;
  occurrenceSummary: string | null;
  occurrenceDescription: string | null;
  occurrenceUrl: string | null;
  sourceFetchedAtMs: number | null;
}

export interface PaceState {
  windowStartedAtMs: number;
  consumed: number;
  cooldownUntilMs: number | null;
}

export const SEND_PACE_WINDOW_MS = 1_000;

function resultRows<T>(result: D1ResultLike<T>): T[] {
  return result.results ?? [];
}

function chunk<T>(items: readonly T[], size: number): T[][] {
  const output: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    output.push(items.slice(index, index + size));
  }
  return output;
}

function toCourse(value: string | null): Course | null {
  return value === 'basic' || value === 'extended' ? value : null;
}

function toStatus(value: string | null): OccurrenceStatus | null {
  return value === 'confirmed' || value === 'cancelled' ? value : null;
}

function nullableNumber(value: unknown): number | null {
  return value === null ? null : Number(value);
}

function nullableString(value: unknown): string | null {
  return value === null ? null : String(value);
}

/** `meta.changes` of a write result, 0 when the driver omits it. */
function reportedChanges(result: D1ResultLike<never> | undefined): number {
  return result?.meta?.changes ?? 0;
}

/**
 * Lock row name for a source. Every custody-guarded statement compares `owner`,
 * `generation` and this name.
 */
function sourceLockName(sourceId: string): string {
  return `source:${sourceId}`;
}

/**
 * The twelve values of one occurrence row, in `OCCURRENCE_COLUMNS` order but
 * without the `revision` literal and `updated_at` that both writers add.
 */
type OccurrenceRowValues = readonly [
  id: string,
  sourceId: string,
  uid: string,
  occurrenceKey: string,
  course: Course,
  startsAtMs: number,
  endsAtMs: number | null,
  summary: string,
  description: string | null,
  url: string | null,
  status: OccurrenceStatus,
  isAllDay: 0 | 1,
];

/** Row values shared by the direct upsert and the staging insert. */
function occurrenceRowValues(write: OccurrenceWrite): OccurrenceRowValues {
  return [
    write.id,
    write.sourceId,
    write.uid,
    write.occurrenceKey,
    write.course,
    write.startsAtMs,
    write.endsAtMs,
    write.summary,
    write.description,
    write.url,
    write.status,
    write.isAllDay ? 1 : 0,
  ];
}

/** Row read by `getUser`; each field is narrowed in the mapper. */
interface UserRow {
  telegram_user_id: unknown;
  chat_id: unknown;
  course: unknown;
  time_zone: unknown;
  active: unknown;
  revision: unknown;
}

/** Row read by `listReminderOffsets`. */
interface ReminderOffsetRow {
  offset_minutes: unknown;
}

/** Row read by `getSource`; each field is narrowed in the mapper. */
interface SourceRow {
  id: unknown;
  kind: unknown;
  etag: unknown;
  last_modified: unknown;
  fetched_at_ms: unknown;
  last_refresh_at_ms: unknown;
  status: unknown;
  revision: unknown;
}

/**
 * Row returned by `claimJobContext`: the job columns plus the correlated user,
 * occurrence and source sub-selects.
 */
interface JobContextRow {
  job_id: unknown;
  kind: unknown;
  telegram_user_id: unknown;
  chat_id: unknown;
  occurrence_id: unknown;
  reminder_offset_minutes: unknown;
  send_at_ms: unknown;
  next_attempt_at_ms: unknown;
  attempt_count: unknown;
  expected_revision: unknown;
  payload_json: unknown;
  source_update_id: unknown;
  last_seen_update_id: unknown;
  user_id: unknown;
  user_active: unknown;
  user_course: unknown;
  user_has_offset: unknown;
  user_time_zone: unknown;
  user_revision: unknown;
  occ_id: unknown;
  occ_status: unknown;
  occ_starts_at_ms: unknown;
  occ_course: unknown;
  occ_revision: unknown;
  occ_key: unknown;
  occ_summary: unknown;
  occ_description: unknown;
  occ_url: unknown;
  source_fetched_at_ms: unknown;
}

/** Row read by `getPaceState`. */
interface PaceStateRow {
  window_started_at_ms: unknown;
  consumed: unknown;
  cooldown_until_ms: unknown;
}

/** Row read by `listUpcomingOccurrences`. */
interface OccurrenceListRow {
  id: unknown;
  occurrence_key: unknown;
  course: unknown;
  starts_at_ms: unknown;
  ends_at_ms: unknown;
  summary: unknown;
  description: unknown;
  url: unknown;
  status: unknown;
}

/** Maps the `JobContextRow` produced by `claimJobContext` into a JobContext. */
function toJobContext(row: JobContextRow): JobContext {
  return {
    jobId: String(row.job_id),
    kind: row.kind === 'command' ? 'command' : 'reminder',
    telegramUserId: Number(row.telegram_user_id),
    chatId: Number(row.chat_id),
    occurrenceId: nullableString(row.occurrence_id),
    reminderOffsetMinutes: nullableNumber(row.reminder_offset_minutes),
    sendAtMs: Number(row.send_at_ms),
    nextAttemptAtMs: Number(row.next_attempt_at_ms),
    attemptCount: Number(row.attempt_count),
    expectedRevision: nullableNumber(row.expected_revision),
    payloadJson: nullableString(row.payload_json),
    sourceUpdateId: nullableNumber(row.source_update_id),
    commandLastSeenUpdateId: nullableNumber(row.last_seen_update_id),
    userFound: row.user_id !== null,
    userActive: Number(row.user_active) === 1,
    userCourse: toCourse(nullableString(row.user_course)),
    userHasReminderOffset: Number(row.user_has_offset) > 0,
    userTimeZone: nullableString(row.user_time_zone),
    userRevision: nullableNumber(row.user_revision),
    occurrenceFound: row.occ_id !== null,
    occurrenceStatus: toStatus(nullableString(row.occ_status)),
    occurrenceStartsAtMs: nullableNumber(row.occ_starts_at_ms),
    occurrenceCourse: toCourse(nullableString(row.occ_course)),
    occurrenceRevision: nullableNumber(row.occ_revision),
    occurrenceKey: nullableString(row.occ_key),
    occurrenceSummary: nullableString(row.occ_summary),
    occurrenceDescription: nullableString(row.occ_description),
    occurrenceUrl: nullableString(row.occ_url),
    sourceFetchedAtMs: nullableNumber(row.source_fetched_at_ms),
  };
}

/**
 * D1 access layer. SQL is prepared and bound in one place (`#statement`), a
 * single statement executes only through `#rows`, `#row` or `#changes`, and
 * statement groups go through `#batch`/`#batchAtomic`, so every execution path
 * tracks the statement budget exactly once.
 *
 * Failure signalling is uniform too: a statement guarded by custody, ownership
 * or eligibility reports rejection as `false` (never as a throw and never by
 * changing rows), while exhausting the armed statement budget throws
 * `StatementBudgetError` before or instead of executing. `activateUser` is the
 * deliberate exception: it throws when its own insert does not round-trip.
 */
export class Repository {
  readonly #db: D1DatabaseLike;
  #budgetLimit: number | null = null;
  #budgetUsed = 0;

  constructor(db: D1DatabaseLike) {
    this.#db = db;
  }

  // ----- statement budget ------------------------------------------------

  /** Arms the per-invocation statement budget and resets the counter. */
  beginInvocation(limit: number): void {
    this.#budgetLimit = limit;
    this.#budgetUsed = 0;
  }

  /** Current statement count for the armed invocation (0 when disarmed). */
  statementsUsed(): number {
    return this.#budgetUsed;
  }

  /** Remaining statements for the armed invocation, or null when disarmed. */
  remainingBudget(): number | null {
    if (this.#budgetLimit === null) {
      return null;
    }
    return Math.max(0, this.#budgetLimit - this.#budgetUsed);
  }

  /** Whether `statements` more statements fit without exceeding the armed budget. */
  canAfford(statements: number): boolean {
    if (this.#budgetLimit === null) {
      return true;
    }
    return this.#budgetUsed + statements <= this.#budgetLimit;
  }

  /** Charges `count` statements and enforces the armed limit. */
  #track(count: number): void {
    this.#budgetUsed += count;
    if (this.#budgetLimit !== null && this.#budgetUsed > this.#budgetLimit) {
      throw new StatementBudgetError(this.#budgetLimit);
    }
  }

  // ----- statement execution ---------------------------------------------

  /** Prepares and binds one statement; the single place SQL is bound. */
  #statement(sql: string, ...params: readonly unknown[]): D1StatementLike {
    return this.#db.prepare(sql).bind(...params);
  }

  /** Executes one statement and returns all rows it produced. */
  async #rows<T = Record<string, unknown>>(
    sql: string,
    ...params: readonly unknown[]
  ): Promise<T[]> {
    const statement = this.#statement(sql, ...params);
    this.#track(1);
    return resultRows<T>(await statement.all<T>());
  }

  /** Executes one statement and returns its first row, or null. */
  async #row<T = Record<string, unknown>>(
    sql: string,
    ...params: readonly unknown[]
  ): Promise<T | null> {
    const statement = this.#statement(sql, ...params);
    this.#track(1);
    return statement.first<T>();
  }

  /** Executes one write statement and returns its reported change count. */
  async #changes(sql: string, ...params: readonly unknown[]): Promise<number> {
    const statement = this.#statement(sql, ...params);
    this.#track(1);
    return reportedChanges(await statement.run());
  }

  /**
   * Runs statements in chunks of at most MAX_D1_BATCH_STATEMENTS. Each chunk is
   * its own batch; use `#batchAtomic` when the group must be one transaction.
   */
  async #batch(statements: readonly D1StatementLike[]): Promise<void> {
    if (statements.length === 0) {
      return;
    }
    for (const group of chunk(statements, MAX_D1_BATCH_STATEMENTS)) {
      this.#track(group.length);
      await this.#db.batch(group);
    }
  }

  /**
   * Executes a small statement group as exactly one D1 batch. A D1 batch is a
   * transaction, so the group is atomic; callers keep the group well under
   * MAX_D1_BATCH_STATEMENTS. The per-statement results are returned so a caller
   * can inspect `meta.changes` of a guard statement.
   */
  async #batchAtomic(statements: readonly D1StatementLike[]): Promise<readonly unknown[]> {
    if (statements.length === 0) {
      return [];
    }
    this.#track(statements.length);
    return this.#db.batch(statements);
  }

  // ----- users -----------------------------------------------------------

  async getUser(telegramUserId: number): Promise<UserRecord | null> {
    const row = await this.#row<UserRow>(
      'SELECT * FROM users WHERE telegram_user_id = ?',
      telegramUserId,
    );
    if (row === null) {
      return null;
    }
    return {
      telegramUserId: Number(row.telegram_user_id),
      chatId: Number(row.chat_id),
      course: toCourse(String(row.course)) ?? 'basic',
      timeZone: row.time_zone === null || row.time_zone === undefined ? null : String(row.time_zone),
      active: Number(row.active) === 1,
      revision: Number(row.revision),
    };
  }

  /**
   * Finding 3 (repo part): bumps `users.revision` ONLY when activating
   * (active 0→1, including a fresh insert) or when `chat_id` changes. A repeat
   * `/start` with the same chat leaves revision and updated_at untouched, so a
   * webhook retry cannot increment the revision twice.
   *
   * FIX-ORDER: when `commandUpdateId` is provided, both the fresh insert and
   * the reactivation update run behind `COMMAND_CURRENT`, so a suspended older
   * `/start` can neither insert a user nor reactivate one that a newer command
   * already deactivated. The guarded overload returns null when the write was
   * rejected and no user row exists, making the rejection observable without
   * throwing (the caller must not enqueue a reply then).
   *
   * The standard rule set is seeded ONLY on the fresh insert; reactivating an
   * existing row goes through a separate guarded UPDATE that never touches
   * `user_reminder_offsets`. A user who cleared every rule therefore keeps an
   * empty set across `/stop` and `/start`. `INSERT OR IGNORE` reports 0 changes
   * when the row already exists, which is what distinguishes the two paths.
   */
  async activateUser(
    telegramUserId: number,
    chatId: number,
    now: number,
  ): Promise<UserRecord>;
  async activateUser(
    telegramUserId: number,
    chatId: number,
    now: number,
    commandUpdateId: number,
  ): Promise<UserRecord | null>;
  async activateUser(
    telegramUserId: number,
    chatId: number,
    now: number,
    commandUpdateId?: number,
  ): Promise<UserRecord | null> {
    const guard = this.#commandGuard(telegramUserId, commandUpdateId);
    const inserted = await this.#changes(
      `INSERT OR IGNORE INTO users (telegram_user_id, chat_id, course, active, revision, created_at_ms, updated_at_ms)
       SELECT ?, ?, 'basic', 1, 1, ?, ?
       WHERE ${COMMAND_CURRENT}`,
      telegramUserId,
      chatId,
      now,
      now,
      ...guard,
    );
    if (inserted === 0) {
      // Existing row: reactivate it without re-adding reminder rules.
      await this.#changes(
        `UPDATE users SET
           chat_id = ?,
           active = 1,
           revision = CASE
             WHEN active = 0 OR chat_id <> ? THEN revision + 1
             ELSE revision
           END,
           updated_at_ms = CASE
             WHEN active = 0 OR chat_id <> ? THEN ?
             ELSE updated_at_ms
           END
         WHERE telegram_user_id = ? AND ${COMMAND_CURRENT}`,
        chatId,
        chatId,
        chatId,
        now,
        telegramUserId,
        ...guard,
      );
    }
    const user = await this.getUser(telegramUserId);
    if (user === null) {
      if (commandUpdateId !== undefined) {
        return null;
      }
      throw new Error('activateUser failed to persist the user');
    }
    return user;
  }

  /** Current rule set of a user, largest lead time first. Empty when none. */
  async listReminderOffsets(telegramUserId: number): Promise<number[]> {
    const rows = await this.#rows<ReminderOffsetRow>(
      `SELECT offset_minutes FROM user_reminder_offsets
       WHERE telegram_user_id = ?
       ORDER BY offset_minutes DESC`,
      telegramUserId,
    );
    return rows.map((row) => Number(row.offset_minutes));
  }

  /** Finding 3 (repo part): true when a command job with this dedup key exists. */
  async hasCommandJob(dedupKey: string): Promise<boolean> {
    const row = await this.#row<{ one: unknown }>(
      'SELECT 1 AS one FROM outbound_jobs WHERE dedup_key = ? LIMIT 1',
      dedupKey,
    );
    return row !== null;
  }

  /**
   * Finding 4 (repo part): monotonic max upsert of the per-user/chat command
   * ordering state. Out-of-order or duplicate updates never move
   * `last_update_id` backwards; only a newer update_id adopts its chat_id and
   * timestamp. Works for users with no subscription row.
   */
  async recordCommandUpdate(
    telegramUserId: number,
    chatId: number,
    updateId: number,
    now: number,
  ): Promise<void> {
    await this.#changes(
      `INSERT INTO user_command_state (telegram_user_id, chat_id, last_update_id, updated_at_ms)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(telegram_user_id) DO UPDATE SET
         chat_id = CASE
           WHEN excluded.last_update_id >= user_command_state.last_update_id
             THEN excluded.chat_id
           ELSE user_command_state.chat_id
         END,
         last_update_id = MAX(user_command_state.last_update_id, excluded.last_update_id),
         updated_at_ms = CASE
           WHEN excluded.last_update_id >= user_command_state.last_update_id
             THEN excluded.updated_at_ms
           ELSE user_command_state.updated_at_ms
         END`,
      telegramUserId,
      chatId,
      updateId,
      now,
    );
  }

  /**
   * C1 (repo part): atomic conditional max-upsert. One statement performs the
   * same max upsert as `recordCommandUpdate` and reports whether this update
   * is now the max (`updateId >= previous max`). True for first contact (no
   * row), for a newer update and for a same-update retry (>=); false for a
   * strictly older update, which leaves the stored max, chat and timestamp
   * untouched. Concurrent overlap may both proceed (accepted, documented):
   * per-statement atomicity plus send-time supersession remain the backstops.
   */
  async claimCommandUpdate(
    telegramUserId: number,
    chatId: number,
    updateId: number,
    now: number,
  ): Promise<boolean> {
    const row = await this.#row<{ last_update_id: unknown }>(
      `INSERT INTO user_command_state (telegram_user_id, chat_id, last_update_id, updated_at_ms)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(telegram_user_id) DO UPDATE SET
         chat_id = CASE
           WHEN excluded.last_update_id >= user_command_state.last_update_id
             THEN excluded.chat_id
           ELSE user_command_state.chat_id
         END,
         last_update_id = MAX(user_command_state.last_update_id, excluded.last_update_id),
         updated_at_ms = CASE
           WHEN excluded.last_update_id >= user_command_state.last_update_id
             THEN excluded.updated_at_ms
           ELSE user_command_state.updated_at_ms
         END
       RETURNING last_update_id`,
      telegramUserId,
      chatId,
      updateId,
      now,
    );
    if (row === null) {
      return false;
    }
    return Number(row.last_update_id) === updateId;
  }

  /**
   * C2 (repo part): callback-answer dedup. Insert-or-ignore BEFORE answering;
   * true means this caller inserted and must answer, false means the query was
   * already answered and the caller must skip. Best-effort: an answer that
   * fails to send is still considered answered for webhook-retry purposes.
   */
  async claimCallbackAnswer(
    callbackQueryId: string,
    updateId: number,
    now: number,
  ): Promise<boolean> {
    const changes = await this.#changes(
      `INSERT OR IGNORE INTO callback_answers (query_id, update_id, answered_at_ms)
       VALUES (?, ?, ?)`,
      callbackQueryId,
      updateId,
      now,
    );
    return changes > 0;
  }

  /**
   * Finding 4 (repo part): highest seen command update for a user, or null
   * when never observed. Consumers compare `lastUpdateId` against a job's
   * `sourceUpdateId`; `recordCommandUpdate` is the only writer, so the value
   * is monotonic per user.
   */
  async getLastCommandUpdate(
    telegramUserId: number,
  ): Promise<{ chatId: number | null; lastUpdateId: number } | null> {
    const row = await this.#row<{ chat_id: unknown; last_update_id: unknown }>(
      'SELECT chat_id, last_update_id FROM user_command_state WHERE telegram_user_id = ?',
      telegramUserId,
    );
    if (row === null) {
      return null;
    }
    return {
      chatId: row.chat_id === null ? null : Number(row.chat_id),
      lastUpdateId: Number(row.last_update_id),
    };
  }

  /**
   * FIX-ORDER: guarded by `COMMAND_CURRENT` when `commandUpdateId` is given,
   * so a suspended callback cannot overwrite a course a newer command already
   * set. Returns whether the row was written (false when stale, unchanged or
   * the user is missing), so a rejected guard is observable to the caller.
   */
  async setUserCourse(
    telegramUserId: number,
    course: Course,
    now: number,
    commandUpdateId?: number,
  ): Promise<boolean> {
    const changes = await this.#changes(
      `UPDATE users SET course = ?, revision = revision + 1, updated_at_ms = ?
       WHERE telegram_user_id = ? AND course <> ? AND ${COMMAND_CURRENT}`,
      course,
      now,
      telegramUserId,
      course,
      commandUpdateId ?? null,
      telegramUserId,
      commandUpdateId ?? null,
      commandUpdateId ?? null,
    );
    return changes > 0;
  }

  /** Bind values of `COMMAND_CURRENT` for a user and optional command update. */
  #commandGuard(telegramUserId: number, commandUpdateId?: number): readonly unknown[] {
    return [
      commandUpdateId ?? null,
      telegramUserId,
      commandUpdateId ?? null,
      commandUpdateId ?? null,
    ];
  }

  /** One guarded, bounded insert-or-ignore of a single reminder rule. */
  #insertReminderOffsetStatement(
    telegramUserId: number,
    offset: number,
    now: number,
    commandUpdateId?: number,
  ): D1StatementLike {
    return this.#statement(
      `INSERT OR IGNORE INTO user_reminder_offsets (telegram_user_id, offset_minutes, created_at_ms)
       SELECT ?, ?, ? WHERE ${COMMAND_CURRENT}
         AND (? = ${START_REMINDER_OFFSET_MINUTES}
              OR (SELECT COUNT(*) FROM user_reminder_offsets
                    WHERE telegram_user_id = ? AND offset_minutes > 0) < ?)`,
      telegramUserId,
      offset,
      now,
      ...this.#commandGuard(telegramUserId, commandUpdateId),
      offset,
      telegramUserId,
      MAX_REMINDER_RULES_PER_USER,
    );
  }

  /** Guarded statement that bumps `users.revision` only when `extraPredicate` holds. */
  #revisionBumpStatement(
    telegramUserId: number,
    now: number,
    commandUpdateId: number | undefined,
    extraPredicate: string,
    extraParams: readonly unknown[],
  ): D1StatementLike {
    return this.#statement(
      `UPDATE users SET revision = revision + 1, updated_at_ms = ?
       WHERE telegram_user_id = ? AND ${extraPredicate} AND ${COMMAND_CURRENT}`,
      now,
      telegramUserId,
      ...extraParams,
      ...this.#commandGuard(telegramUserId, commandUpdateId),
    );
  }

  /**
   * Replaces the whole rule set atomically. Invalid values, more than
   * `MAX_REMINDER_RULES_PER_USER` entries or duplicates are rejected before any
   * statement runs (duplicates are collapsed, not rejected). The user revision
   * is bumped in the same batch so an older settings reply is superseded, and
   * every statement is guarded by `COMMAND_CURRENT` when a command update id is
   * given. Returns whether the stored set changed.
   */
  async setUserReminderOffsets(
    telegramUserId: number,
    offsets: readonly number[],
    now: number,
    commandUpdateId?: number,
  ): Promise<boolean> {
    const normalized = normalizeReminderOffsets(offsets);
    if (normalized === null) {
      return false;
    }
    const statements: D1StatementLike[] = [
      this.#revisionBumpStatement(telegramUserId, now, commandUpdateId, '1 = 1', []),
      this.#statement(
        `DELETE FROM user_reminder_offsets
         WHERE telegram_user_id = ? AND ${COMMAND_CURRENT}`,
        telegramUserId,
        ...this.#commandGuard(telegramUserId, commandUpdateId),
      ),
    ];
    // SQLite's JSON1 extension is part of D1. One bound JSON value keeps a
    // replacement of the maximum 100 rules within the batch and parameter
    // limits, while the surrounding batch keeps delete and insert atomic.
    statements.push(
      this.#statement(
        `INSERT OR IGNORE INTO user_reminder_offsets (telegram_user_id, offset_minutes, created_at_ms)
         SELECT ?, CAST(value AS INTEGER), ?
         FROM json_each(?)
         WHERE ${COMMAND_CURRENT}`,
        telegramUserId,
        now,
        JSON.stringify(normalized),
        ...this.#commandGuard(telegramUserId, commandUpdateId),
      ),
    );
    const results = await this.#batchAtomic(statements);
    return results.some(
      (result) => reportedChanges(result as D1ResultLike<never>) > 0,
    );
  }

  /**
   * Adds one rule if it is valid, not already present and the user is below the
   * per-user cap. Returns true only when a new rule row was inserted.
   */
  async addReminderOffset(
    telegramUserId: number,
    offset: ReminderOffsetMinutes,
    now: number,
    commandUpdateId?: number,
  ): Promise<boolean> {
    if (!isStoredReminderOffset(offset)) {
      return false;
    }
    const results = await this.#batchAtomic([
      this.#revisionBumpStatement(
        telegramUserId,
        now,
        commandUpdateId,
        `(? = ${START_REMINDER_OFFSET_MINUTES}
            OR (SELECT COUNT(*) FROM user_reminder_offsets
                  WHERE telegram_user_id = ? AND offset_minutes > 0) < ?)
           AND NOT EXISTS (
             SELECT 1 FROM user_reminder_offsets
             WHERE telegram_user_id = ? AND offset_minutes = ?
           )`,
        [offset, telegramUserId, MAX_REMINDER_RULES_PER_USER, telegramUserId, offset],
      ),
      this.#insertReminderOffsetStatement(telegramUserId, offset, now, commandUpdateId),
    ]);
    return reportedChanges(results[1] as D1ResultLike<never> | undefined) > 0;
  }

  /**
   * Removes one rule. Idempotent: a missing rule changes nothing and reports
   * false, so a stale or repeated callback never mutates.
   */
  async removeReminderOffset(
    telegramUserId: number,
    offset: ReminderOffsetMinutes,
    now: number,
    commandUpdateId?: number,
  ): Promise<boolean> {
    if (!isStoredReminderOffset(offset)) {
      return false;
    }
    const results = await this.#batchAtomic([
      this.#revisionBumpStatement(
        telegramUserId,
        now,
        commandUpdateId,
        'EXISTS (SELECT 1 FROM user_reminder_offsets WHERE telegram_user_id = ? AND offset_minutes = ?)',
        [telegramUserId, offset],
      ),
      this.#statement(
        `DELETE FROM user_reminder_offsets
         WHERE telegram_user_id = ? AND offset_minutes = ? AND ${COMMAND_CURRENT}`,
        telegramUserId,
        offset,
        ...this.#commandGuard(telegramUserId, commandUpdateId),
      ),
    ]);
    return reportedChanges(results[1] as D1ResultLike<never> | undefined) > 0;
  }

  /**
   * Renames one rule atomically. The guarded UPDATE only runs while the source
   * rule exists and the target does not, so a failed edit (missing source,
   * target already present, stale command) leaves the source rule in place:
   * there is no window in which the old rule is removed before the new one is
   * written. Returns true only when the row actually moved.
   */
  async editReminderOffset(
    telegramUserId: number,
    from: ReminderOffsetMinutes,
    to: ReminderOffsetMinutes,
    now: number,
    commandUpdateId?: number,
  ): Promise<boolean> {
    if (!isValidReminderOffset(from) || !isValidReminderOffset(to) || from === to) {
      return false;
    }
    const sourceExists =
      'EXISTS (SELECT 1 FROM user_reminder_offsets WHERE telegram_user_id = ? AND offset_minutes = ?)';
    const targetMissing =
      'NOT EXISTS (SELECT 1 FROM user_reminder_offsets WHERE telegram_user_id = ? AND offset_minutes = ?)';
    const results = await this.#batchAtomic([
      this.#revisionBumpStatement(
        telegramUserId,
        now,
        commandUpdateId,
        `${sourceExists} AND ${targetMissing}`,
        [telegramUserId, from, telegramUserId, to],
      ),
      this.#statement(
        `UPDATE user_reminder_offsets SET offset_minutes = ?
         WHERE telegram_user_id = ? AND offset_minutes = ?
           AND ${targetMissing}
           AND ${COMMAND_CURRENT}`,
        to,
        telegramUserId,
        from,
        telegramUserId,
        to,
        ...this.#commandGuard(telegramUserId, commandUpdateId),
      ),
    ]);
    return reportedChanges(results[1] as D1ResultLike<never> | undefined) > 0;
  }

  /**
   * Stores the canonical IANA timezone, completing onboarding (`NULL` → value)
   * or changing an existing choice. The value is validated and canonicalized
   * with native `Intl` at this boundary as well, so no caller can persist an
   * unusable zone: empty, whitespace-only, overlong, offset and malformed
   * values report false without a write. Like `setUserCourse`, the write is
   * guarded by `COMMAND_CURRENT` when `commandUpdateId` is given, so a
   * suspended older command cannot overwrite a newer choice. `revision` (and
   * `updated_at`) move only when the canonical value actually changes, so an
   * identical webhook retry is idempotent and does not bump the revision twice;
   * a write that changes nothing reports false. The subscription state
   * (`active`) is never touched here.
   */
  async setUserTimeZone(
    telegramUserId: number,
    timeZone: string,
    now: number,
    commandUpdateId?: number,
  ): Promise<boolean> {
    const canonical = normalizeTimeZone(timeZone);
    if (canonical === null) {
      return false;
    }
    const changes = await this.#changes(
      `UPDATE users SET time_zone = ?, revision = revision + 1, updated_at_ms = ?
       WHERE telegram_user_id = ? AND (time_zone IS NULL OR time_zone <> ?) AND ${COMMAND_CURRENT}`,
      canonical,
      now,
      telegramUserId,
      canonical,
      commandUpdateId ?? null,
      telegramUserId,
      commandUpdateId ?? null,
      commandUpdateId ?? null,
    );
    return changes > 0;
  }

  /**
   * FIX-ORDER: guarded by `COMMAND_CURRENT` when `commandUpdateId` is given,
   * so a suspended older `/stop` cannot deactivate a subscription a newer
   * `/start` already reactivated. Returns whether the user was deactivated.
   */
  async deactivateUser(
    telegramUserId: number,
    now: number,
    commandUpdateId?: number,
  ): Promise<boolean> {
    const changes = await this.#changes(
      `UPDATE users SET active = 0, revision = revision + 1, updated_at_ms = ?
       WHERE telegram_user_id = ? AND active = 1 AND ${COMMAND_CURRENT}`,
      now,
      telegramUserId,
      commandUpdateId ?? null,
      telegramUserId,
      commandUpdateId ?? null,
      commandUpdateId ?? null,
    );
    return changes > 0;
  }

  /**
   * FIX-ORDER: guarded by `COMMAND_CURRENT` when `commandUpdateId` is given,
   * so a suspended older command cannot cancel jobs a newer command queued.
   * Returns the number of cancelled rows; a rejected guard returns 0.
   */
  async cancelPendingJobsForUser(
    telegramUserId: number,
    now: number,
    commandUpdateId?: number,
  ): Promise<number> {
    return this.#changes(
      `UPDATE outbound_jobs
       SET status = 'cancelled', lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ?
       WHERE telegram_user_id = ? AND status IN ('pending', 'enqueued')
         AND ${COMMAND_CURRENT}`,
      now,
      telegramUserId,
      commandUpdateId ?? null,
      telegramUserId,
      commandUpdateId ?? null,
      commandUpdateId ?? null,
    );
  }

  /**
   * FIX-ORDER handler-side freshness check: true when no command update newer
   * than `updateId` has been recorded for this user (a missing state row counts
   * as current, so first-contact guidance stays deliverable). Handlers re-check
   * it immediately before enqueueing a reply; the SQL guards above are the
   * statement-level backstop for mutations that already started.
   */
  async isCommandUpdateCurrent(telegramUserId: number, updateId: number): Promise<boolean> {
    const row = await this.#row<{ current: unknown }>(
      `SELECT COALESCE(
         (SELECT last_update_id FROM user_command_state WHERE telegram_user_id = ?),
         ?
       ) <= ? AS current`,
      telegramUserId,
      updateId,
      updateId,
    );
    return row !== null && Number(row.current) === 1;
  }

  // ----- sources ---------------------------------------------------------

  /** Inserts the source row if it does not exist yet (occurrences FK target). */
  async ensureSource(id: string, kind: Course, now: number): Promise<void> {
    await this.#changes(
      `INSERT INTO sources (${SOURCE_COLUMNS})
       VALUES (?, ?, NULL, NULL, NULL, NULL, 'unknown', NULL, 1, ?)
       ON CONFLICT(id) DO NOTHING`,
      id,
      kind,
      now,
    );
  }

  async getSource(id: string): Promise<SourceRecord | null> {
    const row = await this.#row<SourceRow>('SELECT * FROM sources WHERE id = ?', id);
    if (row === null) {
      return null;
    }
    return {
      id: String(row.id),
      kind: toCourse(String(row.kind)) ?? 'basic',
      etag: row.etag === null || row.etag === undefined ? null : String(row.etag),
      lastModified:
        row.last_modified === null || row.last_modified === undefined
          ? null
          : String(row.last_modified),
      fetchedAtMs: row.fetched_at_ms === null ? null : Number(row.fetched_at_ms),
      lastRefreshAtMs:
        row.last_refresh_at_ms === null || row.last_refresh_at_ms === undefined
          ? null
          : Number(row.last_refresh_at_ms),
      status: row.status === 'ok' || row.status === 'error' ? row.status : 'unknown',
      revision: Number(row.revision),
    };
  }

  /**
   * Acquires (or renews) the source lease. Returns the attempt identity, or null
   * when another owner holds a live lease. The generation increments on every
   * acquisition, so a specific attempt can be recognised later.
   */
  async acquireSourceLease(
    id: string,
    owner: string,
    now: number,
    leaseMs: number,
  ): Promise<SourceLease | null> {
    const row = await this.#row<{ generation: unknown }>(
      `INSERT INTO locks (name, owner, generation, expires_at_ms, updated_at_ms)
       VALUES (?, ?, 1, ?, ?)
       ON CONFLICT(name) DO UPDATE SET
         owner = excluded.owner,
         generation = locks.generation + 1,
         expires_at_ms = excluded.expires_at_ms,
         updated_at_ms = excluded.updated_at_ms
       WHERE locks.expires_at_ms <= excluded.updated_at_ms OR locks.owner = excluded.owner
       RETURNING generation`,
      sourceLockName(id),
      owner,
      now + leaseMs,
      now,
    );
    if (row === null) {
      return null;
    }
    return { owner, generation: Number(row.generation) };
  }

  /**
   * Durable cooldown for operator alerts: returns true only when no unexpired
   * lease with this name exists. Deliberately budget-exempt (a direct statement
   * without `#track`): it runs in failure paths whose invocation budget may
   * already be spent, and an alert must never fail for budget reasons. The
   * random owner makes the row non-renewable, so the cooldown always expires.
   */
  async tryAcquireAdminAlertLease(name: string, now: number, ttlMs: number): Promise<boolean> {
    const statement = this.#statement(
      `INSERT INTO locks (name, owner, generation, expires_at_ms, updated_at_ms)
       VALUES (?, ?, 1, ?, ?)
       ON CONFLICT(name) DO UPDATE SET
         owner = excluded.owner,
         generation = locks.generation + 1,
         expires_at_ms = excluded.expires_at_ms,
         updated_at_ms = excluded.updated_at_ms
       WHERE locks.expires_at_ms <= excluded.updated_at_ms
       RETURNING generation`,
      name,
      crypto.randomUUID(),
      now + ttlMs,
      now,
    );
    return (await statement.first<{ generation: unknown }>()) !== null;
  }

  /** Attempt identity used for staging rows of this acquisition. */
  #attemptId(sourceId: string, lease: SourceLease): string {
    return `${sourceId}#${lease.generation}`;
  }

  /**
   * Records freshness for a 304 (no snapshot change). `clock` is read here, so
   * the live-lease guard is evaluated against the time of this write: an attempt
   * whose lease lapsed cannot relabel the source, even when no newer acquisition
   * has changed its generation. Returns false when custody was lost.
   */
  async recordSourceFresh(
    id: string,
    kind: Course,
    lease: SourceLease,
    etag: string | null,
    lastModified: string | null,
    clock: Clock,
  ): Promise<boolean> {
    const now = clock();
    const lockName = sourceLockName(id);
    const changes = await this.#changes(
      `INSERT INTO sources (${SOURCE_COLUMNS})
       SELECT ?, ?, ?, ?, ?, ?, 'ok', NULL, 1, ?
       WHERE ${LOCKED_LIVE}
       ON CONFLICT(id) DO UPDATE SET
         etag = excluded.etag,
         last_modified = excluded.last_modified,
         fetched_at_ms = excluded.fetched_at_ms,
         last_success_at_ms = excluded.last_success_at_ms,
         status = 'ok',
         last_error_code = NULL,
         updated_at_ms = excluded.updated_at_ms
       WHERE ${LOCKED_LIVE}`,
      id,
      kind,
      etag,
      lastModified,
      now,
      now,
      now,
      lockName,
      lease.owner,
      lease.generation,
      now,
      lockName,
      lease.owner,
      lease.generation,
      now,
    );
    return changes > 0;
  }

  /**
   * Records a fetch/parse failure. `clock` is read here for the same reason as
   * `recordSourceFresh`: an expired attempt must not change the source status.
   */
  async recordSourceError(
    id: string,
    kind: Course,
    lease: SourceLease,
    code: string,
    clock: Clock,
  ): Promise<boolean> {
    const now = clock();
    const lockName = sourceLockName(id);
    const changes = await this.#changes(
      `INSERT INTO sources (${SOURCE_COLUMNS})
       SELECT ?, ?, NULL, NULL, NULL, NULL, 'error', ?, 1, ?
       WHERE ${LOCKED_LIVE}
       ON CONFLICT(id) DO UPDATE SET
         status = 'error',
         last_error_code = excluded.last_error_code,
         updated_at_ms = excluded.updated_at_ms
       WHERE ${LOCKED_LIVE}`,
      id,
      kind,
      code,
      now,
      lockName,
      lease.owner,
      lease.generation,
      now,
      lockName,
      lease.owner,
      lease.generation,
      now,
    );
    return changes > 0;
  }

  // ----- occurrences -----------------------------------------------------

  /**
   * Multi-row upsert statements. Occurrences are written in wide INSERT
   * statements (bounded by MAX_SQL_PARAMS) so refreshing a large snapshot stays
   * well inside the per-invocation statement budget instead of issuing one
   * statement per occurrence.
   */
  #occurrenceUpsertStatements(
    writes: readonly OccurrenceWrite[],
    now: number,
  ): D1StatementLike[] {
    const statements: D1StatementLike[] = [];
    for (const group of chunk(writes, UPSERT_ROWS_PER_STATEMENT)) {
      const tuples = group.map(() => UPSERT_TUPLE).join(', ');
      const values: unknown[] = [];
      for (const write of group) {
        values.push(...occurrenceRowValues(write), now);
      }
      statements.push(
        this.#statement(
          `INSERT INTO occurrences (${OCCURRENCE_COLUMNS})
           VALUES ${tuples}
           ${OCCURRENCE_UPSERT_CONFLICT}`,
          ...values,
        ),
      );
    }
    return statements;
  }

  async upsertOccurrences(writes: readonly OccurrenceWrite[], now: number): Promise<void> {
    await this.#batch(this.#occurrenceUpsertStatements(writes, now));
  }

  /**
   * Statements `applySnapshot` needs for this many writes: the publication
   * batch (`PUBLISH_STATEMENT_COUNT` statements) plus the chunked staging
   * inserts.
   */
  snapshotCost(writeCount: number): number {
    return PUBLISH_STATEMENT_COUNT + Math.ceil(writeCount / STAGING_ROWS_PER_STATEMENT);
  }

  /**
   * Publishes a validated snapshot atomically for the given attempt.
   *
   * The snapshot is first written to `occurrence_staging` under the attempt id
   * (chunked, invisible to readers). The publication is then one transaction:
   * a custody probe followed by the upsert, the horizon deletions, the source
   * freshness/refresh metadata and the staging sweep — every statement before
   * the sweep guarded by the current owner + generation + a live lease. A stale
   * attempt therefore cannot switch, delete or relabel anything, and its
   * staging rows are swept.
   *
   * Deletions are confined to the validated horizon: only rows with
   * `starts_at_ms >= MAX(horizon.startMs, publishedAt)` and
   * `starts_at_ms <= horizon.endMs` (both ends inclusive, exactly like the
   * expansion that produced the snapshot) can disappear. `publishedAt` is the
   * clock read after staging — the same value the custody guard compares
   * against — so rows that lie in the past at publication time survive
   * regardless of what the horizon covers.
   *
   * Returns the committed publication (`upserted` rows and the deletion count
   * taken from the DELETE statement's `meta.changes` inside the same batch), or
   * null when the attempt no longer holds a live lease and nothing was applied.
   * The whole publication is preflighted against the remaining invocation
   * budget: if it cannot fit, this throws before any mutation so the previous
   * snapshot stays visible.
   *
   * `clock` is read again after the staging batches have run, and that later
   * value is what the in-transaction custody guard compares against: an attempt
   * whose lease expired while it was staging must not publish, and reading the
   * time only once up front would silently accept it.
   */
  async applySnapshot(
    sourceId: string,
    writes: readonly OccurrenceWrite[],
    lease: SourceLease,
    cursor: SnapshotCursor,
    clock: Clock,
    horizon: DeletionHorizon,
  ): Promise<SnapshotPublication | null> {
    const needed = this.snapshotCost(writes.length);
    if (!this.canAfford(needed)) {
      throw new StatementBudgetError(this.#budgetLimit ?? 0);
    }
    const attemptId = this.#attemptId(sourceId, lease);
    const stagedAt = clock();
    await this.#batch(this.#occurrenceStagingStatements(attemptId, writes, stagedAt));
    const publishedAt = clock();
    const results = await this.#batchAtomic(
      this.#occurrencePublishStatements(sourceId, attemptId, lease, publishedAt, cursor, horizon),
    );
    const custodyProbe = results[0] as D1ResultLike<never> | undefined;
    if (reportedChanges(custodyProbe) === 0) {
      return null;
    }
    return {
      upserted: writes.length,
      deleted: reportedChanges(results[HORIZON_DELETE_INDEX] as D1ResultLike<never> | undefined),
    };
  }

  /** Wide staging INSERTs; a partially staged snapshot is never visible. */
  #occurrenceStagingStatements(
    attemptId: string,
    writes: readonly OccurrenceWrite[],
    now: number,
  ): D1StatementLike[] {
    const statements: D1StatementLike[] = [];
    for (const group of chunk(writes, STAGING_ROWS_PER_STATEMENT)) {
      const tuples = group.map(() => STAGING_TUPLE).join(', ');
      const values: unknown[] = [];
      for (const write of group) {
        values.push(attemptId, ...occurrenceRowValues(write), now);
      }
      statements.push(
        this.#statement(
          `INSERT INTO occurrence_staging (
             attempt_id, id, source_id, uid, occurrence_key, course, starts_at_ms, ends_at_ms,
             summary, description, url, status, is_all_day, updated_at_ms
           ) VALUES ${tuples}`,
          ...values,
        ),
      );
    }
    return statements;
  }

  /**
   * The atomic switch, in `PUBLISH_STEPS` order:
   *
   * 1. `custodyProbe` - a positive `meta.changes` proves this exact attempt
   *    still holds a live lease; the batch is one transaction, so every later
   *    statement runs under the same custody and this result is the caller's.
   * 2. `stagedUpsert` - copies the attempt's staged rows into `occurrences`,
   *    keeping revisions stable for unchanged content.
   * 3. `horizonDelete` - removes horizon rows this snapshot no longer contains,
   *    bounded by the validated inclusive horizon and clamped to `now`.
   * 4. `cursorUpdate` - writes the HTTP validators and fetch/success timestamps.
   * 5. `refreshMarker` - marks the full re-expansion.
   * 6. `stagingSweep` - drops this attempt's staging rows plus rows of any
   *    attempt whose lease has been dead for SOURCE_LEASE_MS. Only a live
   *    attempt can stage newer rows, so a live attempt is never swept.
   *
   * Steps 1-5 are guarded by the attempt's owner, generation and a live lease;
   * a stale attempt cannot switch, delete or relabel anything.
   */
  #occurrencePublishStatements(
    sourceId: string,
    attemptId: string,
    lease: SourceLease,
    now: number,
    cursor: SnapshotCursor,
    horizon: DeletionHorizon,
  ): D1StatementLike[] {
    const lockName = sourceLockName(sourceId);
    const live = [lockName, lease.owner, lease.generation, now] as const;
    const statements: Record<PublishStep, D1StatementLike> = {
      custodyProbe: this.#statement(
        `UPDATE locks SET updated_at_ms = updated_at_ms
         WHERE name = ? AND owner = ? AND generation = ? AND expires_at_ms > ?`,
        ...live,
      ),
      stagedUpsert: this.#statement(
        `INSERT INTO occurrences (${OCCURRENCE_COLUMNS})
         SELECT id, source_id, uid, occurrence_key, course, starts_at_ms, ends_at_ms,
                summary, description, url, status, is_all_day, 1, updated_at_ms
         FROM occurrence_staging
         WHERE attempt_id = ? AND ${LOCKED_LIVE}
         ${OCCURRENCE_UPSERT_CONFLICT}`,
        attemptId,
        ...live,
      ),
      horizonDelete: this.#statement(
        `DELETE FROM occurrences
         WHERE source_id = ?
           AND starts_at_ms >= MAX(?, ?)
           AND starts_at_ms <= ?
           AND id NOT IN (SELECT id FROM occurrence_staging WHERE attempt_id = ?)
           AND ${LOCKED_LIVE}`,
        sourceId,
        horizon.startMs,
        now,
        horizon.endMs,
        attemptId,
        ...live,
      ),
      cursorUpdate: this.#statement(
        `UPDATE sources
         SET etag = ?, last_modified = ?, fetched_at_ms = ?, last_success_at_ms = ?,
             status = 'ok', last_error_code = NULL, updated_at_ms = ?
         WHERE id = ? AND ${LOCKED_LIVE}`,
        cursor.etag,
        cursor.lastModified,
        now,
        now,
        now,
        sourceId,
        ...live,
      ),
      refreshMarker: this.#statement(
        `UPDATE sources SET last_refresh_at_ms = ?
         WHERE id = ? AND ${LOCKED_LIVE}`,
        now,
        sourceId,
        ...live,
      ),
      stagingSweep: this.#statement(
        `DELETE FROM occurrence_staging
         WHERE source_id = ? AND (attempt_id = ? OR updated_at_ms <= ?)`,
        sourceId,
        attemptId,
        now - SOURCE_LEASE_MS,
      ),
    };
    return PUBLISH_STEPS.map((step) => statements[step]);
  }

  /** Highest materialized occurrence start for a source, or null when empty. */
  async getMaxOccurrenceStart(sourceId: string): Promise<number | null> {
    const row = await this.#row<{ max_start: unknown }>(
      'SELECT MAX(starts_at_ms) AS max_start FROM occurrences WHERE source_id = ?',
      sourceId,
    );
    if (row === null || row.max_start === null || row.max_start === undefined) {
      return null;
    }
    return Number(row.max_start);
  }

  async listUpcomingOccurrences(course: Course, now: number, limit: number): Promise<OccurrenceView[]> {
    const result = await this.#rows<OccurrenceListRow>(
      `SELECT id, occurrence_key, course, starts_at_ms, ends_at_ms, summary, description, url, status
       FROM occurrences
       WHERE course = ? AND status = 'confirmed' AND starts_at_ms >= ?
       ORDER BY starts_at_ms ASC
       LIMIT ?`,
      course,
      now,
      limit,
    );
    return result.map((row) => ({
      id: String(row.id),
      occurrenceKey: String(row.occurrence_key),
      course: toCourse(String(row.course)) ?? 'basic',
      startsAtMs: Number(row.starts_at_ms),
      endsAtMs: nullableNumber(row.ends_at_ms),
      summary: String(row.summary),
      description: nullableString(row.description),
      url: nullableString(row.url),
      status: toStatus(String(row.status)) ?? 'confirmed',
    }));
  }

  // ----- jobs ------------------------------------------------------------

  async insertCommandJob(input: {
    id: string;
    telegramUserId: number;
    chatId: number;
    payloadJson: string;
    dedupKey: string;
    sendAtMs: number;
    now: number;
    /**
     * Subscription revision this reply was composed for; a later `/stop` or
     * settings change supersedes it. `null` means the recipient has no user row
     * yet (first contact) and the reply must still be deliverable.
     */
    expectedRevision: number | null;
    /**
     * Finding 4: Telegram update_id that produced this command. Stored in
     * `source_update_id`; omitted/0/NULL legacy rows are treated as drainable
     * (deliverable) by the ordering guard.
     */
    sourceUpdateId?: number | null;
  }): Promise<void> {
    await this.#changes(
      `INSERT INTO outbound_jobs (
         id, kind, telegram_user_id, chat_id, occurrence_id, reminder_offset_minutes,
         send_at_ms, next_attempt_at_ms, status, attempt_count, expected_revision,
         payload_json, dedup_key, source_update_id, created_at_ms, updated_at_ms
       ) VALUES (?, 'command', ?, ?, NULL, NULL, ?, ?, 'pending', 0, ?, ?, ?, ?, ?, ?)
       ON CONFLICT(dedup_key) DO NOTHING`,
      input.id,
      input.telegramUserId,
      input.chatId,
      input.sendAtMs,
      input.sendAtMs,
      input.expectedRevision ?? null,
      input.payloadJson,
      input.dedupKey,
      input.sourceUpdateId ?? null,
      input.now,
      input.now,
    );
  }

  /**
   * Plans every reminder that has become due, set-based and bounded to the due
   * time window: an occurrence is a candidate only while `starts_at_ms > now`
   * and `starts_at_ms <= now + MAX_REMINDER_OFFSET_MINUTES`, and its send time
   * has already passed. Occurrences outside that window cannot produce a job
   * for any supported offset, so they are filtered by the indexed
   * `(course, starts_at_ms)` range before the user join.
   *
   * Finding 1: candidates whose dedup_key already exists in `delivery_ledger`
   * are excluded, so a delivered reminder can never be re-planned even after
   * its job row was collected, the occurrence was deleted/re-materialized, or
   * the same stable occurrence moved through later horizons.
   *
   * Timezone onboarding: `u.time_zone IS NOT NULL` is a hard eligibility rule,
   * so an active user who has not chosen a timezone yet produces no reminder
   * job at all. There is no other writer of reminder jobs.
   *
   * The conflict update is a no-op for a pending row whose `send_at_ms` and
   * `expected_revision` are unchanged: unchanged data performs no row write and
   * therefore preserves `attempt_count`, `last_error_code` and `updated_at_ms`.
   * A cancelled row is still revived, and a material revision change still
   * reschedules from `send_at_ms` and resets attempts/error exactly as before.
   *
   * One job per (user, occurrence, rule): the join on `user_reminder_offsets`
   * expands a user's whole set, so zero rules produce no work, one rule one job
   * and N rules N jobs. The dedup key carries the stable offset, so a removed
   * rule's job can never collide with a different rule's job and a re-added rule
   * still dedups against the ledger.
   *
   * Returns the number of rows actually inserted or updated.
   */
  async planDueReminders(now: number, horizonEndMs: number): Promise<number> {
    // Job id and dedup key must stay in sync: one job per user/occurrence/offset.
    const dedupKey =
      "'rem:' || u.telegram_user_id || ':' || o.id || ':' || r.offset_minutes";
    const sendAtMs = `o.starts_at_ms - r.offset_minutes * ${MS_PER_MINUTE}`;
    const dueWindowEndMs = now + MAX_REMINDER_OFFSET_MINUTES * MS_PER_MINUTE;
    const rules = await this.#changes(
      `INSERT INTO outbound_jobs (
         id, kind, telegram_user_id, chat_id, occurrence_id, reminder_offset_minutes,
         send_at_ms, next_attempt_at_ms, status, attempt_count, expected_revision,
         dedup_key, created_at_ms, updated_at_ms
       )
       SELECT
         ${dedupKey},
         'reminder', u.telegram_user_id, u.chat_id, o.id, r.offset_minutes,
         ${sendAtMs},
         ${sendAtMs},
         'pending', 0, o.revision,
         ${dedupKey},
         ?, ?
       FROM occurrences o
       JOIN users u ON u.course = o.course
       JOIN user_reminder_offsets r ON r.telegram_user_id = u.telegram_user_id
       WHERE u.active = 1
         AND u.time_zone IS NOT NULL
         AND o.status = 'confirmed'
         AND o.starts_at_ms > ?
         AND ${sendAtMs} <= ?
         AND o.starts_at_ms <= ?
         AND o.starts_at_ms <= ?
         AND NOT EXISTS (
           SELECT 1 FROM delivery_ledger l WHERE l.dedup_key = ${dedupKey}
         )
       ON CONFLICT(dedup_key) DO UPDATE SET
         send_at_ms = excluded.send_at_ms,
         next_attempt_at_ms = CASE
           WHEN outbound_jobs.expected_revision IS excluded.expected_revision
             THEN outbound_jobs.next_attempt_at_ms
           ELSE excluded.send_at_ms
         END,
         expected_revision = excluded.expected_revision,
         attempt_count = CASE
           WHEN outbound_jobs.expected_revision IS excluded.expected_revision
             THEN outbound_jobs.attempt_count
           ELSE 0
         END,
         status = 'pending',
         lease_owner = NULL,
         lease_expires_at_ms = NULL,
         last_error_code = CASE
           WHEN outbound_jobs.expected_revision IS excluded.expected_revision
             THEN outbound_jobs.last_error_code
           ELSE NULL
         END,
         updated_at_ms = excluded.updated_at_ms
       WHERE outbound_jobs.status IN ('pending', 'cancelled')
         AND (outbound_jobs.status = 'cancelled'
              OR outbound_jobs.send_at_ms IS NOT excluded.send_at_ms
              OR outbound_jobs.expected_revision IS NOT excluded.expected_revision)`,
      now,
      now,
      now,
      now,
      horizonEndMs,
      dueWindowEndMs,
    );
    // The at-start job (offset 0) is due exactly at the start, when the strict
    // "starts in the future" predicate no longer holds. This separate statement
    // plans it while the event is inside its delivery grace window; the
    // lead-time statement above keeps its own index range untouched.
    const startDedupKey = `'rem:' || u.telegram_user_id || ':' || o.id || ':${START_REMINDER_OFFSET_MINUTES}'`;
    const start = await this.#changes(
      `INSERT INTO outbound_jobs (
         id, kind, telegram_user_id, chat_id, occurrence_id, reminder_offset_minutes,
         send_at_ms, next_attempt_at_ms, status, attempt_count, expected_revision,
         dedup_key, created_at_ms, updated_at_ms
       )
       SELECT
         ${startDedupKey},
         'reminder', u.telegram_user_id, u.chat_id, o.id, ${START_REMINDER_OFFSET_MINUTES},
         o.starts_at_ms,
         o.starts_at_ms,
         'pending', 0, o.revision,
         ${startDedupKey},
         ?, ?
       FROM occurrences o
       JOIN users u ON u.course = o.course
       JOIN user_reminder_offsets r ON r.telegram_user_id = u.telegram_user_id
       WHERE u.active = 1
         AND u.time_zone IS NOT NULL
         AND o.status = 'confirmed'
         AND r.offset_minutes = ${START_REMINDER_OFFSET_MINUTES}
         AND o.starts_at_ms > ?
         AND o.starts_at_ms <= ?
         AND o.starts_at_ms <= ?
         AND o.starts_at_ms <= ?
         AND NOT EXISTS (
           SELECT 1 FROM delivery_ledger l WHERE l.dedup_key = ${startDedupKey}
         )
       ON CONFLICT(dedup_key) DO UPDATE SET
         send_at_ms = excluded.send_at_ms,
         next_attempt_at_ms = CASE
           WHEN outbound_jobs.expected_revision IS excluded.expected_revision
             THEN outbound_jobs.next_attempt_at_ms
           ELSE excluded.send_at_ms
         END,
         expected_revision = excluded.expected_revision,
         attempt_count = CASE
           WHEN outbound_jobs.expected_revision IS excluded.expected_revision
             THEN outbound_jobs.attempt_count
           ELSE 0
         END,
         status = 'pending',
         lease_owner = NULL,
         lease_expires_at_ms = NULL,
         last_error_code = CASE
           WHEN outbound_jobs.expected_revision IS excluded.expected_revision
             THEN outbound_jobs.last_error_code
           ELSE NULL
         END,
         updated_at_ms = excluded.updated_at_ms
       WHERE outbound_jobs.status IN ('pending', 'cancelled')
         AND (outbound_jobs.status = 'cancelled'
              OR outbound_jobs.send_at_ms IS NOT excluded.send_at_ms
              OR outbound_jobs.expected_revision IS NOT excluded.expected_revision)`,
      now,
      now,
      now - START_REMINDER_GRACE_MS,
      now,
      horizonEndMs,
      dueWindowEndMs,
    );
    return rules + start;
  }

  async cancelStaleJobs(now: number): Promise<number> {
    return this.#changes(
      `UPDATE outbound_jobs
       SET status = 'cancelled', lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ?
       WHERE status IN ('pending', 'enqueued')
         AND kind = 'reminder'
         AND (
           NOT EXISTS (
             SELECT 1 FROM occurrences o
             JOIN users u ON u.telegram_user_id = outbound_jobs.telegram_user_id
             WHERE o.id = outbound_jobs.occurrence_id
               AND o.status = 'confirmed'
               AND u.active = 1
               AND u.course = o.course
           )
           OR NOT EXISTS (
             SELECT 1 FROM user_reminder_offsets r
             WHERE r.telegram_user_id = outbound_jobs.telegram_user_id
               AND r.offset_minutes = outbound_jobs.reminder_offset_minutes
           )
         )`,
      now,
    );
  }

  /**
   * Scheduler-side enqueue reservation. Moves due pending jobs to `enqueued`
   * with a reservation lease; it is not processing ownership. A crashed
   * scheduler is repaired by `repairExpiredLeases` after the lease expires.
   */
  async claimDueJobs(
    owner: string,
    now: number,
    leaseMs: number,
    limit: number,
  ): Promise<{ jobId: string }[]> {
    const result = await this.#rows<{ id: unknown }>(
      `UPDATE outbound_jobs
       SET status = 'enqueued',
           lease_owner = ?,
           lease_expires_at_ms = ?,
           updated_at_ms = ?
       WHERE status = 'pending'
         AND id IN (
           SELECT id FROM outbound_jobs
           WHERE status = 'pending'
             AND next_attempt_at_ms <= ?
             AND (lease_expires_at_ms IS NULL OR lease_expires_at_ms <= ?)
           ORDER BY next_attempt_at_ms ASC, id ASC
           LIMIT ?
         )
       RETURNING id`,
      owner,
      now + leaseMs,
      now,
      now,
      now,
      limit,
    );
    return result.map((row) => ({ jobId: String(row.id) }));
  }

  /**
   * Consumer-side processing ownership plus the joined job context in one
   * statement. The transition is a single atomic eligibility check: an
   * `enqueued` reservation can be taken by any consumer, a due `pending` job
   * can be taken directly (command replies are enqueued by the webhook), and a
   * `leased` job only when the caller already owns it or its lease expired. A
   * second consumer can never steal an active lease, and a failed claim returns
   * null without reading anything else.
   *
   * Claiming is lease activity, not delivery: it never changes `attempt_count`.
   * Only `beginSendAttempt` counts a started Telegram call.
   */
  async claimJobContext(
    jobId: string,
    owner: string,
    now: number,
    leaseMs: number,
  ): Promise<JobContext | null> {
    const row = await this.#row<JobContextRow>(
      `UPDATE outbound_jobs
       SET status = 'leased',
           lease_owner = ?,
           lease_expires_at_ms = ?,
           updated_at_ms = ?
       WHERE id = ?
         AND (
           status = 'enqueued'
           OR (status = 'pending' AND next_attempt_at_ms <= ?)
           OR (status = 'leased' AND (lease_owner = ? OR lease_expires_at_ms <= ?))
         )
        RETURNING
          id AS job_id, kind, telegram_user_id, chat_id, occurrence_id,
          reminder_offset_minutes, send_at_ms, next_attempt_at_ms, attempt_count,
          expected_revision, payload_json, source_update_id,
          (SELECT last_update_id FROM user_command_state
            WHERE telegram_user_id = outbound_jobs.telegram_user_id) AS last_seen_update_id,
         (SELECT telegram_user_id FROM users
           WHERE telegram_user_id = outbound_jobs.telegram_user_id) AS user_id,
         (SELECT active FROM users
           WHERE telegram_user_id = outbound_jobs.telegram_user_id) AS user_active,
         (SELECT course FROM users
            WHERE telegram_user_id = outbound_jobs.telegram_user_id) AS user_course,
         (SELECT COUNT(*) FROM user_reminder_offsets
            WHERE telegram_user_id = outbound_jobs.telegram_user_id
              AND offset_minutes = outbound_jobs.reminder_offset_minutes) AS user_has_offset,
         (SELECT time_zone FROM users
            WHERE telegram_user_id = outbound_jobs.telegram_user_id) AS user_time_zone,
         (SELECT revision FROM users
           WHERE telegram_user_id = outbound_jobs.telegram_user_id) AS user_revision,
         (SELECT id FROM occurrences
           WHERE id = outbound_jobs.occurrence_id) AS occ_id,
         (SELECT status FROM occurrences
           WHERE id = outbound_jobs.occurrence_id) AS occ_status,
         (SELECT starts_at_ms FROM occurrences
           WHERE id = outbound_jobs.occurrence_id) AS occ_starts_at_ms,
         (SELECT course FROM occurrences
           WHERE id = outbound_jobs.occurrence_id) AS occ_course,
         (SELECT revision FROM occurrences
           WHERE id = outbound_jobs.occurrence_id) AS occ_revision,
         (SELECT occurrence_key FROM occurrences
           WHERE id = outbound_jobs.occurrence_id) AS occ_key,
         (SELECT summary FROM occurrences
           WHERE id = outbound_jobs.occurrence_id) AS occ_summary,
         (SELECT description FROM occurrences
           WHERE id = outbound_jobs.occurrence_id) AS occ_description,
         (SELECT url FROM occurrences
           WHERE id = outbound_jobs.occurrence_id) AS occ_url,
         (SELECT s.fetched_at_ms FROM occurrences o
           JOIN sources s ON s.id = o.source_id
           WHERE o.id = outbound_jobs.occurrence_id) AS source_fetched_at_ms`,
      owner,
      now + leaseMs,
      now,
      jobId,
      now,
      owner,
      now,
    );
    return row === null ? null : toJobContext(row);
  }

  /**
   * Atomic pre-send reservation (finding 2 + finding 4 ordering guard). This is
   * the only writer of `attempt_count`, so the persisted value counts real
   * outbound requests: scheduler reservations, queue deliveries,
   * pacing/cooldown deferrals and lease repairs never touch it.
   *
   * A single UPDATE reserves a start only while the caller holds the live
   * processing lease, `attempt_count < maxAttempts`, the command revision guard
   * still holds, and the command update-ordering guard still holds (a command
   * with source_update_id NULL/0 is drainable/deliverable; NULL maxSeen is
   * deliverable; otherwise source_update_id >= maxSeen is required). On success
   * exactly one increment happened and `reserved` is returned together with the
   * recipient's live stored `time_zone`, read in the same statement so every
   * request renders from current durable state at no extra statement cost.
   *
   * On no row, one probe SELECT classifies without incrementing: `superseded`
   * for a stale command (revision or update_id), `exhausted` when the persisted
   * count is already at the cap, otherwise `lost` (missing job, wrong/expired
   * lease, non-leased status). Never calls Telegram unless `reserved`.
   */
  async beginSendAttempt(
    jobId: string,
    owner: string,
    now: number,
    maxAttempts: number,
  ): Promise<SendReservation> {
    const reserved = await this.#row<{ attempt_count: unknown; user_time_zone: unknown }>(
      `UPDATE outbound_jobs
       SET attempt_count = attempt_count + 1
       WHERE id = ?
         AND status = 'leased'
         AND lease_owner = ?
         AND lease_expires_at_ms > ?
         AND attempt_count < ?
         AND (kind <> 'command'
              OR outbound_jobs.expected_revision IS NULL
              OR (SELECT revision FROM users
                    WHERE telegram_user_id = outbound_jobs.telegram_user_id)
                  IS outbound_jobs.expected_revision)
         AND (kind <> 'command'
              OR source_update_id IS NULL
              OR source_update_id = 0
              OR (SELECT last_update_id FROM user_command_state
                    WHERE telegram_user_id = outbound_jobs.telegram_user_id) IS NULL
              OR source_update_id >= (SELECT last_update_id FROM user_command_state
                    WHERE telegram_user_id = outbound_jobs.telegram_user_id))
       RETURNING attempt_count,
         (SELECT time_zone FROM users
           WHERE telegram_user_id = outbound_jobs.telegram_user_id) AS user_time_zone`,
      jobId,
      owner,
      now,
      maxAttempts,
    );
    if (reserved !== null) {
      return {
        status: 'reserved',
        attempt: Number(reserved.attempt_count),
        userTimeZone: nullableString(reserved.user_time_zone),
      };
    }
    const probe = await this.#row<{
      status: unknown;
      lease_owner: unknown;
      lease_expires_at_ms: unknown;
      kind: unknown;
      expected_revision: unknown;
      source_update_id: unknown;
      attempt_count: unknown;
      user_rev: unknown;
      max_seen: unknown;
    }>(
      `SELECT status, lease_owner, lease_expires_at_ms, kind, expected_revision,
              source_update_id, attempt_count,
              (SELECT revision FROM users
                WHERE telegram_user_id = outbound_jobs.telegram_user_id) AS user_rev,
              (SELECT last_update_id FROM user_command_state
                WHERE telegram_user_id = outbound_jobs.telegram_user_id) AS max_seen
       FROM outbound_jobs WHERE id = ?`,
      jobId,
    );
    if (probe === null) {
      return { status: 'lost' };
    }
    const leased =
      String(probe.status) === 'leased' &&
      (probe.lease_owner === null ? false : String(probe.lease_owner) === owner) &&
      probe.lease_expires_at_ms !== null &&
      Number(probe.lease_expires_at_ms) > now;
    if (!leased) {
      return { status: 'lost' };
    }
    if (String(probe.kind) === 'command') {
      const expected = probe.expected_revision === null ? null : Number(probe.expected_revision);
      const userRev = probe.user_rev === null ? null : Number(probe.user_rev);
      const revisionSuperseded =
        expected !== null && (userRev === null || userRev !== expected);
      const src = probe.source_update_id === null ? null : Number(probe.source_update_id);
      const maxSeen = probe.max_seen === null ? null : Number(probe.max_seen);
      const orderSuperseded =
        src !== null && src !== 0 && maxSeen !== null && src < maxSeen;
      if (revisionSuperseded || orderSuperseded) {
        return { status: 'superseded' };
      }
    }
    if (Number(probe.attempt_count) >= maxAttempts) {
      return { status: 'exhausted' };
    }
    return { status: 'lost' };
  }

  /**
   * Returns expired reservations and processing leases to the pending pool
   * (D1 is the source of truth). Covers an enqueue crash as well as a crashed
   * consumer.
   */
  async repairExpiredLeases(now: number): Promise<number> {
    return this.#changes(
      `UPDATE outbound_jobs
       SET status = 'pending', lease_owner = NULL, lease_expires_at_ms = NULL,
           updated_at_ms = ?
       WHERE status IN ('enqueued', 'leased')
         AND lease_expires_at_ms IS NOT NULL
         AND lease_expires_at_ms <= ?`,
      now,
      now,
    );
  }

  /**
   * Finding 1: marks a leased job sent and, for reminders, inserts the durable
   * delivery-ledger row in the SAME atomic batch. The ledger insert selects
   * from the just-updated job row, so it fires only when this owner actually
   * won the `sent` transition; command jobs insert nothing. Cleanup never
   * deletes ledger rows.
   */
  async finishJobSent(jobId: string, owner: string, now: number): Promise<boolean> {
    const results = await this.#batchAtomic([
      this.#statement(
        `UPDATE outbound_jobs
         SET status = 'sent', lease_owner = NULL, lease_expires_at_ms = NULL,
             last_error_code = NULL, updated_at_ms = ?
         WHERE id = ? AND status = 'leased' AND lease_owner = ?`,
        now,
        jobId,
        owner,
      ),
      this.#statement(
        `INSERT OR IGNORE INTO delivery_ledger (dedup_key, occurrence_id, telegram_user_id, sent_at_ms)
         SELECT dedup_key, occurrence_id, telegram_user_id, ?
         FROM outbound_jobs
         WHERE id = ? AND kind = 'reminder' AND status = 'sent'`,
        now,
        jobId,
      ),
    ]);
    return reportedChanges(results[0] as D1ResultLike<never> | undefined) > 0;
  }

  async rescheduleJob(
    jobId: string,
    owner: string,
    nextAttemptAtMs: number,
    errorCode: string,
    now: number,
  ): Promise<boolean> {
    const changes = await this.#changes(
      `UPDATE outbound_jobs
       SET status = 'pending', next_attempt_at_ms = ?, last_error_code = ?,
           lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ?
       WHERE id = ? AND status = 'leased' AND lease_owner = ?`,
      nextAttemptAtMs,
      errorCode,
      now,
      jobId,
      owner,
    );
    return changes > 0;
  }

  async finishJobTerminal(
    jobId: string,
    owner: string,
    status: 'cancelled' | 'failed',
    errorCode: string | null,
    now: number,
  ): Promise<boolean> {
    const changes = await this.#changes(
      `UPDATE outbound_jobs
       SET status = ?, last_error_code = ?, lease_owner = NULL, lease_expires_at_ms = NULL,
           updated_at_ms = ?
       WHERE id = ? AND status = 'leased' AND lease_owner = ?`,
      status,
      errorCode,
      now,
      jobId,
      owner,
    );
    return changes > 0;
  }

  /**
   * Terminal transition for a reservation that must not count as a started
   * call: cancels the job and refunds exactly the just-reserved attempt in one
   * statement, under the live processing lease and guarded by the exact attempt
   * number the reservation returned. A lease race, an expired lease or a
   * concurrent reservation therefore cannot refund the wrong attempt; a failed
   * guard reports false, and the caller retries the delivery instead of
   * acknowledging work that never landed. Used when the reservation's live
   * timezone is unusable, so `attempt_count` keeps counting only started
   * Telegram calls.
   *
   * `reservedAttempt` is a reservation result and must be a positive safe
   * integer; anything else is a caller contract violation and is rejected
   * before SQL runs, so a zero or negative refund can never drive the counter
   * below zero. The statement additionally requires `attempt_count > 0` as
   * database defense.
   */
  async finishJobUnusable(
    jobId: string,
    owner: string,
    reservedAttempt: number,
    errorCode: string,
    now: number,
  ): Promise<boolean> {
    assertSafeInteger(reservedAttempt, 'reservedAttempt');
    if (reservedAttempt <= 0) {
      throw new RangeError(`reservedAttempt must be positive, got ${reservedAttempt}`);
    }
    const changes = await this.#changes(
      `UPDATE outbound_jobs
       SET status = 'cancelled',
           attempt_count = attempt_count - 1,
           last_error_code = ?,
           lease_owner = NULL,
           lease_expires_at_ms = NULL,
           updated_at_ms = ?
       WHERE id = ?
         AND status = 'leased'
         AND lease_owner = ?
         AND lease_expires_at_ms > ?
         AND attempt_count = ?
         AND attempt_count > 0`,
      errorCode,
      now,
      jobId,
      owner,
      now,
      reservedAttempt,
    );
    return changes > 0;
  }

  async getJob(jobId: string): Promise<Record<string, unknown> | null> {
    return this.#row<Record<string, unknown>>(
      'SELECT * FROM outbound_jobs WHERE id = ?',
      jobId,
    );
  }

  // ----- webhook dedup ---------------------------------------------------

  /**
   * Bounded retention cleanup, seven LIMIT-bounded statements per call so one
   * tick can never scan or rewrite a whole table (finding 7: the old single
   * multi-branch jobs OR-query is split into three selective indexed deletes):
   *
   * 1. Occurrences that have already started and began at least `retentionMs`
   *    ago (`starts_at_ms <= MIN(cutoff, now)`). The planner only considers
   *    `starts_at_ms > now`, so these can never be planned again - not even
   *    when a caller passes a negative retention, because the deletion also
   *    never reaches a row that has not started yet. Occurrences are only a
   *    materialization; a later sync can rebuild them, including with a moved
   *    start for the same stable id. Indexed by `idx_occurrences_cleanup`
   *    (starts_at_ms, id).
   * 2a. Command replies (`sent`/`cancelled`/`failed`) on the short `retentionMs`
   *    policy. Indexed by `idx_jobs_cleanup_command`.
   * 2b. Sent reminder rows on `SENT_REMINDER_RETENTION_MS`. These rows are the
   *    transient copy of the delivery identity; the durable identity lives in
   *    `delivery_ledger`, which cleanup NEVER deletes, so collecting an old
   *    sent row cannot reopen a send. Indexed by
   *    `idx_jobs_cleanup_reminder_sent`.
   * 2c. Non-sent terminal reminders (`cancelled`/`failed`) on the short policy
   *    when orphaned. Indexed by `idx_jobs_cleanup_reminder_terminal`.
   * 3. Staging rows of attempts whose lease has been dead for
   *    `SOURCE_LEASE_MS`, ordered by the age index `idx_staging_age`
   *    (updated_at_ms, attempt_id, id) - deliberately NOT starting with
   *    source_id so the age range is selective.
   * 4. Processed webhook updates, as before.
   * 5. C2: answered callback queries older than `retentionMs`, ordered by the
   *    age index `idx_callback_answers_age` (answered_at_ms, query_id). The
   *    webhook retry window is minutes, so a 24h retention keeps dedup exact
   *    across any realistic retry while staying bounded. This is the 7th
   *    statement: the scheduler CLEANUP_RESERVE must be 7 (tick.ts, not owned
   *    here - reported to the integrator).
   *
   * Order matters: occurrences are removed first so orphaned rows are collected
   * in the same call instead of a second pass.
   */
  async cleanup(now: number, retentionMs: number, limit: number): Promise<void> {
    const cutoff = now - retentionMs;
    const sentCutoff = now - SENT_REMINDER_RETENTION_MS;
    await this.#changes(
      `DELETE FROM occurrences WHERE id IN (
         SELECT id FROM occurrences
         WHERE starts_at_ms <= MIN(?, ?)
         ORDER BY starts_at_ms ASC, id ASC
         LIMIT ?
       )`,
      cutoff,
      now,
      limit,
    );
    await this.#changes(
      `DELETE FROM outbound_jobs WHERE id IN (
         SELECT id FROM outbound_jobs
         WHERE kind = 'command'
           AND status IN ('sent', 'cancelled', 'failed')
           AND updated_at_ms <= ?
           AND send_at_ms <= ?
         ORDER BY updated_at_ms ASC, id ASC
         LIMIT ?
       )`,
      cutoff,
      cutoff,
      limit,
    );
    await this.#changes(
      `DELETE FROM outbound_jobs WHERE id IN (
         SELECT id FROM outbound_jobs
         WHERE kind = 'reminder'
           AND status = 'sent'
           AND updated_at_ms <= ?
         ORDER BY updated_at_ms ASC, id ASC
         LIMIT ?
       )`,
      sentCutoff,
      limit,
    );
    await this.#changes(
      `DELETE FROM outbound_jobs WHERE id IN (
         SELECT id FROM outbound_jobs
         WHERE kind = 'reminder'
           AND status IN ('cancelled', 'failed')
           AND updated_at_ms <= ?
           AND send_at_ms <= ?
           AND NOT EXISTS (
             SELECT 1 FROM occurrences o WHERE o.id = outbound_jobs.occurrence_id
           )
         ORDER BY updated_at_ms ASC, id ASC
         LIMIT ?
       )`,
      cutoff,
      cutoff,
      limit,
    );
    await this.#changes(
      `DELETE FROM occurrence_staging WHERE rowid IN (
         SELECT rowid FROM occurrence_staging
         WHERE updated_at_ms <= ?
         ORDER BY updated_at_ms ASC, attempt_id ASC, id ASC
         LIMIT ?
       )`,
      now - SOURCE_LEASE_MS,
      limit,
    );
    await this.#changes(
      `DELETE FROM processed_updates WHERE update_id IN (
         SELECT update_id FROM processed_updates
         WHERE status = 'done' AND processed_at_ms IS NOT NULL AND processed_at_ms <= ?
         LIMIT ?
       )`,
      cutoff,
      limit,
    );
    await this.#changes(
      `DELETE FROM callback_answers WHERE query_id IN (
         SELECT query_id FROM callback_answers
         WHERE answered_at_ms <= ?
         ORDER BY answered_at_ms ASC, query_id ASC
         LIMIT ?
       )`,
      cutoff,
      limit,
    );
  }

  /**
   * Takes processing ownership of a webhook update in one atomic upsert and
   * reports what the caller may do:
   *
   * - a new update, an expired processing lease or the same owner renewing gets
   *   `acquired` and the lease timestamp is written in the same statement;
   * - a completed row is `done` (safe to acknowledge as a duplicate);
   * - a live lease held by another owner is `busy`: the other owner's lease is
   *   left untouched and the caller must ask the sender to retry.
   *
   * `completeUpdate` and `releaseUpdate` keep their existing owner-only
   * semantics.
   */
  async tryBeginUpdate(
    updateId: number,
    owner: string,
    now: number,
    leaseMs: number,
  ): Promise<UpdateAcquisition> {
    const acquired = await this.#row<{ update_id: unknown }>(
      `INSERT INTO processed_updates (update_id, lease_owner, lease_expires_at_ms, status, processed_at_ms)
       VALUES (?, ?, ?, 'processing', NULL)
       ON CONFLICT(update_id) DO UPDATE SET
         lease_owner = excluded.lease_owner,
         lease_expires_at_ms = excluded.lease_expires_at_ms,
         status = 'processing'
       WHERE processed_updates.status = 'processing'
         AND (processed_updates.lease_owner IS NULL
              OR processed_updates.lease_expires_at_ms IS NULL
              OR processed_updates.lease_expires_at_ms <= ?
              OR processed_updates.lease_owner = ?)
       RETURNING update_id`,
      updateId,
      owner,
      now + leaseMs,
      now,
      owner,
    );
    if (acquired !== null) {
      return 'acquired';
    }
    const row = await this.#row<{ status: unknown }>(
      'SELECT status FROM processed_updates WHERE update_id = ?',
      updateId,
    );
    // A row that vanished between the upsert and the read (cleanup) is not
    // knowably done, so answer with the retryable state.
    return row !== null && row.status === 'done' ? 'done' : 'busy';
  }

  async releaseUpdate(updateId: number, owner: string): Promise<boolean> {
    const changes = await this.#changes(
      `UPDATE processed_updates
       SET lease_owner = NULL, lease_expires_at_ms = NULL
       WHERE update_id = ? AND lease_owner = ? AND status = 'processing'`,
      updateId,
      owner,
    );
    return changes > 0;
  }

  async completeUpdate(updateId: number, owner: string, now: number): Promise<boolean> {
    const changes = await this.#changes(
      `UPDATE processed_updates
       SET status = 'done', lease_owner = NULL, lease_expires_at_ms = NULL, processed_at_ms = ?
       WHERE update_id = ? AND lease_owner = ?`,
      now,
      updateId,
      owner,
    );
    return changes > 0;
  }

  // ----- pace ------------------------------------------------------------

  /**
   * Finding 5: durable rolling 20-per-second pacer. One conditional INSERT
   * reserves a start only when the global 429 cooldown is clear (via
   * `rate_state`) AND fewer than `maxPerWindow` starts already exist in the
   * rolling window (now-1000, now] (see `findPacingViolations` in
   * test/helpers/simulation.ts, which uses the same (first, first+20) < 1000ms
   * predicate). No fixed-window reset: bursts split across the old 1s boundary
   * (e.g. 19 at 900ms + 20 at 1000ms) are rejected because the rolling count
   * still sees the 900ms rows.
   *
   * Costs 1 statement. Old-row GC lives in `pruneRateStarts`, called once per
   * consumer batch instead of on every reservation: a pruned row satisfies
   * started_at <= pruneTime-1000 <= now'-1000 for every later admission at
   * now' >= pruneTime, so it could never satisfy the `> now'-1000` predicate
   * anyway and the admission decision is identical with or without it.
   * `rate_state` keeps only the cooldown; its legacy window columns are no
   * longer the admission gate.
   */
  async acquireSendSlot(now: number, maxPerWindow: number): Promise<boolean> {
    const windowStart = now - SEND_PACE_WINDOW_MS;
    const changes = await this.#changes(
      `INSERT INTO rate_starts (started_at_ms)
       SELECT ? WHERE
         EXISTS (SELECT 1 FROM rate_state WHERE name = 'send')
         AND ((SELECT cooldown_until_ms FROM rate_state WHERE name = 'send') IS NULL
          OR (SELECT cooldown_until_ms FROM rate_state WHERE name = 'send') <= ?)
         AND (SELECT COUNT(*) FROM rate_starts
              WHERE started_at_ms > ? AND started_at_ms <= ?) < ?`,
      now,
      now,
      windowStart,
      now,
      maxPerWindow,
    );
    return changes > 0;
  }

  /**
   * Once-per-batch GC for `rate_starts`. Deletes rows at or before the trailing
   * window edge (now-1000); see `acquireSendSlot` for why this never changes an
   * admission decision. Costs 1 statement. Between two prunes the table holds
   * at most the live rolling window (<= maxPerWindow rows under the enforced
   * pace, plus in-flight concurrent inserts) plus the current batch's own
   * inserts, so it stays bounded without a per-send delete.
   */
  async pruneRateStarts(now: number): Promise<void> {
    await this.#changes(`DELETE FROM rate_starts WHERE started_at_ms <= ?`, now - SEND_PACE_WINDOW_MS);
  }

  async setSendCooldown(untilMs: number, now: number): Promise<void> {
    await this.#changes(
      `UPDATE rate_state
       SET cooldown_until_ms = ?, updated_at_ms = ?
       WHERE name = 'send' AND (cooldown_until_ms IS NULL OR cooldown_until_ms < ?)`,
      untilMs,
      now,
      untilMs,
    );
  }

  /**
   * Finding 5: reads the sliding-window occupancy for the pacing wait. The
   * window is anchored at the newest recorded start: `windowStartedAtMs` is
   * the oldest start still inside the trailing 1,000 ms, so
   * `windowStartedAtMs + SEND_PACE_WINDOW_MS - now` is exactly how long the
   * caller must sleep for one slot to free (the same (now-1000, now]
   * predicate `acquireSendSlot` enforces). `consumed` counts that window;
   * `cooldownUntilMs` is the global 429 cooldown from `rate_state`. Returns
   * null only when the `rate_state` row is missing.
   */
  async getPaceState(): Promise<PaceState | null> {
    const row = await this.#row<PaceStateRow>(
      `SELECT
         COALESCE(
           (SELECT MIN(started_at_ms) FROM rate_starts
            WHERE started_at_ms > (SELECT MAX(started_at_ms) FROM rate_starts) - ${SEND_PACE_WINDOW_MS}),
           0) AS window_started_at_ms,
         COALESCE(
           (SELECT COUNT(*) FROM rate_starts
            WHERE started_at_ms > (SELECT MAX(started_at_ms) FROM rate_starts) - ${SEND_PACE_WINDOW_MS}),
           0) AS consumed,
         (SELECT cooldown_until_ms FROM rate_state WHERE name = 'send') AS cooldown_until_ms`,
    );
    if (row === null) {
      return null;
    }
    return {
      windowStartedAtMs: Number(row.window_started_at_ms),
      consumed: Number(row.consumed),
      cooldownUntilMs: nullableNumber(row.cooldown_until_ms),
    };
  }
}
