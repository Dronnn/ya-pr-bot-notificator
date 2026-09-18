/**
 * Fixtures for D1 rows, queue messages and calendar sync inputs.
 */

import type { OccurrenceWrite, Repository } from '../../src/data/repository.ts';
import { EXPANSION_HORIZON_MS, JOB_LEASE_MS, MS_PER_MINUTE, SOURCE_LEASE_MS } from '../../src/util.ts';
import type { ParsedEvent } from '../../src/domain/calendar.ts';
import type { Course } from '../../src/domain/notification-policy.ts';
import type { SourceDefinition } from '../../src/calendar/sync.ts';
import type {
  D1DatabaseLike,
  D1StatementLike,
  OutboundJobMessage,
  QueueMessageLike,
} from '../../src/platform.ts';
import type { Harness } from './harness.ts';

/**
 * A repository-shaped occurrence write. `id` defaults to `${sourceId}:${key}`;
 * unset fields default to a confirmed basic course lesson.
 */
export function occurrence(
  overrides: Partial<OccurrenceWrite> & { occurrenceKey: string; startsAtMs: number },
): OccurrenceWrite {
  const sourceId = overrides.sourceId ?? 'basic';
  const key = overrides.occurrenceKey;
  return {
    id: overrides.id ?? `${sourceId}:${key}`,
    sourceId,
    uid: overrides.uid ?? 'uid-1',
    occurrenceKey: key,
    course: overrides.course ?? 'basic',
    startsAtMs: overrides.startsAtMs,
    endsAtMs: overrides.endsAtMs ?? null,
    summary: overrides.summary ?? 'Lesson',
    description: overrides.description ?? null,
    url: overrides.url ?? null,
    status: overrides.status ?? 'confirmed',
    isAllDay: overrides.isAllDay ?? false,
  };
}

export async function seedSource(
  repository: Repository,
  kind: Course,
  now: number,
): Promise<void> {
  // Fixtures seed through the same custody-checked path as production, but they
  // have no attempt to present. They take a live generation-1 lease for the
  // write and then renew it with zero duration, so the stored lease is expired
  // and the next real sync can take the source over.
  const lease = await repository.acquireSourceLease(kind, `seed-${kind}`, now, SOURCE_LEASE_MS);
  if (lease === null) {
    throw new Error(`seedSource could not seed the ${kind} source lease`);
  }
  const written = await repository.recordSourceFresh(
    kind,
    kind,
    lease,
    `etag-${kind}`,
    `lm-${kind}`,
    () => now,
  );
  if (!written) {
    throw new Error(`seedSource could not write the ${kind} source freshness`);
  }
  await repository.acquireSourceLease(kind, `seed-${kind}`, now, 0);
}

/** Advances the harness clock just past any live source lease. */
export function expireSourceLease(harness: Harness): void {
  harness.clock.advance(SOURCE_LEASE_MS + 1);
}

export interface DueReminderSeedOptions {
  /** Occurrence key of the seeded occurrence; defaults to 'a#1'. */
  occurrenceKey?: string;
  /** Occurrence start; defaults to ten minutes after the current clock. */
  startsAtMs?: number;
  /** Single stored reminder rule; ignored when `reminderOffsets` is given. */
  reminderOffsetMinutes?: number;
  /** Stored reminder rule set; defaults to the legacy single 30-minute rule. */
  reminderOffsets?: readonly number[];
  /** Stored timezone; omitted completes onboarding with Europe/Moscow. */
  timeZone?: string;
}

/** Timezone fixture users are onboarded with when a seed does not say otherwise. */
export const TEST_USER_TIME_ZONE = 'Europe/Moscow';

/**
 * Completes fixture onboarding by writing the stored timezone directly. This
 * represents a user who already finished the timezone step and keeps the
 * fixture revision history identical to pre-timezone seeds (the guarded
 * mutation and its revision semantics are covered by the timezone regressions).
 */
export function onboardUser(
  harness: Harness,
  userId: number,
  timeZone: string = TEST_USER_TIME_ZONE,
): void {
  harness.db.database
    .prepare('UPDATE users SET time_zone = ? WHERE telegram_user_id = ?')
    .run(timeZone, userId);
}

/**
 * Replaces a fixture user's reminder rule set directly, bypassing the guarded
 * mutation and its revision bump so fixtures keep a stable user revision (the
 * same rationale as `onboardUser`). An empty set leaves the user with zero
 * rules.
 */
export function seedReminderOffsets(
  harness: Harness,
  userId: number,
  offsets: readonly number[],
): void {
  const db = harness.db.database;
  db.prepare('DELETE FROM user_reminder_offsets WHERE telegram_user_id = ?').run(userId);
  const insert = db.prepare(
    'INSERT INTO user_reminder_offsets (telegram_user_id, offset_minutes, created_at_ms) VALUES (?, ?, ?)',
  );
  const now = harness.clock.now();
  for (const offset of offsets) {
    insert.run(userId, offset, now);
  }
}

/**
 * Seeds the basic source, activates `userIds` and plans one due occurrence. A
 * numeric `userIds` is read as a recipient count and activates users 1..count.
 */
export async function seedDueReminders(
  harness: Harness,
  userIds: number | readonly number[],
  options: DueReminderSeedOptions = {},
): Promise<void> {
  const now = harness.clock.now();
  const ids =
    typeof userIds === 'number'
      ? Array.from({ length: userIds }, (_, index) => index + 1)
      : userIds;
  await seedSource(harness.repository, 'basic', now);
  const offsets = options.reminderOffsets ?? [options.reminderOffsetMinutes ?? 30];
  for (const userId of ids) {
    await harness.repository.activateUser(userId, userId, now);
    onboardUser(harness, userId, options.timeZone ?? TEST_USER_TIME_ZONE);
    seedReminderOffsets(harness, userId, offsets);
  }
  await harness.repository.upsertOccurrences(
    [
      occurrence({
        occurrenceKey: options.occurrenceKey ?? 'a#1',
        startsAtMs: options.startsAtMs ?? now + 10 * MS_PER_MINUTE,
      }),
    ],
    now,
  );
  await harness.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
}

/** Seeds due reminders and claims them for the scheduler; returns the job ids. */
export async function seedDueJobs(
  harness: Harness,
  userIds: number | readonly number[],
  options: DueReminderSeedOptions = {},
): Promise<string[]> {
  await seedDueReminders(harness, userIds, options);
  const claimed = await harness.repository.claimDueJobs(
    'scheduler',
    harness.clock.now(),
    JOB_LEASE_MS,
    100,
  );
  return claimed.map((job) => job.jobId);
}

/** A parser-shaped event expanded once at `startsAtMs`. */
export function parsedEvent(
  startsAtMs: number,
  overrides: Partial<ParsedEvent> = {},
): ParsedEvent {
  return {
    uid: 'course-1',
    summary: 'Lesson',
    description: null,
    url: null,
    startsAtMs,
    endsAtMs: null,
    status: 'confirmed',
    isAllDay: false,
    recurrenceIdMs: null,
    expandedStartsMs: [startsAtMs],
    timezone: 'Europe/Moscow',
    ...overrides,
  };
}

/** A valid VCALENDAR without events; sync accepts it as an empty snapshot. */
export const EMPTY_ICS = 'BEGIN:VCALENDAR\nVERSION:2.0\nEND:VCALENDAR';

/** The source definition used by the sync regressions. */
export const TEST_SOURCE: SourceDefinition = {
  id: 'basic',
  kind: 'basic',
  url: 'https://example.test/x.ics',
};

export interface TrackedMessage extends QueueMessageLike<OutboundJobMessage> {
  acked: boolean;
  retried: boolean;
  /** Delay requested by the last `retry`, when the consumer passed one. */
  retryDelaySeconds?: number;
}

export function makeQueueMessage(jobId: string): TrackedMessage {
  const message: TrackedMessage = {
    body: { jobId },
    acked: false,
    retried: false,
    ack(): void {
      message.acked = true;
    },
    retry(options?: { delaySeconds?: number }): void {
      message.retried = true;
      message.retryDelaySeconds = options?.delaySeconds;
    },
  };
  return message;
}

/**
 * FIX-ORDER: a D1 shim that suspends the first statement whose SQL matches
 * `matches` until `release()` is called. Wrap one invocation's repository with
 * `db` to hold it inside a chosen statement while another invocation runs to
 * completion against the unwrapped database.
 */
export interface StatementBarrier {
  readonly db: D1DatabaseLike;
  readonly reached: Promise<void>;
  release(): void;
}

export function createStatementBarrier(
  inner: D1DatabaseLike,
  matches: (sql: string) => boolean,
): StatementBarrier {
  let tripped = false;
  let releaseGate: () => void = () => {};
  const released = new Promise<void>((resolve) => {
    releaseGate = resolve;
  });
  let markReached: () => void = () => {};
  const reached = new Promise<void>((resolve) => {
    markReached = resolve;
  });
  const hold = async (sql: string): Promise<void> => {
    if (tripped || !matches(sql)) {
      return;
    }
    tripped = true;
    markReached();
    await released;
  };
  const wrap = (statement: D1StatementLike, sql: string): D1StatementLike => ({
    bind(...values) {
      return wrap(statement.bind(...values), sql);
    },
    async first<T = Record<string, unknown>>() {
      await hold(sql);
      return statement.first<T>();
    },
    async all<T = Record<string, unknown>>() {
      await hold(sql);
      return statement.all<T>();
    },
    async run() {
      await hold(sql);
      return statement.run();
    },
  });
  return {
    db: {
      prepare(query: string): D1StatementLike {
        return wrap(inner.prepare(query), query);
      },
      batch(statements: readonly D1StatementLike[]): Promise<readonly unknown[]> {
        return inner.batch(statements);
      },
    },
    reached,
    release: () => {
      releaseGate();
    },
  };
}
