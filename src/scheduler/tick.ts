/**
 * One cron tick: repair leases, refresh each source once on Monday in
 * Europe/Moscow (plus one initial fetch for a source that was never
 * attempted), plan due reminders set-based, cancel stale work, then claim
 * bounded groups, enqueue ID-only messages and run bounded cleanup.
 */

import type { Repository, SourceRecord } from '../data/repository.ts';
import type { OutboundJobMessage, QueueProducerLike } from '../platform.ts';
import type { Clock, Logger } from '../util.ts';
import {
  CLEANUP_LIMIT,
  EXPANSION_HORIZON_MS,
  JOB_ENQUEUE_LEASE_MS,
  MAX_D1_STATEMENTS_PER_SCHEDULER,
  MAX_SEND_BATCH,
  MOSCOW_OFFSET_MINUTES,
  MS_PER_MINUTE,
  RETENTION_MS,
  assertSafeInteger,
} from '../util.ts';
import { runSourceSync, type SourceDefinition, type SyncStatus } from '../calendar/sync.ts';
import type { CalendarParser } from '../calendar/parser.ts';

/** Upper bound on claim-and-enqueue passes in one tick. */
const MAX_ENQUEUE_GROUPS = 20;
/** claimDueJobs always spends exactly one statement. */
const CLAIM_STATEMENTS = 1;
/**
 * Statements cleanup spends after the enqueue loop (seven bounded deletes); the
 * loop must not consume them, so retention work still runs on an
 * enqueue-heavy tick.
 */
const CLEANUP_RESERVE_STATEMENTS = 7;

export interface TickDeps {
  repository: Repository;
  queue: QueueProducerLike<OutboundJobMessage>;
  now: Clock;
  logger: Logger;
  parser: CalendarParser;
  fetch: typeof fetch;
  sourceTimeZone: string;
  fetchTimeoutMs: number;
  sources: readonly SourceDefinition[];
  ownerFactory: () => string;
}

export interface TickResult {
  repaired: number;
  syncStatuses: string[];
  enqueued: number;
}

/**
 * Start of the current Monday in Europe/Moscow, or null outside Monday.
 * Moscow is fixed at UTC+03:00, matching the source and display timezone.
 */
export function currentMoscowMondayStartMs(now: number): number | null {
  assertSafeInteger(now, 'now');
  const offsetMs = MOSCOW_OFFSET_MINUTES * MS_PER_MINUTE;
  const moscow = new Date(now + offsetMs);
  if (moscow.getUTCDay() !== 1) {
    return null;
  }
  return (
    Date.UTC(moscow.getUTCFullYear(), moscow.getUTCMonth(), moscow.getUTCDate()) - offsetMs
  );
}

/**
 * Outside Monday only a source that was never attempted may run its first
 * fetch, so a fresh deployment does not wait for the next Moscow Monday before
 * any schedule exists. A source that already succeeded or failed waits for
 * Monday like any other refresh, so a broken first attempt is not repeated all
 * week.
 */
async function mayRunInitialFetch(deps: TickDeps, source: SourceDefinition): Promise<boolean> {
  const stored: SourceRecord | null = await deps.repository.getSource(source.id);
  return stored === null || (stored.fetchedAtMs === null && stored.status === 'unknown');
}

export async function runSchedulerTick(deps: TickDeps): Promise<TickResult> {
  deps.repository.beginInvocation(MAX_D1_STATEMENTS_PER_SCHEDULER);
  const startNow = deps.now();
  const repaired = await deps.repository.repairExpiredLeases(startNow);
  const mondayStartMs = currentMoscowMondayStartMs(startNow);

  // Sources run sequentially so each sync sees the remaining invocation budget:
  // a large (or unlucky) source defers instead of starving the planning,
  // enqueue and cleanup tail of the same tick.
  const syncStatuses: SyncStatus[] = [];
  for (const source of deps.sources) {
    try {
      if (mondayStartMs === null && !(await mayRunInitialFetch(deps, source))) {
        syncStatuses.push('skipped');
        continue;
      }
      const result = await runSourceSync(
        {
          repository: deps.repository,
          parser: deps.parser,
          fetch: deps.fetch,
          now: deps.now,
          logger: deps.logger,
          sourceTimeZone: deps.sourceTimeZone,
          fetchTimeoutMs: deps.fetchTimeoutMs,
          skipIfFetchedAtOrAfterMs: mondayStartMs ?? undefined,
        },
        source,
        deps.ownerFactory(),
      );
      syncStatuses.push(result.status);
    } catch {
      syncStatuses.push('error');
      deps.logger.warn('source_sync_crashed', { source: source.id });
    }
  }

  // The sync loop may have run for a while: re-read the clock so the whole
  // tail (plan, cancel, claims, cleanup) works from the post-sync time.
  const now = deps.now();
  await deps.repository.planDueReminders(now, now + EXPANSION_HORIZON_MS);
  await deps.repository.cancelStaleJobs(now);

  let enqueued = 0;
  for (let group = 0; group < MAX_ENQUEUE_GROUPS; group += 1) {
    // Stop before a claim would spend the statements cleanup still needs.
    if (!deps.repository.canAfford(CLAIM_STATEMENTS + CLEANUP_RESERVE_STATEMENTS)) {
      break;
    }
    const owner = deps.ownerFactory();
    // The enqueue reservation deliberately outlives a draining backlog, so an
    // overlapping tick cannot re-enqueue work that is still on the Queue. A
    // truly lost message is repaired once this reservation expires.
    const claimed = await deps.repository.claimDueJobs(
      owner,
      now,
      JOB_ENQUEUE_LEASE_MS,
      MAX_SEND_BATCH,
    );
    if (claimed.length === 0) {
      break;
    }
    await deps.queue.sendBatch(claimed.map((job) => ({ body: { jobId: job.jobId } })));
    enqueued += claimed.length;
    if (claimed.length < MAX_SEND_BATCH) {
      break;
    }
  }

  await deps.repository.cleanup(now, RETENTION_MS, CLEANUP_LIMIT);

  deps.logger.info('scheduler_tick', { repaired, enqueued });
  return { repaired, syncStatuses, enqueued };
}
