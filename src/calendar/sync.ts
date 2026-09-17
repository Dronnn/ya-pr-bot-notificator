/**
 * Source synchronization.
 *
 * A snapshot is only applied after it has been fully parsed and expanded. Any
 * malformed/truncated/unsupported input, oversized body, redirect or failed
 * request leaves the previously stored snapshot untouched.
 *
 * The request deadline covers the whole exchange, headers and body. A 304
 * refreshes freshness while the materialized horizon still covers the feed and,
 * at most once per HORIZON_REFRESH_MIN_INTERVAL_MS, while a needed re-expansion
 * is rate-limited; otherwise it performs a bounded unconditional fetch so
 * recurring occurrences keep being materialized. Because that fetch exists to
 * establish coverage, a second 304 is an invalid source response and is
 * recorded as a bounded failure (`unexpected_304`) instead of freshness.
 *
 * Horizon convention: expansion and publication share one strictly-future
 * range, `(now, horizonEndMs]`. It is represented as the inclusive millisecond
 * range `[now + 1, horizonEndMs]`: `buildOccurrences` filters
 * `ms >= start && ms <= end`, the parser stops at `ms > end`, and
 * `applySnapshot` deletes `starts_at_ms >= MAX(horizon.startMs, publishedAt)
 * AND starts_at_ms <= horizon.endMs`, so the exact range used for
 * `parseAndBuild` is the range handed to the atomic publication.
 */

import { buildOccurrences, type Occurrence } from '../domain/calendar.ts';
import type { Repository, OccurrenceWrite, SourceRecord } from '../data/repository.ts';
import type { Course } from '../domain/notification-policy.ts';
import type { Clock, Logger } from '../util.ts';
import {
  EXPANSION_HORIZON_MS,
  HORIZON_REFRESH_MARGIN_MS,
  HORIZON_REFRESH_MIN_INTERVAL_MS,
  MAX_EXPANSION_ITERATIONS,
  MAX_ICAL_BYTES,
  SOURCE_LEASE_MS,
  SYNC_TAIL_RESERVE,
} from '../util.ts';
import { cancelBody, readBoundedBody } from '../http-body.ts';
import type { CalendarParser } from './parser.ts';

export interface SourceDefinition {
  id: string;
  kind: Course;
  url: string;
}

export interface SyncDeps {
  repository: Repository;
  parser: CalendarParser;
  fetch: typeof fetch;
  now: Clock;
  logger: Logger;
  sourceTimeZone: string;
  fetchTimeoutMs: number;
  /** Skip network I/O when this source already fetched at or after this boundary. */
  skipIfFetchedAtOrAfterMs?: number;
}

export type SyncStatus = 'not-modified' | 'applied' | 'rejected' | 'skipped' | 'error';

/**
 * Outcome of one sync attempt. `upserted` and `deleted` are the committed
 * publication counts, so both are 0 unless `status` is `applied`; `reason`
 * carries the outcome code for every non-`applied` result.
 */
export interface SyncResult {
  status: SyncStatus;
  reason: string | null;
  upserted: number;
  deleted: number;
}

/** Complete snapshot body plus the validators to store alongside it. */
interface FetchedSnapshot {
  text: string;
  etag: string | null;
  lastModified: string | null;
}

/**
 * Classified outcome of one bounded fetch: `failed` is a transport or deadline
 * failure, `not-modified` a 304 whose body is not read, `redirect` and
 * `http-error` statuses whose body is cancelled unread, `truncated` an oversize
 * or incomplete 2xx body, and `complete` a usable snapshot.
 */
type FetchOutcome =
  | { kind: 'failed'; code: string }
  | { kind: 'not-modified' }
  | { kind: 'redirect' }
  | { kind: 'http-error'; status: number }
  | { kind: 'truncated' }
  | { kind: 'complete'; snapshot: FetchedSnapshot };

function errorCode(error: unknown): string {
  if (error instanceof Error && error.name.length > 0) {
    return error.name;
  }
  return 'unknown';
}

/** A skipped attempt that wrote nothing, for the given reason. */
function skipped(reason: string): SyncResult {
  return { status: 'skipped', reason, upserted: 0, deleted: 0 };
}

/** Validator headers that let an unchanged feed answer with 304. */
function conditionalHeadersFor(stored: SourceRecord | null): Record<string, string> {
  const headers: Record<string, string> = {};
  if (stored?.etag !== null && stored?.etag !== undefined) {
    headers['if-none-match'] = stored.etag;
  }
  if (stored?.lastModified !== null && stored?.lastModified !== undefined) {
    headers['if-modified-since'] = stored.lastModified;
  }
  return headers;
}

/**
 * Decides whether a 304 must be followed by a bounded unconditional fetch.
 * The retained horizon is only considered covering while its furthest
 * occurrence sits at least `HORIZON_REFRESH_MARGIN_MS` ahead, and refreshes are
 * rate-limited by `HORIZON_REFRESH_MIN_INTERVAL_MS`.
 */
async function horizonRefreshDue(
  deps: SyncDeps,
  source: SourceDefinition,
  stored: SourceRecord | null,
  now: number,
): Promise<boolean> {
  const maxStartMs = await deps.repository.getMaxOccurrenceStart(source.id);
  const coverageFloorMs = now + EXPANSION_HORIZON_MS - HORIZON_REFRESH_MARGIN_MS;
  const coverageExhausted = maxStartMs === null || maxStartMs < coverageFloorMs;
  const lastRefreshAtMs = stored?.lastRefreshAtMs ?? null;
  const refreshAllowed =
    lastRefreshAtMs === null || now - lastRefreshAtMs >= HORIZON_REFRESH_MIN_INTERVAL_MS;
  return coverageExhausted && refreshAllowed;
}

function looksComplete(text: string): boolean {
  return text.trimEnd().toUpperCase().endsWith('END:VCALENDAR');
}

/**
 * Performs one conditional or unconditional fetch and classifies the response:
 * a 304 is `not-modified`; a redirect or other non-2xx status is never read and
 * its body is cancelled; a 2xx body is read up to `MAX_ICAL_BYTES` and must end
 * with `END:VCALENDAR`. The single deadline stays active until the body has
 * been read or cancelled, and the timer is always cleared in `finally`, so one
 * stalled source cannot leak a timer.
 */
async function fetchSnapshot(
  deps: SyncDeps,
  url: string,
  headers: Record<string, string>,
): Promise<FetchOutcome> {
  const controller = new AbortController();
  const timer = setTimeout(() => {
    controller.abort();
  }, deps.fetchTimeoutMs);
  try {
    let response: Response;
    try {
      response = await deps.fetch(url, {
        method: 'GET',
        headers,
        redirect: 'manual',
        signal: controller.signal,
      });
    } catch (error) {
      return { kind: 'failed', code: errorCode(error) };
    }

    if (response.status === 304) {
      return { kind: 'not-modified' };
    }
    if (!response.ok) {
      // The body of a redirect or error status is never read, but cancellation
      // is still started.
      cancelBody(response);
      if (response.status >= 300 && response.status < 400) {
        return { kind: 'redirect' };
      }
      return { kind: 'http-error', status: response.status };
    }

    const etag = response.headers.get('etag');
    const lastModified = response.headers.get('last-modified');
    try {
      const text = await readBoundedBody(response, MAX_ICAL_BYTES, controller.signal);
      if (text === null || !looksComplete(text)) {
        return { kind: 'truncated' };
      }
      return { kind: 'complete', snapshot: { text, etag, lastModified } };
    } catch (error) {
      return { kind: 'failed', code: errorCode(error) };
    }
  } finally {
    clearTimeout(timer);
  }
}

/** Parses a validated snapshot and expands it into horizon-bounded occurrences. */
async function parseAndBuild(
  deps: SyncDeps,
  text: string,
  horizonStartMs: number,
  horizonEndMs: number,
): Promise<Occurrence[]> {
  const parsed = await deps.parser.parse(text, {
    sourceTimeZone: deps.sourceTimeZone,
    horizonStartMs,
    horizonEndMs,
    maxIterations: MAX_EXPANSION_ITERATIONS,
  });
  return buildOccurrences(parsed, { horizonStartMs, horizonEndMs });
}

function toWrites(source: SourceDefinition, occurrences: readonly Occurrence[]): OccurrenceWrite[] {
  return occurrences.map((occurrence) => ({
    id: `${source.id}:${occurrence.occurrenceKey}`,
    sourceId: source.id,
    uid: occurrence.uid,
    occurrenceKey: occurrence.occurrenceKey,
    course: source.kind,
    startsAtMs: occurrence.startsAtMs,
    endsAtMs: occurrence.endsAtMs,
    summary: occurrence.summary,
    description: occurrence.description,
    url: occurrence.url,
    status: occurrence.status,
    isAllDay: occurrence.isAllDay,
  }));
}

export async function runSourceSync(
  deps: SyncDeps,
  source: SourceDefinition,
  owner: string,
): Promise<SyncResult> {
  const now = deps.now();
  const lease = await deps.repository.acquireSourceLease(source.id, owner, now, SOURCE_LEASE_MS);
  if (lease === null) {
    return skipped('lease-held');
  }
  await deps.repository.ensureSource(source.id, source.kind, now);

  // Every write below is guarded by this attempt's owner and lease generation.
  // When the generation is no longer current the attempt is obsolete: it must
  // not touch the visible snapshot or the source metadata, and it reports a
  // truthful skipped outcome instead of claiming an update it did not make.
  const lostOwnership = (): SyncResult => {
    deps.logger.warn('source_sync_lost_ownership', { source: source.id });
    return skipped('lost-ownership');
  };

  const stored = await deps.repository.getSource(source.id);
  if (
    deps.skipIfFetchedAtOrAfterMs !== undefined &&
    stored?.fetchedAtMs !== null &&
    stored?.fetchedAtMs !== undefined &&
    stored.fetchedAtMs >= deps.skipIfFetchedAtOrAfterMs
  ) {
    return skipped('schedule');
  }

  /**
   * Records a fetch/parse failure under this attempt's custody and returns the
   * outcome to report: the failure result when the record was written, or the
   * lost-ownership result when the attempt is no longer current. The failure is
   * logged with `logFields` when given, and with `code` otherwise.
   */
  const recordFailure = async (
    code: string,
    resultStatus: 'error' | 'rejected',
    logEvent: string,
    logFields?: Record<string, unknown>,
  ): Promise<SyncResult> => {
    const recorded = await deps.repository.recordSourceError(
      source.id,
      source.kind,
      lease,
      code,
      deps.now,
    );
    if (!recorded) {
      return lostOwnership();
    }
    deps.logger.warn(logEvent, { source: source.id, ...(logFields ?? { code }) });
    return { status: resultStatus, reason: code, upserted: 0, deleted: 0 };
  };

  /** Refreshes freshness for an unchanged feed; custody decides the outcome. */
  const recordFresh = async (): Promise<SyncResult> => {
    const recorded = await deps.repository.recordSourceFresh(
      source.id,
      source.kind,
      lease,
      stored?.etag ?? null,
      stored?.lastModified ?? null,
      deps.now,
    );
    if (!recorded) {
      return lostOwnership();
    }
    return { status: 'not-modified', reason: null, upserted: 0, deleted: 0 };
  };

  let fetched = await fetchSnapshot(deps, source.url, conditionalHeadersFor(stored));
  if (fetched.kind === 'failed') {
    return recordFailure(fetched.code, 'error', 'source_fetch_failed');
  }

  if (fetched.kind === 'not-modified') {
    if (!(await horizonRefreshDue(deps, source, stored, now))) {
      return recordFresh();
    }

    // Coverage is running out: re-expand from a bounded unconditional fetch so
    // the retained snapshot advances instead of silently going stale. That
    // fetch needs a body; a second 304 proves nothing about coverage, so it is
    // an invalid source response and must not refresh freshness.
    fetched = await fetchSnapshot(deps, source.url, {});
    if (fetched.kind === 'failed') {
      return recordFailure(fetched.code, 'error', 'source_refresh_failed');
    }
    if (fetched.kind === 'not-modified') {
      return recordFailure('unexpected_304', 'error', 'source_refresh_unexpected_304');
    }
  }

  switch (fetched.kind) {
    case 'redirect':
      return recordFailure('redirect', 'rejected', 'source_redirect_rejected', {});
    case 'http-error':
      return recordFailure(`http_${fetched.status}`, 'error', 'source_http_error', {
        status: fetched.status,
      });
    case 'truncated':
      return recordFailure('truncated', 'rejected', 'source_snapshot_rejected');
    case 'complete':
      break;
  }

  const { text, etag, lastModified } = fetched.snapshot;
  // The materialized snapshot contains only events that have not started.
  const horizonStartMs = now + 1;
  const horizonEndMs = now + EXPANSION_HORIZON_MS;
  let occurrences: Occurrence[];
  try {
    occurrences = await parseAndBuild(deps, text, horizonStartMs, horizonEndMs);
  } catch (error) {
    return recordFailure(errorCode(error), 'rejected', 'source_snapshot_rejected');
  }

  const writes = toWrites(source, occurrences);
  // Preflight the whole publication (plus its trailing status writes) against
  // the invocation budget, keeping a reserve for the rest of the tick. When it
  // cannot fit nothing is mutated and the previous snapshot stays visible.
  const publicationCost = deps.repository.snapshotCost(writes.length) + SYNC_TAIL_RESERVE;
  if (!deps.repository.canAfford(publicationCost)) {
    deps.logger.warn('source_sync_deferred_budget', { source: source.id });
    return skipped('budget');
  }

  // The repository re-checks custody against its own clock after staging, so a
  // lease that lapses while the snapshot is being staged cannot publish. The
  // deletion horizon is exactly the inclusive range `parseAndBuild` expanded.
  const published = await deps.repository.applySnapshot(
    source.id,
    writes,
    lease,
    { etag, lastModified },
    deps.now,
    { startMs: horizonStartMs, endMs: horizonEndMs },
  );
  if (!published) {
    return lostOwnership();
  }
  deps.logger.info('source_snapshot_applied', {
    source: source.id,
    upserted: published.upserted,
    deleted: published.deleted,
  });
  return {
    status: 'applied',
    reason: null,
    upserted: published.upserted,
    deleted: published.deleted,
  };
}
