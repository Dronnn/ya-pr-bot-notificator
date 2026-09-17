/**
 * F7 regression: an unconditional refresh that is required to establish
 * horizon coverage must not accept another 304.
 *
 * A conditional 304 is only a freshness event while the stored coverage is
 * sufficient. When coverage is insufficient, sync makes one bounded
 * unconditional fetch; if that fetch answers 304 again the response proves
 * nothing, so sync records `unexpected_304` and leaves
 * `fetched_at_ms`/`last_success_at_ms`/`last_refresh_at_ms` and the previous
 * snapshot untouched.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { runSourceSync } from '../src/calendar/sync.ts';
import {
  EXPANSION_HORIZON_MS,
  HORIZON_REFRESH_MARGIN_MS,
  HORIZON_REFRESH_MIN_INTERVAL_MS,
  MS_PER_DAY,
  MS_PER_MINUTE,
} from '../src/util.ts';
import { textResponse, type FetchCall } from './helpers/fakes.ts';
import { createHarness, syncDeps, type Harness } from './helpers/harness.ts';
import { EMPTY_ICS, occurrence, parsedEvent, TEST_SOURCE } from './helpers/seed.ts';

/** 2024-06-01T00:00:00Z. */
const START = Date.UTC(2024, 5, 1);

interface SourceTimes {
  fetchedAtMs: number | null;
  lastSuccessAtMs: number | null;
  lastRefreshAtMs: number | null;
  status: string;
  lastErrorCode: string | null;
}

function sourceTimes(harness: Harness): SourceTimes {
  const row = harness.db.database
    .prepare(
      `SELECT fetched_at_ms, last_success_at_ms, last_refresh_at_ms, status, last_error_code
       FROM sources WHERE id = 'basic'`,
    )
    .get() as {
    fetched_at_ms: number | null;
    last_success_at_ms: number | null;
    last_refresh_at_ms: number | null;
    status: string;
    last_error_code: string | null;
  };
  return {
    fetchedAtMs: row.fetched_at_ms,
    lastSuccessAtMs: row.last_success_at_ms,
    lastRefreshAtMs: row.last_refresh_at_ms,
    status: row.status,
    lastErrorCode: row.last_error_code,
  };
}

function conditionalRequest(call: FetchCall): boolean {
  return ((call.init?.headers ?? {}) as Record<string, string>)['if-none-match'] !== undefined;
}

/** Applies one first snapshot at `START`; its publication sets `last_refresh`. */
async function publishFirst(harness: Harness): Promise<void> {
  assert.equal(harness.clock.now(), START);
  harness.setParser({ events: [parsedEvent(START + MS_PER_MINUTE, { uid: 'first' })] });
  harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'v1' }));
  const first = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-1');
  assert.equal(first.status, 'applied');
}

/** One millisecond below the coverage floor at `now`. */
function floorMsBelow(now: number): number {
  return now + EXPANSION_HORIZON_MS - HORIZON_REFRESH_MARGIN_MS - 1;
}

describe('F7 unconditional 304 after an exhausted horizon', () => {
  it('refreshes freshness for a conditional 304 while coverage is sufficient', async () => {
    const harness = createHarness({ now: START });
    const farMs = START + 29 * MS_PER_DAY;
    harness.setParser({ events: [parsedEvent(farMs, { uid: 'far' })] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'v1' }));

    const first = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-1');
    assert.equal(first.status, 'applied');
    assert.equal(await harness.repository.getMaxOccurrenceStart('basic'), farMs);

    harness.clock.advance(MS_PER_DAY);
    const now = harness.clock.now();
    harness.setHandler(() => new Response(null, { status: 304 }));
    const second = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-2');

    assert.equal(second.status, 'not-modified');
    assert.equal(harness.fetchSpy.calls.length, 1, 'sufficient coverage needs no refresh fetch');
    assert.ok(conditionalRequest(harness.fetchSpy.calls[0] as FetchCall));
    const source = await harness.repository.getSource('basic');
    assert.equal(source?.fetchedAtMs, now);
    assert.equal(source?.lastRefreshAtMs, START, 'no re-expansion was needed');
    assert.deepEqual(sourceTimes(harness), {
      fetchedAtMs: now,
      lastSuccessAtMs: now,
      lastRefreshAtMs: START,
      status: 'ok',
      lastErrorCode: null,
    });
  });

  it('follows an insufficient-coverage 304 with a 200 that publishes the expanded coverage', async () => {
    const harness = createHarness({ now: START });
    const firstMs = START + 9 * MS_PER_DAY;
    harness.setParser({ events: [parsedEvent(firstMs, { uid: 'first' })] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'v1' }));
    const first = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-1');
    assert.equal(first.status, 'applied');

    harness.clock.advance(2 * MS_PER_DAY);
    const now = harness.clock.now();
    const expandedMs = now + 25 * MS_PER_DAY;
    harness.setParser({
      events: [parsedEvent(firstMs, { uid: 'first' }), parsedEvent(expandedMs, { uid: 'expanded' })],
    });
    harness.setHandler((_url, init) => {
      const headers = (init?.headers ?? {}) as Record<string, string>;
      if (headers['if-none-match'] === 'v1') {
        return new Response(null, { status: 304 });
      }
      return textResponse(EMPTY_ICS, 200, { etag: 'v2' });
    });

    const second = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-2');

    assert.equal(second.status, 'applied');
    assert.equal(second.upserted, 2);
    assert.equal(second.deleted, 0);
    assert.equal(harness.fetchSpy.calls.length, 2);
    assert.ok(conditionalRequest(harness.fetchSpy.calls[0] as FetchCall));
    assert.ok(!conditionalRequest(harness.fetchSpy.calls[1] as FetchCall), 'the refresh is unconditional');
    assert.equal(
      await harness.repository.getMaxOccurrenceStart('basic'),
      expandedMs,
      'the materialized horizon advanced',
    );
    assert.deepEqual(sourceTimes(harness), {
      fetchedAtMs: now,
      lastSuccessAtMs: now,
      lastRefreshAtMs: now,
      status: 'ok',
      lastErrorCode: null,
    });
  });

  it('rejects an unconditional 304 as unexpected, keeps freshness and the previous snapshot', async () => {
    const harness = createHarness({ now: START });
    const firstMs = START + 9 * MS_PER_DAY;
    harness.setParser({ events: [parsedEvent(firstMs, { uid: 'first' })] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200, { etag: 'v1' }));
    const first = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-1');
    assert.equal(first.status, 'applied');

    harness.clock.advance(2 * MS_PER_DAY);
    const now = harness.clock.now();
    harness.setHandler((_url, init) => {
      const headers = (init?.headers ?? {}) as Record<string, string>;
      if (headers['if-none-match'] === 'v1') {
        return new Response(null, { status: 304 });
      }
      return new Response(null, { status: 304 });
    });

    const second = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-2');

    assert.equal(second.status, 'error');
    assert.equal(second.reason, 'unexpected_304');
    assert.equal(second.upserted, 0);
    assert.equal(second.deleted, 0);
    assert.ok(
      harness.logger.lines.some((line) => line.includes('source_refresh_unexpected_304')),
      'the invalid response is logged as a bounded source failure',
    );
    assert.equal(harness.fetchSpy.calls.length, 2);
    assert.ok(!conditionalRequest(harness.fetchSpy.calls[1] as FetchCall));
    assert.deepEqual(sourceTimes(harness), {
      fetchedAtMs: START,
      lastSuccessAtMs: START,
      lastRefreshAtMs: START,
      status: 'error',
      lastErrorCode: 'unexpected_304',
    });
    assert.equal(
      await harness.repository.getMaxOccurrenceStart('basic'),
      firstMs,
      'the previous coverage is unchanged',
    );
    const upcoming = await harness.repository.listUpcomingOccurrences('basic', now, 10);
    assert.deepEqual(
      upcoming.map((row) => row.startsAtMs),
      [firstMs],
      'the previous snapshot stays visible',
    );
  });

  it('treats a furthest occurrence exactly at the coverage floor as sufficient', async () => {
    const harness = createHarness({ now: START });
    await publishFirst(harness);

    harness.clock.advance(HORIZON_REFRESH_MIN_INTERVAL_MS);
    const now = harness.clock.now();
    const floorMs = now + EXPANSION_HORIZON_MS - HORIZON_REFRESH_MARGIN_MS;
    await harness.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'floor', startsAtMs: floorMs })],
      now,
    );
    harness.setHandler(() => new Response(null, { status: 304 }));

    const second = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-2');

    assert.equal(second.status, 'not-modified');
    assert.equal(harness.fetchSpy.calls.length, 1, 'coverage exactly at the floor needs no refresh');
    assert.equal(
      (await harness.repository.getSource('basic'))?.lastRefreshAtMs,
      START,
      'a freshness-only 304 does not re-expand',
    );
  });

  it('allows the refresh at exactly the minimum interval and blocks it one millisecond earlier', async () => {
    const allowed = createHarness({ now: START });
    await publishFirst(allowed);
    allowed.clock.advance(HORIZON_REFRESH_MIN_INTERVAL_MS);
    const now = allowed.clock.now();
    await allowed.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'below-floor', startsAtMs: floorMsBelow(now) })],
      now,
    );
    allowed.setHandler((_url, init) => {
      const headers = (init?.headers ?? {}) as Record<string, string>;
      if (headers['if-none-match'] === 'v1') {
        return new Response(null, { status: 304 });
      }
      return textResponse(EMPTY_ICS, 200, { etag: 'v2' });
    });

    const refreshed = await runSourceSync(syncDeps(allowed), TEST_SOURCE, 'owner-2');

    assert.equal(refreshed.status, 'applied', 'exactly one interval later the refresh is allowed');
    assert.equal(allowed.fetchSpy.calls.length, 2);
    assert.ok(conditionalRequest(allowed.fetchSpy.calls[0] as FetchCall));
    assert.ok(!conditionalRequest(allowed.fetchSpy.calls[1] as FetchCall));
    assert.equal((await allowed.repository.getSource('basic'))?.lastRefreshAtMs, now);

    const blocked = createHarness({ now: START });
    await publishFirst(blocked);
    blocked.clock.advance(HORIZON_REFRESH_MIN_INTERVAL_MS - 1);
    const blockedNow = blocked.clock.now();
    await blocked.repository.upsertOccurrences(
      [occurrence({ occurrenceKey: 'below-floor', startsAtMs: floorMsBelow(blockedNow) })],
      blockedNow,
    );
    blocked.setHandler(() => new Response(null, { status: 304 }));

    const rateLimited = await runSourceSync(syncDeps(blocked), TEST_SOURCE, 'owner-2');

    assert.equal(rateLimited.status, 'not-modified', 'one ms earlier the refresh is rate-limited');
    assert.equal(blocked.fetchSpy.calls.length, 1);
    const source = await blocked.repository.getSource('basic');
    assert.equal(source?.lastRefreshAtMs, START, 'the blocked refresh must not re-expand');
    assert.equal(source?.fetchedAtMs, blockedNow, 'freshness still refreshes');
  });
});
