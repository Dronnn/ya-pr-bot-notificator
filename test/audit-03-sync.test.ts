/**
 * F3 sync regression: the publication deletes exactly the rows the expansion
 * compared and reports the committed delta.
 *
 * Horizon convention (shared verbatim with `parseAndBuild`): both ends of
 * `[horizonStartMs, horizonEndMs]` are inclusive. `buildOccurrences` keeps
 * `ms >= start && ms <= end`, and the publication deletes
 * `starts_at_ms >= MAX(horizon.startMs, publishedAt) AND starts_at_ms <= horizon.endMs`,
 * with the start additionally clamped to the publication clock so history is
 * never removed by a horizon rule.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { runSourceSync } from '../src/calendar/sync.ts';
import type { ParsedEvent } from '../src/domain/calendar.ts';
import {
  EXPANSION_HORIZON_MS,
  MS_PER_DAY,
  MS_PER_MINUTE,
  SOURCE_LEASE_MS,
} from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { textResponse } from './helpers/fakes.ts';
import { createHarness, syncDeps, type Harness } from './helpers/harness.ts';
import { EMPTY_ICS, occurrence, parsedEvent, seedSource, TEST_SOURCE } from './helpers/seed.ts';

/** A stored occurrence whose key matches what the parser would expand for `uid`. */
function rowFor(uid: string, startsAtMs: number): ReturnType<typeof occurrence> {
  return occurrence({ uid, occurrenceKey: `${uid}#${startsAtMs}`, startsAtMs });
}

/** The parsed-event shape that derives the same key as `rowFor`. */
function eventFor(uid: string, startsAtMs: number): ParsedEvent {
  return parsedEvent(startsAtMs, { uid });
}

function occurrenceIds(harness: Harness): string[] {
  const rows = harness.db.database
    .prepare('SELECT id FROM occurrences ORDER BY id')
    .all() as { id: string }[];
  return rows.map((row) => row.id);
}

function appliedLog(harness: Harness): Record<string, unknown> {
  const line = harness.logger.lines.find((entry) => entry.includes('source_snapshot_applied'));
  assert.ok(line !== undefined, 'expected a source_snapshot_applied log line');
  return JSON.parse(line) as Record<string, unknown>;
}

describe('F3 sync-side horizon publication', () => {
  it('deletes a missing occurrence inside the validated range and reports the real delta', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.upsertOccurrences(
      [
        rowFor('keep', now + 5 * MS_PER_MINUTE),
        rowFor('gone', now + 10 * MS_PER_MINUTE),
        rowFor('far', now + EXPANSION_HORIZON_MS + MS_PER_DAY),
      ],
      now,
    );
    harness.setParser({ events: [eventFor('keep', now + 5 * MS_PER_MINUTE)] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));

    const before = countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences');
    const result = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-1');

    assert.equal(result.status, 'applied');
    assert.equal(result.upserted, 1);
    assert.equal(result.deleted, 1, 'the missing in-horizon occurrence is deleted');
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'),
      before - 1,
      'the reported deleted count equals the committed row delta',
    );
    assert.deepEqual(occurrenceIds(harness), [
      `basic:far#${now + EXPANSION_HORIZON_MS + MS_PER_DAY}`,
      `basic:keep#${now + 5 * MS_PER_MINUTE}`,
    ]);
    assert.equal(appliedLog(harness).deleted, 1, 'the log reports the committed count');
    assert.equal(appliedLog(harness).upserted, 1);
  });

  it('keeps an occurrence beyond the horizon end that the snapshot never compared', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    const beyondMs = now + EXPANSION_HORIZON_MS + MS_PER_MINUTE;
    await harness.repository.upsertOccurrences([rowFor('beyond', beyondMs)], now);
    harness.setParser({ events: [] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));

    const result = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-1');

    assert.equal(result.status, 'applied');
    assert.equal(result.deleted, 0);
    assert.deepEqual(occurrenceIds(harness), [`basic:beyond#${beyondMs}`]);
  });

  it('treats the strictly-future horizon ends as inclusive', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    const atEndMs = now + EXPANSION_HORIZON_MS;
    const afterEndMs = atEndMs + 1;
    const atNowMs = now;
    const justBeforeNowMs = now - 1;
    const atHorizonStartMs = now + 1;
    await harness.repository.upsertOccurrences(
      [
        rowFor('at-end', atEndMs),
        rowFor('after-end', afterEndMs),
        rowFor('at-now', atNowMs),
        rowFor('before-now', justBeforeNowMs),
        rowFor('at-horizon-start', atHorizonStartMs),
      ],
      now,
    );
    harness.setParser({ events: [] });
    harness.setHandler(() => textResponse(EMPTY_ICS, 200));

    const result = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-1');

    assert.equal(result.status, 'applied');
    assert.equal(result.deleted, 2, 'the first future millisecond and inclusive end are deletable');
    assert.deepEqual(occurrenceIds(harness), [
      `basic:after-end#${afterEndMs}`,
      `basic:at-now#${atNowMs}`,
      `basic:before-now#${justBeforeNowMs}`,
    ]);
  });

  it('reports a lost-ownership publisher as skipped and deletes and publishes nothing', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.upsertOccurrences([rowFor('keep', now + 10 * MS_PER_MINUTE)], now);
    harness.setParser({ events: [eventFor('new', now + 20 * MS_PER_MINUTE)] });
    // The fetch consumes the whole lease, so publication must fail custody.
    harness.setHandler(() => {
      harness.clock.advance(SOURCE_LEASE_MS + 1);
      return textResponse(EMPTY_ICS, 200);
    });

    const before = countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences');
    const result = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-1');

    assert.equal(result.status, 'skipped');
    assert.equal(result.reason, 'lost-ownership');
    assert.equal(result.upserted, 0);
    assert.equal(result.deleted, 0);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), before);
    assert.deepEqual(occurrenceIds(harness), [`basic:keep#${now + 10 * MS_PER_MINUTE}`]);
    assert.ok(
      harness.logger.lines.some((line) => line.includes('source_sync_lost_ownership')),
      'the lost attempt is logged',
    );
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrence_staging'),
      0,
      'a stale attempt leaves no staging rows behind',
    );
  });

  it('reports a same-owner generation switch as lost ownership and deletes nothing', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    await harness.repository.upsertOccurrences([rowFor('keep', now + 10 * MS_PER_MINUTE)], now);
    harness.setParser({ events: [eventFor('new', now + 20 * MS_PER_MINUTE)] });

    let reregisteredGeneration = -1;
    harness.setHandler(async () => {
      harness.clock.advance(SOURCE_LEASE_MS + 1);
      const reregistered = await harness.repository.acquireSourceLease(
        'basic',
        'owner-1',
        harness.clock.now(),
        SOURCE_LEASE_MS,
      );
      assert.ok(reregistered !== null, 'the same owner may re-acquire after expiry');
      reregisteredGeneration = reregistered.generation;
      return textResponse(EMPTY_ICS, 200);
    });

    const before = countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences');
    const result = await runSourceSync(syncDeps(harness), TEST_SOURCE, 'owner-1');

    assert.equal(result.status, 'skipped');
    assert.equal(result.reason, 'lost-ownership');
    assert.equal(result.deleted, 0);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), before);
    assert.deepEqual(occurrenceIds(harness), [`basic:keep#${now + 10 * MS_PER_MINUTE}`]);
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrence_staging'), 0);
    const lock = harness.db.database
      .prepare('SELECT owner, generation FROM locks WHERE name = ?')
      .get('source:basic') as { owner: string; generation: number };
    assert.equal(lock.owner, 'owner-1');
    assert.equal(
      Number(lock.generation),
      reregisteredGeneration,
      'the stale attempt must not clobber the newer generation',
    );
  });
});
