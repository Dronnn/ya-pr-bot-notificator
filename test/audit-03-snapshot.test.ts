/**
 * F3 regression: snapshot deletion is confined to the validated horizon.
 *
 * The old publication deleted every future occurrence of the source that was
 * absent from staging, including dates beyond the 30-day expansion the snapshot
 * never validated, and reported a count from a different range. Now the DELETE
 * runs inside the atomic publication batch, matches the expansion's inclusive
 * range clamped to the publication clock, and the reported `deleted` is that
 * statement's `meta.changes`.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import type {
  DeletionHorizon,
  OccurrenceWrite,
  SnapshotPublication,
  SourceLease,
} from '../src/data/repository.ts';
import { EXPANSION_HORIZON_MS, MS_PER_MINUTE, SOURCE_LEASE_MS } from '../src/util.ts';
import { countRows } from './helpers/d1-sqlite.ts';
import { createHarness, type Harness } from './helpers/harness.ts';
import { occurrence, seedSource } from './helpers/seed.ts';

const NO_CURSOR = { etag: null, lastModified: null };

async function publish(
  harness: Harness,
  writes: readonly OccurrenceWrite[],
  now: number,
  horizon: DeletionHorizon,
  owner = 'publisher',
): Promise<SnapshotPublication> {
  const lease = await harness.repository.acquireSourceLease('basic', owner, now, SOURCE_LEASE_MS);
  assert.notEqual(lease, null);
  const published = await harness.repository.applySnapshot(
    'basic',
    writes,
    lease as SourceLease,
    NO_CURSOR,
    () => now,
    horizon,
  );
  assert.notEqual(published, null, 'the fixture lease must still be live');
  return published as SnapshotPublication;
}

function occurrenceIds(harness: Harness): string[] {
  const rows = harness.db.database
    .prepare('SELECT id FROM occurrences ORDER BY id')
    .all() as { id: string }[];
  return rows.map((row) => row.id);
}

describe('horizon-bounded snapshot deletion', () => {
  it('deletes a missing occurrence inside the horizon and reports the real delta', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    const horizon = { startMs: now, endMs: now + 30 * MS_PER_MINUTE };
    const keep = occurrence({ occurrenceKey: 'keep', startsAtMs: now + 5 * MS_PER_MINUTE });
    const gone = occurrence({ occurrenceKey: 'gone', startsAtMs: now + 10 * MS_PER_MINUTE });

    const lease = await harness.repository.acquireSourceLease('basic', 'publisher', now, SOURCE_LEASE_MS);
    assert.notEqual(lease, null);
    harness.db.stats.reset();
    const first = (await harness.repository.applySnapshot(
      'basic',
      [keep, gone],
      lease as SourceLease,
      NO_CURSOR,
      () => now,
      horizon,
    )) as SnapshotPublication;
    assert.deepEqual(first, { upserted: 2, deleted: 0 });
    assert.equal(
      harness.db.stats.statements,
      harness.repository.snapshotCost(2),
      'one staging statement plus the six-statement publication batch',
    );

    const before = countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences');
    const second = await publish(harness, [keep], now, horizon);
    assert.deepEqual(second, { upserted: 1, deleted: 1 });
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), before - 1);
    assert.deepEqual(occurrenceIds(harness), ['basic:keep'], 'only the missing in-horizon row is gone');
  });

  it('keeps an occurrence beyond the horizon end', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    const horizon = { startMs: now, endMs: now + 30 * MS_PER_MINUTE };
    const inside = occurrence({ occurrenceKey: 'inside', startsAtMs: now + 5 * MS_PER_MINUTE });
    const beyond = occurrence({ occurrenceKey: 'beyond', startsAtMs: now + 40 * MS_PER_MINUTE });

    assert.deepEqual(await publish(harness, [inside, beyond], now, horizon), {
      upserted: 2,
      deleted: 0,
    });
    assert.deepEqual(await publish(harness, [inside], now, horizon), {
      upserted: 1,
      deleted: 0,
    });
    assert.deepEqual(occurrenceIds(harness), ['basic:beyond', 'basic:inside']);
  });

  it('treats both horizon boundaries as inclusive and clamps the start to now', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    const horizon = { startMs: now + 10 * MS_PER_MINUTE, endMs: now + 20 * MS_PER_MINUTE };
    const beforeStart = occurrence({ occurrenceKey: 'before-start', startsAtMs: now + 10 * MS_PER_MINUTE - 1 });
    const atStart = occurrence({ occurrenceKey: 'at-start', startsAtMs: now + 10 * MS_PER_MINUTE });
    const atEnd = occurrence({ occurrenceKey: 'at-end', startsAtMs: now + 20 * MS_PER_MINUTE });
    const afterEnd = occurrence({ occurrenceKey: 'after-end', startsAtMs: now + 20 * MS_PER_MINUTE + 1 });

    await publish(harness, [beforeStart, atStart, atEnd, afterEnd], now, horizon);
    const before = countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences');
    const cleared = await publish(harness, [], now, horizon);
    assert.deepEqual(cleared, { upserted: 0, deleted: 2 }, 'exactly the two boundary rows are deletable');
    assert.equal(countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrences'), before - 2);
    assert.deepEqual(
      occurrenceIds(harness),
      ['basic:after-end', 'basic:before-start'],
      'rows on the outside of either inclusive boundary survive',
    );
  });

  it('never deletes occurrences that are already in the past at publication time', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    // A horizon whose start lies in the past must still not delete past rows:
    // the start is clamped to the publication clock.
    const horizon = { startMs: now - 30 * MS_PER_MINUTE, endMs: now + 30 * MS_PER_MINUTE };
    const past = occurrence({ occurrenceKey: 'past', startsAtMs: now - 10 * MS_PER_MINUTE });
    const future = occurrence({ occurrenceKey: 'future', startsAtMs: now + 5 * MS_PER_MINUTE });

    await publish(harness, [past, future], now, horizon);
    const cleared = await publish(harness, [], now, horizon);
    assert.deepEqual(cleared, { upserted: 0, deleted: 1 });
    assert.deepEqual(occurrenceIds(harness), ['basic:past'], 'history is preserved by the clamp');
  });

  it('clamps the deletion start to the clock value read after staging', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    await seedSource(harness.repository, 'basic', now);
    const horizon = { startMs: now, endMs: now + 30 * MS_PER_MINUTE };
    // The lease outlives the staged publication so only the horizon matters.
    const lease = await harness.repository.acquireSourceLease('basic', 'publisher', now, 60 * MS_PER_MINUTE);
    assert.notEqual(lease, null);
    await harness.repository.applySnapshot(
      'basic',
      [
        occurrence({ occurrenceKey: 'in-clamp', startsAtMs: now + MS_PER_MINUTE }),
        occurrence({ occurrenceKey: 'beyond-clamp', startsAtMs: now + 20 * MS_PER_MINUTE }),
      ],
      lease as SourceLease,
      NO_CURSOR,
      () => now,
      horizon,
    );

    // `applySnapshot` reads the clock twice: once before staging and once as
    // `publishedAt`. The deletion must use `MAX(horizon.startMs, publishedAt)`,
    // so the row before `publishedAt` survives even though it is inside the
    // horizon and absent from the new snapshot.
    const times = [now, now + 5 * MS_PER_MINUTE];
    let reads = 0;
    const publishedClock = (): number => times[Math.min(reads++, times.length - 1)] ?? now;
    const renewed = await harness.repository.acquireSourceLease(
      'basic',
      'publisher',
      now,
      60 * MS_PER_MINUTE,
    );
    assert.notEqual(renewed, null);
    const published = await harness.repository.applySnapshot(
      'basic',
      [],
      renewed as SourceLease,
      NO_CURSOR,
      publishedClock,
      horizon,
    );
    assert.deepEqual(published, { upserted: 0, deleted: 1 });
    assert.deepEqual(occurrenceIds(harness), ['basic:in-clamp']);
  });

  it('publishes and deletes nothing from a stale attempt', async () => {
    const harness = createHarness();
    const start = harness.clock.now();
    await seedSource(harness.repository, 'basic', start);
    const horizon = { startMs: start, endMs: start + EXPANSION_HORIZON_MS };
    const initialLease = await harness.repository.acquireSourceLease('basic', 'publisher', start, SOURCE_LEASE_MS);
    assert.notEqual(initialLease, null);
    const initial = await harness.repository.applySnapshot(
      'basic',
      [occurrence({ occurrenceKey: 'keep', startsAtMs: start + 5 * MS_PER_MINUTE })],
      initialLease as SourceLease,
      NO_CURSOR,
      () => start,
      horizon,
    );
    assert.deepEqual(initial, { upserted: 1, deleted: 0 });

    // A same-owner renewal bumps the generation and then expires, so the first
    // lease object is stale even before a new owner takes over.
    const staleLease = await harness.repository.acquireSourceLease('basic', 'publisher', start, 1_000);
    assert.notEqual(staleLease, null);
    harness.clock.advance(1_001);
    const now = harness.clock.now();
    const freshLease = await harness.repository.acquireSourceLease('basic', 'fresh-owner', now, SOURCE_LEASE_MS);
    assert.notEqual(freshLease, null);

    const stale = await harness.repository.applySnapshot(
      'basic',
      [occurrence({ occurrenceKey: 'stale-only', startsAtMs: start + 6 * MS_PER_MINUTE })],
      initialLease as SourceLease,
      NO_CURSOR,
      () => now,
      horizon,
    );
    assert.equal(stale, null, 'an expired, superseded lease cannot publish');
    assert.deepEqual(occurrenceIds(harness), ['basic:keep'], 'nothing was inserted or deleted');
    assert.equal(
      countRows(harness.db, 'SELECT COUNT(*) AS n FROM occurrence_staging'),
      0,
      'the stale attempt stages no visible rows',
    );

    const published = await harness.repository.applySnapshot(
      'basic',
      [occurrence({ occurrenceKey: 'fresh-only', startsAtMs: start + 7 * MS_PER_MINUTE })],
      freshLease as SourceLease,
      NO_CURSOR,
      () => now,
      horizon,
    );
    assert.deepEqual(published, { upserted: 1, deleted: 1 });
    assert.deepEqual(occurrenceIds(harness), ['basic:fresh-only']);
  });
});
