/**
 * F5 regression (repo part): the durable send pacer admits at most 20 starts
 * in ANY rolling 1,000 ms interval, atomically in D1, honoring the global 429
 * cooldown. The old fixed-window counter admitted 1 start at 0 ms, 19 at
 * 900 ms and 20 more at 1,000 ms (39 starts with 19 violations in the rolling
 * [900, 1000] interval). The rolling `rate_starts` guard rejects that burst.
 *
 * Window predicate, shared with the detector: a start at `now` is admitted
 * only when COUNT(started_at_ms IN (now-1000, now]) < max. Restoring the
 * fixed-window reset (mutation probe) reopens the 900/1000 ms burst.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { BOT_SEND_PACE_PER_SECOND } from '../src/util.ts';
import { findPacingViolations } from './helpers/simulation.ts';
import { createHarness } from './helpers/harness.ts';

const MAX = BOT_SEND_PACE_PER_SECOND;

describe('rolling one-second pacer', () => {
  it('rejects the old 0/900/1000 ms reset-boundary burst', async () => {
    const harness = createHarness();
    const base = harness.clock.now();
    const starts: { atMs: number }[] = [];

    assert.equal(await harness.repository.acquireSendSlot(base, MAX), true);
    starts.push({ atMs: base });
    for (let index = 0; index < MAX - 1; index += 1) {
      assert.equal(
        await harness.repository.acquireSendSlot(base + 900, MAX),
        true,
        'the first window still has room at 900 ms',
      );
      starts.push({ atMs: base + 900 });
    }
    // The old fixed window would admit 20 more at the 1,000 ms reset; the
    // rolling window still sees the 19 starts from 900 ms and admits exactly
    // one (20 total in (0, 1000]).
    let admittedAtReset = 0;
    for (let index = 0; index < MAX; index += 1) {
      if (await harness.repository.acquireSendSlot(base + 1000, MAX)) {
        admittedAtReset += 1;
        starts.push({ atMs: base + 1000 });
      }
    }
    assert.equal(admittedAtReset, 1, 'only one slot is free at the old reset boundary');
    assert.equal(
      findPacingViolations(starts),
      0,
      'no rolling one-second interval exceeds 20 starts',
    );
  });

  it('holds the bound across ties, concurrent owners and second boundaries', async () => {
    const harness = createHarness();
    const base = harness.clock.now();
    const starts: { atMs: number }[] = [];

    // Twenty concurrent owners at the same millisecond: all fit, 21st ties fail.
    const tied = await Promise.all(
      Array.from({ length: 25 }, (_, index) =>
        harness.repository.acquireSendSlot(base, MAX).then((ok) => ({ ok, index })),
      ),
    );
    assert.equal(
      tied.filter((result) => result.ok).length,
      MAX,
      'exactly one window-full of tied starts is admitted',
    );
    for (const result of tied) {
      if (result.ok) {
        starts.push({ atMs: base });
      }
    }

    // Spread the next 40 starts 50 ms apart across two seconds.
    for (let step = 1; step <= 40; step += 1) {
      if (await harness.repository.acquireSendSlot(base + step * 50, MAX)) {
        starts.push({ atMs: base + step * 50 });
      }
    }
    assert.equal(findPacingViolations(starts), 0);
    assert.ok(starts.length > MAX, 'later starts still flow as the window slides');
  });

  it('the detector itself flags the old violation shape', () => {
    const fabricated = [
      { atMs: 0 },
      ...Array.from({ length: 19 }, () => ({ atMs: 900 })),
      ...Array.from({ length: 20 }, () => ({ atMs: 1000 })),
    ];
    assert.ok(
      findPacingViolations(fabricated) > 0,
      'the detector is not vacuous: the 39-start burst violates',
    );
  });

  it('honors the global 429 cooldown and prunes old starts', async () => {
    const harness = createHarness();
    const base = harness.clock.now();
    assert.equal(await harness.repository.acquireSendSlot(base, MAX), true);

    harness.clock.set(base + 10);
    await harness.repository.setSendCooldown(base + 5_000, harness.clock.now());
    assert.equal(
      await harness.repository.acquireSendSlot(harness.clock.now(), MAX),
      false,
      'no start may begin inside a recorded cooldown',
    );

    harness.clock.set(base + 5_001);
    assert.equal(
      await harness.repository.acquireSendSlot(harness.clock.now(), MAX),
      true,
      'starts resume once the cooldown ends',
    );
    const remaining = harness.db.database
      .prepare('SELECT COUNT(*) AS n FROM rate_starts')
      .get() as { n: number };
    assert.ok(Number(remaining.n) <= MAX + 1, `pruning bounds the table (has ${remaining.n})`);
  });

  it('a full window drains and refills exactly', async () => {
    const harness = createHarness();
    const base = harness.clock.now();
    for (let index = 0; index < MAX; index += 1) {
      assert.equal(await harness.repository.acquireSendSlot(base, MAX), true);
    }
    assert.equal(await harness.repository.acquireSendSlot(base, MAX), false);
    // At +1000 the boundary rows (== now-1000) no longer count: the window is
    // (now-1000, now], so a full refill is admitted and stays violation-free.
    const refilled: { atMs: number }[] = Array.from({ length: MAX }, () => ({ atMs: base }));
    for (let index = 0; index < MAX; index += 1) {
      assert.equal(await harness.repository.acquireSendSlot(base + 1000, MAX), true);
      refilled.push({ atMs: base + 1000 });
    }
    assert.equal(findPacingViolations(refilled), 0);
  });

  it('once-per-batch prune bounds the table without changing admissions', async () => {
    const harness = createHarness();
    const base = harness.clock.now();
    const countStarts = (): number =>
      Number(
        (
          harness.db.database.prepare('SELECT COUNT(*) AS n FROM rate_starts').get() as {
            n: number;
          }
        ).n,
      );

    // A full window at base, then traffic a full 10 s later: the old rows are
    // outside the new window, so the predicate ignores them even before GC.
    for (let index = 0; index < MAX; index += 1) {
      assert.equal(await harness.repository.acquireSendSlot(base, MAX), true);
    }
    const later = base + 10_000;
    for (let index = 0; index < 5; index += 1) {
      assert.equal(await harness.repository.acquireSendSlot(later, MAX), true);
    }
    assert.equal(countStarts(), MAX + 5, 'reservations never prune on their own');

    // The batch-start prune drops exactly the rows no future admission can
    // observe (started_at <= now-1000 can never satisfy `> now\u2032-1000`).
    await harness.repository.pruneRateStarts(later);
    assert.equal(countStarts(), 5, 'only the live window survives GC');

    // The window still admits exactly to its cap with zero violations.
    const granted: { atMs: number }[] = Array.from({ length: 5 }, () => ({ atMs: later }));
    for (let index = 0; index < MAX - 5; index += 1) {
      assert.equal(await harness.repository.acquireSendSlot(later, MAX), true);
      granted.push({ atMs: later });
    }
    assert.equal(await harness.repository.acquireSendSlot(later, MAX), false);
    assert.equal(findPacingViolations(granted), 0);
    assert.ok(countStarts() <= MAX, `the table holds at most one window (${countStarts()})`);
  });
});
