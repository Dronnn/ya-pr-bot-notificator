/**
 * F1 regression: webhook update acquisition is an explicit three-way state.
 *
 * A completed duplicate is `done`; a live processing lease held by another
 * owner is `busy` and must leave that lease untouched; an expired processing
 * lease or a released update is `acquired` again. The old boolean conflated
 * `done` and `busy`, which made the worker acknowledge an in-flight update and
 * lose it when the other invocation died.
 */

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { WEBHOOK_LEASE_MS } from '../src/util.ts';
import { createHarness, type Harness } from './helpers/harness.ts';

interface UpdateRow {
  lease_owner: unknown;
  lease_expires_at_ms: unknown;
  status: unknown;
}

function updateRow(harness: Harness, updateId: number): UpdateRow {
  const row = harness.db.database
    .prepare('SELECT lease_owner, lease_expires_at_ms, status FROM processed_updates WHERE update_id = ?')
    .get(updateId) as UpdateRow | undefined;
  assert.notEqual(row, undefined, `update ${updateId} must exist`);
  return row as UpdateRow;
}

describe('webhook update acquisition states', () => {
  it('answers done for a completed duplicate without touching it', async () => {
    const harness = createHarness();
    const now = harness.clock.now();

    assert.equal(
      await harness.repository.tryBeginUpdate(7, 'owner-a', now, WEBHOOK_LEASE_MS),
      'acquired',
    );
    assert.equal(await harness.repository.completeUpdate(7, 'owner-a', now), true);

    const before = updateRow(harness, 7);
    harness.db.stats.reset();
    assert.equal(
      await harness.repository.tryBeginUpdate(7, 'owner-b', now + 10_000, WEBHOOK_LEASE_MS),
      'done',
    );
    assert.equal(harness.db.stats.statements, 2, 'a done answer is the upsert plus the status read');
    assert.deepEqual(updateRow(harness, 7), before, 'the completed row is not rewritten');
  });

  it('answers busy for another live owner and leaves that lease untouched', async () => {
    const harness = createHarness();
    const now = harness.clock.now();

    assert.equal(
      await harness.repository.tryBeginUpdate(8, 'owner-a', now, WEBHOOK_LEASE_MS),
      'acquired',
    );
    const held = updateRow(harness, 8);

    harness.db.stats.reset();
    assert.equal(
      await harness.repository.tryBeginUpdate(8, 'owner-b', now + 1, WEBHOOK_LEASE_MS),
      'busy',
    );
    assert.equal(harness.db.stats.statements, 2, 'a busy answer is the upsert plus the status read');
    assert.deepEqual(updateRow(harness, 8), held, "the other owner's lease must not change");
    assert.equal(held.lease_owner, 'owner-a');
    assert.equal(held.status, 'processing');
  });

  it('acquires an expired processing lease and lets it complete exactly once', async () => {
    const harness = createHarness();
    const now = harness.clock.now();

    assert.equal(await harness.repository.tryBeginUpdate(9, 'owner-a', now, 1_000), 'acquired');
    harness.clock.advance(1_001);
    const later = harness.clock.now();

    assert.equal(await harness.repository.tryBeginUpdate(9, 'owner-b', later, 1_000), 'acquired');
    assert.equal(
      await harness.repository.completeUpdate(9, 'owner-a', later),
      false,
      'the expired owner must not complete the update',
    );
    assert.equal(await harness.repository.completeUpdate(9, 'owner-b', later), true);
    assert.equal(
      await harness.repository.tryBeginUpdate(9, 'owner-c', later, 1_000),
      'done',
      'once completed the update stays done',
    );
  });

  it('re-acquires after an explicit release', async () => {
    const harness = createHarness();
    const now = harness.clock.now();

    assert.equal(await harness.repository.tryBeginUpdate(10, 'owner-a', now, WEBHOOK_LEASE_MS), 'acquired');
    assert.equal(await harness.repository.releaseUpdate(10, 'owner-a'), true);
    assert.equal(await harness.repository.tryBeginUpdate(10, 'owner-b', now, WEBHOOK_LEASE_MS), 'acquired');
    assert.equal(await harness.repository.completeUpdate(10, 'owner-b', now), true);
  });

  it('acquires a processing row with a NULL owner or NULL expiry', async () => {
    const harness = createHarness();
    const now = harness.clock.now();
    harness.db.exec(
      `INSERT INTO processed_updates (update_id, lease_owner, lease_expires_at_ms, status, processed_at_ms)
       VALUES (12, NULL, ${now + WEBHOOK_LEASE_MS}, 'processing', NULL),
              (13, 'ghost', NULL, 'processing', NULL)`,
    );

    assert.equal(await harness.repository.tryBeginUpdate(12, 'owner-a', now, 1_000), 'acquired');
    assert.equal(await harness.repository.tryBeginUpdate(13, 'owner-b', now, 1_000), 'acquired');
    assert.equal(updateRow(harness, 12).lease_owner, 'owner-a');
    assert.equal(updateRow(harness, 13).lease_owner, 'owner-b');
  });

  it('lets the current owner renew its own live lease', async () => {
    const harness = createHarness();
    const now = harness.clock.now();

    assert.equal(
      await harness.repository.tryBeginUpdate(11, 'owner-a', now, WEBHOOK_LEASE_MS),
      'acquired',
    );
    const firstExpiry = Number(updateRow(harness, 11).lease_expires_at_ms);
    assert.equal(
      await harness.repository.tryBeginUpdate(11, 'owner-a', now + 5_000, WEBHOOK_LEASE_MS),
      'acquired',
    );
    assert.equal(
      Number(updateRow(harness, 11).lease_expires_at_ms),
      now + 5_000 + WEBHOOK_LEASE_MS,
    );
    assert.ok(firstExpiry < now + 5_000 + WEBHOOK_LEASE_MS);
  });
});
