/**
 * Pure calendar domain: parser output in, normalized occurrences out.
 *
 * Recurrence expansion lives in the parser adapter, which is the only layer
 * that owns timezone-aware ICAL.Time values. This module is a small
 * deterministic normalizer.
 *
 * Invariants:
 * - Identity: every occurrence is keyed `uid#<origin start>`, where the origin
 *   is the generated instance start for regular occurrences and the
 *   RECURRENCE-ID time for overrides. Moving an instance changes startsAtMs
 *   but never its key, so an update replaces the stored occurrence instead of
 *   duplicating it.
 * - Overrides: a RECURRENCE-ID event replaces the generated instance at its
 *   origin; when several overrides share the origin of one generated instance,
 *   the last one in input order wins. An override whose origin was not
 *   expanded (moved into the horizon from beyond it, or parked on an EXDATE)
 *   is still emitted under that origin's identity, provided the override's own
 *   start is in the horizon. An all-day override is dropped individually.
 * - All-day masters are skipped by explicit policy; they drop the whole UID
 *   group, overrides included.
 * - Horizon: every emitted occurrence starts within
 *   [horizonStartMs, horizonEndMs]; instances outside are filtered here as a
 *   safety net.
 * - Duration: each occurrence's end derives from its own start and the
 *   source's duration, never copied from another instance. When the parser
 *   supplies per-occurrence ends (`expandedEndsMs`), those already encode RFC
 *   5545 nominal-vs-exact semantics; otherwise the master's exact delta is
 *   applied per occurrence (DTEND behavior).
 */

import { assertSafeInteger } from '../util.ts';

export type OccurrenceStatus = 'confirmed' | 'cancelled';

export interface ParsedEvent {
  uid: string;
  summary: string;
  description: string | null;
  url: string | null;
  startsAtMs: number;
  endsAtMs: number | null;
  status: OccurrenceStatus;
  isAllDay: boolean;
  /** When set, this event overrides the generated instance at this origin time. */
  recurrenceIdMs: number | null;
  /**
   * Occurrence start times (ms) already expanded by the parser within the
   * requested horizon. Null for overrides and for all-day events.
   */
  expandedStartsMs: readonly number[] | null;
  /**
   * Per-occurrence ends (ms) aligned index-for-index with `expandedStartsMs`.
   * Present only when the master's end derives from DURATION, so nominal
   * day/week components could follow wall-clock semantics across offset
   * transitions in the parser. `null` entries mean "no end"; an absent or null
   * field means ends derive exactly from `endsAtMs` (DTEND behavior).
   */
  expandedEndsMs?: readonly (number | null)[] | null;
  /** Resolved IANA zone, null for floating/all-day input. */
  timezone: string | null;
}

export interface ParsedCalendar {
  events: readonly ParsedEvent[];
}

export interface Occurrence {
  occurrenceKey: string;
  uid: string;
  startsAtMs: number;
  endsAtMs: number | null;
  summary: string;
  description: string | null;
  url: string | null;
  status: OccurrenceStatus;
  isAllDay: boolean;
  revision: number;
}

export class CalendarIntegrityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CalendarIntegrityError';
  }
}

export class AmbiguousCalendarError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'AmbiguousCalendarError';
  }
}

export class UnsupportedRecurrenceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UnsupportedRecurrenceError';
  }
}

export class RecurrenceLimitError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'RecurrenceLimitError';
  }
}

export interface BuildOptions {
  horizonStartMs: number;
  horizonEndMs: number;
}

/**
 * Normalizes parsed calendar events into stable occurrences.
 *
 * The parser owns recurrence expansion and its iteration bound; this function
 * applies the identity, override, all-day, horizon and duration invariants
 * documented above.
 */
export function buildOccurrences(calendar: ParsedCalendar, options: BuildOptions): Occurrence[] {
  const occurrences: Occurrence[] = [];
  for (const [uid, events] of groupEventsByUid(calendar.events)) {
    for (const occurrence of occurrencesForUid(uid, events, options)) {
      occurrences.push(occurrence);
    }
  }
  return occurrences;
}

/** An override of a recurring instance, carrying the origin time it replaces. */
type OverrideEvent = ParsedEvent & { recurrenceIdMs: number };

function isBaseEvent(event: ParsedEvent): boolean {
  return event.recurrenceIdMs === null;
}

function isOverride(event: ParsedEvent): event is OverrideEvent {
  return event.recurrenceIdMs !== null;
}

/** Identity of the generated instance at originStartMs; moving it does not change the key. */
function occurrenceKeyFor(uid: string, originStartMs: number): string {
  return `${uid}#${originStartMs}`;
}

function isWithinHorizon(ms: number, options: BuildOptions): boolean {
  return ms >= options.horizonStartMs && ms <= options.horizonEndMs;
}

function assertEvent(event: ParsedEvent): void {
  assertSafeInteger(event.startsAtMs, 'event.startsAtMs');
  if (event.endsAtMs !== null) {
    assertSafeInteger(event.endsAtMs, 'event.endsAtMs');
    if (event.endsAtMs < event.startsAtMs) {
      throw new CalendarIntegrityError('event ends before it starts');
    }
  }
  if (event.uid.length === 0) {
    throw new CalendarIntegrityError('event UID is empty');
  }
  if (event.recurrenceIdMs !== null) {
    assertSafeInteger(event.recurrenceIdMs, 'event.recurrenceIdMs');
  }
  if (!event.isAllDay && event.timezone === null) {
    throw new CalendarIntegrityError('floating time without an explicit source timezone');
  }
  if (event.expandedStartsMs !== null) {
    for (const startMs of event.expandedStartsMs) {
      assertSafeInteger(startMs, 'event.expandedStartsMs entry');
    }
  }
  const expandedEndsMs = event.expandedEndsMs ?? null;
  if (expandedEndsMs !== null) {
    if (event.expandedStartsMs === null || expandedEndsMs.length !== event.expandedStartsMs.length) {
      throw new CalendarIntegrityError('per-occurrence ends do not align with expanded starts');
    }
    for (const endMs of expandedEndsMs) {
      if (endMs !== null) {
        assertSafeInteger(endMs, 'event.expandedEndsMs entry');
      }
    }
  }
}

/** Per-instance duration so a generated occurrence ends after its own start. */
function durationOf(event: ParsedEvent): number | null {
  if (event.endsAtMs === null || event.endsAtMs < event.startsAtMs) {
    return null;
  }
  return event.endsAtMs - event.startsAtMs;
}

/**
 * One generated instance of a master: the origin start and the end already
 * derived per occurrence by the parser, or null when the end still has to be
 * flattened from the master delta.
 */
interface ExpandedInstance {
  readonly startMs: number;
  readonly endMs: number | null;
}

/**
 * Instances of a master in input order with duplicate starts collapsed (first
 * one wins, matching the previous `Set` behavior). Per-occurrence ends, when
 * present, stay aligned with their starts.
 */
function expandedInstances(base: ParsedEvent): ExpandedInstance[] {
  const starts = base.expandedStartsMs ?? [base.startsAtMs];
  const ends = base.expandedEndsMs ?? null;
  const seen = new Set<number>();
  const instances: ExpandedInstance[] = [];
  for (let index = 0; index < starts.length; index += 1) {
    const startMs = starts[index];
    if (startMs === undefined || seen.has(startMs)) {
      continue;
    }
    seen.add(startMs);
    instances.push({ startMs, endMs: ends === null ? null : ends[index] ?? null });
  }
  return instances;
}

/** Builds a timed occurrence; callers reject all-day input before this point. */
function toOccurrence(
  uid: string,
  originStartMs: number,
  source: ParsedEvent,
  startsAtMs: number,
  endsAtMs: number | null,
): Occurrence {
  return {
    occurrenceKey: occurrenceKeyFor(uid, originStartMs),
    uid,
    startsAtMs,
    endsAtMs,
    summary: source.summary,
    description: source.description,
    url: source.url,
    status: source.status,
    isAllDay: false,
    revision: 0,
  };
}

/** End of an occurrence: per-instance when the parser supplied it, else flattened. */
function occurrenceEndMs(instance: ExpandedInstance, base: ParsedEvent): number | null {
  if (base.expandedEndsMs !== null && base.expandedEndsMs !== undefined) {
    return instance.endMs;
  }
  const durationMs = durationOf(base);
  return durationMs === null ? null : instance.startMs + durationMs;
}

/**
 * Occurrence for an override, keyed by the origin it replaces, or null when
 * policy drops it (all-day, or moved outside the horizon). The override keeps
 * its own resolved start and end.
 */
function overrideOccurrence(
  uid: string,
  override: OverrideEvent,
  options: BuildOptions,
): Occurrence | null {
  if (override.isAllDay || !isWithinHorizon(override.startsAtMs, options)) {
    return null;
  }
  const durationMs = durationOf(override);
  return toOccurrence(
    uid,
    override.recurrenceIdMs,
    override,
    override.startsAtMs,
    durationMs === null ? null : override.startsAtMs + durationMs,
  );
}

/** Groups events by UID, validating every event exactly once. */
function groupEventsByUid(events: readonly ParsedEvent[]): Map<string, ParsedEvent[]> {
  const groups = new Map<string, ParsedEvent[]>();
  for (const event of events) {
    assertEvent(event);
    const group = groups.get(event.uid);
    if (group === undefined) {
      groups.set(event.uid, [event]);
    } else {
      group.push(event);
    }
  }
  return groups;
}

/**
 * Expands one UID's master and applies its overrides: generated instances
 * first (an override replaces the instance at its origin), then overrides
 * whose origin the expansion never produced.
 */
function occurrencesForUid(
  uid: string,
  events: readonly ParsedEvent[],
  options: BuildOptions,
): Occurrence[] {
  const base = events.find(isBaseEvent) ?? null;
  const overrides = events.filter(isOverride);

  if (base !== null && base.isAllDay) {
    return [];
  }

  const overrideByOrigin = new Map<number, OverrideEvent>();
  for (const override of overrides) {
    overrideByOrigin.set(override.recurrenceIdMs, override);
  }

  const occurrences: Occurrence[] = [];
  const appliedOverrideOrigins = new Set<number>();

  if (base !== null) {
    for (const instance of expandedInstances(base)) {
      const override = overrideByOrigin.get(instance.startMs);
      if (override !== undefined) {
        appliedOverrideOrigins.add(instance.startMs);
        const occurrence = overrideOccurrence(uid, override, options);
        if (occurrence !== null) {
          occurrences.push(occurrence);
        }
        continue;
      }
      if (isWithinHorizon(instance.startMs, options)) {
        occurrences.push(
          toOccurrence(uid, instance.startMs, base, instance.startMs, occurrenceEndMs(instance, base)),
        );
      }
    }
  }

  for (const override of overrides) {
    if (appliedOverrideOrigins.has(override.recurrenceIdMs)) {
      continue;
    }
    const occurrence = overrideOccurrence(uid, override, options);
    if (occurrence !== null) {
      occurrences.push(occurrence);
    }
  }

  return occurrences;
}
