/**
 * Calendar parser port. Kept behind an interface so the pipeline can be tested
 * without loading the ical.js runtime and so alternate parsers can be injected.
 *
 * A parser expands recurrence inside the horizon given in `ParseOptions` and
 * rejects malformed or unsupported input by throwing; the sync pipeline treats
 * a thrown error as a rejected snapshot and keeps the previous data.
 */

import type { ParsedCalendar } from '../domain/calendar.ts';

/** Inputs every parser implementation must honour. */
export interface ParseOptions {
  /** Explicit IANA zone used to interpret floating times for this source. */
  sourceTimeZone: string;
  /** Inclusive lower bound (ms) for materialized occurrence starts. */
  horizonStartMs: number;
  /** Inclusive upper bound (ms) for materialized occurrence starts. */
  horizonEndMs: number;
  /** Safety bound on recurrence iteration work per event. */
  maxIterations: number;
}

export interface CalendarParser {
  /** Parses one snapshot; malformed or unsupported input must throw. */
  parse(icsText: string, options: ParseOptions): Promise<ParsedCalendar>;
}
