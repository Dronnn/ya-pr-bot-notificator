/**
 * ical.js backed implementation of the parser port.
 *
 * This module is the only place that imports the runtime dependency. It owns
 * all recurrence expansion: ical.js RecurExpansion applies RRULE/RDATE/EXDATE
 * with correct timezone and DST semantics, and the adapter bounds the work with
 * a horizon and an iteration guard. The domain only normalizes the result.
 *
 * Semantic validation runs in two layers before any value enters the domain
 * model. The raw ICS text pass rejects date/time and duration forms that
 * ical.js silently repairs (lowercase `z`, fractional seconds, numeric UTC
 * offsets, `VALUE=DATE` with a time part, malformed basic syntax). The raw jCal
 * layer (`ICAL.parse` output) then rejects invalid calendar values that survive
 * as text (February 30, month 13, hour 25) and ambiguous structure. Any invalid
 * or ambiguous snapshot rejects as a whole; the sync pipeline then preserves
 * the previously published snapshot.
 */

import ICAL from 'ical.js';

import {
  AmbiguousCalendarError,
  CalendarIntegrityError,
  RecurrenceLimitError,
  UnsupportedRecurrenceError,
  type OccurrenceStatus,
  type ParsedCalendar,
  type ParsedEvent,
} from '../domain/calendar.ts';
import { floatingWallClockToUtcMs, type WallClock } from './floating-time.ts';
import type { CalendarParser, ParseOptions } from './parser.ts';

/*
 * Minimal structural views of the ical.js objects this adapter touches. Each
 * interface below pins exactly the surface the rules use instead of the
 * library's wider published types; the ICAL_NS cast is the only untyped
 * boundary.
 */
interface IcalTimeLike {
  isDate: boolean;
  zone: { tzid?: string } | null;
  toJSDate(): Date;
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
  clone(): IcalTimeLike;
  addDuration(duration: unknown): void;
}

interface IcalDurationLike {
  weeks: number;
  days: number;
  hours: number;
  minutes: number;
  seconds: number;
  isNegative: boolean;
}

interface IcalPropertyLike {
  getParameter(name: string): string | undefined;
}

interface IcalComponentLike {
  getFirstPropertyValue(name: string): unknown;
  getAllProperties(name: string): IcalPropertyLike[];
}

interface IcalIteratorLike {
  next(): IcalTimeLike | null | undefined;
}

interface IcalEventLike {
  uid: string;
  summary: string | null;
  description: string | null;
  startDate: IcalTimeLike | null;
  endDate: IcalTimeLike | null;
  component: IcalComponentLike;
  iterator(): IcalIteratorLike;
}

interface IcalRootLike {
  getAllSubcomponents(name: string): unknown[];
}

interface IcalNamespace {
  parse(input: string): unknown;
  Component: new (jcal: unknown) => IcalRootLike;
  Event: new (component: unknown) => IcalEventLike;
  Duration: {
    fromData(data: {
      weeks?: number;
      days?: number;
      hours?: number;
      minutes?: number;
      seconds?: number;
    }): IcalDurationLike;
  };
}

const ICAL_NS = ICAL as unknown as IcalNamespace;

/**
 * TZIDs ical.js resolves to UTC on its own; the only deliberate built-ins.
 * Mirrors `ICAL.TimezoneService.reset()`, which registers exactly `Z`, `UTC`
 * and `GMT`. The lookup is case-sensitive and whitespace-significant, so a
 * feed declaration only replaces a built-in alias when it matches the TZID
 * byte-for-byte; every other TZID must be defined by the feed itself.
 */
const BUILTIN_UTC_ALIASES: ReadonlySet<string> = new Set(['Z', 'UTC', 'GMT']);

export class IcalJsCalendarParser implements CalendarParser {
  async parse(icsText: string, options: ParseOptions): Promise<ParsedCalendar> {
    const jcal = parseJcal(icsText);
    const unfolded = unfoldIcs(icsText);
    assertSupportedRuleText(unfolded);
    assertValidRawPropertyText(unfolded);
    validateRawCalendar(jcal);
    const root = buildRoot(jcal);

    const events: ParsedEvent[] = [];
    for (const subcomponent of root.getAllSubcomponents('vevent')) {
      events.push(toParsedEvent(subcomponent, options));
    }
    return { events: deduplicateMasters(events) };
  }
}

// ----- parsing --------------------------------------------------------------

/** Structural parse; any ical.js failure becomes a CalendarIntegrityError. */
function parseJcal(icsText: string): unknown {
  try {
    return ICAL_NS.parse(icsText);
  } catch {
    throw new CalendarIntegrityError('failed to parse iCalendar payload');
  }
}

/** Component wrapper; constructed only after the raw layer has been validated. */
function buildRoot(jcal: unknown): IcalRootLike {
  try {
    return new ICAL_NS.Component(jcal);
  } catch {
    throw new CalendarIntegrityError('failed to parse iCalendar payload');
  }
}

// ----- raw jCal layer -------------------------------------------------------

/*
 * Raw jCal tree returned by `ICAL.parse`: a component is
 * `[name, properties, subcomponents]` and a property is
 * `[name, params, type, ...values]`. Validation reads this layer only; it never
 * materializes an `ICAL.Time`, so ical.js normalization cannot hide an invalid
 * calendar value.
 */
interface RawComponent {
  readonly name: string;
  readonly properties: readonly unknown[];
  readonly subcomponents: readonly RawComponent[];
}

interface RawProperty {
  readonly name: string;
  readonly type: string;
  readonly values: readonly unknown[];
  readonly params: Readonly<Record<string, string>>;
}

interface RawTimeParts {
  readonly year: number;
  readonly month: number;
  readonly day: number;
  readonly hour: number;
  readonly minute: number;
  readonly second: number;
}

/** Observance of a VTIMEZONE, reduced to the fields the policy needs. */
interface RawObservance {
  readonly dtstartMs: number | null;
  readonly offsetTo: string | null;
}

/** A VTIMEZONE declared by the feed, keyed by its TZID. */
interface RawTimezone {
  readonly tzid: string;
  readonly observances: readonly RawObservance[];
}

type RawTimezones = ReadonlyMap<string, RawTimezone>;

/** Time-valued properties whose raw values must be calendar-valid. */
const RAW_TIME_PROPERTIES: readonly string[] = [
  'dtstart',
  'dtend',
  'recurrence-id',
  'rdate',
  'exdate',
];

/** Time-valued properties that may appear at most once in a component. */
const SINGLETON_TIME_PROPERTIES: readonly string[] = [
  'dtstart',
  'dtend',
  'recurrence-id',
  'duration',
];

/** Extended jCal date and date-time shapes produced by ical.js. */
const RAW_DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;
const RAW_DATE_TIME_RE = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z?$/;

/*
 * RFC 5545 3.3.14 `utc-offset` in its jCal extended form (`+HH:MM[:SS]`).
 * The shape check alone is insufficient: ical.js normalizes `+9999` to
 * `+99:99` without complaint, so `assertValidUtcOffset` enforces the grammar
 * and the semantic ranges on top of this pattern.
 */
const RAW_UTC_OFFSET_RE = /^[+-]\d{2}:\d{2}(?::\d{2})?$/;

/*
 * RFC 5545 3.3.6 `dur-value`, checked on the raw jCal string. ical.js accepts
 * and silently repairs malformed values (`P1H` as one hour, `PT1D` as one day,
 * `+-PT1H` as positive, fractional seconds truncated), so the grammar is
 * enforced here instead of trusting the library's normalization.
 */
const RAW_DURATION_RE =
  /^[+-]?P(?:\d+W|\d+D(?:T(?:\d+H(?:\d+M(?:\d+S)?)?|\d+M(?:\d+S)?|\d+S))?|T(?:\d+H(?:\d+M(?:\d+S)?)?|\d+M(?:\d+S)?|\d+S))$/;

/** Single-valued date/time properties whose raw ICS text is validated. */
const RAW_TEXT_TIME_PROPERTIES: ReadonlySet<string> = new Set([
  'DTSTART',
  'DTEND',
  'DUE',
  'RECURRENCE-ID',
]);

/** Comma-separated date/time list properties whose raw ICS text is validated. */
const RAW_TEXT_TIME_LIST_PROPERTIES: ReadonlySet<string> = new Set(['EXDATE', 'RDATE']);

/*
 * RFC 5545 3.3.4/3.3.5 basic forms. These are deliberately stricter than the
 * ABNF case-insensitivity would suggest: ical.js repairs every deviation below
 * before jCal exists (lowercase `z` and numeric offsets become floating time,
 * fractional seconds are truncated, `VALUE=DATE` time parts are dropped), so
 * the raw text is the only layer that can still reject them.
 */
const BASIC_DATE_RE = /^\d{8}$/;
const BASIC_DATE_TIME_RE = /^\d{8}T\d{6}Z?$/;

function toRawComponent(value: unknown): RawComponent | null {
  if (!Array.isArray(value) || typeof value[0] !== 'string') {
    return null;
  }
  const properties = Array.isArray(value[1]) ? value[1] : [];
  const rawSubcomponents = Array.isArray(value[2]) ? value[2] : [];
  const subcomponents: RawComponent[] = [];
  for (const subcomponent of rawSubcomponents) {
    const converted = toRawComponent(subcomponent);
    if (converted !== null) {
      subcomponents.push(converted);
    }
  }
  return { name: value[0].toLowerCase(), properties, subcomponents };
}

function toRawProperty(value: unknown): RawProperty | null {
  if (!Array.isArray(value) || typeof value[0] !== 'string') {
    return null;
  }
  const type = typeof value[2] === 'string' ? value[2] : '';
  const params: Record<string, string> = {};
  const rawParams = value[1];
  if (rawParams !== null && typeof rawParams === 'object') {
    for (const [name, raw] of Object.entries(rawParams as Record<string, unknown>)) {
      const first = Array.isArray(raw) ? raw[0] : raw;
      if (typeof first === 'string') {
        params[name.toLowerCase()] = first;
      }
    }
  }
  return { name: value[0].toLowerCase(), type, values: value.slice(3), params };
}

function isLeapYear(year: number): boolean {
  return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
}

function daysInMonth(year: number, month: number): number {
  switch (month) {
    case 2:
      return isLeapYear(year) ? 29 : 28;
    case 4:
    case 6:
    case 9:
    case 11:
      return 30;
    default:
      return 31;
  }
}

/**
 * Validates one raw extended date/date-time string. Strict by design: month
 * 1-12, day valid for the month and Gregorian leap year, hour 0-23, minute and
 * second 0-59. Leap seconds (second 60) are rejected rather than normalized.
 */
function parseRawTime(type: string, value: string): RawTimeParts | null {
  let match: RegExpExecArray | null;
  if (type === 'date') {
    match = RAW_DATE_RE.exec(value);
  } else if (type === 'date-time') {
    match = RAW_DATE_TIME_RE.exec(value);
  } else {
    return null;
  }
  if (match === null) {
    return null;
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = match[4] === undefined ? 0 : Number(match[4]);
  const minute = match[5] === undefined ? 0 : Number(match[5]);
  const second = match[6] === undefined ? 0 : Number(match[6]);
  if (month < 1 || month > 12) {
    return null;
  }
  if (day < 1 || day > daysInMonth(year, month)) {
    return null;
  }
  if (hour > 23 || minute > 59 || second > 59) {
    return null;
  }
  return { year, month, day, hour, minute, second };
}

/**
 * Validates every raw time value in the calendar against the declared
 * VTIMEZONEs. Malformed raw payloads reject the whole snapshot here, before
 * `ICAL.Event`/`ICAL.Time` can normalize them into a plausible-but-wrong date.
 * Timezones are collected first so a reference is checked the same way
 * regardless of whether the VTIMEZONE precedes or follows its VEVENT.
 */
function validateRawCalendar(jcal: unknown): void {
  const root = toRawComponent(jcal);
  if (root === null) {
    throw new CalendarIntegrityError('failed to parse iCalendar payload');
  }
  // D2: only TZIDs referenced by consumed event properties can reject the
  // snapshot. Declared-but-unreferenced zones are skipped entirely, so a
  // broken unused VTIMEZONE never fails a valid UTC event. A declared TZID is
  // kept even when its name is a UTC built-in alias: ical.js prefers the
  // in-feed declaration over `TimezoneService`, so `TZID:GMT` with an offset
  // is a custom zone, not built-in UTC.
  const referenced = collectReferencedTzids(root);
  const timezones = new Map<string, RawTimezone>();
  for (const subcomponent of root.subcomponents) {
    if (subcomponent.name !== 'vtimezone') {
      continue;
    }
    const tzidProperty = firstRawProperty(subcomponent, 'tzid');
    const tzid =
      tzidProperty !== null && typeof tzidProperty.values[0] === 'string'
        ? tzidProperty.values[0]
        : null;
    if (tzid === null || tzid.length === 0 || !referenced.has(tzid)) {
      continue;
    }
    const timezone = toRawTimezone(subcomponent);
    // First declaration wins, mirroring `Component.getTimeZoneByID`, which
    // returns the first matching VTIMEZONE. Every duplicate is still
    // syntax-validated above, so a broken one rejects regardless of order.
    if (timezone !== null && !timezones.has(timezone.tzid)) {
      timezones.set(timezone.tzid, timezone);
    }
  }
  for (const subcomponent of root.subcomponents) {
    if (subcomponent.name === 'vevent') {
      validateRawEvent(subcomponent, timezones);
    }
  }
}

/**
 * TZIDs that can force a rejection: TZID params on the consumed time-valued
 * event properties. RRULE/UNTIL carry no separate TZID param in RFC 5545
 * (UNTIL inherits the DTSTART frame), but any TZID param present on them is
 * included so an UNTIL-scoped reference still counts as a reference. Built-in
 * UTC aliases are collected like any other name so a matching declaration can
 * be validated; the alias/built-in decision is made against the declared set.
 */
const REFERENCED_TZID_PROPERTIES: ReadonlySet<string> = new Set([
  'dtstart',
  'dtend',
  'due',
  'recurrence-id',
  'exdate',
  'rdate',
  'rrule',
  'until',
]);

function collectReferencedTzids(root: RawComponent): Set<string> {
  const referenced = new Set<string>();
  for (const subcomponent of root.subcomponents) {
    if (subcomponent.name !== 'vevent') {
      continue;
    }
    for (const property of subcomponent.properties) {
      const converted = toRawProperty(property);
      if (converted === null || !REFERENCED_TZID_PROPERTIES.has(converted.name)) {
        continue;
      }
      const tzid = converted.params['tzid'];
      if (tzid !== undefined && tzid.length > 0) {
        referenced.add(tzid);
      }
    }
  }
  return referenced;
}

function firstRawProperty(component: RawComponent, name: string): RawProperty | null {
  for (const property of component.properties) {
    const converted = toRawProperty(property);
    if (converted?.name === name) {
      return converted;
    }
  }
  return null;
}

/**
 * RFC 5545 3.3.14 UTC-offset grammar and semantic ranges on the jCal extended
 * form. Hour is 00-23, minute and second (when present) are 00-59, and a
 * negative zero (`-00:00`, `-00:00:00`) is prohibited. Real non-hour offsets
 * (`+05:30`, `+05:45`, `+10:30`) and valid negative offsets pass unchanged.
 */
function assertValidUtcOffset(value: string, property: 'TZOFFSETFROM' | 'TZOFFSETTO'): void {
  const match = /^([+-])(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(value);
  if (match === null || !RAW_UTC_OFFSET_RE.test(value)) {
    throw new CalendarIntegrityError(`VTIMEZONE observance has an invalid ${property}`);
  }
  const sign = match[1];
  const hour = Number(match[2]);
  const minute = Number(match[3]);
  const second = match[4] === undefined ? 0 : Number(match[4]);
  if (hour > 23 || minute > 59 || second > 59) {
    throw new CalendarIntegrityError(`VTIMEZONE observance has an invalid ${property}`);
  }
  if (sign === '-' && hour === 0 && minute === 0 && second === 0) {
    throw new CalendarIntegrityError(`VTIMEZONE observance has an invalid ${property}`);
  }
}

function collectRawProperties(component: RawComponent, name: string): RawProperty[] {
  const matches: RawProperty[] = [];
  for (const property of component.properties) {
    const converted = toRawProperty(property);
    if (converted?.name === name) {
      matches.push(converted);
    }
  }
  return matches;
}

function toRawObservance(component: RawComponent): RawObservance {
  // D1: every instance is validated, and more than one instance of a
  // singleton observance property rejects. First-wins would silently accept
  // e.g. TZOFFSETTO:+0300 followed by TZOFFSETTO:+9999.
  const dtstarts = collectRawProperties(component, 'dtstart');
  for (const candidate of dtstarts) {
    if (
      candidate.values.length !== 1 ||
      typeof candidate.values[0] !== 'string' ||
      parseRawTime(candidate.type, candidate.values[0]) === null
    ) {
      throw new CalendarIntegrityError('VTIMEZONE observance has an invalid DTSTART');
    }
  }
  if (dtstarts.length !== 1) {
    throw new CalendarIntegrityError('VTIMEZONE observance has an invalid DTSTART');
  }
  const dtstart = dtstarts[0] as RawProperty;
  let dtstartMs: number | null = null;
  if (dtstart !== null && dtstart.values.length === 1 && typeof dtstart.values[0] === 'string') {
    const parts = parseRawTime(dtstart.type, dtstart.values[0]);
    if (parts === null) {
      throw new CalendarIntegrityError('VTIMEZONE observance has an invalid DTSTART');
    }
    // A VTIMEZONE DTSTART is a local wall clock. It shares its frame with the
    // referenced time values in the same TZID, so ordering both as naive UTC
    // is sound for the "observance applies at or before the reference" check.
    dtstartMs = Date.UTC(
      parts.year,
      parts.month - 1,
      parts.day,
      parts.hour,
      parts.minute,
      parts.second,
    );
    if (!Number.isFinite(dtstartMs)) {
      dtstartMs = null;
    }
  } else {
    throw new CalendarIntegrityError('VTIMEZONE observance has an invalid DTSTART');
  }

  const offsetFroms = collectRawProperties(component, 'tzoffsetfrom');
  for (const candidate of offsetFroms) {
    const value = candidate.values.length === 1 ? candidate.values[0] : null;
    if (typeof value !== 'string') {
      throw new CalendarIntegrityError('VTIMEZONE observance is missing TZOFFSETFROM');
    }
    assertValidUtcOffset(value, 'TZOFFSETFROM');
  }
  if (offsetFroms.length !== 1) {
    throw new CalendarIntegrityError('VTIMEZONE observance is missing TZOFFSETFROM');
  }
  const offsetFrom = offsetFroms[0] as RawProperty;
  const offsetFromValue =
    offsetFrom !== null &&
    offsetFrom.values.length === 1 &&
    typeof offsetFrom.values[0] === 'string'
      ? offsetFrom.values[0]
      : null;
  if (offsetFromValue === null) {
    throw new CalendarIntegrityError('VTIMEZONE observance is missing TZOFFSETFROM');
  }
  assertValidUtcOffset(offsetFromValue, 'TZOFFSETFROM');

  const offsetTos = collectRawProperties(component, 'tzoffsetto');
  for (const candidate of offsetTos) {
    const value = candidate.values.length === 1 ? candidate.values[0] : null;
    if (typeof value !== 'string') {
      throw new CalendarIntegrityError('VTIMEZONE observance has an invalid TZOFFSETTO');
    }
    assertValidUtcOffset(value, 'TZOFFSETTO');
  }
  if (offsetTos.length !== 1) {
    throw new CalendarIntegrityError('VTIMEZONE observance has an invalid TZOFFSETTO');
  }
  const offsetTo = offsetTos[0] as RawProperty;
  const offsetValue =
    offsetTo !== null && offsetTo.values.length === 1 && typeof offsetTo.values[0] === 'string'
      ? offsetTo.values[0]
      : null;
  if (offsetValue === null) {
    throw new CalendarIntegrityError('VTIMEZONE observance has an invalid TZOFFSETTO');
  }
  assertValidUtcOffset(offsetValue, 'TZOFFSETTO');
  return { dtstartMs, offsetTo: offsetValue };
}

/** Collects one VTIMEZONE; observances are syntax-validated eagerly. */
function toRawTimezone(component: RawComponent): RawTimezone | null {
  const tzidProperty = firstRawProperty(component, 'tzid');
  const tzid =
    tzidProperty !== null && typeof tzidProperty.values[0] === 'string'
      ? tzidProperty.values[0]
      : null;
  if (tzid === null || tzid.length === 0) {
    return null;
  }
  const observances: RawObservance[] = [];
  for (const subcomponent of component.subcomponents) {
    if (subcomponent.name === 'standard' || subcomponent.name === 'daylight') {
      observances.push(toRawObservance(subcomponent));
    }
  }
  return { tzid, observances };
}

/**
 * A custom timezone is usable for a referenced wall clock only when at least
 * one observance has a parseable DTSTART and TZOFFSETTO at or before that wall
 * clock. ical.js resolves any earlier time at offset zero instead of rejecting
 * it, so a declared-but-empty, future-only, or first-onset-inside-the-interval
 * zone must fail here rather than produce a mistimed occurrence. The two wall
 * clocks share the same local-time frame inside one TZID, so ordering them as
 * naive UTC matches the offset lookup ical.js performs.
 */
function hasObservanceAt(timezone: RawTimezone, wallClockMs: number): boolean {
  return timezone.observances.some(
    (observance) => observance.dtstartMs !== null && observance.dtstartMs <= wallClockMs,
  );
}

/** Raw time values of one property, with single-value and syntax rules applied. */
function validateRawTimeProperty(property: RawProperty): RawTimeParts[] {
  if (property.values.length === 0) {
    throw new CalendarIntegrityError(`${property.name} has no value`);
  }
  const multiValued = property.name === 'rdate' || property.name === 'exdate';
  if (!multiValued && property.values.length > 1) {
    throw new CalendarIntegrityError(`${property.name} has multiple values`);
  }
  const parts: RawTimeParts[] = [];
  for (const value of property.values) {
    const parsed = typeof value === 'string' ? parseRawTime(property.type, value) : null;
    if (parsed === null) {
      throw new CalendarIntegrityError(
        `${property.name} has an invalid ${property.type || 'time'} value`,
      );
    }
    parts.push(parsed);
  }
  return parts;
}

/**
 * Enforces RFC 5545 3.3.6 on the raw DURATION string. ical.js repairs malformed
 * grammar (see `RAW_DURATION_RE`), so a value that does not match must be a
 * whole-snapshot rejection; the semantic negative check stays in `readDuration`
 * because `-PT1H` is syntactically valid.
 */
function validateRawDurationProperty(property: RawProperty): void {
  const value = property.values.length === 1 ? property.values[0] : null;
  if (typeof value !== 'string' || !RAW_DURATION_RE.test(value)) {
    throw new CalendarIntegrityError('component has an invalid DURATION');
  }
}

/**
 * Every explicit TZID on a time-valued property must resolve to a declared
 * feed timezone with an observance applicable at or before that wall clock, or
 * to a built-in UTC alias when the feed declares no matching VTIMEZONE.
 * ical.js prefers an in-feed declaration over `TimezoneService` and its lookup
 * is an exact string match, so `TZID=GMT` with a declared `TZID:GMT` zone is
 * validated as a custom zone while a declared-but-not-matching name (case
 * difference, whitespace) leaves the built-in alias in effect. Without the
 * declaration check ical.js silently downgrades an unrecognized TZID to
 * floating time and resolves pre-onset times at offset zero, so an unchecked
 * reference would reinterpret the value as UTC or source-local time.
 */
function validateRawTimezoneReference(
  property: RawProperty,
  times: readonly RawTimeParts[],
  defined: RawTimezones,
): void {
  const tzid = property.params['tzid'];
  if (tzid === undefined || tzid.length === 0) {
    return;
  }
  const timezone = defined.get(tzid);
  if (timezone === undefined) {
    if (BUILTIN_UTC_ALIASES.has(tzid)) {
      return;
    }
    throw new CalendarIntegrityError(`${property.name} references unknown timezone ${tzid}`);
  }
  for (const parts of times) {
    const wallClockMs = Date.UTC(
      parts.year,
      parts.month - 1,
      parts.day,
      parts.hour,
      parts.minute,
      parts.second,
    );
    if (!hasObservanceAt(timezone, wallClockMs)) {
      throw new CalendarIntegrityError(
        `${property.name} references timezone ${tzid} without a usable observance`,
      );
    }
  }
}

/**
 * Rejects incompatible TZID combinations before normalized values enter the
 * domain model. A TZID on a DATE value and a TZID combined with a trailing-Z
 * UTC value are both silently reinterpreted by ical.js (TZID dropped, value
 * read as floating or UTC), so they must fail here on the raw jCal layer.
 */
function validateRawTzidCombination(property: RawProperty): void {
  const tzid = property.params['tzid'];
  if (tzid === undefined || tzid.length === 0) {
    return;
  }
  if (property.type === 'date') {
    throw new CalendarIntegrityError(`${property.name} must not carry TZID on a DATE value`);
  }
  if (property.type === 'date-time') {
    for (const value of property.values) {
      if (typeof value === 'string' && value.endsWith('Z')) {
        throw new CalendarIntegrityError(
          `${property.name} must not combine TZID with a UTC value`,
        );
      }
    }
  }
}

/**
 * Every unzoned DATE-TIME value of one VEVENT must agree on its UTC marking:
 * either every value without a TZID carries a trailing Z or none does.
 * ical.js resolves a floating wall clock through the source zone and a Z value
 * as UTC, so a VEVENT mixing the two frames would normalize its start and end
 * in different frames. Zoned values (TZID present) and DATE values (all-day)
 * are excluded: zoned EXDATE/RECURRENCE-ID alongside a UTC DTSTART remain
 * accepted by explicit policy, as do DATE RDATE lists.
 */
function validateRawEvent(component: RawComponent, definedTimezones: RawTimezones): void {
  const counts = new Map<string, number>();
  let sawUtc = false;
  let sawNonUtc = false;
  for (const property of component.properties) {
    const converted = toRawProperty(property);
    if (converted === null) {
      continue;
    }
    counts.set(converted.name, (counts.get(converted.name) ?? 0) + 1);
    if (RAW_TIME_PROPERTIES.includes(converted.name)) {
      const times = validateRawTimeProperty(converted);
      validateRawTzidCombination(converted);
      const tzid = converted.params['tzid'];
      if (converted.type === 'date-time' && (tzid === undefined || tzid.length === 0)) {
        for (const value of converted.values) {
          if (typeof value !== 'string') {
            continue;
          }
          if (value.endsWith('Z')) {
            sawUtc = true;
          } else {
            sawNonUtc = true;
          }
        }
      }
      validateRawTimezoneReference(converted, times, definedTimezones);
    } else if (converted.name === 'duration') {
      validateRawDurationProperty(converted);
    }
  }
  if (sawUtc && sawNonUtc) {
    throw new CalendarIntegrityError('component mixes UTC and floating date-time values');
  }
  for (const name of SINGLETON_TIME_PROPERTIES) {
    if ((counts.get(name) ?? 0) > 1) {
      throw new CalendarIntegrityError(`component defines ${name} more than once`);
    }
  }
  if ((counts.get('dtend') ?? 0) > 0 && (counts.get('duration') ?? 0) > 0) {
    throw new CalendarIntegrityError('component defines both DTEND and DURATION');
  }
}

// ----- per-event pipeline ---------------------------------------------------

/**
 * Validates one VEVENT, resolves its times and expands a master's occurrences.
 * Unsupported input rejects the whole snapshot; a missing DTSTART or UID is an
 * integrity error rather than a silently skipped event. TZID references were
 * already checked against the raw wall clocks before this point.
 */
function toParsedEvent(subcomponent: unknown, options: ParseOptions): ParsedEvent {
  const event = new ICAL_NS.Event(subcomponent);
  const component = event.component;
  const uid = typeof event.uid === 'string' ? event.uid : '';
  const displayUid = uid.length > 0 ? uid : '(unknown)';
  assertSupportedRecurrence(component, displayUid);

  const startDate = event.startDate;
  if (startDate === null || typeof startDate.isDate !== 'boolean') {
    throw new CalendarIntegrityError('event is missing a usable DTSTART');
  }
  const isAllDay = startDate.isDate === true;
  const startsAtMs = resolveTimeMs(startDate, isAllDay, options);
  if (startsAtMs === null) {
    throw new CalendarIntegrityError('event is missing a usable DTSTART');
  }
  if (uid.length === 0) {
    throw new CalendarIntegrityError('event is missing a UID');
  }

  const duration = readDuration(component, displayUid);
  const endMs = event.endDate === null ? null : resolveTimeMs(event.endDate, false, options);
  if (endMs !== null && endMs < startsAtMs) {
    throw new CalendarIntegrityError(`event ${displayUid} ends before it starts`);
  }
  const recurrenceIdMs = readRecurrenceId(component, options);
  const isMaster = recurrenceIdMs === null;
  const expansion =
    isMaster && !isAllDay ? expandOccurrences(event, isAllDay, uid, options, duration) : null;

  return {
    uid,
    summary: event.summary ?? '(no title)',
    description: asString(event.description),
    url: asString(component.getFirstPropertyValue('url')),
    startsAtMs,
    endsAtMs: endMs,
    status: asStatus(component),
    isAllDay,
    recurrenceIdMs,
    expandedStartsMs: expansion === null ? null : expansion.startsMs,
    expandedEndsMs: expansion === null ? null : expansion.endsMs,
    timezone: resolveZone(startDate, isAllDay, options),
  };
}

/**
 * Multiple non-override masters with one UID are ambiguous. Exactly one copy is
 * kept only when the copies are semantically identical over every property the
 * domain consumes (see `mastersEquivalent`); the first in input order wins,
 * deterministically. Any other difference rejects the whole snapshot - silent
 * first-wins/last-wins is never allowed.
 */
function deduplicateMasters(events: readonly ParsedEvent[]): ParsedEvent[] {
  const kept = new Map<string, ParsedEvent>();
  const result: ParsedEvent[] = [];
  for (const event of events) {
    if (event.recurrenceIdMs !== null) {
      result.push(event);
      continue;
    }
    const existing = kept.get(event.uid);
    if (existing === undefined) {
      kept.set(event.uid, event);
      result.push(event);
      continue;
    }
    if (!mastersEquivalent(existing, event)) {
      throw new AmbiguousCalendarError(`event ${event.uid} has multiple non-identical masters`);
    }
  }
  return result;
}

/** Semantic equality over every property the domain model consumes. */
function mastersEquivalent(a: ParsedEvent, b: ParsedEvent): boolean {
  return (
    a.summary === b.summary &&
    a.description === b.description &&
    a.url === b.url &&
    a.startsAtMs === b.startsAtMs &&
    a.endsAtMs === b.endsAtMs &&
    a.status === b.status &&
    a.isAllDay === b.isAllDay &&
    a.timezone === b.timezone &&
    sameNumbers(a.expandedStartsMs, b.expandedStartsMs) &&
    sameNullableNumbers(a.expandedEndsMs ?? null, b.expandedEndsMs ?? null)
  );
}

function sameNumbers(a: readonly number[] | null, b: readonly number[] | null): boolean {
  if (a === null || b === null) {
    return a === b;
  }
  return a.length === b.length && a.every((value, index) => value === b[index]);
}

function sameNullableNumbers(
  a: readonly (number | null)[] | null,
  b: readonly (number | null)[] | null,
): boolean {
  if (a === null || b === null) {
    return a === b;
  }
  return a.length === b.length && a.every((value, index) => value === b[index]);
}

// ----- rejection rules ------------------------------------------------------

/**
 * Recurrence inputs that change the meaning of a feed but that ical.js does not
 * surface after parsing. They are rejected so the snapshot is never silently
 * reinterpreted.
 */
function assertSupportedRecurrence(component: IcalComponentLike, uid: string): void {
  for (const property of component.getAllProperties('recurrence-id')) {
    const range = property.getParameter('range');
    if (range !== undefined && range.length > 0 && range.toUpperCase() !== 'THIS') {
      throw new UnsupportedRecurrenceError(
        `event ${uid} uses RECURRENCE-ID RANGE=${range}; this-and-future changes are unsupported`,
      );
    }
  }
  if (component.getAllProperties('exrule').length > 0) {
    throw new UnsupportedRecurrenceError(`event ${uid} uses an unsupported EXRULE`);
  }
}

/**
 * ical.js drops RSCALE/SKIP while parsing an RRULE, so they are detected in the
 * raw (unfolded) rule lines first. Ignoring them would reinterpret a
 * non-Gregorian recurrence as a Gregorian one.
 */
function assertSupportedRuleText(unfolded: string): void {
  for (const line of unfolded.split(/\r?\n/)) {
    const upper = line.toUpperCase();
    if (upper.startsWith('RRULE:') && (upper.includes('RSCALE=') || upper.includes('SKIP='))) {
      throw new UnsupportedRecurrenceError('RRULE RSCALE/SKIP is unsupported');
    }
  }
}

/** RFC 5545 folding: a CRLF followed by one space or tab continues the line. */
function unfoldIcs(icsText: string): string {
  return icsText.replace(/\r?\n[ \t]/g, '');
}

/** One unfolded content line split into its name, parameters and raw value. */
interface RawContentLine {
  readonly name: string;
  readonly params: ReadonlyMap<string, string>;
  readonly value: string;
}

/** Splits `text` on `separator`, ignoring separators inside quoted parameters. */
function splitUnquoted(text: string, separator: string): string[] {
  const parts: string[] = [];
  let inQuotes = false;
  let start = 0;
  for (let index = 0; index < text.length; index += 1) {
    const character = text[index];
    if (character === '"') {
      inQuotes = !inQuotes;
    } else if (character === separator && !inQuotes) {
      parts.push(text.slice(start, index));
      start = index + 1;
    }
  }
  parts.push(text.slice(start));
  return parts;
}

/**
 * Reads one unfolded content line. The value separator is the first unquoted
 * colon, so quoted parameter values may contain `:` or `;`. Lines without a
 * separator are ignored here; ical.js rejects them via the ordinary integrity
 * error instead.
 */
function parseRawContentLine(line: string): RawContentLine | null {
  let inQuotes = false;
  let separator = -1;
  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];
    if (character === '"') {
      inQuotes = !inQuotes;
    } else if (character === ':' && !inQuotes) {
      separator = index;
      break;
    }
  }
  if (separator <= 0) {
    return null;
  }
  const segments = splitUnquoted(line.slice(0, separator), ';');
  const name = (segments[0] ?? '').trim().toUpperCase();
  if (name.length === 0) {
    return null;
  }
  const params = new Map<string, string>();
  for (const segment of segments.slice(1)) {
    const equals = segment.indexOf('=');
    if (equals <= 0) {
      continue;
    }
    let paramValue = segment.slice(equals + 1).trim();
    if (paramValue.length >= 2 && paramValue.startsWith('"') && paramValue.endsWith('"')) {
      paramValue = paramValue.slice(1, -1);
    }
    params.set(segment.slice(0, equals).trim().toUpperCase(), paramValue);
  }
  return { name, params, value: line.slice(separator + 1) };
}

/**
 * Validates the raw ICS text of every date/time and duration property the
 * parser consumes. jCal cannot do this: ical.js has already repaired invalid
 * forms by the time the value reaches it (see `BASIC_DATE_TIME_RE`). `UNTIL`
 * is a rule part rather than a property, so RRULE values carry the same check.
 */
function assertValidRawPropertyText(unfolded: string): void {
  for (const line of unfolded.split(/\r?\n/)) {
    if (line.length === 0) {
      continue;
    }
    const property = parseRawContentLine(line);
    if (property === null) {
      continue;
    }
    if (RAW_TEXT_TIME_PROPERTIES.has(property.name)) {
      validateRawTextTimeValue(property, false);
    } else if (RAW_TEXT_TIME_LIST_PROPERTIES.has(property.name)) {
      validateRawTextTimeValue(property, true);
    } else if (property.name === 'DURATION') {
      if (!RAW_DURATION_RE.test(property.value)) {
        throw new CalendarIntegrityError(`${property.name} has an invalid DURATION`);
      }
    } else if (property.name === 'RRULE') {
      validateRawRuleUntil(property.value);
    }
  }
}

function validateRawTextTimeValue(property: RawContentLine, list: boolean): void {
  const valueType = (property.params.get('VALUE') ?? 'DATE-TIME').toUpperCase();
  if (valueType === 'PERIOD') {
    // Periods are RFC-valid but unsupported by this adapter; the jCal layer
    // rejects them as invalid time values, so no text check is added here.
    return;
  }
  const pattern =
    valueType === 'DATE' ? BASIC_DATE_RE : valueType === 'DATE-TIME' ? BASIC_DATE_TIME_RE : null;
  if (pattern === null) {
    throw new CalendarIntegrityError(`${property.name} has an unsupported VALUE type`);
  }
  const tokens = list ? property.value.split(',') : [property.value];
  for (const token of tokens) {
    if (!pattern.test(token)) {
      throw new CalendarIntegrityError(
        `${property.name.toLowerCase()} has an invalid ${valueType.toLowerCase()} value`,
      );
    }
  }
}

/** `UNTIL` is a date or date-time in the same basic syntax as the properties. */
function validateRawRuleUntil(ruleValue: string): void {
  for (const part of ruleValue.split(';')) {
    const equals = part.indexOf('=');
    if (equals <= 0 || part.slice(0, equals).trim().toUpperCase() !== 'UNTIL') {
      continue;
    }
    const value = part.slice(equals + 1).trim();
    if (!BASIC_DATE_RE.test(value) && !BASIC_DATE_TIME_RE.test(value)) {
      throw new CalendarIntegrityError('RRULE UNTIL has an invalid value');
    }
  }
}

// ----- time resolution ------------------------------------------------------

/** Epoch ms of an ical time value, or null when it carries no usable date. */
function timeValueToMs(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  const candidate = value as { toJSDate?: () => Date };
  if (typeof candidate.toJSDate !== 'function') {
    return null;
  }
  const ms = candidate.toJSDate().getTime();
  return Number.isFinite(ms) ? ms : null;
}

function toWallClock(time: IcalTimeLike): WallClock {
  return {
    year: time.year,
    month: time.month,
    day: time.day,
    hour: time.hour,
    minute: time.minute,
    second: time.second,
  };
}

/**
 * True for timezone-less wall clocks. All-day dates also carry no zone, so
 * callers handle them explicitly and never shift a VALUE=DATE.
 */
function isFloating(time: IcalTimeLike): boolean {
  const tzid = time.zone?.tzid;
  return tzid === undefined || tzid === 'floating' || tzid === 'local' || tzid === '';
}

/**
 * Resolves an ical time to epoch ms. A floating wall clock is interpreted in
 * the explicit source zone; an all-day VALUE=DATE is never shifted and keeps
 * ical.js's own date math; a zoned value resolves through its TZID.
 */
function resolveTimeMs(time: IcalTimeLike, isAllDay: boolean, options: ParseOptions): number | null {
  if (!isAllDay && isFloating(time)) {
    return floatingWallClockToUtcMs(toWallClock(time), options.sourceTimeZone);
  }
  return timeValueToMs(time);
}

/** Zone recorded for the event: null for all-day, source zone for floating. */
function resolveZone(startDate: IcalTimeLike, isAllDay: boolean, options: ParseOptions): string | null {
  if (isAllDay) {
    return null;
  }
  if (isFloating(startDate)) {
    return options.sourceTimeZone;
  }
  return startDate.zone?.tzid ?? null;
}

/** Reads RECURRENCE-ID in ms; null means the component is a recurrence master. */
function readRecurrenceId(
  component: IcalComponentLike,
  options: ParseOptions,
): number | null {
  const value = component.getFirstPropertyValue('recurrence-id') as IcalTimeLike | null;
  if (value === null || value === undefined || typeof value.toJSDate !== 'function') {
    return null;
  }
  return resolveTimeMs(value, false, options);
}

/**
 * Reads a valid positive DURATION, or null when the component declares none.
 * Nominal weeks/days and exact hours/minutes/seconds are kept as separate
 * component fields so recurrence ends can follow RFC 5545 3.3.6 semantics. A
 * malformed, negative or zero value rejects the snapshot instead of reaching
 * the domain (ical.js throws a plain `Error` for an unparseable value, which
 * would otherwise be logged under a meaningless error name; a zero duration
 * such as `P0D` would otherwise publish a zero-length event).
 */
function readDuration(component: IcalComponentLike, uid: string): IcalDurationLike | null {
  let value: unknown;
  try {
    value = component.getFirstPropertyValue('duration');
  } catch {
    throw new CalendarIntegrityError(`event ${uid} has an invalid DURATION`);
  }
  if (value === null || value === undefined) {
    return null;
  }
  const duration = value as Partial<IcalDurationLike>;
  const parts = [duration.weeks, duration.days, duration.hours, duration.minutes, duration.seconds];
  if (parts.some((part) => typeof part !== 'number' || !Number.isInteger(part))) {
    throw new CalendarIntegrityError(`event ${uid} has an invalid DURATION`);
  }
  if (duration.isNegative === true || parts.some((part) => (part ?? 0) < 0)) {
    throw new CalendarIntegrityError(`event ${uid} has a negative DURATION`);
  }
  if (parts.every((part) => (part ?? 0) === 0)) {
    throw new CalendarIntegrityError(`event ${uid} has a zero DURATION`);
  }
  return duration as IcalDurationLike;
}

// ----- field mapping --------------------------------------------------------

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

/** Maps ICS STATUS to the occurrence status; only CANCELLED is differentiated. */
function asStatus(component: IcalComponentLike): OccurrenceStatus {
  const status = component.getFirstPropertyValue('status');
  return typeof status === 'string' && status.toUpperCase() === 'CANCELLED'
    ? 'cancelled'
    : 'confirmed';
}

// ----- recurrence expansion -------------------------------------------------

/** ical.js throws on a rule it cannot expand; the whole snapshot is rejected. */
function unsupportedRecurrence(uid: string): UnsupportedRecurrenceError {
  return new UnsupportedRecurrenceError(`event ${uid} has an unsupported recurrence rule`);
}

/** Expanded master instances: starts and, for DURATION masters, aligned ends. */
interface ExpandedInstances {
  readonly startsMs: number[];
  readonly endsMs: (number | null)[] | null;
}

/** Exact (elapsed) hours/minutes/seconds of a DURATION in milliseconds. */
function exactDurationMs(duration: IcalDurationLike): number {
  return ((duration.hours * 60 + duration.minutes) * 60 + duration.seconds) * 1000;
}

/**
 * End of one occurrence under RFC 5545 3.3.6: nominal weeks/days advance the
 * occurrence's wall clock in its own timezone (so a calendar day across a DST
 * transition is 23 or 25 elapsed hours), while exact hours/minutes/seconds stay
 * elapsed milliseconds. One flat millisecond delta from the master is never
 * reused across an offset transition.
 */
function occurrenceEndMs(
  time: IcalTimeLike,
  duration: IcalDurationLike,
  options: ParseOptions,
): number | null {
  const nominalEnd = time.clone();
  nominalEnd.addDuration(ICAL_NS.Duration.fromData({ weeks: duration.weeks, days: duration.days }));
  const nominalEndMs = resolveTimeMs(nominalEnd, false, options);
  if (nominalEndMs === null) {
    return null;
  }
  return nominalEndMs + exactDurationMs(duration);
}

/**
 * Expands a master event into occurrence start times inside the horizon using
 * ical.js recurrence facilities. The iterator is chronological, so iteration
 * stops as soon as it passes the horizon end. Every yielded step counts toward
 * the iteration guard; exceeding it rejects the whole snapshot instead of
 * persisting a truncated expansion. When the master declares DURATION, every
 * occurrence carries its own end derived per instance; DTEND-derived masters
 * keep the exact master delta (`endsMs: null`).
 */
function expandOccurrences(
  event: IcalEventLike,
  isAllDay: boolean,
  uid: string,
  options: ParseOptions,
  duration: IcalDurationLike | null,
): ExpandedInstances {
  // ical.js may yield an RDATE that coincides with an RRULE instance; keyed by
  // start, the first end wins and the insertion order is deterministic.
  const instances = new Map<number, number | null>();
  let iterations = 0;
  let iterator: IcalIteratorLike;
  try {
    iterator = event.iterator();
  } catch {
    throw unsupportedRecurrence(uid);
  }
  for (;;) {
    let next: IcalTimeLike | null | undefined;
    try {
      next = iterator.next();
    } catch {
      throw unsupportedRecurrence(uid);
    }
    if (next === null || next === undefined) {
      break;
    }
    iterations += 1;
    if (iterations > options.maxIterations) {
      throw new RecurrenceLimitError('recurrence expansion exceeded the iteration budget');
    }
    const ms = resolveTimeMs(next, isAllDay, options);
    if (ms === null) {
      continue;
    }
    if (ms > options.horizonEndMs) {
      break;
    }
    if (ms >= options.horizonStartMs) {
      instances.set(ms, duration === null ? null : occurrenceEndMs(next, duration, options));
    }
  }
  const ordered = [...instances.entries()].sort(([a], [b]) => a - b);
  return {
    startsMs: ordered.map(([startMs]) => startMs),
    endsMs: duration === null ? null : ordered.map(([, endMs]) => endMs),
  };
}
