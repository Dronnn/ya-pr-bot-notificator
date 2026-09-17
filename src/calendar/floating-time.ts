/**
 * Interprets a floating (timezone-less) wall clock in an explicit source zone.
 *
 * Only zones with a known fixed offset are supported; anything else is rejected
 * rather than silently treated as UTC. Europe/Moscow has been UTC+3 since 2014.
 */

import { CalendarIntegrityError } from '../domain/calendar.ts';
import { MOSCOW_OFFSET_MINUTES, MS_PER_MINUTE } from '../util.ts';

export interface WallClock {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
}

/**
 * Supported source zones mapped to their fixed UTC offset. The policy is the
 * data: a zone is supported exactly when it appears here, and every missing
 * zone is rejected through the same error path.
 */
const SUPPORTED_OFFSET_MINUTES: ReadonlyMap<string, number> = new Map([
  ['Europe/Moscow', MOSCOW_OFFSET_MINUTES],
]);

/**
 * Epoch ms of `wall` interpreted in `sourceTimeZone`. Throws
 * CalendarIntegrityError when the zone has no supported fixed offset or the
 * wall clock is outside the representable date range, so an unsupported source
 * is never silently treated as UTC.
 */
export function floatingWallClockToUtcMs(wall: WallClock, sourceTimeZone: string): number {
  const offsetMinutes = SUPPORTED_OFFSET_MINUTES.get(sourceTimeZone);
  if (offsetMinutes === undefined) {
    throw new CalendarIntegrityError(
      `floating time is not supported for source timezone ${sourceTimeZone}`,
    );
  }
  const wallClockMs = Date.UTC(
    wall.year,
    wall.month - 1,
    wall.day,
    wall.hour,
    wall.minute,
    wall.second,
  );
  if (!Number.isFinite(wallClockMs)) {
    throw new CalendarIntegrityError('floating wall clock is out of range');
  }
  return wallClockMs - offsetMinutes * MS_PER_MINUTE;
}
