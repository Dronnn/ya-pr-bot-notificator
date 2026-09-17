/**
 * In-process D1 substitute backed by Node's built-in `node:sqlite`. It
 * implements the same structural interface the production code consumes, so
 * tests exercise the real SQL, the real indexes and the real parameter binding
 * instead of mocks.
 */

import { DatabaseSync } from 'node:sqlite';
import { readdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import type {
  D1DatabaseLike,
  D1ResultLike,
  D1StatementLike,
} from '../../src/platform.ts';

type SqlValue = null | number | bigint | string | Uint8Array;

export interface QueryStats {
  statements: number;
  params: number;
  maxParamsPerStatement: number;
  reset(): void;
}

export interface SqliteD1 extends D1DatabaseLike {
  readonly database: DatabaseSync;
  readonly stats: QueryStats;
  exec(sql: string): void;
  close(): void;
}

class SqliteStatement implements D1StatementLike {
  readonly #owner: SqliteD1Impl;
  readonly #sql: string;
  readonly #params: readonly SqlValue[];

  constructor(owner: SqliteD1Impl, sql: string, params: readonly SqlValue[] = []) {
    this.#owner = owner;
    this.#sql = sql;
    this.#params = params;
  }

  get sql(): string {
    return this.#sql;
  }

  get params(): readonly SqlValue[] {
    return this.#params;
  }

  bind(...values: readonly unknown[]): D1StatementLike {
    return new SqliteStatement(this.#owner, this.#sql, values as readonly SqlValue[]);
  }

  async first<T = Record<string, unknown>>(): Promise<T | null> {
    this.#owner.record(this.#params.length);
    const row = this.#owner.database.prepare(this.#sql).get(...(this.#params as never[]));
    return (row ?? null) as T | null;
  }

  async all<T = Record<string, unknown>>(): Promise<D1ResultLike<T>> {
    this.#owner.record(this.#params.length);
    const result = this.#owner.database.prepare(this.#sql).all(...(this.#params as never[]));
    return { results: result as T[], success: true };
  }

  async run(): Promise<D1ResultLike<never>> {
    return this.execute();
  }

  /** Synchronous write used by `batch` inside one transaction. */
  execute(): D1ResultLike<never> {
    this.#owner.record(this.#params.length);
    const result = this.#owner.database.prepare(this.#sql).run(...(this.#params as never[]));
    return { success: true, meta: { changes: Number(result.changes) } };
  }
}

class SqliteD1Impl implements SqliteD1 {
  readonly database: DatabaseSync;
  readonly stats: QueryStats = {
    statements: 0,
    params: 0,
    maxParamsPerStatement: 0,
    reset(): void {
      this.statements = 0;
      this.params = 0;
      this.maxParamsPerStatement = 0;
    },
  };

  constructor() {
    this.database = new DatabaseSync(':memory:');
    this.database.exec('PRAGMA foreign_keys = ON');
  }

  record(paramCount: number): void {
    this.stats.statements += 1;
    this.stats.params += paramCount;
    if (paramCount > this.stats.maxParamsPerStatement) {
      this.stats.maxParamsPerStatement = paramCount;
    }
  }

  prepare(query: string): D1StatementLike {
    return new SqliteStatement(this, query);
  }

  async batch(statements: readonly D1StatementLike[]): Promise<readonly unknown[]> {
    const results: unknown[] = [];
    this.database.exec('BEGIN');
    try {
      for (const statement of statements) {
        if (!(statement instanceof SqliteStatement)) {
          throw new TypeError('batch only accepts statements from this database');
        }
        results.push(statement.execute());
      }
      this.database.exec('COMMIT');
    } catch (error) {
      this.database.exec('ROLLBACK');
      throw error;
    }
    return results;
  }

  exec(sql: string): void {
    this.database.exec(sql);
  }

  close(): void {
    this.database.close();
  }
}

/** A fresh in-memory D1 substitute; call `applyMigrations` before use. */
export function createSqliteD1(): SqliteD1 {
  return new SqliteD1Impl();
}

/** Directory holding the forward-only migration files. */
const MIGRATIONS_DIR = fileURLToPath(new URL('../../migrations/', import.meta.url).href);

/** Every migration file, in the filename order the production database applies. */
export function migrationFiles(): string[] {
  return readdirSync(MIGRATIONS_DIR)
    .filter((name) => name.endsWith('.sql'))
    .sort();
}

/**
 * Applies migration files in order; defaults to every `migrations/*.sql`.
 * Tests that need to observe a single migration's effect pass an explicit list.
 */
export function applyMigrations(
  db: SqliteD1,
  files: readonly string[] = migrationFiles(),
): void {
  for (const name of files) {
    db.exec(readFileSync(`${MIGRATIONS_DIR}${name}`, 'utf8'));
  }
}

/**
 * Row count for a `SELECT COUNT(*) AS n ...` query. Reads the raw database so
 * it never disturbs the statement-budget counters.
 */
export function countRows(db: SqliteD1, sql: string): number {
  return Number((db.database.prepare(sql).get() as { n: number }).n);
}
