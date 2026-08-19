import { EventType } from "@ag-ui/core";
import { createHash } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";

export const RUN_EPISODE_SCHEMA_VERSION = 1 as const;

export type RunEpisodeTerminalStatus = "completed" | "failed" | "canceled";

export type RunEpisodeRecord = {
  readonly id: string;
  readonly user_id: string;
  readonly workspace_id: string;
  readonly run_id: string;
  readonly session_id: string;
  readonly datasource_id?: string;
  readonly schema_version: number;
  readonly terminal_status: RunEpisodeTerminalStatus;
  readonly terminal_event_seq: number;
  readonly snapshot_json: string;
  readonly snapshot_hash: string;
  readonly created_at: string;
};

export type AppendRunEpisodeInput = {
  user_id: string;
  workspace_id: string;
  run_id: string;
  schema_version: typeof RUN_EPISODE_SCHEMA_VERSION;
  terminal_event_seq: number;
  snapshot: unknown;
};

export type RunEpisodeScope = {
  user_id: string;
  workspace_id: string;
};

/** Append-only persistence for immutable, replayable terminal-run snapshots. */
export class RunEpisodeRepository {
  constructor(private readonly db: DatabaseSync) {}

  append(input: AppendRunEpisodeInput): RunEpisodeRecord {
    if (!Number.isInteger(input.terminal_event_seq) || input.terminal_event_seq < 1) {
      throw new Error(`RUN_EPISODE_TERMINAL_EVENT_SEQ_INVALID:${input.run_id}`);
    }

    const snapshotJson = canonicalJson(input.snapshot, input.run_id);
    const snapshotHash = createHash("sha256").update(snapshotJson).digest("hex");
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const episode = this.appendWithinTransaction(input, snapshotJson, snapshotHash);
      this.db.exec("COMMIT");
      return episode;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  findByRun(input: RunEpisodeScope & { run_id: string }): RunEpisodeRecord | undefined {
    return mapRunEpisodeRow(this.db.prepare(`
      SELECT *
      FROM run_episodes
      WHERE user_id = ? AND workspace_id = ? AND run_id = ?
    `).get(input.user_id, input.workspace_id, input.run_id));
  }

  get(input: RunEpisodeScope & { episode_id: string }): RunEpisodeRecord {
    const episode = mapRunEpisodeRow(this.db.prepare(`
      SELECT *
      FROM run_episodes
      WHERE user_id = ? AND workspace_id = ? AND id = ?
    `).get(input.user_id, input.workspace_id, input.episode_id));
    if (!episode) {
      throw new Error(`RUN_EPISODE_NOT_FOUND:${input.episode_id}`);
    }
    return episode;
  }

  private findByRunIdentity(input: { user_id: string; run_id: string }): RunEpisodeRecord | undefined {
    return mapRunEpisodeRow(this.db.prepare(`
      SELECT * FROM run_episodes WHERE user_id = ? AND run_id = ?
    `).get(input.user_id, input.run_id));
  }

  private appendWithinTransaction(
    input: AppendRunEpisodeInput,
    snapshotJson: string,
    snapshotHash: string
  ): RunEpisodeRecord {
    const existing = this.findByRunIdentity({ user_id: input.user_id, run_id: input.run_id });
    if (existing) {
      return resolveIdempotentAppend(existing, input.workspace_id, snapshotHash);
    }
    const run = this.requireTerminalRun(input);
    this.requireLatestTerminalEvent(input, run.status);

    const createdAt = new Date().toISOString();
    const id = `episode:${input.run_id}`;
    this.db.prepare(`
      INSERT OR IGNORE INTO run_episodes (
        id, user_id, workspace_id, run_id, session_id, datasource_id, schema_version,
        terminal_status, terminal_event_seq, snapshot_json, snapshot_hash, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      input.user_id,
      input.workspace_id,
      input.run_id,
      run.session_id,
      run.datasource_id ?? null,
      input.schema_version,
      run.status,
      input.terminal_event_seq,
      snapshotJson,
      snapshotHash,
      createdAt
    );

    const appended = this.findByRunIdentity({ user_id: input.user_id, run_id: input.run_id });
    if (!appended) {
      throw new Error(`RUN_EPISODE_APPEND_FAILED:${input.run_id}`);
    }
    return resolveIdempotentAppend(appended, input.workspace_id, snapshotHash);
  }

  private requireLatestTerminalEvent(input: {
    user_id: string;
    run_id: string;
    terminal_event_seq: number;
  }, terminalStatus: RunEpisodeTerminalStatus): void {
    const row = this.db.prepare(`
      SELECT event_type
      FROM run_events
      WHERE user_id = ? AND run_id = ? AND seq = ?
        AND seq = (
          SELECT MAX(seq) FROM run_events WHERE user_id = ? AND run_id = ?
        )
    `).get(
      input.user_id,
      input.run_id,
      input.terminal_event_seq,
      input.user_id,
      input.run_id
    );
    if (!isRecord(row)) {
      throw new Error(`RUN_EPISODE_TERMINAL_EVENT_INVALID:${input.run_id}:${input.terminal_event_seq}`);
    }
    const eventType = requiredString(row, "event_type");
    const expectedEventType = terminalStatus === "failed" ? EventType.RUN_ERROR : EventType.RUN_FINISHED;
    if (eventType !== expectedEventType) {
      throw new Error(`RUN_EPISODE_TERMINAL_EVENT_INVALID:${input.run_id}:${input.terminal_event_seq}`);
    }
  }

  private requireTerminalRun(input: { user_id: string; run_id: string }): {
    datasource_id?: string;
    session_id: string;
    status: RunEpisodeTerminalStatus;
  } {
    const row = this.db.prepare(`
      SELECT session_id, datasource_id, status
      FROM runs
      WHERE user_id = ? AND id = ?
    `).get(input.user_id, input.run_id);
    if (!isRecord(row)) {
      throw new Error(`RUN_EPISODE_RUN_NOT_FOUND:${input.run_id}`);
    }

    const status = requiredString(row, "status");
    if (!isTerminalStatus(status)) {
      throw new Error(`RUN_EPISODE_RUN_NOT_TERMINAL:${input.run_id}:${status}`);
    }
    const datasourceId = optionalString(row.datasource_id);
    return {
      ...(datasourceId ? { datasource_id: datasourceId } : {}),
      session_id: requiredString(row, "session_id"),
      status
    };
  }
}

export const initializeEvolutionLedgerSchema = (db: DatabaseSync): void => {
  db.exec(`
    CREATE TABLE IF NOT EXISTS run_episodes (
      id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      workspace_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      datasource_id TEXT,
      schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
      terminal_status TEXT NOT NULL CHECK (terminal_status IN ('completed', 'failed', 'canceled')),
      terminal_event_seq INTEGER NOT NULL CHECK (terminal_event_seq >= 1),
      snapshot_json TEXT NOT NULL,
      snapshot_hash TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (user_id, id),
      UNIQUE (user_id, run_id),
      FOREIGN KEY (user_id, run_id) REFERENCES runs(user_id, id),
      FOREIGN KEY (user_id, session_id) REFERENCES sessions(user_id, id),
      FOREIGN KEY (workspace_id, user_id) REFERENCES workspace_memberships(workspace_id, user_id),
      FOREIGN KEY (user_id, run_id, terminal_event_seq) REFERENCES run_events(user_id, run_id, seq)
    );
    CREATE INDEX IF NOT EXISTS idx_run_episodes_scope
      ON run_episodes(workspace_id, user_id, created_at DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_run_episodes_status
      ON run_episodes(workspace_id, user_id, terminal_status, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_run_episodes_datasource
      ON run_episodes(workspace_id, user_id, datasource_id, created_at DESC);
    CREATE TRIGGER IF NOT EXISTS trg_run_episodes_immutable_update
      BEFORE UPDATE ON run_episodes
      BEGIN
        SELECT RAISE(ABORT, 'RUN_EPISODE_IMMUTABLE');
      END;
  `);
};

const resolveIdempotentAppend = (
  existing: RunEpisodeRecord,
  workspaceId: string,
  snapshotHash: string
): RunEpisodeRecord => {
  if (existing.workspace_id !== workspaceId) {
    throw new Error(`RUN_EPISODE_SCOPE_CONFLICT:${existing.run_id}`);
  }
  if (existing.snapshot_hash !== snapshotHash) {
    throw new Error(`RUN_EPISODE_CONFLICT:${existing.run_id}`);
  }
  return existing;
};

const canonicalJson = (snapshot: unknown, runId: string): string => {
  try {
    const canonical = canonicalizeJsonValue(snapshot, new Set<object>());
    const json = JSON.stringify(canonical);
    if (typeof json !== "string") {
      throw new Error("snapshot is not canonical JSON");
    }
    return json;
  } catch {
    throw new Error(`RUN_EPISODE_SNAPSHOT_INVALID:${runId}`);
  }
};

const canonicalizeJsonValue = (value: unknown, ancestors: Set<object>): unknown => {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("non-finite number is not supported");
    }
    return value;
  }
  if (typeof value !== "object") {
    throw new Error(`unsupported JSON value: ${typeof value}`);
  }
  if (ancestors.has(value)) {
    throw new Error("cyclic JSON value is not supported");
  }

  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return canonicalizeJsonArray(value, ancestors);
    }
    const prototype = Object.getPrototypeOf(value) as unknown;
    if (prototype !== Object.prototype && prototype !== null) {
      throw new Error("non-plain object is not supported");
    }
    return Object.fromEntries(
      ownEnumerableDataEntries(value)
        .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
        .map(([key, entry]) => [key, canonicalizeJsonValue(entry, ancestors)])
    );
  } finally {
    ancestors.delete(value);
  }
};

const canonicalizeJsonArray = (value: unknown[], ancestors: Set<object>): unknown[] => {
  const keys = Reflect.ownKeys(value);
  if (keys.some((key) => key !== "length" && !isArrayIndexKey(key, value.length))) {
    throw new Error("array contains unsupported properties");
  }
  const result: unknown[] = [];
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.hasOwn(value, index)) {
      throw new Error("sparse arrays are not supported");
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
    if (!descriptor || !("value" in descriptor)) {
      throw new Error("array accessors are not supported");
    }
    result.push(canonicalizeJsonValue(descriptor.value, ancestors));
  }
  return result;
};

const isArrayIndexKey = (key: string | symbol, length: number): boolean => {
  if (typeof key !== "string") {
    return false;
  }
  const index = Number(key);
  return Number.isInteger(index) && index >= 0 && index < length && String(index) === key;
};

const ownEnumerableDataEntries = (value: object): Array<[string, unknown]> =>
  Reflect.ownKeys(value).map((key) => {
    if (typeof key !== "string") {
      throw new Error("symbol keys are not supported");
    }
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor?.enumerable || !("value" in descriptor)) {
      throw new Error("non-enumerable properties and accessors are not supported");
    }
    return [key, descriptor.value];
  });

const mapRunEpisodeRow = (row: unknown): RunEpisodeRecord | undefined => {
  if (!isRecord(row)) {
    return undefined;
  }
  const datasourceId = optionalString(row.datasource_id);
  return {
    id: requiredString(row, "id"),
    user_id: requiredString(row, "user_id"),
    workspace_id: requiredString(row, "workspace_id"),
    run_id: requiredString(row, "run_id"),
    session_id: requiredString(row, "session_id"),
    ...(datasourceId ? { datasource_id: datasourceId } : {}),
    schema_version: requiredNumber(row, "schema_version"),
    terminal_status: requiredTerminalStatus(row, "terminal_status"),
    terminal_event_seq: requiredNumber(row, "terminal_event_seq"),
    snapshot_json: requiredString(row, "snapshot_json"),
    snapshot_hash: requiredString(row, "snapshot_hash"),
    created_at: requiredString(row, "created_at")
  };
};

const isTerminalStatus = (value: string): value is RunEpisodeTerminalStatus =>
  value === "completed" || value === "failed" || value === "canceled";

const requiredTerminalStatus = (row: Record<string, unknown>, key: string): RunEpisodeTerminalStatus => {
  const value = requiredString(row, key);
  if (!isTerminalStatus(value)) {
    throw new Error(`Expected terminal run status column: ${key}`);
  }
  return value;
};

const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null;

const optionalString = (value: unknown): string | undefined => typeof value === "string" ? value : undefined;

const requiredString = (row: Record<string, unknown>, key: string): string => {
  const value = row[key];
  if (typeof value !== "string") {
    throw new Error(`Expected string column: ${key}`);
  }
  return value;
};

const requiredNumber = (row: Record<string, unknown>, key: string): number => {
  const value = row[key];
  if (typeof value !== "number") {
    throw new Error(`Expected number column: ${key}`);
  }
  return value;
};
