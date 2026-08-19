import { EventType } from "@ag-ui/core";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import {
  createMetadataStore,
  createVerifiedTestIdentity,
  type MetadataStore,
  RunEventWriter
} from "./index.js";

describe("RunEpisodeRepository", () => {
  it("round-trips a canonical terminal-run snapshot", () => {
    const fixture = createFixture("roundtrip");
    try {
      const run = seedRun(fixture.metadata, "roundtrip", "completed");

      const episode = fixture.metadata.runEpisodes.append({
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId,
        schema_version: 1,
        terminal_event_seq: run.eventSeq,
        snapshot: { z: 3, nested: { y: 2, x: 1 }, a: "first" }
      });

      expect(episode).toMatchObject({
        id: `episode:${run.runId}`,
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId,
        session_id: run.sessionId,
        schema_version: 1,
        terminal_status: "completed",
        terminal_event_seq: run.eventSeq
      });
      expect(episode.snapshot_json).toBe('{"a":"first","nested":{"x":1,"y":2},"z":3}');
      expect(episode.snapshot_hash).toMatch(/^[a-f0-9]{64}$/);
      expect(fixture.metadata.runEpisodes.findByRun({
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId
      })).toEqual(episode);
      expect(fixture.metadata.runEpisodes.get({
        user_id: run.userId,
        workspace_id: run.workspaceId,
        episode_id: episode.id
      })).toEqual(episode);
    } finally {
      fixture.close();
    }
  });

  it("returns the existing episode when canonical snapshots have the same hash", () => {
    const fixture = createFixture("idempotent");
    try {
      const run = seedRun(fixture.metadata, "idempotent", "completed");
      const common = {
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId,
        schema_version: 1 as const,
        terminal_event_seq: run.eventSeq
      };

      const first = fixture.metadata.runEpisodes.append({
        ...common,
        snapshot: { z: 1, nested: { y: 2, x: 3 } }
      });
      const replay = fixture.metadata.runEpisodes.append({
        ...common,
        snapshot: { nested: { x: 3, y: 2 }, z: 1 }
      });
      new RunEventWriter(fixture.metadata.runEvents).write({
        user_id: run.userId,
        run_id: run.runId,
        session_id: run.sessionId,
        event: {
          type: EventType.CUSTOM,
          name: "post-episode-test-event",
          value: { runId: run.runId }
        }
      });
      const laterReplay = fixture.metadata.runEpisodes.append({
        ...common,
        snapshot: { nested: { x: 3, y: 2 }, z: 1 }
      });

      expect(replay).toEqual(first);
      expect(laterReplay).toEqual(first);
      expect(fixture.metadata.db.prepare("SELECT COUNT(*) AS count FROM run_episodes").get()).toMatchObject({ count: 1 });
    } finally {
      fixture.close();
    }
  });

  it("rejects a second snapshot with a stable conflict error code", () => {
    const fixture = createFixture("conflict");
    try {
      const run = seedRun(fixture.metadata, "conflict", "completed");
      const common = {
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId,
        schema_version: 1 as const,
        terminal_event_seq: run.eventSeq
      };
      fixture.metadata.runEpisodes.append({ ...common, snapshot: { answer: 1 } });

      expect(() => fixture.metadata.runEpisodes.append({
        ...common,
        snapshot: { answer: 2 }
      })).toThrowError(`RUN_EPISODE_CONFLICT:${run.runId}`);
    } finally {
      fixture.close();
    }
  });

  it("rejects a snapshot while its run is non-terminal", () => {
    const fixture = createFixture("non-terminal");
    try {
      const run = seedRun(fixture.metadata, "non-terminal", "running");

      expect(() => fixture.metadata.runEpisodes.append({
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId,
        schema_version: 1,
        terminal_event_seq: run.eventSeq,
        snapshot: { status: "still-running" }
      })).toThrowError(`RUN_EPISODE_RUN_NOT_TERMINAL:${run.runId}:running`);
    } finally {
      fixture.close();
    }
  });

  it("rejects a terminal watermark that is not the latest terminal event", () => {
    const fixture = createFixture("terminal-watermark");
    try {
      const run = seedRun(fixture.metadata, "terminal-watermark", "completed");
      const latest = new RunEventWriter(fixture.metadata.runEvents).write({
        user_id: run.userId,
        run_id: run.runId,
        session_id: run.sessionId,
        event: {
          type: EventType.CUSTOM,
          name: "post-terminal-test-event",
          value: { runId: run.runId }
        }
      });
      const common = {
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId,
        schema_version: 1 as const,
        snapshot: { invalid: true }
      };

      expect(() => fixture.metadata.runEpisodes.append({
        ...common,
        terminal_event_seq: run.eventSeq
      })).toThrowError(`RUN_EPISODE_TERMINAL_EVENT_INVALID:${run.runId}:${run.eventSeq}`);
      expect(() => fixture.metadata.runEpisodes.append({
        ...common,
        terminal_event_seq: latest.seq
      })).toThrowError(`RUN_EPISODE_TERMINAL_EVENT_INVALID:${run.runId}:${latest.seq}`);
    } finally {
      fixture.close();
    }
  });

  it.each([
    ["completed", EventType.RUN_ERROR],
    ["failed", EventType.RUN_FINISHED],
    ["canceled", EventType.RUN_ERROR]
  ] as const)("rejects a %s run with a mismatched terminal event", (status, terminalEventType) => {
    const fixture = createFixture(`terminal-mismatch-${status}`);
    try {
      const run = seedRun(fixture.metadata, `terminal-mismatch-${status}`, status, { terminalEventType });

      expect(() => fixture.metadata.runEpisodes.append({
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId,
        schema_version: 1,
        terminal_event_seq: run.eventSeq,
        snapshot: { status }
      })).toThrowError(`RUN_EPISODE_TERMINAL_EVENT_INVALID:${run.runId}:${run.eventSeq}`);
    } finally {
      fixture.close();
    }
  });

  it("rejects unsupported nested values instead of conflating NaN with null", () => {
    const fixture = createFixture("invalid-snapshot-values");
    try {
      const run = seedRun(fixture.metadata, "invalid-snapshot-values", "completed");
      const common = {
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId,
        schema_version: 1 as const,
        terminal_event_seq: run.eventSeq
      };
      const unsupported = [
        Number.NaN,
        Number.POSITIVE_INFINITY,
        Number.NEGATIVE_INFINITY,
        undefined,
        () => "unsupported",
        Symbol("unsupported"),
        1n,
        new Date("2026-08-19T00:00:00.000Z")
      ];

      for (const value of unsupported) {
        expect(() => fixture.metadata.runEpisodes.append({
          ...common,
          snapshot: { nested: { value } }
        })).toThrowError(`RUN_EPISODE_SNAPSHOT_INVALID:${run.runId}`);
      }

      const nullEpisode = fixture.metadata.runEpisodes.append({
        ...common,
        snapshot: { nested: { value: null } }
      });
      expect(nullEpisode.snapshot_json).toBe('{"nested":{"value":null}}');
    } finally {
      fixture.close();
    }
  });

  it("keeps identical run and episode ids isolated by user", () => {
    const fixture = createFixture("user-scope");
    try {
      const first = seedRun(fixture.metadata, "first", "completed", {
        runId: "shared-run",
        sessionId: "shared-session"
      });
      const second = seedRun(fixture.metadata, "second", "completed", {
        runId: "shared-run",
        sessionId: "shared-session"
      });
      const firstEpisode = fixture.metadata.runEpisodes.append({
        user_id: first.userId,
        workspace_id: first.workspaceId,
        run_id: first.runId,
        schema_version: 1,
        terminal_event_seq: first.eventSeq,
        snapshot: { owner: "first" }
      });

      expect(fixture.metadata.runEpisodes.findByRun({
        user_id: second.userId,
        workspace_id: second.workspaceId,
        run_id: second.runId
      })).toBeUndefined();

      const secondEpisode = fixture.metadata.runEpisodes.append({
        user_id: second.userId,
        workspace_id: second.workspaceId,
        run_id: second.runId,
        schema_version: 1,
        terminal_event_seq: second.eventSeq,
        snapshot: { owner: "second" }
      });
      expect(firstEpisode.id).toBe(secondEpisode.id);
      expect(JSON.parse(firstEpisode.snapshot_json)).toEqual({ owner: "first" });
      expect(JSON.parse(secondEpisode.snapshot_json)).toEqual({ owner: "second" });
    } finally {
      fixture.close();
    }
  });

  it("rejects direct updates with the immutable-ledger trigger", () => {
    const fixture = createFixture("immutable");
    try {
      const run = seedRun(fixture.metadata, "immutable", "completed");
      const episode = fixture.metadata.runEpisodes.append({
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId,
        schema_version: 1,
        terminal_event_seq: run.eventSeq,
        snapshot: { answer: 42 }
      });

      expect(() => fixture.metadata.db.prepare(`
        UPDATE run_episodes SET snapshot_json = '{}' WHERE user_id = ? AND id = ?
      `).run(run.userId, episode.id)).toThrowError(/RUN_EPISODE_IMMUTABLE/);
      expect(fixture.metadata.runEpisodes.get({
        user_id: run.userId,
        workspace_id: run.workspaceId,
        episode_id: episode.id
      }).snapshot_json).toBe('{"answer":42}');
    } finally {
      fixture.close();
    }
  });

  it("survives database reopen and is removed by permanent session deletion", () => {
    const root = mkdtempSync(join(tmpdir(), "run-episode-reopen-"));
    const databasePath = join(root, "metadata.sqlite");
    let metadata: MetadataStore | undefined = createMetadataStore({ database_path: databasePath });
    try {
      const run = seedRun(metadata, "reopen", "completed");
      const episode = metadata.runEpisodes.append({
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId,
        schema_version: 1,
        terminal_event_seq: run.eventSeq,
        snapshot: { persisted: true }
      });
      metadata.close();
      metadata = undefined;

      metadata = createMetadataStore({ database_path: databasePath });
      expect(metadata.runEpisodes.get({
        user_id: run.userId,
        workspace_id: run.workspaceId,
        episode_id: episode.id
      })).toEqual(episode);
      expect(metadata.db.prepare(`
        SELECT COUNT(*) AS count FROM schema_migrations WHERE id = '0019_run_episodes'
      `).get()).toMatchObject({ count: 1 });

      expect(metadata.sessions.delete({ user_id: run.userId, session_id: run.sessionId })).toEqual({
        deleted: true,
        deletedSessionIds: [run.sessionId]
      });
      expect(metadata.runEpisodes.findByRun({
        user_id: run.userId,
        workspace_id: run.workspaceId,
        run_id: run.runId
      })).toBeUndefined();
    } finally {
      metadata?.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});

const createFixture = (name: string): { close: () => void; metadata: MetadataStore } => {
  const root = mkdtempSync(join(tmpdir(), `run-episode-${name}-`));
  const metadata = createMetadataStore({ database_path: join(root, "metadata.sqlite") });
  return {
    metadata,
    close: () => {
      metadata.close();
      rmSync(root, { recursive: true, force: true });
    }
  };
};

const seedRun = (
  metadata: MetadataStore,
  identity: string,
  status: "canceled" | "completed" | "failed" | "running",
  options: {
    runId?: string;
    sessionId?: string;
    terminalEventType?: EventType.RUN_ERROR | EventType.RUN_FINISHED;
  } = {}
): { eventSeq: number; runId: string; sessionId: string; userId: string; workspaceId: string } => {
  const { userId, workspaceId } = createVerifiedTestIdentity(metadata, {
    email: `${identity}@run-episode.example.test`
  });
  const sessionId = options.sessionId ?? `session-${identity}`;
  const runId = options.runId ?? `run-${identity}`;
  metadata.sessions.create({ user_id: userId, id: sessionId, title: identity });
  metadata.runs.create({
    user_id: userId,
    id: runId,
    session_id: sessionId,
    user_input: identity,
    status
  });
  const writer = new RunEventWriter(metadata.runEvents);
  const started = writer.write({
    user_id: userId,
    run_id: runId,
    session_id: sessionId,
    event: { type: EventType.RUN_STARTED, threadId: sessionId, runId }
  });
  const terminalEventType = options.terminalEventType
    ?? (status === "failed" ? EventType.RUN_ERROR : EventType.RUN_FINISHED);
  const event = status === "running"
    ? started
    : writer.write({
        user_id: userId,
        run_id: runId,
        session_id: sessionId,
        event: terminalEventType === EventType.RUN_ERROR
          ? { type: EventType.RUN_ERROR, message: identity, timestamp: Date.now() }
          : { type: EventType.RUN_FINISHED, threadId: sessionId, runId }
      });
  return { eventSeq: event.seq, runId, sessionId, userId, workspaceId };
};
