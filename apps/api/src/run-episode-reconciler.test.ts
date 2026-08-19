import { EventType, type BaseEvent } from "@ag-ui/client";
import type { ProtocolCompletionDecision } from "@datafoundry/agent-runtime";
import type { FileAssetService } from "@datafoundry/files";
import {
  createMetadataStore,
  createVerifiedTestIdentity,
  type MetadataStore,
  RunEventWriter
} from "@datafoundry/metadata";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";

import { RunEpisodeLedger } from "./run-episode-ledger.js";
import {
  RunEpisodeReconciler,
  RunEpisodeReconciliationScheduler,
  type RunEpisodeReconciliationReport
} from "./run-episode-reconciler.js";
import { RunCancelRegistry } from "./run-cancel-registry.js";
import { RunFinalizer, type RunTerminalObserver } from "./run-finalizer.js";
import { createServer } from "./server.js";
import { reclaimOrphanedQueuedAndRunningRuns } from "./stale-active-runs.js";

describe("RunEpisodeReconciler", () => {
  it("never races a normal finalizer even when teardown exceeds the settle window", async () => {
    const fixture = createFixture();
    const teardown = deferred<void>();
    const teardownStarted = deferred<void>();
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    let completion: Promise<void> | undefined;
    try {
      const runId = createRunningRun(fixture, "slow-finalizer");
      const ledger = new RunEpisodeLedger(fixture.metadata);
      const finalizer = new RunFinalizer({
        destroyWorkspace: async () => {
          teardownStarted.resolve();
          await teardown.promise;
        },
        emit: (event: BaseEvent) => {
          fixture.writer.write({
            user_id: fixture.userId,
            run_id: runId,
            session_id: sessionId(runId),
            event
          });
        },
        fileAssetService: {
          gcOrphanAssets: vi.fn(),
          syncWorkspaceFile: vi.fn()
        } as unknown as FileAssetService,
        flushCompletedMemory: async () => undefined,
        flushDraftsMemory: vi.fn(),
        memoryExtractionTimeoutMs: 100,
        metadataStore: fixture.metadata,
        runId,
        runTerminalObserver: ledger,
        sessionDir: join(fixture.root, "missing-session-dir"),
        sessionId: sessionId(runId),
        userId: fixture.userId,
        workspaceId: fixture.workspaceId
      });
      completion = finalizer.complete({
        terminalDecision: completedDecision,
        terminalEvent: { type: EventType.RUN_FINISHED, runId, threadId: sessionId(runId) } as BaseEvent
      });
      await teardownStarted.promise;

      const report = new RunEpisodeReconciler(fixture.metadata, {
        settleDelayMs: 0
      }).runOnce();
      expect(report).toMatchObject({
        appended: 0,
        missing_terminal_events: 1,
        scanned: 1,
        skipped: 1
      });
      expect(warning).toHaveBeenCalledWith(
        "[data-foundry] run episode reconciliation awaiting durable terminal event",
        expect.objectContaining({ runId, expectedEventType: EventType.RUN_FINISHED })
      );
      expect(fixture.metadata.runEpisodes.findByRun({
        user_id: fixture.userId,
        workspace_id: fixture.workspaceId,
        run_id: runId
      })).toBeUndefined();

      teardown.resolve();
      await completion;
      expect(fixture.metadata.runEpisodes.findByRun({
        user_id: fixture.userId,
        workspace_id: fixture.workspaceId,
        run_id: runId
      })).toMatchObject({ terminal_status: "completed" });
      expect(fixture.metadata.runEvents.listByRun({ user_id: fixture.userId, run_id: runId }).at(-1)?.event_type)
        .toBe(EventType.RUN_FINISHED);
    } finally {
      teardown.resolve();
      await completion?.catch(() => undefined);
      fixture.close();
    }
  });

  it("waits for the terminal settle window instead of racing an in-flight finalizer", () => {
    const fixture = createFixture();
    try {
      const runId = createRunningRun(fixture, "settling");
      fixture.metadata.runs.updateStatus({ user_id: fixture.userId, run_id: runId, status: "completed" });
      fixture.metadata.db.prepare("UPDATE runs SET finished_at = ? WHERE user_id = ? AND id = ?")
        .run("2026-08-19T12:00:00.000Z", fixture.userId, runId);
      fixture.writer.write({
        user_id: fixture.userId,
        run_id: runId,
        session_id: sessionId(runId),
        event: { type: EventType.RUN_FINISHED, runId, threadId: sessionId(runId) }
      });
      const reconciler = new RunEpisodeReconciler(fixture.metadata, {
        now: () => Date.parse("2026-08-19T12:00:30.000Z"),
        settleDelayMs: 60_000
      });

      expect(reconciler.runOnce().scanned).toBe(0);
      fixture.metadata.db.prepare("UPDATE runs SET finished_at = ? WHERE user_id = ? AND id = ?")
        .run("2026-08-19T11:58:00.000Z", fixture.userId, runId);
      expect(reconciler.runOnce()).toMatchObject({ appended: 1, scanned: 1 });
    } finally {
      fixture.close();
    }
  });

  it("records only runs whose latest persisted event is the matching terminal", () => {
    const fixture = createFixture();
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const completed = createRunningRun(fixture, "completed");
      fixture.metadata.runs.updateStatus({ user_id: fixture.userId, run_id: completed, status: "completed" });
      fixture.writer.write({
        user_id: fixture.userId,
        run_id: completed,
        session_id: sessionId(completed),
        event: { type: EventType.RUN_FINISHED, runId: completed, threadId: sessionId(completed) }
      });

      const failed = createRunningRun(fixture, "failed");
      fixture.writer.write({
        user_id: fixture.userId,
        run_id: failed,
        session_id: sessionId(failed),
        event: { type: EventType.CUSTOM, name: "early.failure.context", value: { safe: true } }
      });
      fixture.metadata.runs.updateStatus({
        user_id: fixture.userId,
        run_id: failed,
        status: "failed",
        error_message: "EARLY_FAILURE"
      });
      fixture.writer.write({
        user_id: fixture.userId,
        run_id: failed,
        session_id: sessionId(failed),
        event: { type: EventType.RUN_ERROR, message: "EARLY_FAILURE" }
      });

      const canceled = createRunningRun(fixture, "canceled-with-terminal");
      fixture.metadata.runs.updateStatus({ user_id: fixture.userId, run_id: canceled, status: "canceled" });
      fixture.writer.write({
        user_id: fixture.userId,
        run_id: canceled,
        session_id: sessionId(canceled),
        event: { type: EventType.RUN_FINISHED, runId: canceled, threadId: sessionId(canceled) }
      });

      const staleCanceled = createRunningRun(fixture, "stale");
      expect(reclaimOrphanedQueuedAndRunningRuns({
        metadataStore: fixture.metadata,
        runCancelRegistry: new RunCancelRegistry()
      })).toBe(1);

      const postTerminal = createRunningRun(fixture, "post-terminal");
      fixture.writer.write({
        user_id: fixture.userId,
        run_id: postTerminal,
        session_id: sessionId(postTerminal),
        event: { type: EventType.RUN_FINISHED, runId: postTerminal, threadId: sessionId(postTerminal) }
      });
      fixture.writer.write({
        user_id: fixture.userId,
        run_id: postTerminal,
        session_id: sessionId(postTerminal),
        event: { type: EventType.CUSTOM, name: "late.event", value: { safe: true } }
      });
      fixture.metadata.runs.updateStatus({ user_id: fixture.userId, run_id: postTerminal, status: "completed" });

      const report = new RunEpisodeReconciler(fixture.metadata, { batchSize: 10, settleDelayMs: 0 }).runOnce();

      expect(report).toEqual({
        appended: 3,
        failed: 0,
        has_more: false,
        missing_terminal_events: 2,
        oversized_event_ledgers: 0,
        scanned: 5,
        skipped: 2
      });
      for (const [runId, status] of [
        [completed, "completed"],
        [failed, "failed"],
        [canceled, "canceled"]
      ] as const) {
        expect(fixture.metadata.runEpisodes.findByRun({
          user_id: fixture.userId,
          workspace_id: fixture.workspaceId,
          run_id: runId
        })).toMatchObject({ terminal_status: status });
      }
      for (const runId of [staleCanceled, postTerminal]) {
        expect(fixture.metadata.runEpisodes.findByRun({
          user_id: fixture.userId,
          workspace_id: fixture.workspaceId,
          run_id: runId
        })).toBeUndefined();
      }
      expect(fixture.metadata.runEvents.listByRun({ user_id: fixture.userId, run_id: staleCanceled })).toHaveLength(0);
      expect(fixture.metadata.runEvents.listByRun({ user_id: fixture.userId, run_id: postTerminal })).toHaveLength(2);
      expect(warning).toHaveBeenCalledTimes(2);
    } finally {
      fixture.close();
    }
  });

  it("isolates one observer failure, advances the batch, and retries idempotently", () => {
    const fixture = createFixture();
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const first = createRunningRun(fixture, "a-fails-once");
      const second = createRunningRun(fixture, "b-succeeds");
      fixture.metadata.runs.updateStatus({ user_id: fixture.userId, run_id: first, status: "completed" });
      fixture.metadata.runs.updateStatus({ user_id: fixture.userId, run_id: second, status: "completed" });
      for (const runId of [first, second]) {
        fixture.writer.write({
          user_id: fixture.userId,
          run_id: runId,
          session_id: sessionId(runId),
          event: { type: EventType.RUN_FINISHED, runId, threadId: sessionId(runId) }
        });
      }
      const ledger = new RunEpisodeLedger(fixture.metadata);
      let failedOnce = false;
      const observer: RunTerminalObserver = {
        observeTerminal(input) {
          if (input.runId === first && !failedOnce) {
            failedOnce = true;
            throw new Error("temporary ledger failure");
          }
          ledger.observeTerminal(input);
        }
      };
      const reconciler = new RunEpisodeReconciler(fixture.metadata, {
        batchSize: 10,
        observer,
        settleDelayMs: 0
      });

      expect(reconciler.runOnce()).toMatchObject({ appended: 1, failed: 1, scanned: 2 });
      expect(fixture.metadata.runEpisodes.findByRun({
        user_id: fixture.userId,
        workspace_id: fixture.workspaceId,
        run_id: second
      })).toBeDefined();
      expect(warning).toHaveBeenCalledWith(
        "[data-foundry] run episode reconciliation failed",
        expect.objectContaining({ error: "temporary ledger failure", runId: first })
      );

      expect(reconciler.runOnce()).toMatchObject({
        appended: 1,
        failed: 0,
        missing_terminal_events: 0,
        scanned: 2,
        skipped: 1
      });
      expect(fixture.metadata.runEvents.listByRun({ user_id: fixture.userId, run_id: first }))
        .toHaveLength(1);
    } finally {
      fixture.close();
    }
  });

  it("advances the cursor across already-recorded terminal rows before finding backlog", () => {
    const fixture = createFixture();
    try {
      const ledger = new RunEpisodeLedger(fixture.metadata);
      const runIds = ["page-a", "page-b", "page-c"].map((suffix, index) => {
        const runId = createRunningRun(fixture, suffix);
        fixture.metadata.runs.updateStatus({ user_id: fixture.userId, run_id: runId, status: "completed" });
        fixture.metadata.db.prepare("UPDATE runs SET finished_at = ? WHERE user_id = ? AND id = ?")
          .run(`2026-08-19T00:00:0${index + 1}.000Z`, fixture.userId, runId);
        fixture.writer.write({
          user_id: fixture.userId,
          run_id: runId,
          session_id: sessionId(runId),
          event: { type: EventType.RUN_FINISHED, runId, threadId: sessionId(runId) }
        });
        return runId;
      });
      for (const runId of runIds.slice(0, 2)) {
        ledger.observeTerminal({
          runId,
          sessionId: sessionId(runId),
          status: "completed",
          userId: fixture.userId,
          workspaceId: fixture.workspaceId
        });
      }
      const reconciler = new RunEpisodeReconciler(fixture.metadata, {
        batchSize: 2,
        now: () => Date.parse("2026-08-20T00:00:00.000Z"),
        settleDelayMs: 0
      });

      expect(reconciler.runOnce()).toMatchObject({
        appended: 0,
        has_more: true,
        scanned: 2,
        skipped: 2
      });
      expect(reconciler.runOnce()).toMatchObject({ appended: 1, has_more: false, scanned: 1 });
      expect(fixture.metadata.runEpisodes.findByRun({
        user_id: fixture.userId,
        workspace_id: fixture.workspaceId,
        run_id: runIds[2]!
      })).toBeDefined();
      expect(reconciler.runOnce()).toMatchObject({
        appended: 0,
        has_more: true,
        scanned: 2,
        skipped: 2
      });
    } finally {
      fixture.close();
    }
  });

  it("yields between runs when the per-turn time budget is exhausted", () => {
    const fixture = createFixture();
    try {
      for (const suffix of ["budget-a", "budget-b"]) {
        const runId = createRunningRun(fixture, suffix);
        fixture.metadata.runs.updateStatus({ user_id: fixture.userId, run_id: runId, status: "completed" });
        fixture.writer.write({
          user_id: fixture.userId,
          run_id: runId,
          session_id: sessionId(runId),
          event: { type: EventType.RUN_FINISHED, runId, threadId: sessionId(runId) }
        });
      }
      let monotonicTime = 0;
      const listTerminal = fixture.metadata.runEpisodes.listTerminalForReconciliation
        .bind(fixture.metadata.runEpisodes);
      vi.spyOn(fixture.metadata.runEpisodes, "listTerminalForReconciliation")
        .mockImplementation((input) => {
          const candidates = listTerminal(input);
          monotonicTime = 10;
          return candidates;
        });
      const reconciler = new RunEpisodeReconciler(fixture.metadata, {
        batchSize: 10,
        maxBatchDurationMs: 5,
        monotonicNow: () => monotonicTime,
        settleDelayMs: 0
      });

      expect(reconciler.runOnce()).toMatchObject({ appended: 1, has_more: true, scanned: 1 });
      expect(reconciler.runOnce()).toMatchObject({ appended: 1, has_more: false, scanned: 1 });
    } finally {
      fixture.close();
    }
  });

  it("diagnoses and skips an event ledger above the explicit reconciliation cap", () => {
    const fixture = createFixture();
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const runId = createRunningRun(fixture, "oversized");
      for (const name of ["context.one", "context.two"]) {
        fixture.writer.write({
          user_id: fixture.userId,
          run_id: runId,
          session_id: sessionId(runId),
          event: { type: EventType.CUSTOM, name, value: { safe: true } }
        });
      }
      fixture.metadata.runs.updateStatus({ user_id: fixture.userId, run_id: runId, status: "completed" });
      fixture.writer.write({
        user_id: fixture.userId,
        run_id: runId,
        session_id: sessionId(runId),
        event: { type: EventType.RUN_FINISHED, runId, threadId: sessionId(runId) }
      });
      const listByRun = vi.spyOn(fixture.metadata.runEvents, "listByRun");

      const report = new RunEpisodeReconciler(fixture.metadata, {
        maxEventsPerRun: 2,
        settleDelayMs: 0
      }).runOnce();

      expect(report).toMatchObject({
        appended: 0,
        oversized_event_ledgers: 1,
        scanned: 1,
        skipped: 1
      });
      expect(fixture.metadata.runEvents.latestByRun({ user_id: fixture.userId, run_id: runId }))
        .toMatchObject({ event_type: EventType.RUN_FINISHED, seq: 3 });
      expect(fixture.metadata.runEvents.countByRunUpTo({
        user_id: fixture.userId,
        run_id: runId,
        limit: 3
      })).toBe(3);
      expect(listByRun).not.toHaveBeenCalled();
      expect(warning).toHaveBeenCalledWith(
        "[data-foundry] run episode reconciliation event limit exceeded",
        expect.objectContaining({ eventCountAtLeast: 3, maxEventsPerRun: 2, runId })
      );
      expect(fixture.metadata.runEpisodes.findByRun({
        user_id: fixture.userId,
        workspace_id: fixture.workspaceId,
        run_id: runId
      })).toBeUndefined();
    } finally {
      fixture.close();
    }
  });
});

describe("RunEpisodeReconciliationScheduler", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("starts out of band, drains full batches without overlap, and stops cleanly", async () => {
    vi.useFakeTimers();
    const reports: RunEpisodeReconciliationReport[] = [
      emptyReport({ has_more: true, scanned: 2 }),
      emptyReport()
    ];
    const runner = { runOnce: vi.fn(() => reports.shift() ?? emptyReport()) };
    const scheduler = new RunEpisodeReconciliationScheduler(runner, {
      intervalMs: 60_000,
      log: vi.fn()
    });

    scheduler.start();
    expect(runner.runOnce).not.toHaveBeenCalled();
    await vi.advanceTimersToNextTimerAsync();
    expect(runner.runOnce).toHaveBeenCalledTimes(1);
    await vi.advanceTimersToNextTimerAsync();
    expect(runner.runOnce).toHaveBeenCalledTimes(2);

    scheduler.stop();
    await vi.advanceTimersByTimeAsync(120_000);
    expect(runner.runOnce).toHaveBeenCalledTimes(2);
  });

  it("contains top-level scan errors and schedules a later retry", async () => {
    vi.useFakeTimers();
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const runner = {
      runOnce: vi.fn()
        .mockImplementationOnce(() => { throw new Error("database busy"); })
        .mockReturnValueOnce(emptyReport())
    };
    const scheduler = new RunEpisodeReconciliationScheduler(runner, { intervalMs: 1000 });

    scheduler.start();
    await vi.advanceTimersToNextTimerAsync();
    expect(warning).toHaveBeenCalledWith(
      "[data-foundry] run episode reconciliation scan failed",
      expect.objectContaining({ error: "database busy" })
    );
    await vi.advanceTimersByTimeAsync(1000);
    expect(runner.runOnce).toHaveBeenCalledTimes(2);
    scheduler.stop();
  });
});

describe("createServer run episode reconciliation lifecycle", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("does not scan during startup and cancels the deferred scan before closing metadata", async () => {
    vi.useFakeTimers();
    vi.stubEnv("AUTH_REGISTRATION_MODE", "open");
    vi.stubEnv("AUTH_SESSION_SECRET", "test-session-secret-with-at-least-32-characters");
    vi.stubEnv("AUTH_PUBLIC_BASE_URL", "http://127.0.0.1:3000");
    vi.stubEnv("AUTH_EMAIL_DELIVERY", "test");
    vi.spyOn(console, "log").mockImplementation(() => undefined);
    const root = mkdtempSync(join(tmpdir(), "run-episode-server-lifecycle-"));
    const metadata = createMetadataStore({ database_path: join(root, "metadata.sqlite") });
    const runner = { runOnce: vi.fn(() => emptyReport()) };
    let closedByServer = false;
    try {
      const server = await createServer({
        metadataStore: metadata,
        runEpisodeReconciler: runner,
        runEpisodeReconciliationIntervalMs: 1000,
        taskStateRuntime: {} as never
      });

      expect(runner.runOnce).not.toHaveBeenCalled();
      server.emit("close");
      closedByServer = true;
      await vi.advanceTimersByTimeAsync(2000);
      expect(runner.runOnce).not.toHaveBeenCalled();
    } finally {
      if (!closedByServer) {
        metadata.close();
      }
      rmSync(root, { recursive: true, force: true });
    }
  });
});

const emptyReport = (
  overrides: Partial<RunEpisodeReconciliationReport> = {}
): RunEpisodeReconciliationReport => ({
  appended: 0,
  failed: 0,
  has_more: false,
  missing_terminal_events: 0,
  oversized_event_ledgers: 0,
  scanned: 0,
  skipped: 0,
  ...overrides
});

const createFixture = () => {
  const root = mkdtempSync(join(tmpdir(), "run-episode-reconciler-"));
  const metadata = createMetadataStore({ database_path: join(root, "metadata.sqlite") });
  const { userId, workspaceId } = createVerifiedTestIdentity(metadata);
  const writer = new RunEventWriter(metadata.runEvents);
  return {
    metadata,
    root,
    userId,
    workspaceId,
    writer,
    close: () => {
      metadata.close();
      rmSync(root, { recursive: true, force: true });
    }
  };
};

const createRunningRun = (
  fixture: { metadata: MetadataStore; userId: string },
  suffix: string
): string => {
  const runId = `run-${suffix}`;
  fixture.metadata.sessions.create({
    user_id: fixture.userId,
    id: sessionId(runId),
    title: suffix
  });
  fixture.metadata.runs.create({
    user_id: fixture.userId,
    id: runId,
    session_id: sessionId(runId),
    user_input: suffix,
    status: "running"
  });
  return runId;
};

const sessionId = (runId: string): string => `session-${runId}`;

const completedDecision = {
  status: "completed",
  evaluatedContextPackageRef: { packageId: "context-1", revision: 1 },
  evidenceRefs: []
} satisfies ProtocolCompletionDecision;

const deferred = <T,>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
};
