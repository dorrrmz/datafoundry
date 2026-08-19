import { EventType, type BaseEvent } from "@ag-ui/client";
import type { ProtocolCompletionDecision } from "@datafoundry/agent-runtime";
import type { FileAssetService } from "@datafoundry/files";
import {
  createMetadataStore,
  createVerifiedTestIdentity,
  RunEventWriter
} from "@datafoundry/metadata";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, vi } from "vitest";

import type { ConversationMemoryEventObserver } from "./conversation-memory.js";
import { RunEpisodeLedger, type RunEpisodeSnapshotV1 } from "./run-episode-ledger.js";
import { RunEventPipeline } from "./run-event-pipeline.js";
import { RunFinalizer } from "./run-finalizer.js";
import type { TaskPlanProjector } from "./task-plan-projector.js";
import { ToolCallResultBridge } from "./tool-call-result-bridge.js";

const completedDecision = {
  status: "completed",
  evaluatedContextPackageRef: { packageId: "context-1", revision: 1 },
  evidenceRefs: []
} satisfies ProtocolCompletionDecision;

describe("run episode terminal pipeline", () => {
  it("persists an immutable episode after the pipeline writes the terminal event", async () => {
    const root = mkdtempSync(join(tmpdir(), "run-episode-pipeline-"));
    const metadata = createMetadataStore({ database_path: join(root, "metadata.sqlite") });
    try {
      const { userId, workspaceId } = createVerifiedTestIdentity(metadata);
      const sessionId = "session-pipeline";
      const runId = "run-pipeline";
      metadata.sessions.create({ user_id: userId, id: sessionId, title: "Episode integration" });
      metadata.runs.create({
        user_id: userId,
        id: runId,
        session_id: sessionId,
        user_input: "analyze the run",
        status: "running"
      });

      const pipeline = new RunEventPipeline({
        conversationMemoryObserver: {
          observe: vi.fn()
        } as unknown as ConversationMemoryEventObserver,
        runEventWriter: new RunEventWriter(metadata.runEvents),
        runId,
        sessionId,
        sink: vi.fn(),
        taskPlanProjector: {
          observe: () => []
        } as unknown as TaskPlanProjector,
        toolCallResultBridge: new ToolCallResultBridge(),
        userId
      });
      pipeline.emit({ type: EventType.RUN_STARTED, runId, threadId: sessionId } as BaseEvent);

      const finalizer = new RunFinalizer({
        destroyWorkspace: async () => undefined,
        emit: (event) => pipeline.emit(event),
        fileAssetService: {
          gcOrphanAssets: vi.fn(),
          syncWorkspaceFile: vi.fn()
        } as unknown as FileAssetService,
        flushCompletedMemory: async () => undefined,
        flushDraftsMemory: vi.fn(),
        memoryExtractionTimeoutMs: 100,
        metadataStore: metadata,
        runId,
        runTerminalObserver: new RunEpisodeLedger(metadata),
        sessionDir: join(root, "missing-session-dir"),
        sessionId,
        userId,
        workspaceId
      });

      await finalizer.complete({
        terminalDecision: completedDecision,
        terminalEvent: { type: EventType.RUN_FINISHED, runId, threadId: sessionId } as BaseEvent
      });

      const episode = metadata.runEpisodes.findByRun({ user_id: userId, workspace_id: workspaceId, run_id: runId });
      expect(episode).toMatchObject({
        run_id: runId,
        terminal_status: "completed"
      });
      const snapshot = JSON.parse(episode?.snapshot_json ?? "null") as RunEpisodeSnapshotV1;
      expect(snapshot.terminal_event_seq).toBe(episode?.terminal_event_seq);
      expect(snapshot.events.at(-1)?.event_type).toBe(EventType.RUN_FINISHED);
      expect(metadata.runEvents.listByRun({ user_id: userId, run_id: runId }).at(-1)?.seq)
        .toBe(episode?.terminal_event_seq);
    } finally {
      metadata.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});
