import { EventType, type BaseEvent } from "@ag-ui/client";
import type { ProtocolCompletionDecision } from "@datafoundry/agent-runtime";
import type { FileAssetService } from "@datafoundry/files";
import type { MetadataStore } from "@datafoundry/metadata";
import { afterEach, describe, expect, it, vi } from "vitest";

import { RunFinalizer, type RunTerminalObservation, type RunTerminalObserver } from "./run-finalizer.js";

const terminalEvent = { type: EventType.RUN_FINISHED, timestamp: 10 } as BaseEvent;
const failedEvent = { type: EventType.RUN_ERROR, message: "failed", timestamp: 10 } as BaseEvent;
const completedDecision = {
  status: "completed",
  evaluatedContextPackageRef: { packageId: "context-1", revision: 1 },
  evidenceRefs: []
} satisfies ProtocolCompletionDecision;

describe("RunFinalizer terminal observer", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("observes completion only after the terminal event is emitted", async () => {
    const harness = createHarness();

    await harness.finalizer.complete({ terminalDecision: completedDecision, terminalEvent });

    expect(harness.observer.observeTerminal).toHaveBeenCalledWith(terminalObservation("completed"));
    expect(harness.order.at(-2)).toBe(`emit:${EventType.RUN_FINISHED}`);
    expect(harness.order.at(-1)).toBe("observe:completed");
  });

  it("observes failure after its terminal event", () => {
    const harness = createHarness();

    harness.finalizer.fail({ errorMessage: "failed", terminalEvent: failedEvent });

    expect(harness.observer.observeTerminal).toHaveBeenCalledWith(terminalObservation("failed"));
    expect(harness.order.at(-2)).toBe(`emit:${EventType.RUN_ERROR}`);
    expect(harness.order.at(-1)).toBe("observe:failed");
  });

  it.each(["cancel", "cancelRun"] as const)("observes %s as canceled", async (method) => {
    const harness = createHarness();

    if (method === "cancel") {
      await harness.finalizer.cancel({
        interactionResolvedEvent: { type: EventType.CUSTOM, name: "interaction.resolved" } as BaseEvent,
        terminalEvent
      });
    } else {
      await harness.finalizer.cancelRun({ reason: "requested", terminalEvent });
    }

    expect(harness.observer.observeTerminal).toHaveBeenCalledWith(terminalObservation("canceled"));
    expect(harness.order.at(-2)).toBe(`emit:${EventType.RUN_FINISHED}`);
    expect(harness.order.at(-1)).toBe("observe:canceled");
  });

  it("does not observe a suspended run", () => {
    const harness = createHarness();

    harness.finalizer.suspend();

    expect(harness.observer.observeTerminal).not.toHaveBeenCalled();
  });

  it("keeps a completed run successful when the observer fails", async () => {
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const harness = createHarness({ observerError: new Error("ledger unavailable") });

    await expect(harness.finalizer.complete({
      terminalDecision: completedDecision,
      terminalEvent
    })).resolves.toBeUndefined();

    expect(harness.updateStatus).toHaveBeenLastCalledWith(expect.objectContaining({ status: "completed" }));
    expect(warning).toHaveBeenCalledWith(
      "[data-foundry] run terminal observer failed",
      expect.objectContaining({ error: "ledger unavailable", runId: "run-1", status: "completed" })
    );
  });
});

const createHarness = (options: { observerError?: Error } = {}) => {
  const order: string[] = [];
  const updateStatus = vi.fn((input: { status: string }) => input);
  const observer: RunTerminalObserver = {
    observeTerminal: vi.fn((input: RunTerminalObservation) => {
      order.push(`observe:${input.status}`);
      if (options.observerError) {
        throw options.observerError;
      }
    })
  };
  const finalizer = new RunFinalizer({
    destroyWorkspace: vi.fn(async () => undefined),
    emit: (event) => {
      order.push(`emit:${event.type}`);
    },
    fileAssetService: {} as FileAssetService,
    flushCompletedMemory: vi.fn(async () => undefined),
    flushDraftsMemory: vi.fn(),
    memoryExtractionTimeoutMs: 100,
    metadataStore: { runs: { updateStatus } } as unknown as MetadataStore,
    runId: "run-1",
    runTerminalObserver: observer,
    sessionDir: "/path/that/does/not/exist",
    sessionId: "session-1",
    userId: "user-1",
    workspaceId: "workspace-1"
  });
  return { finalizer, observer, order, updateStatus };
};

const terminalObservation = (
  status: RunTerminalObservation["status"]
): RunTerminalObservation => ({
  runId: "run-1",
  sessionId: "session-1",
  status,
  userId: "user-1",
  workspaceId: "workspace-1"
});
