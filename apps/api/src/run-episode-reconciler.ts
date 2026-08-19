import { EventType } from "@ag-ui/client";
import {
  type MetadataStore,
  type RunEpisodeReconciliationCursor,
  type TerminalRunReconciliationCandidate
} from "@datafoundry/metadata";

import { RunEpisodeLedger } from "./run-episode-ledger.js";
import type { RunTerminalObserver, RunTerminalStatus } from "./run-finalizer.js";

export const DEFAULT_RUN_EPISODE_RECONCILIATION_INTERVAL_MS = 60_000;
export const DEFAULT_RUN_EPISODE_RECONCILIATION_SETTLE_DELAY_MS = 60_000;
const DEFAULT_RECONCILIATION_BATCH_SIZE = 10;
const DEFAULT_RECONCILIATION_MAX_BATCH_DURATION_MS = 25;
const DEFAULT_RECONCILIATION_MAX_EVENTS_PER_RUN = 10_000;

export type RunEpisodeReconciliationReport = {
  appended: number;
  failed: number;
  has_more: boolean;
  missing_terminal_events: number;
  oversized_event_ledgers: number;
  scanned: number;
  skipped: number;
};

export type RunEpisodeReconciliationRunner = {
  runOnce(): RunEpisodeReconciliationReport;
};

type ReconcilerOptions = {
  batchSize?: number | undefined;
  maxBatchDurationMs?: number | undefined;
  maxEventsPerRun?: number | undefined;
  monotonicNow?: (() => number) | undefined;
  now?: (() => number) | undefined;
  observer?: RunTerminalObserver | undefined;
  settleDelayMs?: number | undefined;
  warn?: ((message: string, details: Record<string, unknown>) => void) | undefined;
};

/**
 * Replays one bounded page of durable terminal runs that missed episode capture.
 * A matching persisted terminal event is required: reconciliation never invents
 * events for an in-flight finalizer or makes an incomplete snapshot immutable.
 * The cursor advances across failures so one corrupt historical row cannot starve
 * the rest of the backlog; it resets after reaching the end so failures retry later.
 */
export class RunEpisodeReconciler implements RunEpisodeReconciliationRunner {
  private readonly batchSize: number;
  private readonly maxBatchDurationMs: number;
  private readonly maxEventsPerRun: number;
  private readonly monotonicNow: () => number;
  private readonly observer: RunTerminalObserver;
  private readonly settleDelayMs: number;
  private readonly warn: (message: string, details: Record<string, unknown>) => void;
  private readonly now: () => number;
  private nextCursor: RunEpisodeReconciliationCursor | undefined;

  constructor(
    private readonly metadataStore: MetadataStore,
    options: ReconcilerOptions = {}
  ) {
    this.batchSize = positiveInteger(options.batchSize, DEFAULT_RECONCILIATION_BATCH_SIZE, 500);
    this.maxBatchDurationMs = positiveInteger(
      options.maxBatchDurationMs,
      DEFAULT_RECONCILIATION_MAX_BATCH_DURATION_MS,
      Number.MAX_SAFE_INTEGER
    );
    this.maxEventsPerRun = positiveInteger(
      options.maxEventsPerRun,
      DEFAULT_RECONCILIATION_MAX_EVENTS_PER_RUN,
      1_000_000
    );
    this.monotonicNow = options.monotonicNow ?? (() => performance.now());
    this.now = options.now ?? Date.now;
    this.observer = options.observer ?? new RunEpisodeLedger(metadataStore);
    this.settleDelayMs = nonNegativeInteger(
      options.settleDelayMs,
      DEFAULT_RUN_EPISODE_RECONCILIATION_SETTLE_DELAY_MS
    );
    this.warn = options.warn ?? ((message, details) => console.warn(message, details));
  }

  runOnce(): RunEpisodeReconciliationReport {
    const startedAt = this.monotonicNow();
    const candidates = this.metadataStore.runEpisodes.listTerminalForReconciliation({
      ...(this.nextCursor ? { after: this.nextCursor } : {}),
      finished_before: new Date(this.now() - this.settleDelayMs).toISOString(),
      limit: this.batchSize
    });
    const report: RunEpisodeReconciliationReport = {
      appended: 0,
      failed: 0,
      has_more: false,
      missing_terminal_events: 0,
      oversized_event_ledgers: 0,
      scanned: 0,
      skipped: 0
    };

    if (candidates.length === 0) {
      this.nextCursor = undefined;
      return report;
    }

    for (const candidate of candidates) {
      if (report.scanned > 0 && this.monotonicNow() - startedAt >= this.maxBatchDurationMs) {
        report.has_more = true;
        break;
      }
      report.scanned += 1;
      this.nextCursor = cursorFor(candidate);
      try {
        const result = this.reconcileCandidate(candidate);
        if (result === "missing_terminal") {
          report.missing_terminal_events += 1;
          report.skipped += 1;
        } else if (result === "oversized") {
          report.oversized_event_ledgers += 1;
          report.skipped += 1;
        } else {
          report[result] += 1;
        }
      } catch (error) {
        report.failed += 1;
        this.warn("[data-foundry] run episode reconciliation failed", {
          error: errorMessage(error),
          runId: candidate.run_id,
          status: candidate.status,
          userId: candidate.user_id
        });
      }
    }

    report.has_more ||= report.scanned < candidates.length || candidates.length === this.batchSize;
    if (!report.has_more) {
      this.nextCursor = undefined;
    }
    return report;
  }

  private reconcileCandidate(
    candidate: TerminalRunReconciliationCandidate
  ): "appended" | "missing_terminal" | "oversized" | "skipped" {
    if (candidate.episode_id || this.metadataStore.runEpisodes.findByRun({
      user_id: candidate.user_id,
      workspace_id: candidate.workspace_id,
      run_id: candidate.run_id
    })) {
      return "skipped";
    }

    const run = this.metadataStore.runs.find({ user_id: candidate.user_id, run_id: candidate.run_id });
    if (!run || !isTerminalStatus(run.status)) {
      return "skipped";
    }
    const expectedEventType = terminalEventType(run.status);
    const latestEvent = this.metadataStore.runEvents.latestByRun({
      user_id: run.user_id,
      run_id: run.id
    });
    if (latestEvent?.event_type !== expectedEventType) {
      this.warn("[data-foundry] run episode reconciliation awaiting durable terminal event", {
        actualEventType: latestEvent?.event_type,
        expectedEventType,
        runId: run.id,
        status: run.status,
        userId: run.user_id
      });
      return "missing_terminal";
    }

    const boundedEventCount = this.metadataStore.runEvents.countByRunUpTo({
      user_id: run.user_id,
      run_id: run.id,
      limit: this.maxEventsPerRun + 1
    });
    if (boundedEventCount > this.maxEventsPerRun) {
      this.warn("[data-foundry] run episode reconciliation event limit exceeded", {
        eventCountAtLeast: boundedEventCount,
        maxEventsPerRun: this.maxEventsPerRun,
        runId: run.id,
        status: run.status,
        userId: run.user_id
      });
      return "oversized";
    }

    // A normal Finalizer or another reconciler may have won after the scan.
    if (this.metadataStore.runEpisodes.findByRun({
      user_id: run.user_id,
      workspace_id: candidate.workspace_id,
      run_id: run.id
    })) {
      return "skipped";
    }

    this.observer.observeTerminal({
      runId: run.id,
      sessionId: run.session_id,
      status: run.status,
      userId: run.user_id,
      workspaceId: candidate.workspace_id
    });
    if (!this.metadataStore.runEpisodes.findByRun({
      user_id: run.user_id,
      workspace_id: candidate.workspace_id,
      run_id: run.id
    })) {
      throw new Error(`RUN_EPISODE_RECONCILIATION_NOT_RECORDED:${run.id}`);
    }
    return "appended";
  }
}

type SchedulerOptions = {
  initialDelayMs?: number | undefined;
  intervalMs?: number | undefined;
  log?: ((message: string, details: Record<string, unknown>) => void) | undefined;
  warn?: ((message: string, details: Record<string, unknown>) => void) | undefined;
};

/** Deferred, non-overlapping scheduler. Full pages drain on later timer turns. */
export class RunEpisodeReconciliationScheduler {
  private readonly initialDelayMs: number;
  private readonly intervalMs: number;
  private readonly log: (message: string, details: Record<string, unknown>) => void;
  private readonly warn: (message: string, details: Record<string, unknown>) => void;
  private started = false;
  private timer: ReturnType<typeof setTimeout> | undefined;

  constructor(
    private readonly runner: RunEpisodeReconciliationRunner,
    options: SchedulerOptions = {}
  ) {
    this.initialDelayMs = nonNegativeInteger(options.initialDelayMs, 0);
    this.intervalMs = positiveInteger(
      options.intervalMs,
      DEFAULT_RUN_EPISODE_RECONCILIATION_INTERVAL_MS,
      Number.MAX_SAFE_INTEGER
    );
    this.log = options.log ?? ((message, details) => console.log(message, details));
    this.warn = options.warn ?? ((message, details) => console.warn(message, details));
  }

  start(): void {
    if (this.started) {
      return;
    }
    this.started = true;
    this.schedule(this.initialDelayMs);
  }

  stop(): void {
    this.started = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  private schedule(delayMs: number): void {
    this.timer = setTimeout(() => this.tick(), delayMs);
    this.timer.unref?.();
  }

  private tick(): void {
    this.timer = undefined;
    if (!this.started) {
      return;
    }

    let nextDelayMs = this.intervalMs;
    try {
      const report = this.runner.runOnce();
      if (report.scanned > 0 || report.failed > 0) {
        this.log("[data-foundry] run episode reconciliation", report);
      }
      if (report.has_more) {
        nextDelayMs = 0;
      }
    } catch (error) {
      this.warn("[data-foundry] run episode reconciliation scan failed", {
        error: errorMessage(error)
      });
    }

    if (this.started) {
      this.schedule(nextDelayMs);
    }
  }
}

const terminalEventType = (status: RunTerminalStatus): EventType.RUN_ERROR | EventType.RUN_FINISHED =>
  status === "failed" ? EventType.RUN_ERROR : EventType.RUN_FINISHED;

const cursorFor = (candidate: TerminalRunReconciliationCandidate): RunEpisodeReconciliationCursor => ({
  run_id: candidate.run_id,
  sort_at: candidate.sort_at,
  user_id: candidate.user_id
});

const isTerminalStatus = (status: string): status is RunTerminalStatus =>
  status === "completed" || status === "failed" || status === "canceled";

const positiveInteger = (value: number | undefined, fallback: number, maximum: number): number =>
  typeof value === "number" && Number.isInteger(value) && value > 0
    ? Math.min(value, maximum)
    : fallback;

const nonNegativeInteger = (value: number | undefined, fallback: number): number =>
  typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : fallback;

const errorMessage = (error: unknown): string => error instanceof Error ? error.message : String(error);
