import { EventType } from "@ag-ui/client";
import type { ApiResult } from "@datafoundry/contracts";
import {
  createMetadataStore,
  createVerifiedTestIdentity,
  type MetadataStore,
  RunEventWriter
} from "@datafoundry/metadata";
import { mkdtempSync, rmSync } from "node:fs";
import type { IncomingMessage } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable } from "node:stream";
import { describe, expect, it } from "vitest";

import { handleConfigApiRequest, type ConfigApiContext } from "./config-api.js";

describe("evolution API", () => {
  it("captures a sanitized episode as an immutable eval case", async () => {
    const fixture = createFixture("case");
    try {
      const seeded = seedEpisode(fixture.metadata, "case");
      const response = await request(fixture.context(seeded), "POST", "/api/v1/eval-cases", {
        headers: { "idempotency-key": "capture-case-1" },
        body: {
          sourceRunId: seeded.runId,
          title: "Captured regression",
          tags: ["regression"],
          rubric: { dimensions: ["correctness"] }
        }
      });

      expect(response?.status).toBe(201);
      expect(response?.body).toMatchObject({
        success: true,
        data: {
          case: {
            title: "Captured regression",
            input: {
              prompt: "question-case",
              context: {
                modelName: "test-model",
                runConfig: {
                  model_name: "test-model",
                  protocol: { protocolId: "analysis", protocolVersion: "1" }
                }
              }
            },
            source: {
              kind: "episode",
              episodeId: seeded.episodeId,
              snapshotHash: seeded.snapshotHash
            }
          }
        }
      });
      const serializedCase = JSON.stringify(response?.body);
      expect(serializedCase).not.toContain("/private/sensitive-path");
      expect(serializedCase).not.toContain("free-form goal with opaque credential");
      expect(serializedCase).not.toContain("provider diagnostic secret");
      const created = dataOf<{ case: { id: string } }>(response).case;

      const replay = await request(fixture.context(seeded), "POST", "/api/v1/eval-cases", {
        headers: { "idempotency-key": "capture-case-1" },
        body: {
          sourceRunId: seeded.runId,
          title: "Captured regression",
          tags: ["regression"],
          rubric: { dimensions: ["correctness"] }
        }
      });
      expect(dataOf<{ case: { id: string } }>(replay).case.id).toBe(created.id);

      fixture.metadata.sessions.delete({ user_id: seeded.userId, session_id: seeded.sessionId });
      const replayAfterSourceDeletion = await request(
        fixture.context(seeded),
        "POST",
        "/api/v1/eval-cases",
        {
          headers: { "idempotency-key": "capture-case-1" },
          body: {
            sourceRunId: seeded.runId,
            title: "Captured regression",
            tags: ["regression"],
            rubric: { dimensions: ["correctness"] }
          }
        }
      );
      expect(dataOf<{ case: { id: string } }>(replayAfterSourceDeletion).case.id).toBe(created.id);

      const listed = await request(fixture.context(seeded), "GET", "/api/v1/eval-cases");
      expect(dataOf<{ cases: Array<{ id: string }> }>(listed).cases.map((record) => record.id))
        .toEqual([created.id]);
    } finally {
      fixture.close();
    }
  });

  it("manages draft suite membership and seals through revision-checked routes", async () => {
    const fixture = createFixture("suite");
    try {
      const seeded = seedEpisode(fixture.metadata, "suite");
      const context = fixture.context(seeded);
      const caseResponse = await request(context, "POST", "/api/v1/eval-cases", {
        headers: { "idempotency-key": "manual-case" },
        body: { title: "Manual", input: { prompt: "manual" } }
      });
      const caseId = dataOf<{ case: { id: string } }>(caseResponse).case.id;
      const suiteResponse = await request(context, "POST", "/api/v1/eval-suites", {
        headers: { "idempotency-key": "suite-1" },
        body: { name: "Core suite" }
      });
      const suiteId = dataOf<{ suite: { id: string } }>(suiteResponse).suite.id;

      const membership = await request(
        context,
        "PUT",
        `/api/v1/eval-suites/${suiteId}/cases/${caseId}`,
        { body: { ordinal: 0, expectedRevision: 1 } }
      );
      expect(membership?.body).toMatchObject({
        success: true,
        data: { suite: { revision: 2 }, membership: { caseId, ordinal: 0, required: true } }
      });
      const contradictoryRevision = await request(
        context,
        "POST",
        `/api/v1/eval-suites/${suiteId}/seal`,
        { headers: { "if-match": "\"2\"" }, body: { expectedRevision: 1 } }
      );
      expect(contradictoryRevision).toMatchObject({
        status: 400,
        body: { success: false, error: { code: "BAD_REQUEST" } }
      });
      const malformedIfMatch = await request(
        context,
        "POST",
        `/api/v1/eval-suites/${suiteId}/seal`,
        { headers: { "if-match": "garbage" }, body: { expectedRevision: 2 } }
      );
      expect(malformedIfMatch).toMatchObject({
        status: 400,
        body: { success: false, error: { code: "BAD_REQUEST" } }
      });
      const invalidRequired = await request(
        context,
        "PUT",
        `/api/v1/eval-suites/${suiteId}/cases/${caseId}`,
        { body: { ordinal: 0, expectedRevision: 2, required: "yes" } }
      );
      expect(invalidRequired).toMatchObject({
        status: 400,
        body: { success: false, error: { code: "BAD_REQUEST" } }
      });
      const sealed = await request(context, "POST", `/api/v1/eval-suites/${suiteId}/seal`, {
        headers: { "if-match": "\"2\"" }
      });
      expect(sealed?.body).toMatchObject({
        success: true,
        data: { suite: { status: "sealed", revision: 3 } }
      });

      const staleDelete = await request(
        context,
        "DELETE",
        `/api/v1/eval-suites/${suiteId}/cases/${caseId}`,
        { headers: { "if-match": "\"3\"" } }
      );
      expect(staleDelete).toMatchObject({
        status: 409,
        body: { success: false, error: { code: "CONFLICT" } }
      });
    } finally {
      fixture.close();
    }
  });

  it("records authenticated run-level feedback and maps idempotency conflicts", async () => {
    const fixture = createFixture("feedback");
    try {
      const seeded = seedEpisode(fixture.metadata, "feedback");
      const context = fixture.context(seeded);
      const commonBody = {
        targetKind: "run",
        feedbackKind: "rating",
        rating: 2,
        comment: "Needs work"
      };
      const created = await request(context, "POST", `/api/v1/runs/${seeded.runId}/feedback`, {
        headers: { "idempotency-key": "feedback-1" },
        body: commonBody
      });
      expect(created?.body).toMatchObject({
        success: true,
        data: {
          feedback: {
            runId: seeded.runId,
            target: { kind: "run", ref: seeded.runId },
            actor: { kind: "user", id: seeded.userId },
            sequence: 1,
            rating: 2
          }
        }
      });

      const conflict = await request(context, "POST", `/api/v1/runs/${seeded.runId}/feedback`, {
        headers: { "idempotency-key": "feedback-1" },
        body: { ...commonBody, rating: 5 }
      });
      expect(conflict).toMatchObject({
        status: 409,
        body: { success: false, error: { code: "CONFLICT" } }
      });

      const spoof = await request(context, "POST", `/api/v1/runs/${seeded.runId}/feedback`, {
        headers: { "idempotency-key": "feedback-spoof" },
        body: { ...commonBody, actorId: "another-user" }
      });
      expect(spoof).toMatchObject({
        status: 400,
        body: { success: false, error: { code: "BAD_REQUEST" } }
      });

      const unsupportedTarget = await request(context, "POST", `/api/v1/runs/${seeded.runId}/feedback`, {
        headers: { "idempotency-key": "feedback-artifact" },
        body: {
          targetKind: "artifact",
          targetRef: "artifact-1",
          feedbackKind: "comment",
          comment: "not yet attributable"
        }
      });
      expect(unsupportedTarget).toMatchObject({
        status: 400,
        body: { success: false, error: { code: "BAD_REQUEST" } }
      });

      const ratingOnComment = await request(context, "POST", `/api/v1/runs/${seeded.runId}/feedback`, {
        headers: { "idempotency-key": "feedback-comment-rating" },
        body: {
          targetKind: "run",
          feedbackKind: "comment",
          comment: "invalid rating combination",
          rating: 3
        }
      });
      expect(ratingOnComment).toMatchObject({
        status: 400,
        body: { success: false, error: { code: "BAD_REQUEST" } }
      });

      const emptyLabel = await request(context, "POST", `/api/v1/runs/${seeded.runId}/feedback`, {
        headers: { "idempotency-key": "feedback-empty-label" },
        body: { targetKind: "run", feedbackKind: "label" }
      });
      expect(emptyLabel).toMatchObject({
        status: 400,
        body: { success: false, error: { code: "BAD_REQUEST" } }
      });

      const listed = await request(context, "GET", `/api/v1/runs/${seeded.runId}/feedback`);
      expect(dataOf<{ feedback: unknown[] }>(listed).feedback).toHaveLength(1);

      const trailingSuffix = await request(
        context,
        "GET",
        `/api/v1/runs/${seeded.runId}/feedback/unexpected`
      );
      expect(trailingSuffix?.status).toBe(405);
    } finally {
      fixture.close();
    }
  });

  it("rejects an episode whose stored snapshot identity does not match its ledger row", async () => {
    const fixture = createFixture("snapshot-identity");
    try {
      const seeded = seedEpisode(fixture.metadata, "snapshot-identity", {
        snapshotRunId: "another-run"
      });
      const response = await request(fixture.context(seeded), "POST", "/api/v1/eval-cases", {
        headers: { "idempotency-key": "invalid-snapshot-case" },
        body: { sourceRunId: seeded.runId, title: "Invalid source" }
      });
      expect(response).toMatchObject({
        status: 500,
        body: { success: false, error: { code: "INTERNAL_ERROR" } }
      });
      expect(fixture.metadata.evalCases.list({
        user_id: seeded.userId,
        workspace_id: seeded.workspaceId
      })).toEqual([]);
    } finally {
      fixture.close();
    }
  });

  it("maps an oversized evolution request body to HTTP 413", async () => {
    const fixture = createFixture("oversized-body");
    try {
      const identity = createVerifiedTestIdentity(fixture.metadata);
      const response = await request(fixture.context(identity), "POST", "/api/v1/eval-cases", {
        headers: { "idempotency-key": "oversized" },
        body: { title: "Oversized", input: { prompt: "x".repeat(1024 * 1024) } }
      });
      expect(response).toMatchObject({
        status: 413,
        body: { success: false, error: { code: "BAD_REQUEST" } }
      });
    } finally {
      fixture.close();
    }
  });
});

const request = async (
  context: ConfigApiContext,
  method: string,
  url: string,
  options: { body?: Record<string, unknown>; headers?: Record<string, string> } = {}
) => handleConfigApiRequest(
  incomingRequest(method, url, options),
  new URL(url, "http://127.0.0.1").pathname,
  context
);

const incomingRequest = (
  method: string,
  url: string,
  options: { body?: Record<string, unknown>; headers?: Record<string, string> }
): IncomingMessage => Object.assign(
  Readable.from(options.body === undefined ? [] : [Buffer.from(JSON.stringify(options.body))]),
  {
    headers: {
      ...(options.body === undefined ? {} : { "content-type": "application/json" }),
      ...(options.headers ?? {})
    },
    method,
    url
  }
) as IncomingMessage;

const dataOf = <T>(response: Awaited<ReturnType<typeof handleConfigApiRequest>>): T => {
  if (!response || Buffer.isBuffer(response.body)) {
    throw new Error("Expected JSON response");
  }
  const result = response.body as ApiResult<T>;
  if (!result.success || result.data === undefined) {
    throw new Error(`Expected success response: ${JSON.stringify(result)}`);
  }
  return result.data;
};

const createFixture = (name: string): {
  close: () => void;
  context: (identity: { userId: string; workspaceId: string }) => ConfigApiContext;
  metadata: MetadataStore;
} => {
  const root = mkdtempSync(join(tmpdir(), `evolution-api-${name}-`));
  const metadata = createMetadataStore({ database_path: join(root, "metadata.sqlite") });
  return {
    metadata,
    context: (identity) => ({
      metadataStore: metadata,
      userId: identity.userId,
      workspaceId: identity.workspaceId
    } as ConfigApiContext),
    close: () => {
      metadata.close();
      rmSync(root, { recursive: true, force: true });
    }
  };
};

const seedEpisode = (
  metadata: MetadataStore,
  suffix: string,
  options: { snapshotRunId?: string } = {}
): {
  episodeId: string;
  runId: string;
  sessionId: string;
  snapshotHash: string;
  userId: string;
  workspaceId: string;
} => {
  const { userId, workspaceId } = createVerifiedTestIdentity(metadata, {
    email: `${suffix}@evolution-api.example.test`
  });
  const sessionId = `session-${suffix}`;
  const runId = `run-${suffix}`;
  metadata.sessions.create({ user_id: userId, id: sessionId, title: suffix });
  metadata.runs.create({
    user_id: userId,
    id: runId,
    session_id: sessionId,
    user_input: `question-${suffix}`,
    model_name: "test-model",
    status: "completed"
  });
  const terminal = new RunEventWriter(metadata.runEvents).write({
    user_id: userId,
    run_id: runId,
    session_id: sessionId,
    event: { type: EventType.RUN_FINISHED, threadId: sessionId, runId }
  });
  const episode = metadata.runEpisodes.append({
    user_id: userId,
    workspace_id: workspaceId,
    run_id: runId,
    schema_version: 1,
    terminal_event_seq: terminal.seq,
    snapshot: {
      schema_version: 1,
      workspace_id: workspaceId,
      terminal_event_seq: terminal.seq,
      run: {
        id: options.snapshotRunId ?? runId,
        session_id: sessionId,
        status: "completed",
        user_input: `question-${suffix}`,
        model_name: "test-model",
        started_at: "2026-08-19T00:00:00.000Z"
      },
      events: [{
        seq: 1,
        created_at: "2026-08-19T00:00:00.000Z",
        event_type: EventType.CUSTOM,
        payload: {
          name: "run.config.resolved",
          value: {
            model_name: "test-model",
            protocol: { protocolId: "analysis", protocolVersion: "1" },
            pinned_paths: ["/private/sensitive-path"],
            goal: { objective: "free-form goal with opaque credential" },
            unavailable_resources: [{ id: "resource-1", reason: "provider diagnostic secret" }]
          }
        }
      }]
    }
  });
  return {
    episodeId: episode.id,
    runId,
    sessionId,
    snapshotHash: episode.snapshot_hash,
    userId,
    workspaceId
  };
};
