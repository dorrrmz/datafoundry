import { EventType } from "@ag-ui/client";
import type { MetadataStore, RunEventRecord, RunRecord } from "@datafoundry/metadata";
import { describe, expect, it, vi } from "vitest";

import {
  buildRunEpisodeSnapshotV1,
  RUN_EPISODE_SCHEMA_VERSION,
  RunEpisodeLedger
} from "./run-episode-ledger.js";
import type { RunTerminalObservation } from "./run-finalizer.js";

const observation: RunTerminalObservation = {
  runId: "run-1",
  sessionId: "session-1",
  status: "completed",
  userId: "user-1",
  workspaceId: "workspace-1"
};

const run: RunRecord = {
  id: "run-1",
  user_id: "user-1",
  session_id: "session-1",
  request_fingerprint: "fingerprint-1",
  status: "completed",
  user_input: "analyze revenue with api_key=top-secret",
  model_name: "model-1",
  datasource_id: "datasource-1",
  started_at: "2026-08-19T01:00:00.000Z",
  finished_at: "2026-08-19T01:01:00.000Z"
};

describe("RunEpisodeLedger", () => {
  it("appends a schema-v1 snapshot through the metadata repository", () => {
    const append = vi.fn();
    const metadataStore = createMetadataStore({ append });
    const ledger = new RunEpisodeLedger(metadataStore);

    ledger.observeTerminal(observation);

    expect(append).toHaveBeenCalledOnce();
    expect(append).toHaveBeenCalledWith(expect.objectContaining({
      user_id: "user-1",
      workspace_id: "workspace-1",
      run_id: "run-1",
      schema_version: RUN_EPISODE_SCHEMA_VERSION,
      terminal_event_seq: 2,
      snapshot: expect.objectContaining({
        schema_version: RUN_EPISODE_SCHEMA_VERSION,
        terminal_event_seq: 2
      })
    }));
  });

  it("sorts events, fixes the terminal watermark, and redacts secret-shaped data", () => {
    const metadataStore = createMetadataStore();

    const snapshot = buildRunEpisodeSnapshotV1(metadataStore, observation);
    const serialized = JSON.stringify(snapshot);

    expect(snapshot.events.map((event) => event.seq)).toEqual([1, 2]);
    expect(snapshot.run.user_input).toBe("analyze revenue with api_key=[REDACTED]");
    expect(snapshot.events[0]?.payload).toEqual(expect.objectContaining({
      name: "run.config.resolved",
      value: {
        goal: { maxRuns: 3, objective: "improve the workflow" },
        model_name: "model-1",
        protocol: { protocolId: "data-analysis", protocolVersion: "1" },
        resource_revisions: { "skill:skill-1": 4 },
        skill_ids: ["skill-1"],
        skill_policy: {
          deniedToolNames: ["execute_command"],
          maxSkills: 2,
          requireUserInvocable: true,
          strictSkillTools: true
        }
      }
    }));
    expect(serialized).not.toContain("top-secret");
    expect(serialized).not.toContain("nested-secret");
    expect(serialized).not.toContain("runtime-secret");
    expect(serialized).not.toContain("opaque-token-value");
    expect(serialized).not.toContain("after-terminal");
    expect(serialized).not.toContain("unexpected_top_level");
  });

  it("never persists unknown nested config fields even when their secret key is non-standard", () => {
    const snapshot = buildRunEpisodeSnapshotV1(createMetadataStore(), observation);
    const serialized = JSON.stringify(snapshot.events[0]?.payload);

    expect(serialized).not.toContain("headers");
    expect(serialized).not.toContain("x-api-key");
    expect(serialized).not.toContain("plain-secret");
    expect(serialized).not.toContain("accessKey");
    expect(serialized).not.toContain("another-plain-secret");
    expect(serialized).not.toContain("safe-but-unknown");
  });

  it("redacts credential-bearing headers, URLs, PEM blocks, labels, and provider key prefixes", () => {
    const credentialText = [
      "Authorization: Basic dXNlcjpwYXNz",
      '{"Authorization":"Basic dXNlcjpwYXNz"}',
      "https://user:pass@host.example/v1",
      "https://opaque-token@host.example/v2",
      "-----BEGIN PRIVATE KEY-----\ncHJpdmF0ZS1rZXktYm9keQ==\n-----END PRIVATE KEY-----",
      "openai_compatible_key=opaque-value-openai-compatible-123",
      "provider_api_key is provider-opaque-secret-456",
      "ANTHROPIC_API_KEY=opaque-anthropic-key-123456",
      "AWS_SECRET_ACCESS_KEY=opaque-aws-secret-123456",
      "gsk_abcdefghijklmnopqrstuvwxyz123456",
      "hf_abcdefghijklmnopqrstuvwxyz123456",
      "AIzaSyDUMMYabcdefghijklmnopqrstuvwxyz",
      "AKIAIOSFODNN7EXAMPLE",
      "sk-ant-api03-abcdefghijklmnopqrstuvwxyz",
      "artifact_hash=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    ].join("\n");
    const scopedRun = { ...run, user_input: credentialText };

    const snapshot = buildRunEpisodeSnapshotV1(createMetadataStore({ run: scopedRun }), observation);

    expect(snapshot.run.user_input).toContain("Authorization: [REDACTED]");
    expect(snapshot.run.user_input).toContain('{"Authorization":"[REDACTED]"}');
    expect(snapshot.run.user_input).toContain("https://[REDACTED]@host.example/v1");
    expect(snapshot.run.user_input).toContain("https://[REDACTED]@host.example/v2");
    expect(snapshot.run.user_input).toContain("[REDACTED PRIVATE KEY]");
    expect(snapshot.run.user_input).toContain(
      "artifact_hash=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );
    for (const credential of [
      "dXNlcjpwYXNz",
      "user:pass",
      "cHJpdmF0ZS1rZXktYm9keQ==",
      "opaque-value-openai-compatible-123",
      "provider-opaque-secret-456",
      "opaque-anthropic-key-123456",
      "opaque-aws-secret-123456",
      "gsk_abcdefghijklmnopqrstuvwxyz123456",
      "hf_abcdefghijklmnopqrstuvwxyz123456",
      "AIzaSyDUMMYabcdefghijklmnopqrstuvwxyz",
      "AKIAIOSFODNN7EXAMPLE",
      "sk-ant-api03-abcdefghijklmnopqrstuvwxyz"
    ]) {
      expect(snapshot.run.user_input).not.toContain(credential);
    }
  });

  it("omits unknown custom values and tool argument/result data from the snapshot", () => {
    const events = [
      eventRecord(1, EventType.CUSTOM, {
        type: EventType.CUSTOM,
        name: "third-party.diagnostic",
        value: { opaque: "custom-value-secret" },
        timestamp: 1
      }),
      eventRecord(2, EventType.TOOL_CALL_ARGS, {
        type: EventType.TOOL_CALL_ARGS,
        toolCallId: "call-1",
        delta: "tool-argument-secret",
        timestamp: 2
      }),
      eventRecord(3, EventType.TOOL_CALL_RESULT, {
        type: EventType.TOOL_CALL_RESULT,
        toolCallId: "call-1",
        toolCallName: "fetch_private_data",
        content: "tool-result-secret",
        timestamp: 3
      }),
      eventRecord(4, EventType.RAW, {
        type: EventType.RAW,
        event: { providerPayload: "raw-provider-secret" },
        timestamp: 4
      }),
      eventRecord(5, EventType.RUN_FINISHED, {
        type: EventType.RUN_FINISHED,
        runId: "run-1",
        timestamp: 5
      })
    ];

    const snapshot = buildRunEpisodeSnapshotV1(createMetadataStore({ events }), observation);
    const serialized = JSON.stringify(snapshot.events);

    expect(snapshot.events[0]?.payload).toEqual({ name: "third-party.diagnostic", timestamp: 1 });
    expect(snapshot.events[1]?.payload).toEqual({ timestamp: 2, toolCallId: "call-1" });
    expect(snapshot.events[2]?.payload).toEqual({
      timestamp: 3,
      toolCallId: "call-1",
      toolCallName: "fetch_private_data"
    });
    expect(snapshot.events[3]?.payload).toEqual({ timestamp: 4 });
    expect(serialized).not.toContain("custom-value-secret");
    expect(serialized).not.toContain("tool-argument-secret");
    expect(serialized).not.toContain("tool-result-secret");
    expect(serialized).not.toContain("raw-provider-secret");
  });

  it.each([
    ["completed", EventType.RUN_FINISHED],
    ["canceled", EventType.RUN_FINISHED],
    ["failed", EventType.RUN_ERROR]
  ] as const)("requires %s runs to use a %s terminal event", (status, terminalEventType) => {
    const scopedObservation = { ...observation, status };
    const scopedRun = { ...run, status };
    const events = [eventRecord(1, terminalEventType, { type: terminalEventType, timestamp: 1 })];

    expect(buildRunEpisodeSnapshotV1(
      createMetadataStore({ events, run: scopedRun }),
      scopedObservation
    ).terminal_event_seq).toBe(1);
  });

  it("rejects a terminal event that does not match the run status", () => {
    const failedObservation = { ...observation, status: "failed" as const };
    const failedRun = { ...run, status: "failed" as const };
    const events = [eventRecord(1, EventType.RUN_FINISHED, { type: EventType.RUN_FINISHED, timestamp: 1 })];

    expect(() => buildRunEpisodeSnapshotV1(
      createMetadataStore({ events, run: failedRun }),
      failedObservation
    )).toThrow("RUN_EPISODE_TERMINAL_EVENT_MISMATCH:run-1:failed:RUN_FINISHED");
  });

  it("produces the same canonical snapshot regardless of repository event order", () => {
    const left = buildRunEpisodeSnapshotV1(createMetadataStore(), observation);
    const right = buildRunEpisodeSnapshotV1(createMetadataStore({ reverseEvents: true }), observation);

    expect(JSON.stringify(left)).toBe(JSON.stringify(right));
  });

  it("rejects a snapshot without a durable terminal event", () => {
    const metadataStore = createMetadataStore({ events: [configEvent] });

    expect(() => buildRunEpisodeSnapshotV1(metadataStore, observation)).toThrow(
      "RUN_EPISODE_TERMINAL_EVENT_REQUIRED:run-1"
    );
  });
});

const configEvent = eventRecord(1, EventType.CUSTOM, {
  type: EventType.CUSTOM,
  name: "run.config.resolved",
  value: {
    api_key: "nested-secret",
    goal: { objective: "improve the workflow", maxRuns: 3, headers: { "x-api-key": "goal-secret" } },
    headers: { "x-api-key": "plain-secret" },
    accessKey: "another-plain-secret",
    model_name: "model-1",
    protocol: { protocolId: "data-analysis", protocolVersion: "1", credential: "protocol-secret" },
    resource_revisions: { "skill:skill-1": 4 },
    safe: "safe-but-unknown",
    skill_ids: ["skill-1"],
    skill_policy: {
      deniedToolNames: ["execute_command"],
      maxSkills: 2,
      requireUserInvocable: true,
      strictSkillTools: true,
      headers: { "x-api-key": "policy-secret" }
    },
    token: "opaque-token-value"
  },
  model: { apiKey: "runtime-secret" },
  unexpected_top_level: "drop-me"
});

const terminalEvent = eventRecord(2, EventType.RUN_FINISHED, {
  type: EventType.RUN_FINISHED,
  runId: "run-1",
  timestamp: 2
});

const lateEvent = eventRecord(3, EventType.CUSTOM, {
  type: EventType.CUSTOM,
  name: "session.title",
  value: "after-terminal"
});

const createMetadataStore = (options: {
  append?: ReturnType<typeof vi.fn>;
  events?: RunEventRecord[];
  reverseEvents?: boolean;
  run?: RunRecord;
} = {}): MetadataStore => {
  const events = options.events ?? [lateEvent, configEvent, terminalEvent];
  return {
    runEpisodes: { append: options.append ?? vi.fn() },
    runEvents: {
      listByRun: vi.fn(() => options.reverseEvents ? [...events].reverse() : [...events])
    },
    runs: { get: vi.fn(() => options.run ?? run) }
  } as unknown as MetadataStore;
};

function eventRecord(seq: number, eventType: EventType, payload: Record<string, unknown>): RunEventRecord {
  return {
    id: `run-1:${seq}`,
    user_id: "user-1",
    run_id: "run-1",
    session_id: "session-1",
    seq,
    event_type: eventType,
    payload_json: JSON.stringify(payload),
    created_at: `2026-08-19T01:00:0${seq}.000Z`
  };
}
