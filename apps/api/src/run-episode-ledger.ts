import { EventType } from "@ag-ui/client";
import type { MetadataStore, RunEventRecord, RunRecord } from "@datafoundry/metadata";

import type { RunTerminalObservation, RunTerminalObserver } from "./run-finalizer.js";

export const RUN_EPISODE_SCHEMA_VERSION = 1 as const;

type CanonicalValue = null | boolean | number | string | CanonicalValue[] | CanonicalObject;

type CanonicalObject = { [key: string]: CanonicalValue };

export type RunEpisodeSnapshotV1 = {
  events: Array<{
    created_at: string;
    event_type: string;
    payload?: CanonicalObject;
    payload_invalid?: true;
    seq: number;
  }>;
  run: {
    collection_id?: string;
    datasource_id?: string;
    error_message?: string;
    finished_at?: string;
    id: string;
    model_name?: string;
    model_provider?: string;
    parent_run_id?: string;
    request_fingerprint?: string;
    session_id: string;
    started_at: string;
    status: RunTerminalObservation["status"];
    user_input: string;
  };
  schema_version: typeof RUN_EPISODE_SCHEMA_VERSION;
  terminal_event_seq: number;
  workspace_id: string;
};

const REDACTED = "[REDACTED]";

const TEXT_DELTA_EVENT_TYPES = new Set<string>([
  EventType.TEXT_MESSAGE_CONTENT,
  EventType.TEXT_MESSAGE_CHUNK,
  EventType.REASONING_MESSAGE_CONTENT,
  EventType.REASONING_MESSAGE_CHUNK,
  EventType.THINKING_TEXT_MESSAGE_CONTENT
]);

const MESSAGE_START_EVENT_TYPES = new Set<string>([
  EventType.TEXT_MESSAGE_START,
  EventType.REASONING_MESSAGE_START,
  EventType.THINKING_TEXT_MESSAGE_START
]);

const MESSAGE_END_EVENT_TYPES = new Set<string>([
  EventType.TEXT_MESSAGE_END,
  EventType.REASONING_MESSAGE_END,
  EventType.THINKING_TEXT_MESSAGE_END
]);

const TOOL_EVENT_TYPES = new Set<string>([
  EventType.TOOL_CALL_START,
  EventType.TOOL_CALL_ARGS,
  EventType.TOOL_CALL_END,
  EventType.TOOL_CALL_CHUNK,
  EventType.TOOL_CALL_RESULT
]);

/** Build and append the canonical immutable record for one terminal run. */
export class RunEpisodeLedger implements RunTerminalObserver {
  constructor(private readonly metadataStore: MetadataStore) {}

  observeTerminal(input: RunTerminalObservation): void {
    const snapshot = buildRunEpisodeSnapshotV1(this.metadataStore, input);
    this.metadataStore.runEpisodes.append({
      user_id: input.userId,
      workspace_id: input.workspaceId,
      run_id: input.runId,
      schema_version: RUN_EPISODE_SCHEMA_VERSION,
      terminal_event_seq: snapshot.terminal_event_seq,
      snapshot
    });
  }
}

/** Pure snapshot assembly over the durable run row and its ordered event ledger. */
export const buildRunEpisodeSnapshotV1 = (
  metadataStore: Pick<MetadataStore, "runEvents" | "runs">,
  input: RunTerminalObservation
): RunEpisodeSnapshotV1 => {
  const run = metadataStore.runs.get({ user_id: input.userId, run_id: input.runId });
  assertTerminalRunScope(run, input);

  const orderedEvents = [...metadataStore.runEvents.listByRun({
    user_id: input.userId,
    run_id: input.runId
  })].sort((left, right) => left.seq - right.seq);
  const terminalEventSeq = findTerminalEventSeq(orderedEvents, input.runId, input.status);

  return {
    events: orderedEvents
      .filter((event) => event.seq <= terminalEventSeq)
      .map(eventSnapshot),
    run: runSnapshot(run, input.status),
    schema_version: RUN_EPISODE_SCHEMA_VERSION,
    terminal_event_seq: terminalEventSeq,
    workspace_id: input.workspaceId
  };
};

const assertTerminalRunScope = (run: RunRecord, input: RunTerminalObservation): void => {
  if (run.session_id !== input.sessionId) {
    throw new Error(`RUN_EPISODE_SESSION_MISMATCH:${input.runId}`);
  }
  if (run.status !== input.status) {
    throw new Error(`RUN_EPISODE_STATUS_MISMATCH:${input.runId}:${run.status}:${input.status}`);
  }
};

const findTerminalEventSeq = (
  events: RunEventRecord[],
  runId: string,
  status: RunTerminalObservation["status"]
): number => {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index];
    if (event && (event.event_type === EventType.RUN_FINISHED || event.event_type === EventType.RUN_ERROR)) {
      const expectedType = status === "failed" ? EventType.RUN_ERROR : EventType.RUN_FINISHED;
      if (event.event_type !== expectedType) {
        throw new Error(`RUN_EPISODE_TERMINAL_EVENT_MISMATCH:${runId}:${status}:${event.event_type}`);
      }
      return event.seq;
    }
  }
  throw new Error(`RUN_EPISODE_TERMINAL_EVENT_REQUIRED:${runId}`);
};

const runSnapshot = (
  run: RunRecord,
  status: RunTerminalObservation["status"]
): RunEpisodeSnapshotV1["run"] => ({
  id: run.id,
  session_id: run.session_id,
  ...(run.parent_run_id ? { parent_run_id: redactSensitiveText(run.parent_run_id) } : {}),
  ...(run.request_fingerprint ? { request_fingerprint: run.request_fingerprint } : {}),
  status,
  user_input: redactSensitiveText(run.user_input),
  ...(run.model_provider ? { model_provider: redactSensitiveText(run.model_provider) } : {}),
  ...(run.model_name ? { model_name: redactSensitiveText(run.model_name) } : {}),
  ...(run.datasource_id ? { datasource_id: redactSensitiveText(run.datasource_id) } : {}),
  ...(run.collection_id ? { collection_id: redactSensitiveText(run.collection_id) } : {}),
  started_at: run.started_at,
  ...(run.finished_at ? { finished_at: run.finished_at } : {}),
  ...(run.error_message ? { error_message: redactSensitiveText(run.error_message) } : {})
});

const eventSnapshot = (event: RunEventRecord): RunEpisodeSnapshotV1["events"][number] => {
  const parsed = parseRecord(event.payload_json);
  if (!parsed) {
    return {
      created_at: event.created_at,
      event_type: String(event.event_type),
      payload_invalid: true,
      seq: event.seq
    };
  }

  const payload = eventPayloadSnapshot(String(event.event_type), parsed);

  return {
    created_at: event.created_at,
    event_type: String(event.event_type),
    ...(Object.keys(payload).length > 0 ? { payload: sortObject(payload) } : {}),
    seq: event.seq
  };
};

/**
 * Project a durable event representation by protocol event type. Never copy an
 * arbitrary nested value: AG-UI custom/state/tool payloads can contain credentials
 * or provider runtime objects whose field names are not predictable.
 */
const eventPayloadSnapshot = (eventType: string, parsed: Record<string, unknown>): CanonicalObject => {
  const payload: CanonicalObject = {};
  copyFiniteNumber(parsed, payload, "timestamp");

  if (eventType === EventType.RUN_STARTED || eventType === EventType.RUN_FINISHED) {
    copyString(parsed, payload, "runId");
  } else if (eventType === EventType.RUN_ERROR) {
    copyString(parsed, payload, "runId");
    copyString(parsed, payload, "message");
  } else if (eventType === EventType.STEP_STARTED || eventType === EventType.STEP_FINISHED) {
    copyString(parsed, payload, "stepName");
  } else if (MESSAGE_START_EVENT_TYPES.has(eventType)) {
    copyString(parsed, payload, "messageId");
    copyString(parsed, payload, "role");
  } else if (TEXT_DELTA_EVENT_TYPES.has(eventType)) {
    copyString(parsed, payload, "messageId");
    copyString(parsed, payload, "delta");
  } else if (MESSAGE_END_EVENT_TYPES.has(eventType)) {
    copyString(parsed, payload, "messageId");
  } else if (TOOL_EVENT_TYPES.has(eventType)) {
    copyString(parsed, payload, "toolCallId");
    copyString(parsed, payload, "toolCallName");
    copyString(parsed, payload, "parentMessageId");
    copyString(parsed, payload, "messageId");
    copyString(parsed, payload, "role");
    // Tool args, chunks, and results are intentionally omitted: they are an
    // unbounded data plane and frequently contain connection material.
  } else if (eventType === EventType.STATE_SNAPSHOT) {
    const snapshot = projectStateSnapshot(parsed.snapshot);
    if (snapshot) {
      payload.snapshot = snapshot;
    }
  } else if (eventType === EventType.STATE_DELTA) {
    const delta = projectStateDelta(parsed.delta);
    if (delta) {
      payload.delta = delta;
    }
    copyString(parsed, payload, "runId");
  } else if (eventType === EventType.CUSTOM) {
    copyString(parsed, payload, "name");
    const name = typeof parsed.name === "string" ? parsed.name : undefined;
    const value = name === "run.config.resolved"
      ? projectResolvedRunConfig(parsed.value)
      : name === "skill.selection"
        ? projectSkillSelection(parsed.value)
        : undefined;
    if (value) {
      payload.value = value;
    }
  }

  return payload;
};

const projectResolvedRunConfig = (value: unknown): CanonicalObject | undefined => {
  if (!isRecord(value)) {
    return undefined;
  }

  const result: CanonicalObject = {};
  for (const key of [
    "active_datasource_id",
    "active_skill_id",
    "requested_llm_profile_id",
    "active_llm_profile_id",
    "model_name",
    "skill_mode",
    "workspace_id"
  ]) {
    copyString(value, result, key);
  }
  for (const key of [
    "enabled_datasource_ids",
    "file_ids",
    "enabled_knowledge_ids",
    "enabled_mcp_server_ids",
    "enabled_skill_ids",
    "skill_ids",
    "skill_tags",
    "selected_skill_ids",
    "pinned_paths"
  ]) {
    copyStringArray(value, result, key);
  }
  for (const key of ["context_window", "input_budget", "run_timeout_ms"]) {
    copyFiniteNumber(value, result, key);
  }
  copyBoolean(value, result, "reasoning_model");

  const resourceRevisions = projectNumberMap(value.resource_revisions);
  if (resourceRevisions) {
    result.resource_revisions = resourceRevisions;
  }

  const protocol = projectRecord(value.protocol, (source, target) => {
    copyString(source, target, "protocolId");
    copyString(source, target, "protocolVersion");
  });
  if (protocol) {
    result.protocol = protocol;
  }

  const goal = projectRecord(value.goal, (source, target) => {
    copyString(source, target, "objective");
    copyFiniteNumber(source, target, "maxRuns");
  });
  if (goal) {
    result.goal = goal;
  }

  const skillPolicy = projectRecord(value.skill_policy, (source, target) => {
    copyStringArray(source, target, "allowedToolNames");
    copyStringArray(source, target, "deniedToolNames");
    copyFiniteNumber(source, target, "maxSkills");
    copyBoolean(source, target, "requireUserInvocable");
    copyBoolean(source, target, "strictSkillTools");
  });
  if (skillPolicy) {
    result.skill_policy = skillPolicy;
  }

  const modelSettings = projectRecord(value.model_settings, (source, target) => {
    for (const key of ["frequencyPenalty", "maxOutputTokens", "presencePenalty", "temperature", "topP"]) {
      copyFiniteNumber(source, target, key);
    }
  });
  if (modelSettings) {
    result.model_settings = modelSettings;
  }

  const workspace = projectRecord(value.workspace, (source, target) => {
    copyBoolean(source, target, "command_execution_enabled");
    copyString(source, target, "isolation");
  });
  if (workspace) {
    result.workspace = workspace;
  }

  const mentioned = projectRecord(value.mentioned, (source, target) => {
    for (const key of ["db", "kb", "mcp", "skill"]) {
      copyStringArray(source, target, key);
    }
    const excluded = projectRecordArray(source.excluded, (item, projected) => {
      copyString(item, projected, "kind");
      copyString(item, projected, "id");
    });
    if (excluded) {
      target.excluded = excluded;
    }
  });
  if (mentioned) {
    result.mentioned = mentioned;
  }

  const disabledByPolicy = projectRecordArray(value.disabled_by_policy, (source, target) => {
    copyString(source, target, "kind");
    copyString(source, target, "id");
  });
  if (disabledByPolicy) {
    result.disabled_by_policy = disabledByPolicy;
  }

  const unavailableResources = projectRecordArray(value.unavailable_resources, (source, target) => {
    copyString(source, target, "kind");
    copyString(source, target, "id");
    copyString(source, target, "reason");
  });
  if (unavailableResources) {
    result.unavailable_resources = unavailableResources;
  }

  return Object.keys(result).length > 0 ? sortObject(result) : undefined;
};

const projectSkillSelection = (value: unknown): CanonicalObject | undefined => {
  if (!isRecord(value)) {
    return undefined;
  }

  const result: CanonicalObject = {};
  copyString(value, result, "mode");

  const selected = projectRecordArray(value.selected, (source, target) => {
    copyString(source, target, "id");
    copyString(source, target, "name");
    copyFiniteNumber(source, target, "revision");
    copyStringArray(source, target, "tags");
  });
  if (selected) {
    result.selected = selected;
  }

  const effectiveToolPolicy = projectRecord(value.effective_tool_policy, (source, target) => {
    copyStringArray(source, target, "allowedTools");
    copyStringArray(source, target, "deniedTools");
    copyString(source, target, "mergeStrategy");
  });
  if (effectiveToolPolicy) {
    result.effective_tool_policy = effectiveToolPolicy;
  }

  return Object.keys(result).length > 0 ? sortObject(result) : undefined;
};

const projectStateSnapshot = (value: unknown): CanonicalObject | undefined => projectRecord(
  value,
  (source, target) => {
    copyString(source, target, "selectedDatasourceId");
    copyString(source, target, "runId");
    copyString(source, target, "runStatus");
    copyString(source, target, "sessionId");
  }
);

const projectStateDelta = (value: unknown): CanonicalValue[] | undefined => {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const projected = value.flatMap((item): CanonicalObject[] => {
    if (!isRecord(item) || (item.path !== "/runStatus" && item.path !== "/errorMessage")) {
      return [];
    }
    if (item.op !== "add" && item.op !== "replace" && item.op !== "remove") {
      return [];
    }
    const entry: CanonicalObject = { op: item.op, path: item.path };
    if (item.op !== "remove" && typeof item.value === "string") {
      entry.value = redactSensitiveText(item.value);
    }
    return [entry];
  });
  return projected.length > 0 ? projected : undefined;
};

const projectNumberMap = (value: unknown): CanonicalObject | undefined => {
  if (!isRecord(value)) {
    return undefined;
  }
  const result: CanonicalObject = {};
  for (const key of Object.keys(value).sort(compareKeys)) {
    const item = value[key];
    if (typeof item === "number" && Number.isFinite(item)) {
      result[redactSensitiveText(key)] = item;
    }
  }
  return Object.keys(result).length > 0 ? result : undefined;
};

const projectRecord = (
  value: unknown,
  project: (source: Record<string, unknown>, target: CanonicalObject) => void
): CanonicalObject | undefined => {
  if (!isRecord(value)) {
    return undefined;
  }
  const target: CanonicalObject = {};
  project(value, target);
  return Object.keys(target).length > 0 ? sortObject(target) : undefined;
};

const projectRecordArray = (
  value: unknown,
  project: (source: Record<string, unknown>, target: CanonicalObject) => void
): CanonicalObject[] | undefined => {
  if (!Array.isArray(value)) {
    return undefined;
  }
  const result = value.flatMap((item): CanonicalObject[] => {
    const projected = projectRecord(item, project);
    return projected ? [projected] : [];
  });
  return result.length > 0 ? result : undefined;
};

const copyString = (source: Record<string, unknown>, target: CanonicalObject, key: string): void => {
  const value = source[key];
  if (typeof value === "string") {
    target[key] = redactSensitiveText(value);
  }
};

const copyStringArray = (source: Record<string, unknown>, target: CanonicalObject, key: string): void => {
  const value = source[key];
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
    return;
  }
  target[key] = value.map(redactSensitiveText);
};

const copyFiniteNumber = (source: Record<string, unknown>, target: CanonicalObject, key: string): void => {
  const value = source[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    target[key] = value;
  }
};

const copyBoolean = (source: Record<string, unknown>, target: CanonicalObject, key: string): void => {
  const value = source[key];
  if (typeof value === "boolean") {
    target[key] = value;
  }
};

const sortObject = (value: CanonicalObject): CanonicalObject => Object.fromEntries(
  Object.entries(value).sort(([left], [right]) => compareKeys(left, right))
) as CanonicalObject;

const compareKeys = (left: string, right: string): number => left < right ? -1 : left > right ? 1 : 0;

/**
 * Redact credentials with recognizable context or provider prefixes. Arbitrary
 * context-free opaque strings cannot be identified safely without also deleting
 * normal hashes and resource IDs, so nested unbounded payloads are excluded above.
 */
const redactSensitiveText = (value: string): string => value
  // Replace the whole PEM block, including its multi-line body.
  .replace(
    /-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----/gu,
    "[REDACTED PRIVATE KEY]"
  )
  // Authorization schemes must be handled before the generic key/value rule so
  // it cannot redact only "Basic" while leaving the base64 credential behind.
  .replace(
    /(\b(?:Proxy-)?Authorization\b["']?\s*[:=]\s*["']?)Basic\s+[A-Za-z0-9+/]+={0,2}/giu,
    `$1${REDACTED}`
  )
  .replace(
    /(\b(?:Proxy-)?Authorization\b["']?\s*[:=]\s*["']?)Bearer\s+[A-Za-z0-9._~+/=-]+/giu,
    `$1${REDACTED}`
  )
  .replace(/\bBearer\s+[A-Za-z0-9._~+/=-]+/giu, `Bearer ${REDACTED}`)
  // Any URL userinfo (username-only or username:password) is credential-bearing;
  // retain the safe scheme and host only.
  .replace(/\b([a-z][a-z0-9+.-]*:\/\/)[^/@\s]+@/giu, `$1${REDACTED}@`)
  // Provider-specific tokens with distinctive prefixes. Deliberately avoid a
  // generic "long string" rule because episode IDs and fingerprints are opaque.
  .replace(/\bsk-(?:ant-)?[A-Za-z0-9_-]{8,}\b/gu, REDACTED)
  .replace(/\b(?:gsk|hf)_[A-Za-z0-9_-]{8,}\b/gu, REDACTED)
  .replace(/\bAIza[0-9A-Za-z_-]{20,}\b/gu, REDACTED)
  .replace(/\bAKIA[0-9A-Z]{16}\b/gu, REDACTED)
  .replace(/\b(?:ghp_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|xox[baprs]-[A-Za-z0-9-]{10,})\b/gu, REDACTED)
  // Opaque values are redacted only when a credential label provides context.
  .replace(
    /(\b(?:(?:[a-z][a-z0-9]*)[_-]+)*(?:openai[_\s-]*compatible[_\s-]*(?:api[_\s-]*)?key|provider[_\s-]*api[_\s-]*key|x[_\s-]*api[_\s-]*key|api[_\s-]*key|secret[_\s-]*access[_\s-]*key|access[_\s-]*key|authorization|auth[_\s-]*token|password|private[_\s-]*key|refresh[_\s-]*token|secret|session[_\s-]*token|token)\b\s*(?:["']?\s*[:=]\s*["']?|\s+is\s+["']?))[^,\s;}"']+/giu,
    `$1${REDACTED}`
  );

const parseRecord = (value: string): Record<string, unknown> | undefined => {
  try {
    const parsed: unknown = JSON.parse(value);
    return isRecord(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
