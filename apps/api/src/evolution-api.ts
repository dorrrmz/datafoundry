import { EventType } from "@ag-ui/client";
import { createSuccessResult } from "@datafoundry/contracts";
import type {
  EvalCaseRecord,
  EvalSuiteCaseRecord,
  EvalSuiteRecord,
  MetadataStore,
  RunEpisodeFeedbackRecord,
  RunEpisodeRecord
} from "@datafoundry/metadata";
import type { IncomingMessage } from "node:http";

import type { ConfigApiContext, ConfigApiResponse } from "./routes/types.js";

const MAX_JSON_BODY_BYTES = 1024 * 1024;

type EvolutionApiContext = Pick<
  Required<ConfigApiContext>,
  "metadataStore" | "userId" | "workspaceId"
>;

/** Handle self-evolution asset routes, or return undefined for an unrelated resource. */
export const handleEvolutionApiRequest = async (
  request: IncomingMessage,
  root: string,
  segments: string[],
  context: EvolutionApiContext
): Promise<ConfigApiResponse | undefined> => {
  if (root === "eval-cases") {
    return handleEvalCaseRequest(request, segments, context);
  }
  if (root === "eval-suites") {
    return handleEvalSuiteRequest(request, segments, context);
  }
  if (root === "runs" && segments.length === 2 && segments[0] && segments[1] === "feedback") {
    return handleRunFeedbackRequest(request, segments[0], context);
  }
  return undefined;
};

const handleEvalCaseRequest = async (
  request: IncomingMessage,
  segments: string[],
  context: EvolutionApiContext
): Promise<ConfigApiResponse> => {
  const caseId = segments[0];
  if (!caseId && request.method === "GET") {
    const limit = requestLimit(request);
    return ok({
      cases: context.metadataStore.evalCases.list({
        user_id: context.userId,
        workspace_id: context.workspaceId,
        limit
      }).map(evalCaseDto)
    });
  }
  if (!caseId && request.method === "POST") {
    const body = await readJsonBody(request);
    const idempotencyKey = requiredIdempotencyKey(request);
    const sourceRunId = stringValue(body.sourceRunId) ?? stringValue(body.source_run_id);
    const requestedSourceKind = stringValue(body.sourceKind) ?? stringValue(body.source_kind);
    if (sourceRunId && requestedSourceKind && requestedSourceKind !== "episode") {
      throw new Error(`EVAL_CASE_SOURCE_KIND_INVALID:${requestedSourceKind}`);
    }
    const sourceKind = sourceRunId ? "episode" : requestedSourceKind ?? "manual";
    if (!sourceRunId && sourceKind !== "manual" && sourceKind !== "import") {
      throw new Error(`EVAL_CASE_SOURCE_KIND_INVALID:${sourceKind}`);
    }

    let episode: RunEpisodeRecord | undefined;
    let sourceEpisodeId: string | undefined;
    let derivedInput: unknown;
    if (sourceRunId) {
      const existing = context.metadataStore.evalCases.findByIdempotencyKey({
        user_id: context.userId,
        workspace_id: context.workspaceId,
        idempotency_key: idempotencyKey
      });
      if (existing) {
        sourceEpisodeId = `episode:${sourceRunId}`;
        derivedInput = parseStoredJson(existing.input_json);
      } else {
        episode = context.metadataStore.runEpisodes.findByRun({
          user_id: context.userId,
          workspace_id: context.workspaceId,
          run_id: sourceRunId
        });
        if (!episode) {
          throw new Error(`EVAL_CASE_SOURCE_EPISODE_NOT_FOUND:${sourceRunId}`);
        }
        sourceEpisodeId = episode.id;
        derivedInput = evalInputFromEpisode(episode);
      }
    } else if (!("input" in body)) {
      throw new Error("EVAL_CASE_INPUT_REQUIRED");
    }

    const title = stringValue(body.title)
      ?? (sourceRunId ? `Captured run ${sourceRunId}` : undefined);
    if (!title) {
      throw new Error("EVAL_CASE_TITLE_REQUIRED");
    }
    const tags = stringArrayValue(body.tags);
    const created = context.metadataStore.evalCases.create({
      user_id: context.userId,
      workspace_id: context.workspaceId,
      idempotency_key: idempotencyKey,
      title,
      input: sourceRunId ? derivedInput : body.input,
      ...(body.oracle !== undefined ? { oracle: body.oracle } : {}),
      ...(body.rubric !== undefined ? { rubric: body.rubric } : {}),
      ...(tags ? { tags } : {}),
      source: sourceEpisodeId
        ? { kind: "episode", episode_id: sourceEpisodeId }
        : { kind: sourceKind as "manual" | "import" }
    });
    return ok({ case: evalCaseDto(created) }, 201);
  }
  if (caseId && segments.length === 1 && request.method === "GET") {
    return ok({
      case: evalCaseDto(context.metadataStore.evalCases.get({
        user_id: context.userId,
        workspace_id: context.workspaceId,
        case_id: caseId
      }))
    });
  }
  return methodNotAllowed();
};

const handleEvalSuiteRequest = async (
  request: IncomingMessage,
  segments: string[],
  context: EvolutionApiContext
): Promise<ConfigApiResponse> => {
  const suiteId = segments[0];
  if (!suiteId && request.method === "GET") {
    return ok({
      suites: context.metadataStore.evalSuites.list({
        user_id: context.userId,
        workspace_id: context.workspaceId,
        limit: requestLimit(request)
      }).map(evalSuiteDto)
    });
  }
  if (!suiteId && request.method === "POST") {
    const body = await readJsonBody(request);
    const name = stringValue(body.name);
    if (!name) {
      throw new Error("EVAL_SUITE_NAME_REQUIRED");
    }
    const created = context.metadataStore.evalSuites.create({
      user_id: context.userId,
      workspace_id: context.workspaceId,
      idempotency_key: requiredIdempotencyKey(request),
      name,
      ...(stringValue(body.description) ? { description: stringValue(body.description)! } : {})
    });
    return ok({ suite: evalSuiteDto(created) }, 201);
  }
  if (!suiteId) {
    return methodNotAllowed();
  }

  if (segments.length === 1 && request.method === "GET") {
    const suite = context.metadataStore.evalSuites.get({
      user_id: context.userId,
      workspace_id: context.workspaceId,
      suite_id: suiteId
    });
    return ok({
      suite: {
        ...evalSuiteDto(suite),
        cases: context.metadataStore.evalSuites.listCases({
          user_id: context.userId,
          workspace_id: context.workspaceId,
          suite_id: suiteId
        }).map(evalSuiteCaseDto)
      }
    });
  }

  if (segments[1] === "cases" && segments[2] && segments.length === 3) {
    const caseId = segments[2];
    if (request.method === "PUT") {
      const body = await readJsonBody(request);
      const ordinal = integerValue(body.ordinal);
      if (ordinal === undefined) {
        throw new Error("EVAL_SUITE_ORDINAL_REQUIRED");
      }
      const result = context.metadataStore.evalSuites.addCase({
        user_id: context.userId,
        workspace_id: context.workspaceId,
        suite_id: suiteId,
        case_id: caseId,
        ordinal,
        required: booleanValue(body.required, true),
        expected_revision: requiredExpectedRevision(request, body)
      });
      return ok({
        suite: evalSuiteDto(result.suite),
        membership: evalSuiteCaseDto(result.membership)
      });
    }
    if (request.method === "DELETE") {
      const updated = context.metadataStore.evalSuites.removeCase({
        user_id: context.userId,
        workspace_id: context.workspaceId,
        suite_id: suiteId,
        case_id: caseId,
        expected_revision: requiredExpectedRevision(request)
      });
      return ok({ suite: evalSuiteDto(updated) });
    }
  }

  if (segments.length === 2 && segments[1] === "seal" && request.method === "POST") {
    const body = await readJsonBody(request);
    const sealed = context.metadataStore.evalSuites.seal({
      user_id: context.userId,
      workspace_id: context.workspaceId,
      suite_id: suiteId,
      expected_revision: requiredExpectedRevision(request, body)
    });
    return ok({ suite: evalSuiteDto(sealed) });
  }
  if (segments.length === 2 && segments[1] === "archive" && request.method === "POST") {
    const body = await readJsonBody(request);
    const archived = context.metadataStore.evalSuites.archive({
      user_id: context.userId,
      workspace_id: context.workspaceId,
      suite_id: suiteId,
      expected_revision: requiredExpectedRevision(request, body)
    });
    return ok({ suite: evalSuiteDto(archived) });
  }
  return methodNotAllowed();
};

const handleRunFeedbackRequest = async (
  request: IncomingMessage,
  runId: string,
  context: EvolutionApiContext
): Promise<ConfigApiResponse> => {
  if (request.method === "GET") {
    return ok({
      feedback: context.metadataStore.runEpisodeFeedback.listByRun({
        user_id: context.userId,
        workspace_id: context.workspaceId,
        run_id: runId,
        limit: requestLimit(request)
      }).map(runEpisodeFeedbackDto)
    });
  }
  if (request.method !== "POST") {
    return methodNotAllowed();
  }
  const body = await readJsonBody(request);
  if (body.actorId !== undefined || body.actor_id !== undefined || body.actorKind !== undefined || body.actor_kind !== undefined) {
    throw new Error("EPISODE_FEEDBACK_ACTOR_MANAGED_BY_AUTH");
  }
  const targetKind = feedbackTargetKind(body.targetKind ?? body.target_kind);
  const feedbackKind = feedbackKindValue(body.feedbackKind ?? body.feedback_kind);
  const targetRef = stringValue(body.targetRef) ?? stringValue(body.target_ref)
    ?? (targetKind === "run" ? runId : undefined);
  if (!targetRef) {
    throw new Error("EPISODE_FEEDBACK_TARGET_REQUIRED");
  }
  const sentiment = feedbackSentiment(body.sentiment);
  const rating = integerValue(body.rating);
  const supersedesFeedbackId = stringValue(body.supersedesFeedbackId)
    ?? stringValue(body.supersedes_feedback_id);
  const created = context.metadataStore.runEpisodeFeedback.append({
    user_id: context.userId,
    workspace_id: context.workspaceId,
    run_id: runId,
    idempotency_key: requiredIdempotencyKey(request),
    target_kind: targetKind,
    target_ref: targetRef,
    feedback_kind: feedbackKind,
    ...(sentiment ? { sentiment } : {}),
    ...(rating !== undefined ? { rating } : {}),
    ...(stringValue(body.comment) ?? stringValue(body.commentText) ?? stringValue(body.comment_text)
      ? { comment_text: stringValue(body.comment) ?? stringValue(body.commentText) ?? stringValue(body.comment_text)! }
      : {}),
    ...(body.details !== undefined ? { details: body.details } : {}),
    ...(body.evidenceRefs !== undefined
      ? { evidence_refs: body.evidenceRefs }
      : body.evidence_refs !== undefined
        ? { evidence_refs: body.evidence_refs }
        : {}),
    actor_kind: "user",
    actor_id: context.userId,
    ...(supersedesFeedbackId ? { supersedes_feedback_id: supersedesFeedbackId } : {})
  });
  return ok({ feedback: runEpisodeFeedbackDto(created) }, 201);
};

const evalInputFromEpisode = (episode: RunEpisodeRecord): Record<string, unknown> => {
  const snapshot = parseJsonRecord(episode.snapshot_json, `EVAL_CASE_SOURCE_SNAPSHOT_INVALID:${episode.id}`);
  const run = recordValue(snapshot.run);
  if (
    snapshot.schema_version !== episode.schema_version
    || episode.schema_version !== 1
    || snapshot.workspace_id !== episode.workspace_id
    || snapshot.terminal_event_seq !== episode.terminal_event_seq
    || !run
    || run.id !== episode.run_id
    || run.session_id !== episode.session_id
    || run.status !== episode.terminal_status
    || typeof run.user_input !== "string"
  ) {
    throw new Error(`EVAL_CASE_SOURCE_SNAPSHOT_INVALID:${episode.id}`);
  }
  const context: Record<string, unknown> = {};
  copyString(run, context, "model_provider", "modelProvider");
  copyString(run, context, "model_name", "modelName");
  copyString(run, context, "datasource_id", "datasourceId");
  copyString(run, context, "collection_id", "collectionId");

  const events = Array.isArray(snapshot.events) ? snapshot.events : [];
  const resolvedConfig = customEventValue(events, "run.config.resolved");
  const evalRunConfig = resolvedConfig ? projectEvalRunConfig(resolvedConfig) : undefined;
  if (evalRunConfig) {
    context.runConfig = evalRunConfig;
  }
  const skillSelection = customEventValue(events, "skill.selection");
  const evalSkillSelection = skillSelection ? projectEvalSkillSelection(skillSelection) : undefined;
  if (evalSkillSelection) {
    context.skillSelection = evalSkillSelection;
  }
  return {
    prompt: run.user_input,
    ...(Object.keys(context).length > 0 ? { context } : {})
  };
};

const customEventValue = (events: unknown[], name: string): Record<string, unknown> | undefined => {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = recordValue(events[index]);
    if (!event || event.event_type !== EventType.CUSTOM) {
      continue;
    }
    const payload = recordValue(event.payload);
    if (payload?.name === name) {
      return recordValue(payload.value);
    }
  }
  return undefined;
};

/** A second, eval-purpose projection; never retain free-form diagnostics or paths. */
const projectEvalRunConfig = (source: Record<string, unknown>): Record<string, unknown> | undefined => {
  const result: Record<string, unknown> = {};
  for (const key of [
    "active_datasource_id",
    "active_skill_id",
    "requested_llm_profile_id",
    "active_llm_profile_id",
    "model_name",
    "skill_mode"
  ]) {
    copySameString(source, result, key);
  }
  for (const key of [
    "enabled_datasource_ids",
    "enabled_knowledge_ids",
    "enabled_mcp_server_ids",
    "enabled_skill_ids",
    "selected_skill_ids",
    "skill_ids"
  ]) {
    copyStringArray(source, result, key);
  }
  for (const key of ["context_window", "input_budget", "run_timeout_ms"]) {
    copyFiniteNumber(source, result, key);
  }
  copyBoolean(source, result, "reasoning_model");

  const protocol = recordValue(source.protocol);
  if (protocol) {
    const projected: Record<string, unknown> = {};
    copySameString(protocol, projected, "protocolId");
    copySameString(protocol, projected, "protocolVersion");
    if (Object.keys(projected).length > 0) {
      result.protocol = projected;
    }
  }
  const modelSettings = recordValue(source.model_settings);
  if (modelSettings) {
    const projected: Record<string, unknown> = {};
    for (const key of ["frequencyPenalty", "maxOutputTokens", "presencePenalty", "temperature", "topP"]) {
      copyFiniteNumber(modelSettings, projected, key);
    }
    if (Object.keys(projected).length > 0) {
      result.model_settings = projected;
    }
  }
  const resourceRevisions = recordValue(source.resource_revisions);
  if (resourceRevisions) {
    const projected = Object.fromEntries(
      Object.entries(resourceRevisions)
        .filter((entry): entry is [string, number] => typeof entry[1] === "number" && Number.isFinite(entry[1]))
        .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
    );
    if (Object.keys(projected).length > 0) {
      result.resource_revisions = projected;
    }
  }
  return Object.keys(result).length > 0 ? result : undefined;
};

const projectEvalSkillSelection = (source: Record<string, unknown>): Record<string, unknown> | undefined => {
  const result: Record<string, unknown> = {};
  copySameString(source, result, "mode");
  if (Array.isArray(source.selected)) {
    const selected = source.selected.flatMap((entry): Record<string, unknown>[] => {
      const item = recordValue(entry);
      if (!item) {
        return [];
      }
      const projected: Record<string, unknown> = {};
      copySameString(item, projected, "id");
      copyFiniteNumber(item, projected, "revision");
      return typeof projected.id === "string" ? [projected] : [];
    });
    if (selected.length > 0) {
      result.selected = selected;
    }
  }
  return Object.keys(result).length > 0 ? result : undefined;
};

const evalCaseDto = (record: EvalCaseRecord): Record<string, unknown> => ({
  id: record.id,
  schemaVersion: record.schema_version,
  title: record.title,
  input: parseStoredJson(record.input_json),
  ...(record.oracle_json ? { oracle: parseStoredJson(record.oracle_json) } : {}),
  ...(record.rubric_json ? { rubric: parseStoredJson(record.rubric_json) } : {}),
  tags: parseStoredJson(record.tags_json),
  source: {
    kind: record.source_kind,
    ...(record.source_episode_id ? { episodeId: record.source_episode_id } : {}),
    ...(record.source_snapshot_hash ? { snapshotHash: record.source_snapshot_hash } : {})
  },
  definitionHash: record.definition_hash,
  createdAt: record.created_at
});

const evalSuiteDto = (record: EvalSuiteRecord): Record<string, unknown> => ({
  id: record.id,
  name: record.name,
  ...(record.description ? { description: record.description } : {}),
  status: record.status,
  revision: record.revision,
  ...(record.manifest_hash ? { manifestHash: record.manifest_hash } : {}),
  createdAt: record.created_at,
  updatedAt: record.updated_at,
  ...(record.sealed_at ? { sealedAt: record.sealed_at } : {}),
  ...(record.archived_at ? { archivedAt: record.archived_at } : {})
});

const evalSuiteCaseDto = (record: EvalSuiteCaseRecord): Record<string, unknown> => ({
  caseId: record.case_id,
  ordinal: record.ordinal,
  required: record.required,
  createdAt: record.created_at
});

const runEpisodeFeedbackDto = (record: RunEpisodeFeedbackRecord): Record<string, unknown> => ({
  id: record.id,
  runId: record.run_id,
  episodeId: record.episode_id,
  sequence: record.sequence,
  target: { kind: record.target_kind, ref: record.target_ref },
  kind: record.feedback_kind,
  ...(record.sentiment ? { sentiment: record.sentiment } : {}),
  ...(record.rating !== undefined ? { rating: record.rating } : {}),
  ...(record.comment_text ? { comment: record.comment_text } : {}),
  ...(record.details_json ? { details: parseStoredJson(record.details_json) } : {}),
  ...(record.evidence_refs_json ? { evidenceRefs: parseStoredJson(record.evidence_refs_json) } : {}),
  actor: { kind: record.actor_kind, id: record.actor_id },
  ...(record.supersedes_feedback_id ? { supersedesFeedbackId: record.supersedes_feedback_id } : {}),
  createdAt: record.created_at
});

const requiredIdempotencyKey = (request: IncomingMessage): string => {
  const value = headerValue(request.headers["idempotency-key"]);
  if (!value) {
    throw new Error("IDEMPOTENCY_KEY_REQUIRED");
  }
  return value;
};

const requiredExpectedRevision = (
  request: IncomingMessage,
  body: Record<string, unknown> = {}
): number => {
  const bodyRevision = integerValue(body.expectedRevision ?? body.expected_revision);
  const ifMatch = headerValue(request.headers["if-match"]);
  const headerRevision = parseIfMatch(ifMatch);
  if (ifMatch && headerRevision === undefined) {
    throw new Error(`EVAL_SUITE_EXPECTED_REVISION_INVALID:${ifMatch}`);
  }
  if (bodyRevision !== undefined && headerRevision !== undefined && bodyRevision !== headerRevision) {
    throw new Error(`EVAL_SUITE_EXPECTED_REVISION_INVALID:${bodyRevision}:${headerRevision}`);
  }
  const revision = bodyRevision ?? headerRevision;
  if (revision === undefined || revision < 1) {
    throw new Error("EVAL_SUITE_EXPECTED_REVISION_REQUIRED");
  }
  return revision;
};

const parseIfMatch = (value: string | undefined): number | undefined => {
  if (!value) {
    return undefined;
  }
  const match = /^"([1-9]\d*)"$/.exec(value);
  if (!match?.[1]) {
    return undefined;
  }
  const parsed = Number.parseInt(match[1], 10);
  return Number.isSafeInteger(parsed) ? parsed : undefined;
};

const requestLimit = (request: IncomingMessage): number => {
  const url = new URL(request.url ?? "/", "http://127.0.0.1");
  const parsed = Number.parseInt(url.searchParams.get("limit") ?? "", 10);
  return Number.isInteger(parsed) ? Math.min(500, Math.max(1, parsed)) : 100;
};

const readJsonBody = async (request: IncomingMessage): Promise<Record<string, unknown>> => {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > MAX_JSON_BODY_BYTES) {
      throw new Error("REQUEST_BODY_TOO_LARGE");
    }
    chunks.push(buffer);
  }
  if (chunks.length === 0) {
    return {};
  }
  const parsed: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  if (!isRecord(parsed)) {
    throw new Error("JSON_OBJECT_REQUIRED");
  }
  return parsed;
};

const ok = (data: unknown, status = 200): ConfigApiResponse => ({
  body: createSuccessResult(data),
  status
});

const methodNotAllowed = (): ConfigApiResponse => ({
  body: { error: { code: "BAD_REQUEST", message: "Method not allowed." }, success: false },
  status: 405
});

const parseJsonRecord = (value: string, errorCode: string): Record<string, unknown> => {
  try {
    const parsed: unknown = JSON.parse(value);
    if (isRecord(parsed)) {
      return parsed;
    }
  } catch {
    // Stable domain error below.
  }
  throw new Error(errorCode);
};

const parseStoredJson = (value: string): unknown => JSON.parse(value) as unknown;
const isRecord = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null;
const recordValue = (value: unknown): Record<string, unknown> | undefined => isRecord(value) ? value : undefined;
const stringValue = (value: unknown): string | undefined => typeof value === "string" && value.trim() ? value.trim() : undefined;
const integerValue = (value: unknown): number | undefined => typeof value === "number" && Number.isInteger(value) ? value : undefined;
const booleanValue = (value: unknown, fallback: boolean): boolean => {
  if (value === undefined) {
    return fallback;
  }
  if (typeof value !== "boolean") {
    throw new Error(`EVAL_SUITE_REQUIRED_INVALID:${String(value)}`);
  }
  return value;
};
const headerValue = (value: string | string[] | undefined): string | undefined => {
  const candidate = Array.isArray(value) ? value[0] : value;
  return stringValue(candidate);
};

const stringArrayValue = (value: unknown): string[] | undefined => {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string")) {
    throw new Error("EVAL_CASE_TAGS_INVALID");
  }
  return value;
};

const feedbackTargetKind = (value: unknown): "run" => {
  if (value === "run") {
    return value;
  }
  if (value === "message" || value === "artifact" || value === "tool_call" || value === "step") {
    throw new Error(`EPISODE_FEEDBACK_TARGET_KIND_UNSUPPORTED:${value}`);
  }
  throw new Error(`EPISODE_FEEDBACK_TARGET_KIND_INVALID:${String(value)}`);
};

const feedbackKindValue = (value: unknown): "thumb" | "rating" | "comment" | "correction" | "label" => {
  if (value === "thumb" || value === "rating" || value === "comment" || value === "correction" || value === "label") {
    return value;
  }
  throw new Error(`EPISODE_FEEDBACK_KIND_INVALID:${String(value)}`);
};

const feedbackSentiment = (value: unknown): "positive" | "negative" | "neutral" | "mixed" | undefined => {
  if (value === undefined) {
    return undefined;
  }
  if (value === "positive" || value === "negative" || value === "neutral" || value === "mixed") {
    return value;
  }
  throw new Error(`EPISODE_FEEDBACK_SENTIMENT_INVALID:${String(value)}`);
};

const copyString = (
  source: Record<string, unknown>,
  target: Record<string, unknown>,
  sourceKey: string,
  targetKey: string
): void => {
  if (typeof source[sourceKey] === "string") {
    target[targetKey] = source[sourceKey];
  }
};

const copySameString = (
  source: Record<string, unknown>,
  target: Record<string, unknown>,
  key: string
): void => copyString(source, target, key, key);

const copyStringArray = (
  source: Record<string, unknown>,
  target: Record<string, unknown>,
  key: string
): void => {
  const value = source[key];
  if (Array.isArray(value) && value.every((entry) => typeof entry === "string")) {
    target[key] = [...value];
  }
};

const copyFiniteNumber = (
  source: Record<string, unknown>,
  target: Record<string, unknown>,
  key: string
): void => {
  const value = source[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    target[key] = value;
  }
};

const copyBoolean = (
  source: Record<string, unknown>,
  target: Record<string, unknown>,
  key: string
): void => {
  const value = source[key];
  if (typeof value === "boolean") {
    target[key] = value;
  }
};
