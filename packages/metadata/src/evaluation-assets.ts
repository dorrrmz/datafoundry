import { createHash, randomUUID } from "node:crypto";
import type { DatabaseSync } from "node:sqlite";

export const EVAL_CASE_SCHEMA_VERSION = 1 as const;

export type EvaluationScope = {
  user_id: string;
  workspace_id: string;
};

export type EvalCaseSource =
  | { kind: "episode"; episode_id: string }
  | { kind: "manual" }
  | { kind: "import" };

export type EvalCaseRecord = {
  readonly id: string;
  readonly user_id: string;
  readonly workspace_id: string;
  readonly schema_version: number;
  readonly title: string;
  readonly input_json: string;
  readonly oracle_json?: string;
  readonly rubric_json?: string;
  readonly tags_json: string;
  readonly source_kind: EvalCaseSource["kind"];
  readonly source_episode_id?: string;
  readonly source_snapshot_hash?: string;
  readonly definition_hash: string;
  readonly idempotency_key: string;
  readonly created_at: string;
};

export type CreateEvalCaseInput = EvaluationScope & {
  idempotency_key: string;
  title: string;
  input: unknown;
  oracle?: unknown;
  rubric?: unknown;
  tags?: string[];
  source: EvalCaseSource;
};

export type EvalSuiteStatus = "draft" | "sealed" | "archived";

export type EvalSuiteRecord = {
  readonly id: string;
  readonly user_id: string;
  readonly workspace_id: string;
  readonly name: string;
  readonly description?: string;
  readonly status: EvalSuiteStatus;
  readonly revision: number;
  readonly manifest_hash?: string;
  readonly definition_hash: string;
  readonly idempotency_key: string;
  readonly created_at: string;
  readonly updated_at: string;
  readonly sealed_at?: string;
  readonly archived_at?: string;
};

export type EvalSuiteCaseRecord = {
  readonly user_id: string;
  readonly workspace_id: string;
  readonly suite_id: string;
  readonly case_id: string;
  readonly ordinal: number;
  readonly required: boolean;
  readonly created_at: string;
};

export type EpisodeFeedbackTargetKind = "run" | "message" | "artifact" | "tool_call" | "step";
export type EpisodeFeedbackKind = "thumb" | "rating" | "comment" | "correction" | "label";
export type EpisodeFeedbackSentiment = "positive" | "negative" | "neutral" | "mixed";
export type EpisodeFeedbackActorKind = "user" | "reviewer" | "import";

export type RunEpisodeFeedbackRecord = {
  readonly id: string;
  readonly user_id: string;
  readonly workspace_id: string;
  readonly episode_id: string;
  readonly run_id: string;
  readonly sequence: number;
  readonly target_kind: EpisodeFeedbackTargetKind;
  readonly target_ref: string;
  readonly feedback_kind: EpisodeFeedbackKind;
  readonly sentiment?: EpisodeFeedbackSentiment;
  readonly rating?: number;
  readonly comment_text?: string;
  readonly details_json?: string;
  readonly evidence_refs_json?: string;
  readonly actor_kind: EpisodeFeedbackActorKind;
  readonly actor_id: string;
  readonly supersedes_feedback_id?: string;
  readonly idempotency_key: string;
  readonly content_hash: string;
  readonly created_at: string;
};

export type AppendRunEpisodeFeedbackInput = EvaluationScope & {
  run_id: string;
  idempotency_key: string;
  target_kind: EpisodeFeedbackTargetKind;
  target_ref: string;
  feedback_kind: EpisodeFeedbackKind;
  sentiment?: EpisodeFeedbackSentiment;
  rating?: number;
  comment_text?: string;
  details?: unknown;
  evidence_refs?: unknown;
  actor_kind: EpisodeFeedbackActorKind;
  actor_id: string;
  supersedes_feedback_id?: string;
};

/** Immutable definitions that can be reused across evaluation suites. */
export class EvalCaseRepository {
  constructor(private readonly db: DatabaseSync) {}

  create(input: CreateEvalCaseInput): EvalCaseRecord {
    const idempotencyKey = requiredTrimmed(input.idempotency_key, "EVAL_CASE_IDEMPOTENCY_KEY_INVALID");
    const title = requiredTrimmed(input.title, "EVAL_CASE_TITLE_INVALID");
    const tags = normalizeTags(input.tags);

    this.db.exec("BEGIN IMMEDIATE");
    try {
      const existing = this.findByIdempotencyKey({
        user_id: input.user_id,
        workspace_id: input.workspace_id,
        idempotency_key: idempotencyKey
      });
      const source = existing
        ? this.resolveReplaySource(input, existing, idempotencyKey)
        : this.resolveSource(input);
      const definition = {
        schema_version: EVAL_CASE_SCHEMA_VERSION,
        title,
        input: input.input,
        ...(input.oracle !== undefined ? { oracle: input.oracle } : {}),
        ...(input.rubric !== undefined ? { rubric: input.rubric } : {}),
        tags,
        source_kind: input.source.kind,
        ...source
      };
      const canonical = canonicalJson(definition, `EVAL_CASE_DEFINITION_INVALID:${idempotencyKey}`);
      const definitionHash = sha256(canonical);
      if (existing) {
        const resolved = resolveDefinitionIdempotency(
          existing,
          definitionHash,
          `EVAL_CASE_IDEMPOTENCY_CONFLICT:${idempotencyKey}`
        );
        this.db.exec("COMMIT");
        return resolved;
      }

      const id = `eval-case:${randomUUID()}`;
      const createdAt = new Date().toISOString();
      this.db.prepare(`
        INSERT INTO eval_cases (
          id, user_id, workspace_id, schema_version, title, input_json, oracle_json,
          rubric_json, tags_json, source_kind, source_episode_id, source_snapshot_hash,
          definition_hash, idempotency_key, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        id,
        input.user_id,
        input.workspace_id,
        EVAL_CASE_SCHEMA_VERSION,
        title,
        canonicalJson(input.input, `EVAL_CASE_INPUT_INVALID:${idempotencyKey}`),
        input.oracle === undefined ? null : canonicalJson(input.oracle, `EVAL_CASE_ORACLE_INVALID:${idempotencyKey}`),
        input.rubric === undefined ? null : canonicalJson(input.rubric, `EVAL_CASE_RUBRIC_INVALID:${idempotencyKey}`),
        canonicalJson(tags, `EVAL_CASE_TAGS_INVALID:${idempotencyKey}`),
        input.source.kind,
        source.source_episode_id ?? null,
        source.source_snapshot_hash ?? null,
        definitionHash,
        idempotencyKey,
        createdAt
      );
      const created = this.get({ ...input, case_id: id });
      this.db.exec("COMMIT");
      return created;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  get(input: EvaluationScope & { case_id: string }): EvalCaseRecord {
    const record = mapEvalCaseRow(this.db.prepare(`
      SELECT * FROM eval_cases
      WHERE user_id = ? AND workspace_id = ? AND id = ?
    `).get(input.user_id, input.workspace_id, input.case_id));
    if (!record) {
      throw new Error(`EVAL_CASE_NOT_FOUND:${input.case_id}`);
    }
    return record;
  }

  list(input: EvaluationScope & { limit?: number }): EvalCaseRecord[] {
    const limit = normalizeLimit(input.limit);
    return this.db.prepare(`
      SELECT * FROM eval_cases
      WHERE user_id = ? AND workspace_id = ?
      ORDER BY created_at DESC, id DESC
      LIMIT ?
    `).all(input.user_id, input.workspace_id, limit).map(mapRequiredEvalCaseRow);
  }

  findByIdempotencyKey(
    input: EvaluationScope & { idempotency_key: string }
  ): EvalCaseRecord | undefined {
    const idempotencyKey = requiredTrimmed(input.idempotency_key, "EVAL_CASE_IDEMPOTENCY_KEY_INVALID");
    return mapEvalCaseRow(this.db.prepare(`
      SELECT * FROM eval_cases
      WHERE user_id = ? AND workspace_id = ? AND idempotency_key = ?
    `).get(input.user_id, input.workspace_id, idempotencyKey));
  }

  private resolveSource(input: CreateEvalCaseInput): {
    source_episode_id?: string;
    source_snapshot_hash?: string;
  } {
    if (input.source.kind !== "episode") {
      return {};
    }
    const episodeId = requiredTrimmed(input.source.episode_id, "EVAL_CASE_SOURCE_EPISODE_INVALID");
    const row = this.db.prepare(`
      SELECT id, snapshot_hash
      FROM run_episodes
      WHERE user_id = ? AND workspace_id = ? AND id = ?
    `).get(input.user_id, input.workspace_id, episodeId);
    if (!isRecord(row)) {
      throw new Error(`EVAL_CASE_SOURCE_EPISODE_NOT_FOUND:${episodeId}`);
    }
    return {
      source_episode_id: requiredString(row, "id"),
      source_snapshot_hash: requiredString(row, "snapshot_hash")
    };
  }

  private resolveReplaySource(
    input: CreateEvalCaseInput,
    existing: EvalCaseRecord,
    idempotencyKey: string
  ): { source_episode_id?: string; source_snapshot_hash?: string } {
    if (existing.source_kind !== input.source.kind) {
      throw new Error(`EVAL_CASE_IDEMPOTENCY_CONFLICT:${idempotencyKey}`);
    }
    if (input.source.kind !== "episode") {
      return {};
    }
    const episodeId = requiredTrimmed(input.source.episode_id, "EVAL_CASE_SOURCE_EPISODE_INVALID");
    if (existing.source_episode_id !== episodeId || !existing.source_snapshot_hash) {
      throw new Error(`EVAL_CASE_IDEMPOTENCY_CONFLICT:${idempotencyKey}`);
    }
    return {
      source_episode_id: episodeId,
      source_snapshot_hash: existing.source_snapshot_hash
    };
  }
}

/** Versioned suite manifests. Membership is mutable only while the suite is draft. */
export class EvalSuiteRepository {
  constructor(private readonly db: DatabaseSync) {}

  create(input: EvaluationScope & {
    idempotency_key: string;
    name: string;
    description?: string;
  }): EvalSuiteRecord {
    const idempotencyKey = requiredTrimmed(input.idempotency_key, "EVAL_SUITE_IDEMPOTENCY_KEY_INVALID");
    const name = requiredTrimmed(input.name, "EVAL_SUITE_NAME_INVALID");
    const description = optionalTrimmed(input.description);
    const definitionHash = sha256(canonicalJson(
      { name, ...(description ? { description } : {}) },
      `EVAL_SUITE_DEFINITION_INVALID:${idempotencyKey}`
    ));

    this.db.exec("BEGIN IMMEDIATE");
    try {
      const existing = this.findByIdempotencyKey(input, idempotencyKey);
      if (existing) {
        const resolved = resolveDefinitionIdempotency(
          existing,
          definitionHash,
          `EVAL_SUITE_IDEMPOTENCY_CONFLICT:${idempotencyKey}`
        );
        this.db.exec("COMMIT");
        return resolved;
      }
      const id = `eval-suite:${randomUUID()}`;
      const now = new Date().toISOString();
      this.db.prepare(`
        INSERT INTO eval_suites (
          id, user_id, workspace_id, name, description, status, revision,
          definition_hash, idempotency_key, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'draft', 1, ?, ?, ?, ?)
      `).run(
        id,
        input.user_id,
        input.workspace_id,
        name,
        description ?? null,
        definitionHash,
        idempotencyKey,
        now,
        now
      );
      const created = this.get({ ...input, suite_id: id });
      this.db.exec("COMMIT");
      return created;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  get(input: EvaluationScope & { suite_id: string }): EvalSuiteRecord {
    const record = mapEvalSuiteRow(this.db.prepare(`
      SELECT * FROM eval_suites
      WHERE user_id = ? AND workspace_id = ? AND id = ?
    `).get(input.user_id, input.workspace_id, input.suite_id));
    if (!record) {
      throw new Error(`EVAL_SUITE_NOT_FOUND:${input.suite_id}`);
    }
    return record;
  }

  list(input: EvaluationScope & { limit?: number }): EvalSuiteRecord[] {
    return this.db.prepare(`
      SELECT * FROM eval_suites
      WHERE user_id = ? AND workspace_id = ?
      ORDER BY created_at DESC, id DESC
      LIMIT ?
    `).all(input.user_id, input.workspace_id, normalizeLimit(input.limit)).map(mapRequiredEvalSuiteRow);
  }

  listCases(input: EvaluationScope & { suite_id: string }): EvalSuiteCaseRecord[] {
    this.get(input);
    return this.db.prepare(`
      SELECT * FROM eval_suite_cases
      WHERE user_id = ? AND workspace_id = ? AND suite_id = ?
      ORDER BY ordinal ASC, case_id ASC
    `).all(input.user_id, input.workspace_id, input.suite_id).map(mapRequiredEvalSuiteCaseRow);
  }

  addCase(input: EvaluationScope & {
    suite_id: string;
    case_id: string;
    ordinal: number;
    required?: boolean;
    expected_revision: number;
  }): { suite: EvalSuiteRecord; membership: EvalSuiteCaseRecord } {
    if (!Number.isInteger(input.ordinal) || input.ordinal < 0) {
      throw new Error(`EVAL_SUITE_ORDINAL_INVALID:${input.ordinal}`);
    }
    const required = input.required ?? true;
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const suite = this.get(input);
      const existing = this.findMembership(input);
      if (existing) {
        if (existing.ordinal !== input.ordinal || existing.required !== required) {
          throw new Error(`EVAL_SUITE_MEMBERSHIP_CONFLICT:${input.suite_id}:${input.case_id}`);
        }
        this.db.exec("COMMIT");
        return { suite, membership: existing };
      }
      this.requireDraftRevision(suite, input.expected_revision);
      this.requireCase(input);
      const ordinalOwner = this.db.prepare(`
        SELECT case_id FROM eval_suite_cases
        WHERE user_id = ? AND workspace_id = ? AND suite_id = ? AND ordinal = ?
      `).get(input.user_id, input.workspace_id, input.suite_id, input.ordinal);
      if (isRecord(ordinalOwner)) {
        throw new Error(`EVAL_SUITE_ORDINAL_CONFLICT:${input.suite_id}:${input.ordinal}`);
      }
      const now = new Date().toISOString();
      this.db.prepare(`
        INSERT INTO eval_suite_cases (
          user_id, workspace_id, suite_id, case_id, ordinal, required, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(
        input.user_id,
        input.workspace_id,
        input.suite_id,
        input.case_id,
        input.ordinal,
        required ? 1 : 0,
        now
      );
      this.bumpRevision(input, suite.revision, now);
      const result = {
        suite: this.get(input),
        membership: this.requireMembership(input)
      };
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  removeCase(input: EvaluationScope & {
    suite_id: string;
    case_id: string;
    expected_revision: number;
  }): EvalSuiteRecord {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const suite = this.get(input);
      const existing = this.findMembership(input);
      if (!existing) {
        // Repeating an already-applied DELETE is intentionally a no-op even if
        // the original expected revision is now stale; this path changes no state.
        this.db.exec("COMMIT");
        return suite;
      }
      this.requireDraftRevision(suite, input.expected_revision);
      this.db.prepare(`
        DELETE FROM eval_suite_cases
        WHERE user_id = ? AND workspace_id = ? AND suite_id = ? AND case_id = ?
      `).run(input.user_id, input.workspace_id, input.suite_id, input.case_id);
      this.bumpRevision(input, suite.revision, new Date().toISOString());
      const updated = this.get(input);
      this.db.exec("COMMIT");
      return updated;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  seal(input: EvaluationScope & { suite_id: string; expected_revision: number }): EvalSuiteRecord {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const suite = this.get(input);
      if (suite.status === "sealed") {
        this.db.exec("COMMIT");
        return suite;
      }
      this.requireDraftRevision(suite, input.expected_revision);
      const manifest = this.db.prepare(`
        SELECT membership.case_id, membership.ordinal, membership.required, cases.definition_hash
        FROM eval_suite_cases AS membership
        JOIN eval_cases AS cases
          ON cases.user_id = membership.user_id
          AND cases.workspace_id = membership.workspace_id
          AND cases.id = membership.case_id
        WHERE membership.user_id = ? AND membership.workspace_id = ? AND membership.suite_id = ?
        ORDER BY membership.ordinal ASC, membership.case_id ASC
      `).all(input.user_id, input.workspace_id, input.suite_id).map((row) => {
        if (!isRecord(row)) {
          throw new Error(`EVAL_SUITE_MANIFEST_INVALID:${input.suite_id}`);
        }
        return {
          case_id: requiredString(row, "case_id"),
          definition_hash: requiredString(row, "definition_hash"),
          ordinal: requiredNumber(row, "ordinal"),
          required: requiredBooleanInteger(row, "required")
        };
      });
      if (manifest.length === 0) {
        throw new Error(`EVAL_SUITE_EMPTY:${input.suite_id}`);
      }
      const manifestHash = sha256(canonicalJson(manifest, `EVAL_SUITE_MANIFEST_INVALID:${input.suite_id}`));
      const now = new Date().toISOString();
      const result = this.db.prepare(`
        UPDATE eval_suites
        SET status = 'sealed', revision = revision + 1, manifest_hash = ?, sealed_at = ?, updated_at = ?
        WHERE user_id = ? AND workspace_id = ? AND id = ? AND status = 'draft' AND revision = ?
      `).run(
        manifestHash,
        now,
        now,
        input.user_id,
        input.workspace_id,
        input.suite_id,
        input.expected_revision
      );
      if (result.changes !== 1) {
        throw new Error(`EVAL_SUITE_REVISION_CONFLICT:${input.suite_id}:${input.expected_revision}`);
      }
      const sealed = this.get(input);
      this.db.exec("COMMIT");
      return sealed;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  archive(input: EvaluationScope & { suite_id: string; expected_revision: number }): EvalSuiteRecord {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const suite = this.get(input);
      if (suite.status === "archived") {
        this.db.exec("COMMIT");
        return suite;
      }
      if (suite.status !== "sealed") {
        throw new Error(`EVAL_SUITE_NOT_SEALED:${input.suite_id}`);
      }
      if (suite.revision !== input.expected_revision) {
        throw new Error(`EVAL_SUITE_REVISION_CONFLICT:${input.suite_id}:${input.expected_revision}`);
      }
      const now = new Date().toISOString();
      this.db.prepare(`
        UPDATE eval_suites
        SET status = 'archived', revision = revision + 1, archived_at = ?, updated_at = ?
        WHERE user_id = ? AND workspace_id = ? AND id = ? AND status = 'sealed' AND revision = ?
      `).run(now, now, input.user_id, input.workspace_id, input.suite_id, input.expected_revision);
      const archived = this.get(input);
      this.db.exec("COMMIT");
      return archived;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  private findByIdempotencyKey(scope: EvaluationScope, key: string): EvalSuiteRecord | undefined {
    return mapEvalSuiteRow(this.db.prepare(`
      SELECT * FROM eval_suites
      WHERE user_id = ? AND workspace_id = ? AND idempotency_key = ?
    `).get(scope.user_id, scope.workspace_id, key));
  }

  private requireCase(input: EvaluationScope & { case_id: string }): void {
    const row = this.db.prepare(`
      SELECT 1 FROM eval_cases WHERE user_id = ? AND workspace_id = ? AND id = ?
    `).get(input.user_id, input.workspace_id, input.case_id);
    if (!isRecord(row)) {
      throw new Error(`EVAL_CASE_NOT_FOUND:${input.case_id}`);
    }
  }

  private findMembership(input: EvaluationScope & {
    suite_id: string;
    case_id: string;
  }): EvalSuiteCaseRecord | undefined {
    return mapEvalSuiteCaseRow(this.db.prepare(`
      SELECT * FROM eval_suite_cases
      WHERE user_id = ? AND workspace_id = ? AND suite_id = ? AND case_id = ?
    `).get(input.user_id, input.workspace_id, input.suite_id, input.case_id));
  }

  private requireMembership(input: EvaluationScope & {
    suite_id: string;
    case_id: string;
  }): EvalSuiteCaseRecord {
    const membership = this.findMembership(input);
    if (!membership) {
      throw new Error(`EVAL_SUITE_MEMBERSHIP_NOT_FOUND:${input.suite_id}:${input.case_id}`);
    }
    return membership;
  }

  private requireDraftRevision(suite: EvalSuiteRecord, expectedRevision: number): void {
    if (suite.status !== "draft") {
      throw new Error(`EVAL_SUITE_NOT_DRAFT:${suite.id}:${suite.status}`);
    }
    if (!Number.isInteger(expectedRevision) || suite.revision !== expectedRevision) {
      throw new Error(`EVAL_SUITE_REVISION_CONFLICT:${suite.id}:${expectedRevision}`);
    }
  }

  private bumpRevision(scope: EvaluationScope & { suite_id: string }, revision: number, now: string): void {
    const result = this.db.prepare(`
      UPDATE eval_suites
      SET revision = revision + 1, updated_at = ?
      WHERE user_id = ? AND workspace_id = ? AND id = ? AND status = 'draft' AND revision = ?
    `).run(now, scope.user_id, scope.workspace_id, scope.suite_id, revision);
    if (result.changes !== 1) {
      throw new Error(`EVAL_SUITE_REVISION_CONFLICT:${scope.suite_id}:${revision}`);
    }
  }
}

/** Append-only human/imported feedback anchored to an immutable terminal episode. */
export class RunEpisodeFeedbackRepository {
  constructor(private readonly db: DatabaseSync) {}

  append(input: AppendRunEpisodeFeedbackInput): RunEpisodeFeedbackRecord {
    const normalized = normalizeFeedbackInput(input);
    const episode = this.requireEpisode(input);
    const content = {
      episode_id: episode.id,
      run_id: input.run_id,
      target_kind: input.target_kind,
      target_ref: normalized.target_ref,
      feedback_kind: input.feedback_kind,
      ...(input.sentiment ? { sentiment: input.sentiment } : {}),
      ...(input.rating !== undefined ? { rating: input.rating } : {}),
      ...(normalized.comment_text ? { comment_text: normalized.comment_text } : {}),
      ...(input.details !== undefined ? { details: input.details } : {}),
      ...(input.evidence_refs !== undefined ? { evidence_refs: input.evidence_refs } : {}),
      actor_kind: input.actor_kind,
      actor_id: normalized.actor_id,
      ...(input.supersedes_feedback_id ? { supersedes_feedback_id: input.supersedes_feedback_id } : {})
    };
    const contentHash = sha256(canonicalJson(content, `EPISODE_FEEDBACK_CONTENT_INVALID:${normalized.idempotency_key}`));

    this.db.exec("BEGIN IMMEDIATE");
    try {
      const existing = this.findByIdempotencyKey(input, normalized.idempotency_key);
      if (existing) {
        const resolved = resolveContentIdempotency(
          existing,
          contentHash,
          `EPISODE_FEEDBACK_IDEMPOTENCY_CONFLICT:${normalized.idempotency_key}`
        );
        this.db.exec("COMMIT");
        return resolved;
      }
      if (input.supersedes_feedback_id) {
        this.requireSupersededFeedback(input, episode.id, normalized);
      }
      const sequence = this.nextSequence(input, episode.id);
      const id = `episode-feedback:${randomUUID()}`;
      const createdAt = new Date().toISOString();
      this.db.prepare(`
        INSERT INTO run_episode_feedback (
          id, user_id, workspace_id, episode_id, run_id, sequence, target_kind, target_ref,
          feedback_kind, sentiment, rating, comment_text, details_json, evidence_refs_json,
          actor_kind, actor_id, supersedes_feedback_id, idempotency_key, content_hash, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        id,
        input.user_id,
        input.workspace_id,
        episode.id,
        input.run_id,
        sequence,
        input.target_kind,
        normalized.target_ref,
        input.feedback_kind,
        input.sentiment ?? null,
        input.rating ?? null,
        normalized.comment_text ?? null,
        input.details === undefined ? null : canonicalJson(input.details, `EPISODE_FEEDBACK_DETAILS_INVALID:${normalized.idempotency_key}`),
        input.evidence_refs === undefined ? null : canonicalJson(input.evidence_refs, `EPISODE_FEEDBACK_EVIDENCE_INVALID:${normalized.idempotency_key}`),
        input.actor_kind,
        normalized.actor_id,
        input.supersedes_feedback_id ?? null,
        normalized.idempotency_key,
        contentHash,
        createdAt
      );
      const created = this.get({ ...input, feedback_id: id });
      this.db.exec("COMMIT");
      return created;
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  get(input: EvaluationScope & { feedback_id: string }): RunEpisodeFeedbackRecord {
    const record = mapRunEpisodeFeedbackRow(this.db.prepare(`
      SELECT * FROM run_episode_feedback
      WHERE user_id = ? AND workspace_id = ? AND id = ?
    `).get(input.user_id, input.workspace_id, input.feedback_id));
    if (!record) {
      throw new Error(`EPISODE_FEEDBACK_NOT_FOUND:${input.feedback_id}`);
    }
    return record;
  }

  listByRun(input: EvaluationScope & { run_id: string; limit?: number }): RunEpisodeFeedbackRecord[] {
    return this.db.prepare(`
      SELECT * FROM run_episode_feedback
      WHERE user_id = ? AND workspace_id = ? AND run_id = ?
      ORDER BY sequence ASC
      LIMIT ?
    `).all(input.user_id, input.workspace_id, input.run_id, normalizeLimit(input.limit)).map(mapRequiredRunEpisodeFeedbackRow);
  }

  private requireEpisode(input: EvaluationScope & { run_id: string }): { id: string } {
    const row = this.db.prepare(`
      SELECT id FROM run_episodes
      WHERE user_id = ? AND workspace_id = ? AND run_id = ?
    `).get(input.user_id, input.workspace_id, input.run_id);
    if (!isRecord(row)) {
      throw new Error(`EPISODE_FEEDBACK_EPISODE_NOT_FOUND:${input.run_id}`);
    }
    return { id: requiredString(row, "id") };
  }

  private findByIdempotencyKey(
    scope: EvaluationScope,
    key: string
  ): RunEpisodeFeedbackRecord | undefined {
    return mapRunEpisodeFeedbackRow(this.db.prepare(`
      SELECT * FROM run_episode_feedback
      WHERE user_id = ? AND workspace_id = ? AND idempotency_key = ?
    `).get(scope.user_id, scope.workspace_id, key));
  }

  private nextSequence(scope: EvaluationScope, episodeId: string): number {
    const row = this.db.prepare(`
      SELECT COALESCE(MAX(sequence), 0) + 1 AS next_sequence
      FROM run_episode_feedback
      WHERE user_id = ? AND workspace_id = ? AND episode_id = ?
    `).get(scope.user_id, scope.workspace_id, episodeId);
    if (!isRecord(row)) {
      throw new Error(`EPISODE_FEEDBACK_SEQUENCE_FAILED:${episodeId}`);
    }
    return requiredNumber(row, "next_sequence");
  }

  private requireSupersededFeedback(
    input: AppendRunEpisodeFeedbackInput,
    episodeId: string,
    normalized: { actor_id: string; target_ref: string }
  ): void {
    const previous = this.get({ ...input, feedback_id: input.supersedes_feedback_id! });
    if (
      previous.episode_id !== episodeId
      || previous.run_id !== input.run_id
      || previous.target_kind !== input.target_kind
      || previous.target_ref !== normalized.target_ref
      || previous.actor_kind !== input.actor_kind
      || previous.actor_id !== normalized.actor_id
    ) {
      throw new Error(`EPISODE_FEEDBACK_SUPERSESSION_INVALID:${input.supersedes_feedback_id}`);
    }
    const alreadySuperseded = this.db.prepare(`
      SELECT id FROM run_episode_feedback
      WHERE user_id = ? AND workspace_id = ? AND supersedes_feedback_id = ?
    `).get(input.user_id, input.workspace_id, input.supersedes_feedback_id!);
    if (isRecord(alreadySuperseded)) {
      throw new Error(`EPISODE_FEEDBACK_ALREADY_SUPERSEDED:${input.supersedes_feedback_id}`);
    }
  }
}

export const initializeEvaluationAssetsSchema = (db: DatabaseSync): void => {
  db.exec(`
    CREATE TABLE IF NOT EXISTS eval_cases (
      id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      workspace_id TEXT NOT NULL,
      schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
      title TEXT NOT NULL,
      input_json TEXT NOT NULL,
      oracle_json TEXT,
      rubric_json TEXT,
      tags_json TEXT NOT NULL,
      source_kind TEXT NOT NULL CHECK (source_kind IN ('episode', 'manual', 'import')),
      source_episode_id TEXT,
      source_snapshot_hash TEXT,
      definition_hash TEXT NOT NULL,
      idempotency_key TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (user_id, id),
      UNIQUE (workspace_id, user_id, id),
      UNIQUE (workspace_id, user_id, idempotency_key),
      CHECK (
        (source_kind = 'episode' AND source_episode_id IS NOT NULL AND source_snapshot_hash IS NOT NULL)
        OR (source_kind != 'episode' AND source_episode_id IS NULL AND source_snapshot_hash IS NULL)
      ),
      FOREIGN KEY (workspace_id, user_id) REFERENCES workspace_memberships(workspace_id, user_id)
    );
    CREATE INDEX IF NOT EXISTS idx_eval_cases_scope
      ON eval_cases(workspace_id, user_id, created_at DESC, id DESC);
    CREATE INDEX IF NOT EXISTS idx_eval_cases_source_episode
      ON eval_cases(workspace_id, user_id, source_episode_id);
    CREATE INDEX IF NOT EXISTS idx_eval_cases_definition_hash
      ON eval_cases(workspace_id, user_id, definition_hash);
    CREATE TRIGGER IF NOT EXISTS trg_eval_cases_immutable_update
      BEFORE UPDATE ON eval_cases
      BEGIN
        SELECT RAISE(ABORT, 'EVAL_CASE_IMMUTABLE');
      END;

    CREATE TABLE IF NOT EXISTS eval_suites (
      id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      workspace_id TEXT NOT NULL,
      name TEXT NOT NULL,
      description TEXT,
      status TEXT NOT NULL CHECK (status IN ('draft', 'sealed', 'archived')),
      revision INTEGER NOT NULL CHECK (revision >= 1),
      manifest_hash TEXT,
      definition_hash TEXT NOT NULL,
      idempotency_key TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      sealed_at TEXT,
      archived_at TEXT,
      PRIMARY KEY (user_id, id),
      UNIQUE (workspace_id, user_id, id),
      UNIQUE (workspace_id, user_id, idempotency_key),
      CHECK (
        (status = 'draft' AND manifest_hash IS NULL AND sealed_at IS NULL AND archived_at IS NULL)
        OR (status = 'sealed' AND manifest_hash IS NOT NULL AND sealed_at IS NOT NULL AND archived_at IS NULL)
        OR (status = 'archived' AND manifest_hash IS NOT NULL AND sealed_at IS NOT NULL AND archived_at IS NOT NULL)
      ),
      FOREIGN KEY (workspace_id, user_id) REFERENCES workspace_memberships(workspace_id, user_id)
    );
    CREATE INDEX IF NOT EXISTS idx_eval_suites_scope
      ON eval_suites(workspace_id, user_id, status, created_at DESC, id DESC);
    CREATE TRIGGER IF NOT EXISTS trg_eval_suites_definition_immutable
      BEFORE UPDATE ON eval_suites
      WHEN NEW.id IS NOT OLD.id
        OR NEW.user_id IS NOT OLD.user_id
        OR NEW.workspace_id IS NOT OLD.workspace_id
        OR NEW.name IS NOT OLD.name
        OR NEW.description IS NOT OLD.description
        OR NEW.definition_hash IS NOT OLD.definition_hash
        OR NEW.idempotency_key IS NOT OLD.idempotency_key
        OR NEW.created_at IS NOT OLD.created_at
      BEGIN
        SELECT RAISE(ABORT, 'EVAL_SUITE_DEFINITION_IMMUTABLE');
      END;
    CREATE TRIGGER IF NOT EXISTS trg_eval_suites_state_transition
      BEFORE UPDATE ON eval_suites
      WHEN NOT (
        (
          OLD.status = 'draft' AND NEW.status = 'draft'
          AND NEW.revision = OLD.revision + 1
          AND NEW.manifest_hash IS OLD.manifest_hash
          AND NEW.sealed_at IS OLD.sealed_at
          AND NEW.archived_at IS OLD.archived_at
        )
        OR (
          OLD.status = 'draft' AND NEW.status = 'sealed'
          AND NEW.revision = OLD.revision + 1
          AND NEW.manifest_hash IS NOT NULL
          AND NEW.sealed_at IS NOT NULL
          AND NEW.archived_at IS NULL
        )
        OR (
          OLD.status = 'sealed' AND NEW.status = 'archived'
          AND NEW.revision = OLD.revision + 1
          AND NEW.manifest_hash IS OLD.manifest_hash
          AND NEW.sealed_at IS OLD.sealed_at
          AND NEW.archived_at IS NOT NULL
        )
      )
      BEGIN
        SELECT RAISE(ABORT, 'EVAL_SUITE_STATE_TRANSITION_INVALID');
      END;

    CREATE TABLE IF NOT EXISTS eval_suite_cases (
      user_id TEXT NOT NULL,
      workspace_id TEXT NOT NULL,
      suite_id TEXT NOT NULL,
      case_id TEXT NOT NULL,
      ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
      required INTEGER NOT NULL CHECK (required IN (0, 1)),
      created_at TEXT NOT NULL,
      PRIMARY KEY (workspace_id, user_id, suite_id, case_id),
      UNIQUE (workspace_id, user_id, suite_id, ordinal),
      FOREIGN KEY (workspace_id, user_id, suite_id) REFERENCES eval_suites(workspace_id, user_id, id),
      FOREIGN KEY (workspace_id, user_id, case_id) REFERENCES eval_cases(workspace_id, user_id, id)
    );
    CREATE INDEX IF NOT EXISTS idx_eval_suite_cases_case
      ON eval_suite_cases(workspace_id, user_id, case_id);
    CREATE TRIGGER IF NOT EXISTS trg_eval_suite_cases_insert_draft
      BEFORE INSERT ON eval_suite_cases
      WHEN (SELECT status FROM eval_suites
            WHERE workspace_id = NEW.workspace_id AND user_id = NEW.user_id AND id = NEW.suite_id) != 'draft'
      BEGIN
        SELECT RAISE(ABORT, 'EVAL_SUITE_NOT_DRAFT');
      END;
    CREATE TRIGGER IF NOT EXISTS trg_eval_suite_cases_update_immutable
      BEFORE UPDATE ON eval_suite_cases
      BEGIN
        SELECT RAISE(ABORT, 'EVAL_SUITE_MEMBERSHIP_IMMUTABLE');
      END;
    CREATE TRIGGER IF NOT EXISTS trg_eval_suite_cases_delete_draft
      BEFORE DELETE ON eval_suite_cases
      WHEN (SELECT status FROM eval_suites
            WHERE workspace_id = OLD.workspace_id AND user_id = OLD.user_id AND id = OLD.suite_id) != 'draft'
      BEGIN
        SELECT RAISE(ABORT, 'EVAL_SUITE_NOT_DRAFT');
      END;

    CREATE UNIQUE INDEX IF NOT EXISTS ux_run_episodes_scope_id
      ON run_episodes(workspace_id, user_id, id);
    CREATE UNIQUE INDEX IF NOT EXISTS ux_run_episodes_scope_id_run
      ON run_episodes(workspace_id, user_id, id, run_id);
    CREATE TABLE IF NOT EXISTS run_episode_feedback (
      id TEXT NOT NULL,
      user_id TEXT NOT NULL,
      workspace_id TEXT NOT NULL,
      episode_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      sequence INTEGER NOT NULL CHECK (sequence >= 1),
      target_kind TEXT NOT NULL CHECK (target_kind = 'run'),
      target_ref TEXT NOT NULL,
      feedback_kind TEXT NOT NULL CHECK (feedback_kind IN ('thumb', 'rating', 'comment', 'correction', 'label')),
      sentiment TEXT CHECK (sentiment IN ('positive', 'negative', 'neutral', 'mixed')),
      rating INTEGER CHECK (rating BETWEEN 1 AND 5),
      comment_text TEXT,
      details_json TEXT,
      evidence_refs_json TEXT,
      actor_kind TEXT NOT NULL CHECK (actor_kind IN ('user', 'reviewer', 'import')),
      actor_id TEXT NOT NULL,
      supersedes_feedback_id TEXT,
      idempotency_key TEXT NOT NULL,
      content_hash TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (user_id, id),
      UNIQUE (workspace_id, user_id, id),
      UNIQUE (workspace_id, user_id, idempotency_key),
      UNIQUE (workspace_id, user_id, episode_id, sequence),
      UNIQUE (
        workspace_id, user_id, episode_id, id,
        target_kind, target_ref, actor_kind, actor_id
      ),
      FOREIGN KEY (workspace_id, user_id, episode_id, run_id)
        REFERENCES run_episodes(workspace_id, user_id, id, run_id),
      FOREIGN KEY (
        workspace_id, user_id, episode_id, supersedes_feedback_id,
        target_kind, target_ref, actor_kind, actor_id
      ) REFERENCES run_episode_feedback(
        workspace_id, user_id, episode_id, id,
        target_kind, target_ref, actor_kind, actor_id
      ),
      CHECK (target_ref = run_id),
      CHECK (supersedes_feedback_id IS NULL OR id != supersedes_feedback_id),
      FOREIGN KEY (workspace_id, user_id) REFERENCES workspace_memberships(workspace_id, user_id)
    );
    CREATE INDEX IF NOT EXISTS idx_run_episode_feedback_run
      ON run_episode_feedback(workspace_id, user_id, run_id, sequence ASC);
    CREATE INDEX IF NOT EXISTS idx_run_episode_feedback_episode
      ON run_episode_feedback(workspace_id, user_id, episode_id, sequence ASC);
    CREATE UNIQUE INDEX IF NOT EXISTS ux_run_episode_feedback_supersedes
      ON run_episode_feedback(workspace_id, user_id, supersedes_feedback_id)
      WHERE supersedes_feedback_id IS NOT NULL;
    CREATE TRIGGER IF NOT EXISTS trg_run_episode_feedback_immutable_update
      BEFORE UPDATE ON run_episode_feedback
      BEGIN
        SELECT RAISE(ABORT, 'EPISODE_FEEDBACK_IMMUTABLE');
      END;
  `);
};

const normalizeFeedbackInput = (input: AppendRunEpisodeFeedbackInput): {
  actor_id: string;
  comment_text?: string;
  idempotency_key: string;
  target_ref: string;
} => {
  const idempotencyKey = requiredTrimmed(input.idempotency_key, "EPISODE_FEEDBACK_IDEMPOTENCY_KEY_INVALID");
  const targetRef = requiredTrimmed(input.target_ref, "EPISODE_FEEDBACK_TARGET_INVALID");
  const actorId = requiredTrimmed(input.actor_id, "EPISODE_FEEDBACK_ACTOR_INVALID");
  const commentText = optionalTrimmed(input.comment_text);
  if (input.target_kind !== "run") {
    throw new Error(`EPISODE_FEEDBACK_TARGET_KIND_UNSUPPORTED:${input.target_kind}`);
  }
  if (targetRef !== input.run_id) {
    throw new Error(`EPISODE_FEEDBACK_TARGET_INVALID:${targetRef}`);
  }
  if (input.evidence_refs !== undefined) {
    throw new Error("EPISODE_FEEDBACK_EVIDENCE_NOT_SUPPORTED");
  }
  if (input.actor_kind === "user" && actorId !== input.user_id) {
    throw new Error(`EPISODE_FEEDBACK_ACTOR_SCOPE_INVALID:${actorId}`);
  }
  if (input.feedback_kind === "rating") {
    if (!Number.isInteger(input.rating) || input.rating! < 1 || input.rating! > 5) {
      throw new Error(`EPISODE_FEEDBACK_RATING_INVALID:${String(input.rating)}`);
    }
  } else if (input.rating !== undefined) {
    throw new Error(`EPISODE_FEEDBACK_RATING_NOT_ALLOWED:${input.feedback_kind}`);
  }
  if (input.feedback_kind === "thumb" && input.sentiment !== "positive" && input.sentiment !== "negative") {
    throw new Error(`EPISODE_FEEDBACK_SENTIMENT_INVALID:${String(input.sentiment)}`);
  }
  if ((input.feedback_kind === "comment" || input.feedback_kind === "correction") && !commentText) {
    throw new Error(`EPISODE_FEEDBACK_COMMENT_REQUIRED:${input.feedback_kind}`);
  }
  if (input.feedback_kind === "label" && !commentText && input.details === undefined) {
    throw new Error("EPISODE_FEEDBACK_LABEL_EMPTY");
  }
  return {
    actor_id: actorId,
    ...(commentText ? { comment_text: commentText } : {}),
    idempotency_key: idempotencyKey,
    target_ref: targetRef
  };
};

const normalizeTags = (tags: string[] | undefined): string[] => {
  if (!tags) {
    return [];
  }
  if (!Array.isArray(tags) || tags.some((tag) => typeof tag !== "string")) {
    throw new Error("EVAL_CASE_TAGS_INVALID");
  }
  return [...new Set(tags.map((tag) => tag.trim()).filter(Boolean))].sort();
};

const canonicalJson = (value: unknown, errorCode: string): string => {
  try {
    const result = JSON.stringify(canonicalize(value, new Set<object>()));
    if (typeof result !== "string") {
      throw new Error("not JSON");
    }
    return result;
  } catch {
    throw new Error(errorCode);
  }
};

const canonicalize = (value: unknown, ancestors: Set<object>): unknown => {
  if (value === null || typeof value === "string" || typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error("non-finite number");
    }
    return value;
  }
  if (typeof value !== "object") {
    throw new Error("unsupported JSON value");
  }
  if (ancestors.has(value)) {
    throw new Error("cyclic JSON value");
  }
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      if (Reflect.ownKeys(value).some((key) => key !== "length" && !isArrayIndex(key, value.length))) {
        throw new Error("unsupported array property");
      }
      return Array.from({ length: value.length }, (_, index) => {
        if (!Object.hasOwn(value, index)) {
          throw new Error("sparse array");
        }
        const descriptor = Object.getOwnPropertyDescriptor(value, String(index));
        if (!descriptor || !("value" in descriptor)) {
          throw new Error("array accessor");
        }
        return canonicalize(descriptor.value, ancestors);
      });
    }
    const prototype = Object.getPrototypeOf(value) as unknown;
    if (prototype !== Object.prototype && prototype !== null) {
      throw new Error("non-plain object");
    }
    const entries = Reflect.ownKeys(value).map((key): [string, unknown] => {
      if (typeof key !== "string") {
        throw new Error("symbol key");
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor?.enumerable || !("value" in descriptor)) {
        throw new Error("unsupported property");
      }
      return [key, descriptor.value];
    });
    entries.sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
    return Object.fromEntries(entries.map(([key, entry]) => [key, canonicalize(entry, ancestors)]));
  } finally {
    ancestors.delete(value);
  }
};

const isArrayIndex = (key: string | symbol, length: number): boolean => {
  if (typeof key !== "string") {
    return false;
  }
  const index = Number(key);
  return Number.isInteger(index) && index >= 0 && index < length && String(index) === key;
};

const sha256 = (value: string): string => createHash("sha256").update(value).digest("hex");

const resolveDefinitionIdempotency = <T extends { definition_hash: string }>(
  existing: T,
  expectedHash: string,
  conflictCode: string
): T => {
  if (existing.definition_hash !== expectedHash) {
    throw new Error(conflictCode);
  }
  return existing;
};

const resolveContentIdempotency = <T extends { content_hash: string }>(
  existing: T,
  expectedHash: string,
  conflictCode: string
): T => {
  if (existing.content_hash !== expectedHash) {
    throw new Error(conflictCode);
  }
  return existing;
};

const normalizeLimit = (limit: number | undefined): number => {
  if (limit === undefined) {
    return 100;
  }
  if (!Number.isInteger(limit) || limit < 1 || limit > 500) {
    throw new Error(`EVALUATION_LIST_LIMIT_INVALID:${limit}`);
  }
  return limit;
};

const requiredTrimmed = (value: string, errorCode: string): string => {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(errorCode);
  }
  return value.trim();
};

const optionalTrimmed = (value: string | undefined): string | undefined => {
  if (value === undefined) {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed || undefined;
};

const mapEvalCaseRow = (row: unknown): EvalCaseRecord | undefined => {
  if (!isRecord(row)) {
    return undefined;
  }
  const oracleJson = optionalString(row.oracle_json);
  const rubricJson = optionalString(row.rubric_json);
  const sourceEpisodeId = optionalString(row.source_episode_id);
  const sourceSnapshotHash = optionalString(row.source_snapshot_hash);
  const sourceKind = requiredString(row, "source_kind");
  if (sourceKind !== "episode" && sourceKind !== "manual" && sourceKind !== "import") {
    throw new Error("Expected eval case source kind");
  }
  return {
    id: requiredString(row, "id"),
    user_id: requiredString(row, "user_id"),
    workspace_id: requiredString(row, "workspace_id"),
    schema_version: requiredNumber(row, "schema_version"),
    title: requiredString(row, "title"),
    input_json: requiredString(row, "input_json"),
    ...(oracleJson ? { oracle_json: oracleJson } : {}),
    ...(rubricJson ? { rubric_json: rubricJson } : {}),
    tags_json: requiredString(row, "tags_json"),
    source_kind: sourceKind,
    ...(sourceEpisodeId ? { source_episode_id: sourceEpisodeId } : {}),
    ...(sourceSnapshotHash ? { source_snapshot_hash: sourceSnapshotHash } : {}),
    definition_hash: requiredString(row, "definition_hash"),
    idempotency_key: requiredString(row, "idempotency_key"),
    created_at: requiredString(row, "created_at")
  };
};

const mapRequiredEvalCaseRow = (row: unknown): EvalCaseRecord => {
  const record = mapEvalCaseRow(row);
  if (!record) {
    throw new Error("Expected eval case row");
  }
  return record;
};

const mapEvalSuiteRow = (row: unknown): EvalSuiteRecord | undefined => {
  if (!isRecord(row)) {
    return undefined;
  }
  const status = requiredString(row, "status");
  if (status !== "draft" && status !== "sealed" && status !== "archived") {
    throw new Error("Expected eval suite status");
  }
  const description = optionalString(row.description);
  const manifestHash = optionalString(row.manifest_hash);
  const sealedAt = optionalString(row.sealed_at);
  const archivedAt = optionalString(row.archived_at);
  return {
    id: requiredString(row, "id"),
    user_id: requiredString(row, "user_id"),
    workspace_id: requiredString(row, "workspace_id"),
    name: requiredString(row, "name"),
    ...(description ? { description } : {}),
    status,
    revision: requiredNumber(row, "revision"),
    ...(manifestHash ? { manifest_hash: manifestHash } : {}),
    definition_hash: requiredString(row, "definition_hash"),
    idempotency_key: requiredString(row, "idempotency_key"),
    created_at: requiredString(row, "created_at"),
    updated_at: requiredString(row, "updated_at"),
    ...(sealedAt ? { sealed_at: sealedAt } : {}),
    ...(archivedAt ? { archived_at: archivedAt } : {})
  };
};

const mapRequiredEvalSuiteRow = (row: unknown): EvalSuiteRecord => {
  const record = mapEvalSuiteRow(row);
  if (!record) {
    throw new Error("Expected eval suite row");
  }
  return record;
};

const mapEvalSuiteCaseRow = (row: unknown): EvalSuiteCaseRecord | undefined => {
  if (!isRecord(row)) {
    return undefined;
  }
  return {
    user_id: requiredString(row, "user_id"),
    workspace_id: requiredString(row, "workspace_id"),
    suite_id: requiredString(row, "suite_id"),
    case_id: requiredString(row, "case_id"),
    ordinal: requiredNumber(row, "ordinal"),
    required: requiredBooleanInteger(row, "required"),
    created_at: requiredString(row, "created_at")
  };
};

const mapRequiredEvalSuiteCaseRow = (row: unknown): EvalSuiteCaseRecord => {
  const record = mapEvalSuiteCaseRow(row);
  if (!record) {
    throw new Error("Expected eval suite membership row");
  }
  return record;
};

const mapRunEpisodeFeedbackRow = (row: unknown): RunEpisodeFeedbackRecord | undefined => {
  if (!isRecord(row)) {
    return undefined;
  }
  const targetKind = requiredString(row, "target_kind") as EpisodeFeedbackTargetKind;
  const feedbackKind = requiredString(row, "feedback_kind") as EpisodeFeedbackKind;
  const actorKind = requiredString(row, "actor_kind") as EpisodeFeedbackActorKind;
  const sentiment = optionalString(row.sentiment) as EpisodeFeedbackSentiment | undefined;
  const rating = optionalNumber(row.rating);
  const commentText = optionalString(row.comment_text);
  const detailsJson = optionalString(row.details_json);
  const evidenceRefsJson = optionalString(row.evidence_refs_json);
  const supersedesFeedbackId = optionalString(row.supersedes_feedback_id);
  return {
    id: requiredString(row, "id"),
    user_id: requiredString(row, "user_id"),
    workspace_id: requiredString(row, "workspace_id"),
    episode_id: requiredString(row, "episode_id"),
    run_id: requiredString(row, "run_id"),
    sequence: requiredNumber(row, "sequence"),
    target_kind: targetKind,
    target_ref: requiredString(row, "target_ref"),
    feedback_kind: feedbackKind,
    ...(sentiment ? { sentiment } : {}),
    ...(rating !== undefined ? { rating } : {}),
    ...(commentText ? { comment_text: commentText } : {}),
    ...(detailsJson ? { details_json: detailsJson } : {}),
    ...(evidenceRefsJson ? { evidence_refs_json: evidenceRefsJson } : {}),
    actor_kind: actorKind,
    actor_id: requiredString(row, "actor_id"),
    ...(supersedesFeedbackId ? { supersedes_feedback_id: supersedesFeedbackId } : {}),
    idempotency_key: requiredString(row, "idempotency_key"),
    content_hash: requiredString(row, "content_hash"),
    created_at: requiredString(row, "created_at")
  };
};

const mapRequiredRunEpisodeFeedbackRow = (row: unknown): RunEpisodeFeedbackRecord => {
  const record = mapRunEpisodeFeedbackRow(row);
  if (!record) {
    throw new Error("Expected run episode feedback row");
  }
  return record;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const optionalString = (value: unknown): string | undefined => typeof value === "string" ? value : undefined;
const optionalNumber = (value: unknown): number | undefined => typeof value === "number" ? value : undefined;

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

const requiredBooleanInteger = (row: Record<string, unknown>, key: string): boolean => {
  const value = requiredNumber(row, key);
  if (value !== 0 && value !== 1) {
    throw new Error(`Expected boolean integer column: ${key}`);
  }
  return value === 1;
};
