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

describe("evaluation assets", () => {
  it("creates immutable canonical cases with stable idempotency", () => {
    const fixture = createFixture("case-idempotency");
    try {
      const identity = createVerifiedTestIdentity(fixture.metadata);
      const common = {
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        idempotency_key: "case-manual-1",
        title: "Revenue question",
        input: { prompt: "Revenue?", context: { z: 2, a: 1 } },
        oracle: { answer: 42 },
        rubric: { dimensions: ["correctness"] },
        source: { kind: "manual" as const }
      };

      const created = fixture.metadata.evalCases.create({
        ...common,
        tags: ["finance", "regression", "finance"]
      });
      const replay = fixture.metadata.evalCases.create({
        ...common,
        input: { context: { a: 1, z: 2 }, prompt: "Revenue?" },
        tags: ["regression", "finance"]
      });

      expect(replay).toEqual(created);
      expect(created).toMatchObject({
        schema_version: 1,
        source_kind: "manual",
        tags_json: '["finance","regression"]'
      });
      expect(created.definition_hash).toMatch(/^[a-f0-9]{64}$/);
      expect(fixture.metadata.evalCases.get({
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        case_id: created.id
      })).toEqual(created);
      expect(() => fixture.metadata.evalCases.create({
        ...common,
        input: { prompt: "Different" }
      })).toThrowError("EVAL_CASE_IDEMPOTENCY_CONFLICT:case-manual-1");
      expect(() => fixture.metadata.db.prepare(`
        UPDATE eval_cases SET title = 'mutated' WHERE user_id = ? AND id = ?
      `).run(identity.userId, created.id)).toThrowError(/EVAL_CASE_IMMUTABLE/);
    } finally {
      fixture.close();
    }
  });

  it("anchors episode-derived cases to the immutable snapshot hash without a deletion FK", () => {
    const fixture = createFixture("case-episode");
    try {
      const seeded = seedEpisode(fixture.metadata, "case-episode");
      const createInput = {
        user_id: seeded.userId,
        workspace_id: seeded.workspaceId,
        idempotency_key: "case-from-episode",
        title: "Captured regression",
        input: { prompt: "original prompt", model: "test-model" },
        source: { kind: "episode" as const, episode_id: seeded.episodeId }
      };
      const created = fixture.metadata.evalCases.create(createInput);

      expect(created).toMatchObject({
        source_kind: "episode",
        source_episode_id: seeded.episodeId,
        source_snapshot_hash: seeded.snapshotHash
      });
      expect(() => fixture.metadata.evalCases.create({
        user_id: seeded.userId,
        workspace_id: "another-workspace",
        idempotency_key: "wrong-scope",
        title: "Wrong scope",
        input: {},
        source: { kind: "episode", episode_id: seeded.episodeId }
      })).toThrowError(`EVAL_CASE_SOURCE_EPISODE_NOT_FOUND:${seeded.episodeId}`);

      fixture.metadata.sessions.delete({ user_id: seeded.userId, session_id: seeded.sessionId });
      expect(fixture.metadata.evalCases.create(createInput)).toEqual(created);
      expect(fixture.metadata.evalCases.get({
        user_id: seeded.userId,
        workspace_id: seeded.workspaceId,
        case_id: created.id
      })).toEqual(created);
      expect(fixture.metadata.runEpisodes.findByRun({
        user_id: seeded.userId,
        workspace_id: seeded.workspaceId,
        run_id: seeded.runId
      })).toBeUndefined();
    } finally {
      fixture.close();
    }
  });

  it("seals a deterministic suite manifest and prevents membership mutation", () => {
    const fixture = createFixture("suite-lifecycle");
    try {
      const identity = createVerifiedTestIdentity(fixture.metadata);
      const firstCase = createManualCase(fixture.metadata, identity, "first");
      const secondCase = createManualCase(fixture.metadata, identity, "second");
      const suite = fixture.metadata.evalSuites.create({
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        idempotency_key: "suite-1",
        name: "Core regressions",
        description: "Stable quality gate"
      });

      const firstMembership = fixture.metadata.evalSuites.addCase({
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        suite_id: suite.id,
        case_id: secondCase.id,
        ordinal: 1,
        required: false,
        expected_revision: 1
      });
      expect(firstMembership.suite.revision).toBe(2);
      const secondMembership = fixture.metadata.evalSuites.addCase({
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        suite_id: suite.id,
        case_id: firstCase.id,
        ordinal: 0,
        expected_revision: 2
      });
      expect(secondMembership.suite.revision).toBe(3);
      expect(fixture.metadata.evalSuites.listCases({
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        suite_id: suite.id
      }).map((membership) => [membership.case_id, membership.required])).toEqual([
        [firstCase.id, true],
        [secondCase.id, false]
      ]);

      const sealed = fixture.metadata.evalSuites.seal({
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        suite_id: suite.id,
        expected_revision: 3
      });
      expect(sealed).toMatchObject({ status: "sealed", revision: 4 });
      expect(sealed.manifest_hash).toMatch(/^[a-f0-9]{64}$/);
      expect(fixture.metadata.evalSuites.seal({
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        suite_id: suite.id,
        expected_revision: 3
      })).toEqual(sealed);
      expect(() => fixture.metadata.evalSuites.removeCase({
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        suite_id: suite.id,
        case_id: firstCase.id,
        expected_revision: 4
      })).toThrowError(`EVAL_SUITE_NOT_DRAFT:${suite.id}:sealed`);
      expect(() => fixture.metadata.db.prepare(`
        DELETE FROM eval_suite_cases
        WHERE workspace_id = ? AND user_id = ? AND suite_id = ? AND case_id = ?
      `).run(identity.workspaceId, identity.userId, suite.id, firstCase.id)).toThrowError(/EVAL_SUITE_NOT_DRAFT/);
      expect(() => fixture.metadata.db.prepare(`
        UPDATE eval_suites SET manifest_hash = 'forged' WHERE user_id = ? AND id = ?
      `).run(identity.userId, suite.id)).toThrowError(/EVAL_SUITE_STATE_TRANSITION_INVALID/);
      expect(() => fixture.metadata.db.prepare(`
        UPDATE eval_suites SET name = 'forged' WHERE user_id = ? AND id = ?
      `).run(identity.userId, suite.id)).toThrowError(/EVAL_SUITE_/);
      expect(() => fixture.metadata.db.prepare(`
        UPDATE eval_suites SET revision = revision + 1 WHERE user_id = ? AND id = ?
      `).run(identity.userId, suite.id)).toThrowError(/EVAL_SUITE_STATE_TRANSITION_INVALID/);

      const draft = fixture.metadata.evalSuites.create({
        ...identityScope(identity),
        idempotency_key: "draft-relocation-source",
        name: "Draft relocation source"
      });
      fixture.metadata.evalSuites.addCase({
        ...identityScope(identity),
        suite_id: draft.id,
        case_id: firstCase.id,
        ordinal: 0,
        expected_revision: 1
      });
      expect(() => fixture.metadata.db.prepare(`
        UPDATE eval_suite_cases SET suite_id = ?, ordinal = 2
        WHERE workspace_id = ? AND user_id = ? AND suite_id = ? AND case_id = ?
      `).run(
        suite.id,
        identity.workspaceId,
        identity.userId,
        draft.id,
        firstCase.id
      )).toThrowError(/EVAL_SUITE_MEMBERSHIP_IMMUTABLE/);

      const archived = fixture.metadata.evalSuites.archive({
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        suite_id: suite.id,
        expected_revision: 4
      });
      expect(archived).toMatchObject({ status: "archived", revision: 5 });
    } finally {
      fixture.close();
    }
  });

  it("enforces suite CAS, ordinal uniqueness, and creation idempotency", () => {
    const fixture = createFixture("suite-conflicts");
    try {
      const identity = createVerifiedTestIdentity(fixture.metadata);
      const firstCase = createManualCase(fixture.metadata, identity, "first");
      const secondCase = createManualCase(fixture.metadata, identity, "second");
      const common = {
        user_id: identity.userId,
        workspace_id: identity.workspaceId,
        idempotency_key: "suite-conflict",
        name: "Conflict suite"
      };
      const suite = fixture.metadata.evalSuites.create(common);
      expect(fixture.metadata.evalSuites.create(common)).toEqual(suite);
      expect(() => fixture.metadata.evalSuites.create({ ...common, name: "Changed" }))
        .toThrowError("EVAL_SUITE_IDEMPOTENCY_CONFLICT:suite-conflict");

      fixture.metadata.evalSuites.addCase({
        ...identityScope(identity),
        suite_id: suite.id,
        case_id: firstCase.id,
        ordinal: 0,
        expected_revision: 1
      });
      expect(() => fixture.metadata.evalSuites.addCase({
        ...identityScope(identity),
        suite_id: suite.id,
        case_id: secondCase.id,
        ordinal: 1,
        expected_revision: 1
      })).toThrowError(`EVAL_SUITE_REVISION_CONFLICT:${suite.id}:1`);
      expect(() => fixture.metadata.evalSuites.addCase({
        ...identityScope(identity),
        suite_id: suite.id,
        case_id: secondCase.id,
        ordinal: 0,
        expected_revision: 2
      })).toThrowError(`EVAL_SUITE_ORDINAL_CONFLICT:${suite.id}:0`);
    } finally {
      fixture.close();
    }
  });

  it("records append-only attributed feedback with idempotent supersession", () => {
    const fixture = createFixture("feedback");
    try {
      const seeded = seedEpisode(fixture.metadata, "feedback");
      const common = {
        user_id: seeded.userId,
        workspace_id: seeded.workspaceId,
        run_id: seeded.runId,
        target_kind: "run" as const,
        target_ref: seeded.runId,
        feedback_kind: "rating" as const,
        rating: 2,
        comment_text: "Needs a more precise answer",
        actor_kind: "user" as const,
        actor_id: seeded.userId,
        idempotency_key: "feedback-1"
      };
      const created = fixture.metadata.runEpisodeFeedback.append(common);
      expect(fixture.metadata.runEpisodeFeedback.append(common)).toEqual(created);
      expect(created).toMatchObject({
        episode_id: seeded.episodeId,
        run_id: seeded.runId,
        sequence: 1,
        rating: 2,
        actor_kind: "user"
      });
      expect(created.content_hash).toMatch(/^[a-f0-9]{64}$/);
      expect(() => fixture.metadata.runEpisodeFeedback.append({ ...common, rating: 5 }))
        .toThrowError("EPISODE_FEEDBACK_IDEMPOTENCY_CONFLICT:feedback-1");

      const correction = fixture.metadata.runEpisodeFeedback.append({
        ...common,
        idempotency_key: "feedback-2",
        rating: 4,
        comment_text: "Correction after review",
        supersedes_feedback_id: created.id
      });
      expect(correction.supersedes_feedback_id).toBe(created.id);
      expect(correction.sequence).toBe(2);
      expect(() => fixture.metadata.runEpisodeFeedback.append({
        ...common,
        idempotency_key: "feedback-3",
        rating: 3,
        supersedes_feedback_id: created.id
      })).toThrowError(`EPISODE_FEEDBACK_ALREADY_SUPERSEDED:${created.id}`);
      expect(() => fixture.metadata.db.prepare(`
        UPDATE run_episode_feedback SET rating = 1 WHERE user_id = ? AND id = ?
      `).run(seeded.userId, created.id)).toThrowError(/EPISODE_FEEDBACK_IMMUTABLE/);
      expect(fixture.metadata.runEpisodeFeedback.listByRun({
        user_id: seeded.userId,
        workspace_id: seeded.workspaceId,
        run_id: seeded.runId
      })).toEqual([created, correction]);
    } finally {
      fixture.close();
    }
  });

  it("validates feedback semantics and removes feedback before deleting its episode", () => {
    const fixture = createFixture("feedback-validation");
    try {
      const seeded = seedEpisode(fixture.metadata, "feedback-validation");
      const base = {
        user_id: seeded.userId,
        workspace_id: seeded.workspaceId,
        run_id: seeded.runId,
        target_kind: "run" as const,
        target_ref: seeded.runId,
        actor_kind: "user" as const,
        actor_id: seeded.userId
      };
      expect(() => fixture.metadata.runEpisodeFeedback.append({
        ...base,
        idempotency_key: "invalid-rating",
        feedback_kind: "rating",
        rating: 0
      })).toThrowError("EPISODE_FEEDBACK_RATING_INVALID:0");
      expect(() => fixture.metadata.runEpisodeFeedback.append({
        ...base,
        idempotency_key: "invalid-thumb",
        feedback_kind: "thumb",
        sentiment: "neutral"
      })).toThrowError("EPISODE_FEEDBACK_SENTIMENT_INVALID:neutral");
      expect(() => fixture.metadata.runEpisodeFeedback.append({
        ...base,
        idempotency_key: "invalid-target",
        feedback_kind: "comment",
        comment_text: "wrong target",
        target_ref: "another-run"
      })).toThrowError("EPISODE_FEEDBACK_TARGET_INVALID:another-run");
      expect(() => fixture.metadata.runEpisodeFeedback.append({
        ...base,
        idempotency_key: "unsupported-target",
        feedback_kind: "comment",
        comment_text: "cannot attribute this yet",
        target_kind: "artifact",
        target_ref: "artifact-from-another-run"
      })).toThrowError("EPISODE_FEEDBACK_TARGET_KIND_UNSUPPORTED:artifact");
      expect(() => fixture.metadata.runEpisodeFeedback.append({
        ...base,
        idempotency_key: "unsupported-evidence",
        feedback_kind: "comment",
        comment_text: "evidence is not scoped yet",
        evidence_refs: [{ kind: "artifact", id: "unknown" }]
      })).toThrowError("EPISODE_FEEDBACK_EVIDENCE_NOT_SUPPORTED");
      expect(() => fixture.metadata.runEpisodeFeedback.append({
        ...base,
        idempotency_key: "spoofed-user",
        feedback_kind: "comment",
        comment_text: "spoofed",
        actor_id: "another-user"
      })).toThrowError("EPISODE_FEEDBACK_ACTOR_SCOPE_INVALID:another-user");

      fixture.metadata.runEpisodeFeedback.append({
        ...base,
        idempotency_key: "valid-comment",
        feedback_kind: "comment",
        comment_text: "useful note"
      });
      expect(fixture.metadata.sessions.delete({
        user_id: seeded.userId,
        session_id: seeded.sessionId
      }).deleted).toBe(true);
      expect(fixture.metadata.db.prepare("SELECT COUNT(*) AS count FROM run_episode_feedback").get())
        .toMatchObject({ count: 0 });
    } finally {
      fixture.close();
    }
  });

  it("enforces workspace-scoped feedback supersession at the database boundary", () => {
    const fixture = createFixture("feedback-workspace-scope");
    try {
      const first = seedEpisode(fixture.metadata, "feedback-workspace-primary");
      const firstFeedback = fixture.metadata.runEpisodeFeedback.append({
        user_id: first.userId,
        workspace_id: first.workspaceId,
        run_id: first.runId,
        idempotency_key: "primary-feedback",
        target_kind: "run",
        target_ref: first.runId,
        feedback_kind: "comment",
        comment_text: "primary workspace",
        actor_kind: "user",
        actor_id: first.userId
      });

      const secondWorkspaceId = `secondary-${first.userId}`;
      fixture.metadata.workspaces.createPersonal({
        id: secondWorkspaceId,
        owner_user_id: first.userId,
        name: "Secondary workspace"
      });
      fixture.metadata.workspaceMemberships.upsertOwner({
        workspace_id: secondWorkspaceId,
        user_id: first.userId
      });
      const second = seedEpisodeForIdentity(
        fixture.metadata,
        "feedback-workspace-secondary",
        { userId: first.userId, workspaceId: secondWorkspaceId }
      );

      expect(() => fixture.metadata.db.prepare(`
        INSERT INTO run_episode_feedback (
          id, user_id, workspace_id, episode_id, run_id, sequence, target_kind, target_ref,
          feedback_kind, comment_text, actor_kind, actor_id, supersedes_feedback_id,
          idempotency_key, content_hash, created_at
        ) VALUES (?, ?, ?, ?, ?, 1, 'run', ?, 'comment', ?, 'user', ?, ?, ?, ?, ?)
      `).run(
        "episode-feedback:cross-workspace",
        first.userId,
        second.workspaceId,
        second.episodeId,
        second.runId,
        second.runId,
        "cross workspace",
        first.userId,
        firstFeedback.id,
        "cross-workspace",
        "a".repeat(64),
        new Date().toISOString()
      )).toThrowError(/FOREIGN KEY constraint failed/);
    } finally {
      fixture.close();
    }
  });

  it("binds feedback run identity and supersession to the same episode", () => {
    const fixture = createFixture("feedback-episode-integrity");
    try {
      const first = seedEpisode(fixture.metadata, "feedback-integrity-first");
      const second = seedEpisodeForIdentity(
        fixture.metadata,
        "feedback-integrity-second",
        { userId: first.userId, workspaceId: first.workspaceId }
      );
      const firstFeedback = fixture.metadata.runEpisodeFeedback.append({
        user_id: first.userId,
        workspace_id: first.workspaceId,
        run_id: first.runId,
        idempotency_key: "integrity-parent",
        target_kind: "run",
        target_ref: first.runId,
        feedback_kind: "comment",
        comment_text: "parent feedback",
        actor_kind: "user",
        actor_id: first.userId
      });
      const insert = fixture.metadata.db.prepare(`
        INSERT INTO run_episode_feedback (
          id, user_id, workspace_id, episode_id, run_id, sequence, target_kind, target_ref,
          feedback_kind, comment_text, actor_kind, actor_id, supersedes_feedback_id,
          idempotency_key, content_hash, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'run', ?, 'comment', ?, 'user', ?, ?, ?, ?, ?)
      `);

      expect(() => insert.run(
        "episode-feedback:mismatched-run",
        first.userId,
        first.workspaceId,
        first.episodeId,
        second.runId,
        2,
        second.runId,
        "mismatched run",
        first.userId,
        null,
        "mismatched-run",
        "b".repeat(64),
        new Date().toISOString()
      )).toThrowError(/FOREIGN KEY constraint failed/);

      expect(() => insert.run(
        "episode-feedback:cross-episode",
        first.userId,
        first.workspaceId,
        second.episodeId,
        second.runId,
        1,
        second.runId,
        "cross episode supersession",
        first.userId,
        firstFeedback.id,
        "cross-episode",
        "c".repeat(64),
        new Date().toISOString()
      )).toThrowError(/FOREIGN KEY constraint failed/);

      expect(fixture.metadata.sessions.delete({
        user_id: first.userId,
        session_id: first.sessionId
      }).deleted).toBe(true);
      expect(fixture.metadata.runEpisodes.findByRun({
        user_id: first.userId,
        workspace_id: first.workspaceId,
        run_id: first.runId
      })).toBeUndefined();
      expect(fixture.metadata.runEpisodes.findByRun({
        user_id: second.userId,
        workspace_id: second.workspaceId,
        run_id: second.runId
      })).toBeDefined();
    } finally {
      fixture.close();
    }
  });

  it("persists the 0020 migration and evaluation records across reopen", () => {
    const root = mkdtempSync(join(tmpdir(), "evaluation-assets-reopen-"));
    const databasePath = join(root, "metadata.sqlite");
    let metadata: MetadataStore | undefined = createMetadataStore({ database_path: databasePath });
    try {
      const identity = createVerifiedTestIdentity(metadata);
      const created = createManualCase(metadata, identity, "reopen");
      metadata.close();
      metadata = undefined;

      metadata = createMetadataStore({ database_path: databasePath });
      expect(metadata.evalCases.get({
        ...identityScope(identity),
        case_id: created.id
      })).toEqual(created);
      expect(metadata.db.prepare(`
        SELECT COUNT(*) AS count FROM schema_migrations WHERE id = '0020_evaluation_assets'
      `).get()).toMatchObject({ count: 1 });
    } finally {
      metadata?.close();
      rmSync(root, { recursive: true, force: true });
    }
  });
});

const createFixture = (name: string): { close: () => void; metadata: MetadataStore } => {
  const root = mkdtempSync(join(tmpdir(), `evaluation-assets-${name}-`));
  const metadata = createMetadataStore({ database_path: join(root, "metadata.sqlite") });
  return {
    metadata,
    close: () => {
      metadata.close();
      rmSync(root, { recursive: true, force: true });
    }
  };
};

const identityScope = (identity: { userId: string; workspaceId: string }): {
  user_id: string;
  workspace_id: string;
} => ({ user_id: identity.userId, workspace_id: identity.workspaceId });

const createManualCase = (
  metadata: MetadataStore,
  identity: { userId: string; workspaceId: string },
  suffix: string
) => metadata.evalCases.create({
  ...identityScope(identity),
  idempotency_key: `case-${suffix}`,
  title: `Case ${suffix}`,
  input: { prompt: suffix },
  source: { kind: "manual" }
});

const seedEpisode = (
  metadata: MetadataStore,
  suffix: string
): {
  episodeId: string;
  runId: string;
  sessionId: string;
  snapshotHash: string;
  userId: string;
  workspaceId: string;
} => {
  const { userId, workspaceId } = createVerifiedTestIdentity(metadata, {
    email: `${suffix}@evaluation-assets.example.test`
  });
  return seedEpisodeForIdentity(metadata, suffix, { userId, workspaceId });
};

const seedEpisodeForIdentity = (
  metadata: MetadataStore,
  suffix: string,
  identity: { userId: string; workspaceId: string }
): {
  episodeId: string;
  runId: string;
  sessionId: string;
  snapshotHash: string;
  userId: string;
  workspaceId: string;
} => {
  const { userId, workspaceId } = identity;
  const sessionId = `session-${suffix}`;
  const runId = `run-${suffix}`;
  metadata.sessions.create({ user_id: userId, id: sessionId, title: suffix });
  metadata.runs.create({
    user_id: userId,
    id: runId,
    session_id: sessionId,
    user_input: suffix,
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
    snapshot: { schema_version: 1, run: { id: runId, input: suffix } }
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
