#!/usr/bin/env python3
"""
Stage2 (bucket-local, fixed worker pool):

Input (Stage1):
  - <stage1_out>/pr_events/repo_bucket=####/*.parquet
  - <stage1_out>/other_events/repo_bucket=####/*.parquet

Output (Stage2):
  - Parquet files with the SAME output schema as original Stage2:
      (repo_id, pr_number, pull_request, participants, head_refs, head_shas,
       landed_commit_sha, landed_commit_shas, landed_inferred, pr_body_versions, events)

Changes vs original:
  1) Processes buckets via a fixed pool of Ray tasks (default 32 workers).
  2) Central progress tracking (driver polling) via Ray Actors; minimal stats.
  3) Each worker reserves cpus-per-worker logical CPUs (default 4).
  4) No infer-window: inference considers ALL PushEvents in the bucket.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Set, Tuple

import orjson
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import ray


# ----------------------------
# PR filters (dataset inclusion)
# ----------------------------
ISSUE_REF_RE = re.compile(r"(?<![A-Za-z0-9_])#(\d+)\b")


def _count_linked_issues_from_body(body: Optional[str]) -> int:
    if not isinstance(body, str) or not body:
        return 0
    nums = set(m.group(1) for m in ISSUE_REF_RE.finditer(body))
    return len(nums)


def _contains_trivial_bump_language(body: str) -> bool:
    """
    Super broad filter: if the PR body mentions bump-ish language, consider it trivial.

    This intentionally catches a wide net (false positives expected).
    Examples it should catch:
      - "Bumps X from 2.2.0 to 2.2.1." (common Dependabot-style phrasing) :contentReference[oaicite:1]{index=1}
      - "chore: bump node to v14.15.2 ..."
      - "bumped deps", "dependency bump", etc.
    """
    if not isinstance(body, str):
        return False

    text = body.lower()

    # Add/remove terms freely — keep it simple and language-agnostic-ish.
    bump_terms = (
        "bump",
        "bumps",
        "bumped",
        "bumping",
        "version bump",
        "bump version",
        "dependency bump",
        "dependencies bump",
        "dep bump",
        "deps bump",
        "upgrade dependency",
        "upgrade dependencies",
        "update dependency",
        "update dependencies",
        "chore: bump",
        "chore bump",
        "chore(deps)",
        "chore(deps):",
        "renovate",
        "dependabot",
    )

    return any(term in text for term in bump_terms)


def should_include_pr_row(row: dict) -> bool:
    """
    Include PR iff:
      • PR is merged
      • merged into main/master branch
      • body.strip() > 10
      • 1 <= changed_files <= 15
      • <= 1 distinct issue refs in body
      • AND (new) body does NOT contain trivial bump language
    """
    pr = row.get("pull_request") or {}
    if not isinstance(pr, dict) or not pr:
        return False

    if pr.get("merged") is not True:
        return False

    base = pr.get("base") or {}
    if not isinstance(base, dict) or base.get("ref") not in ("main", "master"):
        return False

    body = pr.get("body")
    if not isinstance(body, str) or len(body.strip()) <= 10:
        return False

    # NEW: skip trivial "bump" PRs based on body text
    title = pr.get("title") or ""
    if _contains_trivial_bump_language(title) or _contains_trivial_bump_language(body):
        return False

    changed_files = pr.get("changed_files")
    if not isinstance(changed_files, int) or changed_files < 1 or changed_files > 15:
        return False

    linked = _count_linked_issues_from_body(body)
    if linked > 1:
        return False

    return True


def _filter_prs_in_place(pr_rows: Dict[Tuple[int, int], dict]) -> Set[Tuple[int, int]]:
    keep = {k for k, row in pr_rows.items() if should_include_pr_row(row)}
    drop = [k for k in pr_rows.keys() if k not in keep]
    for k in drop:
        pr_rows.pop(k, None)
    return keep


# ----------------------------
# PR-relevant event types
# ----------------------------
PR_TYPES = {
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
    "IssueCommentEvent",
    "CommitCommentEvent",
    "StatusEvent",
    "CheckRunEvent",
    "CheckSuiteEvent",
    "PushEvent",
    "status",
    "check_run",
    "check_suite",
}


# ----------------------------
# Time parsing (kept for ordering/metadata only; no infer-window)
# ----------------------------
def iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


# ----------------------------
# Parquet schema (unchanged Stage2 output schema)
# ----------------------------
def _nullable(field: pa.Field) -> pa.Field:
    return pa.field(field.name, field.type, nullable=True, metadata=field.metadata)


user_t = pa.struct(
    [
        pa.field("login", pa.string()),
        pa.field("id", pa.int64()),
        pa.field("url", pa.string()),
        pa.field("html_url", pa.string()),
        pa.field("avatar_url", pa.string()),
        pa.field("type", pa.string()),
        pa.field("site_admin", pa.bool_()),
        pa.field("user_extra_json", pa.binary()),
    ]
)

license_t = pa.struct(
    [
        pa.field("key", pa.string()),
        pa.field("name", pa.string()),
        pa.field("spdx_id", pa.string()),
        pa.field("url", pa.string()),
        pa.field("node_id", pa.string()),
    ]
)

repo_t = pa.struct(
    [
        pa.field("id", pa.int64()),
        pa.field("node_id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("full_name", pa.string()),
        pa.field("private", pa.bool_()),
        pa.field("fork", pa.bool_()),
        pa.field("archived", pa.bool_()),
        pa.field("disabled", pa.bool_()),
        pa.field("description", pa.string()),
        pa.field("homepage", pa.string()),
        pa.field("language", pa.string()),
        pa.field("default_branch", pa.string()),
        pa.field("html_url", pa.string()),
        pa.field("url", pa.string()),
        pa.field("created_at", pa.string()),
        pa.field("updated_at", pa.string()),
        pa.field("pushed_at", pa.string()),
        pa.field("size", pa.int64()),
        pa.field("stargazers_count", pa.int64()),
        pa.field("watchers_count", pa.int64()),
        pa.field("forks_count", pa.int64()),
        pa.field("open_issues_count", pa.int64()),
        pa.field("has_issues", pa.bool_()),
        pa.field("has_projects", pa.bool_()),
        pa.field("has_downloads", pa.bool_()),
        pa.field("has_wiki", pa.bool_()),
        pa.field("has_pages", pa.bool_()),
        pa.field("license", license_t),
        pa.field("owner", user_t),
        pa.field("repo_extra_json", pa.binary()),
    ]
)

pr_ref_t = pa.struct(
    [
        pa.field("label", pa.string()),
        pa.field("ref", pa.string()),
        pa.field("sha", pa.string()),
        pa.field("user", user_t),
        pa.field("repo", repo_t),
        pa.field("ref_extra_json", pa.binary()),
    ]
)

label_t = pa.struct(
    [
        pa.field("id", pa.int64()),
        pa.field("node_id", pa.string()),
        pa.field("url", pa.string()),
        pa.field("name", pa.string()),
        pa.field("color", pa.string()),
        pa.field("default", pa.bool_()),
        pa.field("description", pa.string()),
    ]
)

milestone_t = pa.struct(
    [
        pa.field("id", pa.int64()),
        pa.field("node_id", pa.string()),
        pa.field("number", pa.int64()),
        pa.field("title", pa.string()),
        pa.field("description", pa.string()),
        pa.field("state", pa.string()),
        pa.field("html_url", pa.string()),
        pa.field("created_at", pa.string()),
        pa.field("updated_at", pa.string()),
        pa.field("due_on", pa.string()),
        pa.field("closed_at", pa.string()),
        pa.field("creator", user_t),
        pa.field("milestone_extra_json", pa.binary()),
    ]
)

pull_request_t = pa.struct(
    [
        pa.field("id", pa.int64()),
        pa.field("node_id", pa.string()),
        pa.field("number", pa.int64()),
        pa.field("state", pa.string()),
        pa.field("title", pa.string()),
        pa.field("body", pa.string()),
        pa.field("draft", pa.bool_()),
        pa.field("locked", pa.bool_()),
        pa.field("html_url", pa.string()),
        pa.field("url", pa.string()),
        pa.field("comments_url", pa.string()),
        pa.field("review_comments_url", pa.string()),
        pa.field("commits_url", pa.string()),
        pa.field("statuses_url", pa.string()),
        pa.field("created_at", pa.string()),
        pa.field("updated_at", pa.string()),
        pa.field("closed_at", pa.string()),
        pa.field("merged_at", pa.string()),
        pa.field("merge_commit_sha", pa.string()),
        pa.field("merged", pa.bool_()),
        pa.field("mergeable", pa.bool_()),
        pa.field("rebaseable", pa.bool_()),
        pa.field("mergeable_state", pa.string()),
        pa.field("merged_by", user_t),
        pa.field("author_association", pa.string()),
        pa.field("user", user_t),
        pa.field("labels", pa.list_(label_t)),
        pa.field("assignees", pa.list_(user_t)),
        pa.field("requested_reviewers", pa.list_(user_t)),
        pa.field("milestone", milestone_t),
        pa.field("additions", pa.int64()),
        pa.field("deletions", pa.int64()),
        pa.field("changed_files", pa.int64()),
        pa.field("commits", pa.int64()),
        pa.field("comments", pa.int64()),
        pa.field("review_comments", pa.int64()),
        pa.field("base", pr_ref_t),
        pa.field("head", pr_ref_t),
        pa.field("pr_extra_json", pa.binary()),
    ]
)

issue_t = pa.struct(
    [
        pa.field("number", pa.int64()),
        pa.field("state", pa.string()),
        pa.field("title", pa.string()),
        pa.field("html_url", pa.string()),
        pa.field("updated_at", pa.string()),
        pa.field("issue_extra_json", pa.binary()),
    ]
)

comment_t = pa.struct(
    [
        pa.field("id", pa.int64()),
        pa.field("html_url", pa.string()),
        pa.field("url", pa.string()),
        pa.field("created_at", pa.string()),
        pa.field("updated_at", pa.string()),
        pa.field("author_association", pa.string()),
        pa.field("body", pa.string()),
        pa.field("user", user_t),
        pa.field("comment_extra_json", pa.binary()),
    ]
)

review_t = pa.struct(
    [
        pa.field("id", pa.int64()),
        pa.field("state", pa.string()),
        pa.field("submitted_at", pa.string()),
        pa.field("body", pa.string()),
        pa.field("html_url", pa.string()),
        pa.field("user", user_t),
        pa.field("review_extra_json", pa.binary()),
    ]
)

commit_author_t = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("email", pa.string()),
        pa.field("username", pa.string()),
        pa.field("author_extra_json", pa.binary()),
    ]
)

push_commit_t = pa.struct(
    [
        pa.field("sha", pa.string()),
        pa.field("message", pa.string()),
        pa.field("url", pa.string()),
        pa.field("distinct", pa.bool_()),
        pa.field("author", commit_author_t),
        pa.field("commit_extra_json", pa.binary()),
    ]
)

push_t = pa.struct(
    [
        pa.field("ref", pa.string()),
        pa.field("before", pa.string()),
        pa.field("head", pa.string()),
        pa.field("size", pa.int64()),
        pa.field("distinct_size", pa.int64()),
        pa.field("commits", pa.list_(push_commit_t)),
        pa.field("push_extra_json", pa.binary()),
    ]
)

status_t = pa.struct(
    [
        pa.field("sha", pa.string()),
        pa.field("state", pa.string()),
        pa.field("context", pa.string()),
        pa.field("description", pa.string()),
        pa.field("target_url", pa.string()),
        pa.field("status_extra_json", pa.binary()),
    ]
)

check_run_t = pa.struct(
    [
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("status", pa.string()),
        pa.field("conclusion", pa.string()),
        pa.field("head_sha", pa.string()),
        pa.field("details_url", pa.string()),
        pa.field("external_id", pa.string()),
        pa.field("check_run_extra_json", pa.binary()),
    ]
)

check_suite_t = pa.struct(
    [
        pa.field("id", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("conclusion", pa.string()),
        pa.field("head_sha", pa.string()),
        pa.field("check_suite_extra_json", pa.binary()),
    ]
)

event_t = pa.struct(
    [
        pa.field("id", pa.string()),
        pa.field("type", pa.string()),
        pa.field("created_at", pa.string()),
        pa.field("public", pa.bool_()),
        pa.field("actor", user_t),
        pa.field("event_repo_id", pa.int64()),
        pa.field("event_repo_name", pa.string()),
        pa.field("action", pa.string()),
        pa.field("number", pa.int64()),
        pa.field("issue", issue_t),
        pa.field("comment", comment_t),
        pa.field("review", review_t),
        pa.field("push", push_t),
        pa.field("status", status_t),
        pa.field("check_run", check_run_t),
        pa.field("check_suite", check_suite_t),
        pa.field("event_extra_json", pa.binary()),
    ]
)

body_version_t = pa.struct(
    [
        pa.field("source", pa.string()),
        pa.field("at", pa.string()),
        pa.field("event_id", pa.string()),
        pa.field("body", pa.string()),
    ]
)

SCHEMA = pa.schema(
    [
        pa.field("repo_id", pa.int64()),
        pa.field("pr_number", pa.int64()),
        pa.field("pull_request", pull_request_t),
        pa.field("participants", pa.list_(pa.string())),
        pa.field("head_refs", pa.list_(pa.string())),
        pa.field("head_shas", pa.list_(pa.string())),
        pa.field("landed_commit_sha", pa.string()),
        pa.field("landed_commit_shas", pa.list_(pa.string())),
        pa.field("landed_inferred", pa.bool_()),
        pa.field("pr_body_versions", pa.list_(body_version_t)),
        pa.field("events", pa.list_(event_t)),
    ]
)


# ----------------------------
# Helpers: coercions + extra_json merge
# ----------------------------
def _i64(x: Any) -> Optional[int]:
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    return None


def _s(x: Any) -> Optional[str]:
    return x if isinstance(x, str) else None


def _b(x: Any) -> Optional[bool]:
    return x if isinstance(x, bool) else None


def _merge_extra(existing: Optional[bytes], add: Optional[dict]) -> Optional[bytes]:
    if not add:
        return existing
    base: dict = {}
    if isinstance(existing, (bytes, bytearray)) and existing:
        try:
            base = orjson.loads(existing)
            if not isinstance(base, dict):
                base = {}
        except Exception:
            base = {}
    for k, v in add.items():
        base[k] = v
    if not base:
        return None
    return orjson.dumps(base)


# ----------------------------
# Meta enrichment (choose latest snapshot)
# ----------------------------
def _choose_latest_str(
    meta: dict, field: str, cand_val: Optional[str], cand_ts: Optional[str]
):
    if not isinstance(cand_val, str) or not cand_val:
        return
    old_val = meta.get(field)
    old_ts = meta.get(f"{field}_updated_at")
    if isinstance(cand_ts, str) and (old_ts is None or cand_ts > old_ts):
        meta[field] = cand_val
        meta[f"{field}_updated_at"] = cand_ts
        return
    if not isinstance(old_val, str) or len(cand_val) > len(old_val):
        meta[field] = cand_val


def enrich_from_pull_request(meta: dict, pr: dict):
    if not isinstance(pr, dict) or not pr:
        return
    snap_ts = pr.get("updated_at") or pr.get("created_at") or ""
    prev_ts = meta.get("_pr_updated_at", "")
    if isinstance(snap_ts, str) and snap_ts >= prev_ts:
        meta["_pr_updated_at"] = snap_ts
        meta["pull_request_full"] = pr
        if isinstance(pr.get("number"), int):
            meta["number"] = pr["number"]
        if isinstance(pr.get("merged_at"), str) or pr.get("merged_at") is None:
            meta["merged_at"] = pr.get("merged_at")
        if (
            isinstance(pr.get("merge_commit_sha"), str)
            or pr.get("merge_commit_sha") is None
        ):
            meta["merge_commit_sha"] = pr.get("merge_commit_sha")
        if isinstance(pr.get("merged"), bool) or pr.get("merged") is None:
            meta["merged"] = pr.get("merged")
        _choose_latest_str(meta, "title", pr.get("title"), pr.get("updated_at"))
        _choose_latest_str(meta, "body", pr.get("body"), pr.get("updated_at"))
        base = pr.get("base")
        head = pr.get("head")
        if isinstance(base, dict):
            meta["base"] = base
        if isinstance(head, dict):
            meta["head"] = head


def enrich_from_issue(meta: dict, issue: dict):
    if not isinstance(issue, dict) or not issue:
        return
    _choose_latest_str(meta, "title", issue.get("title"), issue.get("updated_at"))
    _choose_latest_str(meta, "body", issue.get("body"), issue.get("updated_at"))
    meta["issue_full"] = issue


# ----------------------------
# Landed commit inference (NO WINDOW; scan whole bucket)
# ----------------------------
def infer_landed_commits_whole_bucket(
    pr_key: Tuple[int, int],
    meta: dict,
    participants: Dict[Tuple[int, int], set],
    push_index_by_repo_ref: Dict[Tuple[int, str], List[dict]],
) -> List[str]:
    """
    If merge_commit_sha is missing, infer "landed" commits by scanning all pushes
    to the PR base branch within the same repo_bucket.

    Heuristics (whole-bucket):
      - commit message contains PR markers (#NN, "(#NN)", "pull request #NN")
      - OR commit author matches a participant login/name (best-effort)
    """
    base = meta.get("base") or {}
    base_repo = (base.get("repo") or {}) if isinstance(base, dict) else {}
    base_repo_id = base_repo.get("id")
    base_ref = base.get("ref") if isinstance(base, dict) else None
    if not (isinstance(base_repo_id, int) and isinstance(base_ref, str) and base_ref):
        return []

    ref_full = "refs/heads/" + base_ref
    pushes = push_index_by_repo_ref.get((base_repo_id, ref_full), [])
    if not pushes:
        return []

    prn = meta.get("number")
    pr_markers: List[str] = []
    if isinstance(prn, int):
        pr_markers = [f"#{prn}", f"pull request #{prn}", f"(#{prn})"]

    ppl = participants.get(pr_key, set()) if participants else set()

    landed: List[str] = []
    for pe in pushes:
        for c in pe.get("commits") or []:
            sha = c.get("sha")
            msg = (c.get("message") or "").lower()
            author_login = (c.get("author") or {}).get("username") or (
                c.get("author") or {}
            ).get("name")
            if not isinstance(sha, str):
                continue
            if any(m in msg for m in pr_markers):
                landed.append(sha)
                continue
            if author_login and author_login in ppl:
                landed.append(sha)

    seen = set()
    uniq: List[str] = []
    for s in landed:
        if s not in seen:
            uniq.append(s)
            seen.add(s)
    return uniq


# ----------------------------
# Event -> PR key helpers from Stage1 typed rows
# ----------------------------
def _event_repo_id(row: dict) -> Optional[int]:
    return _i64(row.get("repo_id"))


def _event_repo_name(row: dict) -> Optional[str]:
    return _s(row.get("repo_name"))


def _event_type(row: dict) -> Optional[str]:
    return _s(row.get("type"))


def _direct_pr_key(row: dict) -> Optional[Tuple[int, int]]:
    rid = _event_repo_id(row)
    prn = _i64(row.get("direct_pr_number"))
    if isinstance(rid, int) and isinstance(prn, int):
        return (rid, prn)
    return None


def _check_pr_keys(row: dict) -> List[Tuple[int, int]]:
    rid = _event_repo_id(row)
    if not isinstance(rid, int):
        return []
    nums = row.get("check_pr_numbers")
    out: List[Tuple[int, int]] = []
    if isinstance(nums, list):
        for n in nums:
            nn = _i64(n)
            if isinstance(nn, int):
                out.append((rid, nn))
    return out


def _join_sha(row: dict) -> Optional[str]:
    return _s(row.get("join_sha"))


def _push_ref(row: dict) -> Optional[str]:
    return _s(row.get("push_ref"))


def _push_head(row: dict) -> Optional[str]:
    return _s(row.get("push_head"))


def _push_commit_shas(row: dict) -> List[str]:
    shas = row.get("push_commit_shas")
    out: List[str] = []
    if isinstance(shas, list):
        for s in shas:
            ss = _s(s)
            if ss:
                out.append(ss)
    return out


# ----------------------------
# Collect PR meta from Stage1 PR-event row
# ----------------------------
def collect_pr_meta_from_event_row(
    row: dict,
    pr_meta_raw: Dict[Tuple[int, int], dict],
    participants: Dict[Tuple[int, int], set],
    head_shas_by_pr: DefaultDict[Tuple[int, int], set],
    head_refs_by_pr: DefaultDict[Tuple[int, int], set],
    head_repo_id_by_pr: Dict[Tuple[int, int], Optional[int]],
    pr_body_versions: DefaultDict[Tuple[int, int], list],
) -> None:
    key = _direct_pr_key(row)
    if not key:
        return

    actor = (row.get("actor") or {}).get("login")
    if isinstance(actor, str):
        participants.setdefault(key, set()).add(actor)

    meta = pr_meta_raw.setdefault(key, {})

    pr = row.get("pull_request") or {}
    if isinstance(pr, dict) and pr:
        enrich_from_pull_request(meta, pr)

        head = pr.get("head") or {}
        base = pr.get("base") or {}

        href = head.get("ref") if isinstance(head, dict) else None
        if isinstance(href, str) and href:
            head_refs_by_pr[key].add("refs/heads/" + href)

        hsha = head.get("sha") if isinstance(head, dict) else None
        if isinstance(hsha, str) and hsha:
            head_shas_by_pr[key].add(hsha)

        hrepo = ((head.get("repo") or {}).get("id")) if isinstance(head, dict) else None
        head_repo_id_by_pr[key] = _i64(hrepo)

        body = pr.get("body")
        if isinstance(body, str) and body:
            pr_body_versions[key].append(
                {
                    "source": "pull_request",
                    "at": pr.get("updated_at")
                    or pr.get("created_at")
                    or row.get("created_at"),
                    "event_id": row.get("event_id"),
                    "body": body,
                }
            )

    if _event_type(row) == "IssueCommentEvent":
        issue = row.get("issue") or {}
        if isinstance(issue, dict):
            enrich_from_issue(meta, issue)
            ibody = issue.get("body")
            if isinstance(ibody, str) and ibody:
                pr_body_versions[key].append(
                    {
                        "source": "issue",
                        "at": issue.get("updated_at")
                        or issue.get("created_at")
                        or row.get("created_at"),
                        "event_id": row.get("event_id"),
                        "body": ibody,
                    }
                )


# ----------------------------
# Normalize Stage1 event row -> Stage2 event_t dict
# ----------------------------
def norm_event_from_stage1_row(row: dict) -> dict:
    add = {
        "direct_pr_number": row.get("direct_pr_number"),
        "check_pr_numbers": row.get("check_pr_numbers"),
        "join_sha": row.get("join_sha"),
        "push_ref": row.get("push_ref"),
        "push_head": row.get("push_head"),
        "push_commit_shas": row.get("push_commit_shas"),
    }
    base_extra = (
        row.get("event_extra_json")
        if isinstance(row.get("event_extra_json"), (bytes, bytearray))
        else None
    )
    payload_extra = (
        row.get("payload_extra_json")
        if isinstance(row.get("payload_extra_json"), (bytes, bytearray))
        else None
    )
    extra = _merge_extra(base_extra, add)
    if payload_extra:
        try:
            extra = _merge_extra(extra, orjson.loads(payload_extra))
        except Exception:
            pass

    num = _i64(row.get("direct_pr_number"))
    if num is None:
        pr = row.get("pull_request")
        if isinstance(pr, dict):
            num = _i64(pr.get("number"))
    if num is None:
        iss = row.get("issue")
        if isinstance(iss, dict):
            num = _i64(iss.get("number"))

    def _norm_issue(x: Any) -> Optional[dict]:
        if not isinstance(x, dict) or not x:
            return None
        return {
            "number": _i64(x.get("number")),
            "state": _s(x.get("state")),
            "title": _s(x.get("title")),
            "html_url": _s(x.get("html_url")),
            "updated_at": _s(x.get("updated_at")),
            "issue_extra_json": x.get("issue_extra_json")
            if isinstance(x.get("issue_extra_json"), (bytes, bytearray))
            else None,
        }

    def _norm_comment(x: Any) -> Optional[dict]:
        if not isinstance(x, dict) or not x:
            return None
        commit_id = _s(x.get("commit_id"))
        cextra = (
            x.get("comment_extra_json")
            if isinstance(x.get("comment_extra_json"), (bytes, bytearray))
            else None
        )
        if commit_id:
            cextra = _merge_extra(cextra, {"commit_id": commit_id})
        return {
            "id": _i64(x.get("id")),
            "html_url": _s(x.get("html_url")),
            "url": _s(x.get("url")),
            "created_at": _s(x.get("created_at")),
            "updated_at": _s(x.get("updated_at")),
            "author_association": _s(x.get("author_association")),
            "body": _s(x.get("body")),
            "user": x.get("user") if isinstance(x.get("user"), dict) else None,
            "comment_extra_json": cextra,
        }

    def _norm_review(x: Any) -> Optional[dict]:
        if not isinstance(x, dict) or not x:
            return None
        return {
            "id": _i64(x.get("id")),
            "state": _s(x.get("state")),
            "submitted_at": _s(x.get("submitted_at")),
            "body": _s(x.get("body")),
            "html_url": _s(x.get("html_url")),
            "user": x.get("user") if isinstance(x.get("user"), dict) else None,
            "review_extra_json": x.get("review_extra_json")
            if isinstance(x.get("review_extra_json"), (bytes, bytearray))
            else None,
        }

    def _norm_check_run(x: Any) -> Optional[dict]:
        if not isinstance(x, dict) or not x:
            return None
        prs = x.get("pull_requests")
        cextra = (
            x.get("check_run_extra_json")
            if isinstance(x.get("check_run_extra_json"), (bytes, bytearray))
            else None
        )
        if isinstance(prs, list) and prs:
            cextra = _merge_extra(cextra, {"pull_requests": prs})
        return {
            "id": _i64(x.get("id")),
            "name": _s(x.get("name")),
            "status": _s(x.get("status")),
            "conclusion": _s(x.get("conclusion")),
            "head_sha": _s(x.get("head_sha")),
            "details_url": _s(x.get("details_url")),
            "external_id": _s(x.get("external_id")),
            "check_run_extra_json": cextra,
        }

    def _norm_check_suite(x: Any) -> Optional[dict]:
        if not isinstance(x, dict) or not x:
            return None
        prs = x.get("pull_requests")
        cextra = (
            x.get("check_suite_extra_json")
            if isinstance(x.get("check_suite_extra_json"), (bytes, bytearray))
            else None
        )
        if isinstance(prs, list) and prs:
            cextra = _merge_extra(cextra, {"pull_requests": prs})
        return {
            "id": _i64(x.get("id")),
            "status": _s(x.get("status")),
            "conclusion": _s(x.get("conclusion")),
            "head_sha": _s(x.get("head_sha")),
            "check_suite_extra_json": cextra,
        }

    return {
        "id": _s(row.get("event_id")),
        "type": _s(row.get("type")),
        "created_at": _s(row.get("created_at")),
        "public": _b(row.get("public")),
        "actor": row.get("actor") if isinstance(row.get("actor"), dict) else None,
        "event_repo_id": _event_repo_id(row),
        "event_repo_name": _event_repo_name(row),
        "action": _s(row.get("action")),
        "number": num,
        "issue": _norm_issue(row.get("issue")),
        "comment": _norm_comment(row.get("comment")),
        "review": _norm_review(row.get("review")),
        "push": row.get("push") if isinstance(row.get("push"), dict) else None,
        "status": row.get("status") if isinstance(row.get("status"), dict) else None,
        "check_run": _norm_check_run(row.get("check_run")),
        "check_suite": _norm_check_suite(row.get("check_suite")),
        "event_extra_json": extra,
    }


def build_push_candidates_index(
    head_refs_by_pr: Dict[Tuple[int, int], Set[str]],
    head_repo_id_by_pr: Dict[Tuple[int, int], Optional[int]],
) -> Dict[Tuple[int, str], List[Tuple[int, int]]]:
    """
    Map (head_repo_id, head_ref) -> list of PR keys
    """
    idx: DefaultDict[Tuple[int, str], List[Tuple[int, int]]] = defaultdict(list)
    for pr_key, refs in head_refs_by_pr.items():
        hrepo = head_repo_id_by_pr.get(pr_key)
        if not isinstance(hrepo, int):
            continue
        if not refs:
            continue
        for ref in refs:
            if isinstance(ref, str) and ref:
                idx[(hrepo, ref)].append(pr_key)
    return dict(idx)


def attach_rows_to_prs(
    rows: Iterable[dict],
    pr_rows: Dict[Tuple[int, int], dict],
    head_refs_by_pr: Dict[Tuple[int, int], set],
    head_shas_by_pr: Dict[Tuple[int, int], set],
    head_repo_id_by_pr: Dict[Tuple[int, int], Optional[int]],
    sha_to_prs: Dict[str, List[Tuple[int, int]]],
    push_candidates: Optional[Dict[Tuple[int, str], List[Tuple[int, int]]]] = None,
) -> Tuple[int, int]:
    """
    Same behavior, but PushEvent routing uses an index:
      (event_repo_id, push_ref) -> candidate PRs
    """
    if push_candidates is None:
        push_candidates = build_push_candidates_index(
            head_refs_by_pr, head_repo_id_by_pr
        )

    seen = attached = 0

    # timers
    t_push = 0.0
    push_events = 0
    push_candidate_scans = 0  # now should be tiny vs 98k

    for row in rows:
        seen += 1

        et = _event_type(row)
        if et not in PR_TYPES:
            continue
        rid_ev = _event_repo_id(row)
        if not isinstance(rid_ev, int):
            continue

        targets: List[Tuple[int, int]] = []

        dk = _direct_pr_key(row)
        if dk is not None:
            targets = [dk]

        if not targets and et in (
            "CheckRunEvent",
            "check_run",
            "CheckSuiteEvent",
            "check_suite",
        ):
            targets = _check_pr_keys(row)

        if not targets and et == "PushEvent":
            t0 = time.time()
            push_events += 1

            pref = _push_ref(row)
            if isinstance(pref, str) and pref:
                cand = push_candidates.get((rid_ev, pref), [])
                push_candidate_scans += len(cand)

                # optional refinement: intersect by head sha / commit shas if available
                phead = _push_head(row)
                pcommits = _push_commit_shas(row)
                for key in cand:
                    shas = head_shas_by_pr.get(key, set())
                    ok = True
                    if isinstance(phead, str) and phead and shas:
                        ok = phead in shas
                    elif pcommits and shas:
                        ok = any(s in shas for s in pcommits)
                    if ok:
                        targets.append(key)

            t_push += time.time() - t0

        if not targets:
            s = _join_sha(row)
            if isinstance(s, str) and s:
                prs = sha_to_prs.get(s) or []
                if len(prs) == 1:
                    targets = prs

        if not targets:
            continue

        ev_norm = norm_event_from_stage1_row(row)

        did = False
        for key in targets:
            prow = pr_rows.get(key)
            if prow is not None:
                prow["events"].append(ev_norm)
                did = True
        if did:
            attached += 1

    return seen, attached


# ----------------------------
# Output writer
# ----------------------------
@dataclass
class BucketWriter:
    out_dir: str
    repo_bucket: int
    max_rows: int
    seq: int = 0
    buf: Optional[List[dict]] = None

    def __post_init__(self):
        self.buf = []

    def _path(self) -> str:
        d = os.path.join(self.out_dir, f"repo_bucket={self.repo_bucket:04d}")
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"prs_{self.seq:010d}.parquet")

    def add_rows(self, rows: List[dict]):
        assert self.buf is not None
        for r in rows:
            self.buf.append(r)
            if len(self.buf) >= self.max_rows:
                self.flush()

    def flush(self):
        assert self.buf is not None
        if not self.buf:
            return
        path = self._path()
        table = pa.Table.from_pylist(self.buf, schema=SCHEMA)
        pq.write_table(table, path, compression="zstd")
        self.seq += 1
        self.buf.clear()

    def close(self):
        self.flush()


# ----------------------------
# Dataset helpers: list repo_buckets + iterate rows
# ----------------------------
_REPO_BUCKET_DIR_RE = re.compile(r"repo_bucket=(\d+)$")


def list_repo_buckets(stage1_dir: str) -> List[int]:
    pr_root = Path(stage1_dir) / "pr_events"
    if not pr_root.exists():
        return []
    out: List[int] = []
    for p in pr_root.iterdir():
        if not p.is_dir():
            continue
        m = _REPO_BUCKET_DIR_RE.match(p.name)
        if m:
            out.append(int(m.group(1)))
    return sorted(set(out))


def open_bucket_dataset(root: str) -> ds.Dataset:
    # Hive partitioning uses key=value directory names like repo_bucket=0420. :contentReference[oaicite:3]{index=3}
    return ds.dataset(root, format="parquet", partitioning="hive")


def iter_rows_for_bucket(
    dset: ds.Dataset,
    repo_bucket: int,
    columns: Optional[List[str]] = None,
    include_filename: bool = False,
) -> Iterable[dict]:
    filt = ds.field("repo_bucket") == repo_bucket
    cols = columns[:] if columns else None
    if include_filename:
        cols = (cols or []) + [
            "__filename"
        ]  # supported special column :contentReference[oaicite:5]{index=5}

    scanner = dset.scanner(
        filter=filt,
        columns=cols,
        batch_size=8192,
        use_threads=True,  # dataset scan threading :contentReference[oaicite:6]{index=6}
        fragment_readahead=16,  # tune if many fragments/files :contentReference[oaicite:7]{index=7}
        batch_readahead=16,  # tune read-ahead :contentReference[oaicite:8]{index=8}
    )

    for batch in scanner.to_batches():
        for row in batch.to_pylist():
            yield row


def combine_one_repo_bucket_local_log(
    stage1_dir: str,
    out_dir: str,
    repo_bucket: int,
    max_rows_per_file: int,
) -> dict:
    import os
    import time
    from collections import defaultdict

    import ray

    # ---- logging helpers ----
    def _now() -> float:
        return time.time()

    def _log(msg: str):
        # Single-line log prefix makes grepping easy across worker logs
        node = ray.util.get_node_ip_address()
        print(f"[bucket={repo_bucket} node={node}] {msg}", flush=True)

    # ---- config knobs (safe defaults) ----
    LOG_EVERY_SECS = 5 * 60.0
    OTHER_ATTACH_BATCH = 1024  # batch rows before calling attach_rows_to_prs

    t0 = _now()
    last_log = t0

    pr_path = os.path.join(stage1_dir, "pr_events")
    other_path = os.path.join(stage1_dir, "other_events")

    if not os.path.isdir(pr_path):
        return {"repo_bucket": repo_bucket, "error": f"missing {pr_path}"}

    _log("START")

    # Datasets
    pr_ds = open_bucket_dataset(pr_path)
    other_ds = open_bucket_dataset(other_path) if os.path.isdir(other_path) else None

    # -------------------------
    # Pass 1: scan pr_events once
    # -------------------------
    pr_meta_raw: Dict[Tuple[int, int], dict] = {}
    participants: Dict[Tuple[int, int], set] = {}
    head_shas_by_pr: DefaultDict[Tuple[int, int], set] = defaultdict(set)
    head_refs_by_pr: DefaultDict[Tuple[int, int], set] = defaultdict(set)
    head_repo_id_by_pr: Dict[Tuple[int, int], Optional[int]] = {}
    pr_body_versions: DefaultDict[Tuple[int, int], list] = defaultdict(list)
    push_index_by_repo_ref: DefaultDict[Tuple[int, str], List[dict]] = defaultdict(list)

    pr_events_buf: List[dict] = []
    pr_events_in = 0
    pr_rows_seen = 0

    t_scan_pr0 = _now()
    _log("phase=scan_pr_events begin")

    try:
        for row in iter_rows_for_bucket(pr_ds, repo_bucket):
            pr_rows_seen += 1
            pr_events_in += 1
            pr_events_buf.append(row)

            now = _now()
            if now - last_log >= LOG_EVERY_SECS:
                _log(
                    f"phase=scan_pr_events rows_seen={pr_rows_seen} buf={len(pr_events_buf)} "
                    f"elapsed={now - t_scan_pr0:.1f}s total={now - t0:.1f}s"
                )
                last_log = now

            et = _event_type(row)
            if et in PR_TYPES:
                collect_pr_meta_from_event_row(
                    row,
                    pr_meta_raw,
                    participants,
                    head_shas_by_pr,
                    head_refs_by_pr,
                    head_repo_id_by_pr,
                    pr_body_versions,
                )

                # Push index for inference (whole-bucket matching)
                if et == "PushEvent":
                    rid = _event_repo_id(row)
                    ref = _push_ref(row)
                    push = row.get("push") or {}
                    commits = (
                        (push.get("commits") or []) if isinstance(push, dict) else []
                    )
                    if isinstance(rid, int) and isinstance(ref, str) and ref:
                        pe = {"created_at": row.get("created_at"), "commits": []}
                        if isinstance(commits, list):
                            for c in commits:
                                if isinstance(c, dict):
                                    pe["commits"].append(
                                        {
                                            "sha": c.get("sha"),
                                            "message": c.get("message"),
                                            "author": (c.get("author") or {})
                                            if isinstance(c.get("author"), dict)
                                            else {},
                                        }
                                    )
                        push_index_by_repo_ref[(rid, ref)].append(pe)
    except Exception as e:
        _log(f"phase=scan_pr_events ERROR={type(e).__name__}: {e}")
        raise

    t_scan_pr1 = _now()
    _log(
        f"phase=scan_pr_events done rows_seen={pr_rows_seen} pr_events_in={pr_events_in} "
        f"pr_meta_keys={len(pr_meta_raw)} elapsed={t_scan_pr1 - t_scan_pr0:.1f}s"
    )

    # -------------------------
    # Build PR rows (merged only)
    # -------------------------
    t_build0 = _now()
    _log("phase=build_pr_rows begin")

    pr_rows: Dict[Tuple[int, int], dict] = {}
    merged_seen = 0
    inferred_used = 0
    inferred_failed = 0

    built_iter = 0
    last_build_log = _now()

    for key, meta in pr_meta_raw.items():
        built_iter += 1
        now = _now()
        if now - last_build_log >= LOG_EVERY_SECS:
            _log(
                f"phase=build_pr_rows meta_iter={built_iter}/{len(pr_meta_raw)} "
                f"prs_built={len(pr_rows)} merged_seen={merged_seen} "
                f"inferred_used={inferred_used} inferred_failed={inferred_failed} "
                f"elapsed={now - t_build0:.1f}s"
            )
            last_build_log = now

        pr_full = meta.get("pull_request_full")
        pr_struct = pr_full if isinstance(pr_full, dict) else None
        if pr_struct is None:
            continue

        if meta.get("merged") is not True:
            continue
        merged_seen += 1

        ppl = sorted(participants.get(key, set()))
        head_refs = sorted(head_refs_by_pr.get(key, set()))
        head_shas = sorted(head_shas_by_pr.get(key, set()))

        bvers = pr_body_versions.get(key, [])
        uniq_versions: List[dict] = []
        seen_body = set()
        for v in bvers:
            b = v.get("body") or ""
            if b and b not in seen_body:
                uniq_versions.append(v)
                seen_body.add(b)

        landed_commit_sha: Optional[str] = None
        landed_commit_shas: Optional[List[str]] = None
        landed_inferred: Optional[bool] = None

        merge_commit_sha = meta.get("merge_commit_sha")
        if isinstance(merge_commit_sha, str) and merge_commit_sha:
            landed_commit_sha = merge_commit_sha
            landed_commit_shas = [merge_commit_sha]
            landed_inferred = False
        else:
            inferred = infer_landed_commits_whole_bucket(
                key, meta, participants, push_index_by_repo_ref
            )
            if inferred:
                inferred_used += 1
                landed_commit_sha = inferred[0]
                landed_commit_shas = inferred
                landed_inferred = True
            else:
                inferred_failed += 1
                continue

        pr_rows[key] = {
            "repo_id": key[0],
            "pr_number": key[1],
            "pull_request": pr_struct,
            "participants": ppl,
            "head_refs": head_refs,
            "head_shas": head_shas,
            "landed_commit_sha": landed_commit_sha,
            "landed_commit_shas": landed_commit_shas,
            "landed_inferred": landed_inferred,
            "pr_body_versions": uniq_versions if len(uniq_versions) > 1 else None,
            "events": [],
        }

    built = len(pr_rows)
    t_build1 = _now()
    _log(
        f"phase=build_pr_rows done prs_built={built} merged_seen={merged_seen} "
        f"inferred_used={inferred_used} inferred_failed={inferred_failed} "
        f"elapsed={t_build1 - t_build0:.1f}s"
    )

    # -------------------------
    # Filter PRs before attaching events
    # -------------------------
    t_filter0 = _now()
    _log("phase=filter_prs begin")
    kept_keys = _filter_prs_in_place(pr_rows)
    kept = len(kept_keys)
    t_filter1 = _now()
    _log(
        f"phase=filter_prs done prs_kept={kept}/{built} elapsed={t_filter1 - t_filter0:.1f}s"
    )

    if kept == 0:
        _log("DONE (no kept PRs)")
        return {
            "repo_bucket": repo_bucket,
            "prs_built": built,
            "prs_kept": 0,
            "merged_seen": merged_seen,
            "inferred_used": inferred_used,
            "inferred_failed": inferred_failed,
            "pr_events_in": pr_events_in,
            "other_events_in": 0,
            "events_attached": 0,
            "files_written": 0,
            "t_scan_pr_s": t_scan_pr1 - t_scan_pr0,
            "t_build_s": t_build1 - t_build0,
            "t_filter_s": t_filter1 - t_filter0,
            "t_total_s": _now() - t0,
        }

    # Prune indices to kept PRs
    head_refs_by_pr2 = {k: v for k, v in head_refs_by_pr.items() if k in kept_keys}
    head_shas_by_pr2 = {k: v for k, v in head_shas_by_pr.items() if k in kept_keys}
    head_repo_id_by_pr2 = {
        k: v for k, v in head_repo_id_by_pr.items() if k in kept_keys
    }

    sha_to_prs: DefaultDict[str, List[Tuple[int, int]]] = defaultdict(list)
    for key, shas in head_shas_by_pr2.items():
        for s in shas:
            sha_to_prs[s].append(key)

    # -------------------------
    # Pass 2a: attach buffered pr_events (no re-scan)
    # -------------------------
    t_attach_pr0 = _now()
    _log(f"phase=attach_pr_events begin buffered_rows={len(pr_events_buf)}")
    try:
        _, attached_a = attach_rows_to_prs(
            pr_events_buf,
            pr_rows,
            head_refs_by_pr2,
            head_shas_by_pr2,
            head_repo_id_by_pr2,
            sha_to_prs,
        )
    except Exception as e:
        _log(f"phase=attach_pr_events ERROR={type(e).__name__}: {e}")
        raise
    t_attach_pr1 = _now()
    _log(
        f"phase=attach_pr_events done attached={int(attached_a)} "
        f"elapsed={t_attach_pr1 - t_attach_pr0:.1f}s"
    )

    # -------------------------
    # Pass 2b: scan+attach other_events (single scan, batched)
    # -------------------------
    other_in = 0
    attached_b = 0

    t_other0 = _now()
    if other_ds is None:
        _log("phase=scan_other_events skipped (missing other_events/)")
    else:
        _log("phase=scan_other_events begin")

        batch: List[dict] = []
        last_other_log = _now()
        try:
            for row in iter_rows_for_bucket(other_ds, repo_bucket):
                other_in += 1
                batch.append(row)
                if len(batch) >= OTHER_ATTACH_BATCH:
                    _, a = attach_rows_to_prs(
                        batch,
                        pr_rows,
                        head_refs_by_pr2,
                        head_shas_by_pr2,
                        head_repo_id_by_pr2,
                        sha_to_prs,
                    )
                    attached_b += int(a)
                    batch.clear()

                now = _now()
                if now - last_other_log >= LOG_EVERY_SECS:
                    _log(
                        f"phase=scan_other_events rows_seen={other_in} attached_so_far={attached_b} "
                        f"elapsed={now - t_other0:.1f}s"
                    )
                    last_other_log = now

            # flush remainder
            if batch:
                _, a = attach_rows_to_prs(
                    batch,
                    pr_rows,
                    head_refs_by_pr2,
                    head_shas_by_pr2,
                    head_repo_id_by_pr2,
                    sha_to_prs,
                )
                attached_b += int(a)
                batch.clear()

        except Exception as e:
            _log(f"phase=scan_other_events ERROR={type(e).__name__}: {e}")
            raise

        t_other1 = _now()
        _log(
            f"phase=scan_other_events done other_events_in={other_in} attached={attached_b} "
            f"elapsed={t_other1 - t_other0:.1f}s"
        )

    t_other1 = _now()

    # -------------------------
    # Write output
    # -------------------------
    t_write0 = _now()
    _log("phase=write_output begin")

    keys_sorted = sorted(pr_rows.keys())
    rows_out = [pr_rows[k] for k in keys_sorted]

    writer = BucketWriter(
        out_dir=out_dir, repo_bucket=repo_bucket, max_rows=max_rows_per_file
    )
    writer.add_rows(rows_out)
    writer.close()

    t_write1 = _now()
    total_elapsed = t_write1 - t0
    _log(
        f"phase=write_output done files_written={int(writer.seq)} elapsed={t_write1 - t_write0:.1f}s"
    )
    _log(
        f"DONE prs_built={built} prs_kept={kept} pr_events_in={pr_events_in} other_events_in={other_in} "
        f"events_attached={int(attached_a + attached_b)} total_elapsed={total_elapsed:.1f}s"
    )

    return {
        "repo_bucket": repo_bucket,
        "prs_built": built,
        "prs_kept": kept,
        "merged_seen": merged_seen,
        "inferred_used": inferred_used,
        "inferred_failed": inferred_failed,
        "pr_events_in": pr_events_in,
        "other_events_in": other_in,
        "events_attached": int(attached_a + attached_b),
        "files_written": int(writer.seq),
        # lightweight timings to help identify stalls
        "t_scan_pr_s": t_scan_pr1 - t_scan_pr0,
        "t_build_s": t_build1 - t_build0,
        "t_filter_s": t_filter1 - t_filter0,
        "t_attach_pr_s": t_attach_pr1 - t_attach_pr0,
        "t_scan_other_s": t_other1 - t_other0,
        "t_write_s": t_write1 - t_write0,
        "t_total_s": total_elapsed,
    }


# ----------------------------
# Progress + Work queue (Ray Actors)
# ----------------------------
@ray.remote
class WorkQueue:
    def __init__(self, buckets: List[int]):
        self._buckets = list(buckets)
        self._i = 0

    def next_bucket(self) -> Optional[int]:
        if self._i >= len(self._buckets):
            return None
        b = self._buckets[self._i]
        self._i += 1
        return b

    def total(self) -> int:
        return len(self._buckets)


@ray.remote
class ProgressTracker:
    def __init__(self, num_workers: int, total_buckets: int):
        self.num_workers = num_workers
        self.total_buckets = total_buckets
        self.t0 = time.time()
        self.workers: Dict[int, Dict[str, Any]] = {
            i: {
                "done": 0,
                "errors": 0,
                "current_bucket": None,
                "prs_out": 0,
                "files_out": 0,
                "last_update": None,
            }
            for i in range(num_workers)
        }

    def bucket_started(self, worker_id: int, bucket: int) -> None:
        w = self.workers[worker_id]
        w["current_bucket"] = bucket
        w["last_update"] = time.time()

    def bucket_done(
        self, worker_id: int, bucket: int, prs_out: int, files_out: int
    ) -> None:
        w = self.workers[worker_id]
        w["done"] += 1
        w["prs_out"] += int(prs_out)
        w["files_out"] += int(files_out)
        w["current_bucket"] = None
        w["last_update"] = time.time()

    def bucket_error(self, worker_id: int, bucket: int) -> None:
        w = self.workers[worker_id]
        w["errors"] += 1
        w["done"] += 1
        w["current_bucket"] = None
        w["last_update"] = time.time()

    def snapshot(self) -> Dict[str, Any]:
        elapsed = time.time() - self.t0
        done = sum(w["done"] for w in self.workers.values())
        prs = sum(w["prs_out"] for w in self.workers.values())
        files = sum(w["files_out"] for w in self.workers.values())
        errors = sum(w["errors"] for w in self.workers.values())
        return {
            "elapsed_s": elapsed,
            "total_buckets": self.total_buckets,
            "buckets_done": done,
            "prs_out": prs,
            "files_out": files,
            "errors": errors,
            "workers": self.workers,
        }


def start_progress_polling(tracker, *, interval_s: float = 10.0, stream=sys.stderr):
    stop = threading.Event()

    def _run():
        next_t = 0.0
        while not stop.is_set():
            now = time.time()
            if now >= next_t:
                try:
                    snap = ray.get(tracker.snapshot.remote())
                    elapsed = snap["elapsed_s"]
                    done = snap["buckets_done"]
                    total = snap["total_buckets"]
                    rate = done / elapsed if elapsed > 0 else 0.0
                    print(
                        f"[progress] buckets {done}/{total} done "
                        f"(errors={snap['errors']}) prs_out={snap['prs_out']} files_out={snap['files_out']} "
                        f"elapsed={elapsed:.1f}s rate={rate:.2f} buckets/s",
                        file=stream,
                        flush=True,
                    )
                except Exception as e:
                    print(f"[progress] polling error: {e!r}", file=stream, flush=True)
                next_t = now + interval_s
            stop.wait(0.2)

    t = threading.Thread(target=_run, name="stage2-progress-poller", daemon=True)
    t.start()
    return stop, t


# ----------------------------
# Fixed worker task: loops buckets from queue
# ----------------------------
@ray.remote
def bucket_worker_loop(
    worker_id: int,
    queue,
    tracker,
    stage1_out: str,
    out_dir: str,
    max_rows_per_file: int,
) -> dict:
    prs_out = 0
    files_out = 0
    errors = 0

    while True:
        b = ray.get(queue.next_bucket.remote())
        if b is None:
            break

        tracker.bucket_started.remote(worker_id, b)
        try:
            s = combine_one_repo_bucket_local_log(
                stage1_out, out_dir, b, max_rows_per_file
            )
            if "error" in s:
                errors += 1
                tracker.bucket_error.remote(worker_id, b)
                print(
                    f"[worker {worker_id}] bucket={b} ERROR: {s['error']}",
                    file=sys.stderr,
                    flush=True,
                )
                continue

            prs_out += int(s.get("prs_kept", 0))
            files_out += int(s.get("files_written", 0))
            tracker.bucket_done.remote(
                worker_id, b, int(s.get("prs_kept", 0)), int(s.get("files_written", 0))
            )

        except Exception as e:
            errors += 1
            tracker.bucket_error.remote(worker_id, b)
            print(
                f"[worker {worker_id}] bucket={b} EXCEPTION: {e!r}",
                file=sys.stderr,
                flush=True,
            )

    return {
        "worker": worker_id,
        "prs_out": prs_out,
        "files_out": files_out,
        "errors": errors,
    }


# ----------------------------
# Ray init
# ----------------------------
def connect(addr: Optional[str]):
    return ray.init(
        address=(addr or "auto"), namespace="pr-pipeline", log_to_driver=True
    )


# ----------------------------
# CLI / driver
# ----------------------------
def main():
    ap = argparse.ArgumentParser(
        description="Stage2(Parquet): bucket-local PR merge builder with fixed Ray worker pool."
    )
    ap.add_argument(
        "--stage1-out",
        required=True,
        help="Stage1 output root (pr_events/, other_events/)",
    )
    ap.add_argument(
        "--out-dir", required=True, help="Directory to write Stage2 Parquet files"
    )
    ap.add_argument(
        "--ray-address",
        default="auto",
        help='Ray address: "auto" or "ray://host:10001"',
    )
    ap.add_argument(
        "--max-rows-per-file",
        type=int,
        default=100_000,
        help="Max PR rows per output parquet file",
    )
    ap.add_argument(
        "--buckets",
        default="",
        help="Optional comma-separated repo_bucket list (e.g. 420,421)",
    )
    ap.add_argument(
        "--num-workers",
        type=int,
        default=32,
        help="Fixed Ray worker tasks (default: 32)",
    )
    ap.add_argument(
        "--cpus-per-worker",
        type=int,
        default=4,
        help="Logical CPUs reserved per worker task (default: 4)",
    )
    ap.add_argument(
        "--progress-interval-s",
        type=float,
        default=10.0,
        help="Driver progress print interval",
    )
    args = ap.parse_args()

    connect(args.ray_address)

    os.makedirs(args.out_dir, exist_ok=True)

    if args.buckets.strip():
        buckets = sorted({int(x.strip()) for x in args.buckets.split(",") if x.strip()})
    else:
        buckets = list_repo_buckets(args.stage1_out)

    if not buckets:
        print("No repo_bucket=#### directories found under pr_events/", file=sys.stderr)
        sys.exit(1)

    print(
        f"Stage2(bucketed): buckets={len(buckets)} out={args.out_dir} "
        f"workers={args.num_workers} cpus/worker={args.cpus_per_worker} max_rows/file={args.max_rows_per_file}",
        file=sys.stderr,
        flush=True,
    )

    # Shared queue + progress tracker (Actors are stateful shared services). :contentReference[oaicite:4]{index=4}
    queue = WorkQueue.remote(buckets)
    tracker = ProgressTracker.remote(
        num_workers=args.num_workers, total_buckets=len(buckets)
    )

    stop_evt, poll_thread = start_progress_polling(
        tracker, interval_s=args.progress_interval_s, stream=sys.stderr
    )

    try:
        # Fixed pool of tasks; each reserves logical CPUs via num_cpus. :contentReference[oaicite:5]{index=5}
        futs = [
            bucket_worker_loop.options(num_cpus=args.cpus_per_worker).remote(
                i, queue, tracker, args.stage1_out, args.out_dir, args.max_rows_per_file
            )
            for i in range(args.num_workers)
        ]
        stats = ray.get(futs)
    finally:
        stop_evt.set()
        poll_thread.join(timeout=5.0)

    total_prs = sum(int(s.get("prs_out", 0)) for s in stats)
    total_files = sum(int(s.get("files_out", 0)) for s in stats)
    total_errors = sum(int(s.get("errors", 0)) for s in stats)

    print(
        f"Stage2 done. prs_out={total_prs} parquet_files={total_files} errors={total_errors}",
        file=sys.stderr,
        flush=True,
    )

    ray.shutdown()


if __name__ == "__main__":
    main()
