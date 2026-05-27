#!/usr/bin/env python3
# stage1_sharding_typed_parquet.py
#
# Stage 1: Read gzipped NDJSON/JSONL GitHub event files (*.json.gz),
# keep PR-relevant events, normalize into a typed Parquet schema,
# and write a Hive-partitioned dataset by repo_bucket (= repo_id % buckets).
#
# Output:
#   <out_root>/pr_events/repo_bucket=XXXX/pr_events_wWWW_0000000000.parquet
#   <out_root>/other_events/repo_bucket=XXXX/other_events_wWWW_0000000000.parquet
#
# Key goals:
#   - fast Stage2 lookup by repo_bucket + Parquet row-group pruning on repo_id
#   - minimal small-file explosion (flush by rows, row_group_size)
#   - no "too many open files" (no persistent writers per bucket; buffered flush)
#   - optional extra_json auditing to detect unmapped payload keys
#
# Notes:
#   - Hive partitioning uses key=value directories (repo_bucket=0000).  (PyArrow dataset supports this) :contentReference[oaicite:3]{index=3}
#   - Parquet writer supports row_group_size to control row-group boundaries. :contentReference[oaicite:4]{index=4}

from __future__ import annotations

import argparse
import gzip
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from collections import defaultdict

import orjson
import ray

import pyarrow as pa
import pyarrow.parquet as pq

import time
import threading
from typing import Callable, Any, Optional, Iterable, List, Dict, Tuple


# ----------------------------
# Event type sets
# ----------------------------
PR_TYPES = {
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
    "IssueCommentEvent",  # only PR issues retained
    "CommitCommentEvent",
    "StatusEvent",
    "status",
    "CheckRunEvent",
    "check_run",
    "CheckSuiteEvent",
    "check_suite",
    "PushEvent",
}

DIRECT_PR_TYPES = {
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
    "IssueCommentEvent",  # only PR issue comments are direct
}

CHECK_TYPES = {"CheckRunEvent", "check_run", "CheckSuiteEvent", "check_suite"}
SHA_JOIN_TYPES = {
    "StatusEvent",
    "status",
    "CheckRunEvent",
    "check_run",
    "CheckSuiteEvent",
    "check_suite",
    "CommitCommentEvent",
}


# ----------------------------
# Ray init
# ----------------------------
def connect(addr: Optional[str]):
    return ray.init(
        address=(addr or "auto"), namespace="pr-pipeline", log_to_driver=False
    )


# ----------------------------
# Input discovery (gzipped ndjson/jsonl)
# ----------------------------
def discover_inputs(root: str) -> List[str]:
    r = Path(root)
    globs = ["**/*.json.gz", "**/*.jsonl.gz", "**/*.ndjson.gz"]
    out: List[str] = []
    for g in globs:
        out.extend(str(p) for p in r.glob(g) if p.is_file())
    return sorted(out)


def iter_gz_ndjson(
    path: str,
    *,
    # called as on_bad_line(lineno, raw_bytes, exception)
    on_bad_line: Optional[Callable[[int, bytes, Exception], None]] = None,
    # stop reading file if too many bad JSON lines
    max_bad_lines: Optional[int] = 10_000,
) -> Iterable[dict]:
    """
    Stream gzipped NDJSON/JSONL and yield parsed dicts.
    - Skips malformed JSON lines (orjson.JSONDecodeError).
    - Optionally aborts file after max_bad_lines.
    - Still streams line-by-line via gzip (no full-file read).  :contentReference[oaicite:2]{index=2}
    """
    bad = 0
    # gzip.open() returns a file-like object that decompresses on the fly. :contentReference[oaicite:3]{index=3}
    with gzip.open(path, "rb") as fh:
        for lineno, line in enumerate(fh, start=1):
            if not line or line == b"\n":
                continue
            line = line.strip()
            if not line:
                continue
            try:
                yield orjson.loads(line)
            except orjson.JSONDecodeError as e:
                # orjson raises JSONDecodeError on invalid/truncated JSON. :contentReference[oaicite:4]{index=4}
                bad += 1
                if on_bad_line is not None:
                    on_bad_line(lineno, line, e)
                if max_bad_lines is not None and bad >= max_bad_lines:
                    # Stop reading this file; caller can treat as “file has too many corrupt lines”.
                    return


# ----------------------------
# Progress tracker Actor (ADD this)
# ----------------------------
@ray.remote
class ProgressTracker:
    """
    Actor that tracks:
      - how many files assigned per worker
      - started/completed counts
      - bad json line counts, bad file counts
      - event totals (optional)
    Tasks update it; driver polls it and prints progress.
    Ray actors are stateful workers and are the standard pattern for shared state. :contentReference[oaicite:5]{index=5}
    """

    def __init__(self, num_workers: int, total_files: int):
        self.num_workers = num_workers
        self.total_files = total_files
        self.t0 = time.time()

        # worker_id -> stats
        self.workers: Dict[int, Dict[str, Any]] = {
            i: {
                "assigned": 0,
                "started": 0,
                "done": 0,
                "bad_lines": 0,
                "bad_files": 0,
                "events_in": 0,
                "kept": 0,
                "current_file": None,
                "last_update": None,
            }
            for i in range(num_workers)
        }

    def register_worker(self, worker_id: int, assigned_files: int) -> None:
        w = self.workers[worker_id]
        w["assigned"] = int(assigned_files)
        w["last_update"] = time.time()

    def file_started(self, worker_id: int, path: str) -> None:
        w = self.workers[worker_id]
        w["started"] += 1
        w["current_file"] = path
        w["last_update"] = time.time()

    def file_done(
        self,
        worker_id: int,
        path: str,
        *,
        events_in: int,
        kept: int,
        bad_lines: int,
    ) -> None:
        w = self.workers[worker_id]
        w["done"] += 1
        w["events_in"] += int(events_in)
        w["kept"] += int(kept)
        w["bad_lines"] += int(bad_lines)
        w["current_file"] = None
        w["last_update"] = time.time()

    def file_failed(self, worker_id: int, path: str, err: str) -> None:
        w = self.workers[worker_id]
        w["bad_files"] += 1
        # count as done so progress doesn't freeze if you skip a file
        w["done"] += 1
        w["current_file"] = None
        w["last_update"] = time.time()

    def snapshot(self) -> Dict[str, Any]:
        # shallow copy is fine for printing
        elapsed = time.time() - self.t0
        total_done = sum(w["done"] for w in self.workers.values())
        total_started = sum(w["started"] for w in self.workers.values())
        total_bad_lines = sum(w["bad_lines"] for w in self.workers.values())
        total_bad_files = sum(w["bad_files"] for w in self.workers.values())

        return {
            "elapsed_s": elapsed,
            "total_files": self.total_files,
            "total_started": total_started,
            "total_done": total_done,
            "total_bad_lines": total_bad_lines,
            "total_bad_files": total_bad_files,
            "workers": self.workers,
        }


# ----------------------------
# Driver-side polling thread (ADD this)
# ----------------------------
def start_progress_polling(
    tracker: "ray.actor.ActorHandle",
    *,
    interval_s: float = 10.0,
    stream=sys.stderr,
) -> Tuple[threading.Event, threading.Thread]:
    """
    Starts a background thread in the driver that polls the ProgressTracker actor
    and prints status periodically.

    Returns (stop_event, thread). Set stop_event to stop, then join thread.
    """
    stop = threading.Event()

    def _fmt_snapshot(snap: Dict[str, Any]) -> str:
        elapsed = snap["elapsed_s"]
        done = snap["total_done"]
        total = snap["total_files"]
        badl = snap["total_bad_lines"]
        badf = snap["total_bad_files"]
        rate = done / elapsed if elapsed > 0 else 0.0

        # Per-worker brief
        ws = snap["workers"]
        per = []
        for wid in sorted(ws.keys()):
            w = ws[wid]
            per.append(
                f"w{wid:03d} {w['done']}/{w['assigned']} done "
                f"(bad_lines={w['bad_lines']}, bad_files={w['bad_files']})"
            )
        per_str = " | ".join(per[:8]) + (" | ..." if len(per) > 8 else "")

        return (
            f"[progress] {done}/{total} files done "
            f"(started={snap['total_started']}, bad_lines={badl}, bad_files={badf}) "
            f"elapsed={elapsed:.1f}s rate={rate:.2f} files/s\n"
            f"          {per_str}"
        )

    def _run():
        # First print quickly, then interval
        next_t = 0.0
        while not stop.is_set():
            now = time.time()
            if now >= next_t:
                try:
                    snap = ray.get(tracker.snapshot.remote())
                    print(_fmt_snapshot(snap), file=stream, flush=True)
                except Exception as e:
                    print(f"[progress] polling error: {e!r}", file=stream, flush=True)
                next_t = now + interval_s
            stop.wait(0.2)

    t = threading.Thread(target=_run, name="progress-poller", daemon=True)
    t.start()
    return stop, t


# ----------------------------
# Basic coercions
# ----------------------------
def _i64(x: Any) -> Optional[int]:
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        return int(x)
    if isinstance(x, str) and x.isdigit():
        return int(x)
    return None


def _b(x: Any) -> Optional[bool]:
    return x if isinstance(x, bool) else None


def _s(x: Any) -> Optional[str]:
    return x if isinstance(x, str) else None


# ----------------------------
# Extra JSON (audit unmapped keys)
# ----------------------------
def extra_json(obj: Any, keep_keys: set, enabled: bool) -> Optional[bytes]:
    if not enabled:
        return None
    if not isinstance(obj, dict) or not obj:
        return None
    extra = {k: v for k, v in obj.items() if k not in keep_keys}
    if not extra:
        return None
    return orjson.dumps(extra)


# ----------------------------
# Normalized nested types (wide but finite)
# ----------------------------
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
        pa.field("license_extra_json", pa.binary()),
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
        pa.field("label_extra_json", pa.binary()),
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
        pa.field("id", pa.int64()),
        pa.field("number", pa.int64()),
        pa.field("state", pa.string()),
        pa.field("title", pa.string()),
        pa.field("body", pa.string()),
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
        pa.field("commit_id", pa.string()),  # CommitCommentEvent uses commit_id
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

check_pr_ref_t = pa.struct(
    [
        pa.field("number", pa.int64()),
        pa.field("url", pa.string()),
        pa.field("head_sha", pa.string()),
        pa.field("check_pr_ref_extra_json", pa.binary()),
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
        pa.field("pull_requests", pa.list_(check_pr_ref_t)),
        pa.field("check_run_extra_json", pa.binary()),
    ]
)

check_suite_t = pa.struct(
    [
        pa.field("id", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("conclusion", pa.string()),
        pa.field("head_sha", pa.string()),
        pa.field("pull_requests", pa.list_(check_pr_ref_t)),
        pa.field("check_suite_extra_json", pa.binary()),
    ]
)

# ----------------------------
# Stage1 event schema (typed + join helpers)
# ----------------------------
SCHEMA = pa.schema(
    [
        # Partition key (also stored as a column for convenience)
        pa.field("repo_bucket", pa.int32()),
        # Repo identity (may be null if missing; those rows are dropped by default)
        pa.field("repo_id", pa.int64()),
        pa.field("repo_name", pa.string()),
        # Common event header
        pa.field("event_id", pa.string()),
        pa.field("type", pa.string()),
        pa.field("created_at", pa.string()),
        pa.field("public", pa.bool_()),
        pa.field("actor", user_t),
        # Join helpers (derived)
        pa.field("direct_pr_number", pa.int64()),
        pa.field("check_pr_numbers", pa.list_(pa.int64())),
        pa.field("join_sha", pa.string()),
        pa.field("push_ref", pa.string()),
        pa.field("push_head", pa.string()),
        pa.field("push_commit_shas", pa.list_(pa.string())),
        pa.field("action", pa.string()),
        # Typed payload objects (nullable)
        pa.field("pull_request", pull_request_t),
        pa.field("issue", issue_t),
        pa.field("comment", comment_t),
        pa.field("review", review_t),
        pa.field("push", push_t),
        pa.field("status", status_t),
        pa.field("check_run", check_run_t),
        pa.field("check_suite", check_suite_t),
        # Optional audits of unmapped keys
        pa.field("event_extra_json", pa.binary()),
        pa.field("payload_extra_json", pa.binary()),
    ]
)


# ----------------------------
# Normalizers
# ----------------------------
USER_KEYS = {"login", "id", "url", "html_url", "avatar_url", "type", "site_admin"}


def norm_user(u: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(u, dict) or not u:
        return None
    return {
        "login": _s(u.get("login")),
        "id": _i64(u.get("id")),
        "url": _s(u.get("url")),
        "html_url": _s(u.get("html_url")),
        "avatar_url": _s(u.get("avatar_url")),
        "type": _s(u.get("type")),
        "site_admin": _b(u.get("site_admin")),
        "user_extra_json": extra_json(u, USER_KEYS, keep_extra),
    }


LICENSE_KEYS = {"key", "name", "spdx_id", "url", "node_id"}


def norm_license(x: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(x, dict) or not x:
        return None
    out = {
        "key": _s(x.get("key")),
        "name": _s(x.get("name")),
        "spdx_id": _s(x.get("spdx_id")),
        "url": _s(x.get("url")),
        "node_id": _s(x.get("node_id")),
        "license_extra_json": extra_json(x, LICENSE_KEYS, keep_extra),
    }
    return out


REPO_KEYS = {
    "id",
    "node_id",
    "name",
    "full_name",
    "private",
    "fork",
    "archived",
    "disabled",
    "description",
    "homepage",
    "language",
    "default_branch",
    "html_url",
    "url",
    "created_at",
    "updated_at",
    "pushed_at",
    "size",
    "stargazers_count",
    "watchers_count",
    "forks_count",
    "open_issues_count",
    "has_issues",
    "has_projects",
    "has_downloads",
    "has_wiki",
    "has_pages",
    "license",
    "owner",
}


def norm_repo(r: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(r, dict) or not r:
        return None
    return {
        "id": _i64(r.get("id")),
        "node_id": _s(r.get("node_id")),
        "name": _s(r.get("name")),
        "full_name": _s(r.get("full_name")),
        "private": _b(r.get("private")),
        "fork": _b(r.get("fork")),
        "archived": _b(r.get("archived")),
        "disabled": _b(r.get("disabled")),
        "description": _s(r.get("description")),
        "homepage": _s(r.get("homepage")),
        "language": _s(r.get("language")),
        "default_branch": _s(r.get("default_branch")),
        "html_url": _s(r.get("html_url")),
        "url": _s(r.get("url")),
        "created_at": _s(r.get("created_at")),
        "updated_at": _s(r.get("updated_at")),
        "pushed_at": _s(r.get("pushed_at")),
        "size": _i64(r.get("size")),
        "stargazers_count": _i64(r.get("stargazers_count")),
        "watchers_count": _i64(r.get("watchers_count")),
        "forks_count": _i64(r.get("forks_count")),
        "open_issues_count": _i64(r.get("open_issues_count")),
        "has_issues": _b(r.get("has_issues")),
        "has_projects": _b(r.get("has_projects")),
        "has_downloads": _b(r.get("has_downloads")),
        "has_wiki": _b(r.get("has_wiki")),
        "has_pages": _b(r.get("has_pages")),
        "license": norm_license(r.get("license"), keep_extra),
        "owner": norm_user(r.get("owner"), keep_extra),
        "repo_extra_json": extra_json(r, REPO_KEYS, keep_extra),
    }


REF_KEYS = {"label", "ref", "sha", "user", "repo"}


def norm_pr_ref(x: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(x, dict) or not x:
        return None
    return {
        "label": _s(x.get("label")),
        "ref": _s(x.get("ref")),
        "sha": _s(x.get("sha")),
        "user": norm_user(x.get("user"), keep_extra),
        "repo": norm_repo(x.get("repo"), keep_extra),
        "ref_extra_json": extra_json(x, REF_KEYS, keep_extra),
    }


LABEL_KEYS = {"id", "node_id", "url", "name", "color", "default", "description"}


def norm_label(x: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(x, dict) or not x:
        return None
    return {
        "id": _i64(x.get("id")),
        "node_id": _s(x.get("node_id")),
        "url": _s(x.get("url")),
        "name": _s(x.get("name")),
        "color": _s(x.get("color")),
        "default": _b(x.get("default")),
        "description": _s(x.get("description")),
        "label_extra_json": extra_json(x, LABEL_KEYS, keep_extra),
    }


MILESTONE_KEYS = {
    "id",
    "node_id",
    "number",
    "title",
    "description",
    "state",
    "html_url",
    "created_at",
    "updated_at",
    "due_on",
    "closed_at",
    "creator",
}


def norm_milestone(x: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(x, dict) or not x:
        return None
    return {
        "id": _i64(x.get("id")),
        "node_id": _s(x.get("node_id")),
        "number": _i64(x.get("number")),
        "title": _s(x.get("title")),
        "description": _s(x.get("description")),
        "state": _s(x.get("state")),
        "html_url": _s(x.get("html_url")),
        "created_at": _s(x.get("created_at")),
        "updated_at": _s(x.get("updated_at")),
        "due_on": _s(x.get("due_on")),
        "closed_at": _s(x.get("closed_at")),
        "creator": norm_user(x.get("creator"), keep_extra),
        "milestone_extra_json": extra_json(x, MILESTONE_KEYS, keep_extra),
    }


PR_KEYS = {
    "id",
    "node_id",
    "number",
    "state",
    "title",
    "body",
    "draft",
    "locked",
    "html_url",
    "url",
    "comments_url",
    "review_comments_url",
    "commits_url",
    "statuses_url",
    "created_at",
    "updated_at",
    "closed_at",
    "merged_at",
    "merge_commit_sha",
    "merged",
    "author_association",
    "user",
    "labels",
    "assignees",
    "requested_reviewers",
    "milestone",
    "additions",
    "deletions",
    "changed_files",
    "commits",
    "comments",
    "review_comments",
    "base",
    "head",
}


def norm_pull_request(pr: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(pr, dict) or not pr:
        return None
    labels = pr.get("labels")
    assignees = pr.get("assignees")
    req = pr.get("requested_reviewers")
    return {
        "id": _i64(pr.get("id")),
        "node_id": _s(pr.get("node_id")),
        "number": _i64(pr.get("number")),
        "state": _s(pr.get("state")),
        "title": _s(pr.get("title")),
        "body": _s(pr.get("body")),
        "draft": _b(pr.get("draft")),
        "locked": _b(pr.get("locked")),
        "html_url": _s(pr.get("html_url")),
        "url": _s(pr.get("url")),
        "comments_url": _s(pr.get("comments_url")),
        "review_comments_url": _s(pr.get("review_comments_url")),
        "commits_url": _s(pr.get("commits_url")),
        "statuses_url": _s(pr.get("statuses_url")),
        "created_at": _s(pr.get("created_at")),
        "updated_at": _s(pr.get("updated_at")),
        "closed_at": _s(pr.get("closed_at")),
        "merged_at": _s(pr.get("merged_at")),
        "merge_commit_sha": _s(pr.get("merge_commit_sha")),
        "merged": _b(pr.get("merged")),
        "author_association": _s(pr.get("author_association")),
        "user": norm_user(pr.get("user"), keep_extra),
        "labels": [norm_label(x, keep_extra) for x in labels]
        if isinstance(labels, list)
        else None,
        "assignees": [norm_user(x, keep_extra) for x in assignees]
        if isinstance(assignees, list)
        else None,
        "requested_reviewers": [norm_user(x, keep_extra) for x in req]
        if isinstance(req, list)
        else None,
        "milestone": norm_milestone(pr.get("milestone"), keep_extra),
        "additions": _i64(pr.get("additions")),
        "deletions": _i64(pr.get("deletions")),
        "changed_files": _i64(pr.get("changed_files")),
        "commits": _i64(pr.get("commits")),
        "comments": _i64(pr.get("comments")),
        "review_comments": _i64(pr.get("review_comments")),
        "base": norm_pr_ref(pr.get("base"), keep_extra),
        "head": norm_pr_ref(pr.get("head"), keep_extra),
        "pr_extra_json": extra_json(pr, PR_KEYS, keep_extra),
    }


ISSUE_KEYS = {
    "id",
    "number",
    "state",
    "title",
    "body",
    "html_url",
    "updated_at",
    "pull_request",
}


def norm_issue(issue: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(issue, dict) or not issue:
        return None
    return {
        "id": _i64(issue.get("id")),
        "number": _i64(issue.get("number")),
        "state": _s(issue.get("state")),
        "title": _s(issue.get("title")),
        "body": _s(issue.get("body")),
        "html_url": _s(issue.get("html_url")),
        "updated_at": _s(issue.get("updated_at")),
        "issue_extra_json": extra_json(issue, ISSUE_KEYS, keep_extra),
    }


COMMENT_KEYS = {
    "id",
    "html_url",
    "url",
    "created_at",
    "updated_at",
    "author_association",
    "body",
    "user",
    "commit_id",
}


def norm_comment(c: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(c, dict) or not c:
        return None
    return {
        "id": _i64(c.get("id")),
        "html_url": _s(c.get("html_url")),
        "url": _s(c.get("url")),
        "created_at": _s(c.get("created_at")),
        "updated_at": _s(c.get("updated_at")),
        "author_association": _s(c.get("author_association")),
        "body": _s(c.get("body")),
        "commit_id": _s(c.get("commit_id")),
        "user": norm_user(c.get("user"), keep_extra),
        "comment_extra_json": extra_json(c, COMMENT_KEYS, keep_extra),
    }


REVIEW_KEYS = {"id", "state", "submitted_at", "body", "html_url", "user"}


def norm_review(r: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(r, dict) or not r:
        return None
    return {
        "id": _i64(r.get("id")),
        "state": _s(r.get("state")),
        "submitted_at": _s(r.get("submitted_at")),
        "body": _s(r.get("body")),
        "html_url": _s(r.get("html_url")),
        "user": norm_user(r.get("user"), keep_extra),
        "review_extra_json": extra_json(r, REVIEW_KEYS, keep_extra),
    }


AUTHOR_KEYS = {"name", "email", "username"}


def norm_commit_author(a: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(a, dict) or not a:
        return None
    return {
        "name": _s(a.get("name")),
        "email": _s(a.get("email")),
        "username": _s(a.get("username")),
        "author_extra_json": extra_json(a, AUTHOR_KEYS, keep_extra),
    }


PUSH_COMMIT_KEYS = {"sha", "message", "url", "distinct", "author"}


def norm_push_commit(c: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(c, dict) or not c:
        return None
    return {
        "sha": _s(c.get("sha")),
        "message": _s(c.get("message")),
        "url": _s(c.get("url")),
        "distinct": _b(c.get("distinct")),
        "author": norm_commit_author(c.get("author"), keep_extra),
        "commit_extra_json": extra_json(c, PUSH_COMMIT_KEYS, keep_extra),
    }


PUSH_KEYS = {"ref", "before", "head", "size", "distinct_size", "commits", "push_id"}


def norm_push(p: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(p, dict) or not p:
        return None
    commits = p.get("commits")
    out_commits = (
        [norm_push_commit(x, keep_extra) for x in commits]
        if isinstance(commits, list)
        else None
    )
    return {
        "ref": _s(p.get("ref")),
        "before": _s(p.get("before")),
        "head": _s(p.get("head")),
        "size": _i64(p.get("size")),
        "distinct_size": _i64(p.get("distinct_size")),
        "commits": out_commits,
        "push_extra_json": extra_json(p, PUSH_KEYS, keep_extra),
    }


STATUS_KEYS = {"sha", "state", "context", "description", "target_url"}


def norm_status(p: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(p, dict) or not p:
        return None
    return {
        "sha": _s(p.get("sha")),
        "state": _s(p.get("state")),
        "context": _s(p.get("context")),
        "description": _s(p.get("description")),
        "target_url": _s(p.get("target_url")),
        "status_extra_json": extra_json(p, STATUS_KEYS, keep_extra),
    }


CHECK_PR_KEYS = {"number", "url", "head_sha"}


def norm_check_pr_ref(x: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(x, dict) or not x:
        return None
    return {
        "number": _i64(x.get("number")),
        "url": _s(x.get("url")),
        "head_sha": _s(x.get("head_sha")),
        "check_pr_ref_extra_json": extra_json(x, CHECK_PR_KEYS, keep_extra),
    }


CHECK_RUN_KEYS = {
    "id",
    "name",
    "status",
    "conclusion",
    "head_sha",
    "details_url",
    "external_id",
    "pull_requests",
}


def norm_check_run(cr: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(cr, dict) or not cr:
        return None
    prs = cr.get("pull_requests")
    out_prs = (
        [norm_check_pr_ref(x, keep_extra) for x in prs]
        if isinstance(prs, list)
        else None
    )
    return {
        "id": _i64(cr.get("id")),
        "name": _s(cr.get("name")),
        "status": _s(cr.get("status")),
        "conclusion": _s(cr.get("conclusion")),
        "head_sha": _s(cr.get("head_sha")),
        "details_url": _s(cr.get("details_url")),
        "external_id": _s(cr.get("external_id")),
        "pull_requests": out_prs,
        "check_run_extra_json": extra_json(cr, CHECK_RUN_KEYS, keep_extra),
    }


CHECK_SUITE_KEYS = {"id", "status", "conclusion", "head_sha", "pull_requests"}


def norm_check_suite(cs: Any, keep_extra: bool) -> Optional[dict]:
    if not isinstance(cs, dict) or not cs:
        return None
    prs = cs.get("pull_requests")
    out_prs = (
        [norm_check_pr_ref(x, keep_extra) for x in prs]
        if isinstance(prs, list)
        else None
    )
    return {
        "id": _i64(cs.get("id")),
        "status": _s(cs.get("status")),
        "conclusion": _s(cs.get("conclusion")),
        "head_sha": _s(cs.get("head_sha")),
        "pull_requests": out_prs,
        "check_suite_extra_json": extra_json(cs, CHECK_SUITE_KEYS, keep_extra),
    }


# ----------------------------
# Repo id / name
# ----------------------------
def event_repo_id(ev: dict) -> Optional[int]:
    rp = ev.get("repo")
    if isinstance(rp, dict):
        rid = _i64(rp.get("id"))
        if rid is not None:
            return rid
    rid2 = _i64(ev.get("repo_id"))
    return rid2


def event_repo_name(ev: dict) -> Optional[str]:
    rp = ev.get("repo")
    if isinstance(rp, dict):
        nm = rp.get("name")
        if isinstance(nm, str):
            return nm
    rn = ev.get("repo_name")
    return rn if isinstance(rn, str) else None


# ----------------------------
# Relevance filter
# ----------------------------
def is_pr_relevant(ev: dict) -> bool:
    et = ev.get("type")
    if not isinstance(et, str) or et not in PR_TYPES:
        return False
    if et == "IssueCommentEvent":
        issue = (ev.get("payload") or {}).get("issue") or {}
        return isinstance(issue, dict) and ("pull_request" in issue)
    return True


# ----------------------------
# Join helpers extraction
# ----------------------------
def direct_pr_number(ev: dict) -> Optional[int]:
    et = ev.get("type")
    p = ev.get("payload") or {}
    if not isinstance(p, dict):
        return None

    if et == "PullRequestEvent":
        return _i64(p.get("number"))

    if et in ("PullRequestReviewEvent", "PullRequestReviewCommentEvent"):
        pr = p.get("pull_request") or {}
        if isinstance(pr, dict):
            return _i64(pr.get("number"))

    if et == "IssueCommentEvent":
        issue = p.get("issue") or {}
        if isinstance(issue, dict) and ("pull_request" in issue):
            return _i64(issue.get("number"))

    return None


def check_pr_numbers(ev: dict) -> Optional[List[int]]:
    et = ev.get("type")
    p = ev.get("payload") or {}
    if not isinstance(p, dict):
        return None

    out: List[int] = []

    if et in ("CheckRunEvent", "check_run"):
        cr = p.get("check_run") or {}
        if isinstance(cr, dict):
            prs = cr.get("pull_requests")
            if isinstance(prs, list):
                for pr in prs:
                    if isinstance(pr, dict):
                        n = _i64(pr.get("number"))
                        if n is not None:
                            out.append(n)

    if et in ("CheckSuiteEvent", "check_suite"):
        cs = p.get("check_suite") or {}
        if isinstance(cs, dict):
            prs = cs.get("pull_requests")
            if isinstance(prs, list):
                for pr in prs:
                    if isinstance(pr, dict):
                        n = _i64(pr.get("number"))
                        if n is not None:
                            out.append(n)

    if not out:
        return None
    # unique but stable
    seen = set()
    uniq: List[int] = []
    for x in out:
        if x not in seen:
            uniq.append(x)
            seen.add(x)
    return uniq


def join_sha(ev: dict) -> Optional[str]:
    et = ev.get("type")
    p = ev.get("payload") or {}
    if not isinstance(p, dict):
        return None

    if et == "CommitCommentEvent":
        c = p.get("comment") or {}
        if isinstance(c, dict):
            return _s(c.get("commit_id"))

    if et in ("StatusEvent", "status"):
        return _s(p.get("sha"))

    if et in ("CheckRunEvent", "check_run"):
        cr = p.get("check_run") or {}
        if isinstance(cr, dict):
            return _s(cr.get("head_sha"))

    if et in ("CheckSuiteEvent", "check_suite"):
        cs = p.get("check_suite") or {}
        if isinstance(cs, dict):
            return _s(cs.get("head_sha"))

    return None


def push_helpers(ev: dict) -> Tuple[Optional[str], Optional[str], Optional[List[str]]]:
    if ev.get("type") != "PushEvent":
        return None, None, None
    p = ev.get("payload") or {}
    if not isinstance(p, dict):
        return None, None, None
    ref = _s(p.get("ref"))
    head = _s(p.get("head"))
    shas: List[str] = []
    commits = p.get("commits")
    if isinstance(commits, list):
        for c in commits:
            if isinstance(c, dict):
                s = _s(c.get("sha"))
                if s:
                    shas.append(s)
    if not shas:
        shas_out = None
    else:
        # unique stable
        seen = set()
        uniq = []
        for s in shas:
            if s not in seen:
                uniq.append(s)
                seen.add(s)
        shas_out = uniq
    return ref, head, shas_out


# ----------------------------
# Event normalization -> Stage1 row
# ----------------------------
EVENT_TOP_KEYS = {
    "id",
    "type",
    "actor",
    "repo",
    "repo_id",
    "repo_name",
    "created_at",
    "public",
    "payload",
    "org",
}


def norm_event_row(ev: dict, repo_bucket: int, keep_extra: bool) -> Optional[dict]:
    if not is_pr_relevant(ev):
        return None

    rid = event_repo_id(ev)
    if rid is None:
        return None

    et = ev.get("type")
    p = ev.get("payload") or {}
    if not isinstance(et, str) or not isinstance(p, dict):
        return None

    # Build typed payload pieces
    pr_obj = None
    issue_obj = None
    comment_obj = None
    review_obj = None
    push_obj = None
    status_obj = None
    check_run_obj = None
    check_suite_obj = None

    action = _s(p.get("action"))

    if et == "PullRequestEvent":
        pr_obj = norm_pull_request(p.get("pull_request"), keep_extra)

    elif et == "PullRequestReviewEvent":
        review_obj = norm_review(p.get("review"), keep_extra)
        pr_obj = norm_pull_request(p.get("pull_request"), keep_extra)

    elif et == "PullRequestReviewCommentEvent":
        comment_obj = norm_comment(p.get("comment"), keep_extra)
        pr_obj = norm_pull_request(p.get("pull_request"), keep_extra)

    elif et == "IssueCommentEvent":
        # only PR issues kept by is_pr_relevant
        issue_obj = norm_issue(p.get("issue"), keep_extra)
        comment_obj = norm_comment(p.get("comment"), keep_extra)

    elif et == "CommitCommentEvent":
        comment_obj = norm_comment(p.get("comment"), keep_extra)

    elif et in ("StatusEvent", "status"):
        status_obj = norm_status(p, keep_extra)

    elif et in ("CheckRunEvent", "check_run"):
        cr = p.get("check_run") or {}
        check_run_obj = norm_check_run(cr, keep_extra) if isinstance(cr, dict) else None

    elif et in ("CheckSuiteEvent", "check_suite"):
        cs = p.get("check_suite") or {}
        check_suite_obj = (
            norm_check_suite(cs, keep_extra) if isinstance(cs, dict) else None
        )

    elif et == "PushEvent":
        push_obj = norm_push(p, keep_extra)

    # Join helpers
    dpr = direct_pr_number(ev) if et in DIRECT_PR_TYPES else None
    cprs = check_pr_numbers(ev) if et in CHECK_TYPES else None
    jsha = join_sha(ev) if et in SHA_JOIN_TYPES else None
    pref, phead, pshas = push_helpers(ev)

    # Payload extra (audit)
    # keep a relatively conservative key set; anything beyond is captured
    PAYLOAD_KEEP = {
        "action",
        "number",
        "pull_request",
        "issue",
        "comment",
        "review",
        "check_run",
        "check_suite",
        "sha",
        "state",
        "context",
        "description",
        "target_url",
        "ref",
        "before",
        "head",
        "size",
        "distinct_size",
        "commits",
        "push_id",
    }
    payload_extra = extra_json(p, PAYLOAD_KEEP, keep_extra)

    row = {
        "repo_bucket": int(repo_bucket),
        "repo_id": int(rid),
        "repo_name": event_repo_name(ev),
        "event_id": _s(ev.get("id")),
        "type": et,
        "created_at": _s(ev.get("created_at")),
        "public": _b(ev.get("public")),
        "actor": norm_user(ev.get("actor"), keep_extra),
        "direct_pr_number": dpr,
        "check_pr_numbers": cprs,
        "join_sha": jsha,
        "push_ref": pref,
        "push_head": phead,
        "push_commit_shas": pshas,
        "action": action,
        "pull_request": pr_obj,
        "issue": issue_obj,
        "comment": comment_obj,
        "review": review_obj,
        "push": push_obj,
        "status": status_obj,
        "check_run": check_run_obj,
        "check_suite": check_suite_obj,
        "event_extra_json": extra_json(ev, EVENT_TOP_KEYS, keep_extra),
        "payload_extra_json": payload_extra,
    }
    return row


# ----------------------------
# Buffered writer: big flushes, sorted by repo_id for pruning
# ----------------------------
@dataclass
class BucketBuffers:
    # dataset_root points to pr_events/ or other_events/
    dataset_root: str
    worker_id: int
    row_group_size: int
    flush_rows: int
    compression: str
    keep_extra_json: bool

    # bucket -> list[dict]
    bufs: Dict[int, List[dict]] = field(default_factory=lambda: defaultdict(list))
    # bucket -> next seq
    seq: Dict[int, int] = field(default_factory=lambda: defaultdict(int))

    def _dir_for_bucket(self, b: int) -> str:
        # Hive partition directory
        return os.path.join(self.dataset_root, f"repo_bucket={b:04d}")

    def _path_for(self, b: int) -> str:
        s = self.seq[b]
        self.seq[b] = s + 1
        # worker id embedded; unique even with concurrent writers
        fname = f"{Path(self.dataset_root).name}_w{self.worker_id:03d}_{s:010d}.parquet"
        return os.path.join(self._dir_for_bucket(b), fname)

    def add(self, b: int, row: dict):
        buf = self.bufs[b]
        buf.append(row)
        if len(buf) >= self.flush_rows:
            self.flush_bucket(b)

    def flush_bucket(self, b: int):
        buf = self.bufs.get(b)
        if not buf:
            return
        # sort to help row-group stats pruning by repo_id scans
        buf.sort(
            key=lambda r: (
                r.get("repo_id") or -1,
                r.get("created_at") or "",
                r.get("event_id") or "",
            )
        )

        out_dir = self._dir_for_bucket(b)
        os.makedirs(out_dir, exist_ok=True)
        out_path = self._path_for(b)

        table = pa.Table.from_pylist(buf, schema=SCHEMA)
        pq.write_table(
            table,
            out_path,
            compression=self.compression,
            use_dictionary=True,
            write_statistics=True,
            row_group_size=self.row_group_size,
        )
        buf.clear()

    def flush_all(self):
        for b in list(self.bufs.keys()):
            self.flush_bucket(b)


# ----------------------------
# Worker: process a chunk of files (MODIFY your worker_process_files)
# ----------------------------
@ray.remote
def worker_process_files(
    worker_id: int,
    files: List[str],
    out_root: str,
    buckets: int,
    row_group_size: int,
    flush_rows: int,
    keep_extra_json: bool,
    compression: str = "zstd",
    # NEW:
    tracker=None,  # ActorHandle
    max_bad_lines_per_file: int = 10_000,
    log_bad_line_every: int = 200,
) -> dict:
    # Register assigned work once
    if tracker is not None:
        tracker.register_worker.remote(worker_id, len(files))

    pr_root = os.path.join(out_root, "pr_events")
    other_root = os.path.join(out_root, "other_events")

    prw = BucketBuffers(
        dataset_root=pr_root,
        worker_id=worker_id,
        row_group_size=row_group_size,
        flush_rows=flush_rows,
        compression=compression,
        keep_extra_json=keep_extra_json,
    )
    ow = BucketBuffers(
        dataset_root=other_root,
        worker_id=worker_id,
        row_group_size=row_group_size,
        flush_rows=flush_rows,
        compression=compression,
        keep_extra_json=keep_extra_json,
    )

    total = kept = kept_pr = kept_other = 0
    bad_files = 0
    bad_lines_total = 0

    # Worker-visible log (goes to Ray worker logs; aggregated stats go to driver via tracker)
    print(
        f"[worker {worker_id}] starting: assigned_files={len(files)}",
        file=sys.stderr,
        flush=True,
    )

    for fp in files:
        if tracker is not None:
            tracker.file_started.remote(worker_id, fp)

        file_total = 0
        file_kept = 0
        file_bad_lines = 0

        def _on_bad_line(lineno: int, raw: bytes, exc: Exception) -> None:
            nonlocal file_bad_lines
            file_bad_lines += 1
            # occasional local logging so you can find the file/line in worker logs
            if log_bad_line_every and (
                file_bad_lines == 1 or file_bad_lines % log_bad_line_every == 0
            ):
                snippet = raw[:200]
                print(
                    f"[worker {worker_id}] bad json {fp}:{lineno} (bad_lines_in_file={file_bad_lines}): {exc}; "
                    f"startswith={snippet!r}",
                    file=sys.stderr,
                    flush=True,
                )

        try:
            for ev in iter_gz_ndjson(
                fp,
                on_bad_line=_on_bad_line,
                max_bad_lines=max_bad_lines_per_file,
            ):
                file_total += 1
                total += 1

                if not is_pr_relevant(ev):
                    continue

                rid = event_repo_id(ev)
                if rid is None:
                    continue

                b = int(rid % buckets)

                row = norm_event_row(ev, repo_bucket=b, keep_extra=keep_extra_json)
                if row is None:
                    continue

                file_kept += 1
                kept += 1
                et = row.get("type")

                if et in DIRECT_PR_TYPES:
                    prw.add(b, row)
                    kept_pr += 1
                else:
                    ow.add(b, row)
                    kept_other += 1

        except (OSError, EOFError) as e:
            # gzip corruption / truncation; choose: skip this file and continue
            bad_files += 1
            err = f"gzip read failed: {e!r}"
            print(f"[worker {worker_id}] {err} file={fp}", file=sys.stderr, flush=True)
            if tracker is not None:
                tracker.file_failed.remote(worker_id, fp, err)
            continue

        bad_lines_total += file_bad_lines

        if tracker is not None:
            tracker.file_done.remote(
                worker_id,
                fp,
                events_in=file_total,
                kept=file_kept,
                bad_lines=file_bad_lines,
            )

    prw.flush_all()
    ow.flush_all()

    print(
        f"[worker {worker_id}] finished: files={len(files)} events_in={total} kept={kept} "
        f"bad_lines={bad_lines_total} bad_files={bad_files}",
        file=sys.stderr,
        flush=True,
    )

    return {
        "worker": worker_id,
        "files": len(files),
        "events_in": total,
        "kept": kept,
        "kept_pr_events": kept_pr,
        "kept_other_events": kept_other,
        "bad_lines": bad_lines_total,
        "bad_files": bad_files,
    }


# ----------------------------
# CLI
# ----------------------------
def chunkify(xs: List[str], n: int) -> List[List[str]]:
    if n <= 1:
        return [xs]
    out = [[] for _ in range(n)]
    for i, x in enumerate(xs):
        out[i % n].append(x)
    return out


def main():
    ap = argparse.ArgumentParser(
        description="Stage1 (typed parquet): shard gz NDJSON events into Hive-partitioned Parquet by repo_bucket."
    )
    ap.add_argument(
        "--in-root",
        required=True,
        help="Input root containing *.json.gz/*.jsonl.gz/*.ndjson.gz (recursed)",
    )
    ap.add_argument("--out-root", required=True, help="Output root directory")
    ap.add_argument(
        "--buckets",
        type=int,
        default=1024,
        help="repo_bucket count (repo_id %% buckets)",
    )
    ap.add_argument(
        "--num-workers", type=int, default=8, help="Fixed number of Ray workers"
    )
    ap.add_argument(
        "--cpus-per-worker",
        type=int,
        default=4,
        help="Ray logical CPUs reserved per worker task",
    )
    ap.add_argument(
        "--row-group-size", type=int, default=250_000, help="Parquet row_group_size"
    )  # :contentReference[oaicite:5]{index=5}
    ap.add_argument(
        "--flush-rows",
        type=int,
        default=1_000_000,
        help="Rows per (bucket, dataset, worker) Parquet file",
    )
    ap.add_argument(
        "--keep-extra-json",
        action="store_true",
        help="Store *_extra_json audit columns (bigger files; useful for schema inspection)",
    )
    ap.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression codec (default: zstd)",
    )
    ap.add_argument(
        "--ray-address", default="auto", help='Ray address "auto" or "ray://host:10001"'
    )
    # NEW knobs for robustness/progress
    ap.add_argument("--max-bad-lines-per-file", type=int, default=10_000)
    ap.add_argument("--progress-interval-s", type=float, default=10.0)

    args = ap.parse_args()

    connect(args.ray_address)

    inputs = discover_inputs(args.in_root)
    if not inputs:
        print("No input *.json.gz/*.jsonl.gz/*.ndjson.gz files found.", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.out_root, exist_ok=True)

    print(
        f"Stage1: files={len(inputs)} buckets={args.buckets} workers={args.num_workers} "
        f"flush_rows={args.flush_rows} row_group_size={args.row_group_size} keep_extra_json={args.keep_extra_json}",
        file=sys.stderr,
        flush=True,
    )

    parts = chunkify(inputs, args.num_workers)

    # NEW: create tracker actor and start polling in driver
    tracker = ProgressTracker.remote(
        num_workers=args.num_workers, total_files=len(inputs)
    )
    stop_evt, poll_thread = start_progress_polling(
        tracker, interval_s=args.progress_interval_s, stream=sys.stderr
    )

    try:
        futs = [
            worker_process_files.options(num_cpus=args.cpus_per_worker).remote(
                worker_id=i,
                files=parts[i],
                out_root=args.out_root,
                buckets=args.buckets,
                row_group_size=args.row_group_size,
                flush_rows=args.flush_rows,
                keep_extra_json=args.keep_extra_json,
                compression=args.compression,
                tracker=tracker,  # pass actor handle to tasks :contentReference[oaicite:6]{index=6}
                max_bad_lines_per_file=args.max_bad_lines_per_file,
            )
            for i in range(len(parts))
        ]

        stats = ray.get(futs)

    finally:
        # stop polling even if a task fails
        stop_evt.set()
        poll_thread.join(timeout=5.0)

        # Print final snapshot
        try:
            snap = ray.get(tracker.snapshot.remote())
            print("[progress] final snapshot:", snap, file=sys.stderr, flush=True)
        except Exception as e:
            print(
                f"[progress] could not fetch final snapshot: {e!r}",
                file=sys.stderr,
                flush=True,
            )

    total_in = sum(s["events_in"] for s in stats)
    total_kept = sum(s["kept"] for s in stats)
    kept_pr = sum(s["kept_pr_events"] for s in stats)
    kept_other = sum(s["kept_other_events"] for s in stats)
    bad_lines = sum(s.get("bad_lines", 0) for s in stats)
    bad_files = sum(s.get("bad_files", 0) for s in stats)

    print(
        f"Stage1 done. events_in={total_in} kept={total_kept} pr_events={kept_pr} other_events={kept_other} "
        f"bad_lines={bad_lines} bad_files={bad_files} out={args.out_root}",
        file=sys.stderr,
        flush=True,
    )

    ray.shutdown()


if __name__ == "__main__":
    main()
