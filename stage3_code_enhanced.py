#!/usr/bin/env python3
# Stage 3 (git-based code fetcher): consumes Stage 2 bucketed Parquet and emits Stage 3 bucketed Parquet
#
# Input (bucketed Stage2):
#   --in-root/
#     repo_bucket=0000/*.parquet
#     repo_bucket=0001/*.parquet
#     ...
#
# Output (bucketed Stage3):
#   --out-root/
#     repo_bucket=0000/prs_0000000000.parquet   (same naming style as stage2 bucketed, but + "code" column)
#
# Guarantees:
#  - Keeps ALL Stage2 columns exactly as-is.
#  - Adds a "code" struct column per PR row (diffs/commits/tree via git).
#  - NOW ALSO adds full base-version contents for files in code.touched_files (bounded by byte/file caps).
#  - Uses LARGE Arrow types for all text/list fields (large_string/large_list).
#  - Fixed pool of Ray tasks (worker loops) + queue + tracker + periodic progress logging.
#
# Resumable/idempotent:
#  - Per-bucket progress:  out_root/_progress/repo_bucket=####.json
#  - Done marker:          out_root/repo_bucket=####/_DONE.json
#  - Output shards written atomically via .tmp -> rename
#
# Repo-batched behavior:
#  - Stage2 rows sorted by (repo_id, pr_number).
#  - Stream input and process contiguous runs of the same repo_id:
#      - ensure_bare_repo ONCE per repo_id
#      - process all PRs for that repo_id
#      - delete repo_dir right after finishing that repo_id (do not wait for bucket completion)

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import shutil
import socket
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import ray
import pyarrow as pa
import pyarrow.parquet as pq
from ray.util.queue import Queue

DISCUSSION_TYPES = {
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
    "IssueCommentEvent",
}

REPO_BUCKET_RE = re.compile(r"^repo_bucket=(\d+)$")
OUT_FILE_RE = re.compile(r"^prs_(\d{10})\.parquet$")


# ----------------- small utils -----------------
def eprint(*a):
    print(*a, file=sys.stderr, flush=True)


def ensure_dir(p: str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)


def _tail_bytes(b: bytes, limit: int = 4000) -> str:
    if not b:
        return ""
    if len(b) > limit:
        b = b[-limit:]
    try:
        return b.decode("utf-8", "replace")
    except Exception:
        return repr(b)


def _safe_int(x: Any) -> Optional[int]:
    if isinstance(x, int):
        return x
    if isinstance(x, float) and x.is_integer():
        return int(x)
    return None


def _safe_str(x: Any) -> Optional[str]:
    return x if isinstance(x, str) and x else None


def _get_by_path(obj: Any, path: str) -> Any:
    """
    Supports BOTH:
      - flat keys with dots: obj["pull_request.base.sha"]
      - nested dicts: obj["pull_request"]["base"]["sha"]
    """
    if not isinstance(obj, dict) or not path:
        return None

    if path in obj:
        return obj.get(path)

    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        if part not in cur:
            return None
        cur = cur.get(part)
    return cur


def get_any_str(row: Dict[str, Any], paths: List[str]) -> Optional[str]:
    for p in paths:
        v = _get_by_path(row, p)
        s = _safe_str(v)
        if s:
            return s
    return None


def get_any_int(row: Dict[str, Any], paths: List[str]) -> Optional[int]:
    for p in paths:
        v = _get_by_path(row, p)
        i = _safe_int(v)
        if i is not None:
            return i
    return None


def count_discussion_events(row: Dict[str, Any]) -> Optional[int]:
    evs = row.get("events")
    if not isinstance(evs, list):
        return None
    n = 0
    for ev in evs:
        if isinstance(ev, dict):
            t = ev.get("type")
            if t in DISCUSSION_TYPES:
                n += 1
    return n


def redact_url(url: str) -> str:
    return re.sub(r"//([^/@]+)@", "//***:***@", url)


def _atomic_write_text(path: str, text: str) -> None:
    tmp = path + ".tmp"
    ensure_dir(os.path.dirname(path))
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _atomic_write_json(path: str, obj: dict) -> None:
    _atomic_write_text(path, json.dumps(obj, indent=2, sort_keys=True) + "\n")


def _read_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def _utc_now_s() -> str:
    return str(int(time.time()))


# ----------------- git helpers -----------------
@dataclass
class CmdResult:
    rc: int
    out: bytes
    err: bytes


def run_cmd(
    cmd: List[str],
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[int] = None,
    stdin_bytes: Optional[bytes] = None,
) -> CmdResult:
    p = subprocess.Popen(
        cmd,
        cwd=cwd,
        env=env,
        stdin=subprocess.PIPE if stdin_bytes is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        out, err = p.communicate(input=stdin_bytes, timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
        return CmdResult(124, out or b"", (err or b"") + b"\n[TIMEOUT]")
    return CmdResult(p.returncode or 0, out or b"", err or b"")


def git_env() -> Dict[str, str]:
    env = dict(os.environ)
    env["GIT_TERMINAL_PROMPT"] = "0"
    env.setdefault("LC_ALL", "C")
    env.setdefault("LANG", "C")
    return env


def build_clone_url(row: Dict[str, Any], repo_full: str) -> str:
    url = get_any_str(
        row,
        [
            "pull_request.base.repo.clone_url",
            "pull_request.head.repo.clone_url",
            "repo.clone_url",
        ],
    )
    if url:
        if not url.endswith(".git"):
            url = url + ".git"
        return url
    return f"https://github.com/{repo_full}.git"


def ensure_bare_repo(repo_dir: str, clone_url: str) -> Tuple[bool, str]:
    ensure_dir(os.path.dirname(repo_dir))
    env = git_env()

    if os.path.isdir(repo_dir) and os.path.isdir(os.path.join(repo_dir, "objects")):
        res = run_cmd(
            ["git", "fetch", "-p", "origin"], cwd=repo_dir, env=env, timeout=600
        )
        if res.rc == 0:
            return True, ""
        return False, f"git fetch failed rc={res.rc} err={_tail_bytes(res.err)}"

    if os.path.exists(repo_dir):
        shutil.rmtree(repo_dir, ignore_errors=True)

    cmd = [
        "git",
        "clone",
        "--bare",
        "--filter=blob:none",
        "--no-tags",
        clone_url,
        repo_dir,
    ]
    res = run_cmd(cmd, cwd=None, env=env, timeout=900)
    if res.rc != 0:
        return False, f"git clone failed rc={res.rc} err={_tail_bytes(res.err)}"
    return True, ""


def git_has_commit(repo_dir: str, sha: str) -> bool:
    """
    Return True if <sha> exists locally and is a commit object.
    We use 'cat-file -e <sha>^{commit}' which is a common, efficient existence check.
    """
    if not sha or not re.fullmatch(r"[0-9a-f]{40}", sha):
        return False
    env = git_env()
    res = run_cmd(
        ["git", "cat-file", "-e", f"{sha}^{{commit}}"],
        cwd=repo_dir,
        env=env,
        timeout=60,
    )
    return res.rc == 0


def git_fetch_pr_head(repo_dir: str, pr_number: int) -> Tuple[Optional[str], str, str]:
    """
    Best-effort: fetch GitHub PR head ref from the *base repo* remote "origin".
    This is the documented pattern: git fetch origin pull/<ID>/head:<local-ref>.
    Returns (fetched_sha, err_msg, stderr_tail).
    """
    env = git_env()
    local_ref = f"refs/pr/head/{int(pr_number)}"
    # Force-update local ref so reruns are deterministic.
    res = run_cmd(
        ["git", "fetch", "-f", "origin", f"pull/{int(pr_number)}/head:{local_ref}"],
        cwd=repo_dir,
        env=env,
        timeout=600,
    )
    if res.rc != 0:
        return None, f"git fetch PR head failed rc={res.rc}", _tail_bytes(res.err)

    res2 = run_cmd(
        ["git", "rev-parse", local_ref],
        cwd=repo_dir,
        env=env,
        timeout=120,
    )
    if res2.rc != 0:
        return (
            None,
            f"git rev-parse {local_ref} failed rc={res2.rc}",
            _tail_bytes(res2.err),
        )

    sha = res2.out.decode("utf-8", "replace").strip()
    if not sha or not re.fullmatch(r"[0-9a-f]{40}", sha):
        return None, "git rev-parse returned invalid sha", ""
    return sha, "", _tail_bytes(res.err)


def git_merge_base(repo_dir: str, a: str, b: str) -> Optional[str]:
    env = git_env()
    res = run_cmd(["git", "merge-base", a, b], cwd=repo_dir, env=env, timeout=120)
    if res.rc != 0:
        return None
    s = res.out.decode("utf-8", "replace").strip()
    return s if s and re.fullmatch(r"[0-9a-f]{40}", s) else None


def git_rev_list(
    repo_dir: str, base: str, head: str, max_commits: int
) -> Tuple[List[str], bool, str]:
    env = git_env()
    res = run_cmd(
        ["git", "rev-list", "--reverse", f"{base}..{head}"],
        cwd=repo_dir,
        env=env,
        timeout=120,
    )
    if res.rc != 0:
        return [], False, f"git rev-list failed rc={res.rc} err={_tail_bytes(res.err)}"
    commits = [
        x.strip() for x in res.out.decode("utf-8", "replace").splitlines() if x.strip()
    ]
    truncated = False
    if max_commits > 0 and len(commits) > max_commits:
        commits = commits[:max_commits]
        truncated = True
    return commits, truncated, ""


def git_show_patch(
    repo_dir: str, sha: str, max_bytes: int
) -> Tuple[Optional[str], Optional[str], bool, str]:
    env = git_env()
    res = run_cmd(
        ["git", "show", "--format=%s", "--patch", "--no-color", sha],
        cwd=repo_dir,
        env=env,
        timeout=180,
    )
    if res.rc != 0:
        return (
            None,
            None,
            False,
            f"git show failed rc={res.rc} err={_tail_bytes(res.err)}",
        )
    data = res.out
    trunc = False
    if max_bytes > 0 and len(data) > max_bytes:
        data = data[:max_bytes]
        trunc = True
    text = data.decode("utf-8", "replace")
    subject = text.splitlines()[0] if text else ""
    return subject, text, trunc, ""


def git_diff(
    repo_dir: str, base: str, head: str, max_bytes: int
) -> Tuple[Optional[str], bool, str]:
    env = git_env()
    res = run_cmd(
        ["git", "diff", "--no-color", f"{base}..{head}"],
        cwd=repo_dir,
        env=env,
        timeout=300,
    )
    if res.rc != 0:
        return None, False, f"git diff failed rc={res.rc} err={_tail_bytes(res.err)}"
    data = res.out
    trunc = False
    if max_bytes > 0 and len(data) > max_bytes:
        data = data[:max_bytes]
        trunc = True
    return data.decode("utf-8", "replace"), trunc, ""


def git_diff_names(
    repo_dir: str, base: str, head: str, max_files: int
) -> Tuple[List[str], bool, str]:
    env = git_env()
    res = run_cmd(
        ["git", "diff", "--name-only", f"{base}..{head}"],
        cwd=repo_dir,
        env=env,
        timeout=120,
    )
    if res.rc != 0:
        return (
            [],
            False,
            f"git diff --name-only failed rc={res.rc} err={_tail_bytes(res.err)}",
        )
    paths = [
        x.strip() for x in res.out.decode("utf-8", "replace").splitlines() if x.strip()
    ]
    trunc = False
    if max_files > 0 and len(paths) > max_files:
        paths = paths[:max_files]
        trunc = True
    return paths, trunc, ""


def git_ls_tree_files(
    repo_dir: str, sha: str, max_files: int
) -> Tuple[List[str], bool, str]:
    env = git_env()
    res = run_cmd(
        ["git", "ls-tree", "-r", "--name-only", sha], cwd=repo_dir, env=env, timeout=180
    )
    if res.rc != 0:
        return [], False, f"git ls-tree failed rc={res.rc} err={_tail_bytes(res.err)}"
    files = [
        x.strip() for x in res.out.decode("utf-8", "replace").splitlines() if x.strip()
    ]
    trunc = False
    if max_files > 0 and len(files) > max_files:
        files = files[:max_files]
        trunc = True
    return files, trunc, ""


def git_cat_file_batch(
    repo_dir: str, specs: List[str]
) -> Tuple[Dict[str, Tuple[str, int, bytes]], Dict[str, str], str]:
    """
    specs: list of '<sha>:<path>' strings
    Output order matches input order.
    For each spec, returns (blob_sha, size, payload_bytes) if ok.
    """
    env = git_env()
    if not specs:
        return {}, {}, ""

    stdin = ("\n".join(specs) + "\n").encode("utf-8", "replace")
    res = run_cmd(
        ["git", "cat-file", "--batch", "--buffer"],
        cwd=repo_dir,
        env=env,
        timeout=600,
        stdin_bytes=stdin,
    )
    stderr_tail = _tail_bytes(res.err)

    ok: Dict[str, Tuple[str, int, bytes]] = {}
    bad: Dict[str, str] = {}

    if res.rc != 0:
        for s in specs:
            bad[s] = f"cat-file rc={res.rc}"
        return ok, bad, stderr_tail

    data = res.out
    idx = 0

    for spec in specs:
        nl = data.find(b"\n", idx)
        if nl < 0:
            bad[spec] = "truncated: no header"
            break
        header = data[idx:nl].decode("utf-8", "replace").strip()
        idx = nl + 1

        # header: "<sha> <type> <size>" OR "<spec> missing"
        if header.endswith(" missing"):
            bad[spec] = "missing"
            continue

        parts = header.split(" ")
        if len(parts) != 3:
            bad[spec] = f"bad header: {header[:200]}"
            continue

        blob_sha, typ, size_s = parts
        if typ != "blob":
            bad[spec] = f"not_blob:{typ}"
            continue

        try:
            size = int(size_s)
        except Exception:
            bad[spec] = f"bad size: {header[:200]}"
            continue

        if idx + size > len(data):
            bad[spec] = "truncated: payload"
            break

        payload = data[idx : idx + size]
        idx += size

        # trailing newline after payload
        if idx < len(data) and data[idx : idx + 1] == b"\n":
            idx += 1

        ok[spec] = (blob_sha, size, payload)

    return ok, bad, stderr_tail


# ----------------- Arrow schema for code column (LARGE TYPES) -----------------
def make_code_type() -> pa.DataType:
    ls = pa.large_string()

    commit_t = pa.struct(
        [
            pa.field("sha", ls),
            pa.field("subject", ls),
            pa.field("patch", ls),
            pa.field("patch_truncated", pa.bool_()),
        ]
    )

    file_t = pa.struct(
        [
            pa.field("path", ls),
            pa.field("blob_sha", ls),
            pa.field("size", pa.int64()),
            pa.field("content", ls),
            pa.field("content_truncated", pa.bool_()),
            pa.field("error", ls),
        ]
    )

    debug_t = pa.struct(
        [
            pa.field("extract_repo_full_name", ls),
            pa.field("extract_clone_url", ls),
            pa.field("extract_pr_number", pa.int64()),
            pa.field("extract_base_sha", ls),
            pa.field("extract_head_sha", ls),
            pa.field("repo_dir", ls),
            pa.field("git_error", ls),
            pa.field("git_stderr_tail", ls),
        ]
    )

    return pa.struct(
        [
            pa.field("status", ls),
            pa.field("reason", ls),
            pa.field("repo_full_name", ls),
            pa.field("pr_number", pa.int64()),
            pa.field("base_sha", ls),
            pa.field("head_sha", ls),
            pa.field("merge_base", ls),
            pa.field("used_base_sha", ls),
            pa.field("used_head_sha", ls),
            pa.field("discussion_event_count", pa.int32()),
            pa.field("touched_files", pa.large_list(ls)),
            pa.field("touched_files_truncated", pa.bool_()),
            pa.field("repo_files_at_base", pa.large_list(ls)),
            pa.field("repo_files_at_base_truncated", pa.bool_()),
            pa.field("compare_diff", ls),
            pa.field("compare_diff_truncated", pa.bool_()),
            pa.field("commit_count", pa.int32()),
            pa.field("commits_truncated", pa.bool_()),
            pa.field("commits", pa.large_list(commit_t)),
            # NEW: base-version contents for touched files
            pa.field("base_files_at_base", pa.large_list(file_t)),
            pa.field("base_files_at_base_truncated", pa.bool_()),
            pa.field("debug", debug_t),
        ]
    )


# ----------------- bucketed IO helpers -----------------
def list_repo_buckets(
    in_root: str, explicit_buckets: Optional[List[int]] = None
) -> List[int]:
    if explicit_buckets is not None and explicit_buckets:
        return sorted(set(explicit_buckets))

    p = Path(in_root)
    out: List[int] = []
    if not p.exists():
        return out
    for child in p.iterdir():
        if not child.is_dir():
            continue
        m = REPO_BUCKET_RE.match(child.name)
        if m:
            out.append(int(m.group(1)))
    return sorted(set(out))


def bucket_files(in_root: str, repo_bucket: int) -> List[str]:
    d = Path(in_root) / f"repo_bucket={repo_bucket:04d}"
    if not d.is_dir():
        return []
    return sorted(str(p) for p in d.glob("*.parquet") if p.is_file())


def bucket_out_dir(out_root: str, repo_bucket: int) -> str:
    return os.path.join(out_root, f"repo_bucket={repo_bucket:04d}")


def bucket_done_path(out_root: str, repo_bucket: int) -> str:
    return os.path.join(bucket_out_dir(out_root, repo_bucket), "_DONE.json")


def progress_dir(out_root: str) -> str:
    return os.path.join(out_root, "_progress")


def progress_path(out_root: str, repo_bucket: int) -> str:
    return os.path.join(progress_dir(out_root), f"repo_bucket={repo_bucket:04d}.json")


def list_existing_outputs(out_root: str, repo_bucket: int) -> List[Tuple[int, str]]:
    d = bucket_out_dir(out_root, repo_bucket)
    if not os.path.isdir(d):
        return []
    out: List[Tuple[int, str]] = []
    for p in Path(d).iterdir():
        if not p.is_file():
            continue
        m = OUT_FILE_RE.match(p.name)
        if not m:
            continue
        out.append((int(m.group(1)), str(p)))
    out.sort(key=lambda x: x[0])
    return out


def cleanup_tmp_outputs(out_root: str, repo_bucket: int) -> int:
    d = bucket_out_dir(out_root, repo_bucket)
    if not os.path.isdir(d):
        return 0
    n = 0
    for p in Path(d).iterdir():
        if p.is_file() and p.name.endswith(".tmp"):
            try:
                p.unlink()
                n += 1
            except Exception:
                pass
    return n


def count_rows_in_outputs(existing: List[Tuple[int, str]]) -> int:
    tot = 0
    for _seq, fp in existing:
        try:
            pf = pq.ParquetFile(fp)
            tot += int(pf.metadata.num_rows)
        except Exception:
            pass
    return tot


def infer_resume_from_outputs(
    in_files: List[str], committed_rows: int
) -> Tuple[int, int]:
    if committed_rows <= 0:
        return 0, 0

    rem = committed_rows
    for i, fp in enumerate(in_files):
        try:
            pf = pq.ParquetFile(fp)
            n = int(pf.metadata.num_rows)
        except Exception:
            n = 0
        if rem >= n:
            rem -= n
            continue
        return i, int(rem)
    return len(in_files), 0


# ----------------- atomic parquet writer (same output naming) -----------------
class BucketParquetWriterAtomic:
    """
    Writes to: out_root/repo_bucket=####/prs_##########.parquet
    via temp file and atomic rename, so only fully-written files appear.

    Optimization: buffer multiple tiny input tables into a larger "row group flush".
    This avoids producing hundreds of tiny row groups when --batch-size is small.
    """

    def __init__(
        self,
        out_root: str,
        repo_bucket: int,
        max_rows_per_file: int,
        start_seq: int,
        row_group_rows: int,
    ):
        self.out_root = out_root
        self.repo_bucket = repo_bucket
        self.max_rows_per_file = int(max_rows_per_file)
        self.row_group_rows = max(1, int(row_group_rows))
        self.seq = int(start_seq)
        self.rows_in_current = 0
        self.writer: Optional[pq.ParquetWriter] = None
        self._final_path: Optional[str] = None
        self._tmp_path: Optional[str] = None

        # Row-group buffer (bounded by row_group_rows; memory safety depends on row sizes)
        self._rg_tables: List[pa.Table] = []
        self._rg_rows: int = 0

    def _next_paths(self) -> Tuple[str, str]:
        d = bucket_out_dir(self.out_root, self.repo_bucket)
        ensure_dir(d)
        final = os.path.join(d, f"prs_{self.seq:010d}.parquet")
        tmp = final + ".tmp"
        self.seq += 1
        return final, tmp

    def _ensure_writer(self, schema: pa.Schema) -> None:
        if self.writer is not None:
            return
        final, tmp = self._next_paths()
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        self._final_path = final
        self._tmp_path = tmp

        # Writer options:
        # - zstd compression (as before)
        # - dictionary + statistics help many readers prune/encode efficiently
        # - data_page_size sets target uncompressed page size
        self.writer = pq.ParquetWriter(
            tmp,
            schema=schema,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
            data_page_size=1_048_576,  # 1 MiB
            store_schema=True,
        )
        self.rows_in_current = 0
        self._rg_tables = []
        self._rg_rows = 0

    def _flush_row_group_buffer(self) -> None:
        if self.writer is None or self._rg_rows <= 0:
            return
        t = (
            pa.concat_tables(self._rg_tables, promote=True)
            if len(self._rg_tables) > 1
            else self._rg_tables[0]
        )
        # This write_table call will form a row group roughly equal to buffered rows.
        self.writer.write_table(t)
        self._rg_tables = []
        self._rg_rows = 0

    def write_table(self, t: pa.Table) -> None:
        if self.writer is None:
            self._ensure_writer(t.schema)
        assert self.writer is not None

        # Buffer tiny tables into a larger flush (row group).
        self._rg_tables.append(t)
        self._rg_rows += int(t.num_rows)

        self.rows_in_current += int(t.num_rows)

        if self._rg_rows >= self.row_group_rows:
            self._flush_row_group_buffer()

    def should_commit(self) -> bool:
        return (
            self.writer is not None and self.rows_in_current >= self.max_rows_per_file
        )

    def commit_close(self) -> int:
        if self.writer is None:
            return 0

        # Flush any remaining buffered rows as the final row group in this shard.
        self._flush_row_group_buffer()

        rows = int(self.rows_in_current)
        final = self._final_path
        tmp = self._tmp_path

        self.writer.close()
        self.writer = None
        self.rows_in_current = 0
        self._final_path = None
        self._tmp_path = None
        self._rg_tables = []
        self._rg_rows = 0

        if tmp and final:
            os.replace(tmp, final)
        return rows

    def close(self) -> int:
        return self.commit_close()


# ----------------- tracker (for driver progress) -----------------
@ray.remote
class ProgressTracker:
    def __init__(self, total_buckets: int):
        self.total_buckets = int(total_buckets)
        self.done = 0
        self.errors = 0
        self.rows = 0
        self.ok = 0
        self.skip = 0
        self.err = 0
        self.out_files = 0
        self.last_msg = {}  # worker_id -> str
        self.bucket_of = {}  # worker_id -> bucket
        self.t0 = time.time()

    def worker_msg(self, worker_id: int, msg: str) -> None:
        self.last_msg[int(worker_id)] = msg

    def worker_bucket(self, worker_id: int, bucket: Optional[int]) -> None:
        if bucket is None:
            self.bucket_of.pop(int(worker_id), None)
        else:
            self.bucket_of[int(worker_id)] = int(bucket)

    def bucket_done(self, stats: Dict[str, Any]) -> None:
        self.done += 1
        if stats.get("error"):
            self.errors += 1
        self.rows += int(stats.get("rows", 0))
        self.ok += int(stats.get("ok", 0))
        self.skip += int(stats.get("skip", 0))
        self.err += int(stats.get("err", 0))
        self.out_files += int(stats.get("out_files", 0))

    def snapshot(self) -> Dict[str, Any]:
        dt = time.time() - self.t0
        return {
            "total_buckets": self.total_buckets,
            "done": self.done,
            "errors": self.errors,
            "rows": self.rows,
            "ok": self.ok,
            "skip": self.skip,
            "err": self.err,
            "out_files": self.out_files,
            "elapsed_s": dt,
            "bucket_of": dict(self.bucket_of),
            "last_msg": dict(self.last_msg),
        }


# ----------------- core per-row logic -----------------
def process_one_row(
    row: Dict[str, Any],
    repo_dir_by_full: Dict[str, str],
    clone_root: str,
    code_type: pa.DataType,  # kept for signature compatibility
    max_commits: int,
    max_commit_patch_bytes: int,
    max_diff_bytes: int,
    max_touched_files: int,
    max_tree_files: int,
    max_files_per_pr: int,
    max_file_bytes: int,
) -> Dict[str, Any]:
    disc = count_discussion_events(row)
    disc_i32 = int(disc) if isinstance(disc, int) else None

    repo_full = get_any_str(
        row,
        [
            "pull_request.base.repo.full_name",
            "pull_request.head.repo.full_name",
            "repo.full_name",
            "repo.name",
            "event_repo_name",
        ],
    )
    prn = get_any_int(row, ["pr_number", "pull_request.number"])
    base_sha = get_any_str(row, ["pull_request.base.sha", "page.base.sha"])
    head_sha = get_any_str(row, ["pull_request.head.sha", "page.head.sha"])

    dbg: Dict[str, Any] = {
        "extract_repo_full_name": repo_full,
        "extract_clone_url": None,
        "extract_pr_number": prn,
        "extract_base_sha": base_sha,
        "extract_head_sha": head_sha,
        "repo_dir": None,
        "git_error": None,
        "git_stderr_tail": None,
    }

    if not repo_full or prn is None or not base_sha or not head_sha:
        return {
            "status": "skip",
            "reason": "missing_repo_or_pr_or_base",
            "repo_full_name": repo_full,
            "pr_number": prn,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "merge_base": None,
            "used_base_sha": None,
            "used_head_sha": None,
            "discussion_event_count": disc_i32,
            "touched_files": [],
            "touched_files_truncated": False,
            "repo_files_at_base": [],
            "repo_files_at_base_truncated": False,
            "compare_diff": None,
            "compare_diff_truncated": False,
            "commit_count": None,
            "commits_truncated": False,
            "commits": [],
            "base_files_at_base": [],
            "base_files_at_base_truncated": False,
            "debug": dbg,
        }

    clone_url = build_clone_url(row, repo_full)
    dbg["extract_clone_url"] = redact_url(clone_url)

    repo_dir = repo_dir_by_full.get(repo_full)
    if not repo_dir:
        safe = repo_full.replace("/", "__")
        repo_dir = os.path.join(clone_root, f"{safe}.git")
        repo_dir_by_full[repo_full] = repo_dir
    dbg["repo_dir"] = repo_dir

    ok, err = ensure_bare_repo(repo_dir, clone_url)
    if not ok:
        dbg["git_error"] = err
        return {
            "status": "error",
            "reason": "git_clone_or_fetch_failed",
            "repo_full_name": repo_full,
            "pr_number": prn,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "merge_base": None,
            "used_base_sha": None,
            "used_head_sha": None,
            "discussion_event_count": disc_i32,
            "touched_files": [],
            "touched_files_truncated": False,
            "repo_files_at_base": [],
            "repo_files_at_base_truncated": False,
            "compare_diff": None,
            "compare_diff_truncated": False,
            "commit_count": None,
            "commits_truncated": False,
            "commits": [],
            "base_files_at_base": [],
            "base_files_at_base_truncated": False,
            "debug": dbg,
        }

    # Option A: best-effort fetch of GitHub PR head ref, but do NOT change
    # semantics unless head_sha is missing locally.
    fetched_head_sha: Optional[str] = None
    ferr_msg = ""
    ferr_tail = ""
    try:
        fetched_head_sha, ferr_msg, ferr_tail = git_fetch_pr_head(repo_dir, prn)
    except Exception:
        fetched_head_sha = None
        ferr_msg = "git_fetch_pr_head exception"
        ferr_tail = ""

    if ferr_msg:
        dbg["git_stderr_tail"] = (dbg.get("git_stderr_tail") or "") + (
            ("\n" if (dbg.get("git_stderr_tail") or "") else "") + ferr_msg
        )
        if ferr_tail:
            dbg["git_stderr_tail"] = (dbg.get("git_stderr_tail") or "") + (
                "\n" + ferr_tail
            )

    resolved_head = head_sha
    if not git_has_commit(repo_dir, head_sha) and fetched_head_sha:
        resolved_head = fetched_head_sha

    mb = git_merge_base(repo_dir, base_sha, resolved_head)
    used_base = mb or base_sha
    used_head = resolved_head

    touched, touched_trunc, terr = git_diff_names(
        repo_dir, used_base, used_head, max_touched_files
    )
    if terr:
        dbg["git_error"] = terr
        return {
            "status": "error",
            "reason": "git_diff_names_failed",
            "repo_full_name": repo_full,
            "pr_number": prn,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "merge_base": mb,
            "used_base_sha": used_base,
            "used_head_sha": used_head,
            "discussion_event_count": disc_i32,
            "touched_files": [],
            "touched_files_truncated": False,
            "repo_files_at_base": [],
            "repo_files_at_base_truncated": False,
            "compare_diff": None,
            "compare_diff_truncated": False,
            "commit_count": None,
            "commits_truncated": False,
            "commits": [],
            "base_files_at_base": [],
            "base_files_at_base_truncated": False,
            "debug": dbg,
        }

    repo_files, repo_files_trunc, ferr = git_ls_tree_files(
        repo_dir, base_sha, max_tree_files
    )
    if ferr:
        dbg["git_error"] = ferr
        return {
            "status": "error",
            "reason": "git_ls_tree_failed",
            "repo_full_name": repo_full,
            "pr_number": prn,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "merge_base": mb,
            "used_base_sha": used_base,
            "used_head_sha": used_head,
            "discussion_event_count": disc_i32,
            "touched_files": touched,
            "touched_files_truncated": bool(touched_trunc),
            "repo_files_at_base": [],
            "repo_files_at_base_truncated": False,
            "compare_diff": None,
            "compare_diff_truncated": False,
            "commit_count": None,
            "commits_truncated": False,
            "commits": [],
            "base_files_at_base": [],
            "base_files_at_base_truncated": False,
            "debug": dbg,
        }

    diff_text, diff_trunc, derr = git_diff(
        repo_dir, used_base, used_head, max_diff_bytes
    )
    if derr:
        dbg["git_error"] = derr
        return {
            "status": "error",
            "reason": "git_diff_failed",
            "repo_full_name": repo_full,
            "pr_number": prn,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "merge_base": mb,
            "used_base_sha": used_base,
            "used_head_sha": used_head,
            "discussion_event_count": disc_i32,
            "touched_files": touched,
            "touched_files_truncated": bool(touched_trunc),
            "repo_files_at_base": repo_files,
            "repo_files_at_base_truncated": bool(repo_files_trunc),
            "compare_diff": None,
            "compare_diff_truncated": False,
            "commit_count": None,
            "commits_truncated": False,
            "commits": [],
            "base_files_at_base": [],
            "base_files_at_base_truncated": False,
            "debug": dbg,
        }

    commit_shas, commits_trunc, rerr = git_rev_list(
        repo_dir, used_base, used_head, max_commits
    )
    if rerr:
        dbg["git_error"] = rerr
        return {
            "status": "error",
            "reason": "git_rev_list_failed",
            "repo_full_name": repo_full,
            "pr_number": prn,
            "base_sha": base_sha,
            "head_sha": head_sha,
            "merge_base": mb,
            "used_base_sha": used_base,
            "used_head_sha": used_head,
            "discussion_event_count": disc_i32,
            "touched_files": touched,
            "touched_files_truncated": bool(touched_trunc),
            "repo_files_at_base": repo_files,
            "repo_files_at_base_truncated": bool(repo_files_trunc),
            "compare_diff": diff_text,
            "compare_diff_truncated": bool(diff_trunc),
            "commit_count": None,
            "commits_truncated": False,
            "commits": [],
            "base_files_at_base": [],
            "base_files_at_base_truncated": False,
            "debug": dbg,
        }

    commits_out: List[Dict[str, Any]] = []
    for sha in commit_shas:
        subj, patch, p_trunc, perr = git_show_patch(
            repo_dir, sha, max_commit_patch_bytes
        )
        if perr:
            dbg["git_error"] = perr
            commits_out.append(
                {"sha": sha, "subject": None, "patch": None, "patch_truncated": False}
            )
            continue
        commits_out.append(
            {
                "sha": sha,
                "subject": subj or "",
                "patch": patch or "",
                "patch_truncated": bool(p_trunc),
            }
        )

    # NEW: base-version file contents for touched files
    base_files_at_base: List[Dict[str, Any]] = []
    base_files_truncated = False

    touched_for_content = touched
    if max_files_per_pr > 0 and len(touched_for_content) > max_files_per_pr:
        touched_for_content = touched_for_content[:max_files_per_pr]
        base_files_truncated = True

    if touched_for_content:
        specs = [f"{base_sha}:{p}" for p in touched_for_content]
        ok_map, bad_map, cat_stderr = git_cat_file_batch(repo_dir, specs)
        if cat_stderr:
            dbg["git_stderr_tail"] = (dbg.get("git_stderr_tail") or "") + (
                "\n" + cat_stderr
            )

        for p in touched_for_content:
            spec = f"{base_sha}:{p}"
            if spec in ok_map:
                blob_sha, size, payload = ok_map[spec]
                trunc = False
                if max_file_bytes > 0 and len(payload) > max_file_bytes:
                    payload = payload[:max_file_bytes]
                    trunc = True
                base_files_at_base.append(
                    {
                        "path": p,
                        "blob_sha": blob_sha,
                        "size": int(size),
                        "content": payload.decode("utf-8", "replace"),
                        "content_truncated": bool(trunc),
                        "error": None,
                    }
                )
            else:
                base_files_at_base.append(
                    {
                        "path": p,
                        "blob_sha": None,
                        "size": None,
                        "content": None,
                        "content_truncated": False,
                        "error": bad_map.get(spec) or "unresolved",
                    }
                )

    return {
        "status": "ok",
        "reason": None,
        "repo_full_name": repo_full,
        "pr_number": prn,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "merge_base": mb,
        "used_base_sha": used_base,
        "used_head_sha": used_head,
        "discussion_event_count": disc_i32,
        "touched_files": touched,
        "touched_files_truncated": bool(touched_trunc),
        "repo_files_at_base": repo_files,
        "repo_files_at_base_truncated": bool(repo_files_trunc),
        "compare_diff": diff_text,
        "compare_diff_truncated": bool(diff_trunc),
        "commit_count": int(len(commit_shas)),
        "commits_truncated": bool(commits_trunc),
        "commits": commits_out,
        "base_files_at_base": base_files_at_base,
        "base_files_at_base_truncated": bool(base_files_truncated),
        "debug": dbg,
    }


# ----------------- repo_id extraction (for batching) -----------------
def extract_repo_id(row: Dict[str, Any]) -> Optional[int]:
    return get_any_int(
        row,
        [
            "repo_id",
            "pull_request.base.repo.id",
            "pull_request.head.repo.id",
            "repo.id",
            "event_repo_id",
        ],
    )


# ----------------- progress state (simple) -----------------
def load_or_infer_progress(
    out_root: str, in_files: List[str], repo_bucket: int
) -> Dict[str, Any]:
    ensure_dir(progress_dir(out_root))
    ppath = progress_path(out_root, repo_bucket)
    saved = _read_json(ppath)

    existing = list_existing_outputs(out_root, repo_bucket)
    committed_rows = count_rows_in_outputs(existing)
    out_seq_next = (existing[-1][0] + 1) if existing else 0

    if isinstance(saved, dict) and saved.get("input_files") == in_files:
        sr = int(saved.get("committed_rows", 0) or 0)
        committed_rows = max(committed_rows, sr)
        out_seq_next = max(
            out_seq_next, int(saved.get("out_seq_next", out_seq_next) or out_seq_next)
        )

    file_idx, row_in_file = infer_resume_from_outputs(in_files, committed_rows)

    st = {
        "repo_bucket": repo_bucket,
        "input_files": in_files,
        "committed_rows": int(committed_rows),
        "out_seq_next": int(out_seq_next),
        "next_file_idx": int(file_idx),
        "next_row_in_file": int(row_in_file),
        "updated_at": _utc_now_s(),
        "status": "running",
    }
    _atomic_write_json(ppath, st)
    return st


def checkpoint_progress(out_root: str, st: Dict[str, Any]) -> None:
    st["updated_at"] = _utc_now_s()
    _atomic_write_json(progress_path(out_root, int(st["repo_bucket"])), st)


# ----------------- worker loop (fixed pool) -----------------
@ray.remote
def bucket_worker_loop(
    worker_id: int,
    queue: Queue,
    tracker: ray.actor.ActorHandle,
    in_root: str,
    out_root: str,
    clone_root: str,
    max_rows_per_file: int,
    batch_size: int,
    row_group_rows: int,
    max_commits: int,
    max_commit_patch_bytes: int,
    max_diff_bytes: int,
    max_touched_files: int,
    max_tree_files: int,
    max_files_per_pr: int,
    max_file_bytes: int,
    keep_clones: bool,  # NOTE: repo dirs are deleted per-repo regardless (per requirement)
    log_every_s: float,
) -> Dict[str, Any]:
    host = socket.gethostname()
    node_ip = ray.util.get_node_ip_address()

    code_type = make_code_type()

    agg = {
        "buckets": 0,
        "rows": 0,
        "ok": 0,
        "skip": 0,
        "err": 0,
        "out_files": 0,
        "errors": 0,
    }

    while True:
        b = queue.get()
        if b is None:
            tracker.worker_bucket.remote(worker_id, None)
            tracker.worker_msg.remote(worker_id, f"idle on {host}({node_ip})")
            break

        repo_bucket = int(b)
        tracker.worker_bucket.remote(worker_id, repo_bucket)

        done_fp = bucket_done_path(out_root, repo_bucket)
        if os.path.exists(done_fp):
            msg = f"[worker {worker_id}] bucket={repo_bucket} already DONE (marker present)"
            print(msg, flush=True)
            tracker.worker_msg.remote(worker_id, msg)
            tracker.bucket_done.remote(
                {
                    "repo_bucket": repo_bucket,
                    "error": False,
                    "rows": 0,
                    "ok": 0,
                    "skip": 0,
                    "err": 0,
                    "out_files": 0,
                }
            )
            agg["buckets"] += 1
            continue

        cleanup_tmp_outputs(out_root, repo_bucket)

        in_files = bucket_files(in_root, repo_bucket)
        if not in_files:
            msg = f"[worker {worker_id}] bucket={repo_bucket} on {host}({node_ip}) no input files"
            print(msg, flush=True)
            tracker.worker_msg.remote(worker_id, msg)
            tracker.bucket_done.remote(
                {
                    "repo_bucket": repo_bucket,
                    "error": True,
                    "rows": 0,
                    "ok": 0,
                    "skip": 0,
                    "err": 0,
                    "out_files": 0,
                }
            )
            agg["buckets"] += 1
            agg["errors"] += 1
            continue

        st = load_or_infer_progress(out_root, in_files, repo_bucket)
        committed_rows = int(st["committed_rows"])
        out_seq_next = int(st["out_seq_next"])

        bucket_clone_root = os.path.join(clone_root, f"repo_bucket={repo_bucket:04d}")
        ensure_dir(bucket_clone_root)

        msg = (
            f"[worker {worker_id}] bucket={repo_bucket} resume committed_rows={committed_rows} "
            f"out_seq_next={out_seq_next} node={node_ip}"
        )
        print(msg, flush=True)
        tracker.worker_msg.remote(worker_id, msg)

        t0 = time.time()
        last_log = t0
        rows_seen = ok_n = skip_n = err_n = 0
        out_files_written = 0

        writer = BucketParquetWriterAtomic(
            out_root=out_root,
            repo_bucket=repo_bucket,
            max_rows_per_file=max_rows_per_file,
            start_seq=out_seq_next,
            row_group_rows=row_group_rows,
        )

        rows_to_skip = committed_rows

        cur_repo_id: Optional[int] = None
        cur_repo_full: Optional[str] = None
        cur_repo_dir: Optional[str] = None
        cur_repo_dir_by_full: Dict[str, str] = {}
        cur_repo_prs = 0

        def finish_current_repo_if_any():
            nonlocal \
                cur_repo_id, \
                cur_repo_full, \
                cur_repo_dir, \
                cur_repo_dir_by_full, \
                cur_repo_prs
            if cur_repo_dir:
                shutil.rmtree(cur_repo_dir, ignore_errors=True)
            cur_repo_id = None
            cur_repo_full = None
            cur_repo_dir = None
            cur_repo_dir_by_full = {}
            cur_repo_prs = 0

        for fp in in_files:
            pf = pq.ParquetFile(fp)
            for rb in pf.iter_batches(batch_size=batch_size):
                bch = rb

                if rows_to_skip > 0:
                    if rows_to_skip >= bch.num_rows:
                        rows_to_skip -= bch.num_rows
                        continue
                    bch = bch.slice(int(rows_to_skip))
                    rows_to_skip = 0

                t = pa.Table.from_batches([bch])
                rows = t.to_pylist()

                code_rows: List[Dict[str, Any]] = []

                for r in rows:
                    rows_seen += 1

                    rid = extract_repo_id(r)

                    if (
                        rid is not None
                        and cur_repo_id is not None
                        and rid != cur_repo_id
                    ):
                        finish_current_repo_if_any()

                    if cur_repo_id is None:
                        cur_repo_id = rid
                        cur_repo_full = get_any_str(
                            r,
                            [
                                "pull_request.base.repo.full_name",
                                "pull_request.head.repo.full_name",
                                "repo.full_name",
                                "repo.name",
                                "event_repo_name",
                            ],
                        )
                        cur_repo_prs = 0

                        if cur_repo_full:
                            safe = cur_repo_full.replace("/", "__")
                            cur_repo_dir = os.path.join(
                                bucket_clone_root, f"{safe}.git"
                            )
                            clone_url = build_clone_url(r, cur_repo_full)
                            ok_repo, err_repo = ensure_bare_repo(
                                cur_repo_dir, clone_url
                            )
                            if not ok_repo:
                                # keep context; per-row will emit git error if needed
                                _ = err_repo
                            cur_repo_dir_by_full[cur_repo_full] = cur_repo_dir

                            if repo_bucket == 0:
                                msg2 = f"[worker {worker_id}] bucket=0 repo_start repo_id={cur_repo_id} repo={cur_repo_full}"
                                print(msg2, flush=True)
                                tracker.worker_msg.remote(worker_id, msg2)

                    cur_repo_prs += 1

                    c = process_one_row(
                        r,
                        repo_dir_by_full=cur_repo_dir_by_full,
                        clone_root=bucket_clone_root,
                        code_type=code_type,
                        max_commits=max_commits,
                        max_commit_patch_bytes=max_commit_patch_bytes,
                        max_diff_bytes=max_diff_bytes,
                        max_touched_files=max_touched_files,
                        max_tree_files=max_tree_files,
                        max_files_per_pr=max_files_per_pr,
                        max_file_bytes=max_file_bytes,
                    )

                    stt = c.get("status")
                    if stt == "ok":
                        ok_n += 1
                    elif stt == "skip":
                        skip_n += 1
                    else:
                        err_n += 1

                    code_rows.append(c)

                    now = time.time()
                    if now - last_log >= log_every_s:
                        elapsed = now - t0
                        extra = ""
                        if repo_bucket == 0 and cur_repo_full:
                            extra = f" repo={cur_repo_full} repo_prs={cur_repo_prs}"
                        msgp = (
                            f"[worker {worker_id}] bucket={repo_bucket} rows={rows_seen} ok={ok_n} skip={skip_n} err={err_n} "
                            f"out_seq_next={writer.seq} in_shard_rows={writer.rows_in_current} elapsed={elapsed:.1f}s{extra}"
                        )
                        print(msgp, flush=True)
                        tracker.worker_msg.remote(worker_id, msgp)
                        last_log = now

                # build once
                code_arr = pa.array(code_rows, type=code_type)
                out_t = t.append_column("code", code_arr)
                writer.write_table(out_t)

                if writer.should_commit():
                    committed_in_shard = writer.commit_close()
                    if committed_in_shard > 0:
                        out_files_written += 1
                        st["committed_rows"] = int(st["committed_rows"]) + int(
                            committed_in_shard
                        )
                        st["out_seq_next"] = int(writer.seq)
                        checkpoint_progress(out_root, st)

        finish_current_repo_if_any()

        committed_in_last = writer.close()
        if committed_in_last > 0:
            out_files_written += 1
            st["committed_rows"] = int(st["committed_rows"]) + int(committed_in_last)
            st["out_seq_next"] = int(writer.seq)
            checkpoint_progress(out_root, st)

        st["status"] = "done"
        checkpoint_progress(out_root, st)
        _atomic_write_json(
            bucket_done_path(out_root, repo_bucket),
            {
                "repo_bucket": repo_bucket,
                "status": "done",
                "committed_rows": int(st["committed_rows"]),
                "out_seq_next": int(st["out_seq_next"]),
                "updated_at": _utc_now_s(),
            },
        )

        dt = time.time() - t0
        msgd = (
            f"[worker {worker_id}] bucket={repo_bucket} DONE new_rows={rows_seen} ok={ok_n} skip={skip_n} err={err_n} "
            f"new_out_files={out_files_written} seconds={dt:.1f}"
        )
        print(msgd, flush=True)
        tracker.worker_msg.remote(worker_id, msgd)

        tracker.bucket_done.remote(
            {
                "repo_bucket": repo_bucket,
                "error": False,
                "rows": rows_seen,
                "ok": ok_n,
                "skip": skip_n,
                "err": err_n,
                "out_files": int(out_files_written),
            }
        )

        agg["buckets"] += 1
        agg["rows"] += rows_seen
        agg["ok"] += ok_n
        agg["skip"] += skip_n
        agg["err"] += err_n
        agg["out_files"] += int(out_files_written)

    return agg


# ----------------- driver -----------------
def connect(addr: Optional[str]):
    if addr is None:
        addr = "auto"
    return ray.init(address=addr, namespace="pr-pipeline", log_to_driver=False)


def parse_bucket_list(s: str) -> List[int]:
    s = (s or "").strip()
    if not s:
        return []
    out: List[int] = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(part))
    return out


def main():
    ap = argparse.ArgumentParser(
        description="Stage3 (git, bucketed): read Stage2 bucketed parquet, append 'code' column, write Stage3 bucketed parquet (resumable; repo-batched; includes base file contents)."
    )

    ap.add_argument(
        "--in-root",
        "--in-dir",
        dest="in_root",
        required=True,
        help="Stage2 root containing repo_bucket=####/*.parquet",
    )
    ap.add_argument(
        "--out-root",
        "--out-dir",
        dest="out_root",
        required=True,
        help="Directory to write Stage3 bucketed parquet",
    )
    ap.add_argument(
        "--clone-root",
        default=None,
        help="Directory to keep clones (default: <out-root>/_clones)",
    )
    ap.add_argument(
        "--ray-address",
        default="auto",
        help='Ray address: "auto" or "ray://host:10001"',
    )

    ap.add_argument(
        "--num-workers",
        type=int,
        default=32,
        help="Number of concurrent bucket worker loops",
    )
    ap.add_argument(
        "--cpus-per-worker",
        type=int,
        default=4,
        help="Logical CPUs reserved per worker loop",
    )

    ap.add_argument(
        "--buckets",
        default="",
        help="Optional comma-separated bucket list (e.g. 0,1,7). If empty, auto-detect repo_bucket=* dirs.",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=256,
        help="Rows per Arrow batch read from Parquet",
    )
    ap.add_argument(
        "--max-rows-per-file",
        type=int,
        default=100_000,
        help="Max PR rows per output parquet file (per bucket)",
    )

    # NEW: row group buffering (default 128 as requested)
    ap.add_argument(
        "--row-group-rows",
        type=int,
        default=128,
        help="Rows per Parquet row group inside each output file (buffers up to this many rows before writing a row group).",
    )

    ap.add_argument(
        "--log-every-s",
        type=float,
        default=60.0,
        help="Worker periodic log interval in seconds",
    )

    ap.add_argument(
        "--max-commits", type=int, default=50, help="Max commits to store per PR"
    )
    ap.add_argument(
        "--max-commit-patch-bytes",
        type=int,
        default=2_000_000,
        help="Max bytes per commit patch",
    )
    ap.add_argument(
        "--max-diff-bytes",
        type=int,
        default=8_000_000,
        help="Max bytes for full PR diff",
    )
    ap.add_argument(
        "--max-touched-files",
        type=int,
        default=50_000,
        help="Max touched files to store",
    )
    ap.add_argument(
        "--max-tree-files",
        type=int,
        default=200_000,
        help="Max repo file paths to store at base commit",
    )

    # NEW: touched file content controls
    ap.add_argument(
        "--max-files-per-pr",
        type=int,
        default=500,
        help="Max touched files to fetch/store base contents for per PR",
    )
    ap.add_argument(
        "--max-file-bytes",
        type=int,
        default=2_000_000,
        help="Max bytes stored per touched file content (base_sha version)",
    )

    ap.add_argument(
        "--keep-clones",
        action="store_true",
        default=False,
        help="Ignored for per-repo cleanup; kept for CLI compatibility",
    )

    args = ap.parse_args()

    ensure_dir(args.out_root)
    ensure_dir(progress_dir(args.out_root))
    clone_root = args.clone_root or os.path.join(args.out_root, "_clones")
    ensure_dir(clone_root)

    connect(args.ray_address)

    explicit = parse_bucket_list(args.buckets)
    buckets = list_repo_buckets(
        args.in_root, explicit_buckets=explicit if explicit else None
    )
    if not buckets:
        eprint(f"No repo_bucket=#### directories found under {args.in_root}")
        ray.shutdown()
        sys.exit(1)

    eprint(
        f"Stage3(bucketed,repo-batched): buckets={len(buckets)} in={args.in_root} out={args.out_root} "
        f"workers={args.num_workers} cpus/worker={args.cpus_per_worker} batch={args.batch_size} rows/file={args.max_rows_per_file} "
        f"row_group_rows={args.row_group_rows} max_files/pr={args.max_files_per_pr} max_file_bytes={args.max_file_bytes}"
    )

    queue: Queue = Queue()
    tracker = ProgressTracker.remote(total_buckets=len(buckets))

    for b in buckets:
        queue.put(b)
    for _ in range(args.num_workers):
        queue.put(None)

    import threading

    stop_evt = threading.Event()

    def poll_loop():
        while not stop_evt.is_set():
            time.sleep(10.0)
            snap = ray.get(tracker.snapshot.remote())
            done = snap["done"]
            tot = snap["total_buckets"]
            elapsed = snap["elapsed_s"]
            rate = (done / elapsed) if elapsed > 0 else 0.0
            eprint(
                f"[progress] buckets {done}/{tot} done (errors={snap['errors']}) "
                f"rows={snap['rows']} ok={snap['ok']} skip={snap['skip']} err={snap['err']} "
                f"out_files={snap['out_files']} elapsed={elapsed:.1f}s rate={rate:.3f} buckets/s"
            )
            bucket_of = snap.get("bucket_of") or {}
            last_msg = snap.get("last_msg") or {}
            if bucket_of:
                shown = 0
                for wid in sorted(bucket_of.keys()):
                    if shown >= 8:
                        break
                    b = bucket_of[wid]
                    msg = last_msg.get(wid, "")
                    eprint(f"  [w{wid}] bucket={b} {msg}")
                    shown += 1

    poll_thread = threading.Thread(target=poll_loop, daemon=True)
    poll_thread.start()

    try:
        futs = [
            bucket_worker_loop.options(num_cpus=args.cpus_per_worker).remote(
                i,
                queue,
                tracker,
                args.in_root,
                args.out_root,
                clone_root,
                args.max_rows_per_file,
                args.batch_size,
                args.row_group_rows,
                args.max_commits,
                args.max_commit_patch_bytes,
                args.max_diff_bytes,
                args.max_touched_files,
                args.max_tree_files,
                args.max_files_per_pr,
                args.max_file_bytes,
                args.keep_clones,
                args.log_every_s,
            )
            for i in range(args.num_workers)
        ]
        _worker_aggs = ray.get(futs)
    finally:
        stop_evt.set()
        poll_thread.join(timeout=5.0)

    snap = ray.get(tracker.snapshot.remote())
    eprint(
        "Stage3 done.\n"
        f"  buckets={snap['done']}/{snap['total_buckets']} errors={snap['errors']}\n"
        f"  rows={snap['rows']} ok={snap['ok']} skip={snap['skip']} err={snap['err']} out_files={snap['out_files']}\n"
        f"  progress_dir={progress_dir(args.out_root)}\n"
    )

    ray.shutdown()


if __name__ == "__main__":
    main()
