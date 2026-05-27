# GitHub PR Curation Pipeline

This repository contains a three-stage Ray pipeline for processing GitHub Archive pull request data into enriched Parquet datasets.

## 1. Create the environment

Install `uv` if needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Create and activate the virtual environment:

```bash
uv venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
uv pip sync requirements.txt
```

Validate the environment:

```bash
uv pip check
```

## 2. Download raw GitHub Archive events

Stage 1 expects gzipped GitHub Archive event files under an input directory. GH Archive provides one `.json.gz` file per UTC hour, for example:

```bash
mkdir -p /path/to/raw-events/2024
wget -P /path/to/raw-events/2024 https://data.gharchive.org/2024-01-01-0.json.gz
```

To download a full UTC day:

```bash
mkdir -p /path/to/raw-events/2024
wget -P /path/to/raw-events/2024 https://data.gharchive.org/2024-01-01-{0..23}.json.gz
```

Use the raw-events directory as `--in-root` in Stage 1.

## 3. Run Stage 1: shard GitHub events

Stage 1 reads gzipped GitHub Archive JSON/JSONL files, keeps pull-request-relevant events, normalizes them, and writes bucketed Parquet files.

```bash
sbatch launch-stage1.sh \
  --in-root /path/to/raw-events \
  --out-root /path/to/stage1-output
```

## 4. Run Stage 2: group PR events

Stage 2 reads the Stage 1 bucketed output, groups events by repository and pull request, filters merged PRs, and writes grouped PR-level Parquet files.

```bash
sbatch launch-stage2.sh \
  --stage1-out /path/to/stage1-output \
  --out-dir /path/to/stage2-output
```

## 5. Run Stage 3: enrich with code

Stage 3 reads the Stage 2 grouped PRs, clones/fetches GitHub repositories, extracts diffs, commits, touched files, and base-version file contents, then writes enriched Parquet files.

```bash
sbatch launch-stage3.sh \
  --in-root /path/to/stage2-output \
  --out-root /path/to/stage3-output \
  --clone-root /path/to/clones
```

## 6. Monitor logs

Each Slurm job writes logs under a directory named by the job ID:

```text
<submit_dir>/<job_id>/
```

Useful files:

```text
batch.out
batch.err
driver.out
driver.err
ray-head.out
ray-head.err
ray-worker-*.out
ray-worker-*.err
```

Example:

```bash
tail -f <job_id>/driver.err
```

## Notes

Run the stages in order:

```text
Download raw events -> Stage 1 -> Stage 2 -> Stage 3
```

The launch scripts take input and output paths as command-line arguments. Keep the Stage 1 output path consistent with Stage 2 input, and the Stage 2 output path consistent with Stage 3 input.
