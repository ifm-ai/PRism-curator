#!/bin/bash
#SBATCH --job-name=github-curation-stage1
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --time=0
#SBATCH --partition=cpuonly

set -euo pipefail

usage() {
  echo "Usage: sbatch $0 --in-root IN_ROOT --out-root OUT_ROOT"
  exit 2
}

IN_ROOT=""
OUT_ROOT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --in-root)
      IN_ROOT="${2:-}"
      shift 2
      ;;
    --out-root)
      OUT_ROOT="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      usage
      ;;
  esac
done

[[ -n "$IN_ROOT" && -n "$OUT_ROOT" ]] || usage

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/.venv/bin/activate"

logdir="${SLURM_SUBMIT_DIR}/${SLURM_JOB_ID}"
mkdir -p "$logdir"
exec >"${logdir}/batch.out" 2>"${logdir}/batch.err"

echo "JobID: ${SLURM_JOB_ID}"
echo "Log dir: ${logdir}"
echo "Start time: $(date)"
echo "IN_ROOT=${IN_ROOT}"
echo "OUT_ROOT=${OUT_ROOT}"

NODES="${SLURM_JOB_NUM_NODES}"
WORKERS_PER_NODE=8
CPUS_PER_WORKER=4
TOTAL_WORKERS=$((NODES * WORKERS_PER_NODE))
CPUS_PER_NODE=$((WORKERS_PER_NODE * CPUS_PER_WORKER))

echo "NODES=${NODES}"
echo "WORKERS_PER_NODE=${WORKERS_PER_NODE}"
echo "CPUS_PER_WORKER=${CPUS_PER_WORKER}"
echo "TOTAL_WORKERS=${TOTAL_WORKERS}"
echo "CPUS_PER_NODE=${CPUS_PER_NODE}"

nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)
head_node="${nodes_array[0]}"

head_ip=$(
  srun -N1 -n1 -w "$head_node" bash -lc \
    "ip -o -4 addr show dev eth0 scope global | awk '{print \$4}' | cut -d/ -f1 | head -n1"
)

if [[ -z "${head_ip:-}" || "$head_ip" =~ ^127\. ]]; then
  echo "ERROR: Could not determine routable head IP (got: '${head_ip:-}')." >&2
  exit 1
fi

port=6380
echo "Head: ${head_node} (${head_ip}:${port})"

node_manager_port=$((port + 1))
object_manager_port=$((port + 2))
min_worker_port=$((port + 100))
max_worker_port=$((port + 599))

export RAY_ADDRESS="${head_ip}:${port}"

cleanup() {
  echo "Cleaning up Ray at $(date)"
  srun --overlap --nodes="$SLURM_JOB_NUM_NODES" --ntasks="$SLURM_JOB_NUM_NODES" \
    --output="${logdir}/ray-stop.%t.out" --error="${logdir}/ray-stop.%t.err" \
    ray stop -f >/dev/null 2>&1 || true
}
trap cleanup EXIT

srun -l -N1 -n1 -w "$head_node" \
  --output="${logdir}/ray-head.out" --error="${logdir}/ray-head.err" \
  ray start --head \
    --node-ip-address="$head_ip" \
    --port="$port" \
    --node-manager-port="$node_manager_port" \
    --object-manager-port="$object_manager_port" \
    --min-worker-port="$min_worker_port" \
    --max-worker-port="$max_worker_port" \
    --num-cpus="$CPUS_PER_NODE" \
    --block &

for worker_node in "${nodes_array[@]:1}"; do
  worker_ip=$(
    srun -N1 -n1 -w "$worker_node" bash -lc \
      "ip -o -4 addr show dev eth0 scope global | awk '{print \$4}' | cut -d/ -f1 | head -n1"
  )
  if [[ -z "${worker_ip:-}" || "$worker_ip" =~ ^127\. ]]; then
    echo "ERROR: Could not determine routable worker IP for ${worker_node} (got: '${worker_ip:-}')." >&2
    exit 1
  fi

  srun -l -N1 -n1 -w "$worker_node" \
    --output="${logdir}/ray-worker-${worker_node}.out" --error="${logdir}/ray-worker-${worker_node}.err" \
    ray start \
      --address="${head_ip}:${port}" \
      --node-ip-address="$worker_ip" \
      --node-manager-port="$node_manager_port" \
      --object-manager-port="$object_manager_port" \
      --min-worker-port="$min_worker_port" \
      --max-worker-port="$max_worker_port" \
      --num-cpus="$CPUS_PER_NODE" \
      --block &
done

srun --overlap -l -N1 -n1 -w "$head_node" \
  --cpus-per-task=1 \
  --output="${logdir}/ray-wait.out" --error="${logdir}/ray-wait.err" \
  python3 -u - <<'PY'
import os, time, ray
ray.init(address="auto", log_to_driver=True)
expected = int(os.environ["SLURM_JOB_NUM_NODES"])
deadline = time.time() + 300
while time.time() < deadline:
    alive = [n for n in ray.nodes() if n.get("Alive")]
    print(f"Waiting for nodes... {len(alive)}/{expected}", flush=True)
    if len(alive) >= expected:
        print("All nodes joined.", flush=True)
        break
    time.sleep(2)
else:
    raise SystemExit("Timed out waiting for Ray nodes.")
PY

echo "Launching Stage1 driver at $(date)"
srun --overlap -l -N1 -n1 -w "$head_node" \
  --cpus-per-task=1 \
  --output="${logdir}/driver.out" --error="${logdir}/driver.err" \
  python3 -u "${SCRIPT_DIR}/stage1_sharding.py" \
    --in-root "$IN_ROOT" \
    --out-root "$OUT_ROOT" \
    --num-workers "${TOTAL_WORKERS}" \
    --buckets 128 \
    --flush-rows 10000 \
    --row-group-size 10000 \
    --cpus-per-worker "${CPUS_PER_WORKER}" \
    --ray-address auto

echo "Driver finished at $(date)"
