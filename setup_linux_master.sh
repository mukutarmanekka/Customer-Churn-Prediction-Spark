#!/usr/bin/env bash
# Bootstrap HDFS, Spark master, KKBox HDFS inputs, Python environment and Streamlit.
# Usage: MASTER_LAN_IP=192.168.137.1 PHYSICAL_WORKER_HOSTS="192.168.137.113,192.168.137.229" bash setup_linux_master.sh
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"
MASTER_LAN_IP="${MASTER_LAN_IP:-}"
PHYSICAL_WORKER_HOSTS="${PHYSICAL_WORKER_HOSTS:-}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-$PROJECT_ROOT/.venv}"

if [[ -z "$MASTER_LAN_IP" ]]; then
  MASTER_LAN_IP="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1); exit}}')"
fi
if [[ -z "$MASTER_LAN_IP" ]]; then
  echo "Set MASTER_LAN_IP to the master's LAN address reachable by every worker." >&2
  exit 2
fi
command -v docker >/dev/null 2>&1 || { echo "Docker CLI is required. Install Docker Engine or enable Docker Desktop WSL integration." >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "Docker daemon is unavailable. Start Docker Engine/Desktop and retry." >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 plugin is required." >&2; exit 1; }
command -v "$PYTHON_BIN" >/dev/null 2>&1 || { echo "Python 3 is required." >&2; exit 1; }

for file in train.csv transactions.csv members_v3.csv; do
  if [[ ! -s "$PROJECT_ROOT/data/raw/kkbox/$file" ]]; then
    echo "Missing dataset: data/raw/kkbox/$file. Extract the official KKBox competition CSV files there first." >&2
    exit 1
  fi
done

export HDFS_MASTER_HOST="$MASTER_LAN_IP"
export SPARK_DRIVER_HOST="$MASTER_LAN_IP"
export PHYSICAL_WORKER_HOSTS
"$PYTHON_BIN" -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --upgrade pip
"$VENV_DIR/bin/pip" install -r requirements.txt

echo "[1/4] Building Spark image and starting HDFS + Spark master..."
docker compose up -d --build namenode datanode spark-master
docker compose stop spark-worker >/dev/null 2>&1 || true

echo "[2/4] Waiting for Spark master and HDFS..."
ready=0
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:8080/json/ >/dev/null 2>&1; then ready=1; break; fi
  sleep 2
done
[[ "$ready" -eq 1 ]] || { echo "Spark master UI did not become ready on port 8080." >&2; exit 1; }

hdfs_ready=0
for _ in $(seq 1 60); do
  if docker compose exec -T namenode hdfs dfs -ls / >/dev/null 2>&1; then hdfs_ready=1; break; fi
  sleep 2
done
[[ "$hdfs_ready" -eq 1 ]] || { echo "HDFS NameNode did not become ready." >&2; exit 1; }

echo "[3/4] Creating HDFS paths and uploading required inputs..."
docker compose exec -T namenode hdfs dfs -mkdir -p /kkbox/input /kkbox/output
docker compose exec -T namenode hdfs dfs -chmod 777 /kkbox/output
for file in train.csv transactions.csv members_v3.csv; do
  if ! docker compose exec -T namenode hdfs dfs -test -e "/kkbox/input/$file"; then
    docker compose exec -T namenode hdfs dfs -D dfs.client.use.datanode.hostname=false \
      -put -f "/opt/project/data/raw/kkbox/$file" "/kkbox/input/$file"
  fi
done
docker compose exec -T namenode hdfs dfs -ls -h /kkbox/input

echo "[4/4] Master services are ready."
echo "Master LAN IP: $MASTER_LAN_IP"
echo "Spark master: spark://$MASTER_LAN_IP:7077 | UI: http://$MASTER_LAN_IP:8080"
echo "HDFS NameNode UI: http://$MASTER_LAN_IP:9870"
echo "Worker hosts configured: ${PHYSICAL_WORKER_HOSTS:-set PHYSICAL_WORKER_HOSTS in the environment}"
echo "Starting Streamlit dashboard. Keep this terminal open."
export SPARK_MASTER_UI_URL="http://127.0.0.1:8080"
export HDFS_NAMENODE_UI_URL="http://127.0.0.1:9870"
exec "$VENV_DIR/bin/python" -m streamlit run app.py --server.address 0.0.0.0 --server.port 8501
