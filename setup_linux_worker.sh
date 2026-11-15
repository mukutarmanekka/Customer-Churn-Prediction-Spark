#!/usr/bin/env bash
# Configure and start a native Linux Spark Standalone worker plus host telemetry.
# Usage: MASTER_LAN_IP=192.168.137.1 WORKER_LAN_IP=192.168.137.229 bash setup_linux_worker.sh
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MASTER_LAN_IP="${MASTER_LAN_IP:-${1:-}}"
WORKER_LAN_IP="${WORKER_LAN_IP:-${2:-}}"
SPARK_VERSION="${SPARK_VERSION:-4.1.4}"
SPARK_HOME="${SPARK_HOME:-$HOME/opt/spark-${SPARK_VERSION}-bin-hadoop3}"
PYTHON_BIN="${PYTHON_BIN:-python3.10}"
WORKER_CORES="${WORKER_CORES:-2}"
WORKER_MEMORY="${WORKER_MEMORY:-3g}"
STATE_DIR="$HOME/.local/state/kkbox-churn"
VENV_DIR="$HOME/.venvs/kkbox-monitor"

if [[ -z "$MASTER_LAN_IP" || -z "$WORKER_LAN_IP" ]]; then
  echo "Usage: MASTER_LAN_IP=<master-LAN-IP> WORKER_LAN_IP=<this-worker-LAN-IP> bash setup_linux_worker.sh" >&2
  exit 2
fi
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Required executor Python was not found: $PYTHON_BIN" >&2
  echo "Install Python 3.10 and rerun, or set PYTHON_BIN to the Python version used by the cluster." >&2
  exit 1
fi
if ! command -v java >/dev/null 2>&1; then
  echo "Java is required. Install OpenJDK 17 (the project's Spark image uses Java 17)." >&2
  exit 1
fi
JAVA_MAJOR="$(java -version 2>&1 | awk -F '"' '/version/ {split($2,v,"."); print (v[1]=="1" ? v[2] : v[1]); exit}')"
if [[ "$JAVA_MAJOR" != "17" ]]; then
  echo "Expected Java 17 for Spark 4.1.4; detected Java ${JAVA_MAJOR:-unknown}." >&2
  exit 1
fi

if [[ ! -x "$SPARK_HOME/bin/spark-class" ]]; then
  mkdir -p "$HOME/opt"
  ARCHIVE="$HOME/opt/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  echo "Downloading Apache Spark ${SPARK_VERSION}..."
  command -v curl >/dev/null 2>&1 || { echo "curl is required to download Spark." >&2; exit 1; }
  curl -fL --retry 3 "$URL" -o "$ARCHIVE"
  tar -xzf "$ARCHIVE" -C "$HOME/opt"
  rm -f "$ARCHIVE"
fi
export SPARK_HOME
export PATH="$SPARK_HOME/bin:$PATH"
export PYSPARK_PYTHON="$PYTHON_BIN"
export SPARK_LOCAL_IP="$WORKER_LAN_IP"
export SPARK_WORKER_DIR="$HOME/.local/share/spark/work"

"$PYTHON_BIN" -m venv "$VENV_DIR"
"$VENV_DIR/bin/python" -m pip install --upgrade pip
"$VENV_DIR/bin/pip" install 'psutil>=5.9,<8'
mkdir -p "$STATE_DIR" "$SPARK_WORKER_DIR" "$HOME/.local/share/spark/logs"
install -m 0644 "$PROJECT_ROOT/src/node_monitor_agent.py" "$HOME/node_monitor_agent.py"

for name in monitor-agent spark-worker; do
  pid_file="$STATE_DIR/${name}.pid"
  if [[ -f "$pid_file" ]]; then
    old_pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ "$old_pid" =~ ^[0-9]+$ ]] && kill -0 "$old_pid" 2>/dev/null; then
      kill "$old_pid" || true
      sleep 1
    fi
    rm -f "$pid_file"
  fi
done

nohup "$VENV_DIR/bin/python" "$HOME/node_monitor_agent.py" --host 0.0.0.0 --port 9108 \
  >"$HOME/.local/share/spark/logs/node-monitor-agent.log" 2>&1 &
echo $! > "$STATE_DIR/monitor-agent.pid"

for port in 7077 9000 9866; do
  if ! timeout 4 bash -c ">/dev/tcp/${MASTER_LAN_IP}/${port}" 2>/dev/null; then
    echo "Cannot reach master ${MASTER_LAN_IP}:${port}. Start the master and check routing/firewall." >&2
    exit 1
  fi
done

nohup "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
  "spark://${MASTER_LAN_IP}:7077" \
  --host "$WORKER_LAN_IP" --port 7078 --webui-port 8081 \
  --cores "$WORKER_CORES" --memory "$WORKER_MEMORY" \
  >"$HOME/.local/share/spark/logs/spark-worker.log" 2>&1 &
echo $! > "$STATE_DIR/spark-worker.pid"

sleep 4
if ! kill -0 "$(cat "$STATE_DIR/monitor-agent.pid")" 2>/dev/null; then
  echo "Monitoring agent failed to start. See ~/.local/share/spark/logs/node-monitor-agent.log" >&2
  exit 1
fi
if ! kill -0 "$(cat "$STATE_DIR/spark-worker.pid")" 2>/dev/null; then
  echo "Spark worker failed to start. See ~/.local/share/spark/logs/spark-worker.log" >&2
  exit 1
fi

if command -v ufw >/dev/null 2>&1 && sudo ufw status 2>/dev/null | grep -q "Status: active"; then
  for port in 7078 8081 9108 7100:7120; do
    sudo ufw allow from "$MASTER_LAN_IP" to any port "$port" proto tcp >/dev/null
  done
fi

echo
echo "Linux Spark worker and telemetry agent started."
echo "Worker: ${WORKER_LAN_IP}:7078 | UI: http://${WORKER_LAN_IP}:8081"
echo "Telemetry: http://${WORKER_LAN_IP}:9108/metrics"
echo "Logs: $HOME/.local/share/spark/logs/"
echo "PIDs: $STATE_DIR/"
echo "After a reboot, rerun this script (or install optional systemd units described in setup.md)."
