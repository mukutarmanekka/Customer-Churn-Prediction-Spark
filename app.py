from pathlib import Path
import json
import os
import shutil
import subprocess
import socket
import threading
import time
from datetime import datetime, timezone
import pandas as pd
import plotly.express as px
import streamlit as st
from src.monitoring_ui import render_live_monitoring
from src.monitoring import MASTER_UI_URL, fetch_json

ROOT = Path(__file__).resolve().parent
ART = ROOT / "artifacts"
st.set_page_config(page_title="Customer Churn Intelligence", layout="wide")
st.title("Customer Churn Prediction")
st.caption("KKBox WSDM 2018 | HDFS | Apache Spark | Spark MLlib | Distributed worker cluster")
st.caption("Use Results for model evaluation and retention analytics, or Monitoring for live cluster and execution telemetry.")

def run_command(args, timeout=3600):
    return subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=timeout)

def cluster_workers(count):
    # Platform-neutral: Docker Compose is available from both Windows and Linux masters.
    return run_command(["docker", "compose", "up", "-d", "--scale", f"spark-worker={count}",
                        "spark-worker"], timeout=180)

def docker_worker_ips(include_stopped=False):
    ps_args = ["docker", "compose", "ps"]
    if include_stopped:
        ps_args.append("--all")
    ps_args.extend(["-q", "spark-worker"])
    result = run_command(ps_args, timeout=30)
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout or "Could not inspect local Docker workers.")
    container_ids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not container_ids:
        return set()
    inspected = run_command([
        "docker", "inspect", "--format",
        "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}",
        *container_ids
    ], timeout=30)
    if inspected.returncode:
        raise RuntimeError(inspected.stderr or inspected.stdout or "Could not inspect Docker worker IP addresses.")
    return {ip.strip() for ip in inspected.stdout.split() if ip.strip()}


def stop_requested():
    return (ART / "stop_requested.flag").exists()


def check_stop_requested():
    if stop_requested():
        raise InterruptedError("Training was stopped from the dashboard.")


def wait_for_alive_workers(timeout_seconds=90, allowed_hosts=None, excluded_hosts=None, expected_hosts=None):
    master_url = os.getenv("SPARK_MASTER_UI_URL", MASTER_UI_URL).rstrip("/")
    allowed_hosts = {str(x).strip().lower() for x in (allowed_hosts or [])}
    excluded_hosts = {str(x).strip().lower() for x in (excluded_hosts or [])}
    expected_hosts = {str(x).strip().lower() for x in (expected_hosts or [])}
    deadline = time.monotonic() + timeout_seconds
    last_state = "Spark master UI is not reachable yet."
    while time.monotonic() < deadline:
        check_stop_requested()
        master, error = fetch_json(master_url + "/json/", timeout=2)
        if isinstance(master, dict):
            alive = [worker for worker in master.get("workers", [])
                     if str(worker.get("state", "")).upper() == "ALIVE"]
            normalized = [(str(worker.get("host", "")).strip().lower(), worker) for worker in alive]
            if expected_hosts:
                unexpected = [
                    worker for host, worker in normalized
                    if host not in expected_hosts and host not in excluded_hosts
                ]
                if unexpected:
                    hosts = ", ".join(str(worker.get("host", "unknown")) for worker in unexpected)
                    raise RuntimeError(
                        "Hybrid mode found unexpected ALIVE worker host(s): " + hosts +
                        ". Stop unrelated physical or Docker workers before continuing."
                    )
                found = {host for host, _ in normalized}
                selected = [worker for host, worker in normalized if host in expected_hosts]
                missing = expected_hosts - found
                if not missing:
                    return selected
                last_state = "Waiting for expected hybrid worker host(s): " + ", ".join(sorted(missing))
            elif allowed_hosts:
                unexpected = [worker for host, worker in normalized if host not in allowed_hosts]
                if unexpected:
                    hosts = ", ".join(str(worker.get("host", "unknown")) for worker in unexpected)
                    raise RuntimeError(
                        "Docker-only mode found workers outside the local Docker worker set. "
                        f"Unexpected ALIVE host(s): {hosts}. Stop other worker modes first."
                    )
                selected = [worker for host, worker in normalized if host in allowed_hosts]
                if selected:
                    return selected
                last_state = "No selected local Docker worker is registered as ALIVE yet."
            else:
                selected = [worker for host, worker in normalized if host not in excluded_hosts]
                if selected:
                    return selected
                last_state = "No worker in the selected mode is registered as ALIVE."
        else:
            last_state = error or last_state
        time.sleep(3)
    raise RuntimeError(
        "No complete worker set became available in the selected topology. "
        "Check worker startup, LAN routing and firewall rules. Last check: " + str(last_state)
    )


def check_remote_worker_rpc(workers):
    port = int(os.getenv("SPARK_WORKER_RPC_PORT", "7078"))
    failures = []
    for worker in workers:
        host = str(worker.get("host", "")).strip()
        try:
            with socket.create_connection((host, port), timeout=3):
                pass
        except OSError as exc:
            failures.append(f"{host}:{port} ({exc})")
    if failures:
        raise RuntimeError(
            "The master cannot open the Spark Worker RPC listener on: " + "; ".join(failures) +
            ". Check the worker's advertised LAN IP, TCP 7078 listener and firewall before submitting a job."
        )

def append_training_log(log_path, message):
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(f"[{datetime.now(timezone.utc).isoformat()}] {message}\n")
        log_file.flush()

def write_run_status(payload):
    ART.mkdir(parents=True, exist_ok=True)
    status_path = ART / "run_status.json"
    temp_path = ART / "run_status.tmp"
    temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temp_path, status_path)

def train_model(worker_mode, local_count, physical_worker_host, hybrid_docker_worker_host, log_path):
    check_stop_requested()
    append_training_log(log_path, "Starting HDFS NameNode/DataNode and Spark master.")
    start = run_command(["docker", "compose", "up", "-d", "--remove-orphans", "namenode",
                         "datanode", "spark-master"], timeout=300)
    if start.returncode:
        raise RuntimeError(start.stderr or start.stdout)
    check_stop_requested()
    append_training_log(log_path, start.stdout.strip() or "Core Docker services are running.")

    if worker_mode == "Local Docker only":
        append_training_log(log_path, f"Starting {local_count} local Windows Docker worker(s) in Docker-only mode.")
        scale = cluster_workers(local_count)
        if scale.returncode:
            raise RuntimeError(scale.stderr or scale.stdout)
        selected_worker_ips = docker_worker_ips()
        if not selected_worker_ips:
            raise RuntimeError("Local Docker workers were requested, but no worker container IPs were found.")
        append_training_log(log_path, "Allowing only local Docker worker IPs: " + ", ".join(sorted(selected_worker_ips)))
        alive_workers = wait_for_alive_workers(allowed_hosts=selected_worker_ips)

    elif worker_mode == "Hybrid (local Docker + physical)":
        physical_hosts = {
            host.strip().lower() for host in physical_worker_host.split(",") if host.strip()
        }
        if not physical_hosts:
            raise RuntimeError("Hybrid mode requires at least one physical worker LAN IP.")
        append_training_log(
            log_path,
            f"Hybrid mode selected: starting {local_count} Docker worker(s) on the physical master and adding physical workers."
        )
        scale = cluster_workers(local_count)
        if scale.returncode:
            raise RuntimeError(scale.stderr or scale.stdout)
        local_worker_ips = {ip.lower() for ip in docker_worker_ips()}
        if not local_worker_ips:
            raise RuntimeError("Local Docker workers were requested, but no worker container IPs were found.")
        expected_hosts = physical_hosts | local_worker_ips
        alive_workers = wait_for_alive_workers(expected_hosts=expected_hosts)
        found_hosts = {str(worker.get("host", "")).strip().lower() for worker in alive_workers}
        if found_hosts != expected_hosts:
            raise RuntimeError(
                "The registered worker hosts do not match the configured local-Docker-plus-physical topology. "
                f"Expected: {sorted(expected_hosts)}; found: {sorted(found_hosts)}"
            )
        physical_workers = [
            worker for worker in alive_workers
            if str(worker.get("host", "")).strip().lower() in physical_hosts
        ]
        check_remote_worker_rpc(physical_workers)

    else:
        append_training_log(log_path, "Physical-worker mode selected: stopping local Windows Docker bridge workers.")
        stop_local = run_command(["docker", "compose", "stop", "spark-worker"], timeout=180)
        if stop_local.returncode:
            raise RuntimeError(stop_local.stderr or stop_local.stdout)
        excluded_worker_ips = docker_worker_ips(include_stopped=True)
        physical_hosts = {
            host.strip().lower() for host in physical_worker_host.split(",") if host.strip()
        }
        if physical_hosts:
            alive_workers = wait_for_alive_workers(
                expected_hosts=physical_hosts, excluded_hosts=excluded_worker_ips
            )
        else:
            alive_workers = wait_for_alive_workers(excluded_hosts=excluded_worker_ips)
        check_remote_worker_rpc(alive_workers)

    append_training_log(log_path, f"Selected worker topology: {worker_mode}.")
    append_training_log(log_path, "ALIVE workers: " + ", ".join(
        f"{x.get('host', 'unknown')} (cores={x.get('cores')}, memoryMB={x.get('memory')})"
        for x in alive_workers
    ))
    driver_host = os.getenv("SPARK_DRIVER_HOST", "192.168.137.1").strip()
    if not driver_host:
        raise RuntimeError("Set SPARK_DRIVER_HOST to the master machine LAN IP reachable from the workers.")

    for name in ["train.csv", "transactions.csv", "members_v3.csv"]:
        check_stop_requested()
        check = run_command(["docker", "compose", "exec", "-T", "namenode",
                             "hdfs", "dfs", "-test", "-e", f"/kkbox/input/{name}"], timeout=60)
        if check.returncode != 0:
            local_file = ROOT / "data" / "raw" / "kkbox" / name
            if not local_file.exists():
                raise RuntimeError(f"Required dataset file is missing: {local_file}")
            append_training_log(log_path, f"Uploading missing HDFS input: {name}")
            upload = run_command(["docker", "compose", "exec", "-T", "namenode",
                                  "hdfs", "dfs", "-D", "dfs.client.use.datanode.hostname=false",
                                  "-put", "-f", f"/opt/project/data/raw/kkbox/{name}",
                                  f"/kkbox/input/{name}"], timeout=1800)
            if upload.returncode:
                raise RuntimeError(upload.stderr or upload.stdout)
        else:
            append_training_log(log_path, f"HDFS input found: {name}")

    output_dir = run_command(["docker", "compose", "exec", "-T", "namenode",
                              "hdfs", "dfs", "-mkdir", "-p", "/kkbox/output"], timeout=60)
    if output_dir.returncode:
        raise RuntimeError(output_dir.stderr or output_dir.stdout)
    output_permissions = run_command(["docker", "compose", "exec", "-T", "namenode",
                                      "hdfs", "dfs", "-chmod", "777", "/kkbox/output"], timeout=60)
    if output_permissions.returncode:
        raise RuntimeError(output_permissions.stderr or output_permissions.stdout)

    command = (f"export HDFS_MASTER_HOST={driver_host} "
               f"LABELS_PATH=hdfs://{driver_host}:9000/kkbox/input/train.csv "
               f"TRANSACTIONS_PATH=hdfs://{driver_host}:9000/kkbox/input/transactions.csv "
               f"MEMBERS_PATH=hdfs://{driver_host}:9000/kkbox/input/members_v3.csv "
               "OUTPUT_DIR=/opt/project/artifacts "
               f"HDFS_OUTPUT_DIR=hdfs://{driver_host}:9000/kkbox/output "
               "SPARK_PYSPARK_PYTHON=python3.10 "
               "SPARK_PYSPARK_DRIVER_PYTHON=/usr/bin/python3 "
               f"SPARK_DRIVER_HOST={driver_host} SPARK_CORES_MAX=32; "
               "/opt/spark/bin/spark-submit --master spark://spark-master:7077 "
               "--deploy-mode client --driver-memory 512m --executor-memory 512m "
               f"--conf spark.driver.host={driver_host} --conf spark.driver.bindAddress=0.0.0.0 "
               "--conf spark.driver.port=44575 --conf spark.driver.blockManager.port=44576 "
               "--conf spark.ui.bindAddress=0.0.0.0 --conf spark.ui.port=4040 "
               "--conf spark.network.timeout=240s --conf spark.executor.heartbeatInterval=30s "
               "--conf spark.rpc.askTimeout=240s "
               "--conf spark.blockManager.port=7100 --conf spark.port.maxRetries=20 "
               "--conf spark.pyspark.python=python3.10 --conf spark.pyspark.driver.python=/usr/bin/python3 "
               "--conf spark.executorEnv.PYSPARK_PYTHON=python3.10 "
               "--conf spark.hadoop.dfs.client.use.datanode.hostname=true "
               "--conf spark.executor.memoryOverhead=128m --conf spark.driver.memoryOverhead=128m "
               "--conf spark.cores.max=32 --conf spark.executor.cores=1 "
               "/opt/project/src/train_churn_model.py")

    check_stop_requested()
    append_training_log(log_path, "Submitting KKBox MLlib job to Spark master.")
    spark_command = ["docker", "compose", "exec", "-T", "spark-master", "bash", "-lc", command]
    with log_path.open("a", encoding="utf-8") as log_file:
        process = subprocess.Popen(spark_command, cwd=ROOT, stdout=log_file,
                                   stderr=subprocess.STDOUT, text=True)
        try:
            while process.poll() is None:
                if stop_requested():
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                    raise InterruptedError("Training was stopped from the dashboard.")
                time.sleep(2)
            return_code = process.returncode
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            raise TimeoutError("Spark training exceeded the 2-hour execution limit and was terminated.")
    if return_code:
        tail = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-60:]
        raise RuntimeError("Spark training failed. Last log lines:\n" + "\n".join(tail))
    check_stop_requested()
    append_training_log(log_path, "Spark training process exited successfully. Copying HDFS outputs.")

    for name in ("predictions", "model"):
        check_stop_requested()
        destination = ART / name
        if destination.exists():
            shutil.rmtree(destination)
        download = run_command(["docker", "compose", "exec", "-T", "namenode",
                                "hdfs", "dfs", "-D", "dfs.client.use.datanode.hostname=false",
                                "-get", f"/kkbox/output/{name}",
                                f"/opt/project/artifacts/{name}"], timeout=1800)
        if download.returncode:
            raise RuntimeError(f"Could not copy HDFS {name} into local artifacts: " + (download.stderr or download.stdout))
        append_training_log(log_path, f"Copied HDFS {name} into local artifacts.")

    prediction_parts = list((ART / "predictions").glob("part-*.csv"))
    if not prediction_parts or not any(path.stat().st_size > 0 for path in prediction_parts):
        raise RuntimeError("Training returned success, but no non-empty prediction CSV was copied from HDFS.")
    if not (ART / "model").is_dir() or not any((ART / "model").iterdir()):
        raise RuntimeError("Training returned success, but the saved model was not copied from HDFS.")
    metrics_path = ART / "metrics.json"
    if not metrics_path.exists() or json.loads(metrics_path.read_text(encoding="utf-8")).get("dataset") != "KKBox WSDM 2018 Churn Prediction Challenge":
        raise RuntimeError("Training returned success, but a valid KKBox metrics.json was not produced.")
    append_training_log(log_path, "Verified predictions, saved model and metrics.json.")

def snapshot_successful_run(run_metadata, started_at):
    """Keep an immutable copy of the most recent verified successful result set."""
    source_metrics = ART / "metrics.json"
    source_predictions = ART / "predictions"
    source_model = ART / "model"
    if not source_metrics.is_file() or not source_predictions.is_dir() or not source_model.is_dir():
        raise RuntimeError("Cannot preserve successful results: metrics, predictions or model artifacts are missing.")

    snapshot_tmp = ART / "last_successful.tmp"
    snapshot_dir = ART / "last_successful"
    if snapshot_tmp.exists():
        shutil.rmtree(snapshot_tmp)
    snapshot_tmp.mkdir(parents=True)
    shutil.copy2(source_metrics, snapshot_tmp / "metrics.json")
    shutil.copytree(source_predictions, snapshot_tmp / "predictions")
    shutil.copytree(source_model, snapshot_tmp / "model")
    metadata = {
        **run_metadata,
        "state": "COMPLETED",
        "started_at": started_at,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    (snapshot_tmp / "run_status.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    if snapshot_dir.exists():
        shutil.rmtree(snapshot_dir)
    os.replace(snapshot_tmp, snapshot_dir)


def background_training(worker_mode, local_count, physical_worker_host, hybrid_docker_worker_host, started_at):
    log_path = ART / "training.log"
    run_metadata = {
        "worker_mode": worker_mode,
        "worker_hosts": [x.strip() for x in physical_worker_host.split(",") if x.strip()],
    }
    try:
        train_model(worker_mode, local_count, physical_worker_host, hybrid_docker_worker_host, log_path)
        snapshot_successful_run(run_metadata, started_at)
        write_run_status({**run_metadata, "state": "COMPLETED", "started_at": started_at,
                          "completed_at": datetime.now(timezone.utc).isoformat()})
        append_training_log(log_path, "Training run marked COMPLETED.")
    except InterruptedError as exc:
        append_training_log(log_path, f"TRAINING STOPPED: {exc}")
        write_run_status({**run_metadata, "state": "CANCELLED", "started_at": started_at,
                          "cancelled_at": datetime.now(timezone.utc).isoformat(),
                          "message": str(exc)})
    except Exception as exc:
        if stop_requested():
            append_training_log(log_path, f"TRAINING STOPPED: {exc}")
            write_run_status({**run_metadata, "state": "CANCELLED", "started_at": started_at,
                              "cancelled_at": datetime.now(timezone.utc).isoformat(),
                              "message": "Stopped from the dashboard."})
        else:
            append_training_log(log_path, f"TRAINING FAILED: {exc}")
            write_run_status({**run_metadata, "state": "FAILED", "started_at": started_at,
                              "failed_at": datetime.now(timezone.utc).isoformat(),
                              "error": str(exc)})

status_path = ART / "run_status.json"
current_status = json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
training_running = current_status.get("state") == "RUNNING"

with st.sidebar:
    st.header("Cluster control")
    worker_mode = st.selectbox(
        "Worker topology",
        ["Physical only", "Local Docker only", "Hybrid (local Docker + physical)"],
        index=0,
        disabled=training_running,
        help="Hybrid runs Dockerized Spark workers on this physical Windows master and combines them with the selected physical workers on the other PCs."
    )
    local_workers = st.number_input(
        "Local Windows Docker workers", min_value=1, max_value=4, value=1, step=1,
        disabled=training_running or worker_mode == "Physical only",
        help="Number of Spark Worker containers to run on this physical master. Used in Local Docker only and Hybrid modes."
    )
    configured_physical_hosts = [
        x.strip() for x in os.getenv("PHYSICAL_WORKER_HOSTS", os.getenv("PHYSICAL_WORKER_HOST", "192.168.137.113,192.168.137.229")).split(",") if x.strip()
    ]
    live_master, _ = fetch_json(MASTER_UI_URL.rstrip("/") + "/json/", timeout=2)
    live_worker_hosts = sorted({
        str(x.get("host", "")).strip() for x in (live_master or {}).get("workers", [])
        if str(x.get("state", "")).upper() == "ALIVE" and str(x.get("host", "")).strip()
    }) if isinstance(live_master, dict) else []
    physical_worker_options = [x for x in live_worker_hosts if x in configured_physical_hosts]
    if not physical_worker_options:
        physical_worker_options = configured_physical_hosts
    selected_physical_hosts = st.multiselect(
        "Required physical worker nodes (preflight)",
        options=physical_worker_options,
        default=[x for x in configured_physical_hosts if x in physical_worker_options] or physical_worker_options,
        disabled=training_running or worker_mode == "Local Docker only",
        help="Select the physical hosts required for this run. Spark Standalone has no per-application host-affinity setting: every ALIVE worker remains eligible. To target a strict subset, non-selected workers must first be stopped at the worker machine."
    )
    physical_worker_host = ",".join(selected_physical_hosts)
    if not physical_worker_options:
        physical_worker_host = st.text_input(
            "Physical worker LAN IPs (comma-separated)",
            value=os.getenv("PHYSICAL_WORKER_HOSTS", os.getenv("PHYSICAL_WORKER_HOST", "")),
            disabled=training_running or worker_mode == "Local Docker only",
            help="Used when no workers are currently registered."
        )
    if st.button("Apply worker topology", use_container_width=True, disabled=training_running):
        if worker_mode in ("Local Docker only", "Hybrid (local Docker + physical)"):
            result = cluster_workers(int(local_workers))
            message = f"Started/requested {local_workers} local Docker worker(s) on the master."
        else:
            result = run_command(["docker", "compose", "stop", "spark-worker"], timeout=180)
            message = "Local Docker workers stopped. External physical worker registrations were not changed."
        if result.returncode:
            st.error(result.stderr or result.stdout)
        else:
            st.success(message)
            st.code(result.stdout or message)

    st.link_button("Open Spark master UI", "http://localhost:8080", use_container_width=True)
    st.link_button("Open active Spark application UI", "http://localhost:4040", use_container_width=True)
    st.link_button("Open HDFS NameNode UI", "http://localhost:9870", use_container_width=True)
    st.divider()
    st.header("Model training")
    physical_host_set = {
        host.strip().lower() for host in physical_worker_host.split(",") if host.strip()
    }
    hybrid_ready = worker_mode != "Hybrid (local Docker + physical)" or bool(physical_host_set)
    expected_live_hosts = {
        x.strip().lower() for x in live_worker_hosts
        if x.strip() and x.strip() in configured_physical_hosts
    }
    selected_physical_set = {
        x.strip().lower() for x in physical_worker_host.split(",") if x.strip()
    }
    physical_selection_ready = (
        worker_mode == "Local Docker only" or
        (bool(selected_physical_set) and selected_physical_set == expected_live_hosts)
    )
    if worker_mode != "Local Docker only" and selected_physical_hosts and not physical_selection_ready:
        st.warning("The selected hosts are only a required-host check. Spark Standalone will still schedule on every ALIVE worker. Stop non-selected worker daemons on their own machines before starting an exclusive subset run.")
    if st.button("Stop current training run", type="secondary", use_container_width=True,
                 disabled=not training_running):
        (ART / "stop_requested.flag").write_text(datetime.now(timezone.utc).isoformat(), encoding="utf-8")
        # The driver runs in the Spark master container in client mode. Stop only this project's
        # PySpark entry point; the HDFS and Spark master services remain available.
        stop_cmd = run_command([
            "docker", "compose", "exec", "-T", "spark-master", "bash", "-lc",
            "pkill -TERM -f '[o]rg.apache.spark.deploy.SparkSubmit.*train_churn_model.py' || true; "
            "pkill -TERM -f '[t]rain_churn_model.py' || true"
        ], timeout=30)
        st.warning("Stop requested. The Monitoring tab will report CANCELLED when Spark exits.")
        if stop_cmd.returncode:
            st.error(stop_cmd.stderr or stop_cmd.stdout)
        st.rerun()

    if st.button("Run full training pipeline", type="primary", use_container_width=True,
                 disabled=training_running or not hybrid_ready or not physical_selection_ready):
        latest_status = (
            json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
        )
        if latest_status.get("state") == "RUNNING":
            st.warning("A training run is already active. Check the Monitoring tab; do not start a duplicate job.")
        else:
            ART.mkdir(parents=True, exist_ok=True)
            (ART / "stop_requested.flag").unlink(missing_ok=True)
            started_at = datetime.now(timezone.utc).isoformat()
            log_path = ART / "training.log"
            log_path.write_text("", encoding="utf-8")
            write_run_status({"state": "RUNNING", "started_at": started_at,
                              "worker_mode": worker_mode,
                              "worker_hosts": selected_physical_hosts,
                              "message": f"Preparing cluster for {worker_mode}."})
            worker_thread = threading.Thread(
                target=background_training,
                args=(worker_mode, int(local_workers), physical_worker_host.strip(),
                      "", started_at),
                name="kkbox-spark-training", daemon=True
            )
            worker_thread.start()
            st.success("Training was submitted to the background. The Monitoring tab will keep polling while Spark works.")
            st.rerun()

    if worker_mode == "Hybrid (local Docker + physical)":
        st.info(
            "Hybrid mode uses Docker Spark Worker containers on this Windows master plus the selected "
            "native physical workers on the other PCs. Apply worker topology starts the local containers; "
            "the physical workers must already be ALIVE in the Spark Master UI."
        )

results_tab, monitoring_tab = st.tabs(["Results", "Monitoring"])

with monitoring_tab:
    st.header("Cluster & Execution Monitoring")
    st.caption("Live Spark, HDFS, master-host and worker-node telemetry. Refreshes every five seconds.")
    render_live_monitoring()

with results_tab:
    st.header("Model Results & Retention Analytics")
    status_path = ART / "run_status.json"
    if not status_path.exists():
        st.info("No verified training run is recorded yet. Run the pipeline from the sidebar; this tab will show results once fresh metrics, predictions and the saved model have been verified.")
    else:
        run_status = json.loads(status_path.read_text(encoding="utf-8"))
        latest_state = str(run_status.get("state", "UNKNOWN")).upper()
        if latest_state == "RUNNING":
            st.info("A training attempt is currently running. The Results tab continues to show the most recent verified successful run.")
        elif latest_state == "CANCELLED":
            st.warning("The latest training attempt was stopped. No new result was published; showing metrics and predictions from the most recent successful run.")
            st.caption("Stopped attempts are recorded in Monitoring and artifacts/run_status.json. They do not replace successful results.")
        elif latest_state == "FAILED":
            st.error("The latest training attempt failed: " + str(run_status.get("error", "unknown error")))
            st.caption("Showing the most recent successful run, if one is available.")
        result_root = ART / "last_successful"
        if not result_root.is_dir() and latest_state == "COMPLETED":
            result_root = ART  # Backward compatibility for an older completed run.
        if not result_root.is_dir():
            st.info("No successful training run has been preserved yet. Complete one full pipeline run to populate this tab.")
        else:
            metrics_path = result_root / "metrics.json"
            prediction_dir = result_root / "predictions"
            prediction_files = sorted(prediction_dir.glob("part-*.csv")) if prediction_dir.exists() else []
            if not metrics_path.exists() or not prediction_files:
                st.warning("The preserved successful run is missing metrics or prediction files. Re-run the pipeline to rebuild verified outputs.")
            else:
                metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
                if metrics.get("dataset") != "KKBox WSDM 2018 Churn Prediction Challenge":
                    st.error("Artifacts are not from the KKBox churn pipeline. Run training from the sidebar.")
                else:
                    df = pd.concat([pd.read_csv(path) for path in prediction_files], ignore_index=True)
                    st.caption(f"Last completed run: {metrics.get('created_at', 'unknown')} | Feature cutoff: {metrics['feature_cutoff']} | Split: deterministic 70/15/15 subscriber hash")
                    a, b, c, d = st.columns(4)
                    a.metric("Held-out subscribers", f"{metrics['test_rows']:,}")
                    b.metric("Observed churn rate", f"{metrics['churn_rate']:.1%}")
                    c.metric("Selected model", metrics["selected_model"])
                    d.metric("Test PR-AUC", f"{metrics['test_pr_auc']:.3f}")
                    e, f, g, h = st.columns(4)
                    e.metric("Test ROC-AUC", f"{metrics['test_roc_auc']:.3f}")
                    f.metric("Precision", f"{metrics['test_precision']:.1%}")
                    g.metric("Recall", f"{metrics['test_recall']:.1%}")
                    h.metric("F1 score", f"{metrics['test_f1']:.3f}")

                    left, right = st.columns([1.2, 1])
                    with left:
                        st.subheader("Model comparison (validation set)")
                        comparison = pd.DataFrame(metrics["validation_model_comparison"]).sort_values("validation_pr_auc", ascending=False)
                        st.plotly_chart(px.bar(comparison, x="model", y="validation_pr_auc", color="model",
                                               text_auto=".3f", title="PR-AUC (higher is better)"), use_container_width=True)
                        st.dataframe(comparison, use_container_width=True, hide_index=True)
                    with right:
                        st.subheader("Held-out confusion matrix")
                        cm = metrics["test_confusion_matrix"]
                        matrix = pd.DataFrame([[cm["tn"], cm["fp"]], [cm["fn"], cm["tp"]]],
                                              index=["Actually retained", "Actually churned"],
                                              columns=["Predicted retained", "Predicted churned"])
                        st.dataframe(matrix, use_container_width=True)
                        st.caption(f"Decision threshold selected on validation data: {metrics['validation_threshold']:.2f}")

                    left, right = st.columns(2)
                    with left:
                        st.subheader("Subscriber risk segments")
                        risk = df["churn_risk"].value_counts().rename_axis("Risk").reset_index(name="Subscribers")
                        st.plotly_chart(px.pie(risk, names="Risk", values="Subscribers", hole=0.5), use_container_width=True)
                    with right:
                        st.subheader("Churn probability distribution")
                        st.plotly_chart(px.histogram(df, x="churn_probability", color="churn_risk", nbins=40), use_container_width=True)

                    st.subheader("Retention action workbench")
                    risk_levels = st.multiselect("Include risk levels", ["High", "Medium", "Low"], default=["High", "Medium"])
                    minimum = st.slider("Minimum churn probability", 0.0, 1.0, 0.35, 0.05)
                    filtered = df[df["churn_risk"].isin(risk_levels) & (df["churn_probability"] >= minimum)].sort_values("churn_probability", ascending=False)
                    st.write(f"**{len(filtered):,} subscribers** match this intervention rule.")
                    st.dataframe(filtered.head(500), use_container_width=True, hide_index=True)
                    st.download_button("Download filtered retention list", filtered.to_csv(index=False),
                                       "kkbox_churn_retention_list.csv", "text/csv")
                    with st.expander("Evaluation, feature engineering and limitations"):
                        st.json(metrics)
                        st.markdown("""
                        - Target: official KKBox competition is_churn label.
                        - Predictors: subscription transactions on or before the 2017-02-28 feature cutoff plus member attributes.
                        - Models: class-weighted Logistic Regression, Random Forest and Gradient-Boosted Trees.
                        - Model and threshold are selected on validation PR-AUC / F1; the test set is reserved for final reporting.
                        - Split: deterministic subscriber hash split (70/15/15), not a chronological split.
                        - Listening activity logs are not yet included; this run uses transaction and member history only.
                        - This is a retrospective competition benchmark, not a guarantee of real-world retention outcomes.
                        """)
