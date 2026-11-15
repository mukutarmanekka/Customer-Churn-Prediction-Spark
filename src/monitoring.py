"""Live operational metrics for the KKBox Spark/HDFS cluster.

Metrics are read from Spark's real JSON/REST endpoints and Hadoop NameNode JMX.
No synthetic or estimated infrastructure metrics are generated.
"""
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone

try:
    import psutil
except ImportError:  # Optional: live host-level master metrics.
    psutil = None
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

MASTER_UI_URL = os.getenv("SPARK_MASTER_UI_URL", "http://localhost:8080").rstrip("/")
APP_UI_URL = os.getenv("SPARK_APP_UI_URL", "http://localhost:4040").rstrip("/")
NAMENODE_UI_URL = os.getenv("HDFS_NAMENODE_UI_URL", "http://localhost:9870").rstrip("/")
HTTP_TIMEOUT = float(os.getenv("MONITOR_HTTP_TIMEOUT_SECONDS", "2.0"))


def fetch_json(url: str, timeout: float = HTTP_TIMEOUT) -> tuple[dict | list | None, str | None]:
    request = Request(url, headers={"User-Agent": "KKBox-Churn-Monitor/1.0"})
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8")), None
    except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
        return None, str(exc)


def _number(value: object, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _worker_snapshot(worker: dict) -> dict:
    worker_id = str(worker.get("id") or worker.get("workerId") or "unknown")
    host = str(worker.get("host") or worker.get("hostname") or "unknown")
    worker_ui = str(worker.get("webuiaddress") or worker.get("webUiAddress") or "")
    worker_json, worker_error = fetch_json(worker_ui + "/json/") if worker_ui else (None, "Worker UI address not advertised")
    details = worker_json if isinstance(worker_json, dict) else {}
    metrics_url = f"http://{host}:9108/metrics"
    host_metrics_payload, host_metrics_error = fetch_json(metrics_url, timeout=1.5)
    host_metrics = (
        host_metrics_payload if isinstance(host_metrics_payload, dict) and host_metrics_payload.get("status") == "ok"
        else {"reachable": False, "error": host_metrics_error or "Node metrics agent is not running"}
    )
    if isinstance(host_metrics, dict) and host_metrics.get("status") == "ok":
        host_metrics["reachable"] = True
    executors = details.get("executors") or []
    return {
        "worker_id": worker_id,
        "host": host,
        "state": str(worker.get("state") or details.get("state") or "UNKNOWN").upper(),
        "cores": _number(worker.get("cores", details.get("cores"))),
        "cores_used": _number(worker.get("coresused", details.get("coresused"))),
        "memory_mb": _number(worker.get("memory", details.get("memory"))),
        "memory_used_mb": _number(worker.get("memoryused", details.get("memoryused"))),
        "worker_ui": worker_ui or "not advertised",
        "worker_ui_reachable": worker_json is not None,
        "worker_ui_error": worker_error,
        "host_metrics": host_metrics,
        "executors": len(executors),
        "active_apps": len(details.get("activeapps") or []),
    }


def _application_snapshot(app_id: str) -> dict:
    base = f"{APP_UI_URL}/api/v1/applications/{app_id}"
    jobs, jobs_error = fetch_json(base + "/jobs")
    stages, stages_error = fetch_json(base + "/stages")
    executors, executors_error = fetch_json(base + "/executors")
    jobs = jobs if isinstance(jobs, list) else []
    stages = stages if isinstance(stages, list) else []
    executors = executors if isinstance(executors, list) else []
    job_counts = {
        "running": sum(str(x.get("status", "")).upper() == "RUNNING" for x in jobs),
        "succeeded": sum(str(x.get("status", "")).upper() == "SUCCEEDED" for x in jobs),
        "failed": sum(str(x.get("status", "")).upper() == "FAILED" for x in jobs),
    }
    task_counts = {
        "total": sum(_number(x.get("numTasks")) for x in jobs),
        "completed": sum(_number(x.get("numCompletedTasks")) for x in jobs),
        "active": sum(_number(x.get("numActiveTasks")) for x in jobs),
        "failed": sum(_number(x.get("numFailedTasks")) for x in jobs),
    }
    active_stages = [x for x in stages if str(x.get("status", "")).upper() == "ACTIVE"]
    stage_totals = {
        "input_bytes": sum(_number(x.get("inputBytes")) for x in stages),
        "executor_cpu_time_ns": sum(_number(x.get("executorCpuTime")) for x in stages),
        "executor_run_time_ms": sum(_number(x.get("executorRunTime")) for x in stages),
        "shuffle_read_bytes": sum(_number(x.get("shuffleReadBytes")) for x in stages),
        "shuffle_write_bytes": sum(_number(x.get("shuffleWriteBytes")) for x in stages),
        "memory_spilled_bytes": sum(_number(x.get("memoryBytesSpilled")) for x in stages),
        "disk_spilled_bytes": sum(_number(x.get("diskBytesSpilled")) for x in stages),
    }
    executor_rows = [x for x in executors if str(x.get("id")) != "driver"]
    executor_details = [{
        "executor_id": str(x.get("id", "unknown")),
        "host_port": str(x.get("hostPort", "unknown")),
        "active": bool(x.get("isActive", False)),
        "total_tasks": _number(x.get("totalTasks")),
        "active_tasks": _number(x.get("activeTasks")),
        "completed_tasks": _number(x.get("completedTasks")),
        "failed_tasks": _number(x.get("failedTasks")),
        "gc_ms": _number(x.get("totalGCTime")),
        "input_bytes": _number(x.get("totalInputBytes")),
        "shuffle_read_bytes": _number(x.get("totalShuffleRead")),
        "shuffle_write_bytes": _number(x.get("totalShuffleWrite")),
        "memory_used_bytes": _number(x.get("memoryUsed")),
        "disk_used_bytes": _number(x.get("diskUsed")),
    } for x in executor_rows]
    executor_totals = {
        "active": sum(_number(x.get("activeTasks")) for x in executor_rows),
        "completed": sum(_number(x.get("completedTasks")) for x in executor_rows),
        "failed": sum(_number(x.get("failedTasks")) for x in executor_rows),
        "gc_ms": sum(_number(x.get("totalGCTime")) for x in executor_rows),
        "duration_ms": sum(_number(x.get("totalDuration")) for x in executor_rows),
        "input_bytes": sum(_number(x.get("totalInputBytes")) for x in executor_rows),
        "shuffle_read_bytes": sum(_number(x.get("totalShuffleRead")) for x in executor_rows),
        "shuffle_write_bytes": sum(_number(x.get("totalShuffleWrite")) for x in executor_rows),
    }
    errors = [x for x in (jobs_error, stages_error, executors_error) if x]
    return {
        "reachable": not errors and bool(jobs or stages or executors),
        "errors": errors,
        "jobs": job_counts,
        "tasks": task_counts,
        "active_stages": len(active_stages),
        "stages": stages,
        "stage_metrics": stage_totals,
        "executor_count": len(executor_rows),
        "executor_details": executor_details,
        "executor_metrics": executor_totals,
    }


def _parse_size_bytes(value: object) -> int:
    text = str(value or "0").strip().split()[0]
    split_at = next((index for index, char in enumerate(text) if char.isalpha()), len(text))
    try:
        amount = float(text[:split_at])
    except ValueError:
        return 0
    unit = text[split_at:].lower()
    units = {
        "": 1, "b": 1, "kb": 1000, "mb": 1000 ** 2, "gb": 1000 ** 3,
        "tb": 1000 ** 4, "kib": 1024, "mib": 1024 ** 2,
        "gib": 1024 ** 3, "tib": 1024 ** 4,
    }
    return int(amount * units.get(unit, 0))


def _parse_pair_bytes(value: object) -> tuple[int, int]:
    parts = str(value or "").split("/")
    if len(parts) != 2:
        return 0, 0
    return _parse_size_bytes(parts[0]), _parse_size_bytes(parts[1])


def _docker_snapshot() -> dict:
    """Return real Docker resource counters for containers on the master machine."""
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
            capture_output=True, text=True, timeout=4, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"reachable": False, "error": str(exc), "containers": []}
    if result.returncode != 0:
        return {"reachable": False, "error": result.stderr.strip() or "docker stats failed", "containers": []}
    rows = []
    for line in result.stdout.splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        name = str(item.get("Name") or item.get("Container") or "")
        if not name.startswith("churn-") and not name.startswith("ecommerce-churn-cohort-spark-worker"):
            continue
        try:
            cpu = float(str(item.get("CPUPerc", "0%")).replace("%", ""))
        except ValueError:
            cpu = 0.0
        mem_used, mem_limit = _parse_pair_bytes(item.get("MemUsage"))
        net_in, net_out = _parse_pair_bytes(item.get("NetIO"))
        disk_read, disk_write = _parse_pair_bytes(item.get("BlockIO"))
        rows.append({
            "container": name,
            "cpu_percent": cpu,
            "memory_used_mb": round(mem_used / (1024 ** 2), 1),
            "memory_limit_mb": round(mem_limit / (1024 ** 2), 1),
            "memory_percent": str(item.get("MemPerc") or "0%"),
            "network_in_mb": round(net_in / (1024 ** 2), 2),
            "network_out_mb": round(net_out / (1024 ** 2), 2),
            "disk_read_mb": round(disk_read / (1024 ** 2), 2),
            "disk_write_mb": round(disk_write / (1024 ** 2), 2),
        })
    return {
        "reachable": True,
        "containers": rows,
        "cpu_percent_total": round(sum(x["cpu_percent"] for x in rows), 2),
        "memory_used_mb_total": round(sum(x["memory_used_mb"] for x in rows), 1),
        "memory_limit_mb_total": round(sum(x["memory_limit_mb"] for x in rows), 1),
    }


def _master_host_snapshot() -> dict:
    """Host-level metrics for the machine running the Streamlit dashboard."""
    if psutil is None:
        return {"reachable": False, "error": "Install psutil to enable master host metrics."}
    try:
        memory = psutil.virtual_memory()
        drive = (os.getenv("SystemDrive", "C:") + os.sep) if os.name == "nt" else "/"
        disk = psutil.disk_usage(drive)
        network = psutil.net_io_counters()
        return {
            "reachable": True,
            "cpu_percent": round(psutil.cpu_percent(interval=0.1), 1),
            "logical_cpus": psutil.cpu_count(logical=True) or 0,
            "physical_cpus": psutil.cpu_count(logical=False) or 0,
            "memory_used_mb": round(memory.used / (1024 ** 2), 1),
            "memory_total_mb": round(memory.total / (1024 ** 2), 1),
            "memory_percent": round(memory.percent, 1),
            "disk_used_gb": round(disk.used / (1024 ** 3), 2),
            "disk_total_gb": round(disk.total / (1024 ** 3), 2),
            "disk_percent": round(disk.percent, 1),
            "network_sent_mb": round(network.bytes_sent / (1024 ** 2), 2),
            "network_received_mb": round(network.bytes_recv / (1024 ** 2), 2),
        }
    except (OSError, AttributeError) as exc:
        return {"reachable": False, "error": str(exc)}


def _hdfs_snapshot() -> dict:
    info, info_error = fetch_json(NAMENODE_UI_URL + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")
    state, state_error = fetch_json(NAMENODE_UI_URL + "/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState")
    beans = []
    for payload in (info, state):
        if isinstance(payload, dict):
            beans.extend(payload.get("beans") or [])
    if not beans:
        return {"reachable": False, "error": info_error or state_error or "NameNode JMX returned no metrics"}
    merged = {}
    for bean in beans:
        merged.update(bean)
    capacity = _number(merged.get("CapacityTotal"))
    used = _number(merged.get("CapacityUsed"))
    remaining = _number(merged.get("CapacityRemaining"))
    return {
        "reachable": True,
        "capacity_total_bytes": capacity,
        "capacity_used_bytes": used,
        "capacity_remaining_bytes": remaining,
        "capacity_used_percent": round(used * 100 / capacity, 2) if capacity else None,
        "live_datanodes": _number(merged.get("NumLiveDataNodes")),
        "dead_datanodes": _number(merged.get("NumDeadDataNodes")),
        "missing_blocks": _number(merged.get("NumberOfMissingBlocks")),
        "under_replicated_blocks": _number(merged.get("UnderReplicatedBlocks", merged.get("NumberOfUnderReplicatedBlocks"))),
        "files_total": _number(merged.get("FilesTotal")),
        "blocks_total": _number(merged.get("BlocksTotal")),
    }


def collect_snapshot() -> dict:
    """Collect one real-time snapshot from the Spark master, workers and HDFS."""
    now = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    master, master_error = fetch_json(MASTER_UI_URL + "/json/")
    if not isinstance(master, dict):
        return {
            "observed_at": now,
            "master_reachable": False,
            "master_error": master_error or "Spark master returned an unexpected response",
            "master_host": _master_host_snapshot(),
            "workers": [],
            "active_apps": [],
            "application": None,
            "docker": _docker_snapshot(),
            "hdfs": _hdfs_snapshot(),
        }
    workers = [_worker_snapshot(x) for x in (master.get("workers") or [])]
    active_apps = master.get("activeapps") or []
    selected_app = active_apps[0] if active_apps else None
    application = _application_snapshot(str(selected_app.get("id"))) if selected_app else None
    alive = [x for x in workers if x["state"] == "ALIVE"]
    return {
        "observed_at": now,
        "master_reachable": True,
        "master_url": MASTER_UI_URL,
        "master_version": master.get("sparkVersion") or master.get("version"),
        "master_host": _master_host_snapshot(),
        "workers": workers,
        "alive_workers": len(alive),
        "registered_workers": len(workers),
        "total_cores": sum(x["cores"] for x in alive),
        "used_cores": sum(x["cores_used"] for x in alive),
        "total_memory_mb": sum(x["memory_mb"] for x in alive),
        "used_memory_mb": sum(x["memory_used_mb"] for x in alive),
        "active_apps": active_apps,
        "application": application,
        "docker": _docker_snapshot(),
        "hdfs": _hdfs_snapshot(),
    }
