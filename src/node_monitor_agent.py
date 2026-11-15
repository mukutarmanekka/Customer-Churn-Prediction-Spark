"""Small HTTP agent exposing real host-level metrics to the project dashboard.

Run this on each physical Spark worker host. The hybrid Docker worker starts it
inside a host-networked container, so the same endpoint is available on that
Linux host's LAN address.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import datetime, timezone

try:
    import psutil
except ImportError as exc:
    raise SystemExit("psutil is required. Install it with: python -m pip install psutil") from exc


def snapshot() -> dict:
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage(os.getenv("NODE_MONITOR_DISK_PATH", "/"))
    network = psutil.net_io_counters()
    return {
        "status": "ok",
        "observed_at": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "hostname": socket.gethostname(),
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


class Handler(BaseHTTPRequestHandler):
    server_version = "KKBoxNodeMonitor/1.0"

    def do_GET(self) -> None:
        if self.path not in ("/metrics", "/health"):
            self.send_error(404)
            return
        payload = {"status": "ok"} if self.path == "/health" else snapshot()
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Expose host CPU, RAM, disk and network metrics.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9108)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Node metrics agent listening on {args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
