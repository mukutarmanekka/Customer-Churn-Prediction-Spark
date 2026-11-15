import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src.monitoring import collect_snapshot


@st.fragment(run_every=5)
def render_live_monitoring():
    snapshot = collect_snapshot()
    history = st.session_state.setdefault("live_monitor_history", [])
    app = snapshot.get("application")
    stage_metrics = app.get("stage_metrics", {}) if app else {}
    task_metrics = app.get("tasks", {}) if app else {}
    docker_metrics = snapshot.get("docker", {})
    master_host = snapshot.get("master_host", {})
    node_history = st.session_state.setdefault("worker_host_history", [])
    for worker in snapshot.get("workers", []):
        host_metrics = worker.get("host_metrics", {})
        if host_metrics.get("reachable"):
            node_history.append({
                "checked_at": snapshot["observed_at"],
                "worker": worker["host"],
                "cpu_percent": host_metrics.get("cpu_percent"),
                "memory_percent": host_metrics.get("memory_percent"),
                "disk_percent": host_metrics.get("disk_percent"),
                "network_sent_mb": host_metrics.get("network_sent_mb"),
                "network_received_mb": host_metrics.get("network_received_mb"),
            })
    del node_history[:-600]
    sample = {
        "checked_at": snapshot["observed_at"],
        "cores_used": snapshot.get("used_cores", 0),
        "cores_total": snapshot.get("total_cores", 0),
        "worker_memory_used_mb": snapshot.get("used_memory_mb", 0),
        "worker_memory_total_mb": snapshot.get("total_memory_mb", 0),
        "master_docker_cpu_pct": docker_metrics.get("cpu_percent_total") if docker_metrics.get("reachable") else None,
        "master_docker_memory_mb": docker_metrics.get("memory_used_mb_total") if docker_metrics.get("reachable") else None,
        "master_host_cpu_pct": master_host.get("cpu_percent") if master_host.get("reachable") else None,
        "master_host_memory_mb": master_host.get("memory_used_mb") if master_host.get("reachable") else None,
        "master_host_disk_pct": master_host.get("disk_percent") if master_host.get("reachable") else None,
        "completed_tasks": task_metrics.get("completed") if app else None,
        "active_tasks": task_metrics.get("active") if app else None,
        "failed_tasks": task_metrics.get("failed") if app else None,
        "executor_cpu_time_sec": round(stage_metrics.get("executor_cpu_time_ns", 0) / 1_000_000_000, 2) if app else None,
        "shuffle_read_mb": round(stage_metrics.get("shuffle_read_bytes", 0) / 1048576, 2) if app else None,
        "shuffle_write_mb": round(stage_metrics.get("shuffle_write_bytes", 0) / 1048576, 2) if app else None,
        "memory_spill_mb": round(stage_metrics.get("memory_spilled_bytes", 0) / 1048576, 2) if app else None,
        "disk_spill_mb": round(stage_metrics.get("disk_spilled_bytes", 0) / 1048576, 2) if app else None,
    }
    history.append(sample)
    del history[:-120]
    st.caption(f"Last polled: {snapshot['observed_at']}")

    project_root = Path(__file__).resolve().parents[1]
    status_path = project_root / "artifacts" / "run_status.json"
    training_log_path = project_root / "artifacts" / "training.log"
    if status_path.exists():
        try:
            run_status = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            run_status = {"state": "UNKNOWN"}
        state = run_status.get("state", "UNKNOWN")
        if run_status.get("worker_mode"):
            target_hosts = ", ".join(run_status.get("worker_hosts", [])) or "topology-managed workers"
            st.caption(f"Execution pool: {run_status['worker_mode']} | Required physical hosts: {target_hosts}")
        if state in {"COMPLETED", "FAILED"}:
            st.caption("The training process has stopped. Refresh the dashboard to show the latest run state and results.")
            st.button(
                "Refresh dashboard after training",
                key=f"refresh-terminal-run-{run_status.get('started_at', 'unknown')}",
                on_click=lambda: st.rerun(scope="app"),
            )
        if state == "RUNNING":
            st.info("Training run is active. This page will keep refreshing while Spark works in the background.")
        elif state == "COMPLETED":
            st.success(f"Latest training run completed at {run_status.get('completed_at', 'unknown time')}.")
        elif state == "FAILED":
            st.error(f"Latest training run failed: {run_status.get('error', 'see training.log')}")
        elif state == "CANCELLED":
            st.warning(f"Latest training run was stopped: {run_status.get('message', 'cancelled from dashboard')}")
        if training_log_path.exists():
            try:
                with training_log_path.open("rb") as log_file:
                    log_file.seek(0, 2)
                    end = log_file.tell()
                    log_file.seek(max(0, end - 65536))
                    lines = log_file.read().decode("utf-8", errors="replace").splitlines()
            except OSError as exc:
                lines = [f"Log currently unavailable for reading: {exc}"]
            with st.expander("Live training log", expanded=state == "RUNNING"):
                st.code("\n".join(lines[-35:]) if lines else "Waiting for training log output...")

    st.markdown("**Windows master host resources**")
    if master_host.get("reachable"):
        hm1, hm2, hm3, hm4 = st.columns(4)
        hm1.metric("Host CPU", f"{master_host['cpu_percent']:.1f}%")
        hm2.metric("Host RAM", f"{master_host['memory_used_mb']:,.0f} / {master_host['memory_total_mb']:,.0f} MB",
                   f"{master_host['memory_percent']:.1f}% used")
        hm3.metric("System disk", f"{master_host['disk_used_gb']:,.1f} / {master_host['disk_total_gb']:,.1f} GB",
                   f"{master_host['disk_percent']:.1f}% used")
        hm4.metric("Logical CPU cores", master_host["logical_cpus"])
        st.caption(f"Host network totals since boot: received {master_host['network_received_mb']:,.1f} MiB | sent {master_host['network_sent_mb']:,.1f} MiB")
    else:
        st.warning("Master host OS metrics are unavailable: " + str(master_host.get("error", "unknown error")))

    if not snapshot["master_reachable"]:
        st.error(f"Spark master UI is unreachable: {snapshot.get('master_error')}")
        st.info("Start the HDFS and Spark master services, then refresh this page. No worker or application metrics are inferred while the master is unavailable.")
    else:
        workers = snapshot["workers"]
        a, b, c, d = st.columns(4)
        a.metric("ALIVE workers", f"{snapshot['alive_workers']} / {snapshot['registered_workers']}")
        b.metric("Spark cores in use", f"{snapshot['used_cores']} / {snapshot['total_cores']}")
        c.metric("Worker memory in use", f"{snapshot['used_memory_mb']:,.0f} / {snapshot['total_memory_mb']:,.0f} MB")
        d.metric("Active Spark applications", len(snapshot["active_apps"]))

        if workers:
            worker_table = pd.DataFrame([{
                "Worker": x["worker_id"],
                "Host": x["host"],
                "State": x["state"],
                "Cores used / total": f"{x['cores_used']} / {x['cores']}",
                "Memory used / total (MB)": f"{x['memory_used_mb']:,} / {x['memory_mb']:,}",
                "Executors": x["executors"],
                "Host CPU %": (
                    f"{x['host_metrics'].get('cpu_percent', 0):.1f}"
                    if x.get("host_metrics", {}).get("reachable") else "Agent unavailable"
                ),
                "Host RAM used / total MB": (
                    f"{x['host_metrics'].get('memory_used_mb', 0):,.0f} / {x['host_metrics'].get('memory_total_mb', 0):,.0f}"
                    if x.get("host_metrics", {}).get("reachable") else "Agent unavailable"
                ),
                "Host disk %": (
                    f"{x['host_metrics'].get('disk_percent', 0):.1f}"
                    if x.get("host_metrics", {}).get("reachable") else "Agent unavailable"
                ),
                "Host network received MiB": (
                    f"{x['host_metrics'].get('network_received_mb', 0):,.1f}"
                    if x.get("host_metrics", {}).get("reachable") else "Agent unavailable"
                ),
                "Host network sent MiB": (
                    f"{x['host_metrics'].get('network_sent_mb', 0):,.1f}"
                    if x.get("host_metrics", {}).get("reachable") else "Agent unavailable"
                ),
                "Host telemetry": "Online" if x.get("host_metrics", {}).get("reachable") else "Agent unavailable",
                "Worker UI": "Reachable" if x["worker_ui_reachable"] else "Unreachable",
                "Worker UI address": x["worker_ui"],
            } for x in workers])
            st.markdown("**Worker-node status and allocated resources**")
            st.dataframe(worker_table, use_container_width=True, hide_index=True)
            alive_rows = [x for x in workers if x["state"] == "ALIVE"]
            if alive_rows:
                worker_df = pd.DataFrame([{
                    "Worker": f"{x['host']} ({x['worker_id'][-8:]})",
                    "Cores used": x["cores_used"],
                    "Memory used (MB)": x["memory_used_mb"],
                } for x in alive_rows])
                left, right = st.columns(2)
                with left:
                    st.plotly_chart(px.bar(worker_df, x="Worker", y="Cores used", title="Live Spark core allocation"),
                                    use_container_width=True)
                with right:
                    st.plotly_chart(px.bar(worker_df, x="Worker", y="Memory used (MB)", title="Live Spark worker memory allocation"),
                                    use_container_width=True)
                host_rows = [{
                    "Worker": x["host"],
                    "Host CPU %": x["host_metrics"].get("cpu_percent"),
                    "Host RAM %": x["host_metrics"].get("memory_percent"),
                    "Host disk %": x["host_metrics"].get("disk_percent"),
                    "Network received MiB": x["host_metrics"].get("network_received_mb"),
                    "Network sent MiB": x["host_metrics"].get("network_sent_mb"),
                } for x in alive_rows if x.get("host_metrics", {}).get("reachable")]
                if host_rows:
                    host_df = pd.DataFrame(host_rows)
                    left, right = st.columns(2)
                    with left:
                        st.plotly_chart(px.bar(host_df, x="Worker", y="Host CPU %",
                                               title="Actual worker-host CPU usage (%)"),
                                        use_container_width=True)
                    with right:
                        host_mem = host_df.melt(id_vars="Worker",
                                                value_vars=["Host RAM %", "Host disk %"],
                                                var_name="Resource", value_name="Usage %")
                        st.plotly_chart(px.bar(host_mem, x="Worker", y="Usage %", color="Resource",
                                               barmode="group", title="Actual worker-host RAM and disk usage (%)"),
                                        use_container_width=True)
                    network_df = host_df.melt(id_vars="Worker",
                                              value_vars=["Network received MiB", "Network sent MiB"],
                                              var_name="Direction", value_name="Cumulative MiB")
                    st.plotly_chart(px.bar(network_df, x="Worker", y="Cumulative MiB", color="Direction",
                                           barmode="group", title="Worker host network I/O since boot (MiB)"),
                                    use_container_width=True)
                else:
                    st.info("Worker OS-level telemetry needs a small agent on EACH worker host. Native Linux: copy only src/node_monitor_agent.py to the worker, install psutil, then run `python3 node_monitor_agent.py --host 0.0.0.0 --port 9108`. Windows worker: install psutil and run the same script with `python`. The full project, datasets and Spark code are NOT required on the worker for monitoring. Allow TCP 9108 from the master host only.")
                unreachable = [x for x in alive_rows if not x["worker_ui_reachable"]]
                if unreachable:
                    st.warning("Spark master reports these workers ALIVE, but their worker web UIs are unreachable: " +
                               ", ".join(x["host"] for x in unreachable))
            else:
                st.warning("No ALIVE Spark workers are currently registered. Training is not ready to start.")
        else:
            st.warning("The Spark master is reachable, but no workers are registered.")

        hdfs = snapshot.get("hdfs", {})
        st.markdown("**HDFS NameNode and storage health**")
        if hdfs.get("reachable"):
            h1, h2, h3, h4 = st.columns(4)
            h1.metric("Live DataNodes", hdfs["live_datanodes"])
            h2.metric("Dead DataNodes", hdfs["dead_datanodes"])
            used_percent = hdfs["capacity_used_percent"]
            h3.metric("HDFS capacity used", f"{used_percent:.1f}%" if used_percent is not None else "N/A")
            h4.metric("Missing blocks", hdfs["missing_blocks"])
            st.caption(f"Capacity: {hdfs['capacity_used_bytes'] / (1024 ** 3):,.2f} GiB used of {hdfs['capacity_total_bytes'] / (1024 ** 3):,.2f} GiB | "
                       f"Under-replicated blocks: {hdfs['under_replicated_blocks']} | Files: {hdfs['files_total']:,} | Blocks: {hdfs['blocks_total']:,}")
        else:
            st.warning(f"HDFS NameNode JMX metrics are unavailable: {hdfs.get('error', 'unknown error')}")

        active = snapshot["active_apps"]
        if active:
            st.markdown("**Active Spark application**")
            selected = active[0]
            st.caption(f"{selected.get('name', 'Spark application')} | {selected.get('id', 'unknown ID')} | "
                       f"State: {selected.get('state', 'RUNNING')}")
            if app and app.get("reachable"):
                j, k, l, m = st.columns(4)
                j.metric("Jobs running / succeeded", f"{app['jobs']['running']} / {app['jobs']['succeeded']}")
                k.metric("Failed jobs", app["jobs"]["failed"])
                l.metric("Tasks completed", f"{app['tasks']['completed']:,} / {app['tasks']['total']:,}")
                m.metric("Active / failed tasks", f"{app['tasks']['active']:,} / {app['tasks']['failed']:,}")
                s = app["stage_metrics"]
                x, y, z, w = st.columns(4)
                x.metric("Active stages", app["active_stages"])
                y.metric("Executors", app["executor_count"])
                z.metric("Executor CPU time", f"{s['executor_cpu_time_ns'] / 1_000_000_000:,.1f} s")
                w.metric("Executor GC time", f"{app['executor_metrics']['gc_ms'] / 1000:,.1f} s")
                st.caption(f"Input read: {s['input_bytes'] / (1024 ** 3):,.2f} GiB | "
                           f"Shuffle read: {s['shuffle_read_bytes'] / (1024 ** 2):,.1f} MiB | "
                           f"Shuffle write: {s['shuffle_write_bytes'] / (1024 ** 2):,.1f} MiB | "
                           f"Memory spill: {s['memory_spilled_bytes'] / (1024 ** 2):,.1f} MiB | "
                           f"Disk spill: {s['disk_spilled_bytes'] / (1024 ** 2):,.1f} MiB")
                active_stages = [stage for stage in app["stages"]
                                 if str(stage.get("status", "")).upper() == "ACTIVE"]
                if active_stages:
                    stage_rows = [{
                        "Stage": stage.get("stageId"),
                        "Name": stage.get("name", "unknown"),
                        "Tasks complete / total": f"{stage.get('numCompleteTasks', 0)} / {stage.get('numTasks', 0)}",
                        "Active tasks": stage.get("numActiveTasks", 0),
                        "Failed tasks": stage.get("numFailedTasks", 0),
                        "Input MiB": round(stage.get("inputBytes", 0) / (1024 ** 2), 2),
                        "Shuffle read MiB": round(stage.get("shuffleReadBytes", 0) / (1024 ** 2), 2),
                        "Shuffle write MiB": round(stage.get("shuffleWriteBytes", 0) / (1024 ** 2), 2),
                        "Memory spill MiB": round(stage.get("memoryBytesSpilled", 0) / (1024 ** 2), 2),
                        "Disk spill MiB": round(stage.get("diskBytesSpilled", 0) / (1024 ** 2), 2),
                    } for stage in active_stages]
                    st.markdown("**Active Spark stages**")
                    st.dataframe(pd.DataFrame(stage_rows), use_container_width=True, hide_index=True)
                executor_details = app.get("executor_details", [])
                if executor_details:
                    executor_rows = [{
                        "Executor": x["executor_id"],
                        "Host:Port": x["host_port"],
                        "Active": x["active"],
                        "Tasks complete / total": f"{x['completed_tasks']} / {x['total_tasks']}",
                        "Active tasks": x["active_tasks"],
                        "Failed tasks": x["failed_tasks"],
                        "GC seconds": round(x["gc_ms"] / 1000, 2),
                        "Input MiB": round(x["input_bytes"] / (1024 ** 2), 2),
                        "Shuffle read MiB": round(x["shuffle_read_bytes"] / (1024 ** 2), 2),
                        "Shuffle write MiB": round(x["shuffle_write_bytes"] / (1024 ** 2), 2),
                    } for x in executor_details]
                    st.markdown("**Executor-level work distribution**")
                    st.dataframe(pd.DataFrame(executor_rows), use_container_width=True, hide_index=True)
            else:
                st.warning("The Spark application is registered with the master, but its driver REST UI at port 4040 is not reachable.")
        else:
            st.info("No Spark application is currently running. Application task, shuffle, GC and spill charts will populate automatically during the next training run.")

    st.markdown("**Master-node Docker service consumption**")
    if docker_metrics.get("reachable") and docker_metrics.get("containers"):
        docker_rows = docker_metrics["containers"]
        dm1, dm2 = st.columns(2)
        dm1.metric("Master Docker CPU total", f"{docker_metrics['cpu_percent_total']:.1f}%")
        dm2.metric("Master Docker container memory", f"{docker_metrics['memory_used_mb_total']:,.0f} MB")
        docker_df = pd.DataFrame(docker_rows)
        st.dataframe(docker_df.rename(columns={
            "container": "Container", "cpu_percent": "CPU %", "memory_used_mb": "Memory used MB",
            "memory_limit_mb": "Memory limit MB", "memory_percent": "Memory %",
            "network_in_mb": "Network received MB", "network_out_mb": "Network sent MB",
            "disk_read_mb": "Block read MB", "disk_write_mb": "Block write MB",
        }), use_container_width=True, hide_index=True)
        dl, dr = st.columns(2)
        with dl:
            st.plotly_chart(px.bar(docker_df, x="container", y="cpu_percent",
                                   title="Master service CPU usage (%)"), use_container_width=True)
        with dr:
            st.plotly_chart(px.bar(docker_df, x="container", y="memory_used_mb",
                                   title="Master service memory usage (MB)"), use_container_width=True)
    elif docker_metrics.get("reachable"):
        st.info("Docker is reachable, but no project containers are currently running on the master.")
    else:
        st.warning("Docker service metrics are unavailable: " + str(docker_metrics.get("error", "unknown error")))

    history_df = pd.DataFrame(history)
    if len(node_history) > 1:
        node_history_df = pd.DataFrame(node_history)
        st.markdown("**Worker-node OS resource history (from node agents)**")
        node_cpu, node_memory = st.columns(2)
        with node_cpu:
            st.plotly_chart(px.line(node_history_df, x="checked_at", y="cpu_percent",
                                    color="worker", markers=True,
                                    title="Worker host CPU over time (%)"),
                            use_container_width=True)
        with node_memory:
            st.plotly_chart(px.line(node_history_df, x="checked_at", y="memory_percent",
                                    color="worker", markers=True,
                                    title="Worker host RAM over time (%)"),
                            use_container_width=True)
        network_history = node_history_df.melt(
            id_vars=["checked_at", "worker"],
            value_vars=["network_received_mb", "network_sent_mb"],
            var_name="Direction", value_name="Cumulative MiB"
        )
        st.plotly_chart(px.line(network_history, x="checked_at", y="Cumulative MiB",
                                color="worker", line_dash="Direction", markers=True,
                                title="Worker host network I/O totals over time (MiB)"),
                        use_container_width=True)
    if len(history_df) > 1:
        st.markdown("**Live resource and execution history**")
        left, right = st.columns(2)
        with left:
            resource_df = history_df.melt(id_vars="checked_at", value_vars=["cores_used", "cores_total"],
                                          var_name="Metric", value_name="Cores")
            st.plotly_chart(px.line(resource_df, x="checked_at", y="Cores", color="Metric",
                                    markers=True, title="Spark cluster cores over time"),
                            use_container_width=True)
        with right:
            memory_df = history_df.melt(id_vars="checked_at",
                                        value_vars=["worker_memory_used_mb", "worker_memory_total_mb"],
                                        var_name="Metric", value_name="Memory MB")
            st.plotly_chart(px.line(memory_df, x="checked_at", y="Memory MB", color="Metric",
                                    markers=True, title="Spark worker memory over time"),
                            use_container_width=True)
        if history_df["master_host_cpu_pct"].notna().any():
            left, right = st.columns(2)
            with left:
                st.plotly_chart(px.line(history_df, x="checked_at", y="master_host_cpu_pct", markers=True,
                                        title="Windows master host CPU over time (%)"), use_container_width=True)
            with right:
                st.plotly_chart(px.line(history_df, x="checked_at", y="master_host_memory_mb", markers=True,
                                        title="Windows master host RAM over time (MB)"), use_container_width=True)
            st.plotly_chart(px.line(history_df, x="checked_at", y="master_host_disk_pct", markers=True,
                                    title="Windows master system disk usage (%)"), use_container_width=True)
        if history_df["master_docker_cpu_pct"].notna().any():
            left, right = st.columns(2)
            with left:
                st.plotly_chart(px.line(history_df, x="checked_at", y="master_docker_cpu_pct", markers=True,
                                        title="Master Docker CPU over time (%)"), use_container_width=True)
            with right:
                st.plotly_chart(px.line(history_df, x="checked_at", y="master_docker_memory_mb", markers=True,
                                        title="Master Docker memory over time (MB)"), use_container_width=True)
        if history_df["executor_cpu_time_sec"].notna().any():
            st.plotly_chart(px.line(history_df, x="checked_at", y="executor_cpu_time_sec", markers=True,
                                    title="Worker executor task CPU time consumed (seconds)"), use_container_width=True)
        if history_df["completed_tasks"].notna().any():
            left, right = st.columns(2)
            with left:
                task_df = history_df.melt(id_vars="checked_at",
                                          value_vars=["completed_tasks", "active_tasks", "failed_tasks"],
                                          var_name="Metric", value_name="Tasks")
                st.plotly_chart(px.line(task_df, x="checked_at", y="Tasks", color="Metric",
                                        markers=True, title="Spark task progress"),
                                use_container_width=True)
            with right:
                io_df = history_df.melt(id_vars="checked_at",
                                        value_vars=["shuffle_read_mb", "shuffle_write_mb",
                                                    "memory_spill_mb", "disk_spill_mb"],
                                        var_name="Metric", value_name="MiB")
                st.plotly_chart(px.line(io_df, x="checked_at", y="MiB", color="Metric",
                                        markers=True, title="Shuffle and spill metrics"),
                                use_container_width=True)

    with st.expander("Monitoring sources and interpretation"):
        st.markdown("""
        - Master and workers: Spark Standalone JSON endpoints (/json/).
        - Application: Spark driver REST API for jobs, stages, executors, tasks, shuffle, and spill.
        - HDFS: NameNode JMX counters for capacity, DataNodes, missing blocks, and replication.
        - Worker CPU and memory values are Spark's advertised/allocated worker resources, not host operating-system CPU/RAM. OS-level metrics are not reported unless a separate host agent is installed.
        - A worker can be registered as ALIVE while its web UI is unreachable; both facts are shown separately.
        """)
