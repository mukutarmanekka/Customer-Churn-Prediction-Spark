# Cluster Setup Guide

This guide covers master and worker setup for native Linux machines and Windows machines using WSL2. For project design, data engineering, model methodology and benchmark results, see README.md.

## 1. Choose the deployment layout

| Deployment | Master setup | Worker setup |
|---|---|---|
| All Linux | setup_linux_master.sh | setup_linux_worker.sh on each Linux worker |
| Windows master + native Linux worker | Existing Windows/Docker Desktop master setup, or setup_windows_master_wsl.ps1 | setup_linux_worker.sh on native Linux worker |
| Windows master WSL2 + Windows worker WSL2 | setup_windows_master_wsl.ps1 | setup_windows_worker_wsl.ps1 |
| Linux master + Windows worker WSL2 | setup_linux_master.sh | setup_windows_worker_wsl.ps1 |

The master runs HDFS, the Spark Standalone master and the Streamlit dashboard. Each worker runs a Spark Standalone Worker and the host telemetry agent. Workers do not need the KKBox dataset or full project at runtime; they need Spark, Java and the configured Python runtime. The worker setup script uses the project copy to install the small telemetry agent.

**Important:** WSL2 workers must use mirrored networking to advertise a LAN-reachable worker address. The PowerShell setup scripts configure mirrored networking in the Windows user's .wslconfig and shut down WSL once to apply it. This stops currently running WSL distributions.

## 2. Hardware and software prerequisites

### Master
- Docker Engine with Docker Compose v2, or Docker Desktop with WSL integration enabled when running the master inside WSL2.
- Python 3.10+ for the Streamlit application (Python 3.10 is recommended for consistency).
- Git or a local project copy.
- The three required KKBox CSV files in data/raw/kkbox/.
- A stable LAN IPv4 address reachable by every worker.

### Each worker
- Linux x86-64, either native Linux or a WSL2 Linux distribution.
- Java 17.
- Python 3.10, including venv support.
- Network access to the master on TCP 7077, 9000 and 9866.
- A unique LAN-reachable worker IPv4 address.
- Enough RAM and CPU for the configured Spark worker allocation.

The worker setup script downloads Apache Spark 4.1.4 if SPARK_HOME is not already set to a compatible installation. It creates an isolated telemetry virtual environment and starts the Spark worker and telemetry agent.

### Dataset preparation

Extract the official competition files so that these paths exist:
- data/raw/kkbox/train.csv
- data/raw/kkbox/transactions.csv
- data/raw/kkbox/members_v3.csv

The setup script uploads these files to HDFS. Listening activity files are not currently used by the training pipeline.

## 3. Network and port requirements

Use the actual master LAN IP in all commands. The example below is 192.168.137.1; replace it if your network differs.

| Port | Protocol | Direction | Purpose |
|---:|---|---|---|
| 7077 | TCP | Worker/driver to master | Spark Standalone master RPC |
| 8080 | TCP | Browser to master | Spark master UI |
| 9000 | TCP | Worker/driver to master | HDFS NameNode RPC |
| 9866 | TCP | Worker/driver to master | HDFS DataNode block transfer |
| 9870 | TCP | Browser/monitor to master | HDFS NameNode UI/JMX |
| 7078 | TCP | Master to worker | Spark worker RPC listener |
| 8081 | TCP | Master/browser to worker | Spark worker UI |
| 44575 | TCP | Worker executor to master | Spark driver RPC callback |
| 44576 | TCP | Worker executor to master | Spark driver BlockManager |
| 7100-7120 | TCP | Between worker and master | Spark executor BlockManager/shuffle retry range |
| 9108 | TCP | Master to worker | Worker host telemetry agent |
| 8501 | TCP | Browser to master | Streamlit dashboard |
| 4040 | TCP | Browser to master | Active Spark application UI |

Restrict telemetry TCP 9108 to the master LAN IP. Do not expose worker telemetry or Spark/HDFS control ports to the public internet. Configure host firewalls for required LAN traffic.

## 4. Native Linux master

### 4.1 Prepare the master

Copy or clone the project onto the Linux master. Install Docker Engine and the Docker Compose v2 plugin, Python 3, venv, curl and network utilities. Confirm docker info succeeds for the user who will run the project.

Place the required CSV files under data/raw/kkbox/.

### 4.2 Run the master setup

From the project root, either use the no-extension entry point setup_linux_master or invoke setup_linux_master.sh directly:

    chmod +x setup_linux_master.sh
    # Either run the wrapper:
    bash ./setup_linux_master
    # Or pass environment values directly to the full script:
    MASTER_LAN_IP=192.168.137.1 \
    PHYSICAL_WORKER_HOSTS="192.168.137.113,192.168.137.229" \
    bash ./setup_linux_master.sh

The script:
1. Checks Docker, Compose, Python and required input files.
2. Creates .venv and installs requirements.txt.
3. Builds the project Spark image.
4. Starts HDFS NameNode, DataNode and Spark master.
5. Keeps the local Docker bridge Spark worker stopped for physical-worker mode.
6. Waits for Spark and HDFS readiness.
7. Creates HDFS input/output directories and uploads missing source CSVs.
8. Starts Streamlit on port 8501.

Keep the terminal open while using the dashboard. Open http://MASTER_LAN_IP:8501 from a browser on the LAN.

The master setup exports SPARK_DRIVER_HOST and HDFS_MASTER_HOST for the launched dashboard process. If the master's LAN IP changes, rerun setup with the new value.

## 5. Native Linux worker

Repeat this section on every native Linux worker. The worker must use the same Spark version as the master-side Spark image (4.1.4) and Java 17. The training configuration currently expects Python 3.10 on executors.

### 5.1 Prepare the worker

Install Java 17, Python 3.10, python3.10-venv, curl, tar and standard networking tools. Confirm:

    java -version
    python3.10 --version

If Python 3.10 is not available in the distribution's package repositories, install it using the distribution's supported Python package source before proceeding. Do not silently substitute a different interpreter while the driver is configured for Python 3.10.

Copy the project to the worker temporarily, or at minimum make setup_linux_worker.sh and src/node_monitor_agent.py available at the same relative paths.

### 5.2 Start Spark and monitoring

For a worker with address 192.168.137.229 and master 192.168.137.1:

    chmod +x setup_linux_worker.sh
    MASTER_LAN_IP=192.168.137.1 \
    WORKER_LAN_IP=192.168.137.229 \
    WORKER_CORES=2 \
    WORKER_MEMORY=3g \
    bash ./setup_linux_worker.sh

Repeat with each worker's own IP. Use different WORKER_LAN_IP values for different physical machines.

The script downloads Spark 4.1.4 if needed, creates an isolated telemetry venv, installs psutil, copies node_monitor_agent.py to the worker's home directory, starts the agent on TCP 9108 and starts the Spark Standalone Worker on TCP 7078 with UI on TCP 8081.

The agent and worker are started with nohup and PID/log files. They continue after the setup shell exits, but this script does not install automatic boot-time systemd units. Rerun it after a reboot. Logs are in ~/.local/share/spark/logs/ and PID files are in ~/.local/state/kkbox-churn/.

### 5.3 Verify the worker

On the worker:

    pgrep -af 'org.apache.spark.deploy.worker.Worker'
    curl http://127.0.0.1:9108/metrics
    ss -lntp | grep -E '7078|8081|9108'

From the master:

    curl http://192.168.137.229:9108/metrics
    timeout 3 bash -c '</dev/tcp/192.168.137.229/7078'

The worker should appear as ALIVE in http://MASTER_LAN_IP:8080.

## 6. Windows master using WSL2

This mode runs the Linux master setup inside a WSL2 distribution. The Windows project directory is accessed from WSL through /mnt/<drive>/....

### 6.1 Prerequisites

- Windows 11 with a recent WSL2 version that supports mirrored networking.
- An installed Ubuntu WSL2 distribution.
- Docker Desktop with integration enabled for that Ubuntu distribution, or a correctly configured native Docker Engine inside WSL.
- The project and dataset files available on the Windows drive.
- A LAN IP reachable from worker machines.

When using Docker Desktop, open Docker Desktop Settings and enable WSL integration for the selected distro. Verify from WSL that docker info and docker compose version work.

### 6.2 Run the Windows-to-WSL master setup

Open PowerShell in the project directory:

    Set-ExecutionPolicy -Scope Process Bypass
    .\setup_windows_master_wsl.ps1 -Distro Ubuntu -MasterLanIp 192.168.137.1 -PhysicalWorkerHosts "192.168.137.113,192.168.137.229"

The script:
1. Checks the requested WSL distribution.
2. Enables WSL mirrored networking in %UserProfile%\.wslconfig, preserving a timestamped backup.
3. Runs wsl --shutdown to apply the setting.
4. Invokes setup_linux_master.sh from the WSL-mounted project directory.

The Linux setup starts the master services and Streamlit. Open http://192.168.137.1:8501 from the browser.

If Docker commands fail inside WSL, verify Docker Desktop's WSL integration or the native Docker Engine before rerunning the script.

## 7. Windows worker using WSL2

This mode runs a Linux Spark Worker inside WSL2 on a Windows machine. The worker advertises the Windows machine's LAN IP through WSL mirrored networking.

### 7.1 Prerequisites

- WSL2 and an Ubuntu distribution.
- A recent Windows 11 build supporting WSL mirrored networking.
- Java 17 and Python 3.10 available inside the distro.
- Project files available from the WSL-mounted Windows drive.
- Master LAN IP and the Windows worker's LAN IP.

### 7.2 Run the worker setup

Open PowerShell in the project directory and run:

    Set-ExecutionPolicy -Scope Process Bypass
    .\setup_windows_worker_wsl.ps1 -Distro Ubuntu -MasterLanIp 192.168.137.1 -WorkerLanIp 192.168.137.113 -WorkerCores 2 -WorkerMemory 3g

The script enables mirrored networking if necessary, restarts WSL, then invokes setup_linux_worker.sh inside the selected distribution. It installs/uses Spark 4.1.4, starts the Spark worker and starts the telemetry agent.

Use the Windows machine's LAN IP as WORKER_LAN_IP only after confirming that the WSL distro can bind to and advertise it in mirrored mode. Verify from another LAN machine that ports 7078, 8081 and 9108 are reachable.

## 8. Windows physical master with local Docker workers and physical workers

This is the recommended three-PC hybrid topology used by the current dashboard:

- **Master PC (Windows):** Docker Desktop runs HDFS NameNode, HDFS DataNode, Spark Master and one or more Docker Spark Workers. Streamlit runs directly on Windows.
- **Physical Worker 1 (Windows):** a Spark Worker and telemetry agent run inside Ubuntu WSL2 using mirrored networking.
- **Physical Worker 2 (Linux):** a native Linux Spark Worker and telemetry agent run directly on the machine.

The Docker workers on the Windows master use Docker bridge addresses. The other two workers advertise their LAN addresses. All register with the same Spark Master.

### 8.1 First-time setup

Install Docker Desktop with Linux containers enabled, Python 3.10+ on the Windows master, Java 17 and Python 3.10 on each worker, and place the three KKBox CSV files under data/raw/kkbox/ on the master.

Install the Python dashboard dependencies once:

    python -m pip install -r requirements.txt

Start the master-side HDFS and Spark Master services:

    docker compose up -d --build namenode datanode spark-master

Start the physical workers using the instructions in sections 5 and 7. The Windows WSL2 worker setup command is:

    Set-ExecutionPolicy -Scope Process Bypass
    .\setup_windows_worker_wsl.ps1 -Distro Ubuntu -MasterLanIp 192.168.137.1 -WorkerLanIp 192.168.137.113 -WorkerCores 2 -WorkerMemory 3g

On the native Linux worker:

    MASTER_LAN_IP=192.168.137.1 WORKER_LAN_IP=192.168.137.229 WORKER_CORES=2 WORKER_MEMORY=3g bash ./setup_linux_worker.sh

Configure the Windows master's environment and launch Streamlit:

    $env:SPARK_DRIVER_HOST = "192.168.137.1"
    $env:HDFS_MASTER_HOST = "192.168.137.1"
    $env:PHYSICAL_WORKER_HOSTS = "192.168.137.113,192.168.137.229"
    python -m streamlit run app.py --server.address 0.0.0.0 --server.port 8501

Open http://localhost:8501 on the master.

### 8.2 Normal startup after a computer restart

No dependency reinstall is needed for an ordinary reboot.

1. Start Docker Desktop on the Windows master and wait until its engine is ready.
2. In PowerShell, go to the project root and start HDFS and Spark Master:

       cd E:\Codes\Projects\ChatGpt\ecommerce-churn-cohort
       docker compose up -d namenode datanode spark-master

3. Start the Windows WSL2 physical worker by running setup_windows_worker_wsl.ps1 as shown above.
4. Start the native Linux physical worker by rerunning setup_linux_worker.sh with its master and worker IPs.
5. Set SPARK_DRIVER_HOST, HDFS_MASTER_HOST and PHYSICAL_WORKER_HOSTS in the PowerShell session, then start Streamlit.
6. In Spark Master UI at http://localhost:8080, confirm both physical workers are ALIVE.
7. In Streamlit, choose **Hybrid (local Docker + physical)** and set the desired number of local Windows Docker workers.
8. Click **Apply worker topology**. This starts/scales the local Docker workers on the master.
9. Confirm the local Docker workers and both physical workers are ALIVE in Spark Master UI, then start training.

The local Docker workers are intentionally started by the dashboard's Apply worker topology action in Hybrid mode. Do not run the old remote-Linux-Docker hybrid bootstrap for this topology.

### 8.3 Other Windows master modes

For a physical-only setup, start HDFS and Spark Master, keep the local spark-worker service stopped, start the physical workers and select **Physical only** in the dashboard.

For a local Docker-only test, start HDFS and Spark Master and choose **Local Docker only** in the dashboard. The local worker count is controlled in the sidebar.

The older scripts/run_cluster.ps1 bootstrap is a standalone physical-only or Docker-only helper; it is not the startup command for the current combined Hybrid mode.

## 9. Start and operate a training run

1. Ensure the master services are running.
2. Ensure the intended Spark workers are running and registered ALIVE.
3. Open the dashboard at http://MASTER_LAN_IP:8501 (or http://localhost:8501 on the master).
4. Select the worker topology in the sidebar.
5. For Physical only, select the required physical worker IPs. The app checks that selected hosts are registered and reachable.
6. For Local Docker only, specify the number of local Docker workers.
7. For Hybrid (local Docker + physical), set the local Docker worker count and select the required physical worker IPs. Click Apply worker topology to start the local Docker workers on the master.
8. Start Run full training pipeline.
9. Use Monitoring to inspect worker health, Spark jobs/tasks, HDFS and host resource metrics.
10. Use Stop current training run if cancellation is required.

Spark Standalone does not guarantee per-application worker affinity. The physical-worker selection is a required-host preflight, not a scheduler pin. To guarantee an exclusive subset, stop non-selected Spark worker daemons before starting the run.

## 10. Results and cancellation behaviour

A run is marked COMPLETED only after the model, metrics and non-empty prediction outputs have been verified. On success, these artifacts are copied into artifacts/last_successful/.

If a later run is stopped:
- Monitoring reports the attempt as CANCELLED.
- The dashboard records the stop in artifacts/run_status.json and artifacts/training.log.
- The Results tab displays a cancellation notice.
- The Results tab continues to display metrics and predictions from artifacts/last_successful/.
- A cancelled run does not replace the last successful snapshot.

A failed run behaves similarly for result preservation: the failure is reported while previous successful metrics remain available.

## 11. Monitoring and health checks

The Monitoring tab polls its monitoring fragment every five seconds. It does not intentionally refresh the entire Streamlit page every five seconds.

The worker telemetry agent runs on every worker host and serves:
- http://WORKER_LAN_IP:9108/health
- http://WORKER_LAN_IP:9108/metrics

The master reads its own OS metrics locally. The worker agent is not required on the master.

A worker needs only the standalone node_monitor_agent.py file and psutil for OS telemetry. It does not need the full project directory, datasets, or training source just to provide host metrics. A Spark worker still needs Java, Spark and the configured Python interpreter to execute Spark tasks.

Useful worker commands:

    pgrep -af node_monitor_agent.py
    pgrep -af 'org.apache.spark.deploy.worker.Worker'
    ss -lntp | grep -E '7078|8081|9108'
    tail -n 100 ~/.local/share/spark/logs/spark-worker.log
    tail -n 100 ~/.local/share/spark/logs/node-monitor-agent.log

If a worker is marked DEAD while its machine is powered on, Spark has lost worker heartbeats. Check the worker JVM process, worker logs, network route, firewall and host resource pressure. Machine power state and Spark worker process state are different things.

## 12. Troubleshooting

### Worker is powered on but Spark marks it DEAD

- Confirm the Worker JVM is running with pgrep.
- Inspect spark-worker.log for JVM exit, bind or heartbeat errors.
- Confirm TCP 7077 from worker to master.
- Confirm TCP 7078 and 8081 from master to worker.
- Confirm executor callback ports 44575 and 44576 are reachable from worker to master during a job.
- Check firewall rules, route changes, Wi-Fi/Ethernet switching, suspend/resume and memory pressure.
- On Linux, inspect kernel/OOM events with journalctl -k and dmesg.

### Worker is ALIVE but host metrics are missing

- Confirm node_monitor_agent.py is running.
- Confirm it listens on 0.0.0.0:9108, not only 127.0.0.1.
- Test /metrics locally and from the master.
- Allow TCP 9108 from the master LAN IP only.
- Ensure psutil is installed in the agent's virtual environment.

### WSL worker is not reachable from the LAN

- Confirm mirrored networking is enabled in %UserProfile%\.wslconfig under [wsl2].
- Run wsl --shutdown and restart the distro.
- Confirm the chosen WORKER_LAN_IP is assigned/reachable in the WSL environment.
- Check Windows Defender Firewall and Linux firewall rules.
- Do not use the default WSL NAT address as the Spark worker advertised address.
- Re-run setup_windows_worker_wsl.ps1 after correcting networking.

### Spark driver cannot reach HDFS

- Confirm the DataNode advertises the master's LAN IP through HDFS_MASTER_HOST.
- Confirm ports 9000 and 9866 are open from workers to master.
- Set SPARK_DRIVER_HOST and HDFS_MASTER_HOST to the same reachable master LAN address.
- Do not use Docker-only service names such as namenode from physical worker hosts.

### Results tab says no successful run is available

- Check artifacts/run_status.json and artifacts/training.log.
- A successful run must have metrics, a non-empty predictions directory and a saved model.
- The preserved result snapshot is artifacts/last_successful/.
- If the project predates the snapshot feature, complete one successful run to create the snapshot.

## 13. Service URLs

| Service | URL |
|---|---|
| Streamlit dashboard | http://MASTER_LAN_IP:8501 |
| Spark master UI | http://MASTER_LAN_IP:8080 |
| Active Spark application UI | http://MASTER_LAN_IP:4040 |
| HDFS NameNode UI | http://MASTER_LAN_IP:9870 |
| Worker Spark UI | http://WORKER_LAN_IP:8081 |
| Worker telemetry | http://WORKER_LAN_IP:9108/metrics |
