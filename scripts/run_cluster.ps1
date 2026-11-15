param(
    [ValidateRange(0,4)][int]$LocalWorkers = 0,
    [ValidateRange(30,300)][int]$WaitSeconds = 90
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

function Get-DockerWorkerIps([switch]$IncludeStopped) {
    $dockerArgs = @("compose", "ps")
    if ($IncludeStopped) { $dockerArgs += "--all" }
    $dockerArgs += @("-q", "spark-worker")
    $ids = @(& docker @dockerArgs)
    if ($LASTEXITCODE -ne 0) { throw "Could not list Docker Spark worker containers." }
    $ips = @()
    foreach ($id in $ids) {
        if (-not $id) { continue }
        $found = @(& docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}' $id)
        if ($LASTEXITCODE -ne 0) { throw "Could not inspect Docker worker container $id." }
        $ips += ($found -split "\s+" | Where-Object { $_ })
    }
    return @($ips | Sort-Object -Unique)
}

Write-Host "[1/4] Starting HDFS and Spark master..."
docker compose up -d namenode datanode spark-master
if ($LASTEXITCODE -ne 0) { throw "Could not start Docker services." }

if ($LocalWorkers -gt 0) {
    Write-Host "[2/4] Starting $LocalWorkers local Docker worker(s)..."
    & "$PSScriptRoot\worker.ps1" -Workers $LocalWorkers
    if ($LASTEXITCODE -ne 0) { throw "Could not start local Spark workers." }
    $selectedDockerIps = @(Get-DockerWorkerIps)
    if ($selectedDockerIps.Count -eq 0) { throw "No running local Docker worker container IPs were found." }
    Write-Host ("Only accepting local Docker worker IPs: " + ($selectedDockerIps -join ", "))
} else {
    Write-Host "[2/4] Keeping Docker workers stopped for physical-worker mode..."
    docker compose stop spark-worker
    if ($LASTEXITCODE -ne 0) { throw "Could not stop local Docker workers." }
    $excludedDockerIps = @(Get-DockerWorkerIps -IncludeStopped)
    if ($excludedDockerIps.Count -gt 0) {
        Write-Host ("Ignoring stopped Docker worker IPs: " + ($excludedDockerIps -join ", "))
    }
}

Write-Host "[3/4] Waiting for a worker in the selected mode..."
$deadline = (Get-Date).AddSeconds($WaitSeconds)
$aliveWorkers = @()
while ((Get-Date) -lt $deadline) {
    try {
        $master = Invoke-RestMethod -Uri "http://localhost:8080/json/" -TimeoutSec 3
        $allAlive = @($master.workers | Where-Object { $_.state -eq "ALIVE" })
        if ($LocalWorkers -gt 0) {
            $unexpected = @($allAlive | Where-Object { $_.host -notin $selectedDockerIps })
            if ($unexpected.Count -gt 0) {
                $hosts = ($unexpected | ForEach-Object { $_.host }) -join ", "
                throw "Refusing to mix Docker-private and physical Spark workers. Unexpected ALIVE host(s): $hosts"
            }
            $aliveWorkers = @($allAlive | Where-Object { $_.host -in $selectedDockerIps })
        } else {
            $aliveWorkers = @($allAlive | Where-Object { $_.host -notin $excludedDockerIps })
        }
        if ($aliveWorkers.Count -gt 0) { break }
    } catch {
        if ($_.Exception.Message -like "Refusing to mix Docker-private*") { throw }
    }
    Start-Sleep -Seconds 3
}
if ($aliveWorkers.Count -eq 0) {
    if ($LocalWorkers -gt 0) {
        throw "No selected local Docker worker registered as ALIVE. Check the Spark master UI and Docker worker logs."
    }
    throw "No physical Ubuntu worker registered as ALIVE. Start the Ubuntu worker and retry, or explicitly choose -LocalWorkers 1."
}

Write-Host "[4/4] Waiting for HDFS and checking KKBox input paths..."
$hdfsReady = $false
for ($i = 0; $i -lt 60; $i++) {
    docker compose exec -T namenode hdfs dfs -ls / 2>$null | Out-Null
    if ($LASTEXITCODE -eq 0) { $hdfsReady = $true; break }
    Start-Sleep -Seconds 3
}
if (-not $hdfsReady) { throw "HDFS did not become ready." }

docker compose exec -T namenode hdfs dfs -mkdir -p /kkbox/input /kkbox/output
if ($LASTEXITCODE -ne 0) { throw "Could not create HDFS input/output directories." }
docker compose exec -T namenode hdfs dfs -chmod 777 /kkbox/output
if ($LASTEXITCODE -ne 0) { throw "Could not set local-demo output permissions." }

foreach ($file in @("train.csv", "transactions.csv", "members_v3.csv")) {
    docker compose exec -T namenode hdfs dfs -test -e "/kkbox/input/$file"
    if ($LASTEXITCODE -ne 0) {
        $localFile = Join-Path $PSScriptRoot "..\data\raw\kkbox\$file"
        if (-not (Test-Path $localFile)) { throw "Required dataset file is missing: $localFile" }
        docker compose exec -T namenode hdfs dfs -D dfs.client.use.datanode.hostname=false -put -f "/opt/project/data/raw/kkbox/$file" "/kkbox/input/$file"
        if ($LASTEXITCODE -ne 0) { throw "Could not upload $file to HDFS." }
    }
}
docker compose exec -T namenode hdfs dfs -ls -h /kkbox/input

Write-Host ""
Write-Host "Cluster ready. ALIVE workers:"
$aliveWorkers | ForEach-Object { Write-Host ("  " + $_.id + " | " + $_.host + " | cores=" + $_.cores + " | memoryMB=" + $_.memory) }
Write-Host "Spark master UI: http://localhost:8080"
Write-Host "Spark application UI (while a job is running): http://localhost:4040"
Write-Host "HDFS NameNode UI: http://localhost:9870"
Write-Host "Start the Streamlit dashboard with: python -m streamlit run app.py"
