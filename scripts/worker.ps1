param([ValidateRange(1,8)][int]$Workers = 2)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
Write-Host "Starting $Workers Spark worker(s) and registering them with spark://spark-master:7077 ..."
docker compose up -d spark-master
if ($LASTEXITCODE -ne 0) { throw "Could not start Spark master." }
docker compose up -d --scale spark-worker=$Workers spark-worker
if ($LASTEXITCODE -ne 0) { throw "Could not start Spark workers." }
Start-Sleep -Seconds 5
docker compose ps
Write-Host "Workers connect to the master over the private Docker data-net network. Open http://localhost:8080 to verify registration."
