[CmdletBinding()]
param(
    [string]$Distro = "Ubuntu",
    [Parameter(Mandatory=$true)][string]$MasterLanIp,
    [Parameter(Mandatory=$true)][string]$WorkerLanIp,
    [string]$ProjectRoot = $PSScriptRoot,
    [int]$WorkerCores = 2,
    [string]$WorkerMemory = "3g"
)
$ErrorActionPreference = "Stop"

function Ensure-WslMirroredNetworking {
    $config = Join-Path $env:USERPROFILE ".wslconfig"
    $content = if (Test-Path $config) { Get-Content $config -Raw } else { "" }
    if ($content -match '(?im)^\s*networkingMode\s*=\s*mirrored\s*$') { return }
    $backup = "$config.backup-$(Get-Date -Format yyyyMMdd-HHmmss)"
    if (Test-Path $config) { Copy-Item $config $backup }
    if ($content -match '(?im)^\s*networkingMode\s*=') {
        $content = [regex]::Replace($content, '(?im)^\s*networkingMode\s*=.*$', 'networkingMode=mirrored', 1)
    } else {
        $match = [regex]::Match($content, '(?im)^\s*\[wsl2\]\s*$')
        if ($match.Success) {
            $content = $content.Insert($match.Index + $match.Length, [Environment]::NewLine + "networkingMode=mirrored")
        } else {
            $content = $content.TrimEnd() + [Environment]::NewLine + [Environment]::NewLine + "[wsl2]" + [Environment]::NewLine + "networkingMode=mirrored" + [Environment]::NewLine
        }
    }
    [System.IO.File]::WriteAllText($config, $content, [System.Text.UTF8Encoding]::new($false))
    Write-Host "Enabled WSL2 mirrored networking. Backup: $backup"
    Write-Host "Restarting WSL to apply networking configuration..."
    wsl.exe --shutdown
}

if (-not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) { throw "WSL is not installed. Install WSL2 and an Ubuntu distribution first." }
$distros = @(wsl.exe --list --quiet | ForEach-Object { ($_.Replace([string][char]0,"")).Trim() } | Where-Object { $_ })
if ($Distro -notin $distros) { throw "WSL distribution '$Distro' was not found. Available: $($distros -join ', ')" }
Ensure-WslMirroredNetworking

$linuxProject = (& wsl.exe -d $Distro -- wslpath -a "$ProjectRoot").Trim()
if (-not $linuxProject) { throw "Could not translate project path into WSL." }
$linuxCommand = "cd '$linuxProject' && chmod +x setup_linux_worker.sh && MASTER_LAN_IP='$MasterLanIp' WORKER_LAN_IP='$WorkerLanIp' WORKER_CORES='$WorkerCores' WORKER_MEMORY='$WorkerMemory' bash ./setup_linux_worker.sh"
Write-Host "Launching Linux Spark worker + monitoring setup in WSL '$Distro'..."
wsl.exe -d $Distro -- bash -lc $linuxCommand
if ($LASTEXITCODE -ne 0) { throw "Linux worker setup failed inside WSL (exit $LASTEXITCODE)." }
