<#
.SYNOPSIS
Script to run Locust in various modes (local/distributed, UI/headless) for Windows environments.

.DESCRIPTION
This script translates standard Linux Bash execution logic into PowerShell.
It handles background process spawning for distributed load testing and
ensures proper lifecycle cleanup of child processes (Master and Workers) upon termination.
#>
[CmdletBinding()]
param(
    [switch]$Headless,
    [switch]$Ui,
    [switch]$Distributed,

    [Alias("w")]
    [int]$Workers = 5,

    [Alias("u")]
    [int]$Users = 100,
    [Alias("r", "spawn-rate")]
    [int]$SpawnRate = 50,
    [Alias("t", "run-time")]
    [string]$RunTime = "900",

    [Alias("num-jobs")]
    [long]$NumJobs = 10000000,
    [Alias("num-nodes")]
    [long]$NumNodes = 50000,
    [Alias("candidates-count")]
    [int]$CandidatesCount = 500,

    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$RemainingArgs
)

# Set execution mode securely
$Mode = "headless"
if ($Ui) {
    $Mode = "ui"
}

# Generate timestamp and format prefixes for benchmarking output files
$CurrentDate = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$PrefixBase = "locust_${CurrentDate}_jobs-${NumJobs}_nodes-${NumNodes}_cc-${CandidatesCount}_u-${Users}_r-${SpawnRate}_t-${RunTime}"

if ($Distributed) {
    $PrefixBase = "${PrefixBase}_w-${Workers}"
}

$CsvPrefix = "benchmark/results/${PrefixBase}"
$HtmlPrefix = "benchmark/results/html/${PrefixBase}.html"

# Ensure the results directory exists (creates it if missing)
$HtmlDirectory = "benchmark/results/html"
if (-not (Test-Path -Path $HtmlDirectory)) {
    New-Item -ItemType Directory -Path $HtmlDirectory -Force | Out-Null
}

# Construct base arguments array for the Locust executable
$LocustArgs = @(
    "-f", "benchmark/locustfile.py",
    "--num-jobs", $NumJobs.ToString(),
    "--num-nodes", $NumNodes.ToString(),
    "--candidates-count", $CandidatesCount.ToString(),
    "-u", $Users.ToString(),
    "-r", $SpawnRate.ToString(),
    "-t", $RunTime.ToString()
)

# Append any unmapped arguments dynamically
if ($RemainingArgs) {
    $LocustArgs += $RemainingArgs
}

$ReportArgs = @(
    "--csv", $CsvPrefix,
    "--csv-full-history",
    "--html", $HtmlPrefix
)

# Initialize array to track spawned processes for graceful shutdown handling
$RunningProcesses = @()

try {
    if ($Distributed) {
        Write-Host "Starting Locust Master..." -ForegroundColor Cyan

        $MasterArgs = @()
        $MasterArgs += $LocustArgs
        $MasterArgs += "--master"
        $MasterArgs += $ReportArgs

        if ($Mode -eq "headless") {
            $MasterArgs += "--headless"
        }

        # Spawn Master process without opening a new console window
        $MasterProcess = Start-Process -FilePath "locust" -ArgumentList $MasterArgs -PassThru -NoNewWindow
        $RunningProcesses += $MasterProcess

        Write-Host "Starting $Workers Locust Workers..." -ForegroundColor Cyan
        for ($i = 0; $i -lt $Workers; $i++) {
            $WorkerArgs = @("-f", "benchmark/locustfile.py", "--worker")
            $WorkerProcess = Start-Process -FilePath "locust" -ArgumentList $WorkerArgs -PassThru -NoNewWindow
            $RunningProcesses += $WorkerProcess
        }

        if ($Mode -ne "headless") {
            Write-Host "Distributed Locust is running! Go to http://localhost:8089" -ForegroundColor Green
            Write-Host "Press [CTRL+C] to stop all processes." -ForegroundColor Yellow
        } else {
            Write-Host "Distributed Locust is running the headless benchmark..." -ForegroundColor Green
        }

        # Block script execution until the master process concludes
        Wait-Process -InputObject $MasterProcess

    } else {
        Write-Host "Starting Standalone Locust..." -ForegroundColor Cyan

        $StandaloneArgs = @()
        $StandaloneArgs += $LocustArgs
        $StandaloneArgs += $ReportArgs

        if ($Mode -eq "headless") {
            Write-Host "Running in HEADLESS mode with args: $($StandaloneArgs -join ' ')" -ForegroundColor DarkGray
            $StandaloneArgs += "--headless"
        } else {
            Write-Host "Standalone Locust is running! Go to http://localhost:8089" -ForegroundColor Green
            Write-Host "Press [CTRL+C] to stop." -ForegroundColor Yellow
        }

        $StandaloneProcess = Start-Process -FilePath "locust" -ArgumentList $StandaloneArgs -PassThru -NoNewWindow
        $RunningProcesses += $StandaloneProcess

        # Block script execution until the standalone process concludes
        Wait-Process -InputObject $StandaloneProcess
    }

    Write-Host "Benchmark finished." -ForegroundColor Green

} finally {
    # Architecture Guard: Ensure all child processes are terminated, mimicking Bash's 'trap'
    Write-Host "`nStopping everything... Cleaning up child processes." -ForegroundColor Yellow
    foreach ($Process in $RunningProcesses) {
        if ($null -ne $Process -and -not $Process.HasExited) {
            Stop-Process -InputObject $Process -Force -ErrorAction SilentlyContinue
        }
    }
}
