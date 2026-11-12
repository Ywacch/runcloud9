# Waits until the capacity probe writes ReadyFile (wave is up, DELETE has not
# started). Scrapes API /metrics, then runs measure.ps1 to completion, then
# writes ReleaseFile so the probe can delete.
param(
    [Parameter(Mandatory = $true)]
    [string]$ReadyFile,

    [Parameter(Mandatory = $true)]
    [string]$ReleaseFile,

    [string]$OutDir,
    [string]$PrometheusURL = "http://127.0.0.1:9090",
    [string]$ApiMetricsURL = "http://127.0.0.1:18080/metrics",
    [string]$Template = "url-shortener-small",
    [string]$Prefix = "cloud9-url-shortener",
    [int]$ReadyTimeoutSec = 1800
)

$ErrorActionPreference = 'Stop'
$ganymede = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
if (-not $OutDir) {
    $OutDir = Split-Path -Parent $ReleaseFile
}
if (-not $OutDir) {
    $OutDir = "."
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
if (Test-Path -LiteralPath $ReleaseFile) {
    Remove-Item -LiteralPath $ReleaseFile -Force
}

function Publish-Release {
    $parent = Split-Path -Parent $ReleaseFile
    if ($parent) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
    New-Item -ItemType File -Path $ReleaseFile -Force | Out-Null
}

$waited = 0
while (-not (Test-Path -LiteralPath $ReadyFile)) {
    if ($waited -ge $ReadyTimeoutSec) {
        [Console]::Error.WriteLine("ready file not seen after ${ReadyTimeoutSec}s: $ReadyFile")
        exit 2
    }
    Start-Sleep -Seconds 5
    $waited += 5
}

$measureErr = $null
try {
    Set-Location $ganymede
    $ns = @(kubectl get ns -o name 2>&1 | Select-String -SimpleMatch $Prefix).Count
    if ($ns -lt 1) { $ns = 0 }

    $apiFile = Join-Path $OutDir "api-metrics-at-${ns}-ns.txt"
    try {
        $metrics = (Invoke-WebRequest -Uri $ApiMetricsURL -UseBasicParsing -TimeoutSec 30).Content
        Set-Content -LiteralPath $apiFile -Value $metrics -Encoding utf8
        $throttle = @($metrics -split "`n" | Where-Object { $_ -match '^cloud9_k8s_throttle_waits_total ' })
        if ($throttle.Count -gt 0) {
            Set-Content -LiteralPath (Join-Path $OutDir "throttle-at-peak.txt") -Value "THROTTLE_AT_PEAK=$($throttle[0])" -Encoding utf8
        }
    } catch {
        "metrics scrape failed: $_" | Set-Content -LiteralPath (Join-Path $OutDir "api-metrics-at-${ns}-ns.err.txt") -Encoding utf8
        $measureErr = $_
    }

    $measureFile = Join-Path $OutDir "measure-at-${ns}-ns.txt"
    $lines = & "$PSScriptRoot/measure.ps1" -Template $Template -NamespacePrefix $Prefix -PrometheusURL $PrometheusURL
    Set-Content -LiteralPath $measureFile -Value $lines -Encoding utf8
    $lines | ForEach-Object { Write-Output $_ }
} catch {
    $measureErr = $_
    "$_" | Set-Content -LiteralPath (Join-Path $OutDir "measure.err.txt") -Encoding utf8
}

Publish-Release

if ($null -ne $measureErr) {
    [Console]::Error.WriteLine($measureErr)
    exit 1
}
