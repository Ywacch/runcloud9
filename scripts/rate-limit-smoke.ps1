# Smoke-test cloud9-api rate limits against a local server.
#
# Start the API with limiting ON (off by default in non-production):
#   $env:API_RATE_LIMIT_ENABLED = "true"
#   # optional: tight limits so you hit 429 quickly
#   $env:API_RATE_LIMIT_RPS = "2"
#   $env:API_RATE_LIMIT_BURST = "2"
#   $env:API_RATE_LIMIT_EXPENSIVE_BURST = "2"
#   $env:API_RATE_LIMIT_PUBLIC_BURST = "5"
#   go run ./cmd/api
#
# Usage:
#   .\scripts\rate-limit-smoke.ps1 -Mode public
#   .\scripts\rate-limit-smoke.ps1 -Mode standard -Count 80
#   .\scripts\rate-limit-smoke.ps1 -Mode expensive -Count 10

param(
    [ValidateSet("public", "standard", "expensive")]
    [string]$Mode = "public",

    [string]$BaseUrl = "http://localhost:8080",

    [int]$Count = 0,

    [string]$BearerToken = ""
)

$ErrorActionPreference = "Stop"

switch ($Mode) {
    "public" {
        if ($Count -le 0) { $Count = 80 }
        $Method = "GET"
        $Path = "/health"
        $Body = $null
    }
    "standard" {
        if ($Count -le 0) { $Count = 80 }
        $Method = "GET"
        $Path = "/deployments"
        $Body = $null
    }
    "expensive" {
        if ($Count -le 0) { $Count = 10 }
        $Method = "POST"
        $Path = "/deployments"
        # Invalid but small JSON, rate limit runs before handler validation matters.
        $Body = '{"template":"url-shortener"}'
    }
}

$uri = ($BaseUrl.TrimEnd("/") + $Path)
Write-Host "Mode: $Mode  Requests: $Count  URL: $uri"
Write-Host ""

$statusCounts = @{}
$first429 = $null

for ($i = 1; $i -le $Count; $i++) {
    $headers = @{}
    if ($BearerToken) {
        $headers["Authorization"] = "Bearer $BearerToken"
    }

    try {
        $params = @{
            Uri             = $uri
            Method          = $Method
            Headers         = $headers
            UseBasicParsing = $true
            TimeoutSec      = 10
        }
        if ($Body) {
            $params["ContentType"] = "application/json"
            $params["Body"] = $Body
        }
        $resp = Invoke-WebRequest @params
        $code = [int]$resp.StatusCode
        $retryAfter = $resp.Headers["Retry-After"]
    }
    catch {
        if ($_.Exception.Response) {
            $code = [int]$_.Exception.Response.StatusCode
            $retryAfter = $_.Exception.Response.Headers["Retry-After"]
        }
        else {
            Write-Error $_
            exit 1
        }
    }

    if (-not $statusCounts.ContainsKey($code)) {
        $statusCounts[$code] = 0
    }
    $statusCounts[$code]++

    $line = "{0,3}: HTTP {1}" -f $i, $code
    if ($retryAfter) {
        $line += "  Retry-After=$retryAfter"
    }
    Write-Host $line

    if ($code -eq 429 -and -not $first429) {
        $first429 = $i
    }
}

Write-Host ""
Write-Host "Summary:"
$statusCounts.GetEnumerator() | Sort-Object Name | ForEach-Object {
    Write-Host ("  {0}: {1}" -f $_.Key, $_.Value)
}

if ($first429) {
    Write-Host ""
    Write-Host "Rate limit hit on request #$first429 (429)."
}
else {
    Write-Host ""
    Write-Host "No 429 responses. If you expected limits:"
    Write-Host "  - set API_RATE_LIMIT_ENABLED=true before starting the API"
    Write-Host "  - lower API_RATE_LIMIT_*_BURST / RPS for faster trips"
    Write-Host "  - use -Count high enough to exceed burst (e.g. 80 for standard/public)"
}
