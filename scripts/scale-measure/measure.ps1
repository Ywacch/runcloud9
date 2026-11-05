# Count Prometheus series and Kubernetes objects per session namespace.
# See README.md for usage.
param(
    [Parameter(Mandatory = $true)]
    [string]$Template,

    [Parameter(Mandatory = $true)]
    [string]$NamespacePrefix,

    [Parameter(Mandatory = $true)]
    [string]$PrometheusURL,

    [string]$KubeContext = ''
)

$ErrorActionPreference = 'Stop'

$ignoredNamespaces = @('cloud9-system', 'monitoring')

function Invoke-Kubectl {
    param([string[]]$KubectlArgs)
    $all = @()
    if ($KubeContext) {
        $all += '--context', $KubeContext
    }
    $all += $KubectlArgs
    $out = & kubectl @all 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl failed (exit $LASTEXITCODE): $out"
    }
    return ($out | Out-String)
}

function Test-NamespacePrefix {
    param([string]$Prefix)
    if ($Prefix -eq 'cloud9') {
        throw "NamespacePrefix must not be 'cloud9' alone. Use at least 'cloud9-' plus one more character (for example cloud9-url-shortener)."
    }
    if ($Prefix -notmatch '^cloud9-.+') {
        throw "NamespacePrefix must start with 'cloud9-' and include at least one character after it."
    }
}

function Get-SeriesByNamespace {
    param([string]$BaseURL)

    $base = $BaseURL.TrimEnd('/')
    # Single-quoted PromQL so PowerShell does not expand $ in {deployment!=""}.
    $promQuery = 'count by (namespace) ({deployment!=""})'

    $response = curl.exe -sG "${base}/api/v1/query" --data-urlencode "query=$promQuery"
    if ($LASTEXITCODE -ne 0) {
        throw "curl.exe failed querying Prometheus at $base"
    }

    $parsed = $response | ConvertFrom-Json
    if ($parsed.status -ne 'success') {
        throw "Prometheus query failed: $response"
    }

    $byNS = @{}
    foreach ($r in $parsed.data.result) {
        $ns = $r.metric.namespace
        if (-not $ns) {
            continue
        }
        $byNS[$ns] = [int][double]$r.value[1]
    }
    return $byNS
}

function Get-KubernetesObjectCount {
    param([string]$Namespace)

    $resourceList = @(
        'deploy', 'rs', 'po', 'svc', 'endpoints',
        'endpointslices.discovery.k8s.io', 'cm', 'netpol', 'resourcequota', 'sa', 'secret'
    ) -join ','

    $raw = Invoke-Kubectl -KubectlArgs @('get', $resourceList, '-n', $Namespace, '-o', 'json')
    $doc = $raw | ConvertFrom-Json

    $items = 0
    if ($null -ne $doc.items) {
        $items = @($doc.items).Count
    }
    # Namespace object itself.
    return $items + 1
}

function Get-SessionNamespaces {
    param([string]$Prefix)

    $raw = Invoke-Kubectl -KubectlArgs @('get', 'ns', '-o', 'json')
    $doc = $raw | ConvertFrom-Json

    $names = @()
    foreach ($item in $doc.items) {
        $name = $item.metadata.name
        if ($ignoredNamespaces -contains $name) {
            continue
        }
        if (-not $name.StartsWith($Prefix)) {
            continue
        }
        $names += $name
    }
    return ($names | Sort-Object)
}

Test-NamespacePrefix -Prefix $NamespacePrefix

$seriesByNS = Get-SeriesByNamespace -BaseURL $PrometheusURL
$namespaces = Get-SessionNamespaces -Prefix $NamespacePrefix

Write-Output '| namespace | template | series | objects |'

$seriesSum = 0
$objectsSum = 0
$rowCount = 0

foreach ($ns in $namespaces) {
    $series = 0
    if ($seriesByNS.ContainsKey($ns)) {
        $series = $seriesByNS[$ns]
    }
    $objects = Get-KubernetesObjectCount -Namespace $ns
    Write-Output "| $ns | $Template | $series | $objects |"
    $seriesSum += $series
    $objectsSum += $objects
    $rowCount++
}

if ($rowCount -gt 0) {
    $meanSeries = [math]::Round($seriesSum / $rowCount, 2)
    $meanObjects = [math]::Round($objectsSum / $rowCount, 2)
    Write-Output "| mean | $Template | $meanSeries | $meanObjects |"
} else {
    Write-Output "| mean | $Template | 0 | 0 |"
}
