param(
    [Parameter(Mandatory = $true)]
    [string]$Namespace,
    [Parameter(Mandatory = $false)]
    [string]$ExpectedTag = "",
    [Parameter(Mandatory = $false)]
    [string]$RegistryPrefix = ""
)

$ErrorActionPreference = "Stop"

if (-not $RegistryPrefix) {
    $RegistryPrefix = $env:TEMPLATE_IMAGE_REGISTRY
}
if (-not $RegistryPrefix) {
    $RegistryPrefix = "us-central1-docker.pkg.dev/runcloud9/cloud9-images"
}

if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    throw "kubectl not found in PATH"
}
if (-not (Get-Command gcloud -ErrorAction SilentlyContinue)) {
    throw "gcloud not found in PATH"
}

$pods = kubectl get pods -n $Namespace -o json | ConvertFrom-Json
$rows = @()

foreach ($item in $pods.items) {
    $specImages = @{}
    foreach ($c in $item.spec.containers) {
        $specImages[$c.name] = $c.image
    }
    foreach ($st in $item.status.containerStatuses) {
        $specImage = $specImages[$st.name]
        if ($specImage -and $specImage.StartsWith("$RegistryPrefix/")) {
            $rows += [PSCustomObject]@{
                Pod       = $item.metadata.name
                Container = $st.name
                SpecImage = $specImage
                ImageID   = $st.imageID
            }
        }
    }
}

if ($rows.Count -eq 0) {
    throw "No containers found in namespace $Namespace for registry prefix $RegistryPrefix"
}

Write-Host "Verifying namespace: $Namespace"
Write-Host "Registry prefix: $RegistryPrefix"
if ($ExpectedTag) {
    Write-Host "Expected tag: $ExpectedTag"
}
Write-Host ""

$fail = $false
$outputRows = @()

foreach ($row in $rows) {
    $actualDigest = ""
    if ($row.ImageID -match "@(?<digest>sha256:[a-f0-9]+)$") {
        $actualDigest = $matches["digest"]
    }

    $tag = ""
    if ($row.SpecImage -match ":(?<tag>[^:@]+)$") {
        $tag = $matches["tag"]
    }
    if ($ExpectedTag -and $tag -ne $ExpectedTag) {
        Write-Host "TAG MISMATCH: $($row.Pod)/$($row.Container) uses $tag, expected $ExpectedTag"
        $fail = $true
    }

    $registryDigest = ""
    try {
        $registryDigest = gcloud artifacts docker images describe $row.SpecImage --format="value(image_summary.digest)"
    } catch {
        $registryDigest = ""
    }

    if (-not $registryDigest) {
        Write-Host "REGISTRY LOOKUP FAILED: $($row.SpecImage)"
        $fail = $true
    } elseif (-not $actualDigest) {
        Write-Host "MISSING IMAGEID DIGEST: $($row.Pod)/$($row.Container) imageID=$($row.ImageID)"
        $fail = $true
    } elseif ($registryDigest.Trim() -ne $actualDigest) {
        Write-Host "DIGEST MISMATCH: $($row.Pod)/$($row.Container) spec=$($row.SpecImage) running=$actualDigest registry=$registryDigest"
        $fail = $true
    }

    $outputRows += [PSCustomObject]@{
        Pod          = $row.Pod
        Container    = $row.Container
        SpecImage    = $row.SpecImage
        ActualDigest = $(if ($actualDigest) { $actualDigest } else { "<missing>" })
    }
}

$outputRows | Format-Table -AutoSize

if ($fail) {
    throw "Verification failed."
}

Write-Host ""
Write-Host "Verification passed: running digests match registry digests."
