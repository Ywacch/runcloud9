# Phase 4 Test 1 — semi-sync + remote_apply + promote_replica
# Usage: .\scripts\phase4-test1-probe.ps1 -Namespace cloud9-url-shortener-...
param(
    [Parameter(Mandatory = $true)]
    [string]$Namespace,
    [ValidateSet('baseline', 'post-promote')]
    [string]$Phase = 'baseline'
)

$ErrorActionPreference = 'Continue'

function Write-Section($title) {
    Write-Host ""
    Write-Host "=== $title ===" -ForegroundColor Cyan
}

function Get-WriterPod {
    param([string]$Ns)
    $ep = kubectl -n $Ns get endpoints db -o json 2>$null | ConvertFrom-Json
    if ($ep -and $ep.subsets -and $ep.subsets[0].addresses) {
        return $ep.subsets[0].addresses[0].targetRef.name
    }
    return $null
}

function Exec-WriterSQL {
    param([string]$Ns, [string]$Sql)
    $writer = Get-WriterPod -Ns $Ns
    if (-not $writer) {
        Write-Host "writer pod: NOT FOUND (db endpoints empty)"
        return
    }
    $container = if ($writer -match '^postgres-replica') { 'postgres-replica' } else { 'postgres' }
    Write-Host "writer: $writer (container=$container)"
    kubectl -n $Ns exec $writer -c $container -- psql -U syscrack -d syscrack -tAc $Sql 2>&1
}

Write-Section "namespace $Namespace phase=$Phase"
kubectl get ns $Namespace 2>&1
if ($LASTEXITCODE -ne 0) { exit 1 }

Write-Section "workloads"
kubectl -n $Namespace get deploy/postgres 2>&1
kubectl -n $Namespace get sts/postgres-replica 2>&1
kubectl -n $Namespace get pods -l 'app in (postgres,postgres-replica)' -o wide 2>&1

Write-Section "service db endpoints"
kubectl -n $Namespace get endpoints db -o wide 2>&1
$writer = Get-WriterPod -Ns $Namespace
Write-Host "resolved writer pod: $(if ($writer) { $writer } else { '(none)' })"

Write-Section "postgres deploy scale"
$replicas = kubectl -n $Namespace get deploy postgres -o jsonpath='{.spec.replicas}' 2>&1
Write-Host "deployment postgres spec.replicas: $replicas"

Write-Section "sync GUCs on writer"
Exec-WriterSQL -Ns $Namespace -Sql "SHOW synchronous_commit;"
Exec-WriterSQL -Ns $Namespace -Sql "SHOW synchronous_standby_names;"
Exec-WriterSQL -Ns $Namespace -Sql "SHOW wal_keep_size;"

Write-Section "writer recovery state"
Exec-WriterSQL -Ns $Namespace -Sql "SELECT pg_is_in_recovery();"

Write-Section "pg_stat_replication (from writer)"
Exec-WriterSQL -Ns $Namespace -Sql @"
SELECT application_name, state, sync_state,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS lag_bytes,
       replay_lag
FROM pg_stat_replication
ORDER BY application_name;
"@

if ($Phase -eq 'post-promote') {
    Write-Section "standby recovery (each replica pod)"
    $replicaPods = (kubectl -n $Namespace get pods -l app=postgres-replica -o jsonpath='{.items[*].metadata.name}' 2>&1) -split ' '
    foreach ($pod in $replicaPods) {
        if (-not $pod) { continue }
        if ($pod -eq $writer) {
            Write-Host "$pod = writer (skip standby check)"
            continue
        }
        Write-Host "--- $pod ---"
        kubectl -n $Namespace exec $pod -c postgres-replica -- psql -U syscrack -d syscrack -tAc "SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn();" 2>&1
        kubectl -n $Namespace exec $pod -c postgres-replica -- psql -U syscrack -d syscrack -tAc "SELECT pg_is_wal_replay_paused();" 2>&1
    }
}

Write-Section "app env (read routing)"
kubectl -n $Namespace exec deploy/app-server -- printenv 2>&1 | Select-String -Pattern 'READ_|DB_HOST|DB_SYNC'

Write-Section "done"
