# Concurrency Analysis with DDC Artifacts

This guide explains how to estimate safe deployment concurrency using `cloud_deployments` rows and DDC metadata.

## Inputs

- Table: `cloud_deployments`
- Required columns:
  - `namespace`
  - `status`
  - `created_at`
  - `metrics_summary`
- Required `metrics_summary` fields:
  - `terminated_at`
  - `diagnostics.outcome`
  - `diagnostics.failure_signatures`

## SQL Baseline Query

```sql
SELECT
  namespace,
  template,
  status,
  created_at,
  metrics_summary
FROM cloud_deployments
WHERE created_at > NOW() - INTERVAL '7 days'
ORDER BY created_at ASC;
```

The offline report tool computes overlap counts from this dataset.

## Overlap Definition

For deployment `d1`, overlap count is the number of other deployments `d2` where:

- `d2.created_at <= d1.created_at`
- `d2.terminated_at >= d1.created_at`

If `terminated_at` is missing and status is `running|provisioning`, `now()` is used as end time.

## Overlap Buckets

- `0`
- `1-2`
- `3-4`
- `5+`

For each bucket:

- total deployments
- successes / failures
- failure rate
- top failure signatures

## CLI Usage

Environment:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`

Run:

```bash
go run ./cmd/ddc-report -days 7
```

Example output keys:

- `safe_steady_state_concurrency`
- `caution_zone`
- `unsafe_threshold`
- `signature_frequency`

## Interpreting Output

- `safe_steady_state_concurrency`: highest overlap level with strong stability.
- `caution_zone`: overlap range where failures begin to appear intermittently.
- `unsafe_threshold`: first overlap level where failures become frequent.

Use this report as an operational baseline and update it after infrastructure/resource changes.

## Active Capacity Probe

`cmd/capacity-probe` measures live cluster capacity by firing controlled concurrent
deployments. Use it before configuring the admission bouncer (`CLUSTER_DEPLOY_BUDGET_*`
and `MAX_CONCURRENT_PROVISIONS`). Results are valid only for the current GCP account
and quota; rerun after material quota changes or account migration.

### Safety

- Unit tests never call the API.
- Real runs require `-confirm`.
- Auth token must come from env (`CAPACITY_PROBE_TOKEN`), not from checked-in fixtures.
- Reports land in `capacity-probe-results/` (gitignored).

### Prerequisites

1. Deploy an API build where the internal tier allows enough concurrent deployments
   (currently 40).
2. Temporarily raise expensive-route rate limits so the create burst is not rejected
   before it reaches the cluster, for example:
   - `API_RATE_LIMIT_EXPENSIVE_BURST=50`
   - `API_RATE_LIMIT_EXPENSIVE_RPM=3000`
3. Confirm `GET /me` returns `maxConcurrent: 40` for the internal test account.
4. Restore normal rate-limit env values immediately after the campaign.

### Dry-run (no cluster cost)

```bash
go run ./cmd/capacity-probe \
  -manifest ./cmd/capacity-probe/testdata/waves/homogeneous-url-shortener.yaml \
  -dry-run
```

### Live campaign

```bash
export CAPACITY_PROBE_API_URL="https://<cloud9-api-host>"
export CAPACITY_PROBE_TOKEN="<internal-account-jwt>"

go run ./cmd/capacity-probe \
  -manifest ./cmd/capacity-probe/testdata/waves/homogeneous-url-shortener.yaml \
  -confirm \
  -sample-cluster \
  -out ./capacity-probe-results
```

Other fixtures:

- `testdata/waves/mixed-sizes.yaml` — lean vs fat deployments
- `testdata/waves/paced-vs-burst.yaml` — same count, burst vs paced admission

### Choosing bouncer numbers

From the CSV/JSON wave summaries:

- **Room budget** (`CLUSTER_DEPLOY_BUDGET_CPU` / `CLUSTER_DEPLOY_BUDGET_MEMORY`):
  highest total successful weighted occupancy with a strong success rate.
- **Doorway width** (`MAX_CONCURRENT_PROVISIONS`):
  highest stable `peak_simultaneous_provisions` before success collapses.

Prefer the last fully healthy wave, not the first wave that fails.

### Measured defaults (GCP account as of 2026-07-21)

Homogeneous `url-shortener-small` campaign `homogeneous-url-shortener-20260721-091047`:

| Knob | Value | Basis |
|------|-------|--------|
| Doorway | `MAX_CONCURRENT_PROVISIONS=5` | Stable orchestration pace after timeout-after-door fix |
| Room CPU | `CLUSTER_DEPLOY_BUDGET_CPU=18500m` | wave-20 successful weight (20/20) |
| Room memory | `CLUSTER_DEPLOY_BUDGET_MEMORY=49920Mi` | wave-20 successful weight (20/20) |

Unset both budget vars to leave the room gate off.

**Analogies:** the **room** is steady-state weighted occupancy; the **door** is concurrent orchestrations; **person size** is `QuotaResourceLimits` for that design. A jam at the door (too many simultaneous provisions) fails more deployments than the room can hold once people enter in paced batches.

**Ops:** apply [`docs/sql/admitted_weights.sql`](sql/admitted_weights.sql) before enabling the room gate. Keep `cloud9-api` at `replicas: 1` until admission is DB-atomic (in-process mutex + doorway semaphore + deploy queue). Phase 2: room full → **201** `status=queued` (first-fit promote); designs that cannot fit an empty room → **422** `reason=design_exceeds_cluster_capacity` (never queued); queue overflow → **429** `reason=queue_full`. Create still packs free room when the design fits current occupancy. Room `queuePosition` is wait-list index, not strict deploy order (smaller queued entries may promote ahead of a blocked heavier head). Defaults: `MAX_DEPLOY_QUEUE=20`, `DEPLOY_QUEUE_TTL_SECONDS=900`, `PROVISION_ETA_SECONDS=240`. When the doorway is full, admitted deploys stay `provisioning` and set `metricsSummary.provision_phase=waiting_for_provision_slot` until a slot is acquired (orchestration timeout still starts after acquire). GET exposes live `queuePosition` / `estimatedWaitSeconds` for doorway waiters as `ceil(position * doorHoldAvg / doorWidth)` (hold time = acquire→release EWMA; fallback `PROVISION_ETA_SECONDS`).

Narrative copy also lives in local `ganymede_appendix.md` §G / §H.10 (that file is gitignored via `*_appendix.md`).
