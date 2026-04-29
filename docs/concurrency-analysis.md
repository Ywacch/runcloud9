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
