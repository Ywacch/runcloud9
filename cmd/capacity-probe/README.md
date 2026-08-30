# capacity-probe

Live concurrency campaign tool for measuring Cloud9 cluster capacity.

See the runbook in [`docs/concurrency-analysis.md`](../../docs/concurrency-analysis.md#active-capacity-probe).

```bash
# Validate fixtures and print wave weights (no API calls)
go run ./cmd/capacity-probe \
  -manifest ./cmd/capacity-probe/testdata/waves/engineer-launch.yaml \
  -dry-run

# Real campaign (costs GCP money; requires -confirm)
export CAPACITY_PROBE_API_URL="https://<api-host>"
export CAPACITY_PROBE_TOKEN="<internal-jwt>"
go run ./cmd/capacity-probe \
  -manifest ./cmd/capacity-probe/testdata/waves/engineer-launch.yaml \
  -confirm \
  -sample-cluster
```
