# Cloud9 Env and Secrets Reference

Canonical runtime contract for self-hosting the Cloud9 API.

Use this doc as the source of truth for required/optional environment variables and Kubernetes secret keys.

## Runtime Environment Variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SUPABASE_URL` | Yes | None | Supabase project URL used by API auth and deployment store. |
| `SUPABASE_SERVICE_KEY` | Yes | None | Service role key for deployment persistence and updates. |
| `SUPABASE_JWT_SECRET` | Production yes (current mode) | None | JWT verification secret for HS256-compatible auth path. |
| `ENV` | No | Empty/non-production | Runtime mode (`production` enables stricter checks). |
| `DEPLOYER` | No | `local` | `k8s` enables real cluster deployer; otherwise LocalDeployer. |
| `ALLOWED_ORIGINS` | No | `http://localhost:3000` | Comma-separated CORS allow-list. |
| `PROMETHEUS_URL` | No | Empty | Prometheus base URL for metrics collection. |
| `SEED_BUCKET_NAME` | No | `cloud9-seed-data` | Bucket used by seeder for prebuilt DB dumps. |
| `DDC_BUCKET` | No | Empty | Bucket for diagnostics collector artifact uploads. |
| `MAX_ACTIVE_DEPLOYMENTS_PER_USER` | No | `1` | Documented legacy name; per-user active limit is enforced via tier `MaxConcurrent` in code, not this env. |
| `MAX_CONCURRENT_PROVISIONS` | No | unset (unlimited) | Doorway: max in-flight orchestrations per API process (`replicas: 1` only). |
| `CLUSTER_DEPLOY_BUDGET_CPU` | No | unset (gate off) | Room budget CPU (k8s Quantity, e.g. `18500m`). Set **with** memory or leave both unset. |
| `CLUSTER_DEPLOY_BUDGET_MEMORY` | No | unset (gate off) | Room budget memory (e.g. `49920Mi`). Measured Jul 2026 wave-20 for this GCP account. |
| `API_RATE_LIMIT_ENABLED` | No | `true` when `ENV=production`, else `false` | Enable HTTP rate limiting on the API. |
| `API_RATE_LIMIT_BACKEND` | No | `memory` | `memory` (per-pod; use at `replicas: 1`) or `redis` (not implemented). |
| `API_RATE_LIMIT_RPS` | No | `30` | Per-user sustained rate for standard authenticated routes. |
| `API_RATE_LIMIT_BURST` | No | `60` | Burst for standard authenticated routes. |
| `API_RATE_LIMIT_EXPENSIVE_RPM` | No | `6` | Per-user rate for create deploy, loadgen start, chaos (requests/minute). |
| `API_RATE_LIMIT_EXPENSIVE_BURST` | No | `3` | Burst for expensive routes. |
| `API_RATE_LIMIT_PUBLIC_RPS` | No | `60` | Per-IP rate for `/health` and `/templates`. |
| `API_RATE_LIMIT_PUBLIC_BURST` | No | `120` | Burst for public routes. |
| `TEMPLATE_IMAGE_REGISTRY` | No | Empty | Registry prefix for deployable images. |
| `TEMPLATE_IMAGE_TAG` | No | `v1` fallback | Global fallback tag for deployable images. |
| `IMAGE_VERSIONS_FILE` | No | `config/image-versions.json` (best-effort load) | Per-image immutable tag manifest path. |
| `SENTRY_DSN` | No | Empty | Sentry DSN for backend error reporting. |
| `KUBECONFIG` | No | `~/.kube/config` | Kubernetes config path when running out-of-cluster. |

## Kubernetes Secret Contract

Expected secret name in namespace `cloud9-system`:

- `cloud9-api-secrets`

Required keys:

- `supabase-url`
- `supabase-service-key`
- `supabase-jwt-secret` (required by current production auth mode)
- `allowed-origins`

Optional keys:

- `sentry-dsn`

## Notes for Operators

- Never commit real secrets to Git.
- Rotate `supabase-service-key` and `supabase-jwt-secret` on a schedule.
- For local development, prefer environment variables from a local `.env` file (not committed).
- For Kubernetes, prefer `Secret` resources and avoid plain values in Deployment manifests.
- Keep `cloud9-api` at **`replicas: 1`** while doorway (`MAX_CONCURRENT_PROVISIONS`) and room (`CLUSTER_DEPLOY_BUDGET_*`) use in-process mutex/semaphore. Scale only after DB-atomic admission exists.
- SQL for weight columns: [`docs/sql/admitted_weights.sql`](sql/admitted_weights.sql).

