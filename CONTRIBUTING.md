# Contributing to Cloud9

Thank you for your interest in contributing to Cloud9! This document covers our development workflow, branching strategy, commit conventions, and code quality standards.

---

## Branch Strategy

We use a **trunk-based development** model with short-lived feature branches.

```
main (protected)
  ├── feat/template-aware-metrics
  ├── fix/social-feed-cache-invalidation
  ├── refactor/collector-cleanup
  └── docs/contributing-guide
```

### Branch naming convention

```
<type>/<short-description>
```

| Prefix | When to use | Example |
|---|---|---|
| `feat/` | New feature or capability | `feat/event-pipeline` |
| `fix/` | Bug fix | `fix/loadgen-userid-fallback` |
| `refactor/` | Code restructuring, no behaviour change | `refactor/collector-template-aware` |
| `perf/` | Performance improvement | `perf/cache-batch-invalidation` |
| `docs/` | Documentation only | `docs/contributing-guide` |
| `test/` | Adding or improving tests | `test/ring-buffer-overflow` |
| `chore/` | Build, CI, dependency updates | `chore/update-go-1.23` |

### Rules

- **`main` is always deployable.** Never push directly to `main`.
- **Feature branches are short-lived.** Aim for <3 days. If a feature takes longer, break it into sequential PRs.
- **Delete branches after merge.** Enable auto-delete in GitHub settings.
- **No long-lived `develop` branch.** Trunk-based means `main` is the trunk.

---

## Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/) with a **scope** indicating the affected package or area.

### Format

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Examples

```
feat(metrics): add template-aware metric collection

Add collectTemplateMetrics() switch that queries template-specific
PromQL based on the deployment's template type. Thread template name
from handler through streamer to collector.

Closes #42
```

```
fix(loadgen): return numeric userId fallback for valid JSON

The fallback parameter value for userId was generating alphanumeric
strings, producing invalid JSON bodies for POST requests. Changed to
return strconv.Itoa(1 + rand.Intn(50)).
```

```
perf(social-feed): batch cache invalidation using variadic Delete

Replace sequential per-follower Redis DELETE calls with a single
variadic call. Reduces POST /post latency from ~6s to ~50ms for
celebrity users with ~5000 followers.
```

### Type reference

| Type | SemVer effect | Description |
|---|---|---|
| `feat` | minor | New feature |
| `fix` | patch | Bug fix |
| `perf` | patch | Performance improvement |
| `refactor` | — | No behaviour change |
| `test` | — | Adding/fixing tests |
| `docs` | — | Documentation |
| `chore` | — | Build, CI, tooling |
| `revert` | — | Reverting a previous commit |

### Scope reference

Use the Go package name or area:

| Scope | Area |
|---|---|
| `metrics` | `pkg/metrics/` |
| `loadgen` | `pkg/loadgen/` |
| `deployer` | `pkg/deployer/` |
| `api` | `pkg/api/` |
| `store` | `pkg/store/` |
| `templates` | `pkg/templates/` |
| `url-shortener` | `images/url-shortener/` |
| `social-feed` | `images/social-feed/` |
| `ecommerce` | `images/ecommerce/` |
| `fanout-worker` | `images/fanout-worker/` |
| `caching-proxy` | `images/caching-proxy/` |
| `loadgen` | `images/loadgen/` |
| `infra` | Terraform, Helm, k8s manifests |
| `ci` | GitHub Actions |

### Commit hygiene

- **One logical change per commit.** Don't mix a bug fix with a refactor.
- **Write the body for non-obvious changes.** If the diff doesn't explain *why*, the message should.
- **Reference issues.** Use `Closes #N`, `Fixes #N`, or `Relates to #N` in the footer.
- **Don't commit generated files.** Binaries, `.dump` files, `vendor/` — all in `.gitignore`.
- **Don't commit secrets.** No API keys, service account JSON, `.env` files. Ever.

---

## Pull Requests

### Before opening a PR

1. **Rebase on `main`** — `git fetch origin && git rebase origin/main`
2. **Run tests** — `go test ./...`
3. **Run build** — `go build ./...`
4. **Run linter** — `golangci-lint run` (if configured)
5. **Self-review the diff** — read your own diff as if reviewing someone else's code

### PR title

Same format as commit messages:

```
feat(metrics): add event pipeline and cascade detection
```

### PR description template

```markdown
## What

Brief description of what this PR does.

## Why

Context on why this change is needed. Link to issue if applicable.

## How

Technical approach taken. Highlight non-obvious decisions.

## Testing

- [ ] `go test ./...` passes
- [ ] `go build ./...` passes
- [ ] Manual verification (describe what you did)

## Screenshots / Logs

If applicable — WebSocket payload samples, metric dashboard screenshots, log output.
```

### PR size guidelines

| Size | Lines changed | Review expectation |
|---|---|---|
| **Small** | <100 | Quick review, merge same day |
| **Medium** | 100-400 | Focused review, 1 day turnaround |
| **Large** | 400-1000 | Split if possible; needs dedicated review time |
| **Too large** | >1000 | Must be split into sequential PRs |

### Review process

- All PRs require **1 approval** before merge.
- Use **squash merge** for feature branches to keep `main` history clean.
- The squashed commit message should follow conventional commit format.
- Delete the branch after merge.

---

## Code Quality Standards

### Go conventions

- Follow [Effective Go](https://go.dev/doc/effective_go) and the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments).
- Run `gofmt` (or `goimports`) before committing — non-negotiable.
- Exported functions and types must have doc comments.
- Error messages should not be capitalised or end with punctuation (Go convention).
- Use `context.Context` as the first parameter for functions that do I/O.

### Architecture conventions

- **Leave explanatory comments intact.** If a comment explains *why* a design decision was made (not just *what* the code does), preserve it during refactors. If you change the behaviour, update the comment.
- **No hardcoded URLs or credentials.** Use environment variables. See `cmd/api/main.go` for the config pattern.
- **New collector metrics** — every new PromQL query must include the `namespace` label. Never query cluster-wide metrics.
- **New component types** — when adding a new node type to the platform, follow this checklist in order:
  1. **Semantics** — add all `(new, existing)` and `(existing, new)` edge rules to `pkg/deployer/semantics.go`
  2. **Deploy** — add the deploy case to `K8sDeployer` (or the component registry) and teardown
  3. **Env** — add `DeriveEnvVars` target case and follow the existing naming convention
  4. **Metrics** — add container-scoped PromQL queries to `pkg/metrics/collector.go`
  5. **Optional** — frontend type mapping, template registry, seeder, custom image under `images/`

### Testing

- **Unit tests** live alongside the code: `foo.go` → `foo_test.go`.
- **Table-driven tests** are preferred for functions with multiple cases.
- **No test dependencies on external services.** Mock HTTP responses, use test fixtures.
- **Test names** follow `TestFunctionName_Scenario`:
  ```go
  func TestRingBuffer_OverflowDropsOldest(t *testing.T) { ... }
  func TestEventBus_DrainIsAtomic(t *testing.T) { ... }
  ```

---

## Development Setup

### Prerequisites

- Go 1.22+
- Docker (for building template images)
- `kubectl` + a kubeconfig pointing to a GKE cluster (or minikube for local dev)
- Helm 3 (for Prometheus stack deployment)

### Running locally

```bash
# Start the API server on port 8081
go run ./cmd/api/main.go 8081

# Run all tests
go test ./...

# Build all template images
cd images/url-shortener && go build -o url-shortener .
cd images/social-feed && go build -o social-feed .
cd images/ecommerce && go build -o ecommerce .
```

### Environment variables

Canonical configuration docs:

- `docs/env-and-secrets-reference.md` (required/optional env vars + secret contract)
- `.env.example` (local template)
- `docs/self-host-deployment.md` (Kubernetes/operator flow + RBAC requirements)

---

## Releasing

We use **semantic versioning** (semver). Tags trigger CI builds:

```
v0.1.0  → first public release
v0.1.1  → patch (bug fix)
v0.2.0  → minor (new feature, backwards compatible)
v1.0.0  → major (breaking changes)
```

Pre-launch, we're at `v0.x.x`. Don't worry about breaking changes until v1.

### Immutable image rollout process

Use immutable release tags for all deployable images (`url-shortener`, `caching-proxy`, `social-feed`, `ecommerce`, workers, `loadgen`). Do not rely on re-pushing mutable tags such as `:v1` for production rollouts.

Production source of truth is `config/image-versions.json` loaded via `IMAGE_VERSIONS_FILE`. Tag resolution priority is:
1. Manifest entry for image name
2. `TEMPLATE_IMAGE_TAG`
3. `v1` fallback

Pushes to `main` run `.github/workflows/deploy.yaml`. The job rebuilds a template image when `images/<name>/` changed, and rebuilds every in-repo template image when `pkg/`, `go.mod`, `go.sum`, or `.dockerignore` changed (each Dockerfile copies `pkg/`). It pushes `git-<shortsha>` to Artifact Registry, writes those tags into `config/image-versions.json`, builds `cloud9-api` with that file inside the image, deploys it, then opens a pull request with the manifest bump (branch protection blocks a direct push to `main`). `ticket-drop` is built from `runcloud9-templates` and is not updated here.

Docker Desktop remains a fallback. Build and push the same `git-<shortsha>` tag, update `config/image-versions.json`, and merge so the next API image picks it up. Pull `main` first if CI has already committed a bump.

After a deploy, check digest parity with:

- `scripts/verify-rollout.sh`
- `scripts/verify-rollout.ps1`

---

## Code of Conduct

We follow the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). Be respectful, be constructive, be welcoming.
