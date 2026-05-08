# Cloud9 Self-Host Deployment Guide

Provider-agnostic deployment guide for running Cloud9 on Kubernetes.

## 1) Prerequisites

- A Kubernetes cluster and `kubectl` access.
- A Prometheus instance reachable by the API (optional but recommended).
- A Supabase project with:
  - project URL
  - service role key
  - JWT secret (current production auth mode)

## 2) Bootstrap Base Resources

Apply base manifests:

```bash
kubectl apply -k k8s/base
```

The base includes:

- `Namespace cloud9-system`
- `ServiceAccount cloud9-api`
- `ClusterRole cloud9-api-deployer`
- `ClusterRoleBinding cloud9-api-deployer`
- `Deployment cloud9-api`
- `Service cloud9-api`
- portable ingress baseline

## 3) RBAC Requirements

Cloud9 needs cluster-level permissions because it creates and tears down per-session namespaces and deploys resources inside them.

Specifically, the API service account must be able to:

- create/delete namespaces
- create/update/delete core resources (pods, services, configmaps, secrets, PVCs)
- create/update/delete app resources (deployments/statefulsets)
- create/update/delete network resources (ingresses/networkpolicies)
- access `pods/exec` and `pods/log`

Quick checks:

```bash
kubectl auth can-i create namespaces --as=system:serviceaccount:cloud9-system:cloud9-api
kubectl auth can-i delete namespaces --as=system:serviceaccount:cloud9-system:cloud9-api
```

## 4) Configure Secrets

Create/update secret from the template:

```bash
kubectl apply -f k8s/base/cloud9-api-secrets.example.yaml
```

Then patch real values or recreate with literals.

See the canonical contract:

- `docs/env-and-secrets-reference.md`

## 5) Choose Provider Overlay

Base manifests are provider-agnostic. For GKE-specific behavior (managed cert, gce ingress annotations, workload identity annotation), apply the GKE overlay:

```bash
kubectl apply -k k8s/overlays/gke
```

If you are not on GKE, keep base only and supply your ingress/controller-specific configuration in your own overlay.

## 6) Verify API Health

```bash
kubectl rollout status deployment/cloud9-api -n cloud9-system --timeout=180s
kubectl get svc -n cloud9-system
kubectl logs -n cloud9-system deploy/cloud9-api --tail=200
```

## 7) Operator Notes

- Keep `k8s/base` portable and put cloud-specific values into overlays.
- Do not store secrets in plain manifests.
- Prefer immutable image tags for production rollouts.

