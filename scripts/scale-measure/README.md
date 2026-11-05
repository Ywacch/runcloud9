# Session scale measure

PowerShell helper for capacity baseline work: Prometheus **series** and Kubernetes **objects** per live session namespace. It does not change the API.

## Prerequisites

- `kubectl` pointed at the cluster under test
- `curl.exe` on PATH (not the PowerShell `curl` alias)
- Prometheus reachable (usually port-forward)

```powershell
kubectl port-forward -n monitoring svc/kube-prometheus-stack-prometheus 9090:9090
```

## Run

From the `ganymede` module root:

```powershell
./scripts/scale-measure/measure.ps1 -Template url-shortener-small -NamespacePrefix cloud9-url-shortener -PrometheusURL http://127.0.0.1:9090
```

Optional kube context:

```powershell
./scripts/scale-measure/measure.ps1 -Template social-feed -NamespacePrefix cloud9-social-feed -PrometheusURL http://127.0.0.1:9090 -KubeContext my-gke-context
```

`-Template` is copied into the table as-is. The script does not infer template from the namespace name.

`-NamespacePrefix` must be at least `cloud9-` plus one more character. Do not pass `cloud9` alone. Namespaces `cloud9-system` and `monitoring` are always skipped.

## PromQL

Series count per namespace (one query for all namespaces):

```promql
count by (namespace) ({deployment!=""})
```

## Results

Copy the **mean** row `series` and `objects` into your rung 0 per-template baseline table:

- **url-shortener-small** — run while a batch of url-shortener sessions is up
- **social-feed** — one live session is enough; note in your run log if it was a single session

Keep raw command output in your own run notes (for example next to `capacity-probe-results/`).

## Example output (fake numbers)

```text
| namespace | template | series | objects |
| cloud9-url-shortener-abc12 | url-shortener-small | 842 | 118 |
| cloud9-url-shortener-def34 | url-shortener-small | 839 | 119 |
| mean | url-shortener-small | 840.5 | 118.5 |
```
