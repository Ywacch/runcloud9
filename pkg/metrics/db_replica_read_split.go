package metrics

const dbReadLeaderReplicaLabel = "leader"

// ReplicaReadShares returns each host's fraction of total read rate. ok is false when
// there are no replica hosts or total rate is zero.
func ReplicaReadShares(rates map[string]float64) (map[string]float64, bool) {
	if len(rates) == 0 {
		return nil, false
	}
	replicaHosts := 0
	for host := range rates {
		if host != dbReadLeaderReplicaLabel {
			replicaHosts++
		}
	}
	if replicaHosts == 0 {
		return nil, false
	}
	var total float64
	for _, rate := range rates {
		total += rate
	}
	if total <= 0 {
		return nil, false
	}
	shares := make(map[string]float64, len(rates))
	for host, rate := range rates {
		shares[host] = rate / total
	}
	return shares, true
}

// ReplicaReadImbalance returns max read share minus 1/n across labeled hosts.
// A single replica host yields 0; callers should omit facts when shares are unavailable.
func ReplicaReadImbalance(shares map[string]float64) float64 {
	if len(shares) == 0 {
		return 0
	}
	replicaHosts := 0
	for host := range shares {
		if host != dbReadLeaderReplicaLabel {
			replicaHosts++
		}
	}
	if replicaHosts <= 1 {
		return 0
	}
	n := float64(len(shares))
	if n <= 1 {
		return 0
	}
	var maxShare float64
	for _, share := range shares {
		if share > maxShare {
			maxShare = share
		}
	}
	return maxShare - 1/n
}

func appendReplicaReadSplitFacts(l *Ledger, final *DeploymentMetrics) {
	if l == nil || final == nil {
		return
	}
	db, ok := final.Components["db"]
	if !ok || db.Consistency == nil {
		return
	}
	rates := db.Consistency.ReadRateByReplica
	shares, okShares := ReplicaReadShares(rates)
	if !okShares {
		return
	}
	for host, share := range shares {
		l.addFact(Fact{
			Ref:       FactRef("fact:metric:db.readShare." + host),
			Kind:      FactKindMetric,
			Component: "db",
			Value: map[string]any{
				"window":    PromRateWindowLabel,
				"host":      host,
				"readShare": share,
			},
		})
	}
	l.addFact(Fact{
		Ref:       FactRef("fact:derived:db.replicaReadImbalance"),
		Kind:      FactKindDerived,
		Component: "db",
		Value: map[string]any{
			"window":    PromRateWindowLabel,
			"imbalance": ReplicaReadImbalance(shares),
		},
	})
}

func cloneFloatMap(in map[string]float64) map[string]float64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]float64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
