package rules

// LookupMechanism returns a library mechanism by id.
func LookupMechanism(id string) (Mechanism, bool) {
	if id == "" {
		return Mechanism{}, false
	}
	for _, m := range allMechanisms {
		if m.ID == id {
			return m, true
		}
	}
	return Mechanism{}, false
}
