package vu

// DefaultConversations builds the canvas default VU block.
// actionNames are the keys of the template's loadgen.vu.actions.
// ok is false when create_root or create_reply is missing.
// thread_observer is included only when read_thread is present.
// Params are left empty so the behavior factories and
// ApplyObserverStaggerMaxAge keep their defaults.
func DefaultConversations(actionNames map[string]struct{}) (Config, bool) {
	if _, ok := actionNames[actionCreateRoot]; !ok {
		return Config{}, false
	}
	if _, ok := actionNames[actionCreateReply]; !ok {
		return Config{}, false
	}
	behaviors := []BehaviorConfig{
		{Name: behaviorConversationStarter, Count: 2},
		{Name: behaviorReplier, Count: 2},
	}
	if _, ok := actionNames[actionReadThread]; ok {
		behaviors = append(behaviors, BehaviorConfig{Name: behaviorThreadObserver, Count: 1})
	}
	return Config{Behaviors: behaviors}, true
}
