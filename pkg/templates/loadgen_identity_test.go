package templates

import "testing"

func TestValidateLoadgenIdentity_badParam(t *testing.T) {
	t.Parallel()
	err := validateLoadgenIdentity(&LoadgenSpec{
		Identity:  &LoadgenIdentity{Param: "missing"},
		Endpoints: []LoadgenEndpointSpec{{ParamName: "userId"}},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadgenIdentity_socialFeedPull(t *testing.T) {
	t.Parallel()
	id, ok := GetLoadgenIdentity("social-feed-pull")
	if !ok || id.Param != "userId" {
		t.Fatalf("identity: %+v", id)
	}
}
