package vu

import "testing"

func TestAction_RenderAndExtractID(t *testing.T) {
	a := Action{
		Method:  "POST",
		Path:    "/post",
		Body:    `{"user_id":{{.UserID}},"reply_to_id":{{.ParentID}}}`,
		IDField: "post_id",
	}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	path, body := a.render(Vars{UserID: "42", ParentID: "99"})
	if path != "/post" {
		t.Fatalf("path: %q", path)
	}
	if body != `{"user_id":42,"reply_to_id":99}` {
		t.Fatalf("body: %q", body)
	}
	id, err := extractID([]byte(`{"post_id":123}`), "post_id")
	if err != nil || id != "123" {
		t.Fatalf("id: %v err %v", id, err)
	}
	id, err = extractID([]byte(`{"post_id":"abc"}`), "post_id")
	if err != nil || id != "abc" {
		t.Fatalf("string id: %v err %v", id, err)
	}
	if _, err := extractID([]byte(`{"post_id":"bad id"}`), "post_id"); err == nil {
		t.Fatal("expected unsafe id error")
	}
}
