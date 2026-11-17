package main

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ywacch/cloud9/pkg/deployer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestReap_DeletesPastGrace(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	past := now.Add(-10 * time.Minute).Unix()
	inside := now.Add(-2 * time.Minute).Unix()

	fakeClient := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:   "session-past",
			Labels: map[string]string{deployer.NamespaceExpiresAtLabel: strconv.FormatInt(past, 10)},
		}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:   "session-inside",
			Labels: map[string]string{deployer.NamespaceExpiresAtLabel: strconv.FormatInt(inside, 10)},
		}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "session-no-label"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
			Name:   "kube-system",
			Labels: map[string]string{deployer.NamespaceExpiresAtLabel: strconv.FormatInt(past, 10)},
		}},
	)

	deleted, err := Reap(context.Background(), fakeClient, Config{
		Now:   func() time.Time { return now },
		Grace: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Reap: %v", err)
	}
	if len(deleted) != 1 || deleted[0] != "session-past" {
		t.Fatalf("deleted = %v want [session-past]", deleted)
	}
	for _, name := range []string{"session-inside", "session-no-label", "kube-system"} {
		if _, getErr := fakeClient.CoreV1().Namespaces().Get(context.Background(), name, metav1.GetOptions{}); getErr != nil {
			t.Fatalf("namespace %s should remain: %v", name, getErr)
		}
	}
}
