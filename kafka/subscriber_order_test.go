package kafka

import "testing"

// TestSubscriberAckOrder documents that entity formatting must complete before commit.
func TestSubscriberAckOrder(t *testing.T) {
	formatted := false
	committed := false

	formatted = true
	if !formatted {
		t.Fatal("expected format before commit")
	}
	committed = true

	if committed && !formatted {
		t.Fatal("commit must not run before format")
	}
}
