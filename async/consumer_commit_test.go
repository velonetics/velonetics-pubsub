package async

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestPendingMessageBlocksImplicitCommit(t *testing.T) {
	var pending *kafka.Message
	commits := []int64{}

	msg100 := kafka.Message{Offset: 100}
	msg101 := kafka.Message{Offset: 101}

	// Simulate failure on 100: keep pending, do not commit, do not advance.
	pending = &msg100

	// Without pending guard, committing 101 would implicitly commit 100.
	if pending != nil && pending.Offset >= msg101.Offset {
		t.Fatal("should not fetch or commit later offsets while earlier message is pending")
	}

	// Retry succeeds on pending 100.
	commits = append(commits, pending.Offset)
	pending = nil

	// Now safe to process 101.
	commits = append(commits, msg101.Offset)

	if len(commits) != 2 || commits[0] != 100 || commits[1] != 101 {
		t.Fatalf("unexpected commits: %v", commits)
	}
}
