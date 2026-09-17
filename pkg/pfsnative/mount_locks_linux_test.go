//go:build linux && amd64

package pfsnative

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestMountMetadataReleasePreservesHostLock(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "locks")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	first := &Client{}
	if err := first.acquireMountLocksAt(ctx, lockPath, 1); err != nil {
		t.Fatal(err)
	}
	defer first.closeMountLocks()
	// Complete the handshake and release metadata, not the host reservation.
	if err := first.metaLock.Close(); err != nil {
		t.Fatal(err)
	}
	first.metaLock = nil
	second := &Client{}
	if err := second.acquireMountLocksAt(ctx, lockPath, 2); err != nil {
		t.Fatal(err)
	}
	second.closeMountLocks()
	duplicate := &Client{}
	if err := duplicate.acquireMountLocksAt(ctx, lockPath, 1); err == nil {
		duplicate.closeMountLocks()
		t.Fatal("closing metadata released the live host lock")
	}
	first.closeMountLocks()
	if err := duplicate.acquireMountLocksAt(ctx, lockPath, 1); err != nil {
		t.Fatal(err)
	}
	duplicate.closeMountLocks()
}

func TestExclusiveMountLock(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "locks")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	c := &Client{}
	if err := c.acquireMountLocksAt(ctx, lockPath, 0); err != nil {
		t.Fatal(err)
	}
	defer c.closeMountLocks()
	other := &Client{}
	if err := other.acquireMountLocksAt(ctx, lockPath, 1); err == nil {
		other.closeMountLocks()
		t.Fatal("exclusive mount did not exclude another host")
	}
}
