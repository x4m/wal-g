//go:build linux && amd64

package pfsnative

import (
	"context"
	"testing"
)

func TestMkdirAllAcceptsPBDRoot(t *testing.T) {
	client := &Client{pbdRoot: "/vdb"}
	if err := client.MkdirAll(context.Background(), "/vdb/", 0o700); err != nil {
		t.Fatal(err)
	}
}
