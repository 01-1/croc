package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/schollz/croc/v11/src/portmap"
)

func TestCleanupPortsCommand(t *testing.T) {
	previous := cleanupPortMappings
	t.Cleanup(func() { cleanupPortMappings = previous })
	failure := errors.New("router unavailable")
	for _, tt := range []struct {
		name   string
		result portmap.CleanupResult
		err    error
		want   string
	}{
		{"removed", portmap.CleanupResult{Removed: []uint16{9009, 9010}}, nil, "Removed unlimited croc TCP port forwards: [9009 9010]"},
		{"none", portmap.CleanupResult{}, nil, "No unlimited croc port forwards found for this computer."},
		{"active", portmap.CleanupResult{Skipped: []uint16{9009}}, nil, "Left TCP ports [9009] unchanged"},
		{"partial failure", portmap.CleanupResult{Removed: []uint16{9009}}, failure, "Removed unlimited croc TCP port forwards: [9009]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cleanupPortMappings = func(ctx context.Context) (portmap.CleanupResult, error) {
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("cleanup is not bounded by a deadline")
				}
				return tt.result, tt.err
			}
			var output bytes.Buffer
			app := newApp()
			app.Writer = &output
			err := app.Run([]string{"croc", "cleanup-ports"})
			if !errors.Is(err, tt.err) || !strings.Contains(output.String(), tt.want) {
				t.Fatalf("output=%q error=%v", output.String(), err)
			}
		})
	}
}
