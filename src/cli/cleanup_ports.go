package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/schollz/croc/v11/internal/cli"
	"github.com/schollz/croc/v11/src/portmap"
)

var cleanupPortMappings = portmap.Cleanup

func cleanupPorts(c *cli.Context) error {
	if c.Args().Len() != 0 {
		return fmt.Errorf("usage: croc cleanup-ports")
	}
	ctx, cancel := context.WithTimeout(c.Context, 15*time.Second)
	defer cancel()
	result, err := cleanupPortMappings(ctx)
	if len(result.Removed) > 0 {
		fmt.Fprintf(c.App.Writer, "Removed unlimited croc TCP port forwards: %v\n", result.Removed)
	}
	if len(result.Skipped) > 0 {
		fmt.Fprintf(c.App.Writer, "Left TCP ports %v unchanged because they are in use or their forwarding rules changed.\n", result.Skipped)
	}
	if err != nil {
		return fmt.Errorf("clean up router port forwards: %w", err)
	}
	if len(result.Removed) == 0 && len(result.Skipped) == 0 {
		fmt.Fprintln(c.App.Writer, "No unlimited croc port forwards found for this computer.")
	}
	return nil
}
