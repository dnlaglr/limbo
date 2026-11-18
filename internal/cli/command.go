package cli

import (
	"runtime"

	"github.com/spf13/cobra"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/ui"
)

func newVersionCmd(a *app.App) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show version and build information",
		Args:  cobra.NoArgs,
		Run: func(*cobra.Command, []string) {
			b := a.Build
			a.UI.Panel("limbo",
				ui.Row{Key: "Version", Value: b.Version},
				ui.Row{Key: "Commit", Value: b.Commit[:min(len(b.Commit), 7)]},
				ui.Row{Key: "Built", Value: b.Date},
				ui.Row{Key: "Go", Value: runtime.Version()},
				ui.Row{Key: "Platform", Value: runtime.GOOS + "/" + runtime.GOARCH},
			)
		},
	}
}
