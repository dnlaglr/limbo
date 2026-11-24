package cli

import (
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/dnlaglr/limbo/internal/app"
)

func newRootCmd(a *app.App) *cobra.Command {
	var s app.Settings

	root := &cobra.Command{
		Use:   "limbo",
		Short: "Get failed messages out of limbo. Safely.",
		Long:  "Limbo inspects, triages and replays dead-letter queue messages, with the same workflow for every broker.",
		PersistentPreRun: func(cmd *cobra.Command, _ []string) {
			a.Apply(s)
			a.Log.Debug("starting", "command", cmd.CommandPath(), "version", a.Build.Version)
		},
	}

	flags := root.PersistentFlags()
	flags.BoolVar(&s.Debug, "debug", false, "print debug logs to stderr")
	flags.BoolVarP(&s.Quiet, "quiet", "q", false, "only print results and errors")
	flags.VarPF(new(noColor), "no-color", "", "disable colored output (same as NO_COLOR=1)").NoOptDefVal = "true"

	root.AddCommand(newVersionCmd(a), newPeekCmd(a), newReplayCmd(a), newStatsCmd(a), newExportCmd(a), newDeleteCmd(a), newPurgeCmd(a))

	return root
}

type noColor bool

func (n *noColor) Set(v string) error {
	on, err := strconv.ParseBool(v)
	if err != nil {
		return err
	}

	*n = noColor(on)
	if on {
		return os.Setenv("NO_COLOR", "1")
	}

	return nil
}

func (n *noColor) String() string { return strconv.FormatBool(bool(*n)) }

func (n *noColor) Type() string { return "bool" }
