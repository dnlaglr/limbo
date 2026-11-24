package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/broker"
)

type importOptions struct {
	takeOptions
	to   string
	rate float64
	skip int
}

func newImportCmd(a *app.App) *cobra.Command {
	var opts importOptions

	cmd := &cobra.Command{
		Use:   "import <file>",
		Short: "Publish the messages in an export or backup file",
		Long: `Import publishes the messages in a file that export, delete or purge wrote, to the queue each one died in or to --to.
Messages with no dead-letter history fail unless you pass --to, and limbo then exits with code 1. Nothing is removed
from the file.

Limbo reads the whole file before it publishes anything, and stops with the line number if a line isn't a record it
can read. Copies follow replay's rules: they drop the broker's dead-letter headers, carry x-limbo-replay-count and
x-limbo-replayed-at, and on an SQS FIFO queue use limbo- followed by the message ID as their deduplication ID. They
keep the body, headers and the broker properties the file records, with their types. A file from the other broker
imports on a best-effort basis: RabbitMQ takes SQS attributes as string headers, and SQS turns RabbitMQ headers into
attributes up to its limit of 10.

Limbo asks before it imports. Pass --yes to skip the question, which is required without a terminal, or --dry-run to
see where each message would go without publishing anything. If an import stops early, limbo prints the line it
stopped after, so you can run the same command with --skip to continue without sending a message twice.

` + filterHelp,
		Example: `  limbo import orders.dlq-20261122T101500Z.ndjson --dry-run
  limbo import backup.ndjson --to orders.dlq --yes
  limbo import backup.ndjson --reason rejected --rate 50 --skip 500`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runImport(cmd.Context(), a, cmd.InOrStdin(), args[0], opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.url, "url", "", urlUsage)
	flags.StringVar(&opts.to, "to", "", "queue to publish into instead of the queue each message died in")
	flags.IntVarP(&opts.limit, "limit", "n", 0, "maximum number of matching messages to import (default all)")
	flags.Float64Var(&opts.rate, "rate", 0, "maximum messages per second (default unlimited)")
	flags.IntVar(&opts.skip, "skip", 0, "start after this line of the file, to continue an import that stopped")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "show where each message would go without publishing anything")
	flags.BoolVarP(&opts.yes, "yes", "y", false, "import without asking first")
	flags.StringVarP(&opts.output, "output", "o", "text", "output format: text or json")
	addFilterFlags(cmd, &opts.filter)

	return cmd
}

func runImport(ctx context.Context, a *app.App, in io.Reader, path string, opts importOptions) error {
	render, ok := takeRenderers[opts.output]
	switch {
	case !ok:
		return fmt.Errorf("unknown output format %q (use text or json)", opts.output)
	case opts.limit < 0:
		return errors.New("limit must be 0 (all) or more")
	case opts.rate < 0:
		return errors.New("rate must be 0 (unlimited) or more")
	case opts.skip < 0:
		return errors.New("skip must be 0 or more")
	case !opts.yes && !opts.dryRun && !a.UI.CanPrompt(in):
		return errors.New("refusing to import without confirmation: pass --yes, or --dry-run to preview")
	}

	u, err := brokerURL(opts.url)
	if err != nil {
		return err
	}

	var src *messageSource
	err = a.UI.Progress(ctx, "Checking "+path, func(ctx context.Context, set func(float64)) (err error) {
		src, err = readFile(ctx, path, opts.skip, set)
		return err
	})
	if err != nil {
		return err
	}
	if src.count == 0 {
		if opts.skip > 0 {
			a.UI.Info(fmt.Sprintf("%s holds no messages after line %d", path, opts.skip))
		} else {
			a.UI.Info(path + " holds no messages")
		}
		return nil
	}

	d, err := connect(ctx, a, u)
	if err != nil {
		return err
	}
	defer d.Close()

	p, ok := d.(broker.Publisher)
	if !ok {
		return fmt.Errorf("import is not supported for %s:// brokers", u.Scheme)
	}
	if opts.to != "" {
		if _, err := count(ctx, a, d, opts.to); err != nil {
			return err
		}
	}

	run := newTakeRun(path, src.count, opts.takeOptions, importing)
	run.routed, run.last = true, opts.skip
	route := broker.ReplayOptions{To: opts.to}
	if done, err := preview(a, run, opts.takeOptions, render, route.Target, src.read(ctx, run)); done {
		return err
	}

	if !opts.yes {
		question := fmt.Sprintf("Import %s from %s to %s?", run.subject, path, cmp.Or(opts.to, "the queues they died in"))
		if ok, err := confirm(ctx, a, in, question, importing); !ok {
			return err
		}
	}

	t := &taker{take: func(ctx context.Context, run *takeRun, each func(broker.Taken)) error {
		pace, stop := pacer(ctx, opts.rate)
		defer stop()

		return broker.Import(ctx, p, src.messages(ctx, run), broker.ReplayOptions{Selection: run.sel, To: opts.to}, func(res broker.Taken) {
			each(res)
			if !errors.Is(res.Err, broker.ErrSkipped) {
				pace()
			}
		})
	}}
	err = takeAll(ctx, a, t, run)
	return finishImport(a, run, err, render(a, run))
}

// Text and quiet output carry no records, so the error itself names the line to resume after and every line left behind
// A message whose send timed out may not have arrived, so the line to resume after is the one before it, never past it
func finishImport(a *app.App, run *takeRun, err, renderErr error) error {
	resume := run.last
	if errors.Is(err, context.DeadlineExceeded) && len(run.lines) > 0 {
		resume = run.lines[len(run.lines)-1] - 1
	}

	done, failed := 0, []int{}
	for i, r := range run.results {
		switch {
		case r.Err == nil:
			done++
		case run.lines[i] <= resume:
			failed = append(failed, run.lines[i])
		}
	}
	if done > 0 {
		a.UI.Success("Imported " + countMessages(done))
	}

	errs := []error{err}
	switch {
	case err != nil && resume == 0:
		errs = append(errs, fmt.Errorf("stopped before importing anything from %s, run the same command again to start over", run.queue))
	case err != nil:
		errs = append(errs, fmt.Errorf("stopped after line %d of %s, run the same command with --skip %d to continue", resume, run.queue, resume))
	}
	if len(failed) > 0 {
		errs = append(errs, fmt.Errorf("could not import %s from %s: %s", countMessages(len(failed)), run.queue, lineList(failed)))
	}

	return errors.Join(append(errs, renderErr)...)
}

// At most ten lines are listed, so a run where everything failed still ends with a short error
func lineList(lines []int) string {
	shown := make([]string, min(len(lines), 10))
	for i := range shown {
		shown[i] = strconv.Itoa(lines[i])
	}

	list := "line " + shown[0]
	if len(shown) > 1 {
		list = "lines " + strings.Join(shown, ", ")
	}
	if more := len(lines) - len(shown); more > 0 {
		list += fmt.Sprintf(" and %d more", more)
	}

	return list
}
