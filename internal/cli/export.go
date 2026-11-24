package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/broker"
)

type exportOptions struct {
	url    string
	limit  int
	file   string
	filter filterOptions
}

func newExportCmd(a *app.App) *cobra.Command {
	var opts exportOptions

	cmd := &cobra.Command{
		Use:   "export <queue>",
		Short: "Save dead-lettered messages to a file without consuming them",
		Long: `Export writes the messages in a dead-letter queue to a file and leaves them all in the queue.

Each line of the file is one message, in the JSON that peek -o json prints, with "version":1 first and a properties
object that holds what limbo needs to publish an identical copy. The file is created readable by you only, since
message bodies may hold sensitive data, and limbo never overwrites an existing file. Pass --file - to write to stdout.

Limbo holds every message it reads until it is done, then returns them all to the queue, in their original order where
the broker keeps one. It reads until it has --limit messages that match, or until it reaches the end of the queue as
counted when it started.

` + readHelp + `

` + filterHelp,
		Example: `  limbo export orders.dlq
  limbo export orders.dlq --reason rejected --newer-than 1h --file rejected.ndjson
  limbo export orders.dlq --file - | gzip > orders.dlq.ndjson.gz`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExport(cmd.Context(), a, args[0], opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.url, "url", "", urlUsage)
	flags.IntVarP(&opts.limit, "limit", "n", 0, "maximum number of matching messages to export (default all)")
	flags.StringVarP(&opts.file, "file", "f", "", "file to write, or - for stdout (default <queue>-<time>.ndjson)")
	addFilterFlags(cmd, &opts.filter)

	return cmd
}

func runExport(ctx context.Context, a *app.App, queue string, opts exportOptions) error {
	if opts.limit < 0 {
		return errors.New("limit must be 0 (all) or more")
	}

	u, err := brokerURL(opts.url)
	if err != nil {
		return err
	}

	d, err := connect(ctx, a, u)
	if err != nil {
		return err
	}
	defer d.Close()

	depth, err := count(ctx, a, d, queue)
	if err != nil {
		return err
	}
	if depth == 0 {
		a.UI.Info(queue + " is empty")
		return nil
	}

	f, err := createFile(cmp.Or(opts.file, defaultFile(queue, time.Now())), a.UI.Out(), false)
	if err != nil {
		return err
	}

	sel := broker.Selection{Filter: opts.filter.build(time.Now()), Limit: opts.limit, Scan: depth}
	scanned, err := browse(ctx, a, d, queue, sel, f.write)
	err = errors.Join(err, f.close())

	subject := countMessages(f.n)
	if !sel.Filter.IsZero() {
		subject = fmt.Sprintf("%d of %s", f.n, countMessages(scanned))
	}
	switch {
	case f.n == 0 && err == nil:
		a.UI.Info("No messages in " + queue + " matched")
	case err != nil && f.n > 0:
		a.UI.Warn(fmt.Sprintf("Export stopped early, so %s holds only the first %s", f.name(), countMessages(f.n)))
	case err == nil:
		a.UI.Success("Exported " + subject + " to " + f.name())
	}
	if err == nil {
		a.UI.Success("Put " + countMessages(scanned) + " back on " + queue)
	}

	return err
}
