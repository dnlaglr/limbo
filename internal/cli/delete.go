package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/broker"
)

const backupHelp = `Limbo saves a backup first, to <queue>-<time>.ndjson in the current folder or to --backup. Each message is written
and flushed to disk before it is removed, so the backup holds every message limbo removed, even after Ctrl-C. The file
uses export's format, is readable by you only, and never replaces an existing file. Flushing every message makes a
backup run slower, especially on macOS. Pass --no-backup to remove messages without one.`

type deleteOptions struct {
	takeOptions
	backup   string
	noBackup bool
}

func newDeleteCmd(a *app.App) *cobra.Command {
	var opts deleteOptions

	cmd := &cobra.Command{
		Use:   "delete <queue>",
		Short: "Permanently remove chosen messages from a dead-letter queue",
		Long: `Delete removes the messages that match the filters, --id or --limit from a dead-letter queue, for good. It refuses to
run without one of them; limbo purge removes every message. Messages it doesn't delete go back, in their original
order where the broker keeps one.

` + backupHelp + `

Limbo asks before it deletes. Pass --yes to skip the question, which is required without a terminal, or --dry-run to
see which messages would go without deleting anything. With filters, limbo reads the queue once to count the matches
before it asks, and then deletes no more than the messages it counted.

` + filterHelp,
		Example: `  limbo delete orders.dlq --id 4f1c2a7e
  limbo delete orders.dlq --reason expired --older-than 7d --dry-run
  limbo delete orders.dlq --body '"poison":true' --backup poison.ndjson --yes`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runDelete(cmd.Context(), a, cmd.InOrStdin(), args[0], opts)
		},
	}

	flags := cmd.Flags()
	flags.IntVarP(&opts.limit, "limit", "n", 0, "maximum number of matching messages to delete (default all)")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "show which messages would be deleted without deleting any")
	addRemoveFlags(cmd, &opts, deleting)
	addFilterFlags(cmd, &opts.filter)

	return cmd
}

func newPurgeCmd(a *app.App) *cobra.Command {
	var opts deleteOptions

	cmd := &cobra.Command{
		Use:   "purge <queue>",
		Short: "Permanently remove every message from a dead-letter queue",
		Long: `Purge removes every message in a dead-letter queue, as counted when it starts, for good.

` + backupHelp + `

With --no-backup, limbo uses the broker's own purge, which is instant but also removes messages that arrive while it
runs. RabbitMQ purges only the messages no consumer holds. SQS can take up to 60 seconds to finish, allows one purge
per queue a minute, and needs the sqs:PurgeQueue permission. Redis trims the stream to nothing and keeps the key and
its consumer groups.

Limbo asks before it purges. Pass --yes to skip the question, which is required without a terminal, or --dry-run to
list the messages that would go without removing anything.`,
		Example: `  limbo purge orders.dlq
  limbo purge orders.dlq --backup orders-dlq-before-deploy.ndjson --yes
  limbo purge orders.dlq --no-backup --yes`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPurge(cmd.Context(), a, cmd.InOrStdin(), args[0], opts)
		},
	}

	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "list the messages that would be purged without removing any")
	addRemoveFlags(cmd, &opts, purging)

	return cmd
}

func addRemoveFlags(cmd *cobra.Command, opts *deleteOptions, v verb) {
	flags := cmd.Flags()
	flags.StringVar(&opts.url, "url", "", urlUsage)
	flags.BoolVarP(&opts.yes, "yes", "y", false, v.base+" without asking first")
	flags.StringVarP(&opts.output, "output", "o", "text", "output format: text or json")
	flags.StringVar(&opts.backup, "backup", "", "file to save the messages to before removing them (default <queue>-<time>.ndjson)")
	flags.BoolVar(&opts.noBackup, "no-backup", false, "remove the messages without saving a backup")
	cmd.MarkFlagsMutuallyExclusive("backup", "no-backup")
}

func runDelete(ctx context.Context, a *app.App, in io.Reader, queue string, opts deleteOptions) (err error) {
	if opts.filter.build(time.Now()).IsZero() && opts.limit == 0 {
		return errors.New("pick messages with a filter, --id or --limit, or use limbo purge to remove every message")
	}

	f, err := backupFile(queue, opts)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.close()) }()

	return runTake(ctx, a, in, queue, opts.takeOptions, deleting, func(d broker.Driver) *taker {
		t, ok := d.(broker.Taker)
		if !ok {
			return nil
		}

		return remover(t, queue, f, "")
	})
}

func runPurge(ctx context.Context, a *app.App, in io.Reader, queue string, opts deleteOptions) (err error) {
	f, err := backupFile(queue, opts)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.close()) }()

	return runTake(ctx, a, in, queue, opts.takeOptions, purging, func(d broker.Driver) *taker {
		t, ok := d.(broker.Taker)
		if !ok {
			return nil
		}

		r := remover(t, queue, f, "all ")
		if p, ok := d.(broker.Purger); ok && opts.noBackup {
			r.purge = func(ctx context.Context) error { return p.Purge(ctx, queue) }
			r.ask = func(run *takeRun) string {
				return fmt.Sprintf("Purge %s without a backup? All %s in it, including any that arrive meanwhile, will be gone. This cannot be undone.", queue, run.subject)
			}
		}

		return r
	})
}

// The backup is created before limbo asks, so a path it can't write fails before anything else happens
func backupFile(queue string, opts deleteOptions) (*messageFile, error) {
	switch {
	case opts.backup == "-":
		return nil, errors.New("a backup must go to a file, not stdout: pick a path for --backup")
	case opts.noBackup || opts.dryRun:
		return nil, nil
	}

	return createFile(cmp.Or(opts.backup, defaultFile(queue, time.Now())), nil, true)
}

func remover(d broker.Taker, queue string, f *messageFile, all string) *taker {
	return &taker{
		ask: func(run *takeRun) string {
			if f == nil {
				return fmt.Sprintf("Permanently delete %s%s from %s without a backup? This cannot be undone.", all, run.subject, queue)
			}
			return fmt.Sprintf("Permanently delete %s%s from %s? Backup: %s", all, run.subject, queue, f.path)
		},
		take: func(ctx context.Context, run *takeRun, each func(broker.Taken)) error {
			opts := broker.RemoveOptions{Selection: run.sel}
			if f == nil {
				return broker.Remove(ctx, d, queue, opts, each)
			}

			opts.Backup = f.write
			err := broker.Remove(ctx, d, queue, opts, each)
			if f.n > 0 {
				run.backup = f.path
			}
			return err
		},
	}
}
