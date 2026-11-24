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

type replayOptions struct {
	takeOptions
	to   string
	rate float64
}

func newReplayCmd(a *app.App) *cobra.Command {
	var opts replayOptions

	cmd := &cobra.Command{
		Use:   "replay <queue>",
		Short: "Move dead-lettered messages back to the queue they failed in",
		Long: `Replay takes messages off a dead-letter queue and publishes them again, to the queue each one died in or to --to.
Messages with no dead-letter history stay where they are unless you pass --to.

A message leaves the dead-letter queue only after the broker confirms its copy reached the destination, and anything
limbo cannot replay or skips goes back, in its original order where the broker keeps one. Nothing is lost, but if the
connection drops between the confirm and the acknowledgement, the message ends up in both queues, so consumers should
tolerate duplicates.

On RabbitMQ, replayed messages drop the broker's x-death, x-first-death-*, x-last-death-*, x-delivery-count and
x-acquired-count headers and carry x-limbo-replay-count and x-limbo-replayed-at instead.

On SQS, replayed copies keep the body, message attributes and message group, and carry x-limbo-replay-count and
x-limbo-replayed-at unless that would pass the limit of 10 attributes. Copies sent to a FIFO queue use limbo- followed
by the dead-letter copy's message ID as their deduplication ID.

Limbo asks before it replays. Pass --yes to skip the question, which is required without a terminal, or --dry-run to
see where each message would go without moving anything. With filters, limbo reads the queue once to count the
matches before it asks, and then replays no more than the messages it counted. SQS standard queues hand out messages
in no fixed order, so a replay with --limit may pick other matching messages than the dry run listed.

` + filterHelp,
		Example: `  limbo replay orders.dlq --dry-run
  limbo replay orders.dlq --reason rejected --from orders --newer-than 1h
  limbo replay orders.dlq --to orders.v2 -n 100 --rate 50 --yes`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReplay(cmd.Context(), a, cmd.InOrStdin(), args[0], opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.url, "url", "", urlUsage)
	flags.StringVar(&opts.to, "to", "", "queue to replay into instead of the queue each message died in")
	flags.IntVarP(&opts.limit, "limit", "n", 0, "maximum number of matching messages to replay (default all)")
	flags.Float64Var(&opts.rate, "rate", 0, "maximum messages per second (default unlimited)")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "show where each message would go without moving anything")
	flags.BoolVarP(&opts.yes, "yes", "y", false, "replay without asking first")
	flags.StringVarP(&opts.output, "output", "o", "text", "output format: text or json")
	addFilterFlags(cmd, &opts.filter)

	return cmd
}

func runReplay(ctx context.Context, a *app.App, in io.Reader, queue string, opts replayOptions) error {
	switch {
	case opts.rate < 0:
		return errors.New("rate must be 0 (unlimited) or more")
	case opts.to == queue:
		return errors.New("cannot replay a queue into itself, pick another --to")
	}

	route := broker.ReplayOptions{To: opts.to}
	return runTake(ctx, a, in, queue, opts.takeOptions, replaying, func(d broker.Driver) *taker {
		r, ok := d.(broker.Replayer)
		if !ok {
			return nil
		}

		return &taker{
			also:   opts.to,
			target: route.Target,
			note:   ", but one that was in flight may also have reached its destination",
			ask: func(run *takeRun) string {
				return fmt.Sprintf("Replay %s from %s to %s?", run.subject, queue, cmp.Or(opts.to, "the queues they died in"))
			},
			take: func(ctx context.Context, run *takeRun, each func(broker.Taken)) error {
				pace, stop := pacer(ctx, opts.rate)
				defer stop()

				return broker.Replay(ctx, r, queue, broker.ReplayOptions{Selection: run.sel, To: opts.to}, func(res broker.Taken) {
					each(res)
					if !errors.Is(res.Err, broker.ErrSkipped) {
						pace()
					}
				})
			},
		}
	})
}

func pacer(ctx context.Context, rate float64) (pace, stop func()) {
	if rate <= 0 {
		return func() {}, func() {}
	}

	tick := time.NewTicker(max(time.Duration(float64(time.Second)/rate), time.Nanosecond))
	return func() {
		select {
		case <-tick.C:
		case <-ctx.Done():
		}
	}, tick.Stop
}
