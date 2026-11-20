package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/broker"
	"github.com/dnlaglr/limbo/internal/ui"
)

type replayOptions struct {
	url    string
	to     string
	limit  int
	rate   float64
	dryRun bool
	yes    bool
	output string
}

type replayRun struct {
	queue   string
	total   int
	dryRun  bool
	results []broker.Replayed
}

type replayRecord struct {
	ID     string `json:"id,omitempty"`
	To     string `json:"to,omitempty"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

var replayRenderers = map[string]func(*app.App, *replayRun) error{
	"text": renderReplayText,
	"json": renderReplayJSON,
}

func newReplayCmd(a *app.App) *cobra.Command {
	var opts replayOptions

	cmd := &cobra.Command{
		Use:   "replay <queue>",
		Short: "Move dead-lettered messages back to the queue they failed in",
		Long: `Replay takes messages off a dead-letter queue and publishes them again, to the queue each one died in or to --to.
Messages with no dead-letter history stay where they are unless you pass --to.

A message leaves the dead-letter queue only after the broker confirms its copy reached the destination, and anything
limbo cannot replay goes back in its original order. Nothing is lost, but if the connection drops between the confirm
and the acknowledgement, the message ends up in both queues, so consumers should tolerate duplicates.

On RabbitMQ, replayed messages drop the broker's x-death, x-first-death-*, x-last-death-*, x-delivery-count and
x-acquired-count headers and carry x-limbo-replay-count and x-limbo-replayed-at instead.

Limbo asks before it replays. Pass --yes to skip the question, which is required without a terminal, or --dry-run to
see where each message would go without moving anything.`,
		Example: `  limbo replay orders.dlq --dry-run
  limbo replay orders.dlq --to orders.v2 -n 100 --rate 50 --yes`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runReplay(cmd.Context(), a, cmd.InOrStdin(), args[0], opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.url, "url", "", urlUsage)
	flags.StringVar(&opts.to, "to", "", "queue to replay into instead of the queue each message died in")
	flags.IntVarP(&opts.limit, "limit", "n", 0, "maximum number of messages to replay (default all)")
	flags.Float64Var(&opts.rate, "rate", 0, "maximum messages per second (default unlimited)")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "show where each message would go without moving anything")
	flags.BoolVarP(&opts.yes, "yes", "y", false, "replay without asking first")
	flags.StringVarP(&opts.output, "output", "o", "text", "output format: text or json")

	return cmd
}

func runReplay(ctx context.Context, a *app.App, in io.Reader, queue string, opts replayOptions) error {
	render, err := opts.validate(queue)
	if err != nil {
		return err
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

	r, ok := d.(broker.Replayer)
	if !ok {
		return fmt.Errorf("replay is not supported for %s:// brokers", u.Scheme)
	}

	run := &replayRun{queue: queue, dryRun: opts.dryRun}
	if run.total, err = countReplay(ctx, a, d, queue, opts); err != nil {
		return err
	}
	if run.total == 0 {
		a.UI.Info(queue + " is empty")
		return nil
	}

	if opts.dryRun {
		if err := planReplay(ctx, a, d, run, opts.to); err != nil {
			return err
		}
		return render(a, run)
	}

	if !opts.yes {
		if ok, err := confirmReplay(ctx, a, in, run, opts.to); !ok {
			return err
		}
	}

	err = replay(ctx, a, r, run, opts)
	return finishReplay(a, run, err, render(a, run))
}

func (o replayOptions) validate(queue string) (func(*app.App, *replayRun) error, error) {
	render, ok := replayRenderers[o.output]
	switch {
	case !ok:
		return nil, fmt.Errorf("unknown output format %q (use text or json)", o.output)
	case o.limit < 0:
		return nil, errors.New("limit must be 0 (all) or more")
	case o.rate < 0:
		return nil, errors.New("rate must be 0 (unlimited) or more")
	case o.to == queue:
		return nil, errors.New("cannot replay a queue into itself, pick another --to")
	}

	return render, nil
}

// The count is fixed before anything moves, so messages that fail again and return to the queue are not replayed twice
func countReplay(ctx context.Context, a *app.App, d broker.Driver, queue string, opts replayOptions) (n int, err error) {
	err = a.UI.Step(ctx, "Counting "+queue, func(ctx context.Context) (err error) {
		if opts.to != "" {
			if _, err := d.Depth(ctx, opts.to); err != nil {
				return err
			}
		}

		n, err = d.Depth(ctx, queue)
		return err
	})
	if opts.limit > 0 {
		n = min(n, opts.limit)
	}

	return n, err
}

func planReplay(ctx context.Context, a *app.App, d broker.Driver, run *replayRun, to string) error {
	var msgs []broker.Message
	err := a.UI.Step(ctx, "Reading "+run.queue, func(ctx context.Context) (err error) {
		msgs, err = d.Peek(ctx, run.queue, run.total)
		return err
	})

	opts := broker.ReplayOptions{To: to}
	for _, m := range msgs {
		r := broker.Replayed{Message: m}
		r.To, r.Err = opts.Target(m)
		run.results = append(run.results, r)
	}
	run.total = len(run.results)

	return err
}

func confirmReplay(ctx context.Context, a *app.App, in io.Reader, run *replayRun, to string) (bool, error) {
	if to == "" {
		to = "the queues they died in"
	}

	ok, err := a.UI.Confirm(ctx, in, fmt.Sprintf("Replay %s from %s to %s?", countMessages(run.total), run.queue, to))
	switch {
	case errors.Is(err, ui.ErrNoTerminal):
		return false, errors.New("refusing to replay without confirmation: pass --yes, or --dry-run to preview")
	case err == nil && !ok:
		a.UI.Info("Nothing was replayed")
	}

	return ok, err
}

func replay(ctx context.Context, a *app.App, r broker.Replayer, run *replayRun, opts replayOptions) error {
	pace := func() {}
	if opts.rate > 0 {
		tick := time.NewTicker(max(time.Duration(float64(time.Second)/opts.rate), time.Nanosecond))
		defer tick.Stop()
		pace = func() {
			select {
			case <-tick.C:
			case <-ctx.Done():
			}
		}
	}

	title := fmt.Sprintf("Replaying %s from %s", countMessages(run.total), run.queue)
	return a.UI.Progress(ctx, title, func(ctx context.Context, set func(float64)) error {
		return r.Replay(ctx, run.queue, broker.ReplayOptions{To: opts.to, Limit: run.total}, func(res broker.Replayed) {
			run.results = append(run.results, res)
			set(float64(len(run.results)) / float64(run.total))
			pace()
		})
	})
}

func finishReplay(a *app.App, run *replayRun, err, renderErr error) error {
	returned := 0
	for _, r := range run.results {
		if r.Err != nil {
			returned++
		}
	}
	if replayed := len(run.results) - returned; replayed > 0 {
		a.UI.Success("Replayed " + countMessages(replayed))
	}

	switch {
	case err != nil && !errors.Is(err, context.Canceled):
		a.UI.Warn("Replay stopped early. Messages it had not replayed are back on " + run.queue +
			", but one that was in flight may also have reached its destination.")
	case err == nil && returned > 0:
		err = fmt.Errorf("left %s on %s that could not be replayed", countMessages(returned), run.queue)
	}

	return errors.Join(err, renderErr)
}

func renderReplayText(a *app.App, run *replayRun) error {
	title, replayedKey, returnedKey := run.queue, "Replayed", "Returned"
	if run.dryRun {
		title, replayedKey, returnedKey = run.queue+"  dry run", "Would replay", "Would return"
	}

	var replayed, returned []string
	for _, r := range run.results {
		if r.Err == nil {
			replayed = append(replayed, r.To)
		} else {
			returned = append(returned, r.Err.Error())
		}
	}

	rows := []ui.Row{
		{Key: replayedKey, Value: tally(replayed)},
		{Key: returnedKey, Value: tally(returned)},
	}
	if left := run.total - len(run.results); left > 0 {
		rows = append(rows, ui.Row{Key: "Not reached", Value: countMessages(left)})
	}
	for i := range rows {
		rows[i].Value = sanitize(rows[i].Value)
	}
	a.UI.Panel(title, rows...)

	return nil
}

func renderReplayJSON(a *app.App, run *replayRun) error {
	enc := json.NewEncoder(a.UI.Out())
	for _, r := range run.results {
		rec := replayRecord{ID: r.Message.ID, To: r.To, Status: replayStatus(r.Err == nil, run.dryRun)}
		if r.Err != nil {
			rec.Error = r.Err.Error()
		}
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}

	return nil
}

func replayStatus(replayed, dryRun bool) string {
	switch {
	case replayed && dryRun:
		return "would_replay"
	case replayed:
		return "replayed"
	case dryRun:
		return "would_return"
	}

	return "returned"
}

func tally(values []string) string {
	counts := map[string]int{}
	var order []string
	for _, v := range values {
		if counts[v] == 0 {
			order = append(order, v)
		}
		counts[v]++
	}

	lines := make([]string, len(order))
	for i, v := range order {
		lines[i] = fmt.Sprintf("%s ×%d", v, counts[v])
	}

	return strings.Join(lines, "\n")
}
