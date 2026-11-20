package cli

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
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
	filter filterOptions
}

type replayRun struct {
	queue   string
	dryRun  bool
	sel     broker.Selection
	subject string
	results []broker.Replayed
	skipped int
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

var errUnconfirmed = errors.New("refusing to replay without confirmation: pass --yes, or --dry-run to preview")

func newReplayCmd(a *app.App) *cobra.Command {
	var opts replayOptions

	cmd := &cobra.Command{
		Use:   "replay <queue>",
		Short: "Move dead-lettered messages back to the queue they failed in",
		Long: `Replay takes messages off a dead-letter queue and publishes them again, to the queue each one died in or to --to.
Messages with no dead-letter history stay where they are unless you pass --to.

A message leaves the dead-letter queue only after the broker confirms its copy reached the destination, and anything
limbo cannot replay or skips goes back in its original order. Nothing is lost, but if the connection drops between the
confirm and the acknowledgement, the message ends up in both queues, so consumers should tolerate duplicates.

On RabbitMQ, replayed messages drop the broker's x-death, x-first-death-*, x-last-death-*, x-delivery-count and
x-acquired-count headers and carry x-limbo-replay-count and x-limbo-replayed-at instead.

Limbo asks before it replays. Pass --yes to skip the question, which is required without a terminal, or --dry-run to
see where each message would go without moving anything. With filters, limbo reads the queue once to count the
matches before it asks, and then replays no more than the messages it counted.

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
	render, err := opts.validate(queue)
	if err != nil {
		return err
	}
	if !opts.yes && !opts.dryRun && !a.UI.CanPrompt(in) {
		return errUnconfirmed
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

	depth, err := count(ctx, a, d, queue, opts.to)
	if err != nil {
		return err
	}
	if depth == 0 {
		a.UI.Info(queue + " is empty")
		return nil
	}

	run := newReplayRun(queue, depth, opts)
	if opts.dryRun || !run.sel.Filter.IsZero() && !opts.yes {
		if err := planReplay(ctx, a, d, run, opts.to); err != nil {
			return err
		}
		if opts.dryRun {
			return render(a, run)
		}
		if len(run.results) == 0 {
			a.UI.Info("No messages in " + queue + " matched")
			return nil
		}
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

// The run never reads past the depth counted before anything moves, so messages that fail again and return to the queue are not replayed twice
func newReplayRun(queue string, depth int, opts replayOptions) *replayRun {
	run := &replayRun{
		queue:   queue,
		dryRun:  opts.dryRun,
		sel:     broker.Selection{Filter: opts.filter.build(time.Now()), Limit: opts.limit, Scan: depth},
		subject: "matching messages",
	}
	if run.sel.Filter.IsZero() {
		if opts.limit > 0 {
			run.sel.Scan = min(depth, opts.limit)
		}
		run.subject = countMessages(run.sel.Scan)
	}

	return run
}

// The replay that follows a plan covers only the messages the plan read and moves no more than it matched
func planReplay(ctx context.Context, a *app.App, d broker.Driver, run *replayRun, to string) error {
	target := broker.ReplayOptions{To: to}
	scanned, err := browse(ctx, a, d, run.queue, run.sel, func(m broker.Message) {
		r := broker.Replayed{Message: m}
		r.To, r.Err = target.Target(m)
		run.results = append(run.results, r)
	})

	run.skipped = scanned - len(run.results)
	run.sel.Scan, run.sel.Limit = scanned, len(run.results)
	if !run.sel.Filter.IsZero() {
		run.subject = fmt.Sprintf("%d of %s", len(run.results), countMessages(scanned))
	}

	return err
}

func confirmReplay(ctx context.Context, a *app.App, in io.Reader, run *replayRun, to string) (bool, error) {
	if to == "" {
		to = "the queues they died in"
	}

	ok, err := a.UI.Confirm(ctx, in, fmt.Sprintf("Replay %s from %s to %s?", run.subject, run.queue, to))
	if err == nil && !ok {
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

	run.results, run.skipped = nil, 0
	title := fmt.Sprintf("Replaying %s from %s", run.subject, run.queue)
	return a.UI.Progress(ctx, title, func(ctx context.Context, set func(float64)) error {
		return r.Replay(ctx, run.queue, broker.ReplayOptions{Selection: run.sel, To: opts.to}, func(res broker.Replayed) {
			if errors.Is(res.Err, broker.ErrSkipped) {
				run.skipped++
			} else {
				run.results = append(run.results, res)
				pace()
			}
			set(float64(len(run.results)+run.skipped) / float64(run.sel.Scan))
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
	if run.skipped > 0 {
		rows = append(rows, ui.Row{Key: "Skipped", Value: countMessages(run.skipped)})
	}
	if left := run.sel.Scan - len(run.results) - run.skipped; left > 0 && !run.sel.Full(len(run.results)) {
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
	for _, v := range values {
		counts[v]++
	}

	return formatCounts(counts)
}

func formatCounts(counts map[string]int) string {
	keys := slices.SortedFunc(maps.Keys(counts), func(a, b string) int {
		return cmp.Or(cmp.Compare(counts[b], counts[a]), strings.Compare(a, b))
	})

	lines := make([]string, len(keys))
	for i, k := range keys {
		lines[i] = fmt.Sprintf("%s ×%d", k, counts[k])
	}

	return strings.Join(lines, "\n")
}
