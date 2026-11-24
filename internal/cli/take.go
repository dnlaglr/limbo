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

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/broker"
	"github.com/dnlaglr/limbo/internal/ui"
)

type takeOptions struct {
	url    string
	limit  int
	dryRun bool
	yes    bool
	output string
	filter filterOptions
}

type verb struct{ base, past, doing string }

var (
	replaying = verb{"replay", "replayed", "Replaying"}
	deleting  = verb{"delete", "deleted", "Deleting"}
	purging   = verb{"purge", "purged", "Purging"}
)

// A taker is what a command does with the messages it takes, picked once limbo knows what the broker supports
type taker struct {
	also   string
	target func(broker.Message) (string, error)
	ask    func(run *takeRun) string
	note   string
	take   func(ctx context.Context, run *takeRun, each func(broker.Taken)) error
	purge  func(ctx context.Context) error
}

type takeRun struct {
	queue   string
	dryRun  bool
	sel     broker.Selection
	subject string
	results []broker.Taken
	skipped int
	verb    verb
	routed  bool
	backup  string
}

type takeRecord struct {
	ID     string `json:"id,omitempty"`
	To     string `json:"to,omitempty"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type purgeRecord struct {
	Queue  string `json:"queue"`
	Status string `json:"status"`
	Depth  int    `json:"depth"`
}

var takeRenderers = map[string]func(*app.App, *takeRun) error{
	"text": renderTakeText,
	"json": renderTakeJSON,
}

// A nil taker from pick means the broker can't do what the command asks
func runTake(ctx context.Context, a *app.App, in io.Reader, queue string, opts takeOptions, v verb, pick func(broker.Driver) *taker) error {
	render, ok := takeRenderers[opts.output]
	switch {
	case !ok:
		return fmt.Errorf("unknown output format %q (use text or json)", opts.output)
	case opts.limit < 0:
		return errors.New("limit must be 0 (all) or more")
	case !opts.yes && !opts.dryRun && !a.UI.CanPrompt(in):
		return fmt.Errorf("refusing to %s without confirmation: pass --yes, or --dry-run to preview", v.base)
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

	t := pick(d)
	if t == nil {
		return fmt.Errorf("%s is not supported for %s:// brokers", v.base, u.Scheme)
	}

	depth, err := count(ctx, a, d, queue, t.also)
	if err != nil {
		return err
	}
	if depth == 0 {
		a.UI.Info(queue + " is empty")
		return nil
	}

	run := newTakeRun(queue, depth, opts, v)
	run.routed = t.target != nil
	if t.purge != nil && !opts.dryRun {
		return purgeAll(ctx, a, in, t, run, opts)
	}
	if opts.dryRun || !run.sel.Filter.IsZero() && !opts.yes {
		if err := plan(ctx, a, d, run, t.target); err != nil {
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
		if ok, err := confirm(ctx, a, in, t.ask(run), v); !ok {
			return err
		}
	}

	err = takeAll(ctx, a, t, run)
	return finish(a, run, t.note, err, render(a, run))
}

// The run never reads past the depth counted before anything moves, so messages that fail again and return to the queue are not taken twice
func newTakeRun(queue string, depth int, opts takeOptions, v verb) *takeRun {
	run := &takeRun{
		queue:   queue,
		dryRun:  opts.dryRun,
		sel:     broker.Selection{Filter: opts.filter.build(time.Now()), Limit: opts.limit, Scan: depth},
		subject: "matching messages",
		verb:    v,
	}
	if run.sel.Filter.IsZero() {
		if opts.limit > 0 {
			run.sel.Scan = min(depth, opts.limit)
		}
		run.subject = countMessages(run.sel.Scan)
	}

	return run
}

// The run that follows a plan covers only the messages the plan read and takes no more than it matched
func plan(ctx context.Context, a *app.App, d broker.Driver, run *takeRun, target func(broker.Message) (string, error)) error {
	scanned, err := browse(ctx, a, d, run.queue, run.sel, func(m broker.Message) error {
		r := broker.Taken{Message: m}
		if target != nil {
			r.To, r.Err = target(m)
		}
		run.results = append(run.results, r)
		return nil
	})

	run.skipped = scanned - len(run.results)
	run.sel.Scan, run.sel.Limit = scanned, len(run.results)
	if !run.sel.Filter.IsZero() {
		run.subject = fmt.Sprintf("%d of %s", len(run.results), countMessages(scanned))
	}

	return err
}

func confirm(ctx context.Context, a *app.App, in io.Reader, question string, v verb) (bool, error) {
	ok, err := a.UI.Confirm(ctx, in, question)
	if err == nil && !ok {
		a.UI.Info("Nothing was " + v.past)
	}

	return ok, err
}

func takeAll(ctx context.Context, a *app.App, t *taker, run *takeRun) error {
	run.results, run.skipped = nil, 0
	title := fmt.Sprintf("%s %s from %s", run.verb.doing, run.subject, run.queue)
	return a.UI.Progress(ctx, title, func(ctx context.Context, set func(float64)) error {
		return t.take(ctx, run, func(res broker.Taken) {
			if errors.Is(res.Err, broker.ErrSkipped) {
				run.skipped++
			} else {
				run.results = append(run.results, res)
			}
			set(float64(len(run.results)+run.skipped) / float64(run.sel.Scan))
		})
	})
}

func purgeAll(ctx context.Context, a *app.App, in io.Reader, t *taker, run *takeRun, opts takeOptions) error {
	if !opts.yes {
		if ok, err := confirm(ctx, a, in, t.ask(run), run.verb); !ok {
			return err
		}
	}

	if err := a.UI.Step(ctx, "Purging "+run.queue, t.purge); err != nil {
		return err
	}

	var err error
	if opts.output == "json" {
		err = json.NewEncoder(a.UI.Out()).Encode(purgeRecord{Queue: run.queue, Status: "purged", Depth: run.sel.Scan})
	} else {
		a.UI.Panel(run.queue, ui.Row{Key: "Purged", Value: countMessages(run.sel.Scan)})
	}
	a.UI.Success(fmt.Sprintf("Purged %s, which held %s", run.queue, countMessages(run.sel.Scan)))

	return err
}

func finish(a *app.App, run *takeRun, note string, err, renderErr error) error {
	returned := 0
	for _, r := range run.results {
		if r.Err != nil {
			returned++
		}
	}
	if done := len(run.results) - returned; done > 0 {
		a.UI.Success(capitalize(run.verb.past) + " " + countMessages(done))
	}
	if run.backup != "" {
		a.UI.Info("Backup: " + run.backup)
	}

	switch {
	case err != nil && !errors.Is(err, context.Canceled):
		a.UI.Warn(fmt.Sprintf("%s stopped early. Messages it had not %s are back on %s%s.", capitalize(run.verb.base), run.verb.past, run.queue, note))
	case err == nil && returned > 0:
		err = fmt.Errorf("left %s on %s that could not be %s", countMessages(returned), run.queue, run.verb.past)
	}

	return errors.Join(err, renderErr)
}

func renderTakeText(a *app.App, run *takeRun) error {
	title, doneKey, returnedKey := run.queue, capitalize(run.verb.past), "Returned"
	if run.dryRun {
		title, doneKey, returnedKey = run.queue+"  dry run", "Would "+run.verb.base, "Would return"
	}

	var done, returned []string
	for _, r := range run.results {
		if r.Err == nil {
			done = append(done, r.To)
		} else {
			returned = append(returned, r.Err.Error())
		}
	}

	doneValue := tally(done)
	if !run.routed && len(done) > 0 {
		doneValue = countMessages(len(done))
	}
	rows := []ui.Row{
		{Key: doneKey, Value: doneValue},
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

func renderTakeJSON(a *app.App, run *takeRun) error {
	enc := json.NewEncoder(a.UI.Out())
	for _, r := range run.results {
		rec := takeRecord{ID: r.Message.ID, To: r.To, Status: status(run.verb, r.Err == nil, run.dryRun)}
		if r.Err != nil {
			rec.Error = r.Err.Error()
		}
		if err := enc.Encode(rec); err != nil {
			return err
		}
	}

	return nil
}

func status(v verb, done, dryRun bool) string {
	switch {
	case done && dryRun:
		return "would_" + v.base
	case done:
		return v.past
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

func capitalize(s string) string {
	return strings.ToUpper(s[:1]) + s[1:]
}
