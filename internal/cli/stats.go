package cli

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/broker"
	"github.com/dnlaglr/limbo/internal/ui"
)

type statsOptions struct {
	url    string
	output string
	filter filterOptions
}

type queueStats struct {
	Queue   string         `json:"queue"`
	Depth   int            `json:"depth"`
	Scanned int            `json:"scanned"`
	Matched int            `json:"matched"`
	Partial bool           `json:"partial,omitempty"`
	Oldest  time.Time      `json:"oldest,omitzero"`
	Newest  time.Time      `json:"newest,omitzero"`
	Reasons map[string]int `json:"reasons"`

	filtered   bool
	deliveries int
}

var statsRenderers = map[string]func(*app.App, *queueStats) error{
	"text": renderStatsText,
	"json": renderStatsJSON,
}

func newStatsCmd(a *app.App) *cobra.Command {
	var opts statsOptions

	cmd := &cobra.Command{
		Use:   "stats <queue>",
		Short: "Summarize a dead-letter queue without consuming it",
		Long: `Stats reads every message in a dead-letter queue, as counted when it starts, and puts them all back in their
original order. It reports how many messages there are, when the oldest and newest died, and why they died.

Reading a message may count as a delivery: RabbitMQ marks it as redelivered, and quorum queues before RabbitMQ 4.3
count each read toward their delivery limit. Press Ctrl-C to stop early and see the counts so far.

` + filterHelp,
		Example: `  limbo stats orders.dlq
  limbo stats orders.dlq --from orders --newer-than 1h -o json | jq .reasons`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runStats(cmd.Context(), a, args[0], opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.url, "url", "", urlUsage)
	flags.StringVarP(&opts.output, "output", "o", "text", "output format: text or json")
	addFilterFlags(cmd, &opts.filter)

	return cmd
}

func runStats(ctx context.Context, a *app.App, queue string, opts statsOptions) error {
	render, ok := statsRenderers[opts.output]
	if !ok {
		return fmt.Errorf("unknown output format %q (use text or json)", opts.output)
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

	sel := broker.Selection{Filter: opts.filter.build(time.Now()), Scan: depth}
	s := &queueStats{Queue: queue, Depth: depth, Reasons: map[string]int{}, filtered: !sel.Filter.IsZero()}
	s.Scanned, err = browse(ctx, a, d, queue, sel, s.add)
	s.Partial = err != nil

	if renderErr := render(a, s); renderErr != nil {
		return renderErr
	}
	warnDeliveries(a, queue, s.deliveries)
	if s.Partial {
		a.UI.Warn(fmt.Sprintf("Stopped after reading %d of %s, so these counts are partial", s.Scanned, countMessages(depth)))
	} else if s.Scanned > 0 {
		a.UI.Success("Put " + countMessages(s.Scanned) + " back on " + queue)
	}

	return err
}

func (s *queueStats) add(m broker.Message) {
	s.Matched++
	s.Reasons[cmp.Or(m.Reason(), "none")]++
	s.deliveries = max(s.deliveries, m.DeliveryCount)

	if at := m.DiedAt(); !at.IsZero() {
		if s.Oldest.IsZero() || at.Before(s.Oldest) {
			s.Oldest = at
		}
		if at.After(s.Newest) {
			s.Newest = at
		}
	}
}

func renderStatsText(a *app.App, s *queueStats) error {
	rows := []ui.Row{{Key: "Messages", Value: strconv.Itoa(s.Depth)}}
	if s.filtered {
		rows = append(rows, ui.Row{Key: "Matched", Value: strconv.Itoa(s.Matched)})
	}
	if s.Partial {
		rows = append(rows, ui.Row{Key: "Read", Value: fmt.Sprintf("%d of %d", s.Scanned, s.Depth)})
	}
	rows = append(rows,
		ui.Row{Key: "Oldest", Value: formatTime(s.Oldest)},
		ui.Row{Key: "Newest", Value: formatTime(s.Newest)},
		ui.Row{Key: "Reasons", Value: formatCounts(s.Reasons)},
	)

	for i := range rows {
		rows[i].Value = sanitize(rows[i].Value)
	}
	a.UI.Panel(s.Queue+"  stats", rows...)

	return nil
}

func renderStatsJSON(a *app.App, s *queueStats) error {
	return json.NewEncoder(a.UI.Out()).Encode(s)
}
