package cli

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/spf13/cobra"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/broker"
	"github.com/dnlaglr/limbo/internal/ui"
)

const maxBodyPreview = 1024

type peekOptions struct {
	url    string
	limit  int
	output string
}

var peekRenderers = map[string]func(*app.App, []broker.Message) error{
	"text": renderText,
	"json": renderJSON,
}

func newPeekCmd(a *app.App) *cobra.Command {
	var opts peekOptions

	cmd := &cobra.Command{
		Use:   "peek <queue>",
		Short: "Show dead-lettered messages without consuming them",
		Long: `Peek shows the messages waiting in a dead-letter queue and leaves them there.

Limbo holds up to --limit messages while it reads them, then returns them to the queue in their original order.
Brokers may record that as a delivery: RabbitMQ marks the messages as redelivered, and quorum queues before
RabbitMQ 4.3 count each peek toward their delivery limit.`,
		Example: `  limbo peek orders.dlq --url amqp://guest:guest@localhost:5672/
  LIMBO_URL=amqps://user:pass@broker:5671/prod limbo peek orders.dlq -n 50 -o json | jq .body`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPeek(cmd.Context(), a, args[0], opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVar(&opts.url, "url", "", "broker URL such as amqp://user:pass@host:5672/vhost (default $LIMBO_URL)")
	flags.IntVarP(&opts.limit, "limit", "n", 10, "maximum number of messages to show")
	flags.StringVarP(&opts.output, "output", "o", "text", "output format: text or json")

	return cmd
}

func runPeek(ctx context.Context, a *app.App, queue string, opts peekOptions) error {
	render, ok := peekRenderers[opts.output]
	if !ok {
		return fmt.Errorf("unknown output format %q (use text or json)", opts.output)
	}
	if opts.limit < 1 {
		return errors.New("limit must be at least 1")
	}

	u, err := brokerURL(opts.url)
	if err != nil {
		return err
	}

	var d broker.Driver
	err = a.UI.Step(ctx, "Connecting to "+u.Redacted(), func(ctx context.Context) (err error) {
		d, err = a.Brokers.Open(ctx, u)
		return err
	})
	if err != nil {
		return err
	}
	defer d.Close()

	var msgs []broker.Message
	err = a.UI.Step(ctx, "Reading "+queue, func(ctx context.Context) (err error) {
		msgs, err = d.Peek(ctx, queue, opts.limit)
		return err
	})
	if err != nil {
		return err
	}

	if len(msgs) == 0 {
		a.UI.Info(queue + " is empty")
		return nil
	}

	if err := render(a, msgs); err != nil {
		return err
	}
	warnDeliveries(a, queue, msgs)
	a.UI.Success("Put " + countMessages(len(msgs)) + " back on " + queue)

	return nil
}

func brokerURL(flag string) (*url.URL, error) {
	raw := cmp.Or(flag, os.Getenv("LIMBO_URL"))
	if raw == "" {
		return nil, errors.New("no broker URL: pass --url or set LIMBO_URL")
	}

	return broker.ParseURL(raw)
}

func warnDeliveries(a *app.App, queue string, msgs []broker.Message) {
	most := 0
	for _, m := range msgs {
		most = max(most, m.DeliveryCount)
	}

	if most > 0 {
		a.UI.Warn(fmt.Sprintf("Messages in %s were already delivered up to %d times. "+
			"Queues with a delivery limit drop or dead-letter messages past it, and some brokers count each peek.", queue, most))
	}
}

func renderJSON(a *app.App, msgs []broker.Message) error {
	enc := json.NewEncoder(a.UI.Out())
	for _, m := range msgs {
		if err := enc.Encode(m); err != nil {
			return err
		}
	}

	return nil
}

func renderText(a *app.App, msgs []broker.Message) error {
	for i := range msgs {
		a.UI.Panel(fmt.Sprintf("%s  %d/%d", msgs[i].Queue, i+1, len(msgs)), messageRows(&msgs[i])...)
	}

	return nil
}

func messageRows(m *broker.Message) []ui.Row {
	deliveries := ""
	if m.DeliveryCount > 0 {
		deliveries = strconv.Itoa(m.DeliveryCount)
	}

	rows := []ui.Row{
		{Key: "ID", Value: m.ID},
		{Key: "Published", Value: formatTime(m.Timestamp)},
		{Key: "Content type", Value: m.ContentType},
		{Key: "Deliveries", Value: deliveries},
		{Key: "Dead-lettered", Value: formatDeaths(m.Deaths)},
	}
	if len(m.Deaths) > 0 {
		rows = append(rows,
			ui.Row{Key: "Routing key", Value: strings.Join(m.Deaths[0].RoutingKeys, ", ")},
			ui.Row{Key: "Last death", Value: formatTime(m.Deaths[0].Time)},
		)
	}
	rows = append(rows,
		ui.Row{Key: "Headers", Value: formatHeaders(m.Headers)},
		ui.Row{Key: "Body", Value: formatBody(m.Body)},
	)

	for i := range rows {
		rows[i].Value = sanitize(rows[i].Value)
	}

	return rows
}

func formatDeaths(deaths []broker.Death) string {
	lines := make([]string, len(deaths))
	for i, d := range deaths {
		lines[i] = fmt.Sprintf("%s · %s ×%d", d.Queue, d.Reason, d.Count)
	}

	return strings.Join(lines, "\n")
}

func formatHeaders(headers map[string]any) string {
	lines := make([]string, 0, len(headers))
	for _, k := range slices.Sorted(maps.Keys(headers)) {
		lines = append(lines, fmt.Sprintf("%s=%v", k, headers[k]))
	}

	return strings.Join(lines, "\n")
}

func formatBody(body []byte) string {
	switch {
	case len(body) == 0:
		return "(empty)"
	case !utf8.Valid(body):
		return fmt.Sprintf("(%d bytes of binary data, use --output json to see it)", len(body))
	}

	s := string(body)
	var indented bytes.Buffer
	if json.Indent(&indented, body, "", "  ") == nil {
		s = indented.String()
	}

	if len(s) > maxBodyPreview {
		s = strings.ToValidUTF8(s[:maxBodyPreview], "") + fmt.Sprintf("\n… %d bytes in total, use --output json to see all of it", len(body))
	}

	return s
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.Local().Format(time.DateTime) + " (" + ago(time.Since(t)) + ")"
}

func ago(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return strconv.Itoa(int(d.Minutes())) + "m ago"
	case d < 24*time.Hour:
		return strconv.Itoa(int(d.Hours())) + "h ago"
	}

	return strconv.Itoa(int(d.Hours()/24)) + "d ago"
}

func countMessages(n int) string {
	if n == 1 {
		return "1 message"
	}

	return strconv.Itoa(n) + " messages"
}

// Message content is untrusted so control characters are dropped before reaching the terminal
func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || !unicode.IsControl(r) {
			return r
		}

		return -1
	}, s)
}
