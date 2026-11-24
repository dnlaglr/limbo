package cli

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/broker"
)

const (
	filterHelp = `Filters pick messages by their ID, last death, headers, body and age. Different filters must all match, a filter
given more than once matches any of its values, and --not-* filters skip every message they match. Age is the time of
the last death, or the publish time when the broker records no death time, as on SQS. On RabbitMQ the ID is the
message_id its publisher set, which can be missing or shared by several messages.`
	readHelp = `Brokers may record each read as a delivery. RabbitMQ marks the messages as redelivered, and quorum queues before
RabbitMQ 4.3 count each read toward their delivery limit. SQS adds each read to a message's receive count and hides
the message from other consumers until limbo returns it, or for up to 5 minutes if limbo is killed. SQS FIFO queues
hand out at most the first 10 messages of each message group while limbo holds them.`
)

type filterOptions struct {
	filter    broker.Filter
	olderThan age
	newerThan age
}

func addFilterFlags(cmd *cobra.Command, o *filterOptions) {
	f, flags := &o.filter, cmd.Flags()
	flags.Var(listOf(&f.IDs.Any, parseID, "id"), "id", "only messages with this ID")
	flags.Var(listOf(&f.IDs.None, parseID, "id"), "not-id", "skip messages with this ID")
	flags.StringArrayVar(&f.Reasons.Any, "reason", nil, "only messages whose last death had this reason, such as rejected, expired, maxlen or delivery_limit")
	flags.StringArrayVar(&f.Reasons.None, "not-reason", nil, "skip messages whose last death had this reason")
	flags.StringArrayVar(&f.Sources.Any, "from", nil, "only messages that last died in this queue")
	flags.StringArrayVar(&f.Sources.None, "not-from", nil, "skip messages that last died in this queue")
	flags.Var(listOf(&f.Headers.Any, parseHeader, "key[=value]"), "header", "only messages with this header, or with this value for it")
	flags.Var(listOf(&f.Headers.None, parseHeader, "key[=value]"), "not-header", "skip messages with this header, or with this value for it")
	flags.Var(listOf(&f.Bodies.Any, regexp.Compile, "regex"), "body", "only messages whose body matches this regular expression")
	flags.Var(listOf(&f.Bodies.None, regexp.Compile, "regex"), "not-body", "skip messages whose body matches this regular expression")
	flags.Var(&o.olderThan, "older-than", "only messages that died longer ago than this, such as 30m, 2h or 7d")
	flags.Var(&o.newerThan, "newer-than", "only messages that died within this long, such as 30m, 2h or 7d")
}

// Ages become fixed cutoffs once so every message in a run is judged against the same moment
func (o *filterOptions) build(now time.Time) broker.Filter {
	f := o.filter
	if o.olderThan > 0 {
		f.Before = now.Add(-time.Duration(o.olderThan))
	}
	if o.newerThan > 0 {
		f.After = now.Add(-time.Duration(o.newerThan))
	}

	return f
}

// Checks that every queue in also exists before counting queue, so a bad destination fails before anything
func count(ctx context.Context, a *app.App, d broker.Driver, queue string, also ...string) (n int, err error) {
	err = a.UI.Step(ctx, "Counting "+queue, func(ctx context.Context) error {
		for _, q := range also {
			if q == "" {
				continue
			}
			if _, err := d.Depth(ctx, q); err != nil {
				return err
			}
		}

		n, err = d.Depth(ctx, queue)
		return err
	})

	return n, err
}

func browse(ctx context.Context, a *app.App, d broker.Driver, queue string, sel broker.Selection, keep func(broker.Message)) (scanned int, err error) {
	matched := 0
	err = a.UI.Progress(ctx, "Reading "+queue, func(ctx context.Context, set func(float64)) error {
		return d.Browse(ctx, queue, sel.Scan, func(m broker.Message) bool {
			scanned++
			set(float64(scanned) / float64(sel.Scan))
			if sel.Filter.Match(m) {
				matched++
				keep(m)
			}

			return !sel.Full(matched)
		})
	})
	if err == nil && scanned < sel.Scan && !sel.Full(matched) {
		a.UI.Warn(fmt.Sprintf("Read only %d of the %s counted in %s. Other consumers may have taken the rest, or the broker held "+
			"them back, as SQS FIFO queues do past 10 messages per group.", scanned, countMessages(sel.Scan), queue))
	}

	return scanned, err
}

type age time.Duration

func (a *age) Set(s string) error {
	d, err := time.ParseDuration(s)
	if days, ok := strings.CutSuffix(s, "d"); ok {
		var n int
		n, err = strconv.Atoi(days)
		d = time.Duration(n) * 24 * time.Hour
	}
	if err != nil || d <= 0 {
		return errors.New("use a positive duration such as 30m, 2h or 7d")
	}

	*a = age(d)
	return nil
}

func (a *age) String() string {
	if *a == 0 {
		return ""
	}

	return time.Duration(*a).String()
}

func (a *age) Type() string { return "duration" }

type list[T any] struct {
	items *[]T
	parse func(string) (T, error)
	kind  string
}

func listOf[T any](items *[]T, parse func(string) (T, error), kind string) list[T] {
	return list[T]{items: items, parse: parse, kind: kind}
}

func (l list[T]) Set(s string) error {
	v, err := l.parse(s)
	if err == nil {
		*l.items = append(*l.items, v)
	}

	return err
}

func (l list[T]) String() string { return "" }

func (l list[T]) Type() string { return l.kind }

// An empty ID would match every message without one, which on RabbitMQ can be the whole queue
func parseID(s string) (string, error) {
	if s == "" {
		return "", errors.New("an ID can't be empty")
	}

	return s, nil
}

func parseHeader(s string) (broker.Header, error) {
	key, value, hasValue := strings.Cut(s, "=")
	if key == "" {
		return broker.Header{}, errors.New("name the header, as key or key=value")
	}

	return broker.Header{Key: key, Value: value, HasValue: hasValue}, nil
}
