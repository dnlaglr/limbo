package redis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dnlaglr/limbo/internal/broker"
)

const (
	pageSize      = 100
	settleTimeout = 30 * time.Second
	defaultBody   = "payload"

	bodyField        = "body_field"
	fieldTypes       = "field_types"
	replayCountField = "x-limbo-replay-count"
	replayedAtField  = "x-limbo-replayed-at"
)

type driver struct {
	client *goredis.Client
	fields fields
}

// fields names the stream fields that hold a message's body and its death, which the app decides since Redis has no dead-lettering
type fields struct {
	body, origin, reason string
}

// Retries are off because go-redis would resend an XADD whose reply was lost, and a copy must either be confirmed or reported as unsure
func Open(ctx context.Context, u *url.URL) (broker.Driver, error) {
	opts, f, err := parseURL(u)
	if err != nil {
		return nil, err
	}
	opts.MaxRetries = -1

	client := goredis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, err
	}

	return &driver{client: client, fields: f}, nil
}

func (d *driver) Close() error { return d.client.Close() }

func (d *driver) Browse(ctx context.Context, queue string, n int, visit func(broker.Message) bool) error {
	return d.Take(ctx, queue, n, func(m broker.Message, _ func() error) (bool, error) {
		return visit(m), nil
	})
}

// XLEN counts a missing key as an empty stream, so the type is checked first
func (d *driver) Depth(ctx context.Context, queue string) (int, error) {
	if err := d.stream(ctx, queue); err != nil {
		return 0, err
	}

	n, err := d.client.XLen(ctx, queue).Result()
	return int(n), err
}

// The scan ends at the newest entry when it began, so a copy that fails back into the same stream during a run is never taken again
// Nothing is held, so there is nothing to return
func (d *driver) Take(ctx context.Context, queue string, n int, visit func(broker.Message, func() error) (bool, error)) error {
	newest, err := d.client.XRevRangeN(ctx, queue, "+", "-", 1).Result()
	if err != nil || len(newest) == 0 {
		return err
	}

	start, end, visited := "-", newest[0].ID, 0
	for visited < n {
		page, err := d.client.XRangeN(ctx, queue, start, end, int64(min(pageSize, n-visited))).Result()
		if err != nil || len(page) == 0 {
			return err
		}

		for _, e := range page {
			if err := ctx.Err(); err != nil {
				return err
			}
			visited++
			more, err := visit(toMessage(queue, d.fields, e), func() error { return d.remove(ctx, queue, e.ID) })
			if err != nil || !more {
				return err
			}
		}
		start = "(" + page[len(page)-1].ID
	}

	return nil
}

// Redis has no locks, so an entry someone else deleted after limbo read it may have been copied twice
func (d *driver) remove(ctx context.Context, queue, id string) error {
	ctx, cancel := settle(ctx)
	defer cancel()

	n, err := d.client.XDel(ctx, queue, id).Result()
	if err != nil {
		return unsure(err)
	}
	if n == 0 {
		return fmt.Errorf("entry %s was already removed by another client", id)
	}

	return nil
}

// NOMKSTREAM makes a missing destination fail instead of creating a stream nobody reads
func (d *driver) Publish(ctx context.Context, queue string, m broker.Message) error {
	ctx, cancel := settle(ctx)
	defer cancel()

	err := d.client.XAdd(ctx, &goredis.XAddArgs{Stream: queue, NoMkStream: true, Values: copyFields(d.fields, m, time.Now())}).Err()
	if errors.Is(err, goredis.Nil) {
		return fmt.Errorf("queue %q not found", queue)
	}

	return unsure(err)
}

func (d *driver) Purge(ctx context.Context, queue string) error {
	ctx, cancel := settle(ctx)
	defer cancel()

	if err := d.stream(ctx, queue); err != nil {
		return err
	}

	return d.client.XTrimMaxLen(ctx, queue, 0).Err()
}

func (d *driver) stream(ctx context.Context, key string) error {
	t, err := d.client.Type(ctx, key).Result()
	switch {
	case err != nil:
		return err
	case t == "none":
		return fmt.Errorf("queue %q not found", key)
	case t != "stream":
		return fmt.Errorf("queue %q is a %s, limbo reads only Redis streams", key, t)
	}

	return nil
}

// Calls that settle a message's fate ignore cancellation, so Ctrl-C never leaves a message half replayed
func settle(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
}

// A write that timed out may have reached Redis anyway, and wrapping context.DeadlineExceeded makes a replay stop instead of risking more
func unsure(err error) error {
	var netErr net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
	case errors.As(err, &netErr) && netErr.Timeout():
		err = fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
	default:
		return err
	}

	return fmt.Errorf("no reply from Redis in time, it may have gone through anyway: %w", err)
}

// limbo's options are taken out of the query and the rest go to go-redis, which rejects any it doesn't know
func parseURL(u *url.URL) (*goredis.Options, fields, error) {
	query := u.Query()
	f := fields{body: defaultBody}
	for _, opt := range []struct {
		name string
		dst  *string
	}{{"body", &f.body}, {"origin", &f.origin}, {"reason", &f.reason}} {
		if !query.Has(opt.name) {
			continue
		}
		if *opt.dst = query.Get(opt.name); *opt.dst == "" {
			return nil, fields{}, fmt.Errorf("redis URL option %q needs a field name", opt.name)
		}
		query.Del(opt.name)
	}
	if f.body == f.origin || f.body == f.reason || f.origin != "" && f.origin == f.reason {
		return nil, fields{}, errors.New("the body, origin and reason options must name different fields")
	}

	rest := *u
	rest.RawQuery = query.Encode()
	opts, err := goredis.ParseURL(rest.String())
	if err != nil {
		return nil, fields{}, err
	}

	return opts, f, nil
}

// Properties always record the body field, empty when the entry had none, so a copy writes the body back under the same name or not at all
func toMessage(queue string, f fields, e goredis.XMessage) broker.Message {
	at := entryTime(e.ID)
	m := broker.Message{ID: e.ID, Queue: queue, Timestamp: at, Properties: map[string]any{bodyField: ""}}
	var death broker.Death
	types := map[string]string{}
	for k, v := range e.Values {
		s, _ := v.(string)
		switch {
		case k == f.body:
			m.Body, m.Properties[bodyField] = []byte(s), k
		case k == f.origin && k != "":
			death.Queue = s
		case k == f.reason && k != "":
			death.Reason = s
		default:
			if m.Headers == nil {
				m.Headers = map[string]any{}
			}
			if utf8.ValidString(s) {
				m.Headers[k] = s
			} else {
				m.Headers[k], types[k] = []byte(s), "bytes"
			}
		}
	}
	if death.Queue != "" || death.Reason != "" {
		death.Count, death.Time = 1, at
		m.Deaths = []broker.Death{death}
	}
	if len(types) > 0 {
		m.Properties[fieldTypes] = types
	}

	return m
}

// The ID's first part is when Redis added the entry, in milliseconds
func entryTime(id string) time.Time {
	ms, _, _ := strings.Cut(id, "-")
	n, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}
	}

	return time.UnixMilli(n).UTC()
}

// Fields come out in a fixed order, the body first and the rest sorted, since Message keeps no field order
// A message from another broker has no body_field, so its body goes under the URL's body option
func copyFields(f fields, m broker.Message, now time.Time) []any {
	types := broker.StringMap(m.Properties[fieldTypes])
	body, ok := m.Properties[bodyField].(string)
	if !ok {
		body = f.body
	}

	var out []any
	if body != "" {
		out = append(out, body, string(m.Body))
	}
	for _, k := range slices.Sorted(maps.Keys(m.Headers)) {
		if k != body && k != replayCountField && k != replayedAtField {
			out = append(out, k, text(types[k], m.Headers[k]))
		}
	}
	count, _ := strconv.Atoi(text("", m.Headers[replayCountField]))

	return append(out, replayCountField, strconv.Itoa(count+1), replayedAtField, now.UTC().Format(time.RFC3339))
}

// A message read back from a file holds binary values as base64 strings and other brokers' values as any JSON value
func text(t string, v any) string {
	switch v := v.(type) {
	case string:
		if t == "bytes" {
			if b, err := base64.StdEncoding.DecodeString(v); err == nil {
				return string(b)
			}
		}
		return v
	case []byte:
		return string(v)
	case nil:
		return ""
	case map[string]any, []any:
		b, _ := json.Marshal(v)
		return string(b)
	}

	return fmt.Sprint(v)
}
