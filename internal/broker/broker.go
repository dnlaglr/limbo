package broker

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

type Message struct {
	ID            string         `json:"id,omitempty"`
	Queue         string         `json:"queue"`
	Body          []byte         `json:"-"`
	ContentType   string         `json:"content_type,omitempty"`
	Timestamp     time.Time      `json:"timestamp,omitzero"`
	DeliveryCount int            `json:"delivery_count,omitempty"`
	Headers       map[string]any `json:"headers,omitempty"`
	Properties    map[string]any `json:"properties,omitempty"`
	Deaths        []Death        `json:"deaths,omitempty"`
}

type Death struct {
	Queue       string    `json:"queue"`
	Reason      string    `json:"reason"`
	Count       int       `json:"count"`
	Time        time.Time `json:"time,omitzero"`
	Exchange    string    `json:"exchange,omitempty"`
	RoutingKeys []string  `json:"routing_keys,omitempty"`
}

func (m Message) MarshalJSON() ([]byte, error) {
	type message Message
	body, encoding := string(m.Body), "utf-8"
	if !utf8.Valid(m.Body) {
		body, encoding = base64.StdEncoding.EncodeToString(m.Body), "base64"
	}

	return json.Marshal(struct {
		message
		Body         string `json:"body"`
		BodyEncoding string `json:"body_encoding"`
	}{message(m), body, encoding})
}

// Numbers decode as json.Number so 64-bit header values keep their exact value, and drivers restore the types they need
func (m *Message) UnmarshalJSON(b []byte) error {
	type message Message
	var record struct {
		message
		Body         string `json:"body"`
		BodyEncoding string `json:"body_encoding"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&record); err != nil {
		return err
	}

	body := []byte(record.Body)
	switch record.BodyEncoding {
	case "utf-8":
	case "base64":
		var err error
		if body, err = base64.StdEncoding.DecodeString(record.Body); err != nil {
			return fmt.Errorf("body is not base64: %w", err)
		}
	default:
		return fmt.Errorf("unknown body_encoding %q (use utf-8 or base64)", record.BodyEncoding)
	}

	*m = Message(record.message)
	m.Body = body
	return nil
}

// StringMap reads a properties value that maps names to strings, as a driver stores it or as JSON decodes it
func StringMap(v any) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		strs := make(map[string]string, len(m))
		for k, s := range m {
			strs[k], _ = s.(string)
		}
		return strs
	}

	return nil
}

// Deaths are ordered most recent first so the 0 index names the queue the message last died in
func (m Message) Origin() string {
	if len(m.Deaths) == 0 {
		return ""
	}

	return m.Deaths[0].Queue
}

func (m Message) Reason() string {
	if len(m.Deaths) == 0 {
		return ""
	}

	return m.Deaths[0].Reason
}

// Falls back to the publish time when the broker records no death time
func (m Message) DiedAt() time.Time {
	if len(m.Deaths) > 0 && !m.Deaths[0].Time.IsZero() {
		return m.Deaths[0].Time
	}

	return m.Timestamp
}

type Driver interface {
	// Browse visits up to n messages without removing any, in delivery order where the broker keeps one, and stops early when visit returns false or the broker hands out no more
	Browse(ctx context.Context, queue string, n int, visit func(Message) bool) error
	// Depth may be approximate
	Depth(ctx context.Context, queue string) (int, error)
	Close() error
}

// Writes are optional capabilities found by type assertion so each broker implements only what it supports
type Taker interface {
	// Take reads like Browse but hands visit a function that removes the message, and every message visit doesn't remove goes back
	Take(ctx context.Context, queue string, n int, visit func(m Message, remove func() error) (bool, error)) error
}

type Publisher interface {
	// Publish returns nil only once the broker confirms it has the copy
	Publish(ctx context.Context, queue string, m Message) error
}

// Purger removes every message at once, without reading them
type Purger interface {
	Purge(ctx context.Context, queue string) error
}

type Replayer interface {
	Taker
	Publisher
}

type ReplayOptions struct {
	Selection
	To string
}

type RemoveOptions struct {
	Selection
	Backup func(Message) error
}

var ErrNoOrigin = errors.New("no dead-letter history, pass --to to choose a destination")

func (o ReplayOptions) Target(m Message) (string, error) {
	to := cmp.Or(o.To, m.Origin())
	if to == "" {
		return "", ErrNoOrigin
	}

	return to, nil
}

type Taken struct {
	Message Message
	To      string
	Err     error
}

func Replay(ctx context.Context, d Replayer, queue string, opts ReplayOptions, each func(Taken)) error {
	r := &run{sel: opts.Selection, each: each, keep: publishing(ctx, d, opts), stop: unsure}
	return d.Take(ctx, queue, opts.Scan, r.next)
}

// Import publishes messages read from somewhere other than a queue, such as an export file, so there is nothing to remove
func Import(ctx context.Context, d Publisher, msgs iter.Seq2[Message, error], opts ReplayOptions, each func(Taken)) error {
	r := &run{sel: opts.Selection, each: each, keep: publishing(ctx, d, opts), stop: unsure}
	for m, err := range msgs {
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			return err
		}

		more, err := r.next(m, func() error { return nil })
		if err != nil || !more {
			return err
		}
	}

	return nil
}

func publishing(ctx context.Context, d Publisher, opts ReplayOptions) func(Message) (string, error) {
	return func(m Message) (string, error) {
		to, err := opts.Target(m)
		if err == nil {
			err = d.Publish(ctx, to, m)
		}
		return to, err
	}
}

// A send that timed out may have reached its destination anyway, so the run stops instead of risking more
func unsure(err error) bool { return errors.Is(err, context.DeadlineExceeded) }

// A backup that fails ends the run, since every message after it would fail the same way
func Remove(ctx context.Context, d Taker, queue string, opts RemoveOptions, each func(Taken)) error {
	r := &run{sel: opts.Selection, each: each, stop: func(error) bool { return true }}
	r.keep = func(m Message) (string, error) {
		if opts.Backup == nil {
			return "", nil
		}
		return "", opts.Backup(m)
	}

	return d.Take(ctx, queue, opts.Scan, r.next)
}

type run struct {
	sel     Selection
	keep    func(Message) (to string, err error)
	stop    func(error) bool
	each    func(Taken)
	matched int
}

// A message is removed only after keep succeeds, and a failed remove ends the run because the message is then both kept and still queued
func (r *run) next(m Message, remove func() error) (bool, error) {
	res := Taken{Message: m}
	if !r.sel.Filter.Match(m) {
		res.Err = ErrSkipped
		r.each(res)

		return true, nil
	}

	r.matched++
	var err error
	if res.To, res.Err = r.keep(m); res.Err == nil {
		err = remove()
	} else if r.stop(res.Err) {
		err = res.Err
	}
	r.each(res)

	return err == nil && !r.sel.Full(r.matched), err
}

type Opener func(ctx context.Context, u *url.URL) (Driver, error)

type Registry map[string]Opener

// Parse error is rebuilt because url.Error embeds the raw URL, and its detail can quote a password that ends the host early
func ParseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	var urlErr *url.Error
	switch {
	case errors.As(err, &urlErr) && strings.Contains(raw, "@"):
		return nil, errors.New("invalid broker URL: percent-encode special characters such as / ? # @ in the user name and password")
	case errors.As(err, &urlErr):
		return nil, fmt.Errorf("invalid broker URL: %w", urlErr.Err)
	}

	return u, err
}

func (r Registry) Open(ctx context.Context, u *url.URL) (Driver, error) {
	open, ok := r[u.Scheme]
	if !ok {
		return nil, fmt.Errorf("unsupported broker URL scheme %q (use %s)", u.Scheme, r.schemes())
	}

	return open(ctx, u)
}

func (r Registry) schemes() string {
	return strings.Join(slices.Sorted(maps.Keys(r)), "://, ") + "://"
}
