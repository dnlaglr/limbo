package broker

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
type Replayer interface {
	Replay(ctx context.Context, queue string, opts ReplayOptions, each func(Replayed)) error
}

type ReplayOptions struct {
	Selection
	To string
}

var ErrNoOrigin = errors.New("no dead-letter history, pass --to to choose a destination")

func (o ReplayOptions) Target(m Message) (string, error) {
	to := cmp.Or(o.To, m.Origin())
	if to == "" {
		return "", ErrNoOrigin
	}

	return to, nil
}

type Replayed struct {
	Message Message
	To      string
	Err     error
}

type ReplayRun struct {
	opts    ReplayOptions
	each    func(Replayed)
	matched int
}

func (o ReplayOptions) Start(each func(Replayed)) *ReplayRun {
	return &ReplayRun{opts: o, each: each}
}

// A message is removed only after send succeeds, and a failed remove ends the run because the message is then in both queues
func (r *ReplayRun) Next(m Message, send func(to string) error, remove func() error) (Replayed, bool, error) {
	res := Replayed{Message: m}
	if !r.opts.Filter.Match(m) {
		res.Err = ErrSkipped
		r.each(res)

		return res, true, nil
	}

	r.matched++
	if res.To, res.Err = r.opts.Target(m); res.Err == nil {
		res.Err = send(res.To)
	}

	var err error
	if res.Err == nil {
		err = remove()
	}
	r.each(res)

	return res, err == nil && !r.opts.Full(r.matched), err
}

type Opener func(ctx context.Context, u *url.URL) (Driver, error)

type Registry map[string]Opener

// Parse error is rebuilt because url.Error embeds the raw URL and password
func ParseURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
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
