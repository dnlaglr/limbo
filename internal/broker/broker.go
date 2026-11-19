package broker

import (
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

type Driver interface {
	Peek(ctx context.Context, queue string, limit int) ([]Message, error)
	Close() error
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
