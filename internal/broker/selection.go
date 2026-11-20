package broker

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"
)

// Filter is matched by limbo rather than the broker because no supported broker can filter a queue server-side
type Filter struct {
	Reasons Rule[string]
	Sources Rule[string]
	Headers Rule[Header]
	Bodies  Rule[*regexp.Regexp]
	After   time.Time
	Before  time.Time
}

// Rule matches when Any is empty or one of its values matches and none of None match
type Rule[T any] struct {
	Any  []T
	None []T
}

type Header struct {
	Key      string
	Value    string
	HasValue bool
}

// Scan is the queue depth counted before a run, so messages that fail again and flow back are never seen twice
type Selection struct {
	Filter Filter
	Limit  int
	Scan   int
}

var ErrSkipped = errors.New("did not match the filters")

func (f Filter) Match(m Message) bool {
	at := m.DiedAt()

	return f.Reasons.match(equal(m.Reason())) &&
		f.Sources.match(equal(m.Origin())) &&
		f.Headers.match(func(h Header) bool { return h.in(m.Headers) }) &&
		f.Bodies.match(func(re *regexp.Regexp) bool { return re.Match(m.Body) }) &&
		(f.After.IsZero() || at.After(f.After)) &&
		(f.Before.IsZero() || !at.IsZero() && at.Before(f.Before))
}

func (f Filter) IsZero() bool {
	return f.Reasons.empty() && f.Sources.empty() && f.Headers.empty() && f.Bodies.empty() && f.After.IsZero() && f.Before.IsZero()
}

func (s Selection) Full(matched int) bool {
	return s.Limit > 0 && matched >= s.Limit
}

func (r Rule[T]) match(ok func(T) bool) bool {
	return (len(r.Any) == 0 || slices.ContainsFunc(r.Any, ok)) && !slices.ContainsFunc(r.None, ok)
}

func (r Rule[T]) empty() bool {
	return len(r.Any) == 0 && len(r.None) == 0
}

// Values compare in the form peek prints them
func (h Header) in(headers map[string]any) bool {
	v, ok := headers[h.Key]
	return ok && (!h.HasValue || fmt.Sprint(v) == h.Value)
}

func equal(s string) func(string) bool {
	return func(v string) bool { return v == s }
}
