package broker

import (
	"regexp"
	"testing"
	"time"
)

func TestFilterMatch(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	rejected := Message{
		Body:    []byte(`{"orderId":1042}`),
		Headers: map[string]any{"tenant": "acme", "attempt": int64(3)},
		Deaths: []Death{
			{Queue: "orders", Reason: "rejected", Time: now.Add(-30 * time.Minute)},
			{Queue: "orders.retry", Reason: "expired", Time: now.Add(-3 * time.Hour)},
		},
	}
	orphan := Message{Body: []byte{0xff, 0x00, 'o', 'k'}, Timestamp: now.Add(-48 * time.Hour)}
	undated := Message{}

	strs := func(anyOf, noneOf []string) Rule[string] { return Rule[string]{Any: anyOf, None: noneOf} }
	tests := []struct {
		name   string
		filter Filter
		want   [3]bool
	}{
		{"no filter", Filter{}, [3]bool{true, true, true}},
		{"reason", Filter{Reasons: strs([]string{"expired", "rejected"}, nil)}, [3]bool{true, false, false}},
		{"only the last death counts", Filter{Reasons: strs([]string{"expired"}, nil)}, [3]bool{false, false, false}},
		{"not reason", Filter{Reasons: strs(nil, []string{"rejected"})}, [3]bool{false, true, true}},
		{"source", Filter{Sources: strs([]string{"orders"}, nil)}, [3]bool{true, false, false}},
		{"not source", Filter{Sources: strs(nil, []string{"orders"})}, [3]bool{false, true, true}},
		{"header value", Filter{Headers: Rule[Header]{Any: []Header{{Key: "attempt", Value: "3", HasValue: true}}}}, [3]bool{true, false, false}},
		{"header presence", Filter{Headers: Rule[Header]{Any: []Header{{Key: "tenant"}}}}, [3]bool{true, false, false}},
		{"empty header value", Filter{Headers: Rule[Header]{Any: []Header{{Key: "tenant", HasValue: true}}}}, [3]bool{false, false, false}},
		{"not header", Filter{Headers: Rule[Header]{None: []Header{{Key: "tenant", Value: "acme", HasValue: true}}}}, [3]bool{false, true, true}},
		{"body", Filter{Bodies: Rule[*regexp.Regexp]{Any: []*regexp.Regexp{regexp.MustCompile(`"orderId":10\d\d`)}}}, [3]bool{true, false, false}},
		{"binary body", Filter{Bodies: Rule[*regexp.Regexp]{Any: []*regexp.Regexp{regexp.MustCompile(`ok$`)}}}, [3]bool{false, true, false}},
		{"not body", Filter{Bodies: Rule[*regexp.Regexp]{None: []*regexp.Regexp{regexp.MustCompile(`orderId`)}}}, [3]bool{false, true, true}},
		{"newer than", Filter{After: now.Add(-time.Hour)}, [3]bool{true, false, false}},
		{"older than falls back to the publish time", Filter{Before: now.Add(-time.Hour)}, [3]bool{false, true, false}},
		{"all filters must match", Filter{Reasons: strs([]string{"rejected"}, nil), Before: now.Add(-time.Hour)}, [3]bool{false, false, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, m := range []Message{rejected, orphan, undated} {
				if got := tt.filter.Match(m); got != tt.want[i] {
					t.Errorf("Match(%s) = %v, want %v", [3]string{"rejected", "orphan", "undated"}[i], got, tt.want[i])
				}
			}
		})
	}
}
