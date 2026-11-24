package broker

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"
)

func TestRegistryOpen(t *testing.T) {
	var opened string
	opener := func(name string) Opener {
		return func(context.Context, *url.URL) (Driver, error) {
			opened = name
			return nil, nil
		}
	}
	r := Registry{"amqp": opener("amqp"), "amqps": opener("amqps")}

	tests := []struct {
		raw     string
		want    string
		wantErr string
	}{
		{raw: "amqp://guest:guest@localhost:5672/", want: "amqp"},
		{raw: "AMQPS://broker:5671/prod", want: "amqps"},
		{raw: "redis://localhost:6379", wantErr: `unsupported broker URL scheme "redis" (use amqp://, amqps://)`},
		{raw: "localhost:5672", wantErr: `unsupported broker URL scheme "localhost"`},
		{raw: "amqp://user:s3cret@[::1", wantErr: "invalid broker URL"},
		{raw: "amqp://user:s3cret/x@localhost:5672/", wantErr: "percent-encode"},
		{raw: "amqp://user:s3cret?x@localhost:5672/", wantErr: "percent-encode"},
		{raw: "amqp://user:s3cret#x@localhost:5672/", wantErr: "percent-encode"},
		{raw: "amqp://[::1", wantErr: `invalid broker URL: missing ']' in host`},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			opened = ""
			u, err := ParseURL(tt.raw)
			if err == nil {
				_, err = r.Open(context.Background(), u)
			}

			if tt.wantErr == "" {
				if err != nil || opened != tt.want {
					t.Fatalf("opened %q, err %v; want %q", opened, err, tt.want)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "s3cret") {
				t.Errorf("err leaks the password: %v", err)
			}
		})
	}
}

type fakeQueue struct {
	msgs       []Message
	refuse     map[string]error
	failRemove string
	log        []string
}

func (f *fakeQueue) Take(_ context.Context, _ string, n int, visit func(Message, func() error) (bool, error)) error {
	for _, m := range f.msgs[:min(n, len(f.msgs))] {
		more, err := visit(m, func() error {
			f.log = append(f.log, "remove "+m.ID)
			if m.ID == f.failRemove {
				return errRemove
			}
			return nil
		})
		if err != nil || !more {
			return err
		}
	}

	return nil
}

func (f *fakeQueue) Publish(_ context.Context, to string, m Message) error {
	f.log = append(f.log, "publish "+m.ID+">"+to)
	return f.refuse[to]
}

var errRemove = errors.New("remove failed")

func TestTake(t *testing.T) {
	died := func(id, queue string) Message { return Message{ID: id, Deaths: []Death{{Queue: queue}}} }
	refused := errors.New("refused")
	timeout := fmt.Errorf("no confirm: %w", context.DeadlineExceeded)
	notPayments := Filter{Sources: Rule[string]{None: []string{"payments"}}}
	a, b, o, p := died("a", "orders"), died("b", "orders"), Message{ID: "o"}, died("p", "payments")

	tests := []struct {
		name       string
		msgs       []Message
		filter     Filter
		limit      int
		refuse     map[string]error
		failRemove string
		wantLog    []string
		want       []string
		wantErr    error
	}{
		{
			name: "replay", msgs: []Message{a, o, p}, filter: notPayments,
			wantLog: []string{"publish a>orders", "remove a"},
			want:    []string{"a → orders", "o →  ✗ " + ErrNoOrigin.Error(), "p →  ✗ " + ErrSkipped.Error()},
		},
		{
			name: "refused", msgs: []Message{a, p}, refuse: map[string]error{"orders": refused},
			wantLog: []string{"publish a>orders", "publish p>payments", "remove p"},
			want:    []string{"a → orders ✗ refused", "p → payments"},
		},
		{
			name: "timeout", msgs: []Message{a, b}, refuse: map[string]error{"orders": timeout},
			wantLog: []string{"publish a>orders"},
			want:    []string{"a → orders ✗ " + timeout.Error()}, wantErr: context.DeadlineExceeded,
		},
		{
			name: "failed remove", msgs: []Message{a, b}, failRemove: "a",
			wantLog: []string{"publish a>orders", "remove a"},
			want:    []string{"a → orders"}, wantErr: errRemove,
		},
		{
			name: "limit", msgs: []Message{a, b}, limit: 1,
			wantLog: []string{"publish a>orders", "remove a"},
			want:    []string{"a → orders"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &fakeQueue{msgs: tt.msgs, refuse: tt.refuse, failRemove: tt.failRemove}
			sel := Selection{Filter: tt.filter, Limit: tt.limit, Scan: len(tt.msgs)}
			var got []string
			each := func(r Taken) {
				s := r.Message.ID + " → " + r.To
				if r.Err != nil {
					s += " ✗ " + r.Err.Error()
				}
				got = append(got, s)
			}

			err := Replay(context.Background(), q, "dlq", ReplayOptions{Selection: sel}, each)
			if !slices.Equal(q.log, tt.wantLog) || !slices.Equal(got, tt.want) || !errors.Is(err, tt.wantErr) || (tt.wantErr == nil) != (err == nil) {
				t.Errorf("did %q, reported %q, err %v; want %q, %q, %v", q.log, got, err, tt.wantLog, tt.want, tt.wantErr)
			}
		})
	}
}
