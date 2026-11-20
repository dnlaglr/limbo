package broker

import (
	"context"
	"errors"
	"net/url"
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

func TestReplayRun(t *testing.T) {
	died := func(id, queue string) Message { return Message{ID: id, Deaths: []Death{{Queue: queue}}} }
	refused, errRemove := errors.New("refused"), errors.New("remove failed")

	type step struct {
		m                  Message
		sendErr, removeErr error
		want               string
		sent, removed      bool
		more               bool
		err                error
	}
	tests := []struct {
		name  string
		limit int
		steps []step
	}{
		{"filtered", 3, []step{
			{m: died("p", "payments"), want: "p →  ✗ " + ErrSkipped.Error(), more: true},
			{m: Message{ID: "o"}, want: "o →  ✗ " + ErrNoOrigin.Error(), more: true},
			{m: died("a", "orders"), sendErr: refused, want: "a → orders ✗ refused", sent: true, more: true},
			{m: died("b", "orders"), removeErr: errRemove, want: "b → orders", sent: true, removed: true, err: errRemove},
		}},
		{"limit", 1, []step{
			{m: died("c", "orders"), want: "c → orders", sent: true, removed: true},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			opts := ReplayOptions{Selection: Selection{Filter: Filter{Sources: Rule[string]{None: []string{"payments"}}}, Limit: tt.limit}}
			run := opts.Start(func(r Replayed) {
				got = r.Message.ID + " → " + r.To
				if r.Err != nil {
					got += " ✗ " + r.Err.Error()
				}
			})

			for _, s := range tt.steps {
				sent, removed := false, false
				_, more, err := run.Next(s.m,
					func(string) error { sent = true; return s.sendErr },
					func() error { removed = true; return s.removeErr })
				if got != s.want || sent != s.sent || removed != s.removed || more != s.more || !errors.Is(err, s.err) {
					t.Errorf("Next(%s) reported %q, sent %v, removed %v, more %v, err %v; want %q, %v, %v, %v, %v",
						s.m.ID, got, sent, removed, more, err, s.want, s.sent, s.removed, s.more, s.err)
				}
			}
		})
	}
}
