package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
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
	refused, errFull, errRead := errors.New("refused"), errors.New("disk full"), errors.New("line 2: bad record")
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
		remove     bool
		backupFail string
		noBackup   bool
		file       bool
		readErr    error
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
		{
			name: "remove with backup", msgs: []Message{a, p, b}, filter: notPayments, remove: true,
			wantLog: []string{"backup a", "remove a", "backup b", "remove b"},
			want:    []string{"a → ", "p →  ✗ " + ErrSkipped.Error(), "b → "},
		},
		{
			name: "backup fails", msgs: []Message{a, b}, remove: true, backupFail: "a",
			wantLog: []string{"backup a"},
			want:    []string{"a →  ✗ disk full"}, wantErr: errFull,
		},
		{
			name: "remove without backup", msgs: []Message{a, o}, remove: true, noBackup: true,
			wantLog: []string{"remove a", "remove o"},
			want:    []string{"a → ", "o → "},
		},
		{
			name: "import", msgs: []Message{a, o, p}, filter: notPayments, file: true,
			wantLog: []string{"publish a>orders"},
			want:    []string{"a → orders", "o →  ✗ " + ErrNoOrigin.Error(), "p →  ✗ " + ErrSkipped.Error()},
		},
		{
			name: "import timeout", msgs: []Message{a, b}, refuse: map[string]error{"orders": timeout}, file: true,
			wantLog: []string{"publish a>orders"},
			want:    []string{"a → orders ✗ " + timeout.Error()}, wantErr: context.DeadlineExceeded,
		},
		{
			name: "import limit", msgs: []Message{a, b}, limit: 1, file: true,
			wantLog: []string{"publish a>orders"},
			want:    []string{"a → orders"},
		},
		{
			name: "import read error", msgs: []Message{a, b}, file: true, readErr: errRead,
			wantLog: []string{"publish a>orders"},
			want:    []string{"a → orders"}, wantErr: errRead,
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

			var err error
			switch {
			case tt.file:
				err = Import(context.Background(), q, records(tt.msgs, tt.readErr), ReplayOptions{Selection: sel}, each)
			case !tt.remove:
				err = Replay(context.Background(), q, "dlq", ReplayOptions{Selection: sel}, each)
			case tt.noBackup:
				err = Remove(context.Background(), q, "dlq", RemoveOptions{Selection: sel}, each)
			default:
				err = Remove(context.Background(), q, "dlq", RemoveOptions{Selection: sel, Backup: func(m Message) error {
					q.log = append(q.log, "backup "+m.ID)
					if m.ID == tt.backupFail {
						return errFull
					}
					return nil
				}}, each)
			}

			if !slices.Equal(q.log, tt.wantLog) || !slices.Equal(got, tt.want) || !errors.Is(err, tt.wantErr) || (tt.wantErr == nil) != (err == nil) {
				t.Errorf("did %q, reported %q, err %v; want %q, %q, %v", q.log, got, err, tt.wantLog, tt.want, tt.wantErr)
			}
		})
	}
}

// Yields msgs as a file would, failing with readErr after the first when it is set
func records(msgs []Message, readErr error) iter.Seq2[Message, error] {
	return func(yield func(Message, error) bool) {
		for i, m := range msgs {
			if i == 1 && readErr != nil {
				yield(Message{}, readErr)
				return
			}
			if !yield(m, nil) {
				return
			}
		}
	}
}

func TestImportCanceled(t *testing.T) {
	q := &fakeQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Import(ctx, q, records([]Message{{ID: "a", Deaths: []Death{{Queue: "orders"}}}}, nil), ReplayOptions{}, func(Taken) {})
	if !errors.Is(err, context.Canceled) || len(q.log) != 0 {
		t.Errorf("Import() with a canceled context = %v and did %q, want context.Canceled and nothing published", err, q.log)
	}
}

func TestMessageJSON(t *testing.T) {
	at := time.Date(2026, 11, 18, 14, 5, 40, 0, time.UTC)
	full := Message{
		ID:            "4f1c2a7e",
		Queue:         "orders.dlq",
		Body:          []byte(`{"orderId":1042}`),
		ContentType:   "application/json",
		Timestamp:     at.Add(-time.Minute),
		DeliveryCount: 3,
		Headers:       map[string]any{"tenant": "acme", "big": int64(1<<60 + 1)},
		Properties:    map[string]any{"app_id": "shop"},
		Deaths: []Death{
			{Queue: "orders", Reason: "rejected", Count: 2, Time: at, Exchange: "shop", RoutingKeys: []string{"order.created"}},
			{Queue: "orders.retry", Reason: "expired", Count: 1},
		},
	}
	decoded := full
	decoded.Headers = map[string]any{"tenant": "acme", "big": json.Number("1152921504606846977")}
	binary := Message{Queue: "orders.dlq", Body: []byte{0xff, 0x00}}

	for _, tt := range []struct{ in, want Message }{{full, decoded}, {binary, binary}} {
		b, err := json.Marshal(tt.in)
		if err != nil {
			t.Fatal(err)
		}
		var got Message
		if err := json.Unmarshal(b, &got); err != nil || !reflect.DeepEqual(got, tt.want) {
			t.Errorf("round trip of %s =\n%+v, %v\nwant\n%+v", b, got, err, tt.want)
		}
	}

	var m Message
	if err := json.Unmarshal([]byte(`{"queue":"q","body":"x","body_encoding":"rot13"}`), &m); err == nil || !strings.Contains(err.Error(), `body_encoding "rot13"`) {
		t.Errorf("unknown body_encoding: err = %v", err)
	}
}
