package redis

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dnlaglr/limbo/internal/broker"
)

func TestParseURL(t *testing.T) {
	tests := []struct {
		raw     string
		db      int
		user    string
		fields  fields
		tls     bool
		wantErr string
	}{
		{raw: "redis://localhost:6379/2", db: 2, fields: fields{body: "payload"}},
		{raw: "rediss://u:p@h/0", user: "u", tls: true, fields: fields{body: "payload"}},
		{raw: "redis://h?origin=src&reason=why&body=data&dial_timeout=2s", fields: fields{body: "data", origin: "src", reason: "why"}},
		{raw: "redis://u:s3cr%2Ft@h?bogus=1", wantErr: "unexpected option"},
		{raw: "redis://u:s3cr%2Ft@h/x", wantErr: "invalid database number"},
		{raw: "redis://u:s3cr%2Ft@h?body=", wantErr: `redis URL option "body" needs a field name`},
		{raw: "redis://u:s3cr%2Ft@h?body=a&origin=a", wantErr: "the body, origin and reason options must name different fields"},
		{raw: "redis://u:s3cr%2Ft@h?origin=a&reason=a", wantErr: "the body, origin and reason options must name different fields"},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			u, err := broker.ParseURL(tt.raw)
			if err != nil {
				t.Fatal(err)
			}

			opts, f, err := parseURL(u)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) || strings.Contains(err.Error(), "s3cr") {
					t.Fatalf("err = %v, want it to contain %q and no secret", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if opts.DB != tt.db || opts.Username != tt.user || (opts.TLSConfig != nil) != tt.tls || f != tt.fields {
				t.Errorf("got db %d, user %q, tls %v, fields %+v", opts.DB, opts.Username, opts.TLSConfig != nil, f)
			}
		})
	}
}

func TestToMessage(t *testing.T) {
	at := time.UnixMilli(1732400000000).UTC()
	tests := []struct {
		name   string
		fields fields
		values map[string]any
		want   broker.Message
	}{
		{
			name:   "body and header",
			fields: fields{body: "payload"},
			values: map[string]any{"payload": "hi", "tenant": "a"},
			want: broker.Message{
				Body: []byte("hi"), Headers: map[string]any{"tenant": "a"},
				Properties: map[string]any{bodyField: "payload"},
			},
		},
		{
			name:   "origin and reason",
			fields: fields{body: "payload", origin: "src", reason: "why"},
			values: map[string]any{"payload": "hi", "src": "orders", "why": "boom"},
			want: broker.Message{
				Body: []byte("hi"), Properties: map[string]any{bodyField: "payload"},
				Deaths: []broker.Death{{Queue: "orders", Reason: "boom", Count: 1, Time: at}},
			},
		},
		{
			name:   "no body field",
			fields: fields{body: "payload"},
			values: map[string]any{"tenant": "a"},
			want:   broker.Message{Headers: map[string]any{"tenant": "a"}, Properties: map[string]any{bodyField: ""}},
		},
		{
			name:   "binary field",
			fields: fields{body: "payload"},
			values: map[string]any{"payload": "hi", "sig": "\xff\x00"},
			want: broker.Message{
				Body: []byte("hi"), Headers: map[string]any{"sig": []byte{0xff, 0}},
				Properties: map[string]any{bodyField: "payload", fieldTypes: map[string]string{"sig": "bytes"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.want.ID, tt.want.Queue, tt.want.Timestamp = "1732400000000-0", "dlq", at

			got := toMessage("dlq", tt.fields, goredis.XMessage{ID: "1732400000000-0", Values: tt.values})
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("toMessage() =\n%#v\nwant\n%#v", got, tt.want)
			}
		})
	}
}

func TestCopyFields(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.FixedZone("x", 3600))
	stamp := "2026-11-24T11:00:00Z"
	f := fields{body: "payload"}
	fromQueue := toMessage("dlq", f, goredis.XMessage{ID: "1-0", Values: map[string]any{"payload": "hi", "tenant": "a", "sig": "\xff\x00"}})
	tests := []struct {
		name string
		m    broker.Message
		want []any
	}{
		{
			name: "from queue",
			m:    fromQueue,
			want: []any{"payload", "hi", "sig", "\xff\x00", "tenant", "a", replayCountField, "1", replayedAtField, stamp},
		},
		{
			name: "from file",
			m:    viaFile(t, fromQueue),
			want: []any{"payload", "hi", "sig", "\xff\x00", "tenant", "a", replayCountField, "1", replayedAtField, stamp},
		},
		{
			name: "replayed before",
			m:    broker.Message{Body: []byte("hi"), Headers: map[string]any{replayCountField: "2", replayedAtField: "old"}, Properties: map[string]any{bodyField: "data"}},
			want: []any{"data", "hi", replayCountField, "3", replayedAtField, stamp},
		},
		{
			name: "no body field",
			m:    broker.Message{Headers: map[string]any{"tenant": "a"}, Properties: map[string]any{bodyField: ""}},
			want: []any{"tenant", "a", replayCountField, "1", replayedAtField, stamp},
		},
		{
			name: "another broker's file",
			m: viaFile(t, broker.Message{
				Body: []byte("hi"), Headers: map[string]any{"attempt": 5, "nested": map[string]any{"k": "v"}, "ok": true, "none": nil},
				Properties: map[string]any{"header_types": map[string]string{"attempt": "int32"}},
			}),
			want: []any{"payload", "hi", "attempt", "5", "nested", `{"k":"v"}`, "none", "", "ok", "true", replayCountField, "1", replayedAtField, stamp},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := copyFields(f, tt.m, now); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("copyFields() =\n%q\nwant\n%q", got, tt.want)
			}
		})
	}
}

// Writes m as an export file line would and reads it back
func viaFile(t *testing.T, m broker.Message) broker.Message {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}

	var decoded broker.Message
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}

	return decoded
}

// broker.Replay stops only on context.DeadlineExceeded, so every timeout must come out wrapping it
func TestUnsure(t *testing.T) {
	refused := errors.New("connection refused")
	tests := []struct {
		name string
		in   error
		stop bool
	}{
		{"context deadline", context.DeadlineExceeded, true},
		{"socket timeout", &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}, true},
		{"other error", refused, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := unsure(tt.in)
			if errors.Is(got, context.DeadlineExceeded) != tt.stop || !errors.Is(got, tt.in) {
				t.Errorf("unsure(%v) = %v, want wrapping DeadlineExceeded: %v", tt.in, got, tt.stop)
			}
		})
	}
	if unsure(nil) != nil {
		t.Error("unsure(nil) is not nil")
	}
}

func TestBrowseLeavesEntries(t *testing.T) {
	d, dlq := connect(t, ""), key(t, "dlq")
	seed(t, d, dlq, "m0", "m1", "m2")

	visited := 0
	err := d.Browse(context.Background(), dlq, 3, func(broker.Message) bool { visited++; return visited < 2 })
	if err != nil || visited != 2 {
		t.Fatalf("visited %d with err %v, want 2 and nil", visited, err)
	}
	if got := bodies(t, d, dlq); !slices.Equal(got, []string{"m0", "m1", "m2"}) {
		t.Errorf("left %q, want every entry in order", got)
	}
}

func TestMapping(t *testing.T) {
	d, dlq := connect(t, "?origin=src&reason=why"), key(t, "dlq")
	add(t, d, dlq, "payload", "m0", "src", "orders", "why", "boom", "tenant", "a")

	m := browseOne(t, d, dlq)
	if m.Origin() != "orders" || m.Reason() != "boom" || m.DiedAt().IsZero() || !reflect.DeepEqual(m.Headers, map[string]any{"tenant": "a"}) {
		t.Errorf("got origin %q, reason %q, died %v, headers %v", m.Origin(), m.Reason(), m.DiedAt(), m.Headers)
	}
}

func TestNotAStream(t *testing.T) {
	ctx := context.Background()
	d, dlq, list, missing := connect(t, ""), key(t, "dlq"), key(t, "list"), key(t, "nope")
	seed(t, d, dlq, "m0")
	if err := d.client.LPush(ctx, list, "x").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.client.Del(ctx, list) })

	for name, err := range map[string]error{"depth": depthErr(d, list), "purge": d.Purge(ctx, list)} {
		if err == nil || !strings.Contains(err.Error(), "queue \""+list+"\" is a list, limbo reads only Redis streams") {
			t.Errorf("%s: err = %v, want a not-a-stream error", name, err)
		}
	}
	for name, err := range map[string]error{"depth": depthErr(d, missing), "purge": d.Purge(ctx, missing)} {
		if err == nil || err.Error() != "queue \""+missing+"\" not found" {
			t.Errorf("%s of a missing key: err = %v, want not found", name, err)
		}
	}

	results := replay(t, d, dlq, broker.ReplayOptions{Selection: broker.Selection{Scan: 1}, To: list})
	if len(results) != 1 || results[0].Err == nil || len(bodies(t, d, dlq)) != 1 {
		t.Errorf("replay to a list gave %+v, want an error and the entry kept", results)
	}
}

func TestReplay(t *testing.T) {
	ctx := context.Background()
	d := connect(t, "?origin=src")

	t.Run("to origin", func(t *testing.T) {
		dlq, origin := key(t, "dlq"), key(t, "orders")
		emptyStream(t, d, origin)
		add(t, d, dlq, "payload", "m0", "src", origin, "tenant", "a")
		add(t, d, dlq, "payload", "m1", "src", origin, "tenant", "b")

		for _, r := range replay(t, d, dlq, firstN(2)) {
			if r.Err != nil || r.To != origin {
				t.Errorf("%s went to %q with %v", r.Message.ID, r.To, r.Err)
			}
		}
		if n := len(bodies(t, d, dlq)); n != 0 {
			t.Errorf("%d entries left in the DLQ, want 0", n)
		}

		copies, err := d.client.XRange(ctx, origin, "-", "+").Result()
		if err != nil || len(copies) != 2 {
			t.Fatalf("origin holds %d entries (%v), want 2", len(copies), err)
		}
		for i, c := range copies {
			_, hasSrc := c.Values["src"]
			if c.Values["payload"] != "m"+strconv.Itoa(i) || c.Values["tenant"] == nil || c.Values[replayCountField] != "1" || c.Values[replayedAtField] == nil || hasSrc {
				t.Errorf("copy %d has fields %v, want the original's without src, plus the markers", i, c.Values)
			}
		}
	})

	t.Run("missing stream", func(t *testing.T) {
		dlq := key(t, "dlq")
		add(t, d, dlq, "payload", "m0", "src", key(t, "nope"))

		results := replay(t, d, dlq, firstN(1))
		if len(results) != 1 || results[0].Err == nil || !strings.Contains(results[0].Err.Error(), "not found") {
			t.Errorf("got %+v, want a not found error", results)
		}
		if n := len(bodies(t, d, dlq)); n != 1 {
			t.Errorf("%d entries left in the DLQ, want 1", n)
		}
	})

	t.Run("canceled", func(t *testing.T) {
		dlq, origin := key(t, "dlq"), key(t, "orders")
		emptyStream(t, d, origin)
		for i := range 3 {
			add(t, d, dlq, "payload", "m"+strconv.Itoa(i), "src", origin)
		}

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		err := broker.Replay(ctx, d, dlq, firstN(3), func(broker.Taken) { cancel() })
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
		if got := bodies(t, d, dlq); !slices.Equal(got, []string{"m1", "m2"}) {
			t.Errorf("left %q, want m1 and m2", got)
		}
	})

	t.Run("filtered", func(t *testing.T) {
		dlq, origin := key(t, "dlq"), key(t, "orders")
		emptyStream(t, d, origin)
		for i, tenant := range []string{"a", "b", "a"} {
			add(t, d, dlq, "payload", "m"+strconv.Itoa(i), "src", origin, "tenant", tenant)
		}

		opts := firstN(3)
		opts.Filter.Headers.Any = []broker.Header{{Key: "tenant", Value: "b", HasValue: true}}
		replay(t, d, dlq, opts)
		if got := bodies(t, d, dlq); !slices.Equal(got, []string{"m0", "m2"}) {
			t.Errorf("left %q, want m0 and m2 in order", got)
		}
	})

	t.Run("no origin", func(t *testing.T) {
		dlq, to := key(t, "dlq"), key(t, "to")
		emptyStream(t, d, to)
		seed(t, d, dlq, "m0")

		results := replay(t, d, dlq, firstN(1))
		if len(results) != 1 || !errors.Is(results[0].Err, broker.ErrNoOrigin) {
			t.Errorf("got %+v, want ErrNoOrigin", results)
		}

		opts := firstN(1)
		opts.To = to
		if results := replay(t, d, dlq, opts); len(results) != 1 || results[0].Err != nil || len(bodies(t, d, dlq)) != 0 {
			t.Errorf("replay with --to gave %+v, want the entry moved", results)
		}
	})
}

func TestRemoveFiltered(t *testing.T) {
	d, dlq := connect(t, ""), key(t, "dlq")
	seed(t, d, dlq, "m0", "m1", "m2")

	var backup []string
	opts := broker.RemoveOptions{
		Selection: broker.Selection{Scan: 3, Filter: broker.Filter{Bodies: broker.Rule[*regexp.Regexp]{Any: []*regexp.Regexp{regexp.MustCompile("m1")}}}},
		Backup:    func(m broker.Message) error { backup = append(backup, string(m.Body)); return nil },
	}
	if err := broker.Remove(context.Background(), d, dlq, opts, func(broker.Taken) {}); err != nil {
		t.Fatal(err)
	}
	if left := bodies(t, d, dlq); !slices.Equal(backup, []string{"m1"}) || !slices.Equal(left, []string{"m0", "m2"}) {
		t.Errorf("backed up %q and left %q, want m1 and m0, m2", backup, left)
	}
}

func TestPurgeKeepsStream(t *testing.T) {
	ctx := context.Background()
	d, dlq := connect(t, ""), key(t, "dlq")
	seed(t, d, dlq, "m0", "m1")

	if err := d.Purge(ctx, dlq); err != nil {
		t.Fatal(err)
	}
	if n, err := d.Depth(ctx, dlq); n != 0 || err != nil {
		t.Errorf("depth after purge = %d, %v, want 0 and the stream kept", n, err)
	}
}

// Another client deleting the entry between limbo's copy and its remove has made a copy of its own, so the run must stop
func TestConcurrentRemoval(t *testing.T) {
	d, dlq, to := connect(t, ""), key(t, "dlq"), key(t, "to")
	emptyStream(t, d, to)
	seed(t, d, dlq, "m0", "m1")

	racer := racingReplayer{d, dlq}
	err := broker.Replay(context.Background(), racer, dlq, broker.ReplayOptions{Selection: broker.Selection{Scan: 2}, To: to}, func(broker.Taken) {})
	if err == nil || !strings.Contains(err.Error(), "already removed by another client") {
		t.Errorf("err = %v, want the run stopped by the concurrent removal", err)
	}
	if left := bodies(t, d, dlq); !slices.Equal(left, []string{"m1"}) {
		t.Errorf("left %q, want m1 untouched", left)
	}
}

type racingReplayer struct {
	*driver
	dlq string
}

func (r racingReplayer) Publish(ctx context.Context, queue string, m broker.Message) error {
	if err := r.driver.Publish(ctx, queue, m); err != nil {
		return err
	}

	return r.client.XDel(ctx, r.dlq, m.ID).Err()
}

func TestScanBound(t *testing.T) {
	d, dlq := connect(t, ""), key(t, "dlq")
	seed(t, d, dlq, "m0", "m1")

	results := replay(t, d, dlq, broker.ReplayOptions{Selection: broker.Selection{Scan: 10}, To: dlq})
	if len(results) != 2 {
		t.Fatalf("took %d entries, want the 2 that were there when the scan began", len(results))
	}

	entries, err := d.client.XRange(context.Background(), dlq, "-", "+").Result()
	if err != nil || len(entries) != 2 {
		t.Fatalf("DLQ holds %d entries (%v), want the 2 copies", len(entries), err)
	}
	for _, e := range entries {
		if e.Values[replayCountField] != "1" {
			t.Errorf("entry %s has replay count %v, want 1", e.ID, e.Values[replayCountField])
		}
	}
}

func TestImportRoundTrip(t *testing.T) {
	ctx := context.Background()
	d, from, to := connect(t, ""), key(t, "from"), key(t, "to")
	emptyStream(t, d, to)
	add(t, d, from, "payload", "hi", "tenant", "a", "sig", "\xff\x00")
	original := browseOne(t, d, from)

	err := broker.Import(ctx, d, func(yield func(broker.Message, error) bool) { yield(viaFile(t, original), nil) },
		broker.ReplayOptions{To: to}, func(broker.Taken) {})
	if err != nil {
		t.Fatal(err)
	}

	copied := browseOne(t, d, to)
	delete(copied.Headers, replayCountField)
	delete(copied.Headers, replayedAtField)
	if string(copied.Body) != "hi" || !reflect.DeepEqual(copied.Headers, original.Headers) || !reflect.DeepEqual(copied.Properties, original.Properties) {
		t.Errorf("imported copy has headers %#v and properties %#v, want the original's %#v and %#v", copied.Headers, copied.Properties, original.Headers, original.Properties)
	}
}

// Requires Redis, e.g. LIMBO_TEST_REDIS_URL=redis://localhost:6379/0
func connect(t *testing.T, query string) *driver {
	t.Helper()
	raw := os.Getenv("LIMBO_TEST_REDIS_URL")
	if raw == "" {
		t.Skip("LIMBO_TEST_REDIS_URL is not set")
	}
	u, err := broker.ParseURL(raw + query)
	if err != nil {
		t.Fatal(err)
	}

	d, err := Open(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })

	return d.(*driver)
}

func key(t *testing.T, name string) string {
	return "limbo-test:" + t.Name() + ":" + strconv.FormatInt(time.Now().UnixNano(), 36) + ":" + name
}

func add(t *testing.T, d *driver, stream string, values ...any) {
	t.Helper()
	ctx := context.Background()
	if err := d.client.XAdd(ctx, &goredis.XAddArgs{Stream: stream, Values: values}).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.client.Del(ctx, stream) })
}

func seed(t *testing.T, d *driver, stream string, payloads ...string) {
	t.Helper()
	for _, p := range payloads {
		add(t, d, stream, "payload", p)
	}
}

// Publish never creates a stream, so a destination must exist before a test replays into it
func emptyStream(t *testing.T, d *driver, stream string) {
	t.Helper()
	add(t, d, stream, "placeholder", "x")
	if err := d.client.XTrimMaxLen(context.Background(), stream, 0).Err(); err != nil {
		t.Fatal(err)
	}
}

func depthErr(d *driver, stream string) error {
	_, err := d.Depth(context.Background(), stream)
	return err
}

func firstN(n int) broker.ReplayOptions {
	return broker.ReplayOptions{Selection: broker.Selection{Scan: n}}
}

func replay(t *testing.T, d broker.Replayer, dlq string, opts broker.ReplayOptions) []broker.Taken {
	t.Helper()
	var results []broker.Taken
	if err := broker.Replay(context.Background(), d, dlq, opts, func(r broker.Taken) { results = append(results, r) }); err != nil {
		t.Fatal(err)
	}

	return results
}

func browseOne(t *testing.T, d *driver, stream string) broker.Message {
	t.Helper()
	var got []broker.Message
	if err := d.Browse(context.Background(), stream, 10, func(m broker.Message) bool { got = append(got, m); return true }); err != nil || len(got) != 1 {
		t.Fatalf("browsed %d messages (%v), want 1", len(got), err)
	}

	return got[0]
}

func bodies(t *testing.T, d *driver, stream string) []string {
	t.Helper()
	var got []string
	if err := d.Browse(context.Background(), stream, 100, func(m broker.Message) bool { got = append(got, string(m.Body)); return true }); err != nil {
		t.Fatal(err)
	}

	return got
}
