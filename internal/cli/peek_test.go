package cli

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dnlaglr/limbo/internal/broker"
)

type fakeDriver struct {
	msgs   []broker.Message
	err    error
	queue  string
	limit  int
	closed bool
}

func (f *fakeDriver) Peek(_ context.Context, queue string, limit int) ([]broker.Message, error) {
	f.queue, f.limit = queue, limit
	return f.msgs, f.err
}

func (f *fakeDriver) Close() error {
	f.closed = true
	return nil
}

func fakeBrokers(d *fakeDriver) broker.Registry {
	return broker.Registry{"fake": func(context.Context, *url.URL) (broker.Driver, error) { return d, nil }}
}

var (
	publishedAt = time.Date(2026, 11, 18, 14, 2, 11, 0, time.UTC)
	rejectedAt  = time.Date(2026, 11, 18, 14, 5, 40, 0, time.UTC)

	rejectedOrder = broker.Message{
		ID:            "4f1c2a7e",
		Queue:         "orders.dlq",
		Body:          []byte(`{"orderId":1042}`),
		ContentType:   "application/json",
		Timestamp:     publishedAt,
		DeliveryCount: 18,
		Headers:       map[string]any{"tenant": "acme"},
		Deaths: []broker.Death{
			{Queue: "orders", Reason: "rejected", Count: 3, Time: rejectedAt, Exchange: "shop", RoutingKeys: []string{"order.created"}},
		},
	}
)

func TestPeekText(t *testing.T) {
	hostile := broker.Message{Queue: "orders.dlq", Body: []byte("\x1b]0;pwned\x07 hello")}
	d := &fakeDriver{msgs: []broker.Message{rejectedOrder, hostile}}

	code, out, errOut := runWith(t, fakeBrokers(d), "peek", "orders.dlq", "--url", "fake://guest:s3cret@localhost", "-n", "5")
	if code != exitOK {
		t.Fatalf("exit code = %d, stderr:\n%s", code, errOut)
	}
	if d.queue != "orders.dlq" || d.limit != 5 || !d.closed {
		t.Errorf("driver got queue %q, limit %d, closed %v", d.queue, d.limit, d.closed)
	}

	for _, want := range []string{
		"orders.dlq  1/2",
		"4f1c2a7e",
		publishedAt.Local().Format(time.DateTime),
		"orders · rejected ×3",
		"order.created",
		"tenant=acme",
		`"orderId": 1042`,
		"]0;pwned hello",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout is missing %q:\n%s", want, out)
		}
	}
	if strings.ContainsAny(out, "\x1b\x07") {
		t.Errorf("stdout passes control characters from a message through: %q", out)
	}

	for _, want := range []string{"fake://guest:xxxxx@localhost", "up to 18 times", "Put 2 messages back on orders.dlq"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr is missing %q:\n%s", want, errOut)
		}
	}
	if strings.Contains(errOut, "s3cret") {
		t.Errorf("stderr leaks the password:\n%s", errOut)
	}
}

func TestPeekJSON(t *testing.T) {
	binary := broker.Message{Queue: "orders.dlq", Body: []byte{0xff, 0x00, 0x01}}
	d := &fakeDriver{msgs: []broker.Message{rejectedOrder, binary}}

	code, out, errOut := runWith(t, fakeBrokers(d), "peek", "orders.dlq", "--url", "fake://localhost", "-o", "json")
	if code != exitOK {
		t.Fatalf("exit code = %d, stderr:\n%s", code, errOut)
	}

	want := `{"id":"4f1c2a7e","queue":"orders.dlq","content_type":"application/json","timestamp":"2026-11-18T14:02:11Z",` +
		`"delivery_count":18,"headers":{"tenant":"acme"},"deaths":[{"queue":"orders","reason":"rejected","count":3,` +
		`"time":"2026-11-18T14:05:40Z","exchange":"shop","routing_keys":["order.created"]}],` +
		`"body":"{\"orderId\":1042}","body_encoding":"utf-8"}` + "\n" +
		`{"queue":"orders.dlq","body":"/wAB","body_encoding":"base64"}` + "\n"
	if out != want {
		t.Errorf("stdout =\n%s\nwant\n%s", out, want)
	}
}

func TestPeekEmptyQueue(t *testing.T) {
	t.Setenv("LIMBO_URL", "fake://localhost")

	code, out, errOut := runWith(t, fakeBrokers(&fakeDriver{}), "peek", "orders.dlq")
	if code != exitOK || out != "" || !strings.Contains(errOut, "orders.dlq is empty") {
		t.Errorf("exit code %d, stdout %q, stderr %q; want an empty-queue note", code, out, errOut)
	}
}

func TestPeekErrors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		peekErr error
		want    string
	}{
		{"no url", nil, nil, "pass --url or set LIMBO_URL"},
		{"unknown output", []string{"--url", "fake://x", "-o", "yaml"}, nil, `unknown output format "yaml"`},
		{"bad limit", []string{"--url", "fake://x", "-n", "0"}, nil, "limit must be at least 1"},
		{"unsupported scheme", []string{"--url", "kafka://x"}, nil, `unsupported broker URL scheme "kafka"`},
		{"driver error", []string{"--url", "fake://x"}, errors.New(`queue "q" not found`), `queue "q" not found`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("LIMBO_URL", "")

			args := append([]string{"peek", "q"}, tt.args...)
			code, _, errOut := runWith(t, fakeBrokers(&fakeDriver{err: tt.peekErr}), args...)
			if code != exitError || !strings.Contains(strings.ToLower(errOut), strings.ToLower(tt.want)) {
				t.Errorf("exit code %d, stderr:\n%s\nwant exit %d and %q", code, errOut, exitError, tt.want)
			}
		})
	}
}
