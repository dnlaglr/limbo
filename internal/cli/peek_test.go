package cli

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dnlaglr/limbo/internal/broker"
)

type fakeDriver struct {
	msgs    []broker.Message
	err     error
	depths  map[string]int
	queue   string
	n       int
	visited int
	closed  bool
}

func browsing(msgs ...broker.Message) *fakeDriver {
	return &fakeDriver{msgs: msgs, depths: map[string]int{"orders.dlq": len(msgs)}}
}

func (f *fakeDriver) Browse(_ context.Context, queue string, n int, visit func(broker.Message) bool) error {
	f.queue, f.n = queue, n
	for _, m := range f.msgs[:min(n, len(f.msgs))] {
		f.visited++
		if !visit(m) {
			break
		}
	}

	return f.err
}

func (f *fakeDriver) Depth(_ context.Context, queue string) (int, error) {
	n, ok := f.depths[queue]
	if !ok {
		return 0, fmt.Errorf("queue %q not found", queue)
	}

	return n, nil
}

func (f *fakeDriver) Close() error {
	f.closed = true
	return nil
}

func fakeBrokers(d broker.Driver) broker.Registry {
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
		Properties:    map[string]any{"app_id": "shop"},
		Deaths: []broker.Death{
			{Queue: "orders", Reason: "rejected", Count: 3, Time: rejectedAt, Exchange: "shop", RoutingKeys: []string{"order.created"}},
		},
	}

	expiredPayment = broker.Message{
		ID:     "9a0b1c2d",
		Queue:  "orders.dlq",
		Deaths: []broker.Death{{Queue: "payments", Reason: "expired", Count: 1, Time: rejectedAt.Add(-time.Hour)}},
	}
)

const rejectedOrderJSON = `{"id":"4f1c2a7e","queue":"orders.dlq","content_type":"application/json","timestamp":"2026-11-18T14:02:11Z",` +
	`"delivery_count":18,"headers":{"tenant":"acme"},"properties":{"app_id":"shop"},"deaths":[{"queue":"orders","reason":"rejected","count":3,` +
	`"time":"2026-11-18T14:05:40Z","exchange":"shop","routing_keys":["order.created"]}],` +
	`"body":"{\"orderId\":1042}","body_encoding":"utf-8"}`

func TestPeekText(t *testing.T) {
	hostile := broker.Message{Queue: "orders.dlq", Body: []byte("\x1b]0;pwned\x07 hello")}
	d := browsing(rejectedOrder, hostile)

	code, out, errOut := runWith(t, fakeBrokers(d), "peek", "orders.dlq", "--url", "fake://guest:s3cret@localhost", "-n", "5")
	if code != exitOK {
		t.Fatalf("exit code = %d, stderr:\n%s", code, errOut)
	}
	if d.queue != "orders.dlq" || d.visited != 2 || !d.closed {
		t.Errorf("driver got queue %q, visited %d, closed %v", d.queue, d.visited, d.closed)
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

	for _, want := range []string{"Connecting to fake://localhost", "up to 18 times", "Put 2 messages back on orders.dlq"} {
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
	d := browsing(rejectedOrder, binary)

	code, out, errOut := runWith(t, fakeBrokers(d), "peek", "orders.dlq", "--url", "fake://localhost", "-o", "json")
	if code != exitOK {
		t.Fatalf("exit code = %d, stderr:\n%s", code, errOut)
	}

	want := rejectedOrderJSON + "\n" + `{"queue":"orders.dlq","body":"/wAB","body_encoding":"base64"}` + "\n"
	if out != want {
		t.Errorf("stdout =\n%s\nwant\n%s", out, want)
	}
}

func TestPeekEmptyQueue(t *testing.T) {
	t.Setenv("LIMBO_URL", "fake://localhost")

	code, out, errOut := runWith(t, fakeBrokers(browsing()), "peek", "orders.dlq")
	if code != exitOK || out != "" || !strings.Contains(errOut, "orders.dlq is empty") {
		t.Errorf("exit code %d, stdout %q, stderr %q; want an empty-queue note", code, out, errOut)
	}
}

func TestPeekFiltered(t *testing.T) {
	d := browsing(orphan, expiredPayment, rejectedOrder, rejectedOrder)

	code, out, errOut := runWith(t, fakeBrokers(d), "peek", "orders.dlq", "--url", "fake://x", "--reason", "rejected", "--not-from", "payments", "-n", "1")
	if code != exitOK {
		t.Fatalf("exit code = %d, stderr:\n%s", code, errOut)
	}
	if d.n != 4 || d.visited != 3 {
		t.Errorf("visited %d of up to %d messages, want to stop at the first match, the third of 4", d.visited, d.n)
	}
	if !strings.Contains(out, "orders.dlq  1/1") || !strings.Contains(out, rejectedOrder.ID) || strings.Contains(out, orphan.ID) {
		t.Errorf("stdout shows the wrong messages:\n%s", out)
	}
	for _, want := range []string{"1 of 3 messages matched", "Put 3 messages back on orders.dlq"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr is missing %q:\n%s", want, errOut)
		}
	}
}

func TestPeekByID(t *testing.T) {
	d := browsing(rejectedOrder, expiredPayment, orphan)

	code, out, errOut := runWith(t, fakeBrokers(d), "peek", "orders.dlq", "--url", "fake://x", "--id", expiredPayment.ID)
	if code != exitOK || !strings.Contains(out, "orders.dlq  1/1") || !strings.Contains(out, expiredPayment.ID) || strings.Contains(out, rejectedOrder.ID) {
		t.Errorf("exit code %d, stdout:\n%s\nwant only %s; stderr:\n%s", code, out, expiredPayment.ID, errOut)
	}
}

func TestPeekErrors(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		browseErr error
		want      string
	}{
		{"no url", nil, nil, "pass --url or set LIMBO_URL"},
		{"unknown output", []string{"--url", "fake://x", "-o", "yaml"}, nil, `unknown output format "yaml"`},
		{"bad limit", []string{"--url", "fake://x", "-n", "0"}, nil, "limit must be at least 1"},
		{"unsupported scheme", []string{"--url", "kafka://x"}, nil, `unsupported broker URL scheme "kafka"`},
		{"driver error", []string{"--url", "fake://x"}, errors.New("channel closed"), "channel closed"},
		{"bad regex", []string{"--url", "fake://x", "--body", "("}, nil, `invalid argument "(" for "--body" flag: error parsing regexp`},
		{"bad age", []string{"--url", "fake://x", "--older-than", "3w"}, nil, "use a positive duration such as 30m, 2h or 7d"},
		{"nameless header", []string{"--url", "fake://x", "--not-header", "=acme"}, nil, "name the header, as key or key=value"},
		{"empty id", []string{"--url", "fake://x", "--id="}, nil, "an ID can't be empty"},
		{"empty not-id", []string{"--url", "fake://x", "--not-id", ""}, nil, "an ID can't be empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("LIMBO_URL", "")

			args := append([]string{"peek", "q"}, tt.args...)
			code, _, errOut := runWith(t, fakeBrokers(&fakeDriver{depths: map[string]int{"q": 1}, err: tt.browseErr}), args...)
			if code != exitError || !strings.Contains(strings.ToLower(errOut), strings.ToLower(tt.want)) {
				t.Errorf("exit code %d, stderr:\n%s\nwant exit %d and %q", code, errOut, exitError, tt.want)
			}
		})
	}
}

func TestConnectHidesSecrets(t *testing.T) {
	tests := []struct{ url, want string }{
		{"fake://user:s3cret@host:1/vh?endpoint=http://k:s3cret@e", "Connecting to fake://host:1/vh"},
		{"fake://u:s3cret@host:5672/%2F", "Connecting to fake://host:5672/%2F"},
		{"fake://?endpoint=http://k:s3cret@e", "Connecting to fake://"},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			_, _, errOut := runWith(t, fakeBrokers(&fakeDriver{depths: map[string]int{"q": 0}}), "peek", "q", "--url", tt.url)
			if !strings.Contains(errOut, tt.want) || strings.Contains(errOut, "s3cret") || strings.Contains(errOut, "endpoint") {
				t.Errorf("stderr:\n%s\nwant %q and no secret or query", errOut, tt.want)
			}
		})
	}
}
