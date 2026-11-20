package cli

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/dnlaglr/limbo/internal/broker"
)

type fakeReplayer struct {
	fakeDriver
	results   []broker.Replayed
	replayErr error
	opts      *broker.ReplayOptions
}

func (f *fakeReplayer) Replay(_ context.Context, queue string, opts broker.ReplayOptions, each func(broker.Replayed)) error {
	f.queue, f.opts = queue, &opts
	for _, r := range f.results {
		each(r)
	}

	return f.replayErr
}

func replayer(depth int, results ...broker.Replayed) *fakeReplayer {
	return &fakeReplayer{fakeDriver: fakeDriver{depths: map[string]int{"orders.dlq": depth, "orders.v2": 0}}, results: results}
}

var orphan = broker.Message{ID: "7d3e9b01", Queue: "orders.dlq"}

func TestReplayText(t *testing.T) {
	d := replayer(9,
		broker.Replayed{Message: rejectedOrder, To: "orders"},
		broker.Replayed{Message: rejectedOrder, To: "orders"},
		broker.Replayed{Message: rejectedOrder, To: "pay\x1b[2Jments"},
		broker.Replayed{Message: orphan, Err: broker.ErrNoOrigin},
	)

	code, out, errOut := runWith(t, fakeBrokers(d), "replay", "orders.dlq", "--url", "fake://x", "--yes", "-n", "4")
	if code != exitError {
		t.Fatalf("exit code = %d, want %d for a partial replay; stderr:\n%s", code, exitError, errOut)
	}
	if d.opts == nil || !reflect.DeepEqual(*d.opts, broker.ReplayOptions{Selection: broker.Selection{Limit: 4, Scan: 4}}) || d.queue != "orders.dlq" || !d.closed {
		t.Errorf("driver got queue %q, options %+v, closed %v", d.queue, d.opts, d.closed)
	}

	for _, want := range []string{"Replayed", "orders ×2", "pay[2Jments ×1", "Returned", broker.ErrNoOrigin.Error() + " ×1"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "\x1b") || strings.Contains(out, "Not reached") {
		t.Errorf("stdout has control characters or unreached messages:\n%q", out)
	}
	for _, want := range []string{"replayed 3 messages", "left 1 message on orders.dlq that could not be replayed"} {
		if !strings.Contains(strings.ToLower(errOut), want) {
			t.Errorf("stderr is missing %q:\n%s", want, errOut)
		}
	}
}

func TestReplayJSON(t *testing.T) {
	d := replayer(2,
		broker.Replayed{Message: rejectedOrder, To: "orders"},
		broker.Replayed{Message: orphan, To: "orders.v1", Err: errors.New(`no queue named "orders.v1"`)},
	)

	code, out, _ := runWith(t, fakeBrokers(d), "replay", "orders.dlq", "--url", "fake://x", "-y", "-o", "json")
	want := `{"id":"4f1c2a7e","to":"orders","status":"replayed"}` + "\n" +
		`{"id":"7d3e9b01","to":"orders.v1","status":"returned","error":"no queue named \"orders.v1\""}` + "\n"
	if code != exitError || out != want {
		t.Errorf("exit code %d, stdout =\n%s\nwant exit %d and\n%s", code, out, exitError, want)
	}
}

func TestReplayNeedsConfirmation(t *testing.T) {
	d := replayer(3)

	code, _, errOut := runWith(t, fakeBrokers(d), "replay", "orders.dlq", "--url", "fake://x")
	if code != exitError || !strings.Contains(strings.ToLower(errOut), "refusing to replay without confirmation: pass --yes, or --dry-run to preview") {
		t.Errorf("exit code %d, stderr:\n%s\nwant a refusal without a terminal", code, errOut)
	}
	if d.opts != nil {
		t.Error("replayed without confirmation")
	}
}

func TestReplayDryRun(t *testing.T) {
	d := replayer(3)
	d.msgs = []broker.Message{rejectedOrder, orphan, expiredPayment}

	code, out, errOut := runWith(t, fakeBrokers(d), "replay", "orders.dlq", "--url", "fake://x", "--dry-run", "--not-reason", "expired")
	if code != exitOK || d.opts != nil || d.n != 3 || d.visited != 3 {
		t.Fatalf("exit code %d, replayed %v, read %d of %d; want a read of 3 and no replay; stderr:\n%s", code, d.opts != nil, d.visited, d.n, errOut)
	}

	for _, want := range []string{"orders.dlq  dry run", "Would replay", "orders ×1", "Would return", broker.ErrNoOrigin.Error() + " ×1", "Skipped", "1 message"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout is missing %q:\n%s", want, out)
		}
	}
}

func TestReplayFiltered(t *testing.T) {
	d := replayer(3,
		broker.Replayed{Message: orphan, Err: broker.ErrSkipped},
		broker.Replayed{Message: rejectedOrder, To: "orders"},
		broker.Replayed{Message: expiredPayment, Err: broker.ErrSkipped},
	)

	code, out, errOut := runWith(t, fakeBrokers(d), "replay", "orders.dlq", "--url", "fake://x", "-y", "--from", "orders", "-o", "json")
	if code != exitOK {
		t.Fatalf("exit code = %d, want %d when messages were only skipped; stderr:\n%s", code, exitOK, errOut)
	}
	if d.visited != 0 || d.opts == nil || d.opts.Scan != 3 || d.opts.Limit != 0 || len(d.opts.Filter.Sources.Any) != 1 {
		t.Errorf("read %d messages first and replayed with %+v; want one filtered pass over all 3", d.visited, d.opts)
	}
	if want := `{"id":"4f1c2a7e","to":"orders","status":"replayed"}` + "\n"; out != want {
		t.Errorf("stdout =\n%s\nwant only the replayed message\n%s", out, want)
	}
}

func TestReplayAllSkipped(t *testing.T) {
	d := replayer(2, broker.Replayed{Message: orphan, Err: broker.ErrSkipped}, broker.Replayed{Message: expiredPayment, Err: broker.ErrSkipped})

	code, out, errOut := runWith(t, fakeBrokers(d), "replay", "orders.dlq", "--url", "fake://x", "--yes", "--reason", "rejected")
	if code != exitOK || !strings.Contains(out, "Skipped") || !strings.Contains(out, "2 messages") || strings.Contains(out, "Not reached") {
		t.Errorf("exit code %d, stdout:\n%s\nwant exit 0 and 2 skipped messages; stderr:\n%s", code, out, errOut)
	}
}

func TestReplayInterrupted(t *testing.T) {
	d := replayer(5, broker.Replayed{Message: rejectedOrder, To: "orders"})
	d.replayErr = context.Canceled

	code, out, errOut := runWith(t, fakeBrokers(d), "replay", "orders.dlq", "--url", "fake://x", "--yes")
	if code != exitCanceled {
		t.Fatalf("exit code = %d, want %d; stderr:\n%s", code, exitCanceled, errOut)
	}
	for _, want := range []string{"orders ×1", "Not reached", "4 messages"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout is missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(errOut, "in flight") {
		t.Errorf("a clean interruption warns about duplicates:\n%s", errOut)
	}
}

func TestReplayErrors(t *testing.T) {
	lost := replayer(3, broker.Replayed{Message: rejectedOrder, To: "orders"})
	lost.replayErr = errors.New("connection reset by peer")

	tests := []struct {
		name   string
		args   []string
		driver broker.Driver
		want   string
	}{
		{"to is the queue", []string{"--to", "orders.dlq"}, replayer(3), "cannot replay a queue into itself"},
		{"negative limit", []string{"--limit=-1"}, replayer(3), "limit must be 0 (all) or more"},
		{"negative rate", []string{"--rate=-1"}, replayer(3), "rate must be 0 (unlimited) or more"},
		{"missing destination", []string{"--to", "orders.v3"}, replayer(3), `queue "orders.v3" not found`},
		{"unsupported broker", nil, &fakeDriver{}, "replay is not supported for fake:// brokers"},
		{"connection lost", nil, lost, "may also have reached its destination"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"replay", "orders.dlq", "--url", "fake://x", "--yes"}, tt.args...)
			code, _, errOut := runWith(t, fakeBrokers(tt.driver), args...)
			if code != exitError || !strings.Contains(strings.ToLower(errOut), strings.ToLower(tt.want)) {
				t.Errorf("exit code %d, stderr:\n%s\nwant exit %d and %q", code, errOut, exitError, tt.want)
			}
		})
	}
}
