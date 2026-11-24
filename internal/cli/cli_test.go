package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/broker"
)

// Executes limbo with args against in-memory writers
func run(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	return runWith(t, nil, args...)
}

func runWith(t *testing.T, brokers broker.Registry, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	t.Setenv("NO_COLOR", "")

	var out, errOut bytes.Buffer
	build := app.BuildInfo{Version: "v1.2.3", Commit: "0123456789abcdef", Date: "2026-11-18"}
	a := app.New(build, &out, &errOut)
	if brokers != nil {
		a.Brokers = brokers
	}
	code = Execute(context.Background(), a, args)

	return code, out.String(), errOut.String()
}

// Fang wraps error text to fit the terminal, at a space or inside a word longer than a line, such as a temp path, so messages compare with all whitespace removed
func squash(s string) string {
	return strings.Join(strings.Fields(s), "")
}

func TestVersion(t *testing.T) {
	code, out, _ := run(t, "version")
	if code != exitOK {
		t.Fatalf("exit code = %d, want %d", code, exitOK)
	}
	for _, want := range []string{"limbo", "v1.2.3", "0123456", "2026-11-18"} {
		if !strings.Contains(out, want) {
			t.Errorf("version output is missing %q:\n%s", want, out)
		}
	}
}

func TestNoArgsShown(t *testing.T) {
	code, out, _ := run(t)
	if code != exitOK {
		t.Fatalf("exit code = %d, want %d", code, exitOK)
	}
	if !strings.Contains(out, "version") {
		t.Errorf("help does not list the version command:\n%s", out)
	}
}

func TestUnknownCommand(t *testing.T) {
	code, _, errOut := run(t, "nope")
	if code != exitError {
		t.Fatalf("exit code = %d, want %d", code, exitError)
	}
	if !strings.Contains(strings.ToLower(errOut), `unknown command "nope"`) {
		t.Errorf("stderr = %q, want an unknown command error", errOut)
	}
}

func TestDebugLogs(t *testing.T) {
	_, _, errOut := run(t, "--debug", "version")
	if !strings.Contains(errOut, "level=DEBUG") {
		t.Errorf("stderr = %q, want a debug log line", errOut)
	}
}

func TestNoColorSetsEnv(t *testing.T) {
	run(t, "--no-color", "version")
	if got := os.Getenv("NO_COLOR"); got != "1" {
		t.Errorf("NO_COLOR = %q, want %q", got, "1")
	}
}

func TestInterruptShowsJoinedErrors(t *testing.T) {
	tests := []struct {
		name, want, hidden string
		err                error
	}{
		{"joined", "could not return 2 of the held messages", "context canceled", errors.Join(context.Canceled, errors.New("could not return 2 of the held messages"))},
		{"wrapped join", "could not return 2 of the held messages", "context canceled", errors.Join(context.Canceled, fmt.Errorf("could not return 2 of the held messages: %w", errors.Join(errors.New("status 503"), errors.New("retry quota exceeded"))))},
		{"bare", "", "context canceled", context.Canceled},
		{"joined with nil", "", "context canceled", errors.Join(context.Canceled, nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := browsing(rejectedOrder)
			d.depths["orders.dlq"], d.err = 3, tt.err

			code, _, errOut := runWith(t, fakeBrokers(d), "stats", "orders.dlq", "--url", "fake://x")
			if code != exitCanceled || !strings.Contains(strings.ToLower(errOut), tt.want) || strings.Contains(strings.ToLower(errOut), tt.hidden) {
				t.Errorf("exit code %d, stderr:\n%s\nwant exit %d, %q and no %q", code, errOut, exitCanceled, tt.want, tt.hidden)
			}
		})
	}
}
