package cli

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/dnlaglr/limbo/internal/app"
)

// Executes limbo with args against in-memory writers
func run(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	t.Setenv("NO_COLOR", "")

	var out, errOut bytes.Buffer
	build := app.BuildInfo{Version: "v1.2.3", Commit: "0123456789abcdef", Date: "2026-11-18"}
	code = Execute(context.Background(), app.New(build, &out, &errOut), args)

	return code, out.String(), errOut.String()
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
