package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dnlaglr/limbo/internal/broker"
)

// Writes msgs the way export does and returns the file's path
func exportFile(t *testing.T, msgs ...broker.Message) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x.ndjson")
	f, err := createFile(path, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if err := f.write(m); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.close(); err != nil {
		t.Fatal(err)
	}

	return path
}

func writeLines(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "x.ndjson")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	return path
}

var payment = diedIn("p1", "payments")

func TestImportJSON(t *testing.T) {
	d := replayer(0)
	path := exportFile(t, rejectedOrder, orphan, payment)

	code, out, errOut := runWith(t, fakeBrokers(d), "import", path, "--url", "fake://x", "-y", "-o", "json")
	want := `{"line":1,"id":"4f1c2a7e","to":"orders","status":"imported"}` + "\n" +
		`{"line":2,"id":"7d3e9b01","status":"failed","error":"no dead-letter history, pass --to to choose a destination"}` + "\n" +
		`{"line":3,"id":"p1","to":"payments","status":"imported"}` + "\n"
	if code != exitError || out != want || !slices.Equal(d.published, []string{"4f1c2a7e>orders", "p1>payments"}) {
		t.Errorf("exit code %d, published %q, stdout:\n%s\nwant exit %d, two published and\n%s\nstderr:\n%s", code, d.published, out, exitError, want, errOut)
	}
	if !strings.Contains(strings.ToLower(errOut), "could not import 1 message from") || !strings.Contains(squash(errOut), squash(path+": line 2")) {
		t.Errorf("stderr is missing the failure and its line:\n%s", errOut)
	}
}

// Text and quiet output carry no records, so the error itself must name the stop line and every line left behind
func TestImportFailedLines(t *testing.T) {
	d := replayer(0)
	d.refuse = map[string]error{"payments": errors.New("throttled"), "slow": fmt.Errorf("no confirm: %w", context.DeadlineExceeded)}
	path := exportFile(t, rejectedOrder, payment, diedIn("s1", "slow"))

	code, _, errOut := runWith(t, fakeBrokers(d), "-q", "import", path, "--url", "fake://x", "-y")
	if code != exitError || !slices.Equal(d.published, []string{"4f1c2a7e>orders"}) {
		t.Fatalf("exit code %d, published %q; stderr:\n%s", code, d.published, errOut)
	}
	for _, want := range []string{"stopped after line 2 of", "--skip 2", "could not import 1 message from", squash(path + ": line 2")} {
		if !strings.Contains(strings.ToLower(squash(errOut)), strings.ToLower(squash(want))) {
			t.Errorf("stderr is missing %q:\n%s", want, errOut)
		}
	}
}

func TestImportTo(t *testing.T) {
	d := replayer(0)

	code, out, errOut := runWith(t, fakeBrokers(d), "import", exportFile(t, rejectedOrder, orphan, payment), "--url", "fake://x", "--to", "orders.v2", "-y")
	if code != exitOK || !slices.Equal(d.published, []string{"4f1c2a7e>orders.v2", "7d3e9b01>orders.v2", "p1>orders.v2"}) {
		t.Fatalf("exit code %d, published %q; stderr:\n%s", code, d.published, errOut)
	}
	for _, want := range []string{"Imported", "orders.v2 ×3"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout is missing %q:\n%s", want, out)
		}
	}
	if !strings.Contains(errOut, "Imported 3 messages") {
		t.Errorf("stderr is missing the summary:\n%s", errOut)
	}
}

func TestImportBadFile(t *testing.T) {
	good := `{"version":1,` + rejectedOrderJSON[1:]
	tests := []struct {
		name  string
		lines []string
		want  []string
	}{
		{"not json", []string{good, "not json"}, []string{"line 2", "isn't a limbo export record"}},
		{"newer version", []string{`{"version":2,"queue":"q","body":"x","body_encoding":"utf-8"}`}, []string{"line 1", "has version 2"}},
		{"peek output", []string{rejectedOrderJSON}, []string{"line 1", "isn't a limbo export record"}},
		{"bad body", []string{good, `{"version":1,"queue":"q","body":"x","body_encoding":"rot13"}`}, []string{"line 2", "body_encoding"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := replayer(0)
			path := writeLines(t, tt.lines...)

			code, _, errOut := runWith(t, fakeBrokers(d), "import", path, "--url", "fake://x", "-y")
			if code != exitError || len(d.published) != 0 || !strings.Contains(squash(errOut), squash(path)) {
				t.Fatalf("exit code %d, published %q, stderr:\n%s\nwant an error naming %s before anything is published", code, d.published, errOut, path)
			}
			for _, want := range tt.want {
				if !strings.Contains(strings.ToLower(squash(errOut)), squash(want)) {
					t.Errorf("stderr is missing %q:\n%s", want, errOut)
				}
			}
		})
	}
}

func TestImportSkip(t *testing.T) {
	d := replayer(0)

	_, out, errOut := runWith(t, fakeBrokers(d), "import", exportFile(t, rejectedOrder, orphan, payment), "--url", "fake://x", "--skip", "1", "-y", "-o", "json")
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], `{"line":2,`) || !strings.HasPrefix(lines[1], `{"line":3,`) || !slices.Equal(d.published, []string{"p1>payments"}) {
		t.Errorf("published %q, stdout:\n%s\nwant lines 2 and 3 only; stderr:\n%s", d.published, out, errOut)
	}
}

func TestImportResume(t *testing.T) {
	d := replayer(0)
	d.refuse = map[string]error{"payments": fmt.Errorf("no confirm: %w", context.DeadlineExceeded)}

	code, _, errOut := runWith(t, fakeBrokers(d), "import", exportFile(t, rejectedOrder, orphan, payment), "--url", "fake://x", "-y")
	if code != exitError || !slices.Equal(d.published, []string{"4f1c2a7e>orders"}) {
		t.Fatalf("exit code %d, published %q; stderr:\n%s", code, d.published, errOut)
	}
	for _, want := range []string{"stopped after line 2 of", "--skip 2"} {
		if !strings.Contains(strings.ToLower(squash(errOut)), squash(want)) {
			t.Errorf("stderr is missing %q, so the unconfirmed message on line 3 would not be retried:\n%s", want, errOut)
		}
	}
}

func TestImportDryRun(t *testing.T) {
	d := replayer(0)

	code, out, errOut := runWith(t, fakeBrokers(d), "import", exportFile(t, rejectedOrder, orphan, payment), "--url", "fake://x", "--dry-run", "--from", "orders")
	if code != exitOK || len(d.published) != 0 {
		t.Fatalf("exit code %d, published %q; stderr:\n%s", code, d.published, errOut)
	}
	for _, want := range []string{"x.ndjson  dry run", "Would import", "orders ×1", "Skipped", "2 messages"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout is missing %q:\n%s", want, out)
		}
	}
}

func TestImportFileFormats(t *testing.T) {
	large := diedIn("large", "orders")
	large.Body = bytes.Repeat([]byte("a"), 100_000)
	crlf, bom := exportFile(t, rejectedOrder, payment), exportFile(t, rejectedOrder)
	lf, err := os.ReadFile(crlf)
	if err == nil {
		err = os.WriteFile(crlf, bytes.ReplaceAll(lf, []byte("\n"), []byte("\r\n")), 0o600)
	}
	if b, readErr := os.ReadFile(bom); err == nil {
		err = errors.Join(readErr, os.WriteFile(bom, append([]byte("\xef\xbb\xbf"), b...), 0o600))
	}
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		path string
		want []string
	}{
		{"windows line endings", crlf, []string{"4f1c2a7e>orders", "p1>payments"}},
		{"byte order mark", bom, []string{"4f1c2a7e>orders"}},
		{"line over 64 KiB", exportFile(t, large), []string{"large>orders"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := replayer(0)

			code, _, errOut := runWith(t, fakeBrokers(d), "import", tt.path, "--url", "fake://x", "-y")
			if code != exitOK || !slices.Equal(d.published, tt.want) {
				t.Errorf("exit code %d, published %q, want %q; stderr:\n%s", code, d.published, tt.want, errOut)
			}
		})
	}
}

func TestImportNeedsConfirmation(t *testing.T) {
	d := replayer(0)

	code, _, errOut := runWith(t, fakeBrokers(d), "import", exportFile(t, rejectedOrder), "--url", "fake://x")
	if code != exitError || !strings.Contains(strings.ToLower(errOut), "refusing to import without confirmation") || len(d.published) != 0 {
		t.Errorf("exit code %d, published %q, stderr:\n%s\nwant a refusal without a terminal", code, d.published, errOut)
	}
}

func TestImportErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.ndjson")
	tests := []struct {
		name   string
		path   string
		args   []string
		driver broker.Driver
		want   string
	}{
		{"missing destination", "", []string{"--to", "orders.v3"}, replayer(0), `queue "orders.v3" not found`},
		{"unsupported broker", "", nil, &fakeDriver{}, "import is not supported for fake:// brokers"},
		{"negative skip", "", []string{"--skip=-1"}, replayer(0), "skip must be 0 or more"},
		{"missing file", missing, nil, replayer(0), missing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.path
			if path == "" {
				path = exportFile(t, rejectedOrder)
			}

			args := append([]string{"import", path, "--url", "fake://x", "--yes"}, tt.args...)
			code, _, errOut := runWith(t, fakeBrokers(tt.driver), args...)
			if code != exitError || !strings.Contains(squash(strings.ToLower(errOut)), squash(strings.ToLower(tt.want))) {
				t.Errorf("exit code %d, stderr:\n%s\nwant exit %d and %q", code, errOut, exitError, tt.want)
			}
		})
	}
}

func TestImportEmpty(t *testing.T) {
	tests := []struct {
		name string
		path string
		args []string
		want string
	}{
		{"no messages", writeLines(t, ""), nil, "holds no messages"},
		{"all skipped", exportFile(t, rejectedOrder, orphan, payment), []string{"--skip", "3"}, "holds no messages after line 3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := replayer(0)

			args := append([]string{"import", tt.path, "--url", "fake://x", "-y"}, tt.args...)
			code, _, errOut := runWith(t, fakeBrokers(d), args...)
			if code != exitOK || !strings.Contains(errOut, tt.want) || len(d.published) != 0 {
				t.Errorf("exit code %d, published %q, stderr:\n%s\nwant exit 0 and %q", code, d.published, errOut, tt.want)
			}
		})
	}
}
