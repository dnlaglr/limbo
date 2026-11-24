package cli

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dnlaglr/limbo/internal/broker"
)

func TestDefaultFile(t *testing.T) {
	if got, want := defaultFile("a/b:c é", time.Date(2026, 11, 22, 10, 15, 0, 0, time.UTC)), "a_b_c__-20261122T101500Z.ndjson"; got != want {
		t.Errorf("defaultFile() = %q, want %q", got, want)
	}
}

func TestExport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.ndjson")
	binary := broker.Message{ID: "bin", Queue: "orders.dlq", Body: []byte{0xff, 0x00}}
	d := browsing(rejectedOrder, orphan, binary)

	code, out, errOut := runWith(t, fakeBrokers(d), "export", "orders.dlq", "--url", "fake://x", "--file", path)
	if code != exitOK || out != "" {
		t.Fatalf("exit code %d, stdout %q, stderr:\n%s", code, out, errOut)
	}

	lines := readLines(t, path)
	if len(lines) != 3 || lines[0] != `{"version":1,`+rejectedOrderJSON[1:] || !strings.Contains(lines[2], `"body":"/wA=","body_encoding":"base64"`) {
		t.Errorf("file holds:\n%s\nwant 3 versioned records, the binary body in base64", strings.Join(lines, "\n"))
	}
	if info, err := os.Stat(path); err != nil || runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Errorf("stat = %v, %v; want mode 0600", info, err)
	}
	for _, want := range []string{"Exported 3 messages to " + path, "Put 3 messages back on orders.dlq"} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr is missing %q:\n%s", want, errOut)
		}
	}
}

func TestExportFiltered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.ndjson")

	code, _, errOut := runWith(t, fakeBrokers(browsing(orphan, rejectedOrder, expiredPayment)), "export", "orders.dlq", "--url", "fake://x", "--file", path, "--reason", "rejected")
	if lines := readLines(t, path); code != exitOK || len(lines) != 1 || !strings.Contains(errOut, "Exported 1 of 3 messages to "+path) {
		t.Errorf("exit code %d, %d lines, stderr:\n%s\nwant the one rejected message", code, len(lines), errOut)
	}
}

func TestExportNeverOverwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.ndjson")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	d := browsing(rejectedOrder)

	code, _, errOut := runWith(t, fakeBrokers(d), "export", "orders.dlq", "--url", "fake://x", "--file", path)
	if got, _ := os.ReadFile(path); code != exitError || !strings.Contains(errOut, "already exists") || string(got) != "old" || d.visited != 0 {
		t.Errorf("exit code %d, file %q, read %d; stderr:\n%s\nwant a refusal that leaves the file alone", code, got, d.visited, errOut)
	}
}

func TestExportNothingMatched(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.ndjson")

	code, _, errOut := runWith(t, fakeBrokers(browsing(rejectedOrder)), "export", "orders.dlq", "--url", "fake://x", "--file", path, "--reason", "maxlen")
	if _, err := os.Stat(path); code != exitOK || !errors.Is(err, fs.ErrNotExist) || !strings.Contains(errOut, "No messages in orders.dlq matched") {
		t.Errorf("exit code %d, stat err %v, stderr:\n%s\nwant no file and a note", code, err, errOut)
	}
}

func TestExportStdout(t *testing.T) {
	code, out, errOut := runWith(t, fakeBrokers(browsing(rejectedOrder, orphan)), "export", "orders.dlq", "--url", "fake://x", "--file", "-")
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if code != exitOK || len(lines) != 2 || !strings.HasPrefix(lines[1], `{"version":1,"id":"7d3e9b01"`) || !strings.Contains(errOut, "Exported 2 messages to stdout") {
		t.Errorf("exit code %d, stdout:\n%s\nstderr:\n%s", code, out, errOut)
	}
}

func TestExportDefaultName(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)

	code, _, errOut := runWith(t, fakeBrokers(browsing(rejectedOrder)), "export", "orders.dlq", "--url", "fake://x")
	entries, _ := os.ReadDir(dir)
	if code != exitOK || len(entries) != 1 || !regexp.MustCompile(`^orders\.dlq-\d{8}T\d{6}Z\.ndjson$`).MatchString(entries[0].Name()) {
		t.Errorf("exit code %d, files %v; stderr:\n%s\nwant one orders.dlq-<time>.ndjson", code, entries, errOut)
	}
}

type tornWriter struct{ f *os.File }

func (w tornWriter) Write(p []byte) (int, error) {
	n, _ := w.f.Write(p[:len(p)/2])
	return n, errors.New("no space left on device")
}

func TestFileCutsFailedLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.ndjson")
	f, err := createFile(path, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.write(rejectedOrder); err != nil {
		t.Fatal(err)
	}

	f.w = tornWriter{f.file}
	if err := f.write(orphan); err == nil {
		t.Fatal("write() = nil, want the disk error")
	}
	if err := f.close(); err != nil {
		t.Fatal(err)
	}
	if lines := readLines(t, path); len(lines) != 1 || lines[0] != `{"version":1,`+rejectedOrderJSON[1:] || f.n != 1 {
		t.Errorf("file holds %q after a torn write, counted %d; want only the first, complete line", lines, f.n)
	}
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}
