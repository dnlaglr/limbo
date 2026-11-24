package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/dnlaglr/limbo/internal/broker"
)

type fakePurger struct {
	fakeTaker
	purged   bool
	purgeErr error
}

func (f *fakePurger) Purge(context.Context, string) error {
	f.purged = f.purgeErr == nil
	return f.purgeErr
}

func taking(msgs ...broker.Message) *fakeTaker {
	return &fakeTaker{fakeDriver: fakeDriver{msgs: msgs, depths: map[string]int{"orders.dlq": len(msgs)}}}
}

// Runs in an empty folder so a test can check that no default backup was left behind
func inEmptyDir(t *testing.T) func() []os.DirEntry {
	t.Helper()
	dir := t.TempDir()
	t.Chdir(dir)

	return func() []os.DirEntry {
		entries, _ := os.ReadDir(dir)
		return entries
	}
}

func TestDeleteBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.ndjson")
	d := taking(rejectedOrder, orphan, expiredPayment)

	code, out, errOut := runWith(t, fakeBrokers(d), "delete", "orders.dlq", "--url", "fake://x", "--id", "4f1c2a7e", "--yes", "--backup", path)
	if code != exitOK || !slices.Equal(d.removed, []string{"4f1c2a7e"}) {
		t.Fatalf("exit code %d, removed %q; stderr:\n%s", code, d.removed, errOut)
	}
	if lines := readLines(t, path); len(lines) != 1 || lines[0] != `{"version":1,`+rejectedOrderJSON[1:] {
		t.Errorf("backup holds %q, want the deleted message's versioned record", lines)
	}
	if info, err := os.Stat(path); err != nil || runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Errorf("stat = %v, %v; want mode 0600", info, err)
	}
	for _, want := range []string{"Deleted", "1 message", "Skipped", "2 messages"} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout is missing %q:\n%s", want, out)
		}
	}
	for _, want := range []string{"Deleted 1 message", "Backup: " + path} {
		if !strings.Contains(errOut, want) {
			t.Errorf("stderr is missing %q:\n%s", want, errOut)
		}
	}
}

func TestDeleteNeedsSelector(t *testing.T) {
	files := inEmptyDir(t)

	code, _, errOut := runWith(t, fakeBrokers(taking(rejectedOrder)), "delete", "orders.dlq", "--url", "fake://x", "--yes")
	if code != exitError || !strings.Contains(strings.ToLower(errOut), "or use limbo purge to remove every message") || len(files()) != 0 {
		t.Errorf("exit code %d, files %v, stderr:\n%s\nwant a refusal and no backup", code, files(), errOut)
	}
}

func TestDeleteNeedsConfirmation(t *testing.T) {
	files := inEmptyDir(t)
	d := taking(rejectedOrder)

	code, _, errOut := runWith(t, fakeBrokers(d), "delete", "orders.dlq", "--url", "fake://x", "--id", "4f1c2a7e")
	if code != exitError || !strings.Contains(strings.ToLower(errOut), "refusing to delete without confirmation") || d.took != 0 || len(files()) != 0 {
		t.Errorf("exit code %d, took %d, files %v, stderr:\n%s\nwant a refusal without a terminal and no backup left", code, d.took, files(), errOut)
	}
}

func TestDeleteDryRun(t *testing.T) {
	files := inEmptyDir(t)
	d := taking(rejectedOrder, orphan, expiredPayment)

	code, out, errOut := runWith(t, fakeBrokers(d), "delete", "orders.dlq", "--url", "fake://x", "--dry-run", "--reason", "rejected")
	if code != exitOK || d.took != 0 || len(files()) != 0 || !strings.Contains(out, "Would delete") || !strings.Contains(out, "1 message") {
		t.Errorf("exit code %d, took %d, files %v, stdout:\n%s\nstderr:\n%s", code, d.took, files(), out, errOut)
	}
}

func TestDeleteNoBackupJSON(t *testing.T) {
	files := inEmptyDir(t)

	code, out, errOut := runWith(t, fakeBrokers(taking(rejectedOrder, orphan, expiredPayment)), "delete", "orders.dlq", "--url", "fake://x", "--no-backup", "-n", "2", "--yes", "-o", "json")
	want := `{"id":"4f1c2a7e","status":"deleted"}` + "\n" + `{"id":"7d3e9b01","status":"deleted"}` + "\n"
	if code != exitOK || out != want || len(files()) != 0 {
		t.Errorf("exit code %d, files %v, stdout:\n%s\nwant\n%s\nstderr:\n%s", code, files(), out, want, errOut)
	}
}

func TestDeleteBackupUnavailable(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "b.ndjson")
	if err := os.WriteFile(existing, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct{ name, path, why string }{
		{"missing folder", filepath.Join(dir, "missing", "b.ndjson"), "no such file or directory"},
		{"existing file", existing, "already exists"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := taking(rejectedOrder)
			code, _, errOut := runWith(t, fakeBrokers(d), "delete", "orders.dlq", "--url", "fake://x", "--id", "4f1c2a7e", "--yes", "--backup", tt.path)
			if code != exitError || d.took != 0 || len(d.removed) != 0 || !strings.Contains(errOut, tt.path) || !strings.Contains(errOut, tt.why) {
				t.Errorf("exit code %d, took %d, removed %q; stderr:\n%s\nwant an error naming %s and %q before anything is read", code, d.took, d.removed, errOut, tt.path, tt.why)
			}
		})
	}
	if got, _ := os.ReadFile(existing); string(got) != "old" {
		t.Errorf("existing backup now holds %q", got)
	}
}

func TestDeleteInterrupted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.ndjson")
	d := taking(rejectedOrder)
	d.depths["orders.dlq"], d.takeErr = 3, context.Canceled

	code, out, errOut := runWith(t, fakeBrokers(d), "delete", "orders.dlq", "--url", "fake://x", "-n", "3", "--yes", "--backup", path)
	if code != exitCanceled || !strings.Contains(out, "Not reached") || strings.Contains(errOut, "stopped early") {
		t.Fatalf("exit code %d, stdout:\n%s\nstderr:\n%s\nwant exit %d and unreached messages", code, out, errOut, exitCanceled)
	}
	if lines := readLines(t, path); !slices.Equal(d.removed, []string{"4f1c2a7e"}) || len(lines) != 1 || !strings.HasPrefix(lines[0], `{"version":1,"id":"4f1c2a7e"`) {
		t.Errorf("removed %q, backup holds %q; want exactly the removed message", d.removed, lines)
	}
}

func TestDeleteErrors(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		driver broker.Driver
		want   string
	}{
		{"unsupported broker", nil, &fakeDriver{depths: map[string]int{"orders.dlq": 1}}, "delete is not supported for fake:// brokers"},
		{"backup and no backup", []string{"--backup", "b.ndjson", "--no-backup"}, taking(rejectedOrder), "[backup no-backup] were all set"},
		{"negative limit", []string{"--limit=-1"}, taking(rejectedOrder), "limit must be 0 (all) or more"},
		{"backup to stdout", []string{"--backup", "-"}, taking(rejectedOrder), "a backup must go to a file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files := inEmptyDir(t)
			args := append([]string{"delete", "orders.dlq", "--url", "fake://x", "--id", "4f1c2a7e", "--yes"}, tt.args...)
			code, _, errOut := runWith(t, fakeBrokers(tt.driver), args...)
			if code != exitError || !strings.Contains(strings.ToLower(errOut), strings.ToLower(tt.want)) || len(files()) != 0 {
				t.Errorf("exit code %d, files %v, stderr:\n%s\nwant exit %d and %q", code, files(), errOut, exitError, tt.want)
			}
		})
	}
}

func TestPurgeNative(t *testing.T) {
	for _, output := range []string{"text", "json"} {
		t.Run(output, func(t *testing.T) {
			files := inEmptyDir(t)
			d := &fakePurger{fakeTaker: *taking(rejectedOrder, orphan, expiredPayment)}

			code, out, errOut := runWith(t, fakeBrokers(d), "purge", "orders.dlq", "--url", "fake://x", "--no-backup", "--yes", "-o", output)
			if code != exitOK || !d.purged || d.took != 0 || len(files()) != 0 || !strings.Contains(errOut, "Purged orders.dlq, which held 3 messages") {
				t.Fatalf("exit code %d, purged %v, took %d, files %v, stderr:\n%s", code, d.purged, d.took, files(), errOut)
			}
			if want := `{"queue":"orders.dlq","status":"purged","depth":3}` + "\n"; output == "json" && out != want {
				t.Errorf("stdout = %q, want %q", out, want)
			}
			if output == "text" && (!strings.Contains(out, "Purged") || !strings.Contains(out, "3 messages")) {
				t.Errorf("stdout:\n%s\nwant 3 messages purged", out)
			}
		})
	}
}

func TestPurgeBackup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "b.ndjson")
	d := &fakePurger{fakeTaker: *taking(rejectedOrder, orphan, expiredPayment)}

	code, _, errOut := runWith(t, fakeBrokers(d), "purge", "orders.dlq", "--url", "fake://x", "--yes", "--backup", path)
	if lines := readLines(t, path); code != exitOK || d.purged || len(d.removed) != 3 || len(lines) != 3 || !strings.Contains(errOut, "Purged 3 messages") {
		t.Errorf("exit code %d, purged natively %v, removed %q, %d backup lines; stderr:\n%s\nwant each message backed up, then removed", code, d.purged, d.removed, len(lines), errOut)
	}
}

func TestPurgeNoBackupWithoutPurger(t *testing.T) {
	files := inEmptyDir(t)
	d := taking(rejectedOrder, orphan, expiredPayment)

	code, _, errOut := runWith(t, fakeBrokers(d), "purge", "orders.dlq", "--url", "fake://x", "--no-backup", "--yes")
	if code != exitOK || len(d.removed) != 3 || len(files()) != 0 {
		t.Errorf("exit code %d, removed %q, files %v; stderr:\n%s\nwant every message removed one by one", code, d.removed, files(), errOut)
	}
}

func TestPurgeNativeFails(t *testing.T) {
	d := &fakePurger{fakeTaker: *taking(rejectedOrder), purgeErr: errors.New("only one purge per SQS queue is allowed every 60 seconds, try again in a minute")}

	code, _, errOut := runWith(t, fakeBrokers(d), "purge", "orders.dlq", "--url", "fake://x", "--no-backup", "--yes")
	if code != exitError || !strings.Contains(errOut, "every 60 seconds") || strings.Contains(errOut, "stopped early") {
		t.Errorf("exit code %d, stderr:\n%s\nwant the purge error alone", code, errOut)
	}
}
