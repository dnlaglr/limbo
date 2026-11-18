package ui

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// Returns UI writing to buffers. Output is plain text and steps are not animated
func newTestUI() (u *UI, out, errOut *bytes.Buffer) {
	out, errOut = new(bytes.Buffer), new(bytes.Buffer)
	return New(out, errOut), out, errOut
}

func TestStatusLines(t *testing.T) {
	u, out, errOut := newTestUI()
	u.Success("connected")
	u.Failure("rejected")
	u.Warn("slow")
	u.Info("fyi")
	u.Note("aside")

	want := "✓ connected\n✗ rejected\n! slow\n• fyi\naside\n"
	if got := errOut.String(); got != want {
		t.Errorf("stderr =\n%q\nwant\n%q", got, want)
	}
	if out.Len() != 0 {
		t.Errorf("status lines leaked to stdout: %q", out.String())
	}
}

func TestQuietHidesNarrationOnly(t *testing.T) {
	u, out, errOut := newTestUI()
	u.SetQuiet(true)
	u.Success("hidden")
	_ = u.Step(context.Background(), "hidden", func(context.Context) error { return nil })
	u.Panel("", Row{Key: "Result", Value: "shown"})

	if errOut.Len() != 0 {
		t.Errorf("quiet still narrated: %q", errOut.String())
	}
	if !strings.Contains(out.String(), "shown") {
		t.Errorf("quiet hid a result: %q", out.String())
	}
}
