package ui

import (
	"io"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/term"
)

type UI struct {
	out   io.Writer
	err   io.Writer
	live  bool
	quiet bool
}

// Returns a new UI which writes results to out and narration to err
func New(out, err io.Writer) *UI {
	return &UI{out: out, err: err, live: isTerminal(err)}
}

func (u *UI) Out() io.Writer { return u.out }

func (u *UI) Err() io.Writer { return u.err }

func (u *UI) SetQuiet(quiet bool) { u.quiet = quiet }

func (u *UI) narrate(line string) {
	if !u.quiet {
		_, _ = lipgloss.Fprintln(u.err, line)
	}
}

func isTerminal(w io.Writer) bool {
	f, ok := w.(interface{ Fd() uintptr })
	return ok && term.IsTerminal(f.Fd())
}
