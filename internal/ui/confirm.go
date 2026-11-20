package ui

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"

	"charm.land/lipgloss/v2"
)

var ErrNoTerminal = errors.New("not a terminal")

// The answer is read in a goroutine so Ctrl-C can cancel a prompt that is blocked on stdin
func (u *UI) Confirm(ctx context.Context, in io.Reader, question string) (bool, error) {
	if !u.live || !isTerminal(in) {
		return false, ErrNoTerminal
	}

	_, _ = lipgloss.Fprint(u.err, promptMark+" "+question+" [y/N] ")

	answer := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(in).ReadString('\n')
		answer <- line
	}()

	select {
	case <-ctx.Done():
		_, _ = lipgloss.Fprintln(u.err)
		return false, ctx.Err()
	case line := <-answer:
		line = strings.ToLower(strings.TrimSpace(line))
		return line == "y" || line == "yes", nil
	}
}
