package ui

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestStep(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"success", nil, "✓ Connect\n"},
		{"failure", boom, "✗ Connect\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _, errOut := newTestUI()
			err := u.Step(context.Background(), "Connect", func(context.Context) error { return tt.err })
			if !errors.Is(err, tt.err) {
				t.Errorf("err = %v, want %v", err, tt.err)
			}

			if got := errOut.String(); got != tt.want {
				t.Errorf("stderr = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestProgress(t *testing.T) {
	boom := errors.New("boom")
	half := strings.Repeat("█", barWidth/2) + strings.Repeat("░", barWidth/2)
	tests := []struct {
		name   string
		err    error
		prefix string
	}{
		{"success fills the bar", nil, "✓ Download " + strings.Repeat("█", barWidth) + " "},
		{"failure keeps progress", boom, "✗ Download " + half + " "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, _, errOut := newTestUI()
			err := u.Progress(context.Background(), "Download", func(_ context.Context, set func(float64)) error {
				set(0.5)
				return tt.err
			})
			if !errors.Is(err, tt.err) {
				t.Errorf("err = %v, want %v", err, tt.err)
			}

			got := errOut.String()
			if !strings.HasPrefix(got, tt.prefix) || !strings.HasSuffix(got, "s\n") {
				t.Errorf("stderr = %q, want prefix %q and an elapsed time", got, tt.prefix)
			}
		})
	}
}

func TestFormatElapsed(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{0, "0.0s"},
		{340 * time.Millisecond, "0.3s"},
		{59940 * time.Millisecond, "59.9s"},
		{59960 * time.Millisecond, "1m0s"},
		{75 * time.Second, "1m15s"},
	}
	for _, tt := range tests {
		if got := formatElapsed(tt.d); got != tt.want {
			t.Errorf("formatElapsed(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}
