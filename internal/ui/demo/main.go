package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"time"

	"github.com/dnlaglr/limbo/internal/ui"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	u := ui.New(os.Stdout, os.Stderr)
	_ = u.Step(ctx, "Connecting to broker", wait(800*time.Millisecond))
	_ = u.Step(ctx, "Locating dead-letter queue", wait(800*time.Millisecond))
	u.Note("	Found 1,284 messages in orders.dlq")

	_ = u.Progress(ctx, "Downloading messages", func(ctx context.Context, set func(float64)) error {
		const chunks = 40
		for i := range chunks + 1 {
			set(float64(i) / chunks)
			if err := sleep(ctx, 50*time.Millisecond); err != nil {
				return err
			}
		}

		return nil
	})

	_ = u.Step(ctx, "Replaying to orders", func(ctx context.Context) error {
		if err := sleep(ctx, 700*time.Millisecond); err != nil {
			return err
		}

		return errors.New("connection refused")
	})

	u.Warn("3 messages exceeded the retry limit")
	u.Info("Run with --debug for details")
	u.Success("Exported messages")

	u.Panel("Queue info",
		ui.Row{Key: "Name", Value: "orders.dlq"},
		ui.Row{Key: "State", Value: ui.Positive("HEALTHY")},
		ui.Row{Key: "Messages", Value: "1,284"},
		ui.Row{Key: "Export", Value: ui.Highlight("~/limbo/orders.dlq.jsonl")},
	)
}

func wait(d time.Duration) func(context.Context) error {
	return func(ctx context.Context) error { return sleep(ctx, d) }
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
