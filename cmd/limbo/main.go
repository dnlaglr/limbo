package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/cli"
)

var (
	version string
	commit  string
	date    string
)

func main() { os.Exit(run()) }

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	context.AfterFunc(ctx, stop)

	a := app.New(app.BuildInfo{Version: version, Commit: commit, Date: date}, os.Stdout, os.Stderr)
	return cli.Execute(ctx, a, os.Args[1:])
}
