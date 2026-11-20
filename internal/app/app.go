package app

import (
	"io"
	"log/slog"

	"github.com/dnlaglr/limbo/internal/broker"
	"github.com/dnlaglr/limbo/internal/broker/rabbitmq"
	"github.com/dnlaglr/limbo/internal/broker/sqs"
	"github.com/dnlaglr/limbo/internal/ui"
)

type App struct {
	Build   BuildInfo
	UI      *ui.UI
	Log     *slog.Logger
	Brokers broker.Registry

	logLevel *slog.LevelVar
}

func New(build BuildInfo, stdout, stderr io.Writer) *App {
	level := new(slog.LevelVar)
	level.Set(slog.LevelWarn)

	return &App{
		Build:    build.resolve(),
		UI:       ui.New(stdout, stderr),
		Log:      slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level})),
		Brokers:  broker.Registry{"amqp": rabbitmq.Open, "amqps": rabbitmq.Open, "sqs": sqs.Open},
		logLevel: level,
	}
}

type Settings struct {
	Debug bool
	Quiet bool
}

func (a *App) Apply(s Settings) {
	if s.Debug {
		a.logLevel.Set(slog.LevelDebug)
	}
	a.UI.SetQuiet(s.Quiet)
}
