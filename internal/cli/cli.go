package cli

import (
	"context"
	"errors"
	"image/color"
	"io"

	"charm.land/fang/v2"
	"charm.land/lipgloss/v2"
	"github.com/spf13/cobra"

	"github.com/dnlaglr/limbo/internal/app"
	"github.com/dnlaglr/limbo/internal/ui"
)

const (
	exitOK       = 0
	exitError    = 1
	exitCanceled = 130
)

func Execute(ctx context.Context, a *app.App, args []string) int {
	cobra.EnableTraverseRunHooks = true

	root := newRootCmd(a)
	root.SetArgs(args)
	root.SetOut(a.UI.Out())
	root.SetErr(a.UI.Err())

	err := fang.Execute(ctx, root, fang.WithoutVersion(), fang.WithColorSchemeFunc(colorScheme), fang.WithErrorHandler(handleError))
	switch {
	case err == nil:
		return exitOK
	case errors.Is(err, context.Canceled):
		return exitCanceled
	default:
		return exitError
	}
}

func handleError(w io.Writer, styles fang.Styles, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	fang.DefaultErrorHandler(w, styles, err)
}

func colorScheme(c lipgloss.LightDarkFunc) fang.ColorScheme {
	cs := fang.DefaultColorScheme(c)
	cs.Title = ui.Palette.Accent
	cs.Program = ui.Palette.Accent
	cs.Flag = ui.Palette.Positive
	cs.ErrorHeader = [2]color.Color{lipgloss.Color("#FFFFFF"), ui.Palette.Negative}
	return cs
}
