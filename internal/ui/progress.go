package ui

import (
	"context"
	"strconv"
	"time"

	"charm.land/bubbles/v2/progress"
)

// Progress bar's width in terminal cells
const barWidth = 24

func (u *UI) Progress(ctx context.Context, title string, fn func(ctx context.Context, set func(float64)) error) error {
	bar := progress.New(
		progress.WithWidth(barWidth),
		progress.WithoutPercentage(),
		progress.WithColors(Palette.Accent),
		progress.WithFillCharacters(progress.DefaultFullCharFullBlock, progress.DefaultEmptyCharBlock),
	)
	bar.EmptyColor = Palette.Subtle

	return u.run(ctx, newTask(title, &bar), fn)
}

func formatElapsed(d time.Duration) string {
	d = d.Round(100 * time.Millisecond)
	if d < time.Minute {
		return strconv.FormatFloat(d.Seconds(), 'f', 1, 64) + "s"
	}

	return d.Round(time.Second).String()
}
