package ui

import "context"

func (u *UI) Step(ctx context.Context, title string, fn func(context.Context) error) error {
	return u.run(ctx, newTask(title, nil), func(ctx context.Context, _ func(float64)) error {
		return fn(ctx)
	})
}
