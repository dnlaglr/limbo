package ui

import (
	"context"
	"time"

	"charm.land/bubbles/v2/progress"
	"charm.land/bubbles/v2/spinner"
	tea "charm.land/bubbletea/v2"
)

type task struct {
	title   string
	spinner spinner.Model
	bar     *progress.Model
	percent float64
	start   time.Time
	done    bool
}

type (
	percentMsg float64
	doneMsg    struct{}
)

func newTask(title string, bar *progress.Model) task {
	return task{
		title:   title,
		spinner: spinner.New(spinner.WithSpinner(spinner.MiniDot), spinner.WithStyle(accentStyle)),
		bar:     bar,
	}
}

func (t task) Init() tea.Cmd { return t.spinner.Tick }

func (t task) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case percentMsg:
		t.percent = float64(msg)
	case doneMsg:
		t.done = true
		return t, tea.Quit
	case spinner.TickMsg:
		var cmd tea.Cmd
		t.spinner, cmd = t.spinner.Update(msg)
		return t, cmd
	}

	return t, nil
}

func (t task) View() tea.View {
	if t.done {
		return tea.NewView("")
	}

	return tea.NewView(t.line(t.spinner.View()))
}

func (t task) line(mark string) string {
	s := mark + " " + t.title
	if t.bar != nil {
		s += " " + t.bar.ViewAs(t.percent) + " " + subtleStyle.Render(formatElapsed(time.Since(t.start)))
	}

	return s
}

func (u *UI) run(ctx context.Context, t task, fn func(ctx context.Context, set func(float64)) error) error {
	t.start = time.Now()

	var err error
	if u.live && !u.quiet {
		err = u.animate(ctx, &t, fn)
	} else {
		err = fn(ctx, func(p float64) { t.percent = p })
	}

	mark := successMark
	if err != nil {
		mark = failureMark
	} else {
		t.percent = 1
	}
	u.narrate(t.line(mark))

	return err
}

func (u *UI) animate(ctx context.Context, t *task, fn func(ctx context.Context, set func(float64)) error) error {
	p := tea.NewProgram(*t, tea.WithOutput(u.err), tea.WithInput(nil), tea.WithoutSignalHandler())

	errc := make(chan error, 1)
	go func() {
		errc <- fn(ctx, func(pct float64) { p.Send(percentMsg(pct)) })
		p.Send(doneMsg{})
	}()

	final, _ := p.Run()
	err := <-errc
	if m, ok := final.(task); ok {
		*t = m
	}

	return err
}
