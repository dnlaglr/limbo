package ui

import (
	"strings"

	"charm.land/lipgloss/v2"
)

type Row struct {
	Key   string
	Value string
}

const keyGap = 4

func (u *UI) Panel(title string, rows ...Row) {
	visible := make([]Row, 0, len(rows))
	keyWidth := 0
	for _, r := range rows {
		if r.Value == "" {
			continue
		}

		visible = append(visible, r)
		keyWidth = max(keyWidth, lipgloss.Width(r.Key))
	}

	var lines []string
	if title != "" {
		lines = append(lines, badgeStyle.Render(title), "")
	}

	key := subtleStyle.Width(keyWidth + keyGap)
	for _, r := range visible {
		lines = append(lines, lipgloss.JoinHorizontal(lipgloss.Top, key.Render(r.Key), r.Value))
	}

	_, _ = lipgloss.Fprintln(u.out, panelStyle.Render(strings.Join(lines, "\n")))
}
