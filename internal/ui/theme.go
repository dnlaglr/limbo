package ui

import (
	"image/color"

	"charm.land/lipgloss/v2"
)

var Palette = struct {
	Accent   color.Color
	Positive color.Color
	Negative color.Color
	Warning  color.Color
	Subtle   color.Color
}{
	Accent:   lipgloss.Color("#3B82F6"),
	Positive: lipgloss.Color("#22C55E"),
	Negative: lipgloss.Color("#EF4444"),
	Warning:  lipgloss.Color("#EAB308"),
	Subtle:   lipgloss.Color("#8B8B8B"),
}

var (
	accentStyle   = lipgloss.NewStyle().Foreground(Palette.Accent)
	positiveStyle = lipgloss.NewStyle().Foreground(Palette.Positive)
	negativeStyle = lipgloss.NewStyle().Foreground(Palette.Negative)
	warningStyle  = lipgloss.NewStyle().Foreground(Palette.Warning)
	subtleStyle   = lipgloss.NewStyle().Foreground(Palette.Subtle)
	badgeStyle    = lipgloss.NewStyle().Reverse(true).Bold(true).Padding(0, 1)
	panelStyle    = lipgloss.NewStyle().Border(lipgloss.NormalBorder()).BorderForeground(Palette.Subtle).Padding(1, 3)
)

var (
	successMark = positiveStyle.Render("✓")
	failureMark = negativeStyle.Render("✗")
	warningMark = warningStyle.Render("!")
	infoMark    = accentStyle.Render("•")
)

func Positive(s string) string { return positiveStyle.Render(s) }

func Negative(s string) string { return negativeStyle.Render(s) }

func Highlight(s string) string { return warningStyle.Render(s) }

func Subtle(s string) string { return subtleStyle.Render(s) }
