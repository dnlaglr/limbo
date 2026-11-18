package ui

func (u *UI) Success(msg string) { u.narrate(successMark + " " + msg) }

func (u *UI) Failure(msg string) { u.narrate(failureMark + " " + msg) }

func (u *UI) Warn(msg string) { u.narrate(warningMark + " " + msg) }

func (u *UI) Info(msg string) { u.narrate(infoMark + " " + msg) }

func (u *UI) Note(msg string) { u.narrate(subtleStyle.Render(msg)) }
