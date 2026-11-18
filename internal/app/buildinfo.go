package app

import "runtime/debug"

type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

func (b BuildInfo) resolve() BuildInfo {
	if info, ok := debug.ReadBuildInfo(); ok {
		if b.Version == "" && info.Main.Version != "(devel)" {
			b.Version = info.Main.Version
		}
		for _, s := range info.Settings {
			switch {
			case s.Key == "vcs.revision" && b.Commit == "":
				b.Commit = s.Value
			case s.Key == "vcs.time" && b.Date == "":
				b.Date = s.Value
			}
		}
	}
	if b.Version == "" {
		b.Version = "dev"
	}

	return b
}
