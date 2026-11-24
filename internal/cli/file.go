package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/dnlaglr/limbo/internal/broker"
)

const fileVersion = 1

// A messageFile holds one message per line, each in peek's JSON with the file format's version first
type messageFile struct {
	path    string
	n       int
	size    int64
	w       io.Writer
	file    *os.File
	durable bool
}

// Queue names may hold path separators, so only a safe set of characters reaches the file name
func defaultFile(queue string, now time.Time) string {
	safe := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, queue)

	return safe + "-" + now.UTC().Format("20060102T150405Z") + ".ndjson"
}

// Message bodies may hold sensitive data, so the file is readable by its owner only and never replaces an existing one
// A durable file is flushed to disk after every line, so a message removed after its write is never lost
func createFile(path string, stdout io.Writer, durable bool) (*messageFile, error) {
	if path == "-" {
		return &messageFile{path: path, w: stdout}, nil
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return nil, fmt.Errorf("won't overwrite %s, which already exists: pick another path", path)
	}
	if err != nil {
		return nil, err
	}

	return &messageFile{path: path, w: f, file: f, durable: durable}, nil
}

func (f *messageFile) write(m broker.Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	line := fmt.Appendf(nil, "{\"version\":%d,%s\n", fileVersion, b[1:])
	if _, err := f.w.Write(line); err != nil {
		return f.cut(err)
	}
	if f.durable {
		if err := f.file.Sync(); err != nil {
			return f.cut(err)
		}
	}
	f.n++
	f.size += int64(len(line))

	return nil
}

// A line that failed to write or flush is cut off, so every line left belongs to a message limbo kept and is complete
func (f *messageFile) cut(err error) error {
	if f.file == nil {
		return err
	}

	return errors.Join(err, f.file.Truncate(f.size))
}

// A file that holds no messages is removed, so a run that took nothing leaves nothing behind
func (f *messageFile) close() error {
	if f == nil || f.file == nil {
		return nil
	}

	var err error
	if !f.durable {
		err = f.file.Sync()
	}
	err = errors.Join(err, f.file.Close())
	if f.n == 0 {
		err = errors.Join(err, os.Remove(f.path))
	}

	return err
}

func (f *messageFile) name() string {
	if f.file == nil {
		return "stdout"
	}

	return f.path
}
