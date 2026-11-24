package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
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

// A messageSource reads an export or backup file, from the line after skip, and is checked whole before anything is published
type messageSource struct {
	path  string
	skip  int
	count int
}

func readFile(path string, skip int) (*messageSource, error) {
	src := &messageSource{path: path, skip: skip}
	err := src.each(func(int, broker.Message) bool {
		src.count++
		return true
	})

	return src, err
}

// Lines are read whole, however long a message is, and blank lines count toward line numbers but hold no message
func (s *messageSource) each(visit func(line int, m broker.Message) bool) error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for line := 1; ; line++ {
		b, err := r.ReadBytes('\n')
		if line == 1 {
			b = bytes.TrimPrefix(b, []byte("\xef\xbb\xbf"))
		}
		if b = bytes.TrimRight(b, "\r\n"); line > s.skip && len(bytes.TrimSpace(b)) > 0 {
			m, decodeErr := decodeLine(b)
			if decodeErr != nil {
				return fmt.Errorf("line %d of %s: %w", line, s.path, decodeErr)
			}
			if !visit(line, m) {
				return nil
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// messages yields each message for a run, which learns the line it came from
func (s *messageSource) messages(run *takeRun) iter.Seq2[broker.Message, error] {
	return func(yield func(broker.Message, error) bool) {
		if err := s.each(func(line int, m broker.Message) bool {
			run.line = line
			return yield(m, nil)
		}); err != nil {
			yield(broker.Message{}, err)
		}
	}
}

// read previews a run the way browse does for a queue, keeping each message that matches until the limit
func (s *messageSource) read(run *takeRun) func(keep func(broker.Message) error) (int, error) {
	return func(keep func(broker.Message) error) (int, error) {
		scanned, matched := 0, 0
		var kept error
		err := s.each(func(line int, m broker.Message) bool {
			scanned, run.line = scanned+1, line
			if run.sel.Filter.Match(m) {
				matched++
				kept = keep(m)
			}
			return kept == nil && !run.sel.Full(matched)
		})

		return scanned, errors.Join(kept, err)
	}
}

func decodeLine(b []byte) (broker.Message, error) {
	var probe struct {
		Version *int `json:"version"`
	}
	if err := json.Unmarshal(b, &probe); err != nil || probe.Version == nil {
		return broker.Message{}, errors.New("isn't a limbo export record")
	}
	if *probe.Version != fileVersion {
		return broker.Message{}, fmt.Errorf("has version %d, which this limbo can't read", *probe.Version)
	}

	var m broker.Message
	err := json.Unmarshal(b, &m)
	return m, err
}
