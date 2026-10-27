package main

// sse.go: a small Server-Sent Events reader for the upstream stream.

import (
	"bufio"
	"io"
	"strings"
)

type sseEvent struct {
	Name string
	Data string
}

// sseReader reads events from r. onActivity is called for every line read
// (including comments), so the caller can run a read-inactivity timer.
type sseReader struct {
	sc         *bufio.Scanner
	onActivity func()
	onComment  func(string) // optional: comment lines (keepalives), without the ':'
}

func newSSEReader(r io.Reader, onActivity func()) *sseReader {
	sc := bufio.NewScanner(r)
	// Full data sets arrive as one data line; allow large payloads.
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	return &sseReader{sc: sc, onActivity: onActivity}
}

// next returns the next complete event, or an error (io.EOF at the end).
func (s *sseReader) next() (sseEvent, error) {
	var ev sseEvent
	var data []string
	hasData := false
	for s.sc.Scan() {
		if s.onActivity != nil {
			s.onActivity()
		}
		line := strings.TrimSuffix(s.sc.Text(), "\r")
		if line == "" {
			if hasData || ev.Name != "" {
				ev.Data = strings.Join(data, "\n")
				if ev.Name == "" {
					ev.Name = "message"
				}
				return ev, nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			if s.onComment != nil {
				s.onComment(strings.TrimSpace(line[1:]))
			}
			continue // comment / keepalive
		}
		field, value := line, ""
		if i := strings.IndexByte(line, ':'); i >= 0 {
			field, value = line[:i], strings.TrimPrefix(line[i+1:], " ")
		}
		switch field {
		case "event":
			ev.Name = value
		case "data":
			data = append(data, value)
			hasData = true
		}
	}
	if err := s.sc.Err(); err != nil {
		return sseEvent{}, err
	}
	return sseEvent{}, io.EOF
}
