package main

import (
	"bytes"
	"unsafe"

	"minivectordb/internal/engine"
	"minivectordb/internal/proto"
)

// Statements below this size are not worth speculating on: their parse cost
// is smaller than the coordination overhead.
var streamMinBytes = normalizeMin(envInt("MVDB_STREAM_MIN", 32<<10))

func normalizeMin(n int64) int {
	if n < 1 {
		return 1
	}
	return int(n)
}

// streamFeed turns the lineReader's partial-line callbacks into InsertStream
// feeds. The fed text always aliases the reader's line buffer, so a buffer
// move (onMove) must abandon the speculation before the bytes change
// address, and a JSON backslash or the closing quote bounds the fed region.
type streamFeed struct {
	s         *engine.InsertStream
	active    bool // SQLStart matched; feeds may be sent
	abandoned bool
	done      bool // closing quote of the sql value seen
	start     int  // offset of the sql text inside the line
	seen      int  // sql bytes already examined for '\' and '"'
}

func newStreamFeed(s *engine.InsertStream) *streamFeed {
	return &streamFeed{s: s}
}

func (f *streamFeed) onPartial(p []byte) {
	if f.abandoned || f.done {
		return
	}
	if !f.active {
		if len(p) < streamMinBytes {
			return
		}
		st, ok := proto.SQLStart(p)
		if !ok {
			f.abandoned = true
			return
		}
		f.active = true
		f.start = st
	}
	body := p[f.start:]
	if i := bytes.IndexAny(body[f.seen:], `"\`); i >= 0 {
		if body[f.seen+i] == '\\' {
			// The fed text must equal the decoded SQL exactly; JSON escapes
			// would make it differ, so abandon this line.
			f.s.Cancel()
			f.abandoned = true
			return
		}
		// The SQL value ends at this quote; feed the complete text once.
		f.s.Feed(unsafe.String(&body[0], f.seen+i))
		f.done = true
		return
	}
	if len(body) > 0 {
		f.s.Feed(unsafe.String(&body[0], len(body)))
	}
	f.seen = len(body)
}

func (f *streamFeed) onMove() {
	f.s.Cancel()
	f.abandoned = true
}

// reset clears per-line state and the stream before the next request.
func (f *streamFeed) reset() {
	f.s.Reset()
	f.active = false
	f.abandoned = false
	f.done = false
	f.start = 0
	f.seen = 0
}
