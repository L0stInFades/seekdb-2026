package engine

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	sp "minivectordb/internal/sqlparse"
)

// An InsertStream speculatively parses a large INSERT while its request line
// is still arriving: Feed is handed growing prefixes that share one backing
// array, and a persistent worker goroutine turns dispatched row parts into
// encoded spans. When the statement finally executes, ExecStream either uses
// the speculation — it matches the final statement exactly — or throws it
// away and runs the ordinary path, never changing results.
//
// Equivalence: by induction over consecutive parts, each part that lexes and
// parses as a complete row list ends on a real `)` token in a clean lexer
// state (line comments are excluded by the --/# rule on every non-final
// part; unterminated strings and /* */ comments fail to lex), and the next
// part starts at the `(` after the separating comma. Non-final parts cannot
// end in `;` (a mid-part `;` would make ParseRows fail), so the concatenated
// rows equal the rows of the sequential parse. Anything else falls back.
var (
	streamInsertEnabled  = os.Getenv("MVDB_NO_STREAM_INSERT") == ""
	streamChunkBytes     = envInt("MVDB_STREAM_CHUNK", 32<<10)
	streamWorkers        = envInt("MVDB_STREAM_WORKERS", 1)
	streamInsertHandled  atomic.Int64
	streamInsertFallback atomic.Int64
	streamWorkerParts    atomic.Int64 // parts completed by worker goroutines; test observability
)

func init() {
	if streamChunkBytes < 1 {
		streamChunkBytes = 1
	}
	if streamWorkers < 1 {
		streamWorkers = 1
	}
}

// streamPart is one dispatched slice of the row region. The text aliases the
// request line's backing array; enc and spans are filled by whichever parser
// claims the part.
type streamPart struct {
	text   string
	enc    []byte
	spans  []fastSpan
	done   bool
	failed bool
}

// InsertStream is the speculation state for one DB, reused for every request.
// Feed, Cancel, Reset and ExecStream all run on the caller's goroutine.
type InsertStream struct {
	db *DB

	mu   sync.Mutex
	cond sync.Cond

	base    *byte // data pointer of the first fed text
	fedLen  int   // bytes fed so far
	t       *Table
	tname   string
	target  []int
	done    int // SQL offset where not-yet-dispatched rows begin
	frozen  bool
	dead    bool
	failed  bool
	parts   []*streamPart
	next    int // index of the next part to claim
	busy    int // parts currently being parsed
	quit    bool
	started bool

	pool  []*streamPart
	workW []fastPartWorker // one per worker goroutine
	callW fastPartWorker   // owned by the calling goroutine
}

// NewInsertStream creates the single stream of a DB, reused for every request.
func (db *DB) NewInsertStream() *InsertStream {
	s := &InsertStream{db: db}
	s.cond.L = &s.mu
	db.stream = s
	return s
}

// newPart takes a pooled part or allocates one; called on the caller's
// goroutine only, so pool needs no lock.
func (s *InsertStream) newPart(text string) *streamPart {
	if n := len(s.pool); n > 0 {
		p := s.pool[n-1]
		s.pool = s.pool[:n-1]
		p.text = text
		p.done = false
		p.failed = false
		p.enc = p.enc[:0]
		p.spans = p.spans[:0]
		return p
	}
	return &streamPart{text: text}
}

// Feed hands the stream the SQL text received so far: a prefix of the final
// text living in the same backing array, only ever longer.
func (s *InsertStream) Feed(sql string) {
	if !streamInsertEnabled {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return
	}
	if s.base == nil {
		if strings.IndexAny(sql, "'\"") < 0 {
			// No string literal yet: the head may still be arriving. Give up
			// only when it is clearly beyond the 4 KiB head window.
			if len(sql) <= 4096 {
				return
			}
			s.dead = true
			return
		}
		ins, vend, ok := sp.ParseInsertHead(sql)
		if !ok {
			s.dead = true
			return
		}
		t := s.db.tables[strings.ToLower(ins.Table)]
		if t == nil {
			s.dead = true
			return
		}
		target, terr := insertTarget(t, ins.Cols)
		if terr != nil {
			s.dead = true
			return
		}
		s.base = unsafe.StringData(sql)
		s.fedLen = len(sql)
		s.t = t
		s.tname = strings.ToLower(ins.Table)
		s.target = target
		s.done = vend
	}
	if unsafe.StringData(sql) != s.base || len(sql) < s.fedLen {
		// Contract violation: not the same growing text. Abandon.
		s.dead = true
		return
	}
	s.fedLen = len(sql)
	if s.frozen {
		return
	}
	closePos, openPos := lastRowBoundary(sql, s.done)
	if closePos < 0 {
		return
	}
	cand := sql[s.done : closePos+1]
	if len(cand) < streamChunkBytes {
		return
	}
	if hasLineComment(cand) {
		// A -- or # comment could eat the boundary `)`; stop dispatching
		// early parts and leave the whole tail to ExecStream.
		s.frozen = true
		return
	}
	s.parts = append(s.parts, s.newPart(cand))
	s.done = openPos
	s.startWorkerLocked()
	s.cond.Broadcast()
}

// lastRowBoundary finds the last `) ws* , ws* (` pair in sql[done:] with the
// `(` inside the received text, and returns (closeParen, openParen).
func lastRowBoundary(sql string, done int) (int, int) {
	for i := len(sql) - 1; i >= done; i-- {
		if sql[i] != '(' {
			continue
		}
		j := i - 1
		for j >= done && isByteSpace(sql[j]) {
			j--
		}
		if j < done || sql[j] != ',' {
			continue
		}
		j--
		for j >= done && isByteSpace(sql[j]) {
			j--
		}
		if j >= done && sql[j] == ')' {
			return j, i
		}
	}
	return -1, -1
}

func (s *InsertStream) startWorkerLocked() {
	if s.started {
		return
	}
	s.started = true
	s.workW = make([]fastPartWorker, streamWorkers)
	for i := range s.workW {
		go s.worker(&s.workW[i])
	}
}

// worker claims parts from the shared queue and encodes them until shutdown.
func (s *InsertStream) worker(w *fastPartWorker) {
	for {
		s.mu.Lock()
		for s.next < len(s.parts) && !s.quit {
			p := s.parts[s.next]
			s.next++
			if s.dead || s.failed {
				p.done = true
				continue
			}
			s.busy++
			s.mu.Unlock()
			var ok bool
			p.enc, p.spans, ok = w.fill(s.t, s.target, p.text, p.enc, p.spans)
			s.mu.Lock()
			s.busy--
			p.done = true
			if !ok {
				p.failed = true
				s.failed = true
			} else {
				streamWorkerParts.Add(1)
			}
			s.cond.Broadcast()
		}
		if s.quit {
			s.mu.Unlock()
			return
		}
		s.cond.Wait()
		s.mu.Unlock()
	}
}

// Cancel abandons speculation for the current statement and returns only
// once no goroutine reads the fed text any more.
func (s *InsertStream) Cancel() {
	s.mu.Lock()
	s.dead = true
	s.cond.Broadcast()
	for s.busy > 0 {
		s.cond.Wait()
	}
	s.mu.Unlock()
}

// Reset cancels and clears per-statement state before the line buffer is
// reused for the next request.
func (s *InsertStream) Reset() {
	s.Cancel()
	s.mu.Lock()
	for _, p := range s.parts {
		if len(s.pool) >= 64 {
			break
		}
		if cap(p.enc) > 4<<20 {
			p.enc = nil
		}
		p.text = ""
		s.pool = append(s.pool, p)
	}
	s.parts = nil
	s.next = 0
	s.failed = false
	s.dead = false
	s.frozen = false
	s.base = nil
	s.fedLen = 0
	s.t = nil
	s.tname = ""
	s.target = nil
	s.done = 0
	s.mu.Unlock()
}

// shutdown stops the worker goroutine for good; called from DB.Close.
func (s *InsertStream) shutdown() {
	s.mu.Lock()
	s.quit = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// usable reports whether the speculation for the fed prefixes can be used
// for this final statement text.
func (s *InsertStream) usable(sql string) (*Table, []int, bool) {
	if !fastInsertEnabled || !streamInsertEnabled {
		return nil, nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead || s.base == nil || unsafe.StringData(sql) != s.base || len(sql) < s.done {
		return nil, nil, false
	}
	if s.db.tables[s.tname] != s.t {
		return nil, nil, false
	}
	return s.t, s.target, true
}

func (s *InsertStream) fed() bool {
	s.mu.Lock()
	fed := s.base != nil
	s.mu.Unlock()
	return fed
}

// appendRemainder splits sql[done:] into ~streamChunkBytes parts at row
// boundaries (all but the last free of --/# comments; a comment candidate or
// a frozen stream makes the whole remainder one part) and queues them.
func (s *InsertStream) appendRemainder(sql string) {
	var add []*streamPart
	d := s.done
	if !s.frozen {
		for {
			closePos, openPos := nextRowBoundary(sql, d+streamChunkBytes)
			if closePos < 0 {
				break
			}
			if hasLineComment(sql[d : closePos+1]) {
				break
			}
			add = append(add, s.newPart(sql[d:closePos+1]))
			d = openPos
		}
	}
	if tail := sql[d:]; len(tail) > 0 {
		add = append(add, s.newPart(tail))
	}
	s.mu.Lock()
	s.parts = append(s.parts, add...)
	s.done = len(sql)
	s.startWorkerLocked()
	s.cond.Broadcast()
	s.mu.Unlock()
}

// drain claims and runs parts from the shared queue with the caller's own
// parser until every part is done, then reports whether all succeeded.
func (s *InsertStream) drain(t *Table, target []int) bool {
	for {
		s.mu.Lock()
		for s.next < len(s.parts) {
			p := s.parts[s.next]
			s.next++
			if s.dead || s.failed {
				p.done = true
				continue
			}
			s.busy++
			s.mu.Unlock()
			var ok bool
			p.enc, p.spans, ok = s.callW.fill(t, target, p.text, p.enc, p.spans)
			s.mu.Lock()
			s.busy--
			p.done = true
			if !ok {
				p.failed = true
				s.failed = true
			}
			s.cond.Broadcast()
		}
		done := true
		for _, p := range s.parts {
			if !p.done {
				done = false
				break
			}
		}
		if done {
			failed := s.failed
			s.mu.Unlock()
			return !failed
		}
		s.cond.Wait()
		s.mu.Unlock()
	}
}

// ExecStream executes sql, using the stream's speculation when it matches
// the final statement exactly and falling back to the ordinary path
// otherwise. Only a storage write failure escapes the fast path.
func (db *DB) ExecStream(sql string, s *InsertStream) (*Result, error) {
	if s == nil {
		return db.Exec(sql)
	}
	t, target, ok := s.usable(sql)
	if !ok {
		if s.fed() {
			streamInsertFallback.Add(1)
		}
		s.Cancel()
		return db.Exec(sql)
	}
	s.appendRemainder(sql)
	if !s.drain(t, target) {
		streamInsertFallback.Add(1)
		s.Cancel()
		return db.Exec(sql)
	}
	s.mu.Lock()
	units := make([]encSpans, len(s.parts))
	for i, p := range s.parts {
		units[i] = encSpans{enc: p.enc, spans: p.spans}
	}
	s.mu.Unlock()
	res, err, handled := db.finishFastInsert(t, units)
	// Drop chs references so they cannot pin a pooled part's enc after the
	// statement, same as resetFastInsert does for the split path.
	clear(db.fi.chs[:cap(db.fi.chs)])
	db.fi.chs = db.fi.chs[:0]
	if !handled {
		streamInsertFallback.Add(1)
		s.Cancel()
		return db.Exec(sql)
	}
	streamInsertHandled.Add(1)
	return res, err
}
