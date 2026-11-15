package engine

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	sp "minivectordb/internal/sqlparse"
	"minivectordb/internal/types"
)

// The fast path is optimistic: it either completes a whole INSERT statement
// or reports "not handled" with no side effects, after which Exec runs the
// untouched sequential path (sp.Parse -> execInsert) on the original SQL.
// Any lex/parse/type/duplicate/shape problem therefore still surfaces through
// the sequential path with its own error object. The only error the fast
// path returns itself is a storage write failure.
var (
	fastInsertEnabled  = os.Getenv("MVDB_NO_FAST_INSERT") == ""
	fastInsertMinBytes = envInt("MVDB_FAST_INSERT_MIN", 16<<10)
	fastInsertWorkers  = envInt("MVDB_FAST_INSERT_WORKERS", 0)

	fastInsertHandled  atomic.Int64
	fastInsertFallback atomic.Int64 // statements that reached the head parse but fell back
)

func envInt(name string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return n
	}
	return def
}

type fastSpan struct {
	pk         int64
	start, end int
}

// fastPartWorker holds the per-goroutine parse/coerce state shared by the
// two-way split in tryFastInsert and by the insert stream's parts.
type fastPartWorker struct {
	rp      sp.RowParser
	scratch []float32
	row     []types.Value
}

// fill parses text as a complete row list and encodes each row into enc,
// recording (pk, start, end) per row in spans. It fails on the first
// lex/parse/shape/type problem, which the callers turn into a fallback to
// the sequential path; enc and spans are then left unspecified.
func (w *fastPartWorker) fill(t *Table, target []int, text string, enc []byte, spans []fastSpan) ([]byte, []fastSpan, bool) {
	rows, perr := w.rp.ParseRows(text)
	if perr != nil {
		return enc, spans, false
	}
	if len(w.row) != len(t.Cols) {
		w.row = make([]types.Value, len(t.Cols))
	}
	enc = enc[:0]
	spans = spans[:0]
	for _, exprs := range rows {
		if len(exprs) != len(target) {
			return enc, spans, false
		}
		for j, e := range exprs {
			var value types.Value
			switch e := e.(type) {
			case *sp.IntLit:
				value = types.Int(e.V)
			case *sp.FloatLit:
				value = types.Float(e.V)
			case *sp.StrLit:
				value = types.Str(e.V)
			default:
				return enc, spans, false
			}
			col := target[j]
			v, cerr := coerceVectorInto(t.Cols[col], value, w.scratch)
			if cerr != nil {
				return enc, spans, false
			}
			// As in execInsert: the parsed vector becomes the scratch
			// buffer; encode copies it before the next row reuses it.
			if value.K == types.KStr && v.K == types.KVec {
				w.scratch = v.V
			}
			w.row[col] = v
		}
		start := len(enc)
		enc = t.encode(enc, w.row)
		spans = append(spans, fastSpan{pk: w.row[t.PK].I, start: start, end: len(enc)})
	}
	return enc, spans, true
}

// fastInsertWorker owns the buffers one worker fills while parsing and
// encoding its half of the row region. Workers only read shared state.
type fastInsertWorker struct {
	pw     fastPartWorker
	enc    []byte
	spans  []fastSpan
	failed bool
}

type fastInsertBufs struct {
	workers [2]fastInsertWorker
	chs     []change
	seen    map[int64]struct{}
}

// encSpans pairs encoded row bytes with their (pk, start, end) spans.
type encSpans struct {
	enc   []byte
	spans []fastSpan
}

// finishFastInsert runs the duplicate checks and the storage write for
// pre-encoded spans in row order. Duplicate checks run on the calling
// goroutine after all producers finished: in-statement dups and conflicts
// with visible rows both report not-handled so the caller falls back to the
// sequential path.
func (db *DB) finishFastInsert(t *Table, units []encSpans) (*Result, error, bool) {
	n := 0
	for _, u := range units {
		n += len(u.spans)
	}
	if n == 0 {
		return nil, nil, false
	}
	fi := &db.fi
	seen := fi.seen
	if seen == nil {
		seen = make(map[int64]struct{}, n)
	} else {
		clear(seen)
	}
	fi.seen = seen
	chs := fi.chs[:0]
	for _, u := range units {
		for _, s := range u.spans {
			if _, dup := seen[s.pk]; dup {
				fi.chs = chs
				return nil, nil, false
			}
			if _, exists := db.lookup(t, s.pk); exists {
				fi.chs = chs
				return nil, nil, false
			}
			seen[s.pk] = struct{}{}
			chs = append(chs, change{t: t, pk: s.pk, row: u.enc[s.start:s.end:s.end]})
		}
	}
	fi.chs = chs
	if err := db.write(chs); err != nil {
		return nil, ioErr(err), true
	}
	return &Result{Affected: int64(n), HasAffected: true}, nil, true
}

func hasInsertPrefix(sql string) bool {
	i := 0
	for i < len(sql) {
		switch sql[i] {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			i++
		default:
			goto kw
		}
	}
kw:
	for k := 0; k < len("insert"); k++ {
		if i+k >= len(sql) || sql[i+k]|0x20 != "insert"[k] {
			return false
		}
	}
	return true
}

func (db *DB) tryFastInsert(sql string) (res *Result, err error, handled bool) {
	if !fastInsertEnabled || len(sql) < fastInsertMinBytes || !hasInsertPrefix(sql) {
		return nil, nil, false
	}
	fallback := func() (*Result, error, bool) {
		fastInsertFallback.Add(1)
		return nil, nil, false
	}
	ins, vend, ok := sp.ParseInsertHead(sql)
	if !ok {
		return fallback()
	}
	t := db.tables[strings.ToLower(ins.Table)]
	if t == nil {
		return fallback()
	}
	target, terr := insertTarget(t, ins.Cols)
	if terr != nil {
		return fallback()
	}
	region := sql[vend:]
	workers := fastInsertWorkers
	if workers <= 0 {
		workers = min(runtime.GOMAXPROCS(0), 2)
	}
	parts := []string{region}
	if workers >= 2 {
		if a, b, ok := splitInsertRows(region); ok {
			parts = []string{a, b}
		}
	}
	n := 0
	defer func() { db.resetFastInsert(n) }()
	fi := &db.fi
	run := func(w *fastInsertWorker, part string) {
		var ok bool
		w.enc, w.spans, ok = w.pw.fill(t, target, part, w.enc, w.spans)
		w.failed = !ok
	}
	if len(parts) == 2 {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			run(&fi.workers[1], parts[1])
		}()
		run(&fi.workers[0], parts[0])
		wg.Wait()
	} else {
		run(&fi.workers[0], parts[0])
	}
	for i := range parts {
		if fi.workers[i].failed {
			return fallback()
		}
		n += len(fi.workers[i].spans)
	}
	if n == 0 {
		return fallback()
	}
	units := make([]encSpans, len(parts))
	for i := range parts {
		units[i] = encSpans{enc: fi.workers[i].enc, spans: fi.workers[i].spans}
	}
	res, err, handled = db.finishFastInsert(t, units)
	if !handled {
		return fallback()
	}
	fastInsertHandled.Add(1)
	return res, err, true
}

// resetFastInsert returns oversized per-statement buffers so one huge INSERT
// cannot pin memory, and clears chs so it cannot keep a dropped enc alive.
func (db *DB) resetFastInsert(rows int) {
	fi := &db.fi
	for i := range fi.workers {
		w := &fi.workers[i]
		if cap(w.enc) > 4<<20 {
			w.enc = nil
		}
		if cap(w.spans) > 65536 {
			w.spans = nil
		}
	}
	// Appends on a truncated chs can leave row references past its length;
	// clear the whole backing array so a dropped enc is not pinned.
	clear(fi.chs[:cap(fi.chs)])
	fi.chs = fi.chs[:0]
	if rows > 65536 {
		fi.seen = nil
	}
}

// nextRowBoundary finds the first `)` at or after from whose following bytes
// are [ \t\r\n]* , [ \t\r\n]* ( and returns (closeParen, openParen).
func nextRowBoundary(region string, from int) (closePos, openPos int) {
	for i := from; i < len(region); i++ {
		if region[i] != ')' {
			continue
		}
		j := i + 1
		for j < len(region) && isByteSpace(region[j]) {
			j++
		}
		if j >= len(region) || region[j] != ',' {
			continue
		}
		k := j + 1
		for k < len(region) && isByteSpace(region[k]) {
			k++
		}
		if k >= len(region) || region[k] != '(' {
			continue
		}
		return i, k
	}
	return -1, -1
}

// hasLineComment reports whether text contains a `--` or `#` comment start.
// It does not track strings; a false positive only costs a fallback.
func hasLineComment(text string) bool {
	return strings.IndexByte(text, '#') >= 0 || strings.Contains(text, "--")
}

// splitInsertRows splits region at a row boundary near its midpoint. It is
// deliberately naive: it does not track quotes or comments, so it can pick a
// `),(` that lies inside a string or comment. That is still safe: a part A
// ending inside a string or /* */ comment is unterminated and fails to lex,
// while a -- or # line comment would eat the boundary `)` without a lex error
// and let part B start mid-comment, inventing rows — so a candidate is only
// accepted when A contains no -- or # at all. When both halves then lex and
// parse as complete row lists, A ends on a real `)` token (no lookahead
// dependence), tokens(A) + `,` + tokens(B) is exactly the token stream of the
// original region and the rows are rows(A) ++ rows(B).
func splitInsertRows(region string) (a, b string, ok bool) {
	for from := len(region) / 2; ; {
		i, k := nextRowBoundary(region, from)
		if i < 0 {
			return "", "", false
		}
		a = region[:i+1]
		if !hasLineComment(a) {
			return a, region[k:], true
		}
		from = i + 1
	}
}

func isByteSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}
