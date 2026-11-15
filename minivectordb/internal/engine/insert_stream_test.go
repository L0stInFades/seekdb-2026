package engine

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// streamKnobs saves and restores the package-level stream configuration.
func streamKnobs(t *testing.T, enabled bool, chunk int) {
	t.Helper()
	oldE, oldC := streamInsertEnabled, streamChunkBytes
	streamInsertEnabled, streamChunkBytes = enabled, chunk
	t.Cleanup(func() { streamInsertEnabled, streamChunkBytes = oldE, oldC })
}

// aliasStr returns a string that shares buf's backing array, like the SQL
// text a streamed request produces inside the line reader's buffer.
func aliasStr(buf []byte, n int) string {
	if n <= 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(buf), n)
}

// bulkWrite reports whether sql looks like an UPDATE or DELETE without a
// WHERE clause; its write order follows Go map iteration and is
// nondeterministic, so the differential test must not execute one.
func bulkWrite(sql string) bool {
	low := strings.ToLower(strings.TrimSpace(sql))
	if !strings.HasPrefix(low, "update") && !strings.HasPrefix(low, "delete") {
		return false
	}
	return !strings.Contains(low, " where ")
}

// runStreamDiff feeds random growing prefixes of each statement to the
// stream and then executes through ExecStream. It returns the per-statement
// results and the exact final SQL text each statement was executed with,
// which sometimes differs from the generated statement (shorter text, or a
// copy in a different buffer).
func runStreamDiff(t *testing.T, db *DB, s *InsertStream, rng *rand.Rand, stmts []string) ([]diffRes, []string) {
	t.Helper()
	outs := make([]diffRes, len(stmts))
	used := make([]string, len(stmts))
	for i, stmt := range stmts {
		buf := []byte(stmt)
		switch m := rng.Intn(10); {
		case m < 2:
			// No feeds at all.
		case m < 4:
			// One feed of the complete text.
			s.Feed(aliasStr(buf, len(buf)))
		default:
			// Many growing steps; cut points land anywhere, including
			// inside strings, comments and numbers.
			pos := 0
			for pos < len(buf) {
				pos += 1 + rng.Intn(len(buf)/5+17)
				if pos > len(buf) {
					pos = len(buf)
				}
				s.Feed(aliasStr(buf, pos))
			}
		}
		sql := aliasStr(buf, len(buf))
		switch rng.Intn(12) {
		case 0:
			// Final text in a different backing array: must fall back.
			sql = string(buf)
		case 1:
			// Cancelled mid-way.
			s.Cancel()
		case 2:
			// Final text shorter than what was fed. Cut inside the last few
			// bytes so the result is a parse error or, at worst, a point
			// write: a cut that leaves a complete WHERE-less UPDATE/DELETE
			// would write rows in nondeterministic map-iteration order and
			// two sequential databases could not produce identical logs.
			m := len(buf) - 1 - rng.Intn(6)
			if m < 1 {
				m = 1
			}
			cand := aliasStr(buf, m)
			if !bulkWrite(cand) {
				sql = cand
			}
		}
		used[i] = sql
		res, err := db.ExecStream(sql, s)
		o := &outs[i]
		if err != nil {
			o.errS = err.Error()
			o.name = errName(err)
		}
		if res != nil {
			o.aff = res.Affected
			o.hasAff = res.HasAffected
		}
		s.Reset()
		if i%50 == 0 || i == len(stmts)-1 {
			o.snaps = map[string]string{}
			for _, tb := range diffTables {
				r, e := db.Exec("SELECT * FROM " + tb.name + " ORDER BY id")
				if e != nil {
					t.Fatalf("snapshot %s after stmt %d: %v", tb.name, i, e)
				}
				bts, _ := json.Marshal(r.Rows)
				o.snaps[tb.name] = string(bts)
			}
		}
	}
	return outs, used
}

// TestInsertStreamMatchesExec runs the deterministic statement stream on a
// streamed database (random feeding + ExecStream) and a sequential one,
// comparing every result, periodic snapshots, the raw log and the reopened
// tables, across chunk sizes and GOMAXPROCS settings.
func TestInsertStreamMatchesExec(t *testing.T) {
	fastKnobs(t, true, 0, 0)
	streamKnobs(t, true, 0)
	const n = 3000
	stmts := newDiffGen(42).stream(n)
	for _, chunk := range []int{1, 64, 4096, 32768} {
		for _, gmp := range []int{1, 2} {
			streamChunkBytes = chunk
			old := runtime.GOMAXPROCS(gmp)
			dirF, dirS := t.TempDir(), t.TempDir()
			dbF, dbS := open(t, dirF), open(t, dirS)
			for _, tb := range diffTables {
				mustExec(t, dbF, tb.ddl)
				mustExec(t, dbS, tb.ddl)
			}
			stream := dbF.NewInsertStream()
			h0, f0 := streamInsertHandled.Load(), streamInsertFallback.Load()
			outF, used := runStreamDiff(t, dbF, stream, rand.New(rand.NewSource(7)), stmts)
			fastInsertEnabled = false
			outS := runDiffStream(t, dbS, used)
			fastInsertEnabled = true
			handled, fellBack := streamInsertHandled.Load()-h0, streamInsertFallback.Load()-f0
			for i := range stmts {
				f, s := outF[i], outS[i]
				if f.errS != s.errS || f.name != s.name || f.aff != s.aff || f.hasAff != s.hasAff {
					t.Fatalf("chunk=%d gmp=%d stmt %d %q:\nF: err=%q name=%s aff=%d has=%v\nS: err=%q name=%s aff=%d has=%v",
						chunk, gmp, i, stmts[i], f.errS, f.name, f.aff, f.hasAff, s.errS, s.name, s.aff, s.hasAff)
				}
				for tn, sf := range f.snaps {
					if sf != s.snaps[tn] {
						t.Fatalf("chunk=%d gmp=%d stmt %d table %s snapshot diverged:\nF: %s\nS: %s", chunk, gmp, i, tn, sf, s.snaps[tn])
					}
				}
			}
			if err := dbF.Close(); err != nil {
				t.Fatal(err)
			}
			if err := dbS.Close(); err != nil {
				t.Fatal(err)
			}
			lf, err := os.ReadFile(filepath.Join(dirF, "data.log"))
			if err != nil {
				t.Fatal(err)
			}
			ls, err := os.ReadFile(filepath.Join(dirS, "data.log"))
			if err != nil {
				t.Fatal(err)
			}
			if string(lf) != string(ls) {
				t.Fatalf("chunk=%d gmp=%d: data.log files differ (%d vs %d bytes)", chunk, gmp, len(lf), len(ls))
			}
			dbF, dbS = open(t, dirF), open(t, dirS)
			for _, tb := range diffTables {
				rf, _ := dbF.Exec("SELECT * FROM " + tb.name + " ORDER BY id")
				rs, _ := dbS.Exec("SELECT * FROM " + tb.name + " ORDER BY id")
				bf, _ := json.Marshal(rf.Rows)
				bs, _ := json.Marshal(rs.Rows)
				if string(bf) != string(bs) {
					t.Fatalf("chunk=%d gmp=%d: table %s differs after reopen", chunk, gmp, tb.name)
				}
			}
			dbF.Close()
			dbS.Close()
			runtime.GOMAXPROCS(old)
			t.Logf("chunk=%d gmp=%d: %d handled, %d fell back of %d statements", chunk, gmp, handled, fellBack, n)
		}
	}
}

// TestInsertStreamCounters checks that a benchmark-shaped statement fed in
// slices is handled by the stream with parts parsed off the calling
// goroutine, and that a duplicate key falls back to the sequential error.
func TestInsertStreamCounters(t *testing.T) {
	fastKnobs(t, true, 0, 0)
	streamKnobs(t, true, 4096)
	db := open(t, t.TempDir())
	defer db.Close()
	mustExec(t, db, "CREATE TABLE docs (id BIGINT PRIMARY KEY, category VARCHAR(16), embedding VECTOR(128))")
	var sb strings.Builder
	sb.WriteString("INSERT INTO docs VALUES ")
	for i := 1; i <= 100; i++ {
		if i > 1 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, "(%d,'c%d','[", i, i%7)
		for j := 0; j < 128; j++ {
			if j > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "0.%017d", i*1000+j)
		}
		sb.WriteString("]')")
	}
	stmt := sb.String()
	buf := []byte(stmt)
	stream := db.NewInsertStream()
	h0 := streamInsertHandled.Load()
	f0 := streamInsertFallback.Load()
	w0 := streamWorkerParts.Load()
	const slices = 8
	for i := 0; i < slices; i++ {
		stream.Feed(aliasStr(buf, (i+1)*len(buf)/slices))
		time.Sleep(20 * time.Millisecond)
	}
	res, err := db.ExecStream(aliasStr(buf, len(buf)), stream)
	stream.Reset()
	if err != nil {
		t.Fatalf("ExecStream: %v", err)
	}
	if !res.HasAffected || res.Affected != 100 {
		t.Fatalf("got %+v", res)
	}
	if got := streamInsertHandled.Load() - h0; got != 1 {
		t.Fatalf("streamInsertHandled moved by %d, want 1", got)
	}
	if got := streamWorkerParts.Load() - w0; got < 2 {
		t.Fatalf("streamWorkerParts moved by %d, want >= 2 off-caller parts", got)
	}
	// The same statement again hits the duplicate check and must fall back
	// to the sequential duplicate_key error.
	h0 = streamInsertHandled.Load()
	f0 = streamInsertFallback.Load()
	for i := 0; i < slices; i++ {
		stream.Feed(aliasStr(buf, (i+1)*len(buf)/slices))
	}
	_, err = db.ExecStream(aliasStr(buf, len(buf)), stream)
	stream.Reset()
	if errName(err) != "duplicate_key" {
		t.Fatalf("duplicate insert gave %v, want duplicate_key", err)
	}
	if got := streamInsertFallback.Load() - f0; got != 1 {
		t.Fatalf("streamInsertFallback moved by %d, want 1", got)
	}
	if got := streamInsertHandled.Load() - h0; got != 0 {
		t.Fatalf("streamInsertHandled moved by %d, want 0", got)
	}
	if n := len(mustExec(t, db, "SELECT * FROM docs ORDER BY id").Rows); n != 100 {
		t.Fatalf("rejected insert changed the table to %d rows", n)
	}
}
