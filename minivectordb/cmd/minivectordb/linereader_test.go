package main

import (
	"bufio"
	"bytes"
	"errors"
	"hash/fnv"
	"io"
	"math/rand"
	"testing"
)

// scriptReader delivers data in random chunks and ends with err, sometimes
// returning the error together with the last chunk of data.
type scriptReader struct {
	data     []byte
	pos      int
	err      error
	sameRead bool
	r        *rand.Rand
}

func (s *scriptReader) Read(p []byte) (int, error) {
	if s.pos >= len(s.data) {
		return 0, s.err
	}
	n := 1 + s.r.Intn(len(s.data)-s.pos)
	if n > 200<<10 {
		n = 200 << 10
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, s.data[s.pos:s.pos+n])
	s.pos += n
	if s.sameRead && s.pos >= len(s.data) {
		return n, s.err
	}
	return n, nil
}

var errScript = errors.New("scripted source error")

// refReader is the reference: exactly the closure main.go used to compute
// from bufio's ReadSlice + the ErrBufferFull/ReadBytes fallback.
type refReader struct {
	rd *bufio.Reader
}

func (r *refReader) next() ([]byte, error) {
	b, err := r.rd.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		head := append([]byte(nil), b...)
		var rest []byte
		rest, err = r.rd.ReadBytes('\n')
		b = append(head, rest...)
	}
	return b, err
}

// TestLineReaderMatchesBufio runs random line scripts through lineReader and
// the bufio reference, comparing every (bytes, error) pair and the partial
// callback contract.
func TestLineReaderMatchesBufio(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	var totalPartials, totalMoves int
	for script := 0; script < 20000; script++ {
		var data []byte
		for nlines := r.Intn(41); nlines > 0; nlines-- {
			ln := r.Intn(200)
			if r.Intn(40) == 0 {
				ln = 400 + r.Intn(3000)
			}
			if script >= 19900 && r.Intn(10) == 0 {
				ln = 1<<20 + r.Intn(2<<20) // 1-3 MiB
			}
			b := make([]byte, ln)
			r.Read(b)
			for i, c := range b {
				if c == '\n' {
					b[i] = '\r' + byte(i&1)
				}
			}
			data = append(data, b...)
			data = append(data, '\n')
		}
		if r.Intn(4) == 0 && len(data) > 0 {
			data = data[:len(data)-1] // missing final newline
		}
		srcErr := io.EOF
		if r.Intn(3) == 0 {
			srcErr = errScript
		}
		src := &scriptReader{data: data, err: srcErr, sameRead: r.Intn(2) == 0, r: r}
		ref := &refReader{rd: bufio.NewReaderSize(&scriptReader{
			data: append([]byte(nil), data...), err: srcErr, sameRead: src.sameRead,
			r: rand.New(rand.NewSource(r.Int63())),
		}, 1<<20)}
		// Callback bookkeeping: (len, bounded hash) of each partial for the
		// line being read.
		type partial struct {
			n int
			h uint64
		}
		partHash := func(b []byte) uint64 {
			h := fnv.New64a()
			k := min(len(b), 4096)
			h.Write(b[:k])
			h.Write(b[len(b)-k:])
			return h.Sum64()
		}
		var linePartials []partial
		lr := &lineReader{src: src, buf: make([]byte, 1<<20)}
		lr.onPartial = func(p []byte) {
			totalPartials++
			linePartials = append(linePartials, partial{len(p), partHash(p)})
		}
		lr.onMove = func() {
			totalMoves++
		}
		failures := 0
		for {
			rb, rerr := ref.next()
			lb, lerr := lr.next()
			if !bytes.Equal(rb, lb) {
				t.Fatalf("script %d: bytes differ\nref %q\nlr  %q", script, rb[:min(len(rb), 80)], lb[:min(len(lb), 80)])
			}
			if (rerr == nil) != (lerr == nil) || (rerr != nil && lerr != rerr) {
				t.Fatalf("script %d: errors differ: ref %v lr %v", script, rerr, lerr)
			}
			// Contract: every partial is a prefix of the returned line and
			// lengths strictly grow (within one backing array).
			prev := 0
			for _, p := range linePartials {
				if p.n <= prev {
					t.Fatalf("script %d: partial length %d not > %d", script, p.n, prev)
				}
				if p.n > len(lb) {
					t.Fatalf("script %d: partial %d > line %d (rb=%q rerr=%v lb=%q lerr=%v)",
						script, p.n, len(lb), rb[:min(len(rb), 60)], rerr, lb[:min(len(lb), 60)], lerr)
				}
				if partHash(lb[:p.n]) != p.h {
					t.Fatalf("script %d: partial is not a prefix of the line", script)
				}
				prev = p.n
			}
			linePartials = linePartials[:0]
			if len(rb) == 0 && rerr != nil {
				failures++
				if failures == 2 {
					break
				}
			}
		}
	}
	if totalPartials == 0 || totalMoves == 0 {
		t.Fatalf("callbacks not exercised: partials=%d moves=%d", totalPartials, totalMoves)
	}
	t.Logf("partials=%d moves=%d", totalPartials, totalMoves)
}
