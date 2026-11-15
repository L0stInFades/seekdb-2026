package main

import (
	"bytes"
	"io"
)

// lineReader returns request lines from src one at a time, equivalent to
// bufio.Reader.ReadSlice('\n') followed by the ReadBytes fallback for lines
// longer than the buffer: the line including its '\n', or on a source error
// before any '\n' the pending bytes together with that error, and the error
// alone once nothing is pending.
//
// Unlike bufio it reports partial lines: after every read that leaves the
// current line incomplete, onPartial sees buf[r:w] — a slice that keeps the
// same backing array and a strictly growing length until the line completes
// or a move happens. A move (unread bytes slid to the front, or the buffer
// grown) is announced by onMove before bytes change address, and only when
// onPartial was delivered for the current line.
type lineReader struct {
	src       io.Reader
	buf       []byte
	r, w      int
	err       error // sticky, like bufio's readErr
	partial   bool  // onPartial was delivered for the current line
	onPartial func(p []byte)
	onMove    func()
}

func (lr *lineReader) move() {
	if lr.partial && lr.onMove != nil {
		lr.onMove()
	}
	lr.partial = false
}

func (lr *lineReader) next() ([]byte, error) {
	if lr.r == lr.w {
		lr.r, lr.w = 0, 0
	}
	for {
		if i := bytes.IndexByte(lr.buf[lr.r:lr.w], '\n'); i >= 0 {
			b := lr.buf[lr.r : lr.r+i+1]
			lr.r += i + 1
			lr.partial = false
			return b, nil
		}
		if lr.err != nil {
			if lr.w > lr.r {
				b := lr.buf[lr.r:lr.w]
				lr.r = lr.w
				return b, lr.err
			}
			return nil, lr.err
		}
		if lr.w == len(lr.buf) {
			if lr.r > 0 {
				lr.move()
				copy(lr.buf, lr.buf[lr.r:lr.w])
				lr.w -= lr.r
				lr.r = 0
			} else {
				lr.move()
				nb := make([]byte, 2*len(lr.buf))
				copy(nb, lr.buf[:lr.w])
				lr.buf = nb
			}
		}
		n, err := lr.src.Read(lr.buf[lr.w:])
		lr.w += n
		if n > 0 && bytes.IndexByte(lr.buf[lr.r:lr.w], '\n') < 0 && lr.onPartial != nil {
			lr.onPartial(lr.buf[lr.r:lr.w])
			lr.partial = true
		}
		if err != nil {
			lr.err = err
		}
	}
}
