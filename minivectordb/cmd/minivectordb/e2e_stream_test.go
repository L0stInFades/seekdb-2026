package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testBin string

// TestMain builds the real binary once for the end-to-end tests.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mvdb-e2e")
	if err != nil {
		panic(err)
	}
	testBin = filepath.Join(dir, "mvdb")
	out, err := exec.Command("go", "build", "-o", testBin, ".").CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("build: %v\n%s", err, out))
	}
	os.Exit(m.Run())
}

// runSession starts the binary on a fresh data dir with the given env and
// writes requests; when chunked is true each line is split into random
// writes with short sleeps, like the docker -i pipe delivers them.
func runSession(t *testing.T, env []string, lines [][]byte, chunked bool, r *rand.Rand) []byte {
	t.Helper()
	cmd := exec.Command(testBin, "--data-dir", t.TempDir())
	cmd.Env = append(os.Environ(), env...)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	cmd.Stdout = &out
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		if !chunked {
			if _, err := in.Write(line); err != nil {
				t.Fatal(err)
			}
			continue
		}
		for off := 0; off < len(line); {
			n := 1 + r.Intn(1<<16)
			if n > len(line)-off {
				n = len(line) - off
			}
			if _, err := in.Write(line[off : off+n]); err != nil {
				t.Fatal(err)
			}
			off += n
			time.Sleep(time.Duration(r.Intn(300)) * time.Microsecond)
		}
	}
	in.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("binary exited: %v\nstderr: %s", err, errb.String())
	}
	return out.Bytes()
}

// TestStreamedStdinEqualsDisabled sends mixed requests to the real binary in
// chunked writes with stream speculation enabled, and compares every
// response byte with the same binary run under MVDB_NO_STREAM_INSERT.
func TestStreamedStdinEqualsDisabled(t *testing.T) {
	vec := func(n int) string {
		var sb strings.Builder
		sb.WriteByte('[')
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "0.%d", i+1)
		}
		sb.WriteByte(']')
		return sb.String()
	}
	insert := func(id0, rows int) string {
		var sb strings.Builder
		sb.WriteString("INSERT INTO docs VALUES ")
		for i := 0; i < rows; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "(%d,'row%d','%s')", id0+i, id0+i, vec(128))
		}
		return sb.String()
	}
	mkreq := func(id int, sql string) []byte {
		b, _ := json.Marshal(map[string]any{"id": id, "op": "execute", "sql": sql})
		return append(b, '\n')
	}
	var lines [][]byte
	lines = append(lines, mkreq(1, "CREATE TABLE docs (id BIGINT PRIMARY KEY, category VARCHAR(16), embedding VECTOR(128))"))
	lines = append(lines, mkreq(2, insert(1, 3)))
	lines = append(lines, mkreq(3, "SELECT * FROM docs ORDER BY id"))
	// A ~300 KB INSERT.
	lines = append(lines, mkreq(4, insert(10, 130)))
	// An INSERT whose JSON contains \n escapes inside a string value.
	lines = append(lines, mkreq(5, "INSERT INTO docs VALUES (200,'a\\nb','"+vec(128)+"')"))
	// A duplicate-key INSERT (id 10 exists).
	lines = append(lines, mkreq(6, "INSERT INTO docs VALUES (10,'dup','[0.5]')"))
	lines = append(lines, mkreq(7, "SELECT * FROM docs ORDER BY id"))
	lines = append(lines, []byte("\n")) // blank line
	// A line longer than 1 MiB: a huge INSERT.
	lines = append(lines, mkreq(8, insert(300, 450)))
	lines = append(lines, mkreq(9, "SELECT * FROM docs ORDER BY id"))
	lines = append(lines, mkreq(10, "shutdown"))

	streamed := runSession(t, []string{"MVDB_STREAM_MIN=0", "MVDB_STREAM_CHUNK=64", "MVDB_FAST_INSERT_MIN=0"},
		lines, true, rand.New(rand.NewSource(11)))
	plain := runSession(t, []string{"MVDB_NO_STREAM_INSERT=1"}, lines, false, nil)
	if !bytes.Equal(streamed, plain) {
		a := bytes.Split(streamed, []byte("\n"))
		b := bytes.Split(plain, []byte("\n"))
		for i := 0; i < len(a) || i < len(b); i++ {
			var x, y string
			if i < len(a) {
				x = string(a[i])
			}
			if i < len(b) {
				y = string(b[i])
			}
			if x != y {
				t.Fatalf("response line %d differs:\nstream: %.200s\nplain:  %.200s", i, x, y)
			}
		}
	}
}
