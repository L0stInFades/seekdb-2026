package proto

import (
	"bytes"
	"encoding/json"
	"testing"
)

// decodeSQL decodes the complete request with encoding/json (authoritative)
// and returns the sql value.
func decodeSQL(t *testing.T, line []byte) string {
	t.Helper()
	var req struct {
		SQL *string `json:"sql"`
	}
	if err := json.Unmarshal(line, &req); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if req.SQL == nil {
		t.Fatal("no sql field")
	}
	return *req.SQL
}

// TestSQLStart checks the prefix scanner over generated request shapes,
// including truncation at every byte offset and unsupported forms.
func TestSQLStart(t *testing.T) {
	// Shapes like Python json.dumps produces, plus compact and
	// odd-whitespace variants, string and negative ids, sql first/last.
	lines := []string{
		`{"id": 7, "op": "execute", "sql": "INSERT INTO docs VALUES (1,'a'),(2,'b')", "search_mode": "exact"}`,
		`{"id":7,"op":"execute","sql":"SELECT * FROM t WHERE id=3","search_mode":"ann"}`,
		`{"sql": "SELECT 1", "id": "req-41", "op": "execute"}`,
		`{"id" : -42 ,  "op" : "execute" ,"sql" : "SELECT 'x''y'" , "search_mode" : "exact" }`,
		"{\"id\":0,\"op\":\"execute\",\"sql\":\"INSERT INTO t VALUES (1,'v')\"}",
		"{\n\t\"id\": 12,  \"op\": \"execute\",\t\"sql\": \"SELECT 2\"\r\n}",
		`{"op": "execute", "sql": "x", "id": 9, "search_mode": "exact"}`,
		`{"id": -1, "op": "execute", "search_mode": "ann", "sql": "tail last"}`,
	}
	for _, line := range lines {
		b := []byte(line)
		full := decodeSQL(t, b)
		for cut := 0; cut <= len(b); cut++ {
			pref := b[:cut]
			start, ok := SQLStart(pref)
			if !ok {
				continue
			}
			// When ok, the bytes from start to the closing quote must be a
			// prefix of the decoded sql of the complete line (the decoded
			// text may differ at escapes; none of these have any).
			end := bytes.IndexByte(pref[start:], '"')
			var got string
			if end < 0 {
				got = string(pref[start:])
			} else {
				got = string(pref[start : start+end])
			}
			if len(got) > len(full) || full[:len(got)] != got {
				t.Fatalf("%q cut=%d: SQLStart gave %q, want prefix of %q", line, cut, got, full)
			}
		}
		// On the complete line the offset must land exactly at the sql
		// value's first content byte.
		start, ok := SQLStart(b)
		if !ok {
			t.Fatalf("%q: SQLStart failed on a complete line", line)
		}
		if string(b[start-1]) != `"` {
			t.Fatalf("%q: start %d not just past the opening quote", line, start)
		}
		if got := string(b[start : start+len(full)]); got != full {
			t.Fatalf("%q: sql text at start = %q, want %q", line, got, full)
		}
	}
	// Unsupported forms must report ok=false.
	for _, line := range []string{
		`{"id": "a\"b", "sql": "x"}`,   // escaped string before sql
		`{"id": {"a": 1}, "sql": "x"}`, // nested object before sql
		`{"id": [1, 2], "sql": "x"}`,   // nested array before sql
		`{"id": true, "sql": "x"}`,     // bool before sql
		`{"id": null, "sql": "x"}`,     // null before sql
		`["id", 7, "sql"]`,             // not an object
		`"just a string"`,              // not an object
		`{"id": 7.5, "sql": "x"}`,      // non-integer number
		`{"id": 7, "sql": 12}`,         // sql not a string
		`{"id": 7}`,                    // no sql key
		`{}`,                           // empty object
		``,                             // empty
		"  ",                           // whitespace only
		`{"sqlx": "y", "id": 1}`,       // similar key
		"{\"id\": 7,\t\"op\": \"execute\", \"search_mode\": \"exact\"}", // no sql
	} {
		for cut := 1; cut <= len(line); cut++ {
			if _, ok := SQLStart([]byte(line)[:cut]); ok {
				t.Fatalf("%q cut=%d: SQLStart ok on an unsupported form", line, cut)
			}
		}
	}
}
