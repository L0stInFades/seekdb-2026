package proto

import "bytes"

// SQLStart scans the beginning of a possibly incomplete request line and,
// when it can locate the "sql" string value, returns the offset just past
// its opening quote. It is only a hint for speculative feeding; ParseRequest
// remains the authoritative decode.
//
// The accepted shape is `{`, then key/value pairs with optional JSON
// whitespace around tokens, where a value before "sql" is a JSON number
// (-?digits) or a string with no backslash and no control bytes. Anything
// else — escapes, nested values, non-object input, or running out of bytes
// first — reports ok=false.
func SQLStart(line []byte) (start int, ok bool) {
	i, n := 0, len(line)
	ws := func() {
		for i < n && (line[i] == ' ' || line[i] == '\t' || line[i] == '\r' || line[i] == '\n') {
			i++
		}
	}
	strEnd := func() int {
		// From i at an opening quote: end of a plain string, -1 if it runs
		// out, -2 if it contains a backslash or a control byte.
		j := bytes.IndexByte(line[i+1:], '"')
		if j < 0 {
			return -1
		}
		v := line[i+1 : i+1+j]
		if bytes.IndexByte(v, '\\') >= 0 {
			return -2
		}
		for _, c := range v {
			if c < 0x20 {
				return -2
			}
		}
		return i + 1 + j
	}
	ws()
	if i >= n || line[i] != '{' {
		return 0, false
	}
	i++
	for {
		ws()
		if i >= n || line[i] != '"' {
			return 0, false
		}
		j := strEnd()
		if j < 0 {
			return 0, false
		}
		key := string(line[i+1 : j])
		i = j + 1
		ws()
		if i >= n || line[i] != ':' {
			return 0, false
		}
		i++
		ws()
		if key == "sql" {
			if i >= n || line[i] != '"' {
				return 0, false
			}
			return i + 1, true
		}
		if i >= n {
			return 0, false
		}
		if line[i] == '"' {
			j = strEnd()
			if j < 0 {
				return 0, false
			}
			i = j + 1
		} else {
			if line[i] == '-' {
				i++
			}
			d := 0
			for i < n && line[i] >= '0' && line[i] <= '9' {
				i++
				d++
			}
			if d == 0 {
				return 0, false
			}
		}
		ws()
		if i >= n || line[i] != ',' {
			return 0, false
		}
		i++
	}
}
