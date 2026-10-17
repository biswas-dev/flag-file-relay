package main

// validate.go: checks that a flag or segment can be loaded by the strictest
// LaunchDarkly server SDK in use (Rust launchdarkly-server-sdk 2.1.0 /
// evaluation 1.2.0): required fields present with the right JSON types,
// including nested targets, rules, clauses and prerequisites. Optional fields
// may be absent (and, where the SDK allows it, null). Unknown fields are
// allowed and kept by the caller. Only shape is checked, never semantics.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
)

// parseU64 accepts a JSON integer token in the u64 range.
func parseU64(raw json.RawMessage) (uint64, error) {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || t[0] < '0' || t[0] > '9' {
		return 0, fmt.Errorf("not an unsigned integer")
	}
	return strconv.ParseUint(string(t), 10, 64)
}

// scanJSON rejects encodings serde_json refuses although Go accepts them:
// nesting deeper than 128, duplicate keys in an object, numbers outside the
// float64 range, and UTF-16 surrogate escapes that do not form a pair.
func scanJSON(b []byte) error {
	if err := checkSurrogates(b); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	type frame struct {
		obj  bool
		keys map[string]bool
		key  bool // next token in an object is a key
	}
	var stack []*frame
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		var top *frame
		if len(stack) > 0 {
			top = stack[len(stack)-1]
		}
		isKey := top != nil && top.obj && top.key
		switch v := tok.(type) {
		case json.Delim:
			switch v {
			case '{', '[':
				if top != nil && top.obj {
					top.key = true
				}
				if len(stack) >= 128 {
					return fmt.Errorf("JSON nested deeper than 128")
				}
				stack = append(stack, &frame{obj: v == '{', keys: map[string]bool{}, key: true})
			case '}', ']':
				stack = stack[:len(stack)-1]
			}
			continue
		case string:
			if isKey {
				if top.keys[v] {
					return fmt.Errorf("duplicate key %q", v)
				}
				top.keys[v] = true
				top.key = false
				continue
			}
		case json.Number:
			if f, err := strconv.ParseFloat(string(v), 64); err != nil || math.IsInf(f, 0) {
				return fmt.Errorf("number %s out of range", v)
			}
		}
		if top != nil && top.obj {
			top.key = true
		}
	}
}

// checkSurrogates walks every \uXXXX escape: a high surrogate must be
// followed by a low one, and a low one must follow a high one.
func checkSurrogates(b []byte) error {
	hex4 := func(i int) (int, bool) {
		if i+4 > len(b) {
			return 0, false
		}
		v, err := strconv.ParseUint(string(b[i:i+4]), 16, 16)
		return int(v), err == nil
	}
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' || i+1 >= len(b) {
			continue
		}
		if b[i+1] != 'u' {
			i++ // skip the escaped character
			continue
		}
		cp, ok := hex4(i + 2)
		if !ok {
			return fmt.Errorf("bad \\u escape")
		}
		switch {
		case cp >= 0xD800 && cp <= 0xDBFF:
			if i+11 < len(b) && b[i+6] == '\\' && b[i+7] == 'u' {
				if lo, ok := hex4(i + 8); ok && lo >= 0xDC00 && lo <= 0xDFFF {
					i += 11
					continue
				}
			}
			return fmt.Errorf("unpaired UTF-16 surrogate escape")
		case cp >= 0xDC00 && cp <= 0xDFFF:
			return fmt.Errorf("unpaired UTF-16 surrogate escape")
		}
		i += 5
	}
	return nil
}

// Context kinds as the Rust SDK accepts them: non-empty ASCII letters,
// digits, '.', '_' and '-', and not "kind" or "multi".
var kindPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func isKind(raw json.RawMessage) bool {
	var k string
	if !isStr(raw) || json.Unmarshal(raw, &k) != nil {
		return false
	}
	return kindPattern.MatchString(k) && k != "kind" && k != "multi"
}

type jsonObj map[string]json.RawMessage

func asObj(raw json.RawMessage) (jsonObj, bool) {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || t[0] != '{' {
		return nil, false
	}
	var o jsonObj
	if json.Unmarshal(t, &o) != nil {
		return nil, false
	}
	return o, true
}

func asArr(raw json.RawMessage) ([]json.RawMessage, bool) {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || t[0] != '[' {
		return nil, false
	}
	var a []json.RawMessage
	if json.Unmarshal(t, &a) != nil {
		return nil, false
	}
	return a, true
}

func isStr(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	return len(t) > 0 && t[0] == '"'
}

func isBool(raw json.RawMessage) bool {
	t := string(bytes.TrimSpace(raw))
	return t == "true" || t == "false"
}

func isNull(raw json.RawMessage) bool { return string(bytes.TrimSpace(raw)) == "null" }

// isInt: a JSON number token without fraction (never a quoted string).
func isInt(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || !(t[0] == '-' || (t[0] >= '0' && t[0] <= '9')) {
		return false
	}
	var n json.Number
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&n) != nil {
		return false
	}
	_, err := n.Int64()
	return err == nil
}

// isU64: a JSON integer in the u64 range (Rust u64 fields).
func isU64(raw json.RawMessage) bool {
	_, err := parseU64(raw)
	return err == nil
}

// isF32: a JSON number a Rust f32 field accepts.
func isF32(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || !(t[0] == '-' || (t[0] >= '0' && t[0] <= '9')) {
		return false
	}
	f, err := strconv.ParseFloat(string(t), 64)
	return err == nil && math.Abs(f) <= math.MaxFloat32
}

// field checks
type check func(json.RawMessage) bool

func strArr(raw json.RawMessage) bool {
	a, ok := asArr(raw)
	if !ok {
		return false
	}
	for _, v := range a {
		if !isStr(v) {
			return false
		}
	}
	return true
}

func arrOf(each func(jsonObj) error) func(json.RawMessage) error {
	return func(raw json.RawMessage) error {
		a, ok := asArr(raw)
		if !ok {
			return fmt.Errorf("not an array")
		}
		for i, v := range a {
			o, ok := asObj(v)
			if !ok {
				return fmt.Errorf("[%d] is not an object", i)
			}
			if err := each(o); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
		return nil
	}
}

// need: field must be present and pass c. opt: absent is fine (and null if nullOK).
func need(o jsonObj, name string, c check) error {
	v, ok := o[name]
	if !ok {
		return fmt.Errorf("missing %q", name)
	}
	if !c(v) {
		return fmt.Errorf("%q has the wrong type", name)
	}
	return nil
}

func opt(o jsonObj, name string, nullOK bool, c check) error {
	v, ok := o[name]
	if !ok || (nullOK && isNull(v)) {
		return nil
	}
	if !c(v) {
		return fmt.Errorf("%q has the wrong type", name)
	}
	return nil
}

func needEach(o jsonObj, name string, each func(jsonObj) error) error {
	v, ok := o[name]
	if !ok {
		return fmt.Errorf("missing %q", name)
	}
	if err := arrOf(each)(v); err != nil {
		return fmt.Errorf("%q%w", name, err)
	}
	return nil
}

func firstErr(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func validTarget(t jsonObj) error {
	return firstErr(need(t, "values", strArr), need(t, "variation", isInt), opt(t, "contextKind", false, isKind))
}

// validClause: contextKind is not checked: when it is unusable the Rust SDK
// falls back to the legacy clause form, which ignores it.
func validClause(c jsonObj) error {
	return firstErr(need(c, "attribute", isStr), need(c, "op", isStr),
		need(c, "values", func(r json.RawMessage) bool { _, ok := asArr(r); return ok }),
		opt(c, "negate", false, isBool))
}

func validFlagRule(r jsonObj) error {
	return firstErr(opt(r, "id", false, isStr), needEach(r, "clauses", validClause), need(r, "trackEvents", isBool))
}

func validPrereq(p jsonObj) error {
	return firstErr(need(p, "key", isStr), need(p, "variation", isInt))
}

func validFlag(f jsonObj) error {
	isArrC := func(r json.RawMessage) bool { _, ok := asArr(r); return ok }
	return firstErr(
		need(f, "key", isStr), need(f, "on", isBool), need(f, "variations", isArrC),
		// fallthrough: any JSON value (the SDK keeps a malformed one and
		// reports an evaluation error), but it must be present.
		need(f, "fallthrough", func(json.RawMessage) bool { return true }), need(f, "salt", isStr),
		needEach(f, "targets", validTarget), needEach(f, "rules", validFlagRule),
		needEach(f, "prerequisites", validPrereq),
		func() error {
			if _, ok := f["contextTargets"]; ok {
				return needEach(f, "contextTargets", validTarget)
			}
			return nil
		}(),
		opt(f, "offVariation", true, isInt), opt(f, "trackEvents", false, isBool),
		opt(f, "trackEventsFallthrough", false, isBool), opt(f, "debugEventsUntilDate", true, isU64),
		opt(f, "clientSide", false, isBool),
		opt(f, "clientSideAvailability", false, func(r json.RawMessage) bool {
			o, ok := asObj(r)
			return ok && need(o, "usingMobileKey", isBool) == nil && need(o, "usingEnvironmentId", isBool) == nil
		}),
	)
}

func validSegmentTarget(t jsonObj) error {
	return firstErr(need(t, "values", strArr), need(t, "contextKind", isKind))
}

func validSegmentRule(r jsonObj) error {
	return firstErr(opt(r, "id", true, isStr), needEach(r, "clauses", validClause),
		opt(r, "weight", true, isF32), opt(r, "bucketBy", true, isStr))
}

func validSegment(s jsonObj) error {
	optEach := func(name string, each func(jsonObj) error) error {
		if _, ok := s[name]; ok { // present: must be an array (null is not)
			return needEach(s, name, each)
		}
		return nil
	}
	return firstErr(
		need(s, "key", isStr), need(s, "included", strArr), need(s, "excluded", strArr),
		need(s, "salt", isStr), needEach(s, "rules", validSegmentRule),
		optEach("includedContexts", validSegmentTarget), optEach("excludedContexts", validSegmentTarget),
		opt(s, "unbounded", false, isBool), opt(s, "generation", true, isInt),
	)
}
