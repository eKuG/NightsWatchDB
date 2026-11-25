package main

import (
	"encoding/binary"
	"math/bits"
)

// Hand-rolled protobuf encoding for the few OTLP messages we emit. Spans are
// encoded once, by the worker that builds them, as a ready-to-concatenate
// ScopeSpans.spans entry.

func varintLen(x uint64) int { return (bits.Len64(x|1) + 6) / 7 }

func appendVarint(b []byte, x uint64) []byte {
	for x >= 0x80 {
		b = append(b, byte(x)|0x80)
		x >>= 7
	}
	return append(b, byte(x))
}

func appendFixed64(b []byte, x uint64) []byte { return binary.LittleEndian.AppendUint64(b, x) }

// Attribute value kinds.
const (
	kStr    = iota // string made of parts, optionally wrapped as a JSON array
	kInt           // int64
	kStrArr        // array of short strings
)

type attr struct {
	key   string
	kind  int
	parts [][]byte // kStr: concatenated, or comma-joined inside [ ] when arr is set
	arr   bool
	n     int64
	strs  []string
}

func str(key, v string) attr        { return attr{key: key, parts: [][]byte{[]byte(v)}} }
func raw(key string, v []byte) attr { return attr{key: key, parts: [][]byte{v}} }
func num(key string, n int64) attr  { return attr{key: key, kind: kInt, n: n} }

func (a *attr) strLen() int {
	n := 0
	for _, p := range a.parts {
		n += len(p)
	}
	if a.arr {
		n += 2
		if len(a.parts) > 1 {
			n += len(a.parts) - 1
		}
	}
	return n
}

func strArrInner(strs []string) int {
	n := 0
	for _, s := range strs {
		l := 1 + varintLen(uint64(len(s))) + len(s)
		n += 1 + varintLen(uint64(l)) + l
	}
	return n
}

// anyLen is the encoded size of the AnyValue message.
func (a *attr) anyLen() int {
	switch a.kind {
	case kInt:
		return 1 + varintLen(uint64(a.n))
	case kStrArr:
		in := strArrInner(a.strs)
		return 1 + varintLen(uint64(in)) + in
	}
	l := a.strLen()
	return 1 + varintLen(uint64(l)) + l
}

func (a *attr) kvLen() int {
	al := a.anyLen()
	return 1 + varintLen(uint64(len(a.key))) + len(a.key) + 1 + varintLen(uint64(al)) + al
}

func (a *attr) fullLen() int {
	kl := a.kvLen()
	return 1 + varintLen(uint64(kl)) + kl
}

// payload is the number of string value bytes carried by the attribute.
func (a *attr) payload() int {
	if a.kind == kStr {
		return a.strLen()
	}
	return 0
}

// appendAttr writes the attribute as a KeyValue field with the given tag.
func appendAttr(b []byte, tag byte, a *attr) []byte {
	b = append(b, tag)
	b = appendVarint(b, uint64(a.kvLen()))
	b = append(b, 0x0A)
	b = appendVarint(b, uint64(len(a.key)))
	b = append(b, a.key...)
	b = append(b, 0x12)
	b = appendVarint(b, uint64(a.anyLen()))
	switch a.kind {
	case kInt:
		b = append(b, 0x18)
		b = appendVarint(b, uint64(a.n))
	case kStrArr:
		b = append(b, 0x2A)
		b = appendVarint(b, uint64(strArrInner(a.strs)))
		for _, s := range a.strs {
			l := 1 + varintLen(uint64(len(s))) + len(s)
			b = append(b, 0x0A)
			b = appendVarint(b, uint64(l))
			b = append(b, 0x0A)
			b = appendVarint(b, uint64(len(s)))
			b = append(b, s...)
		}
	default:
		b = append(b, 0x0A)
		b = appendVarint(b, uint64(a.strLen()))
		if a.arr {
			b = append(b, '[')
		}
		for i, p := range a.parts {
			if i > 0 && a.arr {
				b = append(b, ',')
			}
			b = append(b, p...)
		}
		if a.arr {
			b = append(b, ']')
		}
	}
	return b
}

// OTLP span kinds and status codes.
const (
	kindInternal = 1
	kindClient   = 3
	statusError  = 2
)

type span struct {
	traceID    [16]byte
	id, parent [8]byte
	hasParent  bool
	name       string
	kind       int
	start, end int64
	attrs      []attr
	isErr      bool
	chat       bool // LLM call span
	agent      int  // agent index for chat spans, -1 otherwise
}

// encode returns the span as a ScopeSpans.spans entry (field 2) and the number
// of string attribute value bytes it carries.
func (s *span) encode() ([]byte, int) {
	size := 18 + 10 + 1 + varintLen(uint64(len(s.name))) + len(s.name) + 2 + 9 + 9
	if s.hasParent {
		size += 10
	}
	payload := 0
	for i := range s.attrs {
		size += s.attrs[i].fullLen()
		payload += s.attrs[i].payload()
	}
	if s.isErr {
		size += 4
	}
	b := make([]byte, 0, 1+varintLen(uint64(size))+size)
	b = append(b, 0x12)
	b = appendVarint(b, uint64(size))
	b = append(b, 0x0A, 16)
	b = append(b, s.traceID[:]...)
	b = append(b, 0x12, 8)
	b = append(b, s.id[:]...)
	if s.hasParent {
		b = append(b, 0x22, 8)
		b = append(b, s.parent[:]...)
	}
	b = append(b, 0x2A)
	b = appendVarint(b, uint64(len(s.name)))
	b = append(b, s.name...)
	b = append(b, 0x30, byte(s.kind))
	b = append(b, 0x39)
	b = appendFixed64(b, uint64(s.start))
	b = append(b, 0x41)
	b = appendFixed64(b, uint64(s.end))
	for i := range s.attrs {
		b = appendAttr(b, 0x4A, &s.attrs[i])
	}
	if s.isErr {
		b = append(b, 0x7A, 2, 0x18, statusError)
	}
	return b, payload
}

// appendEscaped appends s with JSON string escaping (no surrounding quotes).
func appendEscaped(dst, s []byte) []byte {
	start := 0
	for i, c := range s {
		if c >= 0x20 && c != '"' && c != '\\' {
			continue
		}
		dst = append(dst, s[start:i]...)
		switch c {
		case '"', '\\':
			dst = append(dst, '\\', c)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = append(dst, '\\', 'u', '0', '0', "0123456789abcdef"[c>>4], "0123456789abcdef"[c&15])
		}
		start = i + 1
	}
	return append(dst, s[start:]...)
}

// encodeResource returns the Resource message body for the given attributes.
func encodeResource(attrs []attr) []byte {
	var b []byte
	for i := range attrs {
		b = appendAttr(b, 0x0A, &attrs[i])
	}
	return b
}

// encodeScope returns the ScopeSpans.scope field (tag 1) for the fixed scope.
func encodeScope(name, version string) []byte {
	in := []byte{0x0A}
	in = appendVarint(in, uint64(len(name)))
	in = append(in, name...)
	in = append(in, 0x12)
	in = appendVarint(in, uint64(len(version)))
	in = append(in, version...)
	b := []byte{0x0A}
	b = appendVarint(b, uint64(len(in)))
	return append(b, in...)
}

// dropContent removes the content attributes, keeping all others in order.
func (s *span) dropContent() {
	kept := s.attrs[:0]
	for _, a := range s.attrs {
		switch a.key {
		case names.InputMsgs, names.OutputMsgs, names.SystemInst, names.ToolDefs, names.ToolArgs, names.ToolResult:
		default:
			kept = append(kept, a)
		}
	}
	s.attrs = kept
}
