package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// cls maps a byte to its lower-cased token byte, or 0 when it separates tokens.
var cls = func() (t [256]byte) {
	for c := 0; c < 256; c++ {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c >= 0x80:
			t[c] = byte(c)
		case c >= 'A' && c <= 'Z':
			t[c] = byte(c + 32)
		}
	}
	return
}()

const maxToken = 64

type posting struct{ ids []uint32 }

// index maps token to the ascending distinct document ids that contain it.
type index struct {
	m        map[string]*posting
	postings int64
}

func newIndex() *index { return &index{m: map[string]*posting{}} }

// addDoc tokenises the concatenation of parts as one text, so a token may span a part boundary.
// Documents must be added in ascending id order.
func (ix *index) addDoc(doc uint32, parts ...[]byte) {
	var buf [maxToken]byte
	n := 0
	for _, p := range parts {
		for _, b := range p {
			if c := cls[b]; c != 0 {
				if n < maxToken {
					buf[n] = c
				}
				n++
			} else if n > 0 {
				ix.add(buf[:min(n, maxToken)], n, doc)
				n = 0
			}
		}
	}
	if n > 0 {
		ix.add(buf[:min(n, maxToken)], n, doc)
	}
}

// add records one token occurrence; n is the true token length (tokens over maxToken are dropped).
func (ix *index) add(tok []byte, n int, doc uint32) {
	if n > maxToken {
		return
	}
	p := ix.m[string(tok)]
	if p == nil {
		p = &posting{}
		ix.m[string(tok)] = p
	}
	if l := len(p.ids); l == 0 || p.ids[l-1] != doc {
		p.ids = append(p.ids, doc)
		ix.postings++
	}
}

// write emits tokens in ascending byte order in the documented varint/delta format.
func (ix *index) write(w io.Writer) error {
	keys := make([]string, 0, len(ix.m))
	for k := range ix.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for _, k := range keys {
		ids := ix.m[k].ids
		b = binary.AppendUvarint(b[:0], uint64(len(k)))
		b = append(b, k...)
		b = binary.AppendUvarint(b, uint64(len(ids)))
		prev := uint32(0)
		for _, id := range ids {
			b = binary.AppendUvarint(b, uint64(id-prev))
			prev = id
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func runIndex(ctx context.Context, dir, out string) (result, error) {
	if out == "" {
		return nil, fmt.Errorf("index needs --out")
	}
	p, err := openPq(filepath.Join(dir, "payloads.parquet"), 256)
	if err != nil {
		return nil, err
	}
	defer p.rdr.Close()
	ix := newIndex()
	var doc uint32
	var textBytes int64
	t0 := nowSec()
	// One row group at a time keeps the decoded text bounded.
	for rg := 0; rg < p.rdr.NumRowGroups(); rg++ {
		err := p.scan(ctx, []string{"output_messages", "tool_call_result"}, []int{rg}, func(rec arrow.RecordBatch, col func(string) arrow.Array) error {
			om, tr := col("output_messages").(*array.LargeBinary), col("tool_call_result").(*array.LargeBinary)
			for i := 0; i < om.Len(); i++ {
				var a, b []byte
				if om.IsValid(i) {
					a = om.Value(i)
				}
				if tr.IsValid(i) {
					b = tr.Value(i)
				}
				textBytes += int64(len(a) + len(b))
				ix.addDoc(doc, a, b)
				doc++
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	el := nowSec() - t0
	f, err := os.Create(out)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	cw := &countWriter{w: io.MultiWriter(f, h)}
	bw := bufio.NewWriterSize(cw, 1<<20)
	err = ix.write(bw)
	if err == nil {
		err = bw.Flush()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return nil, err
	}
	return result{"text_bytes": textBytes, "text_mb_per_s": float64(textBytes) / 1e6 / el, "docs": doc,
		"distinct_tokens": len(ix.m), "postings": ix.postings, "index_bytes": cw.n,
		"index_sha256": hex.EncodeToString(h.Sum(nil))}, nil
}
