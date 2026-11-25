package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func buildIndex(docs ...string) *index {
	ix := newIndex()
	for i, d := range docs {
		ix.addDoc(uint32(i), []byte(d))
	}
	return ix
}

func TestIndexWorkedExample(t *testing.T) {
	ix := buildIndex("Error: file_not_found (code 404)", "error error OK", "404")
	var buf bytes.Buffer
	if err := ix.write(&buf); err != nil {
		t.Fatal(err)
	}
	want := "03343034020002" + "04636f6465 0100" + "056572726f72020001" + "0466696c65 0100" +
		"05666f756e64 0100" + "036e6f74 0100" + "026f6b0101"
	if got := hex.EncodeToString(buf.Bytes()); got != strings.ReplaceAll(want, " ", "") {
		t.Fatalf("got %s want %s", got, want)
	}
	if ix.postings != 9 || len(ix.m) != 7 {
		t.Fatalf("postings %d tokens %d", ix.postings, len(ix.m))
	}
}

func TestTokenizer(t *testing.T) {
	ix := newIndex()
	// A token spans the join of the two parts; uppercase folds; bytes >= 0x80 are token bytes; 65-byte tokens drop, 64 stay.
	ix.addDoc(0, []byte("ab-AB"), []byte("cd e\xc3\xa9"), []byte(" "+strings.Repeat("x", 65)+" "+strings.Repeat("y", 64)))
	var got []string
	for k := range ix.m {
		got = append(got, k)
	}
	want := map[string]bool{"ab": true, "abcd": true, "e\xc3\xa9": true, strings.Repeat("y", 64): true}
	if len(got) != len(want) {
		t.Fatalf("tokens %q", got)
	}
	for _, k := range got {
		if !want[k] {
			t.Fatalf("unexpected token %q", k)
		}
	}
}

func TestIndexLargeDelta(t *testing.T) {
	ix := newIndex()
	ix.addDoc(1, []byte("a"))
	ix.addDoc(300, []byte("a"))
	var buf bytes.Buffer
	_ = ix.write(&buf)
	if got := hex.EncodeToString(buf.Bytes()); got != "0161"+"02"+"01"+"ab02" {
		t.Fatalf("got %s", got)
	}
}

func TestSampleIDs(t *testing.T) {
	mk := func(n int) (ids [][16]byte) {
		for i := 0; i < n; i++ {
			ids = append(ids, [16]byte{byte(i), byte(i >> 8)})
		}
		return
	}
	pos := func(ids [][16]byte) (p []int) {
		for _, id := range ids {
			p = append(p, int(id[0])|int(id[1])<<8)
		}
		return
	}
	for _, c := range []struct {
		distinct, n int
		want        []int
	}{
		{10, 200, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}}, // fewer than n: k = 1, all written
		{450, 200, nil},  // k = 2, stops at n
		{1000, 200, nil}, // k = 5
		{0, 200, []int{}},
	} {
		got := pos(sampleIDs(mk(c.distinct), c.n))
		if c.want != nil {
			if fmt.Sprint(got) != fmt.Sprint(c.want) {
				t.Fatalf("%v: got %v", c, got)
			}
			continue
		}
		k := max(1, c.distinct/c.n)
		if len(got) != min(c.n, (c.distinct+k-1)/k) || got[1] != k || got[len(got)-1] != (len(got)-1)*k {
			t.Fatalf("%v: k=%d got %d ids, last %d", c, k, len(got), got[len(got)-1])
		}
	}
}

// writeInput frames synthetic requests: traces of 10 spans, 100 traces per record.
func writeInput(t *testing.T, path string, traces int) (payload map[[16]byte]int64) {
	payload = map[[16]byte]int64{}
	f, _ := os.Create(path)
	defer f.Close()
	var m ptrace.ProtoMarshaler
	td := ptrace.NewTraces()
	flush := func() {
		b, err := m.MarshalTraces(td)
		if err != nil {
			t.Fatal(err)
		}
		var h [4]byte
		binary.BigEndian.PutUint32(h[:], uint32(len(b)))
		f.Write(h[:])
		f.Write(b)
		td = ptrace.NewTraces()
	}
	var ss ptrace.ScopeSpans
	for i := 0; i < traces; i++ {
		if i%100 == 0 {
			rs := td.ResourceSpans().AppendEmpty()
			rs.Resource().Attributes().PutStr("service.name", "svc")
			ss = rs.ScopeSpans().AppendEmpty()
		}
		var tid [16]byte
		binary.BigEndian.PutUint64(tid[8:], uint64(i)*0x9e3779b97f4a7c15+1)
		binary.BigEndian.PutUint64(tid[:8], uint64(i)+7)
		for s := 0; s < 10; s++ {
			sp := ss.Spans().AppendEmpty()
			sp.SetTraceID(tid)
			sp.SetSpanID(pcommon.SpanID{byte(s + 1)})
			if s > 0 {
				sp.SetParentSpanID(pcommon.SpanID{1})
			}
			sp.SetName(fmt.Sprintf("op %d", s))
			sp.SetKind(ptrace.SpanKindClient)
			sp.SetStartTimestamp(pcommon.Timestamp(1_000_000 + i*1000 + s))
			sp.SetEndTimestamp(pcommon.Timestamp(2_000_000 + i*1000 + s))
			a := sp.Attributes()
			a.PutStr("gen_ai.operation.name", "chat")
			a.PutStr("gen_ai.request.model", []string{"m1", "m2"}[i%2])
			a.PutInt("gen_ai.usage.input_tokens", 3)
			a.PutInt("gen_ai.usage.output_tokens", 2)
			a.PutStr("gen_ai.response.model", "other")
			if s%3 == 0 {
				v := strings.Repeat("z", 100+s)
				a.PutStr("gen_ai.input.messages", v)
				a.PutStr("gen_ai.tool.call.result", "R"+v)
				payload[tid] += int64(len(v)*2 + 1)
			}
		}
		if i%100 == 99 {
			flush()
		}
	}
	return payload
}

func TestPipeline(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.bin")
	const traces = 2000 // 20000 spans: 3 batches, 10 payload row groups
	payload := writeInput(t, in, traces)
	out := filepath.Join(dir, "out")
	_ = os.Mkdir(out, 0o755)
	st, err := convert(context.Background(), in, out, "all", 3)
	if err != nil {
		t.Fatal(err)
	}
	if st.rows != traces*10 || st.sumIn != 3*traces*10 || st.sumOut != 2*traces*10 || st.nonnull[0] != 4*traces || st.nonnull[1] != 0 {
		t.Fatalf("stats %+v", st)
	}
	s, err := openStore(out)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if !s.spans.hasBloom() {
		t.Fatal("no bloom filter written")
	}
	if n := s.pay.rdr.NumRowGroups(); n != 10 {
		t.Fatalf("payload row groups %d", n)
	}
	// Every trace is found, with the right row count and payload bytes.
	n := 0
	for tid, want := range payload {
		if n++; n%20 != 0 {
			continue
		}
		rows, pb, err := s.lookup(context.Background(), tid)
		if err != nil || rows != 10 || pb != want {
			t.Fatalf("lookup %x: rows %d bytes %d (want %d) err %v", tid, rows, pb, want, err)
		}
	}
	if rows, _, _ := s.lookup(context.Background(), [16]byte{0xff}); rows != 0 {
		t.Fatal("found a missing trace")
	}
	win, rows, _, err := s.agg(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := int64(1_000_000), int64(1_000_000+(traces-1)*1000+9)
	if win != [2]int64{lo + (hi-lo)/4, lo + 3*(hi-lo)/4} || len(rows) != 2 || *rows[0].Model != "m1" || rows[0].Count+rows[1].Count == 0 {
		t.Fatalf("agg %v %+v", win, rows)
	}
	var want [2]int64
	for i := 0; i < traces; i++ {
		for s := 0; s < 10; s++ {
			if ts := int64(1_000_000 + i*1000 + s); ts >= win[0] && ts < win[1] {
				want[i%2]++
			}
		}
	}
	if rows[0].Count != want[0] || rows[1].Count != want[1] || rows[0].InTokens != 3*want[0] || rows[1].OutToks != 2*want[1] {
		t.Fatalf("agg counts %+v want %v", rows, want)
	}
	// decode-only stage reports the same totals.
	st2, err := convert(context.Background(), in, "", "decode", 2)
	if err != nil || st2.rows != st.rows || st2.payBytes != st.payBytes {
		t.Fatalf("decode stage: %+v %v", st2, err)
	}
}
