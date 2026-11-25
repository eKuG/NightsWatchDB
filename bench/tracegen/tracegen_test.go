package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
)

var words = strings.Fields(`the storage engine writes sorted runs to disk and merges them in the background while readers
consult an index of block offsets before decoding a page which keeps latency low under load although
compaction can stall writes when levels grow faster than the merge budget allows so operators watch
amplification metrics closely and tune the size ratio between levels depending on whether the workload
favours point lookups or long scans of recent data in time order`)

func corpusDir(t *testing.T) string {
	dir := t.TempDir()
	r := rand.New(rand.NewPCG(7, 7))
	var sb strings.Builder
	for sb.Len() < 300_000 {
		sb.WriteString(words[r.IntN(len(words))])
		if r.IntN(12) == 0 {
			sb.WriteString(".\n\"café\" ")
		} else {
			sb.WriteByte(' ')
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func testConfig(t *testing.T, mix string, traces int, seed uint64) *config {
	m, err := parseMix(mix)
	if err != nil {
		t.Fatal(err)
	}
	return &config{seed: seed, traces: traces, corpusDir: corpusDir(t), corpusMax: 1 << 30, mix: m,
		tracesPerSession: 3, contextCap: 500_000, maxPayload: 1 << 20, window: 24 * time.Hour,
		start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), batchSpans: 512, batchBytes: 3 << 20}
}

func generate(t *testing.T, c *config) (string, *summary) {
	path := filepath.Join(t.TempDir(), "out.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	c.sinks = []sink{newBufFileSink(f)}
	sum, err := run(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.sinks[0].close(); err != nil {
		t.Fatal(err)
	}
	return path, sum
}

func readBatches(t *testing.T, path string) (batches []ptrace.Traces, sizes []int) {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for len(b) > 0 {
		n := int(binary.BigEndian.Uint32(b))
		td, err := (&ptrace.ProtoUnmarshaler{}).UnmarshalTraces(b[4 : 4+n])
		if err != nil {
			t.Fatal(err)
		}
		batches, sizes = append(batches, td), append(sizes, n)
		b = b[4+n:]
	}
	return
}

func eachSpan(batches []ptrace.Traces, f func(ptrace.Span)) {
	for _, td := range batches {
		for i := 0; i < td.ResourceSpans().Len(); i++ {
			rs := td.ResourceSpans().At(i)
			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				ss := rs.ScopeSpans().At(j)
				for k := 0; k < ss.Spans().Len(); k++ {
					f(ss.Spans().At(k))
				}
			}
		}
	}
}

func sha(t *testing.T, path string) [32]byte {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(b)
}

func TestDeterministic(t *testing.T) {
	c1 := testConfig(t, "chat=0.5,task=0.5", 40, 5)
	p1, _ := generate(t, c1)
	c2 := testConfig(t, "chat=0.5,task=0.5", 40, 5)
	p2, _ := generate(t, c2)
	c3 := testConfig(t, "chat=0.5,task=0.5", 40, 6)
	p3, _ := generate(t, c3)
	if sha(t, p1) != sha(t, p2) {
		t.Error("same seed produced different output")
	}
	if sha(t, p1) == sha(t, p3) {
		t.Error("different seed produced identical output")
	}
}

func TestStructure(t *testing.T) {
	c := testConfig(t, "chat=0.5,task=0.5", 60, 1)
	path, sum := generate(t, c)
	batches, _ := readBatches(t, path)
	type tr struct {
		spans map[pcommon.SpanID]ptrace.Span
		roots int
	}
	traces := map[pcommon.TraceID]*tr{}
	var rootStarts []pcommon.Timestamp
	eachSpan(batches, func(s ptrace.Span) {
		x := traces[s.TraceID()]
		if x == nil {
			x = &tr{spans: map[pcommon.SpanID]ptrace.Span{}}
			traces[s.TraceID()] = x
		}
		x.spans[s.SpanID()] = s
		if s.ParentSpanID().IsEmpty() {
			x.roots++
			rootStarts = append(rootStarts, s.StartTimestamp())
		}
	})
	if uint64(len(traces)) != sum.traces {
		t.Errorf("distinct trace ids %d, want %d", len(traces), sum.traces)
	}
	for id, x := range traces {
		if x.roots != 1 {
			t.Fatalf("trace %s has %d roots", id, x.roots)
		}
		for _, s := range x.spans {
			if s.ParentSpanID().IsEmpty() {
				continue
			}
			p, ok := x.spans[s.ParentSpanID()]
			if !ok {
				t.Fatalf("trace %s: parent missing", id)
			}
			if s.StartTimestamp() < p.StartTimestamp() || s.EndTimestamp() > p.EndTimestamp() {
				t.Fatalf("trace %s: child %q outside parent %q", id, s.Name(), p.Name())
			}
		}
	}
	for i := 1; i < len(rootStarts); i++ {
		if rootStarts[i] < rootStarts[i-1] {
			t.Fatalf("trace start decreased at %d", i)
		}
	}
	if len(rootStarts) != 60 {
		t.Errorf("got %d traces", len(rootStarts))
	}
}

func TestLimits(t *testing.T) {
	c := testConfig(t, "chat=0.3,task=0.7", 30, 2)
	c.maxPayload, c.batchSpans, c.batchBytes = 8192, 50, 1<<16
	path, _ := generate(t, c)
	batches, sizes := readBatches(t, path)
	var big int
	for i, td := range batches {
		if td.SpanCount() > c.batchSpans {
			t.Errorf("batch has %d spans", td.SpanCount())
		}
		if sizes[i] > c.batchBytes+4096 {
			t.Errorf("batch is %d bytes", sizes[i])
		}
	}
	eachSpan(batches, func(s ptrace.Span) {
		s.Attributes().Range(func(k string, v pcommon.Value) bool {
			if v.Type() == pcommon.ValueTypeStr {
				if len(v.Str()) > c.maxPayload {
					t.Errorf("%s is %d bytes", k, len(v.Str()))
				}
				if len(v.Str()) > c.maxPayload/2 {
					big++
				}
			}
			return true
		})
	})
	if big == 0 {
		t.Error("payload cap was never approached; test is not exercising truncation")
	}
}

type countServer struct {
	ptraceotlp.UnimplementedGRPCServer
	spans atomic.Int64
}

func (s *countServer) Export(_ context.Context, r ptraceotlp.ExportRequest) (ptraceotlp.ExportResponse, error) {
	s.spans.Add(int64(r.Traces().SpanCount()))
	return ptraceotlp.NewExportResponse(), nil
}

func TestGRPC(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv, cs := grpc.NewServer(), &countServer{}
	ptraceotlp.RegisterGRPCServer(srv, cs)
	go srv.Serve(lis)
	defer srv.Stop()

	gs, err := newGRPCSink(lis.Addr().String(), 0)
	if err != nil {
		t.Fatal(err)
	}
	c := testConfig(t, "simple=0.8,task=0.2", 40, 3)
	c.sinks = []sink{gs}
	sum, err := run(c)
	if err != nil {
		t.Fatal(err)
	}
	gs.close()
	if sum.spans == 0 || uint64(cs.spans.Load()) != sum.spans || gs.accepted != sum.spans {
		t.Errorf("generated %d, server got %d, sink accepted %d", sum.spans, cs.spans.Load(), gs.accepted)
	}
}

func TestGRPCFailureIsLoud(t *testing.T) {
	gs, err := newGRPCSink("127.0.0.1:1", 0)
	if err != nil {
		t.Fatal(err)
	}
	c := testConfig(t, "chat=1", 5, 3)
	c.sinks = []sink{gs}
	if _, err := run(c); err == nil {
		t.Error("expected an export error")
	}
}

type msg struct {
	Role  string `json:"role"`
	Parts []struct {
		Content  string `json:"content"`
		Response string `json:"response"`
		ID       string `json:"id"`
	} `json:"parts"`
}

func (m msg) key() string {
	p := m.Parts[0]
	return m.Role + p.Content + p.Response + p.ID
}

func TestAccumulation(t *testing.T) {
	c := testConfig(t, "task=1", 12, 4)
	c.contextCap = 12_000
	path, _ := generate(t, c)
	batches, _ := readBatches(t, path)
	type chat struct {
		in  []msg
		out string
	}
	byParent := map[pcommon.SpanID][]chat{}
	eachSpan(batches, func(s ptrace.Span) {
		a := s.Attributes()
		if op, _ := a.Get("gen_ai.operation.name"); op.Str() != "chat" {
			return
		}
		var ch chat
		in, _ := a.Get("gen_ai.input.messages")
		out, _ := a.Get("gen_ai.output.messages")
		var outs []msg
		if err := json.Unmarshal([]byte(in.Str()), &ch.in); err != nil {
			t.Fatalf("input is not JSON: %v", err)
		}
		if err := json.Unmarshal([]byte(out.Str()), &outs); err != nil || len(outs) != 1 {
			t.Fatalf("output is not one message: %v", err)
		}
		ch.out = outs[0].Parts[0].Content
		byParent[s.ParentSpanID()] = append(byParent[s.ParentSpanID()], ch)
	})
	contained, compacted := 0, 0
	for _, chats := range byParent {
		for i := 1; i < len(chats); i++ {
			prev, cur := chats[i-1], chats[i]
			found := false
			for _, m := range cur.in {
				if m.Role == "assistant" && m.Parts[0].Content == prev.out {
					found = true
				}
			}
			if !found && cur.in[0].key() == prev.in[0].key() {
				t.Fatal("later input lost the earlier assistant output without compaction")
			}
			if found {
				contained++
			}
			if cur.in[0].key() != prev.in[0].key() {
				compacted++
			}
		}
	}
	if contained < 50 || compacted == 0 {
		t.Errorf("contained=%d compacted=%d; expected many contained and some compaction", contained, compacted)
	}
}

func TestBytesStop(t *testing.T) {
	c := testConfig(t, "simple=0.9,task=0.1", 0, 9)
	c.bytes = 3_000_000
	path, sum := generate(t, c)
	st, _ := os.Stat(path)
	if st.Size() < c.bytes || st.Size() != sum.bytes {
		t.Errorf("file %d bytes, summary %d, want >= %d", st.Size(), sum.bytes, c.bytes)
	}
}

func TestCorpusErrors(t *testing.T) {
	if _, err := loadCorpus(t.TempDir(), 1<<30); err == nil {
		t.Error("empty corpus accepted")
	}
	if _, err := loadCorpus(filepath.Join(t.TempDir(), "nope"), 1<<30); err == nil {
		t.Error("missing corpus accepted")
	}
	d := t.TempDir()
	os.WriteFile(filepath.Join(d, "x"), bytes.Repeat([]byte("é"), 5000), 0o644)
	got, err := loadCorpus(d, 4097)
	if err != nil || len(got.text[roleTool]) < 4096 {
		t.Errorf("cap handling: %v", err)
	}
}

// writeRecords writes entries in the role-directory record format.
func writeRecords(t *testing.T, path string, entries ...string) {
	var b []byte
	for _, e := range entries {
		b = binary.BigEndian.AppendUint32(b, uint32(len(e)))
		b = append(b, e...)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRoleCorpus(t *testing.T) {
	c := testConfig(t, "task=1", 10, 11)
	writeRecords(t, filepath.Join(c.corpusDir, "tooldefs", "s0"),
		`[{"name":"a"}]`, `[{"name":"b","x":"`+strings.Repeat("y", 3000)+`"}]`)
	writeRecords(t, filepath.Join(c.corpusDir, "system", "s0"), "You are helpful.\nBe brief.")
	path, _ := generate(t, c)
	batches, _ := readBatches(t, path)
	chats := 0
	eachSpan(batches, func(s ptrace.Span) {
		a := s.Attributes()
		if op, _ := a.Get("gen_ai.operation.name"); op.Str() != "chat" {
			return
		}
		chats++
		td, _ := a.Get("gen_ai.tool.definitions")
		if !json.Valid([]byte(td.Str())) || !strings.HasPrefix(td.Str(), `[{"name":"`) {
			t.Fatalf("tool definitions are not a corpus entry: %.40s", td.Str())
		}
		si, _ := a.Get("gen_ai.system_instructions")
		if !strings.Contains(si.Str(), "You are helpful.") {
			t.Fatalf("system instructions are not a corpus entry: %.60s", si.Str())
		}
	})
	if chats == 0 {
		t.Fatal("no chat spans")
	}
}

func TestToolDefinitions(t *testing.T) {
	c := testConfig(t, "simple=0.3,task=0.7", 30, 12)
	c.toolDefSizes = []int{340_000}
	path, _ := generate(t, c)
	batches, _ := readBatches(t, path)
	byAgent := map[string]string{}
	chats := 0
	eachSpan(batches, func(s ptrace.Span) {
		a := s.Attributes()
		if op, _ := a.Get("gen_ai.operation.name"); op.Str() != "chat" {
			return
		}
		chats++
		td, ok := a.Get("gen_ai.tool.definitions")
		if !ok || !json.Valid([]byte(td.Str())) {
			t.Fatal("chat span without valid tool definitions")
		}
		if len(td.Str()) < 300_000 || len(td.Str()) > c.maxPayload {
			t.Fatalf("tool definitions are %d bytes", len(td.Str()))
		}
		ag, _ := a.Get("gen_ai.agent.name")
		sys, _ := a.Get("gen_ai.system_instructions")
		if prev, ok := byAgent[ag.Str()]; ok && prev != td.Str()+sys.Str() {
			t.Fatalf("agent %s changed its fixed content", ag.Str())
		}
		byAgent[ag.Str()] = td.Str() + sys.Str()
	})
	if chats == 0 {
		t.Fatal("no chat spans")
	}
}

func TestRepeatedFraction(t *testing.T) {
	c := testConfig(t, "task=0.5,coding=0.5", 24, 13)
	_, sum := generate(t, c)
	for _, name := range []string{"task", "coding"} {
		for i := range profiles {
			if profiles[i].name != name {
				continue
			}
			p := &sum.per[i]
			if f := float64(p.rep) / float64(p.den); p.traces == 0 || f <= 0.85 {
				t.Errorf("%s: repeated fraction %.3f over %d traces", name, f, p.traces)
			}
		}
	}
}

func TestNoContent(t *testing.T) {
	type rec struct {
		trace, span       string
		in, out           int64
		hasContent, extra bool
	}
	collect := func(c *config) ([]rec, *summary) {
		path, sum := generate(t, c)
		batches, _ := readBatches(t, path)
		var rs []rec
		eachSpan(batches, func(s ptrace.Span) {
			r := rec{trace: s.TraceID().String(), span: s.SpanID().String()}
			a := s.Attributes()
			if v, ok := a.Get("gen_ai.usage.input_tokens"); ok {
				r.in = v.Int()
			}
			if v, ok := a.Get("gen_ai.usage.output_tokens"); ok {
				r.out = v.Int()
			}
			for _, k := range []string{"gen_ai.input.messages", "gen_ai.output.messages", "gen_ai.system_instructions",
				"gen_ai.tool.definitions", "gen_ai.tool.call.arguments", "gen_ai.tool.call.result"} {
				if _, ok := a.Get(k); ok {
					r.hasContent = true
				}
			}
			rs = append(rs, r)
		})
		return rs, sum
	}
	full, fs := collect(testConfig(t, "simple=0.3,chat=0.2,task=0.4,coding=0.1", 30, 21))
	c := testConfig(t, "simple=0.3,chat=0.2,task=0.4,coding=0.1", 30, 21)
	c.noContent = true
	lean, ls := collect(c)
	if fs.traces != ls.traces || fs.spans != ls.spans || len(full) != len(lean) {
		t.Fatalf("traces %d/%d spans %d/%d", fs.traces, ls.traces, fs.spans, ls.spans)
	}
	content := false
	for i := range full {
		content = content || full[i].hasContent
		if lean[i].hasContent {
			t.Fatal("content attribute present with no-content")
		}
		f, l := full[i], lean[i]
		if f.trace != l.trace || f.span != l.span || f.in != l.in || f.out != l.out {
			t.Fatalf("span %d differs: %+v vs %+v", i, f, l)
		}
	}
	if !content {
		t.Fatal("normal run had no content attributes")
	}
}

func TestSpansStop(t *testing.T) {
	c := testConfig(t, "simple=0.5,task=0.5", 0, 22)
	c.spans = 400
	path, sum := generate(t, c)
	batches, _ := readBatches(t, path)
	per := map[pcommon.TraceID]int{}
	var order []pcommon.TraceID
	eachSpan(batches, func(s ptrace.Span) {
		per[s.TraceID()]++
		if s.ParentSpanID().IsEmpty() {
			order = append(order, s.TraceID())
		}
	})
	if uint64(len(order)) != sum.traces || sum.spans < 400 {
		t.Fatalf("traces %d (summary %d), spans %d", len(order), sum.traces, sum.spans)
	}
	if before := int(sum.spans) - per[order[len(order)-1]]; before >= 400 {
		t.Fatalf("stopped late: %d spans before the last trace", before)
	}
}
