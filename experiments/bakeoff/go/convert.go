package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"golang.org/x/sync/errgroup"
)

const (
	batchRows     = 8192
	spansRowGroup = 65536
	payRowGroup   = 2048
)

// Attribute slots: 0-10 feed spans columns, 11-16 the payload columns.
const (
	nSlots     = 17
	firstPayld = 11
)

var slotOf = map[string]int{
	"gen_ai.operation.name": 0, "gen_ai.provider.name": 1, "gen_ai.request.model": 2,
	"gen_ai.usage.input_tokens": 3, "gen_ai.usage.output_tokens": 4,
	"gen_ai.conversation.id": 5, "session.id": 6, "user.id": 7,
	"gen_ai.agent.name": 8, "gen_ai.tool.name": 9, "error.type": 10,
	"gen_ai.input.messages": 11, "gen_ai.output.messages": 12, "gen_ai.system_instructions": 13,
	"gen_ai.tool.definitions": 14, "gen_ai.tool.call.arguments": 15, "gen_ai.tool.call.result": 16,
}

// strCols maps attribute slots to spans column positions.
var strCols = [...][2]int{{0, 9}, {1, 10}, {2, 11}, {5, 14}, {6, 15}, {7, 16}, {8, 17}, {9, 18}, {10, 19}}

var payloadCols = [6]string{"input_messages", "output_messages", "system_instructions",
	"tool_definitions", "tool_call_arguments", "tool_call_result"}

func fld(name string, t arrow.DataType, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: t, Nullable: nullable}
}

var (
	fsb16 = &arrow.FixedSizeBinaryType{ByteWidth: 16}
	fsb8  = &arrow.FixedSizeBinaryType{ByteWidth: 8}

	spansSchema = arrow.NewSchema([]arrow.Field{
		fld("trace_id", fsb16, false), fld("span_id", fsb8, false), fld("parent_span_id", fsb8, true),
		fld("name", arrow.BinaryTypes.String, false), fld("kind", arrow.PrimitiveTypes.Int32, false),
		fld("start_ns", arrow.PrimitiveTypes.Int64, false), fld("end_ns", arrow.PrimitiveTypes.Int64, false),
		fld("status_code", arrow.PrimitiveTypes.Int32, false),
		fld("service_name", arrow.BinaryTypes.String, true), fld("operation", arrow.BinaryTypes.String, true),
		fld("provider", arrow.BinaryTypes.String, true), fld("request_model", arrow.BinaryTypes.String, true),
		fld("input_tokens", arrow.PrimitiveTypes.Int64, true), fld("output_tokens", arrow.PrimitiveTypes.Int64, true),
		fld("conversation_id", arrow.BinaryTypes.String, true), fld("session_id", arrow.BinaryTypes.String, true),
		fld("user_id", arrow.BinaryTypes.String, true), fld("agent_name", arrow.BinaryTypes.String, true),
		fld("tool_name", arrow.BinaryTypes.String, true), fld("error_type", arrow.BinaryTypes.String, true),
		fld("other_attr_count", arrow.PrimitiveTypes.Int32, false),
	}, nil)

	payloadsSchema = func() *arrow.Schema {
		f := []arrow.Field{fld("trace_id", fsb16, false), fld("span_id", fsb8, false)}
		for _, c := range payloadCols {
			f = append(f, fld(c, arrow.BinaryTypes.LargeBinary, true))
		}
		return arrow.NewSchema(f, nil)
	}()
)

// convStats are the totals reported by convert.
type convStats struct {
	rows, sumIn, sumOut int64
	nonnull, payBytes   [6]int64
}

// batcher turns decoded spans into 8192-row Arrow batches for both tables.
type batcher struct {
	sb, pb *array.RecordBuilder
	rows   int
	st     convStats
}

func newBatcher(mem memory.Allocator) *batcher {
	b := &batcher{sb: array.NewRecordBuilder(mem, spansSchema), pb: array.NewRecordBuilder(mem, payloadsSchema)}
	return b
}

func (b *batcher) release() { b.sb.Release(); b.pb.Release() }

func strOrNull(b *array.StringBuilder, v pcommon.Value, ok bool) {
	if ok && v.Type() == pcommon.ValueTypeStr {
		b.Append(v.Str())
	} else {
		b.AppendNull()
	}
}

func intOrNull(b *array.Int64Builder, v pcommon.Value, ok bool) (n int64) {
	if ok && v.Type() == pcommon.ValueTypeInt {
		n = v.Int()
		b.Append(n)
	} else {
		b.AppendNull()
	}
	return n
}

func (b *batcher) addSpan(svc string, svcOK bool, sp ptrace.Span) {
	var vals [nSlots]pcommon.Value
	var have [nSlots]bool
	other := 0
	for k, v := range sp.Attributes().All() {
		if i, ok := slotOf[k]; ok {
			vals[i], have[i] = v, true
		} else {
			other++
		}
	}
	f := b.sb.Fields()
	tid, sid, pid := sp.TraceID(), sp.SpanID(), sp.ParentSpanID()
	f[0].(*array.FixedSizeBinaryBuilder).Append(tid[:])
	f[1].(*array.FixedSizeBinaryBuilder).Append(sid[:])
	if pid.IsEmpty() {
		f[2].AppendNull()
	} else {
		f[2].(*array.FixedSizeBinaryBuilder).Append(pid[:])
	}
	f[3].(*array.StringBuilder).Append(sp.Name())
	f[4].(*array.Int32Builder).Append(int32(sp.Kind()))
	f[5].(*array.Int64Builder).Append(int64(sp.StartTimestamp()))
	f[6].(*array.Int64Builder).Append(int64(sp.EndTimestamp()))
	f[7].(*array.Int32Builder).Append(int32(sp.Status().Code()))
	if svcOK {
		f[8].(*array.StringBuilder).Append(svc)
	} else {
		f[8].AppendNull()
	}
	for _, m := range strCols {
		strOrNull(f[m[1]].(*array.StringBuilder), vals[m[0]], have[m[0]])
	}
	b.st.sumIn += intOrNull(f[12].(*array.Int64Builder), vals[3], have[3])
	b.st.sumOut += intOrNull(f[13].(*array.Int64Builder), vals[4], have[4])
	f[20].(*array.Int32Builder).Append(int32(other))

	p := b.pb.Fields()
	p[0].(*array.FixedSizeBinaryBuilder).Append(tid[:])
	p[1].(*array.FixedSizeBinaryBuilder).Append(sid[:])
	for j := 0; j < 6; j++ {
		v := vals[firstPayld+j]
		if have[firstPayld+j] && v.Type() == pcommon.ValueTypeStr {
			s := v.Str()
			p[2+j].(*array.BinaryBuilder).AppendString(s)
			b.st.nonnull[j]++
			b.st.payBytes[j] += int64(len(s))
		} else {
			p[2+j].AppendNull()
		}
	}
	b.rows++
	b.st.rows++
}

func (b *batcher) flush() (s, p arrow.RecordBatch) {
	b.rows = 0
	return b.sb.NewRecordBatch(), b.pb.NewRecordBatch()
}

func writerProps(rowGroup int64, bloom bool) *parquet.WriterProperties {
	opts := []parquet.WriterProperty{
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
		parquet.WithStats(true),
		parquet.WithPageIndexEnabled(true),
		parquet.WithMaxRowGroupLength(rowGroup),
	}
	if bloom {
		// Sized for the largest possible row group instead of the 1 MiB library default.
		opts = append(opts, parquet.WithBloomFilterEnabledFor("trace_id", true), parquet.WithBloomFilterNDVFor("trace_id", spansRowGroup))
	}
	return parquet.NewWriterProperties(opts...)
}

// pqSink is one Parquet output file fed by a goroutine.
type pqSink struct {
	f  *os.File
	bw *bufio.Writer
	w  *pqarrow.FileWriter
}

func newSink(path string, schema *arrow.Schema, props *parquet.WriterProperties) (*pqSink, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	w, err := pqarrow.NewFileWriter(schema, bw, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		f.Close()
		return nil, err
	}
	return &pqSink{f, bw, w}, nil
}

// run writes batches until ch closes. It keeps draining after an error so the producer never blocks.
func (s *pqSink) run(ch <-chan arrow.RecordBatch) (err error) {
	for rec := range ch {
		if err == nil {
			err = s.w.WriteBuffered(rec)
		}
		rec.Release()
	}
	if err == nil {
		err = s.w.Close()
	}
	if err == nil {
		err = s.bw.Flush()
	}
	if cerr := s.f.Close(); err == nil {
		err = cerr
	}
	return err
}

type job struct {
	buf []byte
	res chan jobResult
}
type jobResult struct {
	td  ptrace.Traces
	err error
}

var bufPool = sync.Pool{New: func() any { b := make([]byte, 0, 1<<20); return &b }}

// readRecords reads length-prefixed records and hands them to the decode workers in order.
func readRecords(ctx context.Context, path string, jobs, order chan<- job) error {
	defer close(jobs)
	defer close(order)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	var hdr [4]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("record header: %w", err)
		}
		n := int(binary.BigEndian.Uint32(hdr[:]))
		bp := bufPool.Get().(*[]byte)
		if cap(*bp) < n {
			*bp = make([]byte, n)
		}
		buf := (*bp)[:n]
		if _, err := io.ReadFull(r, buf); err != nil {
			return fmt.Errorf("record body: %w", err)
		}
		j := job{buf, make(chan jobResult, 1)}
		select {
		case jobs <- j:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case order <- j:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// convert decodes in and, unless stage is "decode", writes spans.parquet and payloads.parquet to outDir.
func convert(ctx context.Context, in, outDir, stage string, threads int) (*convStats, error) {
	write := stage != "decode"
	var sCh, pCh chan arrow.RecordBatch
	var sSink, pSink *pqSink
	if write {
		var err error
		if sSink, err = newSink(filepath.Join(outDir, "spans.parquet"), spansSchema, writerProps(spansRowGroup, true)); err != nil {
			return nil, err
		}
		if pSink, err = newSink(filepath.Join(outDir, "payloads.parquet"), payloadsSchema, writerProps(payRowGroup, false)); err != nil {
			sSink.f.Close()
			return nil, err
		}
		sCh, pCh = make(chan arrow.RecordBatch), make(chan arrow.RecordBatch)
	}
	g, ctx := errgroup.WithContext(ctx)
	if write {
		g.Go(func() error { return sSink.run(sCh) })
		g.Go(func() error { return pSink.run(pCh) })
	}
	jobs := make(chan job, threads)
	order := make(chan job, threads*2)
	g.Go(func() error { return readRecords(ctx, in, jobs, order) })
	for i := 0; i < threads; i++ {
		g.Go(func() error {
			var um ptrace.ProtoUnmarshaler
			for j := range jobs {
				td, err := um.UnmarshalTraces(j.buf)
				b := j.buf
				bufPool.Put(&b)
				j.res <- jobResult{td, err}
			}
			return nil
		})
	}
	b := newBatcher(memory.DefaultAllocator)
	defer b.release()
	emit := func() error {
		s, p := b.flush()
		if !write {
			s.Release()
			p.Release()
			return nil
		}
		select {
		case sCh <- s:
		case <-ctx.Done():
			s.Release()
			p.Release()
			return ctx.Err()
		}
		select {
		case pCh <- p:
		case <-ctx.Done():
			p.Release()
			return ctx.Err()
		}
		return nil
	}
	g.Go(func() error {
		if write {
			defer close(sCh)
			defer close(pCh)
		}
		for j := range order {
			r := <-j.res
			if r.err != nil {
				return r.err
			}
			rss := r.td.ResourceSpans()
			for i := 0; i < rss.Len(); i++ {
				rs := rss.At(i)
				sv, ok := rs.Resource().Attributes().Get("service.name")
				svc := ""
				if ok {
					svc = sv.Str()
				}
				for k := 0; k < rs.ScopeSpans().Len(); k++ {
					spans := rs.ScopeSpans().At(k).Spans()
					for m := 0; m < spans.Len(); m++ {
						b.addSpan(svc, ok, spans.At(m))
						if b.rows == batchRows {
							if err := emit(); err != nil {
								return err
							}
						}
					}
				}
			}
		}
		if b.rows > 0 {
			return emit()
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return &b.st, nil
}

func runConvert(ctx context.Context, in, out, stage string, threads int) (result, error) {
	if in == "" || (stage != "decode" && stage != "all") || (stage == "all" && out == "") {
		return nil, fmt.Errorf("convert needs --in, and --out unless --stage decode")
	}
	fi, err := os.Stat(in)
	if err != nil {
		return nil, err
	}
	if stage == "all" {
		if err := os.MkdirAll(out, 0o755); err != nil {
			return nil, err
		}
	}
	t0 := nowSec()
	st, err := convert(ctx, in, out, stage, threads)
	if err != nil {
		return nil, err
	}
	el := nowSec() - t0
	nn, pb := result{}, result{}
	for i, c := range payloadCols {
		nn[c], pb[c] = st.nonnull[i], st.payBytes[i]
	}
	r := result{"input_bytes": fi.Size(), "input_mb_per_s": float64(fi.Size()) / 1e6 / el, "rows": st.rows,
		"sum_input_tokens": st.sumIn, "sum_output_tokens": st.sumOut, "nonnull": nn, "payload_bytes": pb,
		"spans_parquet_bytes": nil, "payloads_parquet_bytes": nil, "bloom_filter": false}
	if stage == "all" {
		for k, f := range map[string]string{"spans_parquet_bytes": "spans.parquet", "payloads_parquet_bytes": "payloads.parquet"} {
			s, err := os.Stat(filepath.Join(out, f))
			if err != nil {
				return nil, err
			}
			r[k] = s.Size()
		}
		p, err := openPq(filepath.Join(out, "spans.parquet"), batchRows)
		if err != nil {
			return nil, err
		}
		defer p.rdr.Close()
		r["bloom_filter"] = p.hasBloom()
	}
	return r, nil
}
