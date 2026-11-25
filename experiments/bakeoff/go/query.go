package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

var t0proc = time.Now()

func nowSec() float64 { return time.Since(t0proc).Seconds() }

// pqFile is a Parquet file opened once, with an Arrow reader on top.
type pqFile struct {
	rdr *file.Reader
	fr  *pqarrow.FileReader
	rgs []rgInfo // per row group, loaded once at open
}

// rgInfo is the in-memory metadata the trace lookup needs for one row group.
type rgInfo struct {
	start, n int64
	min, max []byte // trace_id statistics, nil when absent
	bloom    metadata.BloomFilter
}

func openPq(path string, batch int64) (*pqFile, error) {
	r, err := file.OpenParquetFile(path, false)
	if err != nil {
		return nil, err
	}
	fr, err := pqarrow.NewFileReader(r, pqarrow.ArrowReadProperties{BatchSize: batch}, memory.DefaultAllocator)
	if err != nil {
		r.Close()
		return nil, err
	}
	p := &pqFile{rdr: r, fr: fr}
	tc := p.colIdx("trace_id")
	var off int64
	for i := 0; i < r.NumRowGroups(); i++ {
		ri := rgInfo{start: off, n: r.MetaData().RowGroup(i).NumRows()}
		off += ri.n
		if tc >= 0 {
			if st, ok := p.stats(i, tc).(*metadata.FixedLenByteArrayStatistics); ok {
				ri.min, ri.max = st.Min(), st.Max()
			}
			if cc, err := r.MetaData().RowGroup(i).ColumnChunk(tc); err == nil && cc.BloomFilterOffset() > 0 {
				if br, err := r.GetBloomFilterReader().RowGroup(i); err == nil {
					ri.bloom, _ = br.GetColumnBloomFilter(tc)
				}
			}
		}
		p.rgs = append(p.rgs, ri)
	}
	return p, nil
}

func (p *pqFile) colIdx(name string) int { return p.rdr.MetaData().Schema.ColumnIndexByName(name) }

// scan streams the named columns of the given row groups to fn. Columns are looked up by name in each batch.
func (p *pqFile) scan(ctx context.Context, cols []string, rgs []int, fn func(rec arrow.RecordBatch, col func(string) arrow.Array) error) error {
	if len(rgs) == 0 {
		return nil
	}
	idx := make([]int, len(cols))
	for i, c := range cols {
		if idx[i] = p.colIdx(c); idx[i] < 0 {
			return fmt.Errorf("no column %q", c)
		}
	}
	rr, err := p.fr.GetRecordReader(ctx, idx, rgs)
	if err != nil {
		return err
	}
	defer rr.Release()
	for rr.Next() {
		rec := rr.RecordBatch()
		col := func(n string) arrow.Array { return rec.Column(rec.Schema().FieldIndices(n)[0]) }
		if err := fn(rec, col); err != nil {
			return err
		}
	}
	return rr.Err()
}

// scanRows reads rows [first, first+count) of one row group, skipping pages through the offset index
// when the file has one. fn gets each batch and how many of its leading rows are in range.
func (p *pqFile) scanRows(ctx context.Context, cols []string, rg int, first, count int64, fn func(rec arrow.RecordBatch, col func(string) arrow.Array, n int)) error {
	idx := make([]int, len(cols))
	for i, c := range cols {
		idx[i] = p.colIdx(c)
	}
	rr, err := p.fr.GetRecordReader(ctx, idx, []int{rg})
	if err != nil {
		return err
	}
	defer rr.Release()
	if first > 0 {
		if err := rr.SeekToRow(first); err != nil {
			return err
		}
	}
	for count > 0 && rr.Next() {
		rec := rr.RecordBatch()
		n := int(min(count, rec.NumRows()))
		fn(rec, func(c string) arrow.Array { return rec.Column(rec.Schema().FieldIndices(c)[0]) }, n)
		count -= int64(n)
	}
	return rr.Err()
}

func (p *pqFile) hasBloom() bool {
	c := p.colIdx("trace_id")
	for i := 0; i < p.rdr.NumRowGroups(); i++ {
		if cc, err := p.rdr.MetaData().RowGroup(i).ColumnChunk(c); err == nil && cc.BloomFilterOffset() > 0 {
			return true
		}
	}
	return false
}

func (p *pqFile) colNames() []string {
	var n []string
	for i := 0; i < p.rdr.MetaData().Schema.NumColumns(); i++ {
		n = append(n, p.rdr.MetaData().Schema.Column(i).Name())
	}
	return n
}

func (p *pqFile) allRowGroups() []int {
	rg := make([]int, p.rdr.NumRowGroups())
	for i := range rg {
		rg[i] = i
	}
	return rg
}

// stats returns the typed column statistics of one row group, or nil when absent.
func (p *pqFile) stats(rg, col int) metadata.TypedStatistics {
	cc, err := p.rdr.MetaData().RowGroup(rg).ColumnChunk(col)
	if err != nil {
		return nil
	}
	st, err := cc.Statistics()
	if err != nil || st == nil || !st.HasMinMax() {
		return nil
	}
	return st
}

// store is the pair of files the query commands work on.
type store struct{ spans, pay *pqFile }

func openStore(dir string) (*store, error) {
	s, err := openPq(filepath.Join(dir, "spans.parquet"), batchRows)
	if err != nil {
		return nil, err
	}
	p, err := openPq(filepath.Join(dir, "payloads.parquet"), 256)
	if err != nil {
		s.rdr.Close()
		return nil, err
	}
	return &store{s, p}, nil
}

func (s *store) close() { s.spans.rdr.Close(); s.pay.rdr.Close() }

type aggRow struct {
	Model    *string `json:"model"`
	Count    int64   `json:"count"`
	InTokens int64   `json:"input_tokens"`
	OutToks  int64   `json:"output_tokens"`
}

// timeRange returns min and max start_ns from row-group statistics, scanning when statistics are missing.
func (s *store) timeRange(ctx context.Context) (lo, hi int64, err error) {
	c := s.spans.colIdx("start_ns")
	lo, hi = math.MaxInt64, math.MinInt64
	for i := 0; i < s.spans.rdr.NumRowGroups(); i++ {
		if st, ok := s.spans.stats(i, c).(*metadata.Int64Statistics); ok {
			lo, hi = min(lo, st.Min()), max(hi, st.Max())
			continue
		}
		err = s.spans.scan(ctx, []string{"start_ns"}, []int{i}, func(_ arrow.RecordBatch, col func(string) arrow.Array) error {
			for _, v := range col("start_ns").(*array.Int64).Int64Values() {
				lo, hi = min(lo, v), max(hi, v)
			}
			return nil
		})
		if err != nil {
			return
		}
	}
	return
}

// agg runs the windowed aggregation and returns the window, rows sorted by model and row groups read.
func (s *store) agg(ctx context.Context) (win [2]int64, rows []aggRow, read int, err error) {
	lo, hi, err := s.timeRange(ctx)
	if err != nil {
		return
	}
	win = [2]int64{lo + (hi-lo)/4, lo + 3*(hi-lo)/4}
	tc, oc := s.spans.colIdx("start_ns"), s.spans.colIdx("operation")
	var rgs []int
	for i := 0; i < s.spans.rdr.NumRowGroups(); i++ {
		if st, ok := s.spans.stats(i, tc).(*metadata.Int64Statistics); ok && (st.Max() < win[0] || st.Min() >= win[1]) {
			continue
		}
		if st, ok := s.spans.stats(i, oc).(*metadata.ByteArrayStatistics); ok && (string(st.Min()) > "chat" || string(st.Max()) < "chat") {
			continue
		}
		rgs = append(rgs, i)
	}
	groups := map[string]*aggRow{}
	var null aggRow
	err = s.spans.scan(ctx, []string{"operation", "request_model", "start_ns", "input_tokens", "output_tokens"}, rgs,
		func(rec arrow.RecordBatch, col func(string) arrow.Array) error {
			op, mdl := col("operation").(*array.String), col("request_model").(*array.String)
			st, in, out := col("start_ns").(*array.Int64), col("input_tokens").(*array.Int64), col("output_tokens").(*array.Int64)
			for i := 0; i < int(rec.NumRows()); i++ {
				if op.IsNull(i) || op.Value(i) != "chat" || st.IsNull(i) || st.Value(i) < win[0] || st.Value(i) >= win[1] {
					continue
				}
				g := &null
				if !mdl.IsNull(i) {
					if g = groups[mdl.Value(i)]; g == nil {
						m := mdl.Value(i)
						g = &aggRow{Model: &m}
						groups[m] = g
					}
				}
				g.Count++
				if !in.IsNull(i) {
					g.InTokens += in.Value(i)
				}
				if !out.IsNull(i) {
					g.OutToks += out.Value(i)
				}
			}
			return nil
		})
	if null.Count > 0 {
		rows = append(rows, null)
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		rows = append(rows, *groups[k])
	}
	return win, rows, len(rgs), err
}

func runAgg(ctx context.Context, dir string, runs int) (result, error) {
	s, err := openStore(dir)
	if err != nil {
		return nil, err
	}
	defer s.close()
	var ms []float64
	var win [2]int64
	var rows []aggRow
	var read int
	for i := 0; i < runs; i++ {
		t := time.Now()
		if win, rows, read, err = s.agg(ctx); err != nil {
			return nil, err
		}
		ms = append(ms, float64(time.Since(t).Microseconds())/1000)
	}
	if rows == nil {
		rows = []aggRow{}
	}
	return result{"window": win, "result": rows, "runs_ms": ms, "row_groups_read": read, "row_groups_total": s.spans.rdr.NumRowGroups()}, nil
}

// sampleIDs picks ids at positions 0, k, 2k, ... with k = max(1, len/n), at most n of them.
func sampleIDs(ids [][16]byte, n int) [][16]byte {
	k := max(1, len(ids)/max(n, 1))
	var out [][16]byte
	for i := 0; i < len(ids) && len(out) < n; i += k {
		out = append(out, ids[i])
	}
	return out
}

func runSample(ctx context.Context, dir string, n int, out string) (result, error) {
	if out == "" {
		return nil, fmt.Errorf("sample-ids needs --out")
	}
	p, err := openPq(filepath.Join(dir, "spans.parquet"), batchRows)
	if err != nil {
		return nil, err
	}
	defer p.rdr.Close()
	seen := map[[16]byte]struct{}{}
	var ids [][16]byte
	err = p.scan(ctx, []string{"trace_id"}, p.allRowGroups(), func(_ arrow.RecordBatch, col func(string) arrow.Array) error {
		a := col("trace_id").(*array.FixedSizeBinary)
		for i := 0; i < a.Len(); i++ {
			var k [16]byte
			copy(k[:], a.Value(i))
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				ids = append(ids, k)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	pick := sampleIDs(ids, n)
	var sb strings.Builder
	for _, id := range pick {
		sb.WriteString(hex.EncodeToString(id[:]) + "\n")
	}
	if err := os.WriteFile(out, []byte(sb.String()), 0o644); err != nil {
		return nil, err
	}
	return result{"distinct_traces": len(ids), "written": len(pick)}, nil
}

func readIDs(path string) ([][16]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var ids [][16]byte
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l == "" {
			continue
		}
		var id [16]byte
		if b, err := hex.DecodeString(l); err != nil || len(b) != 16 {
			return nil, fmt.Errorf("bad trace id %q", l)
		} else {
			copy(id[:], b)
		}
		ids = append(ids, id)
	}
	return ids, sc.Err()
}

// lookup returns the number of span rows of a trace and the raw bytes of its payload values.
// Span row groups are pruned by trace_id min/max statistics and then the Parquet bloom filter, both
// held in memory. Only the row range of the trace is then read, from spans and from payloads (the
// tables are row aligned), and the payload rows are verified by trace_id.
func (s *store) lookup(ctx context.Context, id [16]byte) (rows, payBytes int64, err error) {
	sp := s.spans
	lo, hi := int64(math.MaxInt64), int64(-1)
	for rg, ri := range sp.rgs {
		if ri.min != nil && (bytes.Compare(id[:], ri.min) < 0 || bytes.Compare(id[:], ri.max) > 0) {
			continue
		}
		if ri.bloom != nil && !ri.bloom.CheckHash(ri.bloom.Hasher().Sum64(id[:])) {
			continue
		}
		// Locate the matching rows from trace_id alone, so bloom false positives stay cheap.
		var pos, first, last, n int64 = 0, -1, -1, 0
		err = sp.scan(ctx, []string{"trace_id"}, []int{rg}, func(_ arrow.RecordBatch, col func(string) arrow.Array) error {
			a := col("trace_id").(*array.FixedSizeBinary)
			for i := 0; i < a.Len(); i++ {
				if bytes.Equal(a.Value(i), id[:]) {
					if first < 0 {
						first = pos + int64(i)
					}
					last = pos + int64(i)
					n++
				}
			}
			pos += int64(a.Len())
			return nil
		})
		if err != nil {
			return
		}
		if n == 0 {
			continue
		}
		rows += n
		lo, hi = min(lo, ri.start+first), max(hi, ri.start+last)
		err = sp.scanRows(ctx, sp.colNames(), rg, first, last-first+1, func(arrow.RecordBatch, func(string) arrow.Array, int) {})
		if err != nil {
			return
		}
	}
	if rows == 0 {
		return
	}
	var found int64
	for rg, ri := range s.pay.rgs {
		if ri.start > hi || ri.start+ri.n <= lo {
			continue
		}
		first, last := max(lo, ri.start)-ri.start, min(hi, ri.start+ri.n-1)-ri.start
		err = s.pay.scanRows(ctx, append([]string{"trace_id"}, payloadCols[:]...), rg, first, last-first+1, func(rec arrow.RecordBatch, col func(string) arrow.Array, n int) {
			t := col("trace_id").(*array.FixedSizeBinary)
			var cols [6]*array.LargeBinary
			for j, c := range payloadCols {
				cols[j] = col(c).(*array.LargeBinary)
			}
			for i := 0; i < n; i++ {
				if bytes.Equal(t.Value(i), id[:]) {
					found++
					for _, c := range cols {
						if c.IsValid(i) {
							payBytes += int64(len(c.Value(i)))
						}
					}
				}
			}
		})
		if err != nil {
			return
		}
	}
	if found != rows {
		err = fmt.Errorf("trace %x: %d span rows but %d payload rows", id, rows, found)
	}
	return
}

// pct is the nearest-rank percentile of an ascending slice.
func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[max(0, int(math.Ceil(p*float64(len(sorted))))-1)]
}

func runTrace(ctx context.Context, dir, idsPath string) (result, error) {
	ids, err := readIDs(idsPath)
	if err != nil {
		return nil, err
	}
	s, err := openStore(dir)
	if err != nil {
		return nil, err
	}
	defer s.close()
	var ms []float64
	var rows, pb int64
	for _, id := range ids {
		t := time.Now()
		r, b, err := s.lookup(ctx, id)
		if err != nil {
			return nil, err
		}
		ms = append(ms, float64(time.Since(t).Microseconds())/1000)
		if r == 0 {
			return nil, fmt.Errorf("trace %x not found", id)
		}
		rows, pb = rows+r, pb+b
	}
	sort.Float64s(ms)
	return result{"lookups": len(ids), "rows_returned": rows, "payload_bytes_returned": pb,
		"p50_ms": pct(ms, 0.5), "p99_ms": pct(ms, 0.99), "max_ms": pct(ms, 1),
		"pruning": "spans: trace_id min/max statistics then parquet bloom filter; payloads: only row groups overlapping the matched span rows (tables are row aligned), verified by trace_id"}, nil
}
