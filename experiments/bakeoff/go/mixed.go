package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

// gcWatch samples the GC pause ring buffer to track count, total and max pause since start.
type gcWatch struct {
	mu         sync.Mutex
	base       runtime.MemStats
	seen       uint32
	total, max uint64
	stop, done chan struct{}
}

func (g *gcWatch) sample() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	g.mu.Lock()
	defer g.mu.Unlock()
	cur := m.NumGC - g.base.NumGC
	if cur-g.seen > 256 { // ring overflowed between samples: max is a floor, total stays exact
		g.seen = cur - 256
	}
	for ; g.seen < cur; g.seen++ {
		g.max = max(g.max, m.PauseNs[(g.base.NumGC+g.seen)%256])
	}
	g.total = m.PauseTotalNs - g.base.PauseTotalNs
}

func startGCWatch() *gcWatch {
	g := &gcWatch{stop: make(chan struct{}), done: make(chan struct{})}
	runtime.ReadMemStats(&g.base)
	go func() {
		defer close(g.done)
		for t := time.NewTicker(100 * time.Millisecond); ; {
			select {
			case <-t.C:
				g.sample()
			case <-g.stop:
				return
			}
		}
	}()
	return g
}

func ms(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }

func runMixed(ctx context.Context, in, dir, idsPath string, secs float64, threads int) (result, error) {
	ids, err := readIDs(idsPath)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 || in == "" {
		return nil, fmt.Errorf("mixed needs --in and a non-empty --ids file")
	}
	s, err := openStore(dir)
	if err != nil {
		return nil, err
	}
	defer s.close()
	fi, err := os.Stat(in)
	if err != nil {
		return nil, err
	}
	scratch, err := os.MkdirTemp("", "bake-mixed-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(scratch)

	ctx, cancel := context.WithTimeout(ctx, time.Duration(secs*float64(time.Second)))
	defer cancel()
	gc := startGCWatch()
	start := time.Now()
	var passes int
	var aggMs, traceMs []float64
	var ingestErr, queryErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // ingest: a pass interrupted by the deadline is not counted
		defer wg.Done()
		for ctx.Err() == nil {
			if _, err := convert(ctx, in, scratch, "all", max(threads-1, 1)); err != nil {
				if ctx.Err() == nil {
					ingestErr = err
				}
				return
			}
			passes++
			os.Remove(filepath.Join(scratch, "spans.parquet"))
			os.Remove(filepath.Join(scratch, "payloads.parquet"))
		}
	}()
	go func() { // queries: agg then one trace lookup; one cut off by the deadline is dropped
		defer wg.Done()
		for i := 0; ctx.Err() == nil; i++ {
			t := time.Now()
			_, _, _, err := s.agg(ctx)
			if err != nil {
				if ctx.Err() == nil {
					queryErr = err
				}
				return
			}
			a := ms(t)
			t = time.Now()
			if _, _, err = s.lookup(ctx, ids[i%len(ids)]); err != nil {
				if ctx.Err() == nil {
					queryErr = err
				}
				return
			}
			aggMs, traceMs = append(aggMs, a), append(traceMs, ms(t))
		}
	}()
	wg.Wait()
	el := time.Since(start).Seconds()
	close(gc.stop)
	<-gc.done
	gc.sample()
	if ingestErr != nil || queryErr != nil {
		return nil, fmt.Errorf("ingest: %v; query: %v", ingestErr, queryErr)
	}
	sort.Float64s(aggMs)
	sort.Float64s(traceMs)
	return result{"ingest_passes": passes, "ingest_mb_per_s": float64(passes) * float64(fi.Size()) / 1e6 / el,
		"agg_p50_ms": pct(aggMs, .5), "agg_p99_ms": pct(aggMs, .99), "agg_max_ms": pct(aggMs, 1),
		"trace_p50_ms": pct(traceMs, .5), "trace_p99_ms": pct(traceMs, .99), "trace_max_ms": pct(traceMs, 1),
		"query_rounds": len(aggMs), "gc_count": gc.seen, "gc_pause_total_ms": float64(gc.total) / 1e6,
		"gc_pause_max_ms": float64(gc.max) / 1e6}, nil
}
