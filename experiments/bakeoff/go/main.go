// Command bake is one half of a Go/Rust bake-off: a small span pipeline over
// OTLP trace files, Parquet and an inverted index. See ../SPEC.md.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type result map[string]any

func main() {
	// Soft memory limit at 80% of the cgroup limit, when there is one.
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		if n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil && n > 0 {
			debug.SetMemoryLimit(n / 10 * 8)
		}
	}
	if len(os.Args) < 2 {
		fatal(fmt.Errorf("usage: bake convert|agg|sample-ids|trace|index|mixed [flags]"))
	}
	task := os.Args[1]
	fs := flag.NewFlagSet(task, flag.ExitOnError)
	threads := fs.Int("threads", runtime.GOMAXPROCS(0), "worker threads")
	in := fs.String("in", "", "input file")
	out := fs.String("out", "", "output directory or file")
	dir := fs.String("dir", "", "directory holding the parquet files")
	ids := fs.String("ids", "", "file of hex trace ids")
	stage := fs.String("stage", "all", "decode|all")
	runs := fs.Int("runs", 5, "agg repetitions")
	n := fs.Int("n", 200, "ids to sample")
	secs := fs.Float64("seconds", 60, "mixed duration")
	_ = fs.Parse(os.Args[2:])
	if *threads < 1 {
		*threads = 1
	}

	ctx := context.Background()
	start := time.Now()
	var r result
	var err error
	switch task {
	case "convert":
		r, err = runConvert(ctx, *in, *out, *stage, *threads)
	case "agg":
		r, err = runAgg(ctx, *dir, *runs)
	case "sample-ids":
		r, err = runSample(ctx, *dir, *n, *out)
	case "trace":
		r, err = runTrace(ctx, *dir, *ids)
	case "index":
		r, err = runIndex(ctx, *dir, *out)
	case "mixed":
		r, err = runMixed(ctx, *in, *dir, *ids, *secs, *threads)
	default:
		err = fmt.Errorf("unknown subcommand %q", task)
	}
	if err != nil {
		fatal(err)
	}
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	r["lang"], r["task"], r["threads"] = "go", task, *threads
	r["wall_s"] = time.Since(start).Seconds()
	r["user_cpu_s"] = float64(ru.Utime.Sec) + float64(ru.Utime.Usec)/1e6
	r["sys_cpu_s"] = float64(ru.Stime.Sec) + float64(ru.Stime.Usec)/1e6
	r["peak_rss_bytes"] = procKB("/proc/self/status", "VmHWM:")
	r["cgroup_peak_bytes"] = cgroupPeak()
	b, _ := json.Marshal(r)
	fmt.Println(string(b))
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "bake:", err)
	os.Exit(1)
}

// procKB returns the byte value of a "Key: N kB" line, or nil.
func procKB(path, key string) any {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	for _, l := range strings.Split(string(b), "\n") {
		if f := strings.Fields(l); len(f) >= 2 && f[0] == key {
			if v, err := strconv.ParseInt(f[1], 10, 64); err == nil {
				return v * 1024
			}
		}
	}
	return nil
}

func cgroupPeak() any {
	b, err := os.ReadFile("/sys/fs/cgroup/memory.peak")
	if err != nil {
		return nil
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return nil
	}
	return v
}
