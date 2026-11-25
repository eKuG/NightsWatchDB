// querybench measures the HTTP API of a running NightsWatchDB server and
// prints one JSON object. It also has two small helpers the benchmark script
// needs because the host has no sub-second clock: -wait and -elapsed.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type summary struct {
	TraceID     string   `json:"trace_id"`
	StartNs     int64    `json:"start_ns"`
	RootService string   `json:"root_service"`
	Models      []string `json:"models"`
}

// parseList reads the body of the list endpoint.
func parseList(b []byte) ([]summary, error) {
	var r struct {
		Traces []summary `json:"traces"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return r.Traces, nil
}

// parseTokens reads the body of the token endpoint and returns the number of
// spans it counted, which must equal the number of spans stored.
func parseTokens(b []byte) (spans uint64, truncated bool, err error) {
	var r struct {
		Models []struct {
			Count uint64 `json:"count"`
		} `json:"models"`
		Truncated bool `json:"truncated"`
	}
	if err = json.Unmarshal(b, &r); err != nil {
		return
	}
	for _, m := range r.Models {
		spans += m.Count
	}
	return spans, r.Truncated, nil
}

// percentile is the nearest-rank percentile of a sorted slice.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

type client struct {
	base string
	http *http.Client
}

func (c *client) get(path string, q url.Values) ([]byte, error) {
	u := c.base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	resp, err := c.http.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return b, err
	}
	if resp.StatusCode != 200 {
		return b, fmt.Errorf("%s: status %d", path, resp.StatusCode)
	}
	return b, nil
}

// discover pages the list endpoint from the newest trace back to `from`. The
// endpoint has no cursor, so each page asks for traces that start before the
// oldest one seen; a trace that shares that start time is seen twice, which
// the set absorbs.
func (c *client) discover(from, to int64) (ids []string, services, models map[string]int, err error) {
	seen := map[string]bool{}
	services, models = map[string]int{}, map[string]int{}
	for {
		q := url.Values{"from": {strconv.FormatInt(from, 10)}, "to": {strconv.FormatInt(to, 10)}, "limit": {"1000"}}
		b, err := c.get("/api/v1/traces", q)
		if err != nil {
			return nil, nil, nil, err
		}
		page, err := parseList(b)
		if err != nil {
			return nil, nil, nil, err
		}
		fresh := 0
		oldest := to
		for _, s := range page {
			if s.StartNs < oldest {
				oldest = s.StartNs
			}
			if seen[s.TraceID] {
				continue
			}
			seen[s.TraceID] = true
			fresh++
			ids = append(ids, s.TraceID)
			services[s.RootService]++
			for _, m := range s.Models {
				models[m]++
			}
		}
		if len(page) < 1000 {
			return ids, services, models, nil
		}
		// Ask again from the oldest start time (to is exclusive, so +1 keeps
		// traces that share it) unless that would repeat the same page; then
		// step past it.
		if fresh == 0 || oldest+1 >= to {
			to = oldest
		} else {
			to = oldest + 1
		}
		if to <= from {
			return ids, services, models, nil
		}
	}
}

// busiest returns the most frequent key, ties broken by name, so the filter
// values are the same on every run against the same data.
func busiest(m map[string]int) string {
	best, n := "", 0
	for k, v := range m {
		if k != "" && (v > n || (v == n && k < best)) {
			best, n = k, v
		}
	}
	return best
}

type kindResult struct {
	Kind   string  `json:"kind"`
	Count  int     `json:"count"`
	Errors int     `json:"errors"`
	P50    float64 `json:"p50_ms"`
	P95    float64 `json:"p95_ms"`
	P99    float64 `json:"p99_ms"`
	Max    float64 `json:"max_ms"`
	Bytes  int64   `json:"bytes"`
}

// run issues warm+n requests built by req(i) with the given concurrency and
// measures the last n. Failed requests count as errors and are left out of
// the percentiles.
func run(c *client, kind string, warm, n, conc int, req func(i int) (string, url.Values)) kindResult {
	type res struct {
		ms    float64
		bytes int
		err   bool
	}
	total := warm + n
	out := make([]res, total)
	next := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				path, q := req(i)
				t := time.Now()
				b, err := c.get(path, q)
				out[i] = res{float64(time.Since(t)) / 1e6, len(b), err != nil}
			}
		}()
	}
	for i := 0; i < total; i++ {
		next <- i
	}
	close(next)
	wg.Wait()
	r := kindResult{Kind: kind, Count: n}
	var lat []float64
	for _, x := range out[warm:] {
		r.Bytes += int64(x.bytes)
		if x.err {
			r.Errors++
		} else {
			lat = append(lat, x.ms)
		}
	}
	sort.Float64s(lat)
	r.P50, r.P95, r.P99 = ms3(percentile(lat, 50)), ms3(percentile(lat, 95)), ms3(percentile(lat, 99))
	if len(lat) > 0 {
		r.Max = ms3(lat[len(lat)-1])
	}
	return r
}

// ms3 rounds to microseconds so the JSON has no long fractions.
func ms3(v float64) float64 { return math.Round(v*1000) / 1000 }

// waitHealthy polls the URL every 5 ms and prints the Unix time in
// nanoseconds of the first 200 answer.
func waitHealthy(url string, limit time.Duration) error {
	cl := &http.Client{Timeout: time.Second}
	end := time.Now().Add(limit)
	for time.Now().Before(end) {
		if resp, err := cl.Get(url); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				fmt.Println(time.Now().UnixNano())
				return nil
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("no answer from %s within %s", url, limit)
}

// elapsedMs prints the milliseconds between a container start time as Docker
// writes it and a Unix time in nanoseconds.
func elapsedMs(started, readyNs string) error {
	t, err := time.Parse(time.RFC3339Nano, started)
	if err != nil {
		return err
	}
	ns, err := strconv.ParseInt(readyNs, 10, 64)
	if err != nil {
		return err
	}
	fmt.Printf("%.1f\n", float64(ns-t.UnixNano())/1e6)
	return nil
}

func main() {
	if err := realMain(); err != nil {
		fmt.Fprintln(os.Stderr, "querybench:", err)
		os.Exit(1)
	}
}

func realMain() error {
	const start = 1767225600000000000 // the generator's default start, 2026-01-01
	const hourNs = 3600 * 1000000000
	addr := flag.String("addr", "127.0.0.1:4320", "server HTTP API address")
	from := flag.Int64("from-ns", start, "start of the dataset's time range, Unix ns")
	// The generator spreads 24 hours of traces with jitter, so a little more.
	to := flag.Int64("to-ns", start+25*hourNs, "end of the range (exclusive), Unix ns")
	hourFrom := flag.Int64("hour-start-ns", start+12*hourNs, "start of the one-hour token query, Unix ns")
	n := flag.Int("n", 200, "measured requests per kind")
	warm := flag.Int("warmup", 20, "unmeasured requests per kind")
	seed := flag.Int64("seed", 1, "seed for choosing trace ids")
	conc := flag.Int("concurrency", 1, "requests in flight")
	wait := flag.String("wait", "", "URL: poll until it answers 200, print the Unix ns of the answer, and exit")
	elapsed := flag.Bool("elapsed", false, "with two arguments STARTED READY_NS: print the milliseconds between them")
	flag.Parse()
	if *wait != "" {
		return waitHealthy(*wait, 10*time.Minute)
	}
	if *elapsed {
		if flag.NArg() != 2 {
			return fmt.Errorf("-elapsed needs STARTED and READY_NS")
		}
		return elapsedMs(flag.Arg(0), flag.Arg(1))
	}
	if *n < 1 || *conc < 1 || *warm < 0 {
		return fmt.Errorf("n and concurrency must be at least 1, warmup at least 0")
	}
	c := &client{base: "http://" + *addr, http: &http.Client{
		Timeout:   5 * time.Minute,
		Transport: &http.Transport{MaxIdleConnsPerHost: *conc},
	}}
	t0 := time.Now()
	ids, services, models, err := c.discover(*from, *to)
	if err != nil {
		return fmt.Errorf("discovering trace ids: %w", err)
	}
	if len(ids) == 0 {
		return fmt.Errorf("no traces in the range")
	}
	discoverMs := float64(time.Since(t0)) / 1e6
	service, model := busiest(services), busiest(models)
	fmt.Fprintf(os.Stderr, "discovered %d traces in %.0f ms; service %q, model %q\n", len(ids), discoverMs, service, model)

	rng := rand.New(rand.NewSource(*seed))
	pick := make([]string, *warm+*n)
	for i := range pick {
		pick[i] = ids[rng.Intn(len(ids))]
	}
	ns := func(v int64) string { return strconv.FormatInt(v, 10) }
	list := func(extra url.Values) func(int) (string, url.Values) {
		return func(int) (string, url.Values) {
			q := url.Values{"from": {ns(*from)}, "to": {ns(*to)}, "limit": {"50"}}
			for k, v := range extra {
				q[k] = v
			}
			return "/api/v1/traces", q
		}
	}
	tokens := func(f, t int64) func(int) (string, url.Values) {
		return func(int) (string, url.Values) {
			return "/api/v1/tokens", url.Values{"from": {ns(f)}, "to": {ns(t)}, "limit": {"10000"}}
		}
	}
	var kinds []kindResult
	add := func(k string, req func(int) (string, url.Values)) {
		kinds = append(kinds, run(c, k, *warm, *n, *conc, req))
	}
	add("open_trace", func(i int) (string, url.Values) { return "/api/v1/traces/" + pick[i], nil })
	add("list_newest_50", list(nil))
	add("list_service_50", list(url.Values{"service": {service}}))
	add("list_model_50", list(url.Values{"model": {model}}))
	add("list_error_50", list(url.Values{"error": {"true"}}))
	add("tokens_range", tokens(*from, *to))
	add("tokens_hour", tokens(*hourFrom, *hourFrom+hourNs))

	// One more token query, not timed, gives the number of spans the server
	// holds in the range, for the caller to compare with what it sent.
	b, err := c.get(tokens(*from, *to)(0))
	if err != nil {
		return err
	}
	spans, truncated, err := parseTokens(b)
	if err != nil {
		return err
	}

	// One line per kind, so a shell script can read the numbers with awk.
	fmt.Printf("{\"config\":{\"addr\":%q,\"from_ns\":%d,\"to_ns\":%d,\"hour_start_ns\":%d,\"n\":%d,\"warmup\":%d,\"seed\":%d,\"concurrency\":%d,\"service\":%q,\"model\":%q},\n",
		*addr, *from, *to, *hourFrom, *n, *warm, *seed, *conc, service, model)
	fmt.Printf("\"discovered_traces\":%d,\"discovery_ms\":%.1f,\"server_span_count\":%d,\"token_rows_truncated\":%v,\n\"kinds\":[\n", len(ids), discoverMs, spans, truncated)
	for i, k := range kinds {
		line, _ := json.Marshal(k)
		sep := ","
		if i == len(kinds)-1 {
			sep = ""
		}
		fmt.Printf("%s%s\n", line, sep)
	}
	fmt.Println("]}")
	return nil
}
