// querybench measures the HTTP API of a running NightsWatchDB server and
// prints one JSON object. It also has two small helpers the benchmark script
// needs because the host has no sub-second clock: -wait and -elapsed.
package main

import (
	"bytes"
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
	EndNs       int64    `json:"end_ns"`
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

// parseTokenCounts reads the body of the token endpoint as span counts per
// request model. Spans without a model have no name and are left out.
func parseTokenCounts(b []byte) (map[string]int, error) {
	var r struct {
		Models []struct {
			Model string `json:"model"`
			Count int    `json:"count"`
		} `json:"models"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, m := range r.Models {
		if m.Model != "" {
			counts[m.Model] += m.Count
		}
	}
	return counts, nil
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

// post sends a JSON body and returns the answer; a status other than 200 is
// an error, as for get.
func (c *client) post(path string, body []byte) ([]byte, error) {
	resp, err := c.http.Post(c.base+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return b, err
	}
	if resp.StatusCode != 200 {
		return b, fmt.Errorf("%s: status %d: %s", path, resp.StatusCode, b)
	}
	return b, nil
}

// found is what paging through the traces learns: the ids, how many traces
// each root service has, how many traces list each model, the earliest and
// latest trace start, and the latest span end.
type found struct {
	ids                        []string
	services, models           map[string]int
	minStart, maxStart, maxEnd int64
}

// discover pages the list endpoint from the newest trace back to `from`. The
// endpoint has no cursor, so each page asks for traces that start before the
// oldest one seen; a trace that shares that start time is seen twice, which
// the set absorbs.
func (c *client) discover(from, to int64) (found, error) {
	f := found{services: map[string]int{}, models: map[string]int{}}
	seen := map[string]bool{}
	for {
		q := url.Values{"from": {strconv.FormatInt(from, 10)}, "to": {strconv.FormatInt(to, 10)}, "limit": {"1000"}}
		b, err := c.get("/api/v1/traces", q)
		if err != nil {
			return found{}, err
		}
		page, err := parseList(b)
		if err != nil {
			return found{}, err
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
			if len(f.ids) == 0 || s.StartNs < f.minStart {
				f.minStart = s.StartNs
			}
			if len(f.ids) == 0 || s.StartNs > f.maxStart {
				f.maxStart = s.StartNs
			}
			if len(f.ids) == 0 || s.EndNs > f.maxEnd {
				f.maxEnd = s.EndNs
			}
			f.ids = append(f.ids, s.TraceID)
			f.services[s.RootService]++
			for _, m := range s.Models {
				f.models[m]++
			}
		}
		if len(page) < 1000 {
			return f, nil
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
			return f, nil
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

// expParams are the filter values of the exp_* kinds, derived by the rule the
// experiment used: S is the most frequent root service, M the most frequent
// request model, and the window is the middle half of the span start-time
// range.
type expParams struct {
	Service string `json:"service"`
	Model   string `json:"model"`
	FromNs  int64  `json:"from_ns"`
	ToNs    int64  `json:"to_ns"`
	// The range the window comes from, printed so a reader can check it.
	MinSpanStartNs int64 `json:"min_span_start_ns"`
	MaxSpanStartNs int64 `json:"max_span_start_ns"`
}

// middleHalf is [min+(max-min)/4, min+3*(max-min)/4) in integer nanoseconds,
// the experiment's window; the end is exclusive.
func middleHalf(min, max int64) (from, to int64) {
	d := max - min
	return min + d/4, min + 3*d/4
}

// firstTrue returns the smallest t in [lo, hi] for which ok(t) holds, given
// that ok is false below some point and true from it on. It fails if ok(hi)
// is false.
func firstTrue(lo, hi int64, ok func(int64) (bool, error)) (int64, error) {
	good, err := ok(hi)
	if err != nil {
		return 0, err
	}
	if !good {
		return 0, fmt.Errorf("the condition does not hold at the upper bound %d", hi)
	}
	for lo < hi {
		mid := lo + (hi-lo)/2
		good, err := ok(mid)
		if err != nil {
			return 0, err
		}
		if good {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo, nil
}

// spanCount returns how many spans start in [from, to) and how many of them
// each request model has, from the token endpoint.
func (c *client) spanCount(from, to int64) (uint64, map[string]int, error) {
	q := url.Values{"from": {strconv.FormatInt(from, 10)}, "to": {strconv.FormatInt(to, 10)}, "limit": {"10000"}}
	b, err := c.get("/api/v1/tokens", q)
	if err != nil {
		return 0, nil, err
	}
	n, truncated, err := parseTokens(b)
	if err != nil {
		return 0, nil, err
	}
	if truncated {
		return 0, nil, fmt.Errorf("more than 10000 models, so the counts are cut")
	}
	counts, err := parseTokenCounts(b)
	return n, counts, err
}

// deriveExp finds the experiment's parameters from what the server returns.
// The list has trace starts only. The earliest span start is the earliest
// trace start. The latest span start is found by asking the token endpoint
// how many spans start before a time, and searching for the first time by
// which all of them have. Model counts are span counts, as the experiment
// counted them, so they come from the token endpoint as well.
func (c *client) deriveExp(f found) (expParams, error) {
	var p expParams
	total, models, err := c.spanCount(f.minStart, math.MaxInt64)
	if err != nil {
		return p, err
	}
	t, err := firstTrue(f.maxStart+1, f.maxEnd+1, func(t int64) (bool, error) {
		n, _, err := c.spanCount(f.minStart, t)
		return n == total, err
	})
	if err != nil {
		return p, fmt.Errorf("finding the latest span start: %w", err)
	}
	p.MinSpanStartNs, p.MaxSpanStartNs = f.minStart, t-1
	p.FromNs, p.ToNs = middleHalf(p.MinSpanStartNs, p.MaxSpanStartNs)
	p.Service, p.Model = busiest(f.services), busiest(models)
	if p.Service == "" || p.Model == "" {
		return p, fmt.Errorf("the data has no root service or no request model to filter by")
	}
	return p, nil
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

// annotation is one item of the annotations request.
type annotation struct {
	TraceID string   `json:"trace_id"`
	Name    string   `json:"name"`
	Source  string   `json:"source,omitempty"`
	Value   *float64 `json:"value,omitempty"`
	Label   string   `json:"label,omitempty"`
}

// annotationBody is the request body for a batch of items.
func annotationBody(items []annotation) ([]byte, error) {
	return json.Marshal(struct {
		Annotations []annotation `json:"annotations"`
	}{items})
}

// parseAnnotateReply reads the answer to an annotations request.
func parseAnnotateReply(b []byte) (accepted, rejected int, err error) {
	var r struct {
		Accepted *int              `json:"accepted"`
		Rejected []json.RawMessage `json:"rejected"`
	}
	if err = json.Unmarshal(b, &r); err != nil {
		return
	}
	if r.Accepted == nil {
		return 0, 0, fmt.Errorf("the answer has no accepted count: %s", b)
	}
	return *r.Accepted, len(r.Rejected), nil
}

// frac is the share of n that is selected, at least one item when n > 0.
func frac(n, den int) int {
	if n > 0 && n/den == 0 {
		return 1
	}
	return n / den
}

// buildAnnotations picks, with the seed, 10% of the ids for a quality
// annotation (value uniform in [0,1), label good from 0.5, else bad) and 1%
// for a human annotation (label down). The same ids and seed always give the
// same items. It also returns the ids that got a quality annotation.
func buildAnnotations(ids []string, seed int64) (items []annotation, annotated []string) {
	rng := rand.New(rand.NewSource(seed))
	perm := rng.Perm(len(ids))
	for _, i := range perm[:frac(len(ids), 10)] {
		v := rng.Float64()
		label := "bad"
		if v >= 0.5 {
			label = "good"
		}
		items = append(items, annotation{TraceID: ids[i], Name: "quality", Source: "bench", Value: &v, Label: label})
		annotated = append(annotated, ids[i])
	}
	for _, i := range perm[:frac(len(ids), 100)] {
		items = append(items, annotation{TraceID: ids[i], Name: "human", Source: "bench", Label: "down"})
	}
	return items, annotated
}

// annStats are the annotation fields of /api/v1/stats.
type annStats struct {
	Buffered, Parts, Bytes, Written, Dropped uint64
}

// parseAnnStats reads them and fails by name if the server does not report one.
func parseAnnStats(b []byte) (annStats, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return annStats{}, err
	}
	m := map[string]*uint64{}
	for k, v := range raw { // the other fields are not all numbers
		var x uint64
		if json.Unmarshal(v, &x) == nil {
			m[k] = &x
		}
	}
	var s annStats
	for _, f := range []struct {
		name string
		to   *uint64
	}{{"annotations_buffered", &s.Buffered}, {"annotation_parts", &s.Parts}, {"annotation_bytes", &s.Bytes}, {"annotations_written", &s.Written}, {"annotations_dropped", &s.Dropped}} {
		v, ok := m[f.name]
		if !ok || v == nil {
			return annStats{}, fmt.Errorf("/api/v1/stats has no field %q: the server is too old for the annotation phase (run with -annotate=false)", f.name)
		}
		*f.to = *v
	}
	return s, nil
}

func (c *client) annStats() (annStats, error) {
	b, err := c.get("/api/v1/stats", nil)
	if err != nil {
		return annStats{}, err
	}
	return parseAnnStats(b)
}

// annReport is the annotate step in the output.
type annReport struct {
	Sent          int     `json:"sent"`
	Accepted      int     `json:"accepted"`
	Rejected      int     `json:"rejected"`
	Seconds       float64 `json:"seconds"`
	PerSecond     float64 `json:"per_s"`
	FlushWaitS    float64 `json:"flush_wait_s"`
	QualityTraces int     `json:"quality_traces"`
	Parts         uint64  `json:"annotation_parts"`
	Bytes         uint64  `json:"annotation_bytes"`
	Written       uint64  `json:"annotations_written"`
	Dropped       uint64  `json:"annotations_dropped"`
	kind          kindResult
}

// annotate sends the items in requests of `batch`, one at a time, and times
// each request. Any error or rejected item fails the phase.
func (c *client) annotate(items []annotation, batch int) (annReport, error) {
	r := annReport{Sent: len(items), kind: kindResult{Kind: "annotate_post_" + strconv.Itoa(batch)}}
	var lat []float64
	t0 := time.Now()
	for i := 0; i < len(items); i += batch {
		end := i + batch
		if end > len(items) {
			end = len(items)
		}
		body, err := annotationBody(items[i:end])
		if err != nil {
			return r, err
		}
		t := time.Now()
		b, err := c.post("/api/v1/annotations", body)
		ms := float64(time.Since(t)) / 1e6
		r.kind.Bytes += int64(len(b))
		r.kind.Count++
		if err != nil {
			r.kind.Errors++
			return r, fmt.Errorf("annotate request %d: %w", r.kind.Count, err)
		}
		lat = append(lat, ms)
		acc, rej, err := parseAnnotateReply(b)
		if err != nil {
			return r, err
		}
		r.Accepted += acc
		r.Rejected += rej
		if rej > 0 {
			return r, fmt.Errorf("annotate request %d: %d items rejected: %s", r.kind.Count, rej, b)
		}
	}
	d := time.Since(t0).Seconds()
	r.Seconds = math.Round(d*1000) / 1000
	if d > 0 {
		r.PerSecond = math.Round(float64(r.Accepted)/d*10) / 10
	}
	sort.Float64s(lat)
	r.kind.P50, r.kind.P95, r.kind.P99 = ms3(percentile(lat, 50)), ms3(percentile(lat, 95)), ms3(percentile(lat, 99))
	if len(lat) > 0 {
		r.kind.Max = ms3(lat[len(lat)-1])
	}
	return r, nil
}

// waitFlushed polls the stats until no annotation is left in the memory
// buffers, and returns how long that took and the stats then.
func (c *client) waitFlushed(limit time.Duration) (time.Duration, annStats, error) {
	t0 := time.Now()
	for {
		s, err := c.annStats()
		if err != nil {
			return 0, s, err
		}
		if s.Buffered == 0 {
			return time.Since(t0), s, nil
		}
		if time.Since(t0) > limit {
			return 0, s, fmt.Errorf("%d annotations still buffered after %s", s.Buffered, limit)
		}
		time.Sleep(200 * time.Millisecond)
	}
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
	annotate := flag.Bool("annotate", true, "after the other kinds, write annotations and time the queries that use them (false for a server without annotations)")
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
	disc, err := c.discover(*from, *to)
	if err != nil {
		return fmt.Errorf("discovering trace ids: %w", err)
	}
	ids := disc.ids
	if len(ids) == 0 {
		return fmt.Errorf("no traces in the range")
	}
	discoverMs := float64(time.Since(t0)) / 1e6
	service, model := busiest(disc.services), busiest(disc.models)
	fmt.Fprintf(os.Stderr, "discovered %d traces in %.0f ms; service %q, model %q\n", len(ids), discoverMs, service, model)
	// Kept out of the timings, like the discovery above.
	exp, err := c.deriveExp(disc)
	if err != nil {
		return fmt.Errorf("deriving the experiment parameters: %w", err)
	}
	fmt.Fprintf(os.Stderr, "experiment parameters: service %q, model %q, window [%d, %d)\n", exp.Service, exp.Model, exp.FromNs, exp.ToNs)

	rng := rand.New(rand.NewSource(*seed))
	pick := make([]string, *warm+*n)
	for i := range pick {
		pick[i] = ids[rng.Intn(len(ids))]
	}
	ns := func(v int64) string { return strconv.FormatInt(v, 10) }
	listIn := func(f, t int64, extra url.Values) func(int) (string, url.Values) {
		return func(int) (string, url.Values) {
			q := url.Values{"from": {ns(f)}, "to": {ns(t)}, "limit": {"50"}}
			for k, v := range extra {
				q[k] = v
			}
			return "/api/v1/traces", q
		}
	}
	list := func(extra url.Values) func(int) (string, url.Values) { return listIn(*from, *to, extra) }
	tokensOp := func(f, t int64, op string) func(int) (string, url.Values) {
		return func(int) (string, url.Values) {
			q := url.Values{"from": {ns(f)}, "to": {ns(t)}, "limit": {"10000"}}
			if op != "" {
				q.Set("operation", op)
			}
			return "/api/v1/tokens", q
		}
	}
	tokens := func(f, t int64) func(int) (string, url.Values) { return tokensOp(f, t, "") }
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
	// The queries of the earlier experiment, with its parameters.
	add("exp_list1", listIn(exp.FromNs, exp.ToNs, url.Values{"service": {exp.Service}}))
	add("exp_list2", listIn(exp.FromNs, exp.ToNs, url.Values{"model": {exp.Model}, "error": {"true"}}))
	add("exp_agg", tokensOp(exp.FromNs, exp.ToNs, "chat"))
	// Search of message text, newest 20 spans of the whole range (without
	// from and to the server searches the last hour only). A dataset without
	// text answers with no spans; the bytes column shows which.
	search := func(q string) func(int) (string, url.Values) {
		return func(int) (string, url.Values) {
			return "/api/v1/search", url.Values{"q": {q}, "from": {ns(*from)}, "to": {ns(*to)}, "limit": {"20"}}
		}
	}
	add("search_word", search("traceback"))
	add("search_common_word", search("error"))
	add("search_and", search("error timeout"))
	add("search_or", search("timeout OR retry"))
	add("search_not", search("error -import"))
	add("search_phrase", search(`"not found"`))
	add("search_prefix", search("config*"))
	add("search_no_hit", search("zqxjkvw"))

	var ann *annReport
	if *annotate {
		items, annotated := buildAnnotations(ids, *seed)
		fmt.Fprintf(os.Stderr, "annotating: %d items on %d traces\n", len(items), len(annotated))
		r, err := c.annotate(items, 500)
		if err != nil {
			return err
		}
		r.QualityTraces = len(annotated)
		kinds = append(kinds, r.kind)
		wait, st, err := c.waitFlushed(120 * time.Second)
		if err != nil {
			return err
		}
		r.FlushWaitS = math.Round(wait.Seconds()*10) / 10
		r.Parts, r.Bytes, r.Written, r.Dropped = st.Parts, st.Bytes, st.Written, st.Dropped
		ann = &r
		arng := rand.New(rand.NewSource(*seed + 2))
		apick := make([]string, *warm+*n)
		for i := range apick {
			apick[i] = annotated[arng.Intn(len(annotated))]
		}
		add("open_trace_annotated", func(i int) (string, url.Values) { return "/api/v1/traces/" + apick[i], nil })
		add("list_annotated_50", list(url.Values{"annotation": {"quality"}}))
		add("list_annotation_label_50", list(url.Values{"annotation": {"human"}, "annotation_label": {"down"}}))
		add("list_annotation_value_50", list(url.Values{"annotation": {"quality"}, "annotation_min": {"0.999"}}))
		add("open_trace_after", func(i int) (string, url.Values) { return "/api/v1/traces/" + pick[i], nil })
		add("list_newest_50_after", list(nil))
	}

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
	aj, _ := json.Marshal(ann) // null when skipped
	fmt.Printf("\"annotation\":%s,\n", aj)
	pj, _ := json.Marshal(exp)
	fmt.Printf("\"exp_params\":%s,\n", pj)
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
