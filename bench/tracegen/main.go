// Command tracegen emits deterministic, LLM-agent-shaped OpenTelemetry traces
// as length-prefixed OTLP requests, to a file and/or an OTLP gRPC endpoint.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	seed                   uint64
	traces                 int
	bytes                  int64
	corpusDir              string
	corpusMax              int64
	mix                    []float64 // weights aligned with profiles
	tracesPerSession       float64
	contextCap, maxPayload int
	subAgentProb           float64
	spans                  uint64
	noContent              bool
	toolDefSizes           []int // test hook: tool definition sizes to draw from
	window                 time.Duration
	start                  time.Time
	batchSpans, batchBytes int
	rate                   float64
	sinks                  []sink
}

type sizeFlag int64

func (s *sizeFlag) String() string { return strconv.FormatInt(int64(*s), 10) }
func (s *sizeFlag) Set(v string) error {
	n, err := parseSize(v)
	*s = sizeFlag(n)
	return err
}

func parseSize(v string) (int64, error) {
	mult := int64(1)
	u := strings.ToUpper(strings.TrimSpace(v))
	for _, x := range []struct {
		suf string
		m   int64
	}{{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30}, {"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"B", 1}} {
		if strings.HasSuffix(u, x.suf) {
			mult, u = x.m, strings.TrimSuffix(u, x.suf)
			break
		}
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(u), 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("bad size %q", v)
	}
	return int64(f * float64(mult)), nil
}

func parseMix(s string) ([]float64, error) {
	w := make([]float64, len(profiles))
	sum := 0.0
	for _, kv := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(kv, "=")
		f, err := strconv.ParseFloat(v, 64)
		if !ok || err != nil || f < 0 {
			return nil, fmt.Errorf("bad --mix entry %q", kv)
		}
		found := false
		for i := range profiles {
			if profiles[i].name == k {
				w[i], found = f, true
			}
		}
		if !found {
			return nil, fmt.Errorf("unknown profile %q in --mix", k)
		}
		sum += f
	}
	if sum == 0 {
		return nil, errors.New("--mix weights sum to zero")
	}
	for i := range w {
		w[i] /= sum
	}
	return w, nil
}

type job struct {
	profile int
	seed    uint64
	start   int64
	sess    *session
	out     chan chunk
}

type enc struct {
	b       []byte
	payload int
	chat    bool
}

type chunk struct {
	spans []enc
	sess  *session
	last  bool
	acct  *acct // set on the last chunk of a trace
}

// acct is what one trace reports about itself beyond its spans.
type acct struct {
	cls      [nClasses]uint64  // bytes by attribute class
	rep, den uint64            // repeated and total bytes of input messages, system instructions, tool definitions
	use      map[int][2]uint64 // per agent: system instruction and tool definition bytes emitted
}

// Attribute classes for byte accounting.
const (
	clsInput = iota
	clsOutput
	clsSystem
	clsToolDefs
	clsToolIO
	clsOther
	nClasses
)

var classNames = [nClasses]string{"input_messages", "output_messages", "system_instructions", "tool_definitions", "tool_arguments_results", "other"}

func (a *acct) add(s *span, encoded int) {
	n := 0
	for i := range s.attrs {
		at := &s.attrs[i]
		c := -1
		switch at.key {
		case names.InputMsgs:
			c = clsInput
		case names.OutputMsgs:
			c = clsOutput
		case names.SystemInst:
			c = clsSystem
		case names.ToolDefs:
			c = clsToolDefs
		case names.ToolArgs, names.ToolResult:
			c = clsToolIO
		}
		if c >= 0 {
			a.cls[c] += uint64(at.strLen())
			n += at.strLen()
		}
	}
	a.cls[clsOther] += uint64(encoded - n)
}

var scope = encodeScope("tracegen.genai", "0.1.0")

// plan hands out trace jobs in a fixed order. All randomness that affects
// ordering or identity comes from here or from the per-trace seed, so output
// does not depend on worker scheduling.
func plan(ctx context.Context, c *config, order chan<- *job, work chan<- *job) {
	defer close(order)
	defer close(work)
	r := rand.New(rand.NewPCG(c.seed, 0x9e3779b97f4a7c15))
	n := float64(c.traces)
	if c.traces == 0 {
		// Byte- and span-limited runs do not know the trace count; assume a mean size.
		n = float64(c.bytes) / estTraceBytes
		if c.spans > 0 {
			n = float64(c.spans) / estTraceSpans
		}
	}
	interval := float64(c.window.Nanoseconds()) / math.Max(n, 1)
	k := 0
	for c.traces == 0 || k < c.traces {
		sess := newSession(r)
		nt := 1
		for r.Float64() > 1/c.tracesPerSession {
			nt++
		}
		for i := 0; i < nt && (c.traces == 0 || k < c.traces); i++ {
			x := r.Float64() * sumW(c.mix)
			pi := len(c.mix) - 1
			for j, w := range c.mix {
				if x < w {
					pi = j
					break
				}
				x -= w
			}
			j := &job{profile: pi, seed: r.Uint64(), sess: sess, out: make(chan chunk, 4),
				start: c.start.UnixNano() + int64((float64(k)+r.Float64())*interval)}
			select {
			case order <- j:
			case <-ctx.Done():
				return
			}
			select {
			case work <- j:
			case <-ctx.Done():
				return
			}
			k++
		}
	}
}

const (
	estTraceBytes = 1_000_000
	estTraceSpans = 18
)

func sumW(w []float64) (s float64) {
	for _, x := range w {
		s += x
	}
	return
}

func worker(ctx context.Context, c *config, cp *corpus, agents []agent, work <-chan *job) {
	for j := range work {
		func() {
			defer close(j.out)
			t := &traceGen{c: c, cp: cp, agents: agents, p: &profiles[j.profile], sess: j.sess,
				acct: &acct{use: map[int][2]uint64{}}, seen: map[int]bool{},
				rng: rand.New(rand.NewPCG(j.seed, 0xda942042e4dd58b5)), cursor: j.start}
			for i := range t.traceID {
				t.traceID[i] = byte(t.rng.Uint32())
			}
			var cur []enc
			size := 0
			send := func(last bool) bool {
				ch := chunk{spans: cur, sess: j.sess, last: last}
				if last {
					ch.acct = t.acct
				}
				select {
				case j.out <- ch:
					cur, size = nil, 0
					return true
				case <-ctx.Done():
					return false
				}
			}
			ok := true
			t.emit = func(s *span) {
				if !ok {
					return
				}
				if c.noContent {
					s.dropContent()
				}
				b, p := s.encode()
				t.acct.add(s, len(b))
				cur = append(cur, enc{b, p, s.chat})
				size += len(b)
				if len(cur) >= 256 || size >= 1<<20 {
					ok = send(false)
				}
			}
			t.run()
			if ok {
				send(true)
			}
		}()
	}
}

type batcher struct {
	c      *config
	segs   []seg
	spans  int
	bytes  int
	buf    []byte
	total  int64 // framed bytes written
	nspans uint64
}

type seg struct {
	sess  *session
	spans [][]byte
}

func (b *batcher) add(sess *session, e []byte) error {
	if b.spans > 0 && (b.spans >= b.c.batchSpans || b.bytes+len(e) > b.c.batchBytes) {
		if err := b.flush(); err != nil {
			return err
		}
	}
	if n := len(b.segs); n == 0 || b.segs[n-1].sess != sess {
		b.segs = append(b.segs, seg{sess: sess})
	}
	s := &b.segs[len(b.segs)-1]
	s.spans = append(s.spans, e)
	b.spans++
	b.bytes += len(e)
	return nil
}

func (b *batcher) flush() error {
	if b.spans == 0 {
		return nil
	}
	buf := append(b.buf[:0], 0, 0, 0, 0)
	for _, s := range b.segs {
		in := len(scope)
		for _, e := range s.spans {
			in += len(e)
		}
		rl := 1 + varintLen(uint64(len(s.sess.resource))) + len(s.sess.resource)
		rs := rl + 1 + varintLen(uint64(in)) + in
		buf = append(buf, 0x0A)
		buf = appendVarint(buf, uint64(rs))
		buf = append(buf, 0x0A)
		buf = appendVarint(buf, uint64(len(s.sess.resource)))
		buf = append(buf, s.sess.resource...)
		buf = append(buf, 0x12)
		buf = appendVarint(buf, uint64(in))
		buf = append(buf, scope...)
		for _, e := range s.spans {
			buf = append(buf, e...)
		}
	}
	binary.BigEndian.PutUint32(buf, uint32(len(buf)-4))
	b.buf = buf
	for _, s := range b.c.sinks {
		if err := s.write(buf, b.spans); err != nil {
			return err
		}
	}
	b.total += int64(len(buf))
	b.nspans += uint64(b.spans)
	b.segs, b.spans, b.bytes = b.segs[:0], 0, 0
	return nil
}

type summary struct {
	seed               uint64
	sessions           int
	traces             uint64
	spans              uint64
	bytes              int64
	begin              time.Time
	elapsed            time.Duration
	crossRep, crossAll [2]uint64 // system instruction and tool definition bytes repeated from earlier traces, and total
	overall            profStats
	per                []profStats
}

func run(c *config) (*summary, error) {
	cp, err := loadCorpus(c.corpusDir, c.corpusMax)
	if err != nil {
		return nil, err
	}
	agents := buildAgents(c, cp)
	begin := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }()
	nw := runtime.NumCPU()
	order := make(chan *job, nw*2)
	work := make(chan *job, nw*2)
	wg.Add(1 + nw)
	go func() { defer wg.Done(); plan(ctx, c, order, work) }()
	for i := 0; i < nw; i++ {
		go func() { defer wg.Done(); worker(ctx, c, cp, agents, work) }()
	}

	sum := &summary{seed: c.seed, per: make([]profStats, len(profiles))}
	b := &batcher{c: c}
	var last *session
	seenAgent := map[int]bool{}
	for j := range order {
		if j.sess != last {
			sum.sessions++
			last = j.sess
		}
		var nspans, tbytes uint64
		var a *acct
		for ch := range j.out {
			for _, e := range ch.spans {
				if err := b.add(ch.sess, e.b); err != nil {
					return nil, err
				}
				nspans++
				tbytes += uint64(len(e.b))
				for _, p := range []*profStats{&sum.overall, &sum.per[j.profile]} {
					p.payloadPerSpan.add(uint64(e.payload))
					if e.chat {
						p.chatPayload.add(uint64(e.payload))
					}
				}
			}
			if ch.acct != nil {
				a = ch.acct
			}
		}
		if ctx.Err() != nil {
			break
		}
		sum.traces++
		sum.spans += nspans
		for _, p := range []*profStats{&sum.overall, &sum.per[j.profile]} {
			p.traces++
			p.spansPerTrace.add(nspans)
			p.bytesPerTrace.add(tbytes)
			p.rep += a.rep
			p.den += a.den
			for i, v := range a.cls {
				p.cls[i] += v
			}
		}
		for ai, u := range a.use {
			if seenAgent[ai] {
				sum.crossRep[0] += u[0]
				sum.crossRep[1] += u[1]
			}
			sum.crossAll[0] += u[0]
			sum.crossAll[1] += u[1]
		}
		for ai := range a.use {
			seenAgent[ai] = true
		}
		if c.spans > 0 && sum.spans >= c.spans {
			cancel()
			break
		}
		if c.bytes > 0 && b.total+int64(b.bytes) >= c.bytes {
			cancel()
			break
		}
	}
	if err := b.flush(); err != nil {
		return nil, err
	}
	sum.bytes = b.total
	sum.begin = begin
	return sum, nil
}

func (s *summary) json() []byte {
	per := map[string]any{}
	for i := range profiles {
		per[profiles[i].name] = s.per[i].summary()
	}
	sec := s.elapsed.Seconds()
	out, _ := json.MarshalIndent(map[string]any{
		"seed": s.seed, "sessions": s.sessions, "traces": s.traces, "spans": s.spans,
		"encoded_bytes": s.bytes, "elapsed_seconds": math.Round(sec*1000) / 1000,
		"mb_per_second": math.Round(float64(s.bytes)/1e6/sec*10) / 10,
		"overall":       s.overall.summary(), "profiles": per,
		"repeated_across_traces": map[string]float64{
			"system_instructions": frac(s.crossRep[0], s.crossAll[0]),
			"tool_definitions":    frac(s.crossRep[1], s.crossAll[1]),
		},
	}, "", "  ")
	return out
}

func main() {
	if err := realMain(); err != nil {
		fmt.Fprintln(os.Stderr, "tracegen:", err)
		os.Exit(1)
	}
}

func realMain() error {
	// The corpus is a large pointer-free heap; collect often so garbage stays small next to it.
	debug.SetGCPercent(25)
	c := &config{}
	var traces int
	var byteLimit, corpusMax, ctxCap, maxPayload, batchBytes sizeFlag
	corpusMax, ctxCap, maxPayload, batchBytes = 1<<30, 500_000, 1<<20, 3<<20
	mix := "simple=0.50,chat=0.25,task=0.17,coding=0.08"
	var startStr, outFile, endpoint string
	flag.Uint64Var(&c.seed, "seed", 1, "seed for all randomness")
	flag.IntVar(&traces, "traces", 0, "stop after N traces")
	flag.Uint64Var(&c.spans, "spans", 0, "stop at the first trace boundary at or after N spans")
	flag.BoolVar(&c.noContent, "no-content", false, "omit message, system instruction, tool definition and tool argument/result attributes")
	flag.Var(&byteLimit, "bytes", "stop after at least SIZE bytes of encoded output (KB, MB, GB, KiB, ...)")
	flag.StringVar(&c.corpusDir, "corpus", "", "directory of text files used as message content (required)")
	flag.Var(&corpusMax, "corpus-max-bytes", "cap on total corpus bytes read")
	flag.StringVar(&mix, "mix", mix, "profile weights")
	flag.Float64Var(&c.tracesPerSession, "traces-per-session", 3, "mean traces per session (geometric)")
	flag.Var(&ctxCap, "context-cap", "accumulated message bytes above which the oldest half is dropped")
	flag.Var(&maxPayload, "max-payload-bytes", "cap on any single attribute value")
	flag.Float64Var(&c.subAgentProb, "subagent-prob", 0, "chance a tool call is a nested agent run")
	flag.DurationVar(&c.window, "window", 24*time.Hour, "time window trace starts are spread over")
	flag.StringVar(&startStr, "start", "2026-01-01T00:00:00Z", "start of the window (RFC 3339)")
	flag.IntVar(&c.batchSpans, "batch-spans", 512, "max spans per batch")
	flag.Var(&batchBytes, "batch-bytes", "max encoded span bytes per batch")
	flag.Float64Var(&c.rate, "rate", 0, "gRPC spans per second (0 = unlimited)")
	flag.StringVar(&outFile, "out-file", "", "write length-prefixed OTLP requests to PATH")
	flag.StringVar(&endpoint, "otlp-endpoint", "", "send batches to HOST:PORT over OTLP gRPC (insecure)")
	flag.Parse()

	if n := b2i(traces > 0) + b2i(byteLimit > 0) + b2i(c.spans > 0); n != 1 {
		return errors.New("exactly one of --traces, --bytes and --spans is required")
	}
	if c.corpusDir == "" {
		return errors.New("--corpus is required")
	}
	if outFile == "" && endpoint == "" {
		return errors.New("at least one of --out-file and --otlp-endpoint is required")
	}
	if maxPayload < 4096 || c.batchSpans < 1 || c.tracesPerSession < 1 || batchBytes < 1<<16 {
		return errors.New("--max-payload-bytes >= 4096, --batch-spans >= 1, --batch-bytes >= 64KiB, --traces-per-session >= 1 required")
	}
	var err error
	if c.start, err = time.Parse(time.RFC3339, startStr); err != nil {
		return fmt.Errorf("--start: %w", err)
	}
	if c.mix, err = parseMix(mix); err != nil {
		return err
	}
	c.traces, c.bytes, c.corpusMax = traces, int64(byteLimit), int64(corpusMax)
	c.contextCap, c.maxPayload, c.batchBytes = int(ctxCap), int(maxPayload), int(batchBytes)

	if outFile != "" {
		f, err := os.Create(outFile)
		if err != nil {
			return err
		}
		c.sinks = append(c.sinks, newBufFileSink(f))
	}
	var gs *grpcSink
	if endpoint != "" {
		if gs, err = newGRPCSink(endpoint, c.rate); err != nil {
			return err
		}
		c.sinks = append(c.sinks, gs)
	}
	sum, err := run(c)
	for _, s := range c.sinks {
		if cerr := s.close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		return err
	}
	sum.elapsed = time.Since(sum.begin)
	fmt.Println(string(sum.json()))
	if gs != nil {
		fmt.Fprintf(os.Stderr, "server accepted %d of %d spans\n", gs.accepted, sum.spans)
		if gs.accepted != sum.spans {
			return errors.New("accepted span count does not match generated span count")
		}
	}
	return nil
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
