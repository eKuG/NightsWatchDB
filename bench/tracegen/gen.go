package main

import (
	"encoding/hex"
	"math"
	"math/rand/v2"
	"sort"
	"unicode/utf8"
)

// Attribute names. Verified against the GenAI semantic conventions
// repository (see README); change them here only.
var names = struct {
	Operation, Provider, ReqModel, RespModel, InTokens, OutTokens          string
	Conversation, AgentName, ToolName, ToolCallID, ToolType                string
	ToolArgs, ToolResult, FinishReasons, InputMsgs, OutputMsgs, SystemInst string
	ToolDefs, ErrorType, UserID, SessionID                                 string
}{
	Operation: "gen_ai.operation.name", Provider: "gen_ai.provider.name",
	ReqModel: "gen_ai.request.model", RespModel: "gen_ai.response.model",
	InTokens: "gen_ai.usage.input_tokens", OutTokens: "gen_ai.usage.output_tokens",
	Conversation: "gen_ai.conversation.id", AgentName: "gen_ai.agent.name",
	ToolName: "gen_ai.tool.name", ToolCallID: "gen_ai.tool.call.id", ToolType: "gen_ai.tool.type",
	ToolArgs: "gen_ai.tool.call.arguments", ToolResult: "gen_ai.tool.call.result",
	FinishReasons: "gen_ai.response.finish_reasons", InputMsgs: "gen_ai.input.messages",
	OutputMsgs: "gen_ai.output.messages", SystemInst: "gen_ai.system_instructions",
	ToolDefs:  "gen_ai.tool.definitions",
	ErrorType: "error.type", UserID: "user.id", SessionID: "session.id",
}

// dist is a lognormal size distribution (median and p99, clamped to max).
type dist struct{ median, p99, max float64 }

// profile holds the per-profile parameters. LLM calls per trace are uniform
// in [callsMin, callsMax] when callsMedian is 0, otherwise lognormal from
// (callsMedian, callsP90) clamped to the same range. Each non-final call makes
// no tool call, one with probability tool1, two with probability tool2, and
// otherwise none; with probability burst it makes 3 to 15 instead.
type profile struct {
	name               string
	callsMin, callsMax int
	callsMedian        float64
	callsP90           float64
	tool1, tool2       float64
	burst              float64
	toolResult         dist
	toolErr            float64
}

var (
	userDist      = dist{3000, 25000, 100_000}
	assistantDist = dist{400, 7000, 18_000}
	toolResult    = dist{500, 20000, 100_000}
	codingResult  = dist{900, 30000, 100_000}
)

var profiles = []profile{
	{name: "simple", callsMin: 1, callsMax: 2, toolResult: toolResult, toolErr: 0.03},
	{name: "chat", callsMin: 3, callsMax: 8, tool1: 0.05, toolResult: toolResult, toolErr: 0.03},
	{name: "task", callsMin: 1, callsMax: 150, callsMedian: 12, callsP90: 40,
		tool1: 0.72, tool2: 0.20, burst: 0.01, toolResult: toolResult, toolErr: 0.03},
	{name: "coding", callsMin: 1, callsMax: 150, callsMedian: 50, callsP90: 90,
		tool1: 1, toolResult: codingResult, toolErr: 0.15},
}

const (
	maxDepth   = 3
	chatErr    = 0.05
	maxToolDef = 340_000
)

type model struct {
	name, provider string
	weight         float64
}

var models = []model{
	{"gpt-4o", "openai", 22}, {"gpt-4o-mini", "openai", 18}, {"claude-sonnet-4-5", "anthropic", 18},
	{"claude-haiku-4-5", "anthropic", 12}, {"gemini-2.5-pro", "gcp.gemini", 10},
	{"gemini-2.5-flash", "gcp.gemini", 8}, {"mistral-large-latest", "mistral", 7}, {"deepseek-chat", "deepseek", 5},
}

var toolNames = []string{
	"web_search", "read_file", "write_file", "list_directory", "run_shell", "run_python", "http_get",
	"sql_query", "vector_search", "send_email", "create_ticket", "get_weather", "calendar_lookup",
	"git_diff", "git_log", "grep_code", "fetch_url", "summarize_document", "translate_text", "calculator",
}

var agentNames = []string{
	"support-agent", "research-agent", "coding-agent", "planner", "triage-agent", "data-analyst",
	"sales-agent", "docs-agent", "ops-agent", "review-agent", "billing-agent", "onboarding-agent",
}

var serviceNames = []string{
	"assistant-api", "support-bot", "code-review-agent", "research-worker", "ticket-triage", "sales-copilot",
	"docs-qa", "data-agent", "ops-agent", "onboarding-bot", "search-gateway", "workflow-runner",
}

var hostNames = func() []string {
	h := make([]string, 24)
	for i := range h {
		h[i] = "ip-10-0-" + string(rune('0'+i/10)) + string(rune('0'+i%10)) + "-12"
	}
	return h
}()

var versions = []string{"1.8.2", "1.9.0", "1.9.1", "2.0.0-rc1"}
var envs = []string{"production", "production", "production", "staging", "development"}

type session struct {
	id, user string
	resource []byte // encoded Resource message body
}

func hexID(r *rand.Rand, nbytes int) string {
	b := make([]byte, nbytes)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return hex.EncodeToString(b)
}

func pick(r *rand.Rand, n int) int { return r.IntN(n) }

func pickModel(r *rand.Rand) *model {
	total := 0.0
	for _, m := range models {
		total += m.weight
	}
	x := r.Float64() * total
	for i := range models {
		if x < models[i].weight {
			return &models[i]
		}
		x -= models[i].weight
	}
	return &models[len(models)-1]
}

func newSession(r *rand.Rand) *session {
	s := &session{id: hexID(r, 16), user: "u_" + hexID(r, 6)}
	s.resource = encodeResource([]attr{
		str("service.name", serviceNames[pick(r, len(serviceNames))]),
		str("service.version", versions[pick(r, len(versions))]),
		str("deployment.environment.name", envs[pick(r, len(envs))]),
		str("host.name", hostNames[pick(r, len(hostNames))]),
	})
	return s
}

// lognP draws a lognormal value from its median and the value at the given
// z-score (2.3263 for p99, 1.2816 for p90), clamped to [lo, hi].
func lognP(r *rand.Rand, median, hiv, z float64, lo, hi int) int {
	sigma := (math.Log(hiv) - math.Log(median)) / z
	v := int(math.Exp(math.Log(median) + sigma*r.NormFloat64()))
	return max(lo, min(hi, v))
}

func (d dist) draw(r *rand.Rand, capBytes int) int {
	return lognP(r, d.median, d.p99, 2.3263, 16, min(int(d.max), capBytes))
}

// slice returns about n bytes of data aligned to UTF-8 boundaries.
func slice(r *rand.Rand, data []byte, n int) []byte {
	n = min(n, len(data))
	off := r.IntN(len(data) - n + 1)
	for off < len(data) && !utf8.RuneStart(data[off]) {
		off++
	}
	end := min(off+n, len(data))
	for end < len(data) && !utf8.RuneStart(data[end]) {
		end++
	}
	return data[off:end]
}

// agent is the fixed per-run identity of an agent name: its system
// instructions and tool definitions repeat on every one of its chat spans.
type agent struct {
	name         string
	system, tool []byte // ready-to-emit attribute values
}

// toolDefSize draws from the tool-definition size mix.
func toolDefSize(r *rand.Rand) int {
	switch x := r.Float64(); {
	case x < 0.20:
		return 700
	case x < 0.70:
		return 20_000
	case x < 0.95:
		return 75_000
	}
	return maxToolDef
}

// buildAgents creates the agent pool from the seed alone.
func buildAgents(c *config, cp *corpus) []agent {
	r := rand.New(rand.NewPCG(c.seed, 0x5851f42d4c957f2d))
	out := make([]agent, len(agentNames))
	for i, name := range agentNames {
		a := &out[i]
		a.name = name
		sizes := c.toolDefSizes
		if len(sizes) == 0 {
			sizes = []int{toolDefSize(r)}
		}
		want := sizes[r.IntN(len(sizes))]
		a.tool = cp.toolDef(r, want, c.maxPayload)

		var body []byte
		if e := cp.fitting(roleSystem, c.maxPayload/2); len(e) > 0 {
			body = e[r.IntN(len(e))]
		} else {
			n := lognP(r, 5000, 12000, 2.3263, 2000, 12000)
			body = slice(r, cp.text[roleSystem], min(n, c.maxPayload/4))
		}
		a.system = append(appendEscaped([]byte(`[{"type":"text","content":"`), body), `"}]`...)
	}
	return out
}

// toolDef returns tool definitions of about the wanted size: a whole real
// entry of the closest size when the corpus has them, else a JSON array built
// from corpus text.
func (cp *corpus) toolDef(r *rand.Rand, want, maxPayload int) []byte {
	want = min(want, maxPayload)
	if e := cp.fitting(roleToolDefs, maxPayload); len(e) > 0 {
		i := sort.Search(len(e), func(i int) bool { return len(e[i]) >= want })
		i = min(i, len(e)-1)
		// Spread over entries of similar size so agents do not all share one.
		j := min(i+r.IntN(3), len(e)-1)
		return e[j]
	}
	b := []byte("[")
	for n := 0; n == 0 || len(b) < want-1800; n++ {
		if n > 0 {
			b = append(b, ',')
		}
		b = append(b, `{"type":"function","name":"`...)
		b = append(b, toolNames[r.IntN(len(toolNames))]...)
		b = append(b, '_')
		b = append(b, hexID(r, 3)...)
		b = append(b, `","description":"`...)
		b = appendEscaped(b, slice(r, cp.text[roleTool], 300+r.IntN(1200)))
		b = append(b, `","parameters":{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}}`...)
	}
	return append(b, ']')
}

// traceGen builds one trace. It is single-goroutine and fully determined by
// its rng.
type traceGen struct {
	c       *config
	cp      *corpus
	agents  []agent
	rng     *rand.Rand
	p       *profile
	sess    *session
	traceID [16]byte
	cursor  int64
	emit    func(*span)
	acct    *acct
	seen    map[int]bool // agents whose fixed content was already emitted in this trace
}

func (t *traceGen) id8() (id [8]byte) {
	v := t.rng.Uint64() | 1
	for i := range id {
		id[i] = byte(v >> (8 * i))
	}
	return
}

func (t *traceGen) body(role, n int) []byte { return slice(t.rng, t.cp.text[role], n) }

func (t *traceGen) draw(role int, d dist) []byte {
	return t.body(role, d.draw(t.rng, t.c.maxPayload/4))
}

func (t *traceGen) advance(ns int64) int64 {
	start := t.cursor
	t.cursor += ns
	return start
}

func (t *traceGen) gap() { t.cursor += int64(1+t.rng.IntN(10)) * 1e6 }

func (t *traceGen) errType(p float64, kinds []string) (bool, string) {
	if t.rng.Float64() < p {
		return true, kinds[t.rng.IntN(len(kinds))]
	}
	return false, ""
}

func (t *traceGen) finish(s *span, isErr bool, et string) {
	s.isErr = isErr
	if isErr {
		s.attrs = append(s.attrs, str(names.ErrorType, et))
	}
	t.emit(s)
}

type toolCall struct {
	id, name string
	args     []byte // JSON object
	task     []byte // raw argument text
	sub      bool
}

type message struct {
	b    []byte
	seen bool
}

func (t *traceGen) calls() int {
	p := t.p
	if p.callsMedian == 0 {
		return p.callsMin + t.rng.IntN(p.callsMax-p.callsMin+1)
	}
	return lognP(t.rng, p.callsMedian, p.callsP90, 1.2816, p.callsMin, p.callsMax)
}

func (t *traceGen) toolCount() int {
	p := t.p
	if p.burst > 0 && t.rng.Float64() < p.burst {
		return 3 + t.rng.IntN(13)
	}
	switch x := t.rng.Float64(); {
	case x < p.tool1:
		return 1
	case x < p.tool1+p.tool2:
		return 2
	}
	return 0
}

// agentRun emits one invoke_agent span with its steps and returns the final
// assistant text.
func (t *traceGen) agentRun(depth int, ai int, parent [8]byte, hasParent bool, steps int, task []byte) []byte {
	ag := &t.agents[ai]
	name := ag.name
	id := t.id8()
	start := t.cursor
	m := pickModel(t.rng)
	user := encodeMsg("user", task)
	msgs := []*message{{b: user}}
	total := len(user)
	var lastText []byte

	for i := 0; i < steps; i++ {
		if total > t.c.contextCap && len(msgs) > 1 {
			half := len(msgs) / 2
			for _, d := range msgs[:half] {
				total -= len(d.b)
			}
			msgs = append([]*message(nil), msgs[half:]...)
		}
		last := i == steps-1
		var calls []toolCall
		if !last {
			if n := t.toolCount(); n > 0 {
				calls = make([]toolCall, n)
			}
		}
		for j := range calls {
			c := &calls[j]
			c.id = "call_" + hexID(t.rng, 6)
			c.sub = depth < maxDepth && t.c.subAgentProb > 0 && t.rng.Float64() < t.c.subAgentProb
			c.name = toolNames[pick(t.rng, len(toolNames))]
			key := `{"input":"`
			if c.sub {
				c.name = agentNames[pick(t.rng, len(agentNames))]
				key = `{"task":"`
			}
			c.task = t.body(roleAssistant, lognP(t.rng, 150, 2000, 2.3263, 16, t.c.maxPayload/4))
			c.args = append(appendEscaped([]byte(key), c.task), `"}`...)
		}
		text := t.draw(roleAssistant, assistantDist)
		finishReason := "stop"
		if len(calls) > 0 {
			finishReason = "tool_calls"
		}
		base := append(appendEscaped([]byte(`{"role":"assistant","parts":[{"type":"text","content":"`), text), `"}`...)
		for _, c := range calls {
			base = append(base, `,{"type":"tool_call","id":"`...)
			base = append(base, c.id...)
			base = append(base, `","name":"`...)
			base = append(base, c.name...)
			base = append(base, `","arguments":`...)
			base = append(base, c.args...)
			base = append(base, '}')
		}
		base = append(base, ']', '}')

		// Input is everything so far, truncated from the front to the payload cap.
		j, inLen := 0, 2+len(msgs)-1
		for _, d := range msgs {
			inLen += len(d.b)
		}
		for inLen > t.c.maxPayload && j < len(msgs)-1 {
			inLen -= len(msgs[j].b) + 1
			j++
		}
		parts := make([][]byte, 0, len(msgs)-j)
		for _, d := range msgs[j:] {
			parts = append(parts, d.b)
			t.acct.den += uint64(len(d.b))
			if d.seen {
				t.acct.rep += uint64(len(d.b))
			}
			d.seen = true
		}
		fixed := uint64(len(ag.system) + len(ag.tool))
		t.acct.den += fixed
		if t.seen[ai] {
			t.acct.rep += fixed
		}
		t.seen[ai] = true
		u := t.acct.use[ai]
		u[0] += uint64(len(ag.system))
		u[1] += uint64(len(ag.tool))
		t.acct.use[ai] = u

		outTok := int64(len(base) / 4)
		dur := int64(1e9) + outTok*int64(30e6)*int64(80+t.rng.IntN(41))/100
		dur = min(dur, 30e9)
		isErr, et := t.errType(chatErr, []string{"timeout", "rate_limit_exceeded", "server_error"})
		s := &span{traceID: t.traceID, id: t.id8(), parent: id, hasParent: true,
			name: "chat " + m.name, kind: kindClient, chat: true, agent: ai}
		s.start = t.advance(dur)
		s.end = t.cursor
		s.attrs = []attr{
			str(names.Operation, "chat"), str(names.Provider, m.provider),
			str(names.ReqModel, m.name), str(names.RespModel, m.name),
			num(names.InTokens, int64((inLen+len(ag.system)+len(ag.tool))/4)), num(names.OutTokens, outTok),
			str(names.Conversation, t.sess.id), str(names.AgentName, name),
			{key: names.FinishReasons, kind: kStrArr, strs: []string{finishReason}},
			{key: names.InputMsgs, parts: parts, arr: true},
			{key: names.OutputMsgs, parts: [][]byte{base}, arr: true},
			raw(names.SystemInst, ag.system),
			raw(names.ToolDefs, ag.tool),
		}
		t.finish(s, isErr, et)
		t.gap()
		msgs = append(msgs, &message{b: base})
		total += len(base)
		lastText = text

		for _, c := range calls {
			var result []byte
			if c.sub {
				result = t.agentRun(depth+1, pick(t.rng, len(t.agents)), id, true, 1+t.rng.IntN(8), c.task)
			} else {
				result = t.draw(roleTool, t.p.toolResult)
				dur := int64(10e6 * math.Exp(math.Log(3000)*math.Pow(t.rng.Float64(), 2)))
				isErr, et := t.errType(t.p.toolErr, []string{"timeout", "tool_error"})
				ts := &span{traceID: t.traceID, id: t.id8(), parent: id, hasParent: true,
					name: "execute_tool " + c.name, kind: kindInternal, agent: -1}
				ts.start = t.advance(dur)
				ts.end = t.cursor
				ts.attrs = []attr{
					str(names.Operation, "execute_tool"), str(names.ToolName, c.name),
					str(names.ToolCallID, c.id), str(names.ToolType, "function"),
					raw(names.ToolArgs, c.args), raw(names.ToolResult, result),
				}
				t.finish(ts, isErr, et)
				t.gap()
			}
			tm := encodeToolMsg(c.id, result)
			msgs = append(msgs, &message{b: tm})
			total += len(tm)
		}
	}

	t.cursor += 2e6
	s := &span{traceID: t.traceID, id: id, parent: parent, hasParent: hasParent, agent: -1,
		name: "invoke_agent " + name, kind: kindInternal, start: start, end: t.cursor}
	s.attrs = []attr{
		str(names.Operation, "invoke_agent"), str(names.AgentName, name),
		str(names.Conversation, t.sess.id), str(names.Provider, m.provider), str(names.ReqModel, m.name),
	}
	if !hasParent {
		s.attrs = append(s.attrs, str(names.SessionID, t.sess.id), str(names.UserID, t.sess.user))
	}
	t.emit(s)
	return lastText
}

func encodeMsg(role string, body []byte) []byte {
	b := appendEscaped([]byte(`{"role":"`+role+`","parts":[{"type":"text","content":"`), body)
	return append(b, `"}]}`...)
}

func encodeToolMsg(id string, body []byte) []byte {
	b := []byte(`{"role":"tool","parts":[{"type":"tool_call_response","id":"` + id + `","response":"`)
	b = appendEscaped(b, body)
	return append(b, `"}]}`...)
}

// run generates the whole trace, emitting spans as they finish (children
// before parents).
func (t *traceGen) run() {
	ai := pick(t.rng, len(t.agents))
	t.agentRun(1, ai, [8]byte{}, false, t.calls(), t.draw(roleUser, userDist))
}
