package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestPercentile(t *testing.T) {
	s := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, c := range []struct{ p, want float64 }{{50, 5}, {95, 10}, {99, 10}, {10, 1}, {0, 1}, {100, 10}} {
		if got := percentile(s, c.p); got != c.want {
			t.Errorf("p%v = %v, want %v", c.p, got, c.want)
		}
	}
	if percentile(nil, 50) != 0 {
		t.Error("empty input must give 0")
	}
	if percentile([]float64{7}, 99) != 7 {
		t.Error("one value is every percentile")
	}
}

func TestParseList(t *testing.T) {
	body := `{"traces":[{"trace_id":"aa","start_ns":1767225600123456789,"root_service":"agent","models":["m1","m2"],"span_count":3,"error":false}]}`
	got, err := parseList([]byte(body))
	if err != nil || len(got) != 1 {
		t.Fatal(got, err)
	}
	if got[0].StartNs != 1767225600123456789 || got[0].RootService != "agent" || len(got[0].Models) != 2 {
		t.Errorf("%+v", got[0])
	}
	if _, err := parseList([]byte(`{"error":"x"`)); err == nil {
		t.Error("broken JSON must fail")
	}
}

func TestParseTokens(t *testing.T) {
	n, trunc, err := parseTokens([]byte(`{"models":[{"model":"a","count":5,"input_tokens":1,"output_tokens":2},{"model":"","count":7}],"truncated":true}`))
	if err != nil || n != 12 || !trunc {
		t.Fatal(n, trunc, err)
	}
}

// A fake server with 2500 traces whose start times repeat (three per start
// time, so a page can end in the middle of a group), to check that paging
// finds each id exactly once.
func TestDiscover(t *testing.T) {
	type tr struct {
		id    string
		start int64
	}
	var all []tr // newest first
	for i := 0; i < 2500; i++ {
		all = append(all, tr{"id" + strconv.Itoa(i), int64(100000 - i/3)})
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		to, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		var out []map[string]any
		for _, x := range all {
			if x.start >= from && x.start < to && len(out) < 1000 {
				out = append(out, map[string]any{"trace_id": x.id, "start_ns": x.start, "root_service": "s", "models": []string{"m"}})
			}
		}
		json.NewEncoder(w).Encode(map[string]any{"traces": out})
	}))
	defer srv.Close()
	c := &client{base: srv.URL, http: srv.Client()}
	f, err := c.discover(0, 200000)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.ids) != 2500 || f.services["s"] != 2500 {
		t.Errorf("found %d ids", len(f.ids))
	}
	if f.minStart != 100000-833 || f.maxStart != 100000 {
		t.Errorf("start range %d..%d", f.minStart, f.maxStart)
	}
}

func TestBusiest(t *testing.T) {
	if got := busiest(map[string]int{"b": 2, "a": 2, "c": 1, "": 9}); got != "a" {
		t.Errorf("got %q", got)
	}
}

func TestMostFrequentTieBreak(t *testing.T) {
	// Equal counts go to the smaller name, whatever the map order; spans
	// without a model never win.
	counts, err := parseTokenCounts([]byte(`{"models":[{"model":"b","count":4},{"model":"a","count":4},{"count":9},{"model":"c","count":3}]}`))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		if got := busiest(counts); got != "a" {
			t.Fatalf("got %q", got)
		}
	}
	if busiest(map[string]int{}) != "" {
		t.Error("no data gives no name")
	}
}

func TestMiddleHalf(t *testing.T) {
	for _, c := range []struct{ min, max, from, to int64 }{
		{0, 100, 25, 75},         // divisible by 4
		{0, 101, 25, 75},         // odd: 101/4 = 25, 303/4 = 75
		{0, 102, 25, 76},         // even, not divisible by 4: 3*102/4 = 76
		{0, 103, 25, 77},         // 309/4 = 77
		{1000, 1000, 1000, 1000}, // one instant: empty window
		{1767225600000000007, 1767311999999999999, 1767247200000000005, 1767290400000000001},
	} {
		from, to := middleHalf(c.min, c.max)
		if from != c.from || to != c.to {
			t.Errorf("middleHalf(%d, %d) = %d, %d; want %d, %d", c.min, c.max, from, to, c.from, c.to)
		}
	}
}

func TestFirstTrue(t *testing.T) {
	for _, edge := range []int64{10, 11, 57, 99, 100} {
		got, err := firstTrue(10, 100, func(x int64) (bool, error) { return x >= edge, nil })
		if err != nil || got != edge {
			t.Errorf("edge %d: got %d, %v", edge, got, err)
		}
	}
	if _, err := firstTrue(10, 100, func(int64) (bool, error) { return false, nil }); err == nil {
		t.Error("a condition that never holds must fail")
	}
}

// The whole derivation against a fake server: spans start at 1000, 1010 and
// 1990 (the last one in a trace that starts earlier), so the latest span
// start is 1990 although no trace starts there and the latest trace start is
// 1010.
func TestDeriveExp(t *testing.T) {
	spans := []int64{1000, 1010, 1990}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		n := 0
		for _, s := range spans {
			if s >= from && s < to {
				n++
			}
		}
		// Two spans have model m1, one has m2.
		m1 := n
		if n > 0 {
			m1 = n - 1
		}
		json.NewEncoder(w).Encode(map[string]any{"models": []map[string]any{{"model": "m1", "count": m1}, {"model": "m2", "count": n - m1}}})
	}))
	defer srv.Close()
	c := &client{base: srv.URL, http: srv.Client()}
	f := found{services: map[string]int{"svc": 2, "other": 1}, minStart: 1000, maxStart: 1010, maxEnd: 2000}
	p, err := c.deriveExp(f)
	if err != nil {
		t.Fatal(err)
	}
	// Range 1000..1990, so the window is [1247, 1742).
	want := expParams{Service: "svc", Model: "m1", FromNs: 1247, ToNs: 1742, MinSpanStartNs: 1000, MaxSpanStartNs: 1990}
	if p != want {
		t.Errorf("got %+v, want %+v", p, want)
	}
}

func TestElapsed(t *testing.T) {
	if err := elapsedMs("2026-01-01T00:00:00.5Z", "1767225601500000000"); err != nil {
		t.Error(err)
	}
	if err := elapsedMs("not a time", "1"); err == nil {
		t.Error("bad time must fail")
	}
}

func testIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = "t" + strconv.Itoa(i)
	}
	return ids
}

func TestBuildAnnotations(t *testing.T) {
	ids := testIDs(1000)
	a, annotated := buildAnnotations(ids, 1)
	b, _ := buildAnnotations(ids, 1)
	if len(a) != 100+10 || len(annotated) != 100 {
		t.Fatalf("%d items, %d annotated", len(a), len(annotated))
	}
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	if string(ab) != string(bb) {
		t.Error("the same seed must give the same items")
	}
	c, _ := buildAnnotations(ids, 2)
	cb, _ := json.Marshal(c)
	if string(ab) == string(cb) {
		t.Error("another seed must give other items")
	}
	humans := 0
	for _, x := range a {
		switch x.Name {
		case "quality":
			if x.Value == nil || *x.Value < 0 || *x.Value >= 1 || x.Source != "bench" {
				t.Fatalf("%+v", x)
			}
			if (*x.Value >= 0.5) != (x.Label == "good") || (x.Label != "good" && x.Label != "bad") {
				t.Errorf("label %q for %v", x.Label, *x.Value)
			}
		case "human":
			humans++
			if x.Value != nil || x.Label != "down" {
				t.Errorf("%+v", x)
			}
		default:
			t.Errorf("%+v", x)
		}
	}
	if humans != 10 {
		t.Errorf("%d human items", humans)
	}
	if items, _ := buildAnnotations(testIDs(3), 1); len(items) != 2 {
		t.Errorf("a small set gets one item of each kind, got %d", len(items))
	}
}

func TestAnnotationBody(t *testing.T) {
	v := 0.25
	b, err := annotationBody([]annotation{{TraceID: "aa", Name: "quality", Source: "bench", Value: &v, Label: "bad"}, {TraceID: "bb", Name: "human", Label: "down"}})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"annotations":[{"trace_id":"aa","name":"quality","source":"bench","value":0.25,"label":"bad"},{"trace_id":"bb","name":"human","label":"down"}]}`
	if string(b) != want {
		t.Errorf("%s", b)
	}
}

func TestParseAnnotateReply(t *testing.T) {
	a, r, err := parseAnnotateReply([]byte(`{"accepted":3,"rejected":[{"index":1,"error":"unknown trace"}]}`))
	if err != nil || a != 3 || r != 1 {
		t.Fatal(a, r, err)
	}
	if _, _, err := parseAnnotateReply([]byte(`{"rejected":[]}`)); err == nil {
		t.Error("a missing accepted count must fail")
	}
}

func TestParseAnnStats(t *testing.T) {
	s, err := parseAnnStats([]byte(`{"parts":4,"merging":false,"last_compaction_error":null,"annotations_buffered":0,"annotation_parts":2,"annotation_bytes":900,"annotations_written":110,"annotations_dropped":1}`))
	if err != nil || s != (annStats{0, 2, 900, 110, 1}) {
		t.Fatal(s, err)
	}
	_, err = parseAnnStats([]byte(`{"annotations_buffered":0}`))
	if err == nil || !strings.Contains(err.Error(), "annotation_parts") {
		t.Errorf("a missing field must be named: %v", err)
	}
}

// The annotate step sends requests of 500, one at a time, with the JSON
// content type, and fails on a rejected item.
func TestAnnotate(t *testing.T) {
	var sizes []int
	reject := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
			w.WriteHeader(415)
			return
		}
		var req struct {
			Annotations []annotation `json:"annotations"`
		}
		json.NewDecoder(r.Body).Decode(&req)
		sizes = append(sizes, len(req.Annotations))
		if reject {
			w.Write([]byte(`{"accepted":0,"rejected":[{"index":0,"error":"unknown trace"}]}`))
			return
		}
		w.Write([]byte(`{"accepted":` + strconv.Itoa(len(req.Annotations)) + `,"rejected":[]}`))
	}))
	defer srv.Close()
	c := &client{base: srv.URL, http: srv.Client()}
	items, _ := buildAnnotations(testIDs(6000), 1) // 600 + 60
	r, err := c.annotate(append(items, items...)[:1100], 500)
	if err != nil || r.Accepted != 1100 || r.Rejected != 0 || r.kind.Count != 3 {
		t.Fatal(r, err)
	}
	if len(sizes) != 3 || sizes[0] != 500 || sizes[2] != 100 {
		t.Errorf("%v", sizes)
	}
	reject = true
	if _, err := c.annotate(items, 500); err == nil {
		t.Error("a rejected item must fail the phase")
	}
}
