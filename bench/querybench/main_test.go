package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
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
	ids, services, _, err := c.discover(0, 200000)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2500 || services["s"] != 2500 {
		t.Errorf("found %d ids", len(ids))
	}
}

func TestBusiest(t *testing.T) {
	if got := busiest(map[string]int{"b": 2, "a": 2, "c": 1, "": 9}); got != "a" {
		t.Errorf("got %q", got)
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
