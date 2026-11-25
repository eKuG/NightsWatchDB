package main

import (
	"math"
	"math/bits"
)

// hist is a fixed-size log-scaled histogram: exact below 16, then 16 buckets
// per power of two (about 4% resolution). Max is tracked exactly.
type hist struct {
	b   [976]uint64
	n   uint64
	max uint64
}

func bucket(v uint64) int {
	if v < 16 {
		return int(v)
	}
	l := bits.Len64(v) - 1
	return 16 + (l-4)*16 + int((v>>(l-4))&15)
}

func bucketMid(i int) uint64 {
	if i < 16 {
		return uint64(i)
	}
	l, f := (i-16)/16+4, uint64((i-16)%16)
	return (16+f)<<(l-4) + (uint64(1)<<(l-4))/2
}

func (h *hist) add(v uint64) {
	h.b[bucket(v)]++
	h.n++
	h.max = max(h.max, v)
}

func (h *hist) quantile(q float64) uint64 {
	if h.n == 0 {
		return 0
	}
	target := uint64(q*float64(h.n)) + 1
	var seen uint64
	for i, c := range h.b {
		seen += c
		if seen >= target {
			return min(bucketMid(i), h.max)
		}
	}
	return h.max
}

func (h *hist) summary() map[string]uint64 {
	return map[string]uint64{"p50": h.quantile(0.5), "p90": h.quantile(0.9), "p99": h.quantile(0.99), "max": h.max}
}

// profStats are the three distributions reported per profile and overall.
type profStats struct {
	traces                                                    uint64
	spansPerTrace, payloadPerSpan, bytesPerTrace, chatPayload hist
	cls                                                       [nClasses]uint64
	rep, den                                                  uint64
}

func frac(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return math.Round(float64(a)/float64(b)*1e4) / 1e4
}

func (p *profStats) summary() map[string]any {
	return map[string]any{
		"traces":                  p.traces,
		"spans_per_trace":         p.spansPerTrace.summary(),
		"payload_bytes_per_span":  p.payloadPerSpan.summary(),
		"bytes_per_trace":         p.bytesPerTrace.summary(),
		"chat_span_payload_bytes": p.chatPayload.summary(),
		"bytes_by_class":          p.classes(),
		"repeated_fraction":       frac(p.rep, p.den),
	}
}

func (p *profStats) classes() map[string]uint64 {
	m := map[string]uint64{}
	for i, v := range p.cls {
		m[classNames[i]] = v
	}
	return m
}
