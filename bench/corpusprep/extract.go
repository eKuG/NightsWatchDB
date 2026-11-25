package main

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
)

const (
	shardMax   = 64 << 20
	entryMax   = 1 << 20
	minFreeGB  = 60
	sweStride  = 3 // use every third trajectory so all repositories are covered
	sweDataset = "nebius__SWE-rebench-openhands-trajectories"
	exgDataset = "Exgentic__agent-llm-traces"
	oasDataset = "OpenAssistant__oasst1"
)

var roleNames = []string{"system", "user", "assistant", "tool", "tooldefs"}

// caps limits bytes written per role and source; zero means unlimited
// (de-duplication keeps system prompts and tool definitions small).
var caps = map[string]int64{
	"tool/swe": 1000e6, "tool/exgentic": 250e6, "assistant/swe": 250e6, "assistant/oasst1": 40e6,
	"user/swe": 80e6, "user/oasst1": 40e6,
}

type roleOut struct {
	dir   string
	seen  map[[16]byte]struct{}
	f     *os.File
	w     *bufio.Writer
	size  int64
	shard int
	sizes []uint32
	stats map[string]*srcStats
}

type srcStats struct{ entries, bytes, dups int64 }

type writer struct{ roles map[string]*roleOut }

func freeGB(path string) (float64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, err
	}
	return float64(st.Bavail) * float64(st.Bsize) / 1e9, nil
}

func (w *writer) full(role, src string) bool {
	c := caps[role+"/"+src]
	return c > 0 && w.roles[role].stats[src] != nil && w.roles[role].stats[src].bytes >= c
}

// add writes one entry unless it is empty, oversized, invalid or a duplicate.
func (w *writer) add(role, src string, b []byte) error {
	r := w.roles[role]
	st := r.stats[src]
	if st == nil {
		st = &srcStats{}
		r.stats[src] = st
	}
	if len(b) == 0 || len(b) > entryMax || !utf8.Valid(b) || w.full(role, src) {
		return nil
	}
	h := sha256.Sum256(b)
	k := [16]byte(h[:16])
	if _, dup := r.seen[k]; dup {
		st.dups++
		return nil
	}
	r.seen[k] = struct{}{}
	if r.f == nil || r.size+4+int64(len(b)) > shardMax {
		if err := r.closeShard(); err != nil {
			return err
		}
		f, err := os.Create(filepath.Join(r.dir, fmt.Sprintf("shard-%04d.bin", r.shard)))
		if err != nil {
			return err
		}
		r.shard++
		r.f, r.w, r.size = f, bufio.NewWriterSize(f, 1<<20), 0
	}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(b)))
	r.w.Write(n[:])
	r.w.Write(b)
	r.size += 4 + int64(len(b))
	st.entries++
	st.bytes += int64(len(b))
	r.sizes = append(r.sizes, uint32(len(b)))
	return nil
}

func (r *roleOut) closeShard() error {
	if r.f == nil {
		return nil
	}
	err := r.w.Flush()
	if cerr := r.f.Close(); err == nil {
		err = cerr
	}
	r.f = nil
	return err
}

// Parquet row shapes: only the columns we read.
type sweRow struct {
	Trajectory []sweMsg `parquet:"trajectory,optional,list"`
}
type sweMsg struct {
	Content   *string   `parquet:"content,optional"`
	Role      *string   `parquet:"role,optional"`
	ToolCalls []sweCall `parquet:"tool_calls,optional,list"`
}
type sweCall struct {
	Function *struct {
		Arguments *string `parquet:"arguments,optional"`
	} `parquet:"function,optional"`
}

type exgRow struct {
	Spans []struct {
		Attributes *struct {
			Input *string `parquet:"gen_ai.input.messages,optional"`
			Tools *string `parquet:"gen_ai.tool.definitions,optional"`
		} `parquet:"attributes,optional"`
	} `parquet:"spans,optional,list"`
}

func readParquet[T any](path string, f func(*T) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	st, _ := file.Stat()
	pf, err := parquet.OpenFile(file, st.Size())
	if err != nil {
		return err
	}
	rd := parquet.NewGenericReader[T](pf)
	defer rd.Close()
	buf := make([]T, 4)
	for {
		n, err := rd.Read(buf)
		for i := 0; i < n; i++ {
			if ferr := f(&buf[i]); ferr != nil {
				return ferr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func s(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func extractSWE(w *writer, path string) error {
	i := 0
	return readParquet(path, func(r *sweRow) error {
		i++
		if i%sweStride != 0 {
			return nil
		}
		firstUser := true
		for _, m := range r.Trajectory {
			c := s(m.Content)
			var err error
			switch s(m.Role) {
			case "system":
				err = w.add("system", "swe", []byte(c))
			case "user":
				if firstUser {
					err = w.add("user", "swe", []byte(c))
					firstUser = false
				}
			case "assistant":
				if err = w.add("assistant", "swe", []byte(c)); err == nil {
					for _, tc := range m.ToolCalls {
						if tc.Function != nil {
							err = w.add("assistant", "swe", []byte(s(tc.Function.Arguments)))
						}
					}
				}
			case "tool":
				err = w.add("tool", "swe", []byte(c))
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

type part map[string]json.RawMessage
type chatMsg struct {
	Role  string `json:"role"`
	Parts []part `json:"parts"`
}

func extractExg(w *writer, path string, seenMsg map[[16]byte]struct{}) error {
	return readParquet(path, func(r *exgRow) error {
		for _, sp := range r.Spans {
			if sp.Attributes == nil {
				continue
			}
			if t := s(sp.Attributes.Tools); t != "" {
				if err := w.add("tooldefs", "exgentic", []byte(t)); err != nil {
					return err
				}
			}
			var msgs []json.RawMessage
			if json.Unmarshal([]byte(s(sp.Attributes.Input)), &msgs) != nil {
				continue
			}
			for _, raw := range msgs {
				h := sha256.Sum256(raw)
				k := [16]byte(h[:16])
				if _, ok := seenMsg[k]; ok {
					continue
				}
				seenMsg[k] = struct{}{}
				var m chatMsg
				if json.Unmarshal(raw, &m) != nil {
					continue
				}
				for _, p := range m.Parts {
					var err error
					switch m.Role {
					case "tool":
						// Tool results are JSON values; keep them as JSON text.
						for _, key := range []string{"result", "response"} {
							if v, ok := p[key]; ok {
								var str string
								if json.Unmarshal(v, &str) == nil {
									err = w.add("tool", "exgentic", []byte(str))
								} else {
									err = w.add("tool", "exgentic", v)
								}
							}
						}
					case "system":
						var c string
						if json.Unmarshal(p["content"], &c) == nil {
							err = w.add("system", "exgentic", []byte(c))
						}
					}
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

func extractOasst(w *writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(gz)
	for {
		var m struct{ Role, Text, Lang string }
		if err := dec.Decode(&m); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		if m.Lang != "en" {
			continue
		}
		switch m.Role {
		case "prompter":
			err = w.add("user", "oasst1", []byte(m.Text))
		case "assistant":
			err = w.add("assistant", "oasst1", []byte(m.Text))
		}
		if err != nil {
			return err
		}
	}
}

func extract(raw, out string) error {
	if free, err := freeGB(filepath.Dir(raw)); err != nil || free < minFreeGB {
		return fmt.Errorf("free disk %.0f GB is below the %d GB floor (err %v)", free, minFreeGB, err)
	}
	if err := os.RemoveAll(out); err != nil {
		return err
	}
	w := &writer{roles: map[string]*roleOut{}}
	for _, r := range roleNames {
		dir := filepath.Join(out, r)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		w.roles[r] = &roleOut{dir: dir, seen: map[[16]byte]struct{}{}, stats: map[string]*srcStats{}}
	}
	if err := extractSWE(w, filepath.Join(raw, sweDataset, "trajectories.parquet")); err != nil {
		return fmt.Errorf("swe: %w", err)
	}
	shards, _ := filepath.Glob(filepath.Join(raw, exgDataset, "data", "*.parquet"))
	sort.Strings(shards)
	seenMsg := map[[16]byte]struct{}{}
	for _, p := range shards {
		if err := extractExg(w, p, seenMsg); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
	}
	if err := extractOasst(w, filepath.Join(raw, oasDataset, "2023-04-12_oasst_ready.messages.jsonl.gz")); err != nil {
		return fmt.Errorf("oasst1: %w", err)
	}
	var total int64
	for _, name := range roleNames {
		r := w.roles[name]
		if err := r.closeShard(); err != nil {
			return err
		}
		sort.Slice(r.sizes, func(i, j int) bool { return r.sizes[i] < r.sizes[j] })
		var bytes int64
		for _, st := range r.stats {
			bytes += st.bytes
		}
		total += bytes
		fmt.Printf("%-9s entries %9d  bytes %12d  p50 %7d  p90 %7d  p99 %8d  max %8d\n", name, len(r.sizes), bytes,
			pct(r.sizes, 0.5), pct(r.sizes, 0.9), pct(r.sizes, 0.99), pct(r.sizes, 1))
		var srcs []string
		for k := range r.stats {
			srcs = append(srcs, k)
		}
		sort.Strings(srcs)
		for _, k := range srcs {
			st := r.stats[k]
			fmt.Printf("    %-9s entries %9d  bytes %12d  duplicates dropped %9d\n", k, st.entries, st.bytes, st.dups)
		}
	}
	fmt.Printf("total bytes %d\n", total)
	return nil
}

func pct(s []uint32, q float64) uint32 {
	if len(s) == 0 {
		return 0
	}
	i := int(q * float64(len(s)))
	if i >= len(s) {
		i = len(s) - 1
	}
	return s[i]
}
