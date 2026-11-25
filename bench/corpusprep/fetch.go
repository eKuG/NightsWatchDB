package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const ua = "bench-fetch/1.0"

type source struct {
	repo  string
	files func(name string) bool // which repo files to download
}

var sources = []source{
	{"nebius/SWE-rebench-openhands-trajectories", func(n string) bool { return n == "trajectories.parquet" || n == "LICENSE" || n == "README.md" }},
	{"Exgentic/agent-llm-traces", func(n string) bool { return strings.HasPrefix(n, "data/") || n == "README.md" }},
	{"OpenAssistant/oasst1", func(n string) bool {
		return n == "2023-04-12_oasst_ready.messages.jsonl.gz" || n == "LICENSE" || n == "README.md"
	}},
}

func get(url string) (*http.Response, error) {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", ua)
	resp, err := http.DefaultClient.Do(req)
	if err == nil && resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, fmt.Errorf("%s: %s", url, resp.Status)
	}
	return resp, err
}

// fetch downloads the selected files of each dataset into raw/<owner>__<name>/,
// skipping files whose size already matches the Hub listing.
func fetch(raw string) error {
	for _, s := range sources {
		resp, err := get("https://huggingface.co/api/datasets/" + s.repo + "?blobs=true")
		if err != nil {
			return err
		}
		var meta struct {
			Siblings []struct {
				Name string `json:"rfilename"`
				Size int64  `json:"size"`
			}
		}
		err = json.NewDecoder(resp.Body).Decode(&meta)
		resp.Body.Close()
		if err != nil {
			return err
		}
		dir := filepath.Join(raw, strings.ReplaceAll(s.repo, "/", "__"))
		for _, f := range meta.Siblings {
			if !s.files(f.Name) {
				continue
			}
			dst := filepath.Join(dir, f.Name)
			if st, err := os.Stat(dst); err == nil && st.Size() == f.Size {
				continue
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			if free, err := freeGB(raw); err != nil || free-float64(f.Size)/1e9 < minFreeGB {
				return fmt.Errorf("downloading %s would leave less than %d GB free (%.0f GB free now, err %v)", f.Name, minFreeGB, free, err)
			}
			r, err := get("https://huggingface.co/datasets/" + s.repo + "/resolve/main/" + f.Name)
			if err != nil {
				return err
			}
			out, err := os.Create(dst)
			if err != nil {
				return err
			}
			n, err := io.Copy(out, r.Body)
			r.Body.Close()
			out.Close()
			if err != nil {
				return err
			}
			if n != f.Size {
				return fmt.Errorf("%s: got %d bytes, Hub lists %d", dst, n, f.Size)
			}
			fmt.Printf("fetched %s (%d bytes)\n", dst, n)
		}
	}
	return nil
}
