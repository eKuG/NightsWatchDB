package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"unicode/utf8"
)

// Corpus roles. A corpus directory may hold a subdirectory per role, each
// with shard files of records (4-byte big-endian length, then that many bytes
// of UTF-8 text). Other files under the directory are plain text and serve as
// the fallback for roles without a subdirectory.
const (
	roleSystem = iota
	roleUser
	roleAssistant
	roleTool
	roleToolDefs
	nRoles
)

var roleDirs = [nRoles]string{"system", "user", "assistant", "tool", "tooldefs"}

type corpus struct {
	text    [nRoles][]byte   // bytes sliced at random offsets
	entries [nRoles][][]byte // whole records, sorted by size (system, tooldefs)
}

// fitting returns the role's whole entries no larger than limit, smallest first.
func (cp *corpus) fitting(role, limit int) [][]byte {
	e := cp.entries[role]
	return e[:sort.Search(len(e), func(i int) bool { return len(e[i]) > limit })]
}

func readRecords(path string, f func([]byte)) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for len(b) >= 4 {
		n := int(binary.BigEndian.Uint32(b))
		if 4+n > len(b) {
			return fmt.Errorf("%s: truncated record", path)
		}
		f(b[4 : 4+n])
		b = b[4+n:]
	}
	return nil
}

func validUTF8(b []byte) []byte {
	if utf8.Valid(b) {
		return b
	}
	return bytes.ToValidUTF8(b, nil)
}

// loadCorpus reads at most max bytes in total. Whole-entry roles are loaded
// first; the rest of the budget is split between the other roles.
func loadCorpus(dir string, max int64) (*corpus, error) {
	cp := &corpus{}
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		return nil, fmt.Errorf("corpus %q is missing or not a directory", dir)
	}
	present := map[int]bool{}
	for r, d := range roleDirs {
		if st, err := os.Stat(filepath.Join(dir, d)); err == nil && st.IsDir() {
			present[r] = true
		}
	}
	left := max
	for _, r := range []int{roleToolDefs, roleSystem, roleUser, roleAssistant, roleTool} {
		if !present[r] {
			continue
		}
		budget, n := left, 0
		for _, q := range []int{roleUser, roleAssistant, roleTool} {
			if q >= r && r >= roleUser && present[q] {
				n++
			}
		}
		if n > 1 {
			budget = left / int64(n)
		}
		var used int64
		files, _ := filepath.Glob(filepath.Join(dir, roleDirs[r], "*"))
		sort.Strings(files)
		var onDisk int64
		for _, p := range files {
			if st, err := os.Stat(p); err == nil {
				onDisk += st.Size()
			}
		}
		if r != roleSystem && r != roleToolDefs {
			cp.text[r] = make([]byte, 0, min(budget, onDisk))
		}
		for _, p := range files {
			if used >= budget {
				break
			}
			err := readRecords(p, func(e []byte) {
				if used+int64(len(e))+1 > budget {
					return
				}
				e = validUTF8(e)
				used += int64(len(e)) + 1
				if r == roleSystem || r == roleToolDefs {
					cp.entries[r] = append(cp.entries[r], append([]byte(nil), e...))
				} else {
					cp.text[r] = append(append(cp.text[r], e...), '\n')
				}
			})
			if err != nil {
				return nil, err
			}
		}
		left -= used
	}
	for r := range cp.entries {
		e := cp.entries[r]
		sort.SliceStable(e, func(i, j int) bool { return len(e[i]) < len(e[j]) })
	}

	// Fallback: plain files outside the role directories.
	var fb []byte
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && p != dir && present[roleOf(d.Name())] && filepath.Dir(p) == dir {
			return filepath.SkipDir
		}
		if !d.Type().IsRegular() || int64(len(fb)) >= left {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		b = validUTF8(b)
		if rest := left - int64(len(fb)); int64(len(b)) > rest {
			b = b[:rest]
			for len(b) > 0 && !utf8.Valid(b) {
				b = b[:len(b)-1]
			}
		}
		fb = append(fb, b...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(fb) < minText {
		// No plain files: reuse the largest role text as the fallback.
		for _, r := range []int{roleTool, roleAssistant, roleUser} {
			if len(cp.text[r]) > len(fb) {
				fb = cp.text[r]
			}
		}
	}
	if len(fb) < minText {
		return nil, fmt.Errorf("corpus %q is missing or too small", dir)
	}
	for r := range cp.text {
		if len(cp.text[r]) < minText {
			cp.text[r] = fb
		}
	}
	return cp, nil
}

const minText = 4096

func roleOf(name string) int {
	for r, d := range roleDirs {
		if d == name {
			return r
		}
	}
	return -1
}
