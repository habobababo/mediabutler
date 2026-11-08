package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Store liest und schreibt dieselben JSON-Dateien wie die Python-Version (DATA_DIR), Formate bleiben kompatibel.
type Store struct {
	mu      sync.Mutex
	dir     string
	mem     map[string][]byte
	persist bool

	rules    []Rule
	rulesOK  bool
	groups   map[string]Group
	groupsOK bool
	paths    map[string]PathOv
	pathsOK  bool
}

type Rule struct {
	Key      string `json:"key"`
	Detected string `json:"detected"`
	Name     string `json:"name"`
	Year     string `json:"year"`
	TMDB     string `json:"tmdb"`
	Category string `json:"category"`
}

type Group struct {
	Name   string `json:"name"`
	Year   string `json:"year"`
	TMDB   string `json:"tmdb"`
	Cat    string `json:"cat"`
	Season int    `json:"season"`
	Ep     int    `json:"ep"`
}

type PathOv struct {
	Folder string `json:"folder,omitempty"`
	N      int    `json:"n,omitempty"`
	Cat    string `json:"cat"`
	Dst    string `json:"dst,omitempty"`
}

type HistoryEntry struct {
	T      string `json:"t"`
	Cat    string `json:"cat"`
	Name   string `json:"name"`
	Full   string `json:"full"`
	Size   int64  `json:"size"`
	Status string `json:"status"`
}

type Config struct {
	Enabled      bool   `json:"enabled"`
	Cron         string `json:"cron"`
	MoveAfter    bool   `json:"move_after"`
	CleanupAfter bool   `json:"cleanup_after"`
	LastRun      string `json:"last_run"`
	LastResult   string `json:"last_result"`
	TgEnabled    bool   `json:"tg_enabled"`
	TgToken      string `json:"tg_token"`
	TgChat       string `json:"tg_chat"`
	TgStart      bool   `json:"tg_start"`
	TgDone       bool   `json:"tg_done"`
	TgUnclear    bool   `json:"tg_unclear"`
	TgUnclearSig string `json:"tg_unclear_sig"`
	TgNew        bool   `json:"tg_new"`
	TgProgress   bool   `json:"tg_progress"`
	TgErrors     bool   `json:"tg_errors"`
	TgSilent     bool   `json:"tg_silent"`
	Jellyfin     bool   `json:"jellyfin"`
	JfScan       bool   `json:"jf_scan"`
	JfURL        string `json:"jf_url"`
	JfKey        string `json:"jf_key"`
	JfLast       string `json:"jf_last"`
	JfMode       string `json:"jf_mode"`
	ExtractAuto  bool   `json:"extract_auto"`
	ExtractDel   bool   `json:"extract_delete"`
	TgPw         bool   `json:"tg_pw"`
	JfRoot       string `json:"jf_root"`
	PublicURL    string `json:"public_url"`
}

func defaultConfig() Config {
	return Config{Cron: "0 3 * * *", MoveAfter: env("MOVE_AFTER", "0") == "1", TgStart: true, TgDone: true, TgUnclear: true, TgNew: true,
		TgProgress: true, TgErrors: true, TgSilent: true, Jellyfin: true,
		JfScan: env("JELLYFIN_URL", "") != "" && env("JELLYFIN_API_KEY", "") != "",
		JfURL:  strings.TrimRight(env("JELLYFIN_URL", ""), "/"), JfKey: env("JELLYFIN_API_KEY", ""), JfMode: "partial",
		ExtractAuto: true, ExtractDel: true, TgPw: true, JfRoot: strings.TrimRight(env("JELLYFIN_ROOT", ""), "/"),
		PublicURL: strings.TrimRight(env("PUBLIC_URL", ""), "/")}
}

func env(k, def string) string {
	if v, ok := os.LookupEnv(k); ok {
		return v
	}
	return def
}

func NewStore(dir string) *Store {
	s := &Store{dir: dir, mem: map[string][]byte{}}
	if err := os.MkdirAll(dir, 0o755); err == nil {
		t := filepath.Join(dir, ".w")
		if os.WriteFile(t, []byte("x"), 0o644) == nil {
			os.Remove(t)
			s.persist = true
		}
	}
	return s
}

func (s *Store) load(name string, v any) bool {
	b, err := os.ReadFile(filepath.Join(s.dir, name))
	if err != nil {
		s.mu.Lock()
		b = s.mem[name]
		s.mu.Unlock()
		if b == nil {
			return false
		}
	}
	return json.Unmarshal(b, v) == nil
}

func (s *Store) save(name string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mem[name] = b
	os.MkdirAll(s.dir, 0o755)
	tmp := filepath.Join(s.dir, name+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "Speichern fehlgeschlagen:", err)
		return
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, name)); err != nil {
		fmt.Fprintln(os.Stderr, "Speichern fehlgeschlagen:", err)
	}
}

func (s *Store) cfg() Config {
	c := defaultConfig()
	var raw map[string]json.RawMessage
	if s.load("config.json", &raw) { // gespeicherte Werte über die Defaults legen, fehlende Schlüssel behalten ihren Default
		b, _ := json.Marshal(raw)
		json.Unmarshal(b, &c)
	}
	return c
}

func (s *Store) saveCfg(f func(*Config)) {
	c := s.cfg()
	f(&c)
	s.save("config.json", c)
}

// ---------- Verlauf, erledigte Quellen ----------

func (s *Store) pushHistory(cat, name, full string, size int64, status string) {
	var h []HistoryEntry
	s.load("history.json", &h)
	h = append(h, HistoryEntry{time.Now().Format("2006-01-02 15:04:05"), cat, name, full, size, status})
	if len(h) > 2000 {
		h = h[len(h)-2000:]
	}
	s.save("history.json", h)
}

type sig [2]int64

func fileSig(p string) (sig, error) {
	st, err := os.Stat(p)
	if err != nil {
		return sig{}, err
	}
	return sig{st.Size(), st.ModTime().Unix()}, nil
}

func (s *Store) copied() map[string]sig {
	m := map[string]sig{}
	s.load("copied.json", &m)
	return m
}

func (s *Store) markCopied(rel, full string) {
	sg, err := fileSig(full)
	if err != nil {
		return
	}
	s.mu.Lock()
	cm := map[string]sig{}
	if b, e := os.ReadFile(filepath.Join(s.dir, "copied.json")); e == nil {
		json.Unmarshal(b, &cm)
	}
	s.mu.Unlock()
	cm[rel] = sig{sg[0], sg[1]}
	if len(cm) > 20000 { // älteste verwerfen
		keys := make([]string, 0, len(cm))
		for k := range cm {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys[:len(cm)-20000] {
			delete(cm, k)
		}
	}
	s.save("copied.json", cm)
}

// ---------- Regeln, Gruppen, eigene Zielpfade ----------

func (s *Store) getRules() []Rule {
	s.mu.Lock()
	ok := s.rulesOK
	r := s.rules
	s.mu.Unlock()
	if ok {
		return r
	}
	var all []Rule
	s.load("rules.json", &all)
	var out []Rule
	for _, x := range all {
		if x.Key != "" {
			out = append(out, x)
		}
	}
	s.mu.Lock()
	s.rules, s.rulesOK = out, true
	s.mu.Unlock()
	return out
}

func (s *Store) setRules(r []Rule) {
	s.mu.Lock()
	s.rules, s.rulesOK = r, true
	s.mu.Unlock()
	if r == nil {
		r = []Rule{}
	}
	s.save("rules.json", r)
}

func (s *Store) ruleFor(title string) *Rule {
	k := skey(title)
	for _, r := range s.getRules() {
		if r.Key == k {
			x := r
			return &x
		}
	}
	return nil
}

func (s *Store) getGroups() map[string]Group {
	s.mu.Lock()
	ok, g := s.groupsOK, s.groups
	s.mu.Unlock()
	if ok {
		return g
	}
	m := map[string]Group{}
	s.load("groups.json", &m)
	s.mu.Lock()
	s.groups, s.groupsOK = m, true
	s.mu.Unlock()
	return m
}

func (s *Store) setGroups(m map[string]Group) {
	s.mu.Lock()
	s.groups, s.groupsOK = m, true
	s.mu.Unlock()
	s.save("groups.json", m)
}

func (s *Store) getPaths() map[string]PathOv {
	s.mu.Lock()
	ok, p := s.pathsOK, s.paths
	s.mu.Unlock()
	if ok {
		return p
	}
	m := map[string]PathOv{}
	s.load("paths.json", &m)
	s.mu.Lock()
	s.paths, s.pathsOK = m, true
	s.mu.Unlock()
	return m
}

func (s *Store) setPaths(m map[string]PathOv) {
	s.mu.Lock()
	s.paths, s.pathsOK = m, true
	s.mu.Unlock()
	s.save("paths.json", m)
}
