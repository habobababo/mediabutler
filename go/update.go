package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Selbst-Update der Go-Version: lädt das Release-Asset "mediabutler-linux-<arch>" aus GitHub (Workflow .github/workflows/go-release.yml),
// ersetzt die laufende Datei und beendet den Prozess; restart: unless-stopped startet sie neu.

var (
	updMu  sync.Mutex
	reRepo = regexp.MustCompile(`^[\w.-]+/[\w.-]+$`)
	ghAPI  = strings.TrimRight(env("GITHUB_API", "https://api.github.com"), "/")
)

func (a *App) updConf() (string, string) {
	d := M{}
	a.load("update.json", &d)
	repo := firstNonEmpty(strings.TrimSpace(str(d["repo"])), strings.TrimSpace(env("UPDATE_REPO", "")), "habobababo/mediabutler")
	return strings.Trim(repo, "/"), strings.TrimSpace(firstNonEmpty(str(d["token"]), env("UPDATE_TOKEN", "")))
}

func ghGet(url, token, accept string) ([]byte, error) {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", accept)
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "MediaButler")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{Timeout: 120 * time.Second}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("GitHub nicht erreichbar: %v", err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 401 || resp.StatusCode == 403:
		return nil, errors.New("GitHub lehnt den Zugriff ab. Token prüfen (Recht: Contents lesen, Repo ausgewählt).")
	case resp.StatusCode == 404:
		return nil, errors.New("Repo oder Release nicht gefunden. Ohne Token ist ein privates Repo unsichtbar.")
	case resp.StatusCode >= 400:
		return nil, fmt.Errorf("GitHub-Fehler %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

type release struct {
	Tag, Name, Date string
	URL             string
	Data            []byte
}

func (a *App) fetchRelease(repo, token string, withData bool) (*release, error) {
	b, err := ghGet(fmt.Sprintf("%s/repos/%s/releases/latest", ghAPI, repo), token, "application/vnd.github+json")
	if err != nil {
		return nil, err
	}
	var j struct {
		Tag    string `json:"tag_name"`
		Name   string `json:"name"`
		Date   string `json:"published_at"`
		Assets []struct {
			Name string `json:"name"`
			URL  string `json:"url"`
		} `json:"assets"`
	}
	json.Unmarshal(b, &j)
	want := "mediabutler-linux-" + runtime.GOARCH
	r := &release{Tag: j.Tag, Name: j.Name, Date: j.Date}
	for _, as := range j.Assets {
		if as.Name == want {
			r.URL = as.URL
		}
	}
	if r.URL == "" {
		return nil, fmt.Errorf("Im neuesten Release (%s) fehlt die Datei %s.", j.Tag, want)
	}
	if withData {
		d, err := ghGet(r.URL, token, "application/octet-stream")
		if err != nil {
			return nil, err
		}
		if len(d) < 1<<20 || string(d[:4]) != "\x7fELF" {
			return nil, errors.New("Die geladene Datei sieht nicht nach dem MediaButler-Programm aus. Nichts geändert.")
		}
		r.Data = d
	}
	return r, nil
}

func sha7(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func exePath() string { p, _ := os.Executable(); p, _ = filepath.EvalSymlinks(p); return p }

func (a *App) writeExe(data []byte) error {
	exe := exePath()
	old, err := os.ReadFile(exe)
	if err != nil {
		return err
	}
	os.MkdirAll(a.dir, 0o755)
	os.WriteFile(filepath.Join(a.dir, "mediabutler.bak"), old, 0o755)
	tmp := exe + ".new"
	if err := os.WriteFile(tmp, data, 0o755); err != nil {
		if errors.Is(err, syscall.EROFS) || errors.Is(err, syscall.EACCES) {
			return errors.New("Das Programm liegt schreibgeschützt im Container. Bei Docker: Update über neues Image (docker compose pull/build).")
		}
		return fmt.Errorf("Programmdatei nicht beschreibbar: %v", err)
	}
	if err := os.Rename(tmp, exe); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("Programmdatei nicht ersetzbar: %v", err)
	}
	return nil
}

func restartSoon() { time.AfterFunc(1500*time.Millisecond, func() { os.Exit(0) }) }

func (a *App) updBlocked() string {
	a.qmu.Lock()
	n := len(a.pending)
	a.qmu.Unlock()
	if a.busy() || n > 0 {
		return "Es läuft gerade ein Kopiervorgang. Bitte warten, bis er fertig ist."
	}
	return ""
}

func (a *App) updRoutes(get, post func(string, http.HandlerFunc)) {
	get("/api/update", func(w http.ResponseWriter, r *http.Request) {
		repo, token := a.updConf()
		_, bakErr := os.Stat(filepath.Join(a.dir, "mediabutler.bak"))
		tf := filepath.Join(filepath.Dir(exePath()), ".w")
		wr := os.WriteFile(tf, []byte("x"), 0o644) == nil
		os.Remove(tf)
		js(w, 200, M{"repo": repo, "token_set": token != "", "version": version, "build": a.build, "backup": bakErr == nil, "writable": wr})
	})
	post("/api/update/config", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		d := M{}
		a.load("update.json", &d)
		repo := strings.Trim(strings.TrimSpace(str(b["repo"])), "/")
		if repo != "" && !reRepo.MatchString(repo) {
			js(w, 400, M{"error": "Repo bitte als „besitzer/name“ angeben."})
			return
		}
		d["repo"] = repo
		if t := strings.TrimSpace(str(b["token"])); t != "" {
			d["token"] = t
		}
		if b["clear_token"] == true {
			delete(d, "token")
		}
		a.save("update.json", d)
		js(w, 200, M{"ok": true})
	})
	post("/api/update/check", func(w http.ResponseWriter, r *http.Request) {
		repo, token := a.updConf()
		rel, err := a.fetchRelease(repo, token, true)
		if err != nil {
			js(w, 502, M{"error": err.Error()})
			return
		}
		cur, _ := os.ReadFile(exePath())
		out := M{"ok": true, "current": sha7(rel.Data) == sha7(cur), "remote_version": strings.TrimPrefix(rel.Tag, "v"), "remote_build": sha7(rel.Data)[:7],
			"pending": sha7(cur)[:7] != a.build, "sha": rel.Tag, "msg": rel.Name}
		if len(rel.Date) >= 16 {
			out["date"] = strings.Replace(rel.Date[:16], "T", " ", 1)
		}
		js(w, 200, out)
	})
	post("/api/update/install", func(w http.ResponseWriter, r *http.Request) {
		updMu.Lock()
		defer updMu.Unlock()
		if why := a.updBlocked(); why != "" {
			js(w, 409, M{"error": why})
			return
		}
		repo, token := a.updConf()
		rel, err := a.fetchRelease(repo, token, true)
		if err != nil {
			js(w, 502, M{"error": err.Error()})
			return
		}
		cur, _ := os.ReadFile(exePath())
		if sha7(rel.Data) == sha7(cur) {
			js(w, 200, M{"ok": true, "changed": false, "restart": false})
			return
		}
		if err := a.writeExe(rel.Data); err != nil {
			js(w, 502, M{"error": err.Error()})
			return
		}
		fmt.Fprintf(os.Stderr, "Update installiert: %s -> %s, Neustart\n", version, rel.Tag)
		restartSoon()
		js(w, 200, M{"ok": true, "changed": true, "restart": true, "version": strings.TrimPrefix(rel.Tag, "v"), "build": sha7(rel.Data)[:7]})
	})
	post("/api/update/rollback", func(w http.ResponseWriter, r *http.Request) {
		bak, err := os.ReadFile(filepath.Join(a.dir, "mediabutler.bak"))
		if err != nil {
			js(w, 404, M{"error": "Keine Sicherung vorhanden."})
			return
		}
		if why := a.updBlocked(); why != "" {
			js(w, 409, M{"error": why})
			return
		}
		if len(bak) < 1<<20 || string(bak[:4]) != "\x7fELF" {
			js(w, 500, M{"error": "Die Sicherung ist unbrauchbar."})
			return
		}
		if err := a.writeExe(bak); err != nil {
			js(w, 500, M{"error": err.Error()})
			return
		}
		restartSoon()
		js(w, 200, M{"ok": true, "restart": true})
	})
}
