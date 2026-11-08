package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed web/*
var webFS embed.FS

const version = "1.1.0-go"

var publicPaths = map[string]bool{"/icon-64.png": true, "/icon-192.png": true, "/icon-512.png": true,
	"/apple-touch-icon.png": true, "/favicon.ico": true, "/manifest.webmanifest": true}

func main() {
	src, _ := filepath.Abs(env("SOURCE_DIR", "/incoming"))
	tgt, _ := filepath.Abs(env("TARGET_DIR", "/media"))
	minAge, _ := strconv.Atoi(env("MIN_AGE", "120"))
	a := &App{Store: NewStore(env("DATA_DIR", "/data")), source: src, target: tgt, minAge: time.Duration(minAge) * time.Second,
		user: env("AUTH_USER", "admin"), pass: env("AUTH_PASS", ""), apiKey: env("API_KEY", ""), version: version}
	a.qcond = sync.NewCond(&a.qmu)
	go a.queueWorker()
	go a.scheduler()
	port := env("PORT", "8080")
	fmt.Printf("MediaButler (Go) %s auf :%s – Quelle %s, Ziel %s\n", version, port, src, tgt)
	if err := http.ListenAndServe(":"+port, a.routes()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func eq(x, y string) bool { return subtle.ConstantTimeCompare([]byte(x), []byte(y)) == 1 }

func (a *App) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.pass == "" || publicPaths[r.URL.Path] ||
			(a.apiKey != "" && r.URL.Path == "/api/hook" && eq(r.Header.Get("X-API-Key"), a.apiKey)) {
			next.ServeHTTP(w, r)
			return
		}
		if u, p, ok := r.BasicAuth(); ok && eq(u, a.user) && eq(p, a.pass) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="MediaButler", charset="UTF-8"`)
		http.Error(w, "Login erforderlich", http.StatusUnauthorized)
	})
}

type M = map[string]any

func js(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func body(r *http.Request) M {
	m := M{}
	json.NewDecoder(r.Body).Decode(&m)
	return m
}

func str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case nil:
		return ""
	}
	return fmt.Sprint(v)
}

func strs(v any, max int) []string {
	l, _ := v.([]any)
	out := []string{}
	for _, x := range l {
		if s, ok := x.(string); ok && len(out) < max {
			out = append(out, s)
		}
	}
	return out
}

func toInt(v any) (int, bool) {
	switch x := v.(type) {
	case float64:
		return int(x), true
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(x))
		return n, err == nil
	case bool, nil:
		return 0, false
	}
	return 0, false
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	get := func(p string, h http.HandlerFunc) { mux.HandleFunc("GET "+p, h) }
	post := func(p string, h http.HandlerFunc) { mux.HandleFunc("POST "+p, h) }
	notYet := func(w http.ResponseWriter, r *http.Request) {
		js(w, 501, M{"error": "In der Go-Version noch nicht verfügbar."})
	}
	for _, p := range []string{"/api/extract", "/api/dupes", "/api/update", "/api/meta", "/api/analysis/status", "/api/analysis/results"} {
		get(p, notYet)
	}
	for _, p := range []string{"/api/extract/scan", "/api/extract/settings", "/api/extract/start", "/api/extract/cancel", "/api/extract/dismiss",
		"/api/extract/password", "/api/extract/password/delete", "/api/hook", "/api/analysis/start", "/api/analysis/cancel", "/api/dupes/start",
		"/api/dupes/cancel", "/api/dupes/delete", "/api/update/config", "/api/update/check", "/api/update/install", "/api/update/rollback",
		"/api/meta/config", "/api/meta/search", "/api/jellyfin/test", "/api/telegram/test"} {
		post(p, notYet)
	}

	page, _ := webFS.ReadFile("web/page.html")
	sum := sha256.Sum256(page)
	build := hex.EncodeToString(sum[:])[:7]
	get("/{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.Write([]byte(strings.ReplaceAll(string(page), "{{VERSION}}", "v"+version+" · "+build)))
	})
	asset := func(file, mime string) http.HandlerFunc {
		b, _ := webFS.ReadFile("web/" + file)
		return func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", mime)
			w.Header().Set("Cache-Control", "public, max-age=86400")
			w.Write(b)
		}
	}
	get("/favicon.ico", asset("icon-64.png", "image/png"))
	get("/icon-64.png", asset("icon-64.png", "image/png"))
	get("/icon-192.png", asset("icon-192.png", "image/png"))
	get("/apple-touch-icon.png", asset("icon-192.png", "image/png"))
	get("/icon-512.png", asset("icon-512.png", "image/png"))
	get("/manifest.webmanifest", asset("manifest.json", "application/manifest+json"))

	get("/api/files", a.hFiles)
	get("/api/rules", func(w http.ResponseWriter, r *http.Request) { js(w, 200, M{"rules": a.rulesOut()}) })
	post("/api/rules", a.hRulesSet)
	post("/api/rules/delete", func(w http.ResponseWriter, r *http.Request) {
		k := str(body(r)["key"])
		var n []Rule
		for _, x := range a.getRules() {
			if x.Key != k {
				n = append(n, x)
			}
		}
		a.setRules(n)
		js(w, 200, M{"ok": true, "rules": a.rulesOut()})
	})
	post("/api/series/preview", a.hSeriesPreview)
	post("/api/preview", func(w http.ResponseWriter, r *http.Request) {
		d := body(r)
		t := ""
		if c := str(d["category"]); c != "" {
			t = a.targetFor(str(d["path"]), c)
		}
		js(w, 200, M{"target": t})
	})
	post("/api/preview/batch", func(w http.ResponseWriter, r *http.Request) {
		l, _ := body(r)["items"].([]any)
		out := []string{}
		for i, x := range l {
			if i >= 5000 {
				break
			}
			e, _ := x.(M)
			if c := str(e["category"]); inCats(c) {
				out = append(out, a.targetFor(str(e["path"]), c))
			} else {
				out = append(out, "")
			}
		}
		js(w, 200, M{"targets": out})
	})
	post("/api/groups/preview", a.hGroupsPreview)
	post("/api/groups", func(w http.ResponseWriter, r *http.Request) {
		mg, _, err := a.mgBuild(body(r))
		if err != nil {
			js(w, 400, M{"error": err.Error()})
			return
		}
		n := map[string]Group{}
		for k, v := range a.getGroups() {
			n[k] = v
		}
		for k, v := range mg {
			n[k] = v
		}
		a.setGroups(n)
		js(w, 200, M{"ok": true, "n": len(mg)})
	})
	post("/api/groups/delete", func(w http.ResponseWriter, r *http.Request) {
		del := map[string]bool{}
		for _, x := range strs(body(r)["items"], 5000) {
			del[x] = true
		}
		n := map[string]Group{}
		for k, v := range a.getGroups() {
			if !del[k] {
				n[k] = v
			}
		}
		a.setGroups(n)
		js(w, 200, M{"ok": true})
	})
	post("/api/paths/preview", func(w http.ResponseWriter, r *http.Request) {
		d := body(r)
		rels := strs(d["items"], 5000)
		if len(rels) == 0 {
			js(w, 200, M{"targets": []string{}})
			return
		}
		_, _, t, err := a.folderTargets(rels, str(d["folder"]))
		if err != nil {
			js(w, 400, M{"error": err.Error()})
			return
		}
		js(w, 200, M{"targets": t})
	})
	post("/api/paths", a.hPathsSet)
	post("/api/start", a.hStart)
	post("/api/files/delete", a.hFilesDelete)
	post("/api/queue/remove", func(w http.ResponseWriter, r *http.Request) {
		id, _ := toInt(body(r)["id"])
		a.qmu.Lock()
		var n []*batch
		for _, b := range a.pending {
			if b.ID != id || b == a.active {
				n = append(n, b)
			}
		}
		a.pending = n
		a.qmu.Unlock()
		js(w, 200, M{"ok": true})
	})
	post("/api/queue/clear", func(w http.ResponseWriter, r *http.Request) {
		a.qmu.Lock()
		var n []*batch
		for _, b := range a.pending {
			if b == a.active {
				n = append(n, b)
			}
		}
		a.pending = n
		a.qmu.Unlock()
		js(w, 200, M{"ok": true})
	})
	post("/api/run", func(w http.ResponseWriter, r *http.Request) {
		if ok, msg := a.trigger("manuell (alle fertigen)"); ok {
			js(w, 200, M{"ok": true, "info": msg})
		} else {
			js(w, 409, M{"error": msg})
		}
	})
	get("/api/status", a.hStatus)
	get("/api/job/queue", a.hJobQueue)
	post("/api/cancel", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		a.job.Cancel = true
		a.mu.Unlock()
		js(w, 200, M{"ok": true})
	})
	get("/api/history", func(w http.ResponseWriter, r *http.Request) {
		var h []HistoryEntry
		a.load("history.json", &h)
		out := make([]HistoryEntry, 0, len(h))
		for i := len(h) - 1; i >= 0; i-- {
			out = append(out, h[i])
		}
		js(w, 200, M{"items": out})
	})
	post("/api/history/clear", func(w http.ResponseWriter, r *http.Request) {
		a.save("history.json", []HistoryEntry{})
		js(w, 200, M{"ok": true})
	})
	get("/api/leftovers", func(w http.ResponseWriter, r *http.Request) {
		out := []M{}
		for _, n := range a.topEntries() {
			out = append(out, a.entryInfo(n))
		}
		js(w, 200, M{"dirs": out, "min_age": int(a.minAge.Seconds())})
	})
	post("/api/leftovers/delete", a.hLeftoversDelete)
	post("/api/leftovers/empty", func(w http.ResponseWriter, r *http.Request) {
		if a.jobRunning() || a.busy() {
			js(w, 409, M{"error": "Während des Kopierens nicht möglich."})
			return
		}
		js(w, 200, M{"deleted": a.removeEmptyDirs()})
	})
	get("/api/settings", func(w http.ResponseWriter, r *http.Request) { js(w, 200, a.settingsPayload()) })
	post("/api/settings", a.hSettingsSet)
	get("/api/cron", func(w http.ResponseWriter, r *http.Request) {
		n, err := nextRuns(r.URL.Query().Get("expr"), 3)
		if err != nil {
			js(w, 200, M{"ok": false, "error": err.Error()})
			return
		}
		js(w, 200, M{"ok": true, "next": n})
	})
	return a.auth(mux)
}

func (a *App) jobRunning() bool { a.mu.Lock(); defer a.mu.Unlock(); return a.job.Running }

func (a *App) rulesOut() []Rule {
	r := a.getRules()
	if r == nil {
		return []Rule{}
	}
	return r
}
