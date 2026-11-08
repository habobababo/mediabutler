package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Bibliothek-Abgleich: prüft alle Titel-Ordner im Zielordner gegen TMDB/TVDB/AniList/Jikan.
// ok = Name, Jahr und ID passen; fix = eindeutiger Treffer, Ordner (und Dateinamen darin) können angepasst werden;
// abweichend / nicht gefunden = manuell prüfen.

type LibItem struct {
	Path     string     `json:"path"` // Kategorie/Ordner
	Cat      string     `json:"cat"`
	Folder   string     `json:"folder"`
	Title    string     `json:"title"`
	Year     string     `json:"year"`
	ID       string     `json:"id"`
	Status   string     `json:"status"` // ok | fix | abweichend | nicht gefunden | ignoriert
	Proposal string     `json:"proposal,omitempty"`
	PName    string     `json:"pname,omitempty"`
	PYear    string     `json:"pyear,omitempty"`
	PID      string     `json:"pid,omitempty"`
	Src      string     `json:"src,omitempty"`
	Cands    []AutoCand `json:"cands,omitempty"`
	T        string     `json:"t"`
}

type libMatchFile struct {
	Items map[string]LibItem `json:"items"`
	TS    string             `json:"ts"`
}

type libMatchState struct {
	mu            sync.Mutex
	running, stop bool
	done, total   int
	cur           string
}

var lm libMatchState
var reFolder = regexp.MustCompile(`^(.*?)(?:\s*\((\d{4})\))?(?:\s*\[(tmdb|tvdb)id-(\d+)\])?$`)

func (a *App) lmLoad() libMatchFile {
	f := libMatchFile{Items: map[string]LibItem{}}
	a.load("libmatch.json", &f)
	if f.Items == nil {
		f.Items = map[string]LibItem{}
	}
	return f
}

func parseLibFolder(name string) (title, year, id string) {
	m := reFolder.FindStringSubmatch(name)
	if m == nil {
		return name, "", ""
	}
	title, year = strings.TrimSpace(m[1]), m[2]
	if m[3] != "" {
		id = idNorm(m[3] + "id-" + m[4])
		if m[3] == "tvdb" {
			id = "tvdb:" + m[4]
		} else {
			id = m[4]
		}
	}
	return
}

func libKind(cat string) string {
	switch cat {
	case "Filme":
		return "movie"
	case "Anime":
		return "anime"
	}
	return "tv"
}

func (a *App) libFolders(cats map[string]bool) []LibItem {
	var out []LibItem
	for _, cat := range categories {
		if len(cats) > 0 && !cats[cat] {
			continue
		}
		ents, err := os.ReadDir(filepath.Join(a.target, cat))
		if err != nil {
			continue
		}
		for _, e := range ents {
			if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
				continue
			}
			t, y, id := parseLibFolder(e.Name())
			if skey(t) == "" {
				continue
			}
			out = append(out, LibItem{Path: cat + "/" + e.Name(), Cat: cat, Folder: e.Name(), Title: t, Year: y, ID: id})
		}
	}
	return out
}

func (a *App) lmEval(it LibItem, res []M) LibItem {
	it.T = time.Now().Format("2006-01-02 15:04:05")
	sub := autoSubject{key: skey(it.Title), title: it.Title, year: it.Year, kind: libKind(it.Cat)}
	ev, _ := evalMatch(sub, res)
	switch ev.Status {
	case "zugeordnet":
		prop := showName(pretty(ev.Name), ev.RYear)
		if ev.IDVal != "" && idNorm(ev.IDVal) != "" {
			prop += idTag(ev.IDVal)
		} else if it.ID != "" { // vorhandene ID nicht verlieren
			if strings.HasPrefix(it.ID, "tvdb:") {
				prop += " [tvdbid-" + it.ID[5:] + "]"
			} else {
				prop += " [tmdbid-" + it.ID + "]"
			}
		}
		it.PName, it.PYear, it.PID, it.Src = ev.Name, ev.RYear, ev.IDVal, ev.Src
		if it.ID != "" && ev.IDVal != "" && idNorm(ev.IDVal) != it.ID {
			// vorhandene ID weicht von der API ab -> nicht automatisch ändern
			it.Status, it.Proposal = "abweichend", prop
			it.Cands = append([]AutoCand{{Src: ev.Src, Title: ev.Name, Year: ev.RYear, IDVal: ev.IDVal}}, it.Cands...)
			return it
		}
		if prop == it.Folder {
			it.Status, it.Proposal = "ok", ""
		} else {
			it.Status, it.Proposal = "fix", prop
		}
	case "nicht gefunden":
		it.Status = "nicht gefunden"
		if it.ID != "" {
			it.Status = "ok" // ID vorhanden, Titel lässt sich nur nicht eindeutig suchen
			it.Proposal = ""
		}
	default:
		it.Status, it.Cands = "abweichend", ev.Cands
		if it.ID != "" {
			it.Status = "ok" // vorhandene ID hat Vorrang vor unscharfer Suche
		}
	}
	return it
}

func (a *App) lmRun(cats map[string]bool, force bool) {
	lm.mu.Lock()
	if lm.running {
		lm.mu.Unlock()
		return
	}
	lm.running, lm.stop, lm.done = true, false, 0
	lm.mu.Unlock()
	defer func() { lm.mu.Lock(); lm.running = false; lm.cur = ""; lm.mu.Unlock() }()
	items := a.libFolders(cats)
	sort.Slice(items, func(i, j int) bool { return items[i].Path < items[j].Path })
	lm.mu.Lock()
	lm.total = len(items)
	lm.mu.Unlock()
	st := a.lmLoad()
	// verschwundene Ordner aufräumen
	exist := map[string]bool{}
	for _, it := range items {
		exist[it.Path] = true
	}
	for k, v := range st.Items {
		if (len(cats) == 0 || cats[v.Cat]) && !exist[k] {
			delete(st.Items, k)
		}
	}
	var nOK, nFix, nCheck int
	for _, it := range items {
		lm.mu.Lock()
		stop := lm.stop
		lm.cur = it.Folder
		lm.mu.Unlock()
		if stop {
			break
		}
		old, have := st.Items[it.Path]
		if have && !force && (old.Status == "ok" || old.Status == "ignoriert") && old.Folder == it.Folder {
			lm.mu.Lock()
			lm.done++
			lm.mu.Unlock()
			continue
		}
		res, errs := a.metaSearchAll(it.Title, libKind(it.Cat), it.Year)
		if len(res) == 0 && len(errs) > 0 {
			it.Status, it.T = "nicht gefunden", time.Now().Format("2006-01-02 15:04:05")
			it.Proposal = "Quellen nicht erreichbar oder kein API-Key hinterlegt"
		} else {
			it = a.lmEval(it, res)
		}
		st.Items[it.Path] = it
		lm.mu.Lock()
		lm.done++
		lm.mu.Unlock()
		if lm.done%5 == 0 {
			st.TS = time.Now().Format("2006-01-02 15:04:05")
			a.save("libmatch.json", st)
		}
		time.Sleep(350 * time.Millisecond)
	}
	st.TS = time.Now().Format("2006-01-02 15:04:05")
	a.save("libmatch.json", st)
	for _, v := range st.Items {
		switch v.Status {
		case "ok":
			nOK++
		case "fix":
			nFix++
		case "abweichend", "nicht gefunden":
			nCheck++
		}
	}
	if !lm.stop {
		a.notify(fmt.Sprintf("🔗 Bibliotheks-Abgleich fertig\n✅ %d passen · 🛠️ %d mit eindeutigem Vorschlag · ⚠️ %d prüfen%s", nOK, nFix, nCheck, a.linkLine("lmatch")), "tg_done")
	}
}

// lmApply benennt Ordner und Dateien darin um. -> neuer Ordnername
func (a *App) lmApply(it LibItem, name, year, idval string) (string, error) {
	base := showName(pretty(name), year)
	newFolder := base
	if t := idTag(idval); t != "" {
		newFolder += t
	} else if it.ID != "" {
		if strings.HasPrefix(it.ID, "tvdb:") {
			newFolder += " [tvdbid-" + it.ID[5:] + "]"
		} else {
			newFolder += " [tmdbid-" + it.ID + "]"
		}
	}
	oldBase := showName(it.Title, it.Year)
	oldDir := filepath.Join(a.target, it.Cat, it.Folder)
	newDir := filepath.Join(a.target, it.Cat, newFolder)
	if _, err := os.Stat(oldDir); err != nil {
		return "", fmt.Errorf("Ordner nicht mehr vorhanden.")
	}
	if newFolder != it.Folder {
		if _, err := os.Stat(newDir); err == nil {
			return "", fmt.Errorf("Zielordner existiert schon: %s. Bitte Duplikate prüfen.", newFolder)
		}
		if err := os.Rename(oldDir, newDir); err != nil {
			return "", err
		}
	}
	// Dateien/Unterordner mit altem Namenspräfix anpassen (tiefste zuerst)
	var paths []string
	filepath.WalkDir(newDir, func(p string, d os.DirEntry, err error) error {
		if err == nil && p != newDir {
			paths = append(paths, p)
		}
		return nil
	})
	sort.Slice(paths, func(i, j int) bool { return len(paths[i]) > len(paths[j]) })
	if oldBase != base {
		for _, p := range paths {
			n := filepath.Base(p)
			if strings.HasPrefix(n, oldBase) {
				np := filepath.Join(filepath.Dir(p), base+strings.TrimPrefix(n, oldBase))
				if _, err := os.Stat(np); err != nil {
					os.Rename(p, np)
				}
			}
		}
	}
	// Regel anlegen, damit neue Folgen im selben Ordner landen
	key := skey(it.Title)
	if it.Cat == "Filme" {
		key = movieKey(it.Title, it.Year)
	}
	a.autoAddRule(AutoItem{Key: key, Detected: it.Title}, name, year, idval)
	a.libInvalidate()
	return newFolder, nil
}

func (a *App) lmRoutes(get, post func(string, http.HandlerFunc)) {
	get("/api/libmatch", func(w http.ResponseWriter, r *http.Request) {
		st := a.lmLoad()
		items := make([]LibItem, 0, len(st.Items))
		for _, v := range st.Items {
			items = append(items, v)
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Path < items[j].Path })
		lm.mu.Lock()
		defer lm.mu.Unlock()
		js(w, 200, M{"running": lm.running, "done": lm.done, "total": lm.total, "cur": lm.cur, "ts": st.TS, "items": items})
	})
	post("/api/libmatch/start", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		cats := map[string]bool{}
		for _, c := range strs(b["cats"], 10) {
			cats[c] = true
		}
		lm.mu.Lock()
		run := lm.running
		lm.mu.Unlock()
		if run {
			js(w, 200, M{"ok": true})
			return
		}
		go a.lmRun(cats, b["force"] == true)
		time.Sleep(200 * time.Millisecond)
		js(w, 200, M{"ok": true})
	})
	post("/api/libmatch/stop", func(w http.ResponseWriter, r *http.Request) {
		lm.mu.Lock()
		lm.stop = true
		lm.mu.Unlock()
		js(w, 200, M{"ok": true})
	})
	post("/api/libmatch/apply", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		p := str(b["path"])
		st := a.lmLoad()
		it, ok := st.Items[p]
		if !ok {
			js(w, 400, M{"error": "Eintrag nicht gefunden."})
			return
		}
		name, year, idv := it.PName, it.PYear, it.PID
		if idx, has := toInt(b["idx"]); has && b["idx"] != nil {
			if idx < 0 || idx >= len(it.Cands) {
				js(w, 400, M{"error": "Vorschlag nicht gefunden."})
				return
			}
			c := it.Cands[idx]
			name, year, idv = c.Title, c.Year, c.IDVal
		} else if it.Status != "fix" {
			js(w, 400, M{"error": "Kein eindeutiger Vorschlag."})
			return
		}
		nf, err := a.lmApply(it, name, year, idv)
		if err != nil {
			js(w, 400, M{"error": err.Error()})
			return
		}
		delete(st.Items, p)
		it.Folder, it.Path, it.Title, it.Year = nf, it.Cat+"/"+nf, pretty(name), year
		it.ID, it.Status, it.Proposal, it.Cands = idNorm(idv), "ok", "", nil
		st.Items[it.Path] = it
		a.save("libmatch.json", st)
		js(w, 200, M{"ok": true, "folder": nf})
	})
	post("/api/libmatch/applyall", func(w http.ResponseWriter, r *http.Request) {
		st := a.lmLoad()
		n, fail := 0, []string{}
		for p, it := range st.Items {
			if it.Status != "fix" {
				continue
			}
			nf, err := a.lmApply(it, it.PName, it.PYear, it.PID)
			if err != nil {
				fail = append(fail, it.Folder+": "+err.Error())
				continue
			}
			delete(st.Items, p)
			it.Folder, it.Path, it.Title, it.Year = nf, it.Cat+"/"+nf, pretty(it.PName), it.PYear
			it.ID, it.Status, it.Proposal = idNorm(it.PID), "ok", ""
			st.Items[it.Path] = it
			n++
		}
		a.save("libmatch.json", st)
		js(w, 200, M{"ok": true, "applied": n, "failed": fail})
	})
	post("/api/libmatch/ignore", func(w http.ResponseWriter, r *http.Request) {
		p := str(body(r)["path"])
		st := a.lmLoad()
		if it, ok := st.Items[p]; ok {
			it.Status = "ignoriert"
			st.Items[p] = it
			a.save("libmatch.json", st)
		}
		js(w, 200, M{"ok": true})
	})
}
