package main

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

// Automatischer Abgleich neuer Titel mit TMDB/TVDB/AniList/Jikan:
// genau ein eindeutiger Treffer -> Regel wird angelegt, sonst "abweichend" / "nicht gefunden" (Telegram + Oberfläche).

type AutoCand struct {
	Src   string `json:"src"`
	Title string `json:"title"`
	Orig  string `json:"orig"`
	Year  string `json:"year"`
	Kind  string `json:"kind"`
	IDVal string `json:"idval"`
	ID    string `json:"id"`
}

type AutoItem struct {
	Key      string     `json:"key"`
	Detected string     `json:"detected"`
	Year     string     `json:"year"`
	Kind     string     `json:"kind"`
	Status   string     `json:"status"` // zugeordnet | abweichend | nicht gefunden | ignoriert
	Name     string     `json:"name,omitempty"`
	RYear    string     `json:"ryear,omitempty"`
	IDVal    string     `json:"idval,omitempty"`
	Src      string     `json:"src,omitempty"`
	Cands    []AutoCand `json:"cands,omitempty"`
	T        string     `json:"t"`
}

type autoFile struct {
	Items map[string]AutoItem `json:"items"`
}

var autoMu sync.Mutex

func (a *App) autoLoad() autoFile {
	f := autoFile{Items: map[string]AutoItem{}}
	a.load("autometa.json", &f)
	if f.Items == nil {
		f.Items = map[string]AutoItem{}
	}
	return f
}

func (a *App) autoEnabled() bool {
	d := M{}
	a.load("meta.json", &d)
	v, ok := d["auto"].(bool)
	return !ok || v
}

type autoSubject struct{ key, title, year, kind string }

func (a *App) autoSubject(rel, cat string) (autoSubject, bool) {
	if _, ok := a.getGroups()[rel]; ok {
		return autoSubject{}, false
	}
	switch cat {
	case "Serien", "Anime", "Dokus":
		ep := parseEp(rel, cat == "Anime", cat == "Serien" || cat == "Anime", false, nil)
		if ep == nil || ep.title == "" {
			return autoSubject{}, false
		}
		year := ep.year
		if year == "" {
			_, year, _ = dirYear(rel, ep.title)
		}
		kind := "tv"
		if cat == "Anime" {
			kind = "anime"
		}
		return autoSubject{skey(ep.title), ep.title, year, kind}, skey(ep.title) != ""
	case "Filme":
		stm := stem(path.Base(rel))
		title, year := titleYear(stm)
		if year == "" || title == "" {
			if t2, y2, _ := dirYear(rel, title); y2 != "" {
				title, year = t2, y2
			}
		}
		if skey(title) == "" {
			return autoSubject{}, false
		}
		return autoSubject{movieKey(title, year), title, year, "movie"}, true
	}
	return autoSubject{}, false
}

func movieKey(title, year string) string { return "m:" + skey(title) + year }

// autoInfo: Zuordnung für die Anzeige in der Dateiliste.
func (a *App) autoInfo(sub autoSubject, st autoFile) M {
	if r := a.ruleByKey(sub.key); r != nil {
		src := "Regel"
		if it, ok := st.Items[sub.key]; ok && it.Status == "zugeordnet" {
			src = "automatisch"
		}
		return M{"status": "zugeordnet", "name": firstNonEmpty(r.Name, sub.title), "year": r.Year, "id": r.TMDB, "src": src}
	}
	if it, ok := st.Items[sub.key]; ok && it.Status != "zugeordnet" {
		return M{"status": it.Status, "n": len(it.Cands)}
	}
	return nil
}

func hitFits(kind, hk string) bool {
	if kind == "movie" {
		return hk == "Film" || hk == "Anime-Film"
	}
	return hk != "Film"
}

func evalMatch(sub autoSubject, res []M) (AutoItem, bool) {
	it := AutoItem{Key: sub.key, Detected: sub.title, Year: sub.year, Kind: sub.kind, T: time.Now().Format("2006-01-02 15:04:05")}
	dk := skey(sub.title)
	var fit []AutoCand
	for _, r := range res {
		c := AutoCand{str(r["src"]), str(r["title"]), str(r["orig"]), str(r["year"]), str(r["kind"]), str(r["idval"]), str(r["id"])}
		if hitFits(sub.kind, c.Kind) {
			fit = append(fit, c)
		}
	}
	if len(fit) == 0 {
		it.Status = "nicht gefunden"
		return it, true
	}
	groups := map[string][]AutoCand{}
	var order []string
	for _, c := range fit {
		if (skey(c.Title) == dk || skey(c.Orig) == dk) && (sub.year == "" || c.Year == "" || sub.year == c.Year) {
			k := skey(c.Title) + "|" + c.Year
			if _, ok := groups[k]; !ok {
				order = append(order, k)
			}
			groups[k] = append(groups[k], c)
		}
	}
	if len(order) == 1 {
		g := groups[order[0]]
		best := g[0]
		rank := func(c AutoCand) int {
			switch {
			case c.Src == "tmdb" && c.IDVal != "":
				return 0
			case c.Src == "tvdb" && c.IDVal != "":
				return 1
			case c.IDVal != "":
				return 2
			}
			return 3
		}
		for _, c := range g {
			if rank(c) < rank(best) {
				best = c
			}
		}
		it.Status, it.Name, it.RYear, it.IDVal, it.Src = "zugeordnet", best.Title, best.Year, best.IDVal, best.Src
		return it, true
	}
	it.Status = "abweichend"
	seen := map[string]bool{}
	for _, c := range fit {
		k := c.Src + "|" + c.ID
		if !seen[k] && len(it.Cands) < 5 {
			seen[k] = true
			it.Cands = append(it.Cands, c)
		}
	}
	return it, true
}

func (a *Store) ruleByKey(k string) *Rule {
	for _, r := range a.getRules() {
		if r.Key == k {
			x := r
			return &x
		}
	}
	return nil
}

func (a *App) autoAddRule(it AutoItem, name, year, idval string) {
	rules := []Rule{}
	for _, x := range a.getRules() {
		if x.Key != it.Key {
			rules = append(rules, x)
		}
	}
	rules = append(rules, Rule{Key: it.Key, Detected: it.Detected, Name: safe(pretty(strings.TrimSpace(name))), Year: year, TMDB: idNorm(idval)})
	a.setRules(rules)
}

// autoMatchCycle prüft bis zu limit neue Titel. -> Anzahl neu geprüfter Titel
func (a *App) autoMatchCycle(limit int) int {
	if !a.autoEnabled() {
		return 0
	}
	autoMu.Lock()
	defer autoMu.Unlock()
	st := a.autoLoad()
	cm, bl := a.copied(), a.extractBlocked()
	var subs []autoSubject
	seen := map[string]bool{}
	for _, rel := range walkFiles(a.source) {
		name := path.Base(rel)
		if !videoExt[lowerExt(name)] || strings.HasPrefix(name, ".") || reSample.MatchString(name) || hiddenRel(rel) || exBlocked(rel, bl) || a.isCopied(cm, rel) {
			continue
		}
		cat := a.guessCategory(rel)
		if cat == "" {
			continue
		}
		sub, ok := a.autoSubject(rel, cat)
		if !ok || seen[sub.key] {
			continue
		}
		seen[sub.key] = true
		if _, done := st.Items[sub.key]; done || a.ruleByKey(sub.key) != nil {
			continue
		}
		subs = append(subs, sub)
	}
	n := 0
	var pending []AutoItem
	for _, sub := range subs {
		if n >= limit {
			break
		}
		res, errs := a.metaSearchAll(sub.title, sub.kind, sub.year)
		if len(res) == 0 && len(errs) > 0 && !onlyEmptyErrs(errs) {
			continue // Quellen nicht erreichbar oder kein Key: später erneut versuchen
		}
		it, _ := evalMatch(sub, res)
		n++
		if it.Status == "zugeordnet" {
			a.autoAddRule(it, it.Name, it.RYear, it.IDVal)
		} else {
			pending = append(pending, it)
		}
		st.Items[it.Key] = it
		a.save("autometa.json", st)
		time.Sleep(400 * time.Millisecond)
	}
	if len(pending) > 0 {
		a.autoNotify(pending)
	}
	return n
}

func onlyEmptyErrs(errs M) bool { return false }

func (a *App) autoNotify(items []AutoItem) {
	var lines []string
	for i, it := range items {
		if i >= 8 {
			break
		}
		t := it.Detected
		if it.Year != "" {
			t += " (" + it.Year + ")"
		}
		l := fmt.Sprintf("• %s: %s", t, it.Status)
		if it.Status == "abweichend" && len(it.Cands) > 0 {
			var cs []string
			for j, c := range it.Cands {
				if j < 3 {
					y := ""
					if c.Year != "" {
						y = " (" + c.Year + ")"
					}
					cs = append(cs, c.Title+y)
				}
			}
			l += " – " + strings.Join(cs, " / ")
		}
		lines = append(lines, l)
	}
	txt := fmt.Sprintf("🔎 MediaButler: %d Titel bitte prüfen (kein eindeutiger Treffer)\n%s", len(items), strings.Join(lines, "\n"))
	if len(items) > 8 {
		txt += fmt.Sprintf("\n… und %d weitere", len(items)-8)
	}
	a.notifyW(txt+a.linkLine("sort"), "tg_unclear", false, false)
}

func (a *App) autoRoutes(get, post func(string, http.HandlerFunc)) {
	get("/api/automatch", func(w http.ResponseWriter, r *http.Request) {
		st := a.autoLoad()
		pending, done := []AutoItem{}, []AutoItem{}
		for _, it := range st.Items {
			switch it.Status {
			case "abweichend", "nicht gefunden":
				if a.ruleByKey(it.Key) == nil {
					pending = append(pending, it)
				}
			case "zugeordnet":
				done = append(done, it)
			}
		}
		sort.Slice(pending, func(i, j int) bool { return pending[i].Detected < pending[j].Detected })
		sort.Slice(done, func(i, j int) bool { return done[i].T > done[j].T })
		if len(done) > 20 {
			done = done[:20]
		}
		js(w, 200, M{"enabled": a.autoEnabled(), "pending": pending, "done": done})
	})
	post("/api/automatch/apply", func(w http.ResponseWriter, r *http.Request) {
		d := body(r)
		key := str(d["key"])
		idx, _ := toInt(d["idx"])
		autoMu.Lock()
		defer autoMu.Unlock()
		st := a.autoLoad()
		it, ok := st.Items[key]
		if !ok || idx < 0 || idx >= len(it.Cands) {
			js(w, 400, M{"error": "Eintrag nicht gefunden."})
			return
		}
		c := it.Cands[idx]
		a.autoAddRule(it, c.Title, c.Year, c.IDVal)
		it.Status, it.Name, it.RYear, it.IDVal, it.Src = "zugeordnet", c.Title, c.Year, c.IDVal, c.Src
		st.Items[key] = it
		a.save("autometa.json", st)
		js(w, 200, M{"ok": true})
	})
	post("/api/automatch/dismiss", func(w http.ResponseWriter, r *http.Request) {
		key := str(body(r)["key"])
		autoMu.Lock()
		defer autoMu.Unlock()
		st := a.autoLoad()
		if it, ok := st.Items[key]; ok {
			it.Status = "ignoriert"
			st.Items[key] = it
			a.save("autometa.json", st)
		}
		js(w, 200, M{"ok": true})
	})
	post("/api/automatch/retry", func(w http.ResponseWriter, r *http.Request) {
		key := str(body(r)["key"])
		autoMu.Lock()
		st := a.autoLoad()
		delete(st.Items, key)
		a.save("autometa.json", st)
		autoMu.Unlock()
		go a.autoMatchCycle(5)
		js(w, 200, M{"ok": true})
	})
}

func (a *App) autoLoop() {
	time.Sleep(10 * time.Second)
	for {
		if st, err := os.Stat(a.source); err == nil && st.IsDir() {
			a.autoMatchCycle(4)
		}
		time.Sleep(watchEvery)
	}
}
