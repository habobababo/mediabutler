package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	reNonDigit = regexp.MustCompile(`\D`)
	reHTTP     = regexp.MustCompile(`^https?://\S+$`)
	reNat      = regexp.MustCompile(`(\d+)`)
)

func yearOf(v any) string {
	y := reNonDigit.ReplaceAllString(str(v), "")
	if len(y) > 4 {
		y = y[:4]
	}
	return y
}

func natLess(x, y string) bool {
	a, b := reNat.Split(strings.ToLower(x), -1), reNat.Split(strings.ToLower(y), -1)
	na, nb := reNat.FindAllString(strings.ToLower(x), -1), reNat.FindAllString(strings.ToLower(y), -1)
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
		if i < len(na) && i < len(nb) {
			p, q := atoi(na[i]), atoi(nb[i])
			if p != q {
				return p < q
			}
		}
	}
	return len(a) < len(b)
}

func uniq(l []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, x := range l {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

func (a *App) hFiles(w http.ResponseWriter, r *http.Request) {
	a.purgeSamples(true)
	cm, busy, bl := a.copied(), a.busySrc(), a.extractBlocked()
	out := []M{}
	shows := map[string]bool{}
	for _, rel := range walkFiles(a.source) {
		name := path.Base(rel)
		if !videoExt[lowerExt(name)] || reSample.MatchString(name) || hiddenRel(rel) || exBlocked(rel, bl) {
			continue
		}
		st, err := os.Stat(a.abs(rel))
		if err != nil {
			continue
		}
		cat := a.guessCategory(rel)
		tgt := ""
		if cat != "" {
			tgt = a.targetFor(rel, cat)
		}
		si := a.seriesInfo(rel)
		if si != nil && tgt != "" {
			shows[showDir(tgt)] = true
		}
		var series any
		if si != nil {
			series = si
		}
		cp := a.isCopied(cm, rel)
		var dup any
		if !cp && !busy[rel] {
			sk, src := 0, rel
			if tgt != "" {
				sk, src = 1, tgt
			}
			if m := a.libMatch(src, st.Size(), sk); m != nil {
				dup = m
			}
		}
		out = append(out, M{"path": rel, "size": st.Size(), "category": cat, "target": tgt, "series": series,
			"copied": cp, "busy": busy[rel], "dup": dup})
	}
	ts, _ := os.Stat(a.target)
	js(w, 200, M{"library": a.libraryEps(shows), "files": out, "categories": categories, "source": a.source, "target": a.target,
		"target_mounted": ts != nil && ts.IsDir(), "extracting": a.exSummary(), "dupidx": a.libReady()})
}

func (a *App) hRulesSet(w http.ResponseWriter, r *http.Request) {
	d := body(r)
	detected := strings.TrimSpace(str(d["detected"]))
	name := safe(pretty(strings.TrimSpace(str(d["name"]))))
	year := yearOf(d["year"])
	cat := str(d["category"])
	if !inCats(cat) {
		cat = ""
	}
	switch {
	case detected == "" || skey(detected) == "":
		js(w, 400, M{"error": "Erkannter Name fehlt."})
		return
	case name == "":
		js(w, 400, M{"error": "Bitte einen Namen eingeben."})
		return
	case year != "" && !reYEAR.full(year):
		js(w, 400, M{"error": "Jahr ungültig."})
		return
	}
	k := skey(detected)
	rules := []Rule{}
	for _, x := range a.getRules() {
		if x.Key != k {
			rules = append(rules, x)
		}
	}
	rules = append(rules, Rule{k, detected, name, year, idNorm(str(d["tmdb"])), cat})
	a.setRules(rules)
	js(w, 200, M{"ok": true, "rules": rules})
}

func (a *App) hSeriesPreview(w http.ResponseWriter, r *http.Request) {
	d := body(r)
	name := safe(pretty(strings.TrimSpace(str(d["name"]))))
	year := yearOf(d["year"])
	tmdb := idNorm(str(d["tmdb"]))
	cat := str(d["category"])
	if !inCats(cat) {
		cat = "Serien"
	}
	out := []string{}
	l, _ := d["samples"].([]any)
	for i, x := range l {
		if i >= 500 {
			break
		}
		e, _ := x.(M)
		p := str(e["path"])
		ep := parseEp(p, cat == "Anime", true, false, nil)
		if ep == nil || name == "" {
			out = append(out, "")
			continue
		}
		show := name
		if year != "" {
			show = name + " (" + year + ")"
		}
		es := fmt.Sprintf("S%02dE%02d", ep.season, ep.eps[0])
		for _, n := range ep.eps[1:] {
			es += fmt.Sprintf("-E%02d", n)
		}
		out = append(out, path.Join(cat, show+idTag(tmdb), fmt.Sprintf("Season %02d", ep.season), show+" "+es+lowerExt(p)))
	}
	js(w, 200, M{"targets": out})
}

func (a *App) mgBuild(d M) (map[string]Group, []string, error) {
	order := uniq(strs(d["items"], 5000))
	rels := append([]string{}, order...)
	sort.SliceStable(rels, func(i, j int) bool { return natLess(rels[i], rels[j]) })
	for _, r := range rels {
		if _, err := a.safeSource(r); err != nil {
			return nil, nil, err
		}
	}
	name := safe(pretty(strings.TrimSpace(str(d["name"]))))
	year := yearOf(d["year"])
	tmdb := idNorm(str(d["tmdb"]))
	cat := str(d["category"])
	switch {
	case len(rels) == 0:
		return nil, nil, errors.New("Keine Dateien ausgewählt.")
	case name == "":
		return nil, nil, errors.New("Bitte einen Namen eingeben.")
	case cat != "Serien" && cat != "Anime" && cat != "Dokus":
		return nil, nil, errors.New("Kategorie muss Serien, Anime oder Dokus sein.")
	case year != "" && !reYEAR.full(year):
		return nil, nil, errors.New("Jahr ungültig.")
	}
	season := 1
	if v := d["season"]; v != nil && v != "" && v != float64(0) && v != false {
		n, ok := toInt(v)
		if !ok {
			return nil, nil, errors.New("Staffel ungültig.")
		}
		season = max(0, min(99, n))
	}
	start := 1
	if v := d["start"]; v != nil && v != "" {
		n, ok := toInt(v)
		if !ok {
			return nil, nil, errors.New("Startnummer ungültig.")
		}
		start = max(0, n)
	}
	eps := map[string]int{}
	if str(d["mode"]) == "renumber" {
		for i, r := range rels {
			eps[r] = start + i
		}
	} else {
		mx, has := start-1, false
		for _, r := range rels {
			if e := parseEp(r, false, true, false, nil); e != nil && len(e.eps) > 0 {
				eps[r] = e.eps[0]
			}
		}
		for _, v := range eps {
			if !has || v > mx {
				mx, has = v, true
			}
		}
		for _, r := range rels {
			if _, ok := eps[r]; !ok {
				mx++
				eps[r] = mx
			}
		}
	}
	out := map[string]Group{}
	for _, r := range rels {
		out[r] = Group{Name: name, Year: year, TMDB: tmdb, Cat: cat, Season: season, Ep: eps[r]}
	}
	return out, order, nil
}

func (a *App) hGroupsPreview(w http.ResponseWriter, r *http.Request) {
	d := body(r)
	mg, order, err := a.mgBuild(d)
	if err != nil {
		js(w, 400, M{"error": err.Error()})
		return
	}
	t, e := []string{}, []int{}
	for _, rel := range order {
		t = append(t, mgPath(rel, str(d["category"]), mg[rel]))
		e = append(e, mg[rel].Ep)
	}
	js(w, 200, M{"targets": t, "eps": e})
}

func (a *App) hPathsSet(w http.ResponseWriter, r *http.Request) {
	d := body(r)
	rels := strs(d["items"], 5000)
	paths := map[string]PathOv{}
	for k, v := range a.getPaths() {
		paths[k] = v
	}
	for _, rel := range rels {
		if _, err := a.safeSource(rel); err != nil {
			js(w, 400, M{"error": err.Error()})
			return
		}
	}
	if f := strings.TrimSpace(str(d["folder"])); f != "" {
		if len(rels) > 0 {
			parts, n, _, err := a.folderTargets(rels, f)
			if err != nil {
				js(w, 400, M{"error": err.Error()})
				return
			}
			for _, rel := range rels {
				paths[rel] = PathOv{Folder: strings.Join(parts, "/"), N: n, Cat: parts[0]}
			}
		}
	} else {
		for _, rel := range rels {
			delete(paths, rel)
		}
	}
	a.setPaths(paths)
	js(w, 200, M{"ok": true})
}

func (a *App) hStart(w http.ResponseWriter, r *http.Request) {
	if st, err := os.Stat(a.target); err != nil || !st.IsDir() {
		js(w, 400, M{"error": fmt.Sprintf("Ziel %s nicht erreichbar (NFS gemountet?)", a.target)})
		return
	}
	l, _ := body(r)["items"].([]any)
	var items []item
	for _, x := range l {
		e, _ := x.(M)
		p, c := str(e["path"]), str(e["category"])
		if !inCats(c) {
			js(w, 400, M{"error": "Kategorie fehlt: " + p})
			return
		}
		if _, err := a.safeSource(p); err != nil {
			js(w, 400, M{"error": err.Error()})
			return
		}
		items = append(items, item{p, a.targetFor(p, c)})
	}
	if len(items) == 0 {
		js(w, 400, M{"error": "Nichts ausgewählt."})
		return
	}
	n, waits := a.enqueue(items, "manuell")
	if n == 0 {
		js(w, 409, M{"error": "Diese Dateien laufen schon oder warten bereits in der Warteschlange."})
		return
	}
	js(w, 200, M{"ok": true, "queued": waits, "n": n, "position": len(a.waitingBatches())})
}

func (a *App) hFilesDelete(w http.ResponseWriter, r *http.Request) {
	rels := uniq(strs(body(r)["items"], 5000))
	busy := a.busySrc()
	gone, skipped, errs := map[string]bool{}, 0, []string{}
	for _, rel := range rels {
		p, err := a.safeSource(rel)
		if err == nil && !videoExt[lowerExt(rel)] {
			err = fmt.Errorf("Keine Videodatei: %s", rel)
		}
		if err != nil {
			errs = append(errs, rel+": "+err.Error())
			continue
		}
		if busy[rel] {
			skipped++
			continue
		}
		if err := os.Remove(p); err != nil {
			errs = append(errs, rel+": "+err.Error())
			continue
		}
		gone[rel] = true
		fmt.Fprintln(os.Stderr, "Quelldatei gelöscht:", rel)
	}
	if len(gone) > 0 {
		np := map[string]PathOv{}
		for k, v := range a.getPaths() {
			if !gone[k] {
				np[k] = v
			}
		}
		a.setPaths(np)
		ng := map[string]Group{}
		for k, v := range a.getGroups() {
			if !gone[k] {
				ng[k] = v
			}
		}
		a.setGroups(ng)
		cm := a.copied()
		for k := range gone {
			delete(cm, k)
		}
		a.save("copied.json", cm)
	}
	js(w, 200, M{"ok": true, "deleted": len(gone), "skipped": skipped, "errors": errs})
}

func (a *App) hStatus(w http.ResponseWriter, r *http.Request) {
	counts := M{}
	a.mu.Lock()
	j := a.job
	for _, q := range j.Queue {
		n, _ := counts[q.State].(int)
		counts[q.State] = n + 1
	}
	a.mu.Unlock()
	ts := func(t time.Time) float64 {
		if t.IsZero() {
			return 0
		}
		return float64(t.UnixNano()) / 1e9
	}
	el := 0.0
	if !j.Started.IsZero() {
		end := j.Ended
		if j.Running || end.IsZero() {
			end = time.Now()
		}
		el = end.Sub(j.Started).Seconds()
	}
	errs := j.Errors
	if errs == nil {
		errs = []string{}
	}
	a.qmu.Lock()
	nb := len(a.pending)
	a.qmu.Unlock()
	waiting := []M{}
	for _, b := range a.waitingBatches() {
		waiting = append(waiting, M{"id": b.ID, "source": b.Source, "files": len(b.Items), "size": b.Size})
	}
	c := a.cfg()
	next := ""
	if c.Enabled {
		if cr, err := NewCron(c.Cron); err != nil {
			next = "ungültiger Ausdruck"
		} else if t, ok := cr.NextAfter(time.Now()); ok {
			next = fmtDt(t)
		}
	}
	js(w, 200, M{"running": j.Running, "cancel": j.Cancel, "total": j.Total, "done": j.Done, "files_total": j.FilesTotal,
		"files_done": j.FilesDone, "current": j.Current, "current_size": j.CurrentSize, "current_done": j.CurrentDone, "speed": j.Speed,
		"errors": errs, "finished": j.Finished, "started": ts(j.Started), "ended": ts(j.Ended), "source": j.Source, "counts": counts,
		"elapsed": el, "schedule": M{"enabled": c.Enabled, "cron": c.Cron, "next": next, "last": c.LastResult},
		"extract": a.exSummary(), "disk": a.diskInfo(), "batches": nb, "waiting": waiting})
}

func (a *App) hJobQueue(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	q := make([]queueItem, len(a.job.Queue))
	for i, x := range a.job.Queue {
		q[i] = *x
	}
	a.mu.Unlock()
	var cur, bad, wait, done []queueItem
	for _, x := range q {
		switch x.State {
		case "kopiert":
			cur = append(cur, x)
		case "fehler", "abgebrochen":
			bad = append(bad, x)
		case "wartet":
			wait = append(wait, x)
		case "fertig", "vorhanden":
			done = append(done, x)
		}
	}
	out := append(append([]queueItem{}, cur...), bad...)
	hidden := 0
	if len(wait) > 100 {
		hidden += len(wait) - 100
		wait = wait[:100]
	}
	out = append(out, wait...)
	if len(done) > 100 {
		hidden += len(done) - 100
		done = done[len(done)-100:]
	}
	for i := len(done) - 1; i >= 0; i-- {
		out = append(out, done[i])
	}
	js(w, 200, M{"items": out, "hidden": hidden})
}

func (a *App) hLeftoversDelete(w http.ResponseWriter, r *http.Request) {
	if a.jobRunning() || a.busy() {
		js(w, 409, M{"error": "Während des Kopierens nicht möglich."})
		return
	}
	d := body(r)
	force, all := d["force"] == true, d["all"] == true
	var names []string
	if all {
		for _, n := range a.topEntries() {
			if force || a.entryInfo(n)["locked"] == "" {
				names = append(names, n)
			}
		}
	} else {
		names = strs(d["paths"], 100000)
	}
	deleted, errs := 0, []string{}
	for _, n := range names {
		if err := a.deleteEntry(n, force); err != nil {
			errs = append(errs, err.Error())
		} else {
			deleted++
		}
	}
	if all {
		deleted += a.removeEmptyDirs()
	}
	js(w, 200, M{"deleted": deleted, "errors": errs})
}

func (a *App) settingsPayload() M {
	c := a.cfg()
	var m M
	b, _ := json.Marshal(c)
	json.Unmarshal(b, &m)
	delete(m, "tg_token")
	delete(m, "tg_unclear_sig")
	delete(m, "jf_key")
	nx, err := nextRuns(c.Cron, 3)
	es := ""
	if err != nil {
		nx, es = []string{}, err.Error()
	}
	m["tg_token_set"], m["jf_key_set"] = c.TgToken != "", c.JfKey != ""
	m["next"], m["error"] = nx, es
	m["tz"], m["persist"] = time.Now().Format("MST"), a.persist
	_, ffErr := lookPath("ffprobe")
	m["info"] = M{"source": a.source, "target": a.target, "data": a.dir, "min_age": int(a.minAge.Seconds()), "auth": a.pass != "",
		"ffprobe": ffErr == nil, "sevenz": sevenz() != "", "watch": 60, "go": runtime.Version()}
	return m
}

func (a *App) hSettingsSet(w http.ResponseWriter, r *http.Request) {
	d := body(r)
	if _, err := NewCron(str(d["cron"])); err != nil {
		js(w, 400, M{"error": err.Error()})
		return
	}
	old := a.cfg()
	tok := strings.TrimSpace(str(d["tg_token"]))
	if tok == "" {
		tok = old.TgToken
	}
	chat := strings.TrimSpace(str(d["tg_chat"]))
	if d["tg_enabled"] == true && (tok == "" || chat == "") {
		js(w, 400, M{"error": "Telegram: Bot-Token und Chat-ID eintragen."})
		return
	}
	url := strings.TrimRight(strings.TrimSpace(str(d["public_url"])), "/")
	if url != "" && !reHTTP.MatchString(url) {
		js(w, 400, M{"error": "Web-UI-Adresse muss mit http:// oder https:// beginnen."})
		return
	}
	jurl := strings.TrimRight(strings.TrimSpace(str(d["jf_url"])), "/")
	jkey := strings.TrimSpace(str(d["jf_key"]))
	if jkey == "" {
		jkey = old.JfKey
	}
	if d["jf_scan"] == true && !(reHTTP.MatchString(jurl) && jkey != "") {
		js(w, 400, M{"error": "Jellyfin: Adresse (http://…) und API-Key eintragen."})
		return
	}
	jroot := strings.TrimRight(strings.TrimSpace(str(d["jf_root"])), "/")
	flag := func(k string, def bool) bool {
		if v, ok := d[k]; ok {
			b, _ := v.(bool)
			return b
		}
		return def
	}
	a.saveCfg(func(c *Config) {
		c.JfScan, c.JfURL, c.JfKey, c.JfRoot = d["jf_scan"] == true, jurl, jkey, jroot
		c.JfMode = "partial"
		if str(d["jf_mode"]) == "full" {
			c.JfMode = "full"
		}
		c.Enabled, c.Cron = d["enabled"] == true, strings.Join(strings.Fields(str(d["cron"])), " ")
		c.MoveAfter, c.CleanupAfter = d["move_after"] == true, d["cleanup_after"] == true
		c.TgEnabled, c.TgToken, c.TgChat = d["tg_enabled"] == true, tok, chat
		c.TgStart, c.TgDone, c.TgUnclear, c.PublicURL = d["tg_start"] == true, d["tg_done"] == true, d["tg_unclear"] == true, url
		c.TgNew, c.TgProgress = flag("tg_new", old.TgNew), flag("tg_progress", old.TgProgress)
		c.TgErrors, c.TgSilent, c.Jellyfin = flag("tg_errors", old.TgErrors), flag("tg_silent", old.TgSilent), flag("jellyfin", old.Jellyfin)
	})
	js(w, 200, a.settingsPayload())
}

var _ = strconv.Itoa
