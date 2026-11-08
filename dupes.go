package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const dupMinSize = 50 * 1024 * 1024

var (
	analysisExt = map[string]bool{".mkv": true, ".mp4": true, ".avi": true, ".m4v": true, ".mov": true, ".wmv": true, ".ts": true, ".mpg": true, ".mpeg": true}
	sidecarExt  = map[string]bool{".srt": true, ".ass": true, ".sub": true, ".idx": true, ".nfo": true}
	reNums      = regexp.MustCompile(`\d+`)
	libTTL      = time.Duration(atoi(env("LIB_INDEX_TTL", "300"))) * time.Second
	codecNames  = map[string]string{"h264": "H.264", "hevc": "H.265", "av1": "AV1", "vp9": "VP9", "vp8": "VP8", "mpeg4": "MPEG-4",
		"msmpeg4v3": "MPEG-4", "msmpeg4v2": "MPEG-4", "mpeg2video": "MPEG-2", "mpeg1video": "MPEG-1",
		"vc1": "VC-1", "wmv1": "WMV", "wmv2": "WMV", "wmv3": "WMV", "theora": "Theora", "prores": "ProRes"}
)

// ---------- difflib.SequenceMatcher.ratio() ----------

func lcs(a, b []rune, alo, ahi, blo, bhi int) (int, int, int) {
	bi, bj, bk := alo, blo, 0
	prev := make([]int, len(b)+1)
	for i := alo; i < ahi; i++ {
		cur := make([]int, len(b)+1)
		for j := blo; j < bhi; j++ {
			if a[i] == b[j] {
				k := prev[j] + 1
				cur[j+1] = k
				if k > bk {
					bi, bj, bk = i-k+1, j-k+1, k
				}
			}
		}
		prev = cur
	}
	return bi, bj, bk
}

func matchTotal(a, b []rune, alo, ahi, blo, bhi int) int {
	if alo >= ahi || blo >= bhi {
		return 0
	}
	i, j, k := lcs(a, b, alo, ahi, blo, bhi)
	if k == 0 {
		return 0
	}
	return k + matchTotal(a, b, alo, i, blo, j) + matchTotal(a, b, i+k, ahi, j+k, bhi)
}

func ratio(x, y string) float64 {
	a, b := []rune(x), []rune(y)
	if len(a)+len(b) == 0 {
		return 1
	}
	return 2 * float64(matchTotal(a, b, 0, len(a), 0, len(b))) / float64(len(a)+len(b))
}

func similar(a, b string) bool {
	a, b = cmpKey(a), cmpKey(b)
	if a == b {
		return true
	}
	if strings.Join(reNums.FindAllString(a, -1), ",") != strings.Join(reNums.FindAllString(b, -1), ",") ||
		min(len([]rune(a)), len([]rune(b))) < 5 {
		return false
	}
	return ratio(a, b) >= .88
}

func yearsOK(y1, y2 string, exact bool) bool {
	if y1 == "" || y2 == "" {
		return !exact
	}
	a, b := atoi(y1), atoi(y2)
	if exact {
		return a == b
	}
	return a-b <= 1 && b-a <= 1
}

func compat(a, b mediaID) string {
	if a.typ != b.typ || a.extra != b.extra {
		return ""
	}
	if a.key == b.key && a.year == b.year {
		return "exakt"
	}
	if a.key == b.key && (a.year == "" || b.year == "") {
		return "ähnlich"
	}
	if yearsOK(a.year, b.year, false) && similar(a.key, b.key) {
		return "ähnlich"
	}
	return ""
}

// ---------- Bibliothek durchlaufen ----------

func walkSkip(root, skip string) []string {
	var out []string
	var skipInfo os.FileInfo
	if skip != "" {
		skipInfo, _ = os.Stat(skip)
	}
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && p != root {
			if strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			if skipInfo != nil {
				if st, e := os.Stat(p); e == nil && os.SameFile(st, skipInfo) {
					return filepath.SkipDir
				}
			}
		}
		if d.Type().IsRegular() {
			if r, e := filepath.Rel(root, p); e == nil {
				out = append(out, filepath.ToSlash(r))
			}
		}
		return nil
	})
	return out
}

type libFile struct {
	rel     string
	size, m int64
}

func (a *App) walkLibrary() []libFile {
	var out []libFile
	for _, rel := range walkSkip(a.target, a.source) {
		n := path.Base(rel)
		if !analysisExt[lowerExt(n)] || strings.HasPrefix(n, ".") || reSample.MatchString(n) {
			continue
		}
		if st, err := os.Stat(filepath.Join(a.target, filepath.FromSlash(rel))); err == nil {
			out = append(out, libFile{rel, st.Size(), st.ModTime().Unix()})
		}
	}
	return out
}

// ---------- Analyse-Cache (analysis_*.json) ----------

type AEntry struct {
	S       int64  `json:"s"`
	M       int64  `json:"m"`
	Codec   string `json:"codec"`
	Profile string `json:"profile"`
	W       int    `json:"w"`
	H       int    `json:"h"`
	Br      int64  `json:"br"`
	Dur     int64  `json:"dur"`
	Pix     string `json:"pix"`
	Bad     bool   `json:"bad"`
}

type AFile struct {
	TS    string            `json:"ts"`
	Files map[string]AEntry `json:"files"`
}

func (a *App) loadAnalysis(scope string) AFile {
	f := AFile{Files: map[string]AEntry{}}
	a.load("analysis_"+scope+".json", &f)
	if f.Files == nil {
		f.Files = map[string]AEntry{}
	}
	return f
}

// ---------- Duplikat-Gruppen ----------

type dEnt struct {
	rel     string
	size, m int64
	id      mediaID
	codec   string
	w, h    int
}

type DupFile struct {
	Path  string `json:"path"`
	Size  int64  `json:"size"`
	M     int64  `json:"m"`
	Codec string `json:"codec"`
	W     int    `json:"w"`
	H     int    `json:"h"`
}

type DupGroup struct {
	Label   string    `json:"label"`
	Type    string    `json:"type"`
	Level   string    `json:"level"`
	Reasons []string  `json:"reasons"`
	Keep    string    `json:"keep"`
	Waste   int64     `json:"waste"`
	Files   []DupFile `json:"files"`
}

type DupFileJSON struct {
	TS     string     `json:"ts"`
	Files  int        `json:"files"`
	Groups []DupGroup `json:"groups"`
}

func findGroups(ents []dEnt) ([][]int, []map[string]bool) {
	n := len(ents)
	par := make([]int, n)
	why := make([]map[string]bool, n)
	for i := range par {
		par[i], why[i] = i, map[string]bool{}
	}
	var find func(int) int
	find = func(i int) int {
		for par[i] != i {
			par[i] = par[par[i]]
			i = par[i]
		}
		return i
	}
	union := func(i, j int, reason string) {
		x, y := find(i), find(j)
		if x != y {
			par[y] = x
			for k := range why[y] {
				why[x][k] = true
			}
		}
		why[x][reason] = true
	}
	ex := map[string][]int{}
	var exOrder []string
	for i, e := range ents {
		k := e.id.id4()
		if _, ok := ex[k]; !ok {
			exOrder = append(exOrder, k)
		}
		ex[k] = append(ex[k], i)
	}
	for _, k := range exOrder {
		ids := ex[k]
		for _, j := range ids[1:] {
			union(ids[0], j, "Gleicher Titel")
		}
	}
	bySize := map[string][]int{}
	var szOrder []string
	for i, e := range ents {
		if e.size >= dupMinSize {
			k := fmt.Sprintf("%d|%s|%s", e.size, e.id.typ, e.id.extra)
			if _, ok := bySize[k]; !ok {
				szOrder = append(szOrder, k)
			}
			bySize[k] = append(bySize[k], i)
		}
	}
	for _, k := range szOrder {
		ids := bySize[k]
		for _, j := range ids[1:] {
			union(ids[0], j, "Gleiche Dateigröße")
		}
	}
	repSet := map[int]bool{}
	for i := 0; i < n; i++ {
		repSet[find(i)] = true
	}
	reps := []int{}
	for r := range repSet {
		reps = append(reps, r)
	}
	sort.Ints(reps)
	var ms []int
	byYear, epsB := map[string][]int{}, map[string][]int{}
	var yOrder, eOrder []string
	for _, r := range reps {
		id := ents[r].id
		if id.typ == "m" {
			ms = append(ms, r)
			if _, ok := byYear[id.year]; !ok {
				yOrder = append(yOrder, id.year)
			}
			byYear[id.year] = append(byYear[id.year], r)
		} else {
			if _, ok := epsB[id.extra]; !ok {
				eOrder = append(eOrder, id.extra)
			}
			epsB[id.extra] = append(epsB[id.extra], r)
		}
	}
	for _, y := range yOrder {
		lst := byYear[y]
		cand := ms
		if y != "" {
			cand = append(append(append(append([]int{}, lst...), byYear[""]...), byYear[strconv.Itoa(atoi(y)-1)]...), byYear[strconv.Itoa(atoi(y)+1)]...)
		}
		for _, r := range lst {
			for _, q := range cand {
				if q != r && find(q) != find(r) && compat(ents[r].id, ents[q].id) != "" {
					union(r, q, "Ähnlicher Titel")
				}
			}
		}
	}
	for _, x := range eOrder {
		lst := epsB[x]
		for i := range lst {
			for j := i + 1; j < len(lst); j++ {
				if find(lst[i]) != find(lst[j]) && compat(ents[lst[i]].id, ents[lst[j]].id) != "" {
					union(lst[i], lst[j], "Ähnlicher Titel")
				}
			}
		}
	}
	groups := map[int][]int{}
	var gOrder []int
	for i := 0; i < n; i++ {
		r := find(i)
		if _, ok := groups[r]; !ok {
			gOrder = append(gOrder, r)
		}
		groups[r] = append(groups[r], i)
	}
	var idx [][]int
	var whys []map[string]bool
	for _, r := range gOrder {
		if len(groups[r]) > 1 {
			idx = append(idx, groups[r])
			whys = append(whys, why[r])
		}
	}
	return idx, whys
}

type dupState struct {
	mu              sync.Mutex
	Running, Cancel bool
	Total, Done     int
	Current, Error  string
}

func (a *App) runDupes() {
	d := a.dp
	set := func(f func()) { d.mu.Lock(); f(); d.mu.Unlock() }
	set(func() { d.Running, d.Cancel, d.Total, d.Done, d.Current, d.Error = true, false, 0, 0, "", "" })
	defer set(func() { d.Running, d.Current = false, "" })
	fail := func(err error) { set(func() { d.Error = err.Error() }) }
	if st, err := os.Stat(a.target); err != nil || !st.IsDir() {
		fail(fmt.Errorf("%s nicht erreichbar", a.target))
		return
	}
	cache := a.loadAnalysis("target").Files
	var files []libFile
	for _, f := range a.walkLibrary() {
		files = append(files, f)
		if len(files)%25 == 0 {
			set(func() { d.Total, d.Current = len(files), path.Dir(f.rel) })
		}
		d.mu.Lock()
		c := d.Cancel
		d.mu.Unlock()
		if c {
			fail(errors.New("Abgebrochen"))
			return
		}
	}
	set(func() { d.Total, d.Done, d.Current = len(files), len(files), "Vergleiche …" })
	ents := make([]dEnt, len(files))
	for i, f := range files {
		e := dEnt{rel: f.rel, size: f.size, m: f.m, id: a.mediaID(f.rel, 1)}
		if c, ok := cache[f.rel]; ok && c.S == f.size {
			e.codec, e.w, e.h = c.Codec, c.W, c.H
		}
		ents[i] = e
	}
	idx, whys := findGroups(ents)
	groups := []DupGroup{}
	for gi, ids := range idx {
		fs := make([]dEnt, len(ids))
		for i, k := range ids {
			fs[i] = ents[k]
		}
		sort.SliceStable(fs, func(i, j int) bool {
			if fs[i].h != fs[j].h {
				return fs[i].h > fs[j].h
			}
			if fs[i].size != fs[j].size {
				return fs[i].size > fs[j].size
			}
			return fs[i].rel < fs[j].rel
		})
		why := whys[gi]
		exact := why["Gleicher Titel"] || why["Gleiche Dateigröße"]
		reasons := []string{}
		for _, r := range []string{"Gleicher Titel", "Gleiche Dateigröße", "Ähnlicher Titel"} {
			if why[r] {
				reasons = append(reasons, r)
			}
		}
		cnt, order := map[string]int{}, []string{}
		var total int64
		for _, e := range fs {
			if cnt[e.id.display] == 0 {
				order = append(order, e.id.display)
			}
			cnt[e.id.display]++
			total += e.size
		}
		label := order[0]
		for _, o := range order {
			if cnt[o] > cnt[label] {
				label = o
			}
		}
		typ, lvl := "Film", "ähnlich"
		if fs[0].id.typ == "e" {
			typ = "Folge"
		}
		if exact {
			lvl = "exakt"
		}
		g := DupGroup{Label: label, Type: typ, Level: lvl, Reasons: reasons, Keep: fs[0].rel, Waste: total - fs[0].size}
		for _, e := range fs {
			g.Files = append(g.Files, DupFile{e.rel, e.size, e.m, e.codec, e.w, e.h})
		}
		groups = append(groups, g)
	}
	sort.SliceStable(groups, func(i, j int) bool {
		if (groups[i].Level != "exakt") != (groups[j].Level != "exakt") {
			return groups[i].Level == "exakt"
		}
		return groups[i].Waste > groups[j].Waste
	})
	a.save("dupes.json", DupFileJSON{time.Now().Format("2006-01-02 15:04:05"), len(files), groups})
}

// ---------- Bibliotheks-Index für den Abgleich ----------

type libEnt struct {
	rel  string
	size int64
	id   mediaID
}

type libIndex struct {
	mu   sync.Mutex
	ts   time.Time
	busy bool
	data *libData
	memo map[string]M
}

type libData struct {
	all    []libEnt
	bySize map[int64][]libEnt
	m, e   map[string][]libEnt
}

func (a *App) buildLibIndex() {
	li := a.lib
	defer func() { li.mu.Lock(); li.busy = false; li.mu.Unlock() }()
	if st, err := os.Stat(a.target); err != nil || !st.IsDir() {
		return
	}
	d := &libData{bySize: map[int64][]libEnt{}, m: map[string][]libEnt{}, e: map[string][]libEnt{}}
	for _, f := range a.walkLibrary() {
		e := libEnt{f.rel, f.size, a.mediaID(f.rel, 1)}
		d.all = append(d.all, e)
		if e.size >= dupMinSize {
			d.bySize[e.size] = append(d.bySize[e.size], e)
		}
		if e.id.typ == "e" {
			d.e[e.id.extra] = append(d.e[e.id.extra], e)
		} else {
			d.m[e.id.year] = append(d.m[e.id.year], e)
		}
	}
	li.mu.Lock()
	li.data, li.ts, li.memo = d, time.Now(), map[string]M{}
	li.mu.Unlock()
}

func (a *App) libInvalidate() { a.lib.mu.Lock(); a.lib.ts = time.Time{}; a.lib.mu.Unlock() }

func (a *App) libIdx() *libData {
	li := a.lib
	li.mu.Lock()
	defer li.mu.Unlock()
	if time.Since(li.ts) > libTTL && !li.busy {
		li.busy = true
		go a.buildLibIndex()
	}
	return li.data
}

// libMatch: nil = Index noch nicht bereit.
func (a *App) libMatch(rel string, size int64, skip int) M {
	idx := a.libIdx()
	if idx == nil {
		return nil
	}
	mid := a.mediaID(rel, skip)
	mk := fmt.Sprintf("%s|%d", mid.id4(), size)
	a.lib.mu.Lock()
	if v, ok := a.lib.memo[mk]; ok {
		a.lib.mu.Unlock()
		return v
	}
	a.lib.mu.Unlock()
	found := map[string]M{}
	add := func(e libEnt, level, why string) {
		cur, ok := found[e.rel]
		if !ok || (level == "exakt" && cur["level"] != "exakt") {
			found[e.rel] = M{"path": e.rel, "size": e.size, "level": level, "why": why}
		}
	}
	if size >= dupMinSize {
		for _, e := range idx.bySize[size] {
			if e.id.typ == mid.typ && e.id.extra == mid.extra {
				add(e, "exakt", "Gleiche Dateigröße")
			}
		}
	}
	var pool []libEnt
	if mid.typ == "e" {
		pool = idx.e[mid.extra]
	} else if mid.year == "" {
		pool = idx.all
	} else {
		y := atoi(mid.year)
		for _, yy := range []string{mid.year, "", strconv.Itoa(y - 1), strconv.Itoa(y + 1)} {
			pool = append(pool, idx.m[yy]...)
		}
	}
	for _, e := range pool {
		if e.id.typ != mid.typ {
			continue
		}
		if c := compat(mid, e.id); c != "" {
			why := "Ähnlicher Titel"
			if c == "exakt" {
				why = "Gleicher Titel"
			}
			add(e, c, why)
		}
	}
	res := []M{}
	for _, v := range found {
		res = append(res, v)
	}
	sort.Slice(res, func(i, j int) bool {
		ei, ej := res[i]["level"] == "exakt", res[j]["level"] == "exakt"
		if ei != ej {
			return ei
		}
		return res[i]["path"].(string) < res[j]["path"].(string)
	})
	level := ""
	if len(res) > 0 {
		level = res[0]["level"].(string)
	}
	items := res
	if len(items) > 3 {
		items = items[:3]
	}
	out := M{"level": level, "n": len(res), "items": items}
	a.lib.mu.Lock()
	a.lib.memo[mk] = out
	a.lib.mu.Unlock()
	return out
}

// ---------- Analyse (ffprobe) ----------

type anState struct {
	mu              sync.Mutex
	Running, Cancel bool
	Scope           string
	Total, Done     int
	Current, Error  string
}

func (a *App) analysisRoot(scope string) string {
	if scope == "target" {
		return a.target
	}
	return a.source
}

func probeFile(p string) (map[string]any, map[string]any) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
		"stream=codec_name,profile,width,height,pix_fmt:format=duration,bit_rate", "-of", "json", p).Output()
	var j struct {
		Streams []map[string]any `json:"streams"`
		Format  map[string]any   `json:"format"`
	}
	if err == nil {
		json.Unmarshal(out, &j)
	}
	s := map[string]any{}
	if len(j.Streams) > 0 {
		s = j.Streams[0]
	}
	if j.Format == nil {
		j.Format = map[string]any{}
	}
	return s, j.Format
}

func fnum(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	}
	return 0
}

func makeEntry(size, mtime int64, s, f map[string]any) AEntry {
	raw := str(s["codec_name"])
	dur := fnum(f["duration"])
	br := int64(fnum(f["bit_rate"]))
	if br == 0 && dur > 0 {
		br = int64(float64(size) * 8 / dur)
	}
	codec := codecNames[raw]
	if codec == "" {
		codec = strings.ToUpper(raw)
		if raw == "" {
			codec = "Unbekannt"
		}
	}
	return AEntry{S: size, M: mtime, Codec: codec, Profile: str(s["profile"]), W: int(fnum(s["width"])), H: int(fnum(s["height"])),
		Br: br, Dur: int64(dur + .5), Pix: str(s["pix_fmt"]), Bad: raw == ""}
}

func (a *App) runAnalysis(scope string) {
	an := a.an
	set := func(f func()) { an.mu.Lock(); f(); an.mu.Unlock() }
	root := a.analysisRoot(scope)
	set(func() {
		an.Running, an.Cancel, an.Scope, an.Total, an.Done, an.Current, an.Error = true, false, scope, 0, 0, "", ""
	})
	defer set(func() { an.Running, an.Current = false, "" })
	if st, err := os.Stat(root); err != nil || !st.IsDir() {
		set(func() { an.Error = root + " nicht erreichbar" })
		return
	}
	cache := a.loadAnalysis(scope).Files
	skip := ""
	if scope == "target" {
		skip = a.source
	}
	newF := map[string]AEntry{}
	type todo struct {
		p, rel  string
		size, m int64
	}
	var jobs []todo
	found := 0
	for _, rel := range walkSkip(root, skip) {
		n := path.Base(rel)
		if !analysisExt[lowerExt(n)] || strings.HasPrefix(n, ".") {
			continue
		}
		p := filepath.Join(root, filepath.FromSlash(rel))
		st, err := os.Stat(p)
		if err != nil {
			continue
		}
		found++
		if c, ok := cache[rel]; ok && c.S == st.Size() && c.M == st.ModTime().Unix() && !c.Bad {
			newF[rel] = c
		} else {
			jobs = append(jobs, todo{p, rel, st.Size(), st.ModTime().Unix()})
		}
	}
	set(func() { an.Total, an.Done = found, len(newF) })
	var wg sync.WaitGroup
	var rm sync.Mutex
	ch := make(chan todo)
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range ch {
				an.mu.Lock()
				c := an.Cancel
				if !c {
					an.Current = j.rel
				}
				an.mu.Unlock()
				if !c {
					s, f := probeFile(j.p)
					rm.Lock()
					newF[j.rel] = makeEntry(j.size, j.m, s, f)
					rm.Unlock()
				}
				set(func() { an.Done++ })
			}
		}()
	}
	for _, j := range jobs {
		ch <- j
	}
	close(ch)
	wg.Wait()
	a.save("analysis_"+scope+".json", AFile{time.Now().Format("2006-01-02 15:04:05"), newF})
}

// ---------- HTTP ----------

func (a *App) dupRoutes(get, post func(string, http.HandlerFunc)) {
	get("/api/dupes", func(w http.ResponseWriter, r *http.Request) {
		a.dp.mu.Lock()
		d := a.dp
		j := M{"running": d.Running, "cancel": d.Cancel, "total": d.Total, "done": d.Done, "current": d.Current, "error": d.Error}
		a.dp.mu.Unlock()
		var f DupFileJSON
		a.load("dupes.json", &f)
		if f.Groups == nil {
			f.Groups = []DupGroup{}
		}
		var waste int64
		for _, g := range f.Groups {
			waste += g.Waste
		}
		st, err := os.Stat(a.target)
		j["ts"], j["scanned"], j["groups"], j["root"], j["mounted"], j["waste"] = f.TS, f.Files, f.Groups, a.target, err == nil && st.IsDir(), waste
		js(w, 200, j)
	})
	post("/api/dupes/start", func(w http.ResponseWriter, r *http.Request) {
		if st, err := os.Stat(a.target); err != nil || !st.IsDir() {
			js(w, 400, M{"error": "Das Ziel ist nicht erreichbar. Ist das NFS-Share eingebunden?"})
			return
		}
		a.dp.mu.Lock()
		if a.dp.Running {
			a.dp.mu.Unlock()
			js(w, 409, M{"error": "Es läuft bereits ein Scan."})
			return
		}
		a.dp.Running = true
		a.dp.mu.Unlock()
		go a.runDupes()
		js(w, 200, M{"ok": true})
	})
	post("/api/dupes/cancel", func(w http.ResponseWriter, r *http.Request) {
		a.dp.mu.Lock()
		a.dp.Cancel = true
		a.dp.mu.Unlock()
		js(w, 200, M{"ok": true})
	})
	post("/api/dupes/delete", a.hDupesDelete)

	get("/api/analysis/status", func(w http.ResponseWriter, r *http.Request) {
		a.an.mu.Lock()
		an := a.an
		j := M{"running": an.Running, "cancel": an.Cancel, "scope": an.Scope, "total": an.Total, "done": an.Done, "current": an.Current, "error": an.Error}
		a.an.mu.Unlock()
		_, err := lookPath("ffprobe")
		j["ffprobe"], j["roots"] = err == nil, M{"target": a.target, "source": a.source}
		js(w, 200, j)
	})
	post("/api/analysis/start", func(w http.ResponseWriter, r *http.Request) {
		scope := str(body(r)["scope"])
		if scope == "" {
			scope = "target"
		}
		if scope != "target" && scope != "source" {
			js(w, 400, M{"error": "Ungültiger Bereich"})
			return
		}
		if _, err := lookPath("ffprobe"); err != nil {
			js(w, 400, M{"error": "ffprobe fehlt (Docker-Image mit ffmpeg neu bauen)"})
			return
		}
		a.an.mu.Lock()
		if a.an.Running {
			a.an.mu.Unlock()
			js(w, 409, M{"error": "Es läuft bereits eine Analyse."})
			return
		}
		a.an.Running = true
		a.an.mu.Unlock()
		go a.runAnalysis(scope)
		js(w, 200, M{"ok": true})
	})
	post("/api/analysis/cancel", func(w http.ResponseWriter, r *http.Request) {
		a.an.mu.Lock()
		a.an.Cancel = true
		a.an.mu.Unlock()
		js(w, 200, M{"ok": true})
	})
	get("/api/analysis/results", func(w http.ResponseWriter, r *http.Request) {
		scope := r.URL.Query().Get("scope")
		if scope == "" {
			scope = "target"
		}
		if scope != "target" && scope != "source" {
			js(w, 400, M{"error": "Ungültiger Bereich"})
			return
		}
		f := a.loadAnalysis(scope)
		keys := make([]string, 0, len(f.Files))
		for k := range f.Files {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		items := []M{}
		for _, k := range keys {
			b, _ := json.Marshal(f.Files[k])
			m := M{}
			json.Unmarshal(b, &m)
			m["path"] = k
			items = append(items, m)
		}
		js(w, 200, M{"ts": f.TS, "scope": scope, "root": a.analysisRoot(scope), "items": items})
	})
}

func (a *App) rmSidecars(p string) {
	es, err := os.ReadDir(filepath.Dir(p))
	if err != nil {
		return
	}
	stem := strings.TrimSuffix(filepath.Base(p), filepath.Ext(p))
	for _, e := range es {
		n := e.Name()
		s := strings.TrimSuffix(n, filepath.Ext(n))
		if e.Type().IsRegular() && sidecarExt[lowerExt(n)] && (s == stem || strings.HasPrefix(s, stem+".")) {
			os.Remove(filepath.Join(filepath.Dir(p), n))
		}
	}
}

func (a *App) rmEmptyParents(p string) {
	d := filepath.Dir(p)
	for d != a.target {
		rel, err := filepath.Rel(a.target, d)
		if err != nil || len(strings.Split(rel, string(filepath.Separator))) < 2 {
			break
		}
		if es, err := os.ReadDir(d); err != nil || len(es) > 0 {
			break
		}
		if os.Remove(d) != nil {
			break
		}
		d = filepath.Dir(d)
	}
}

func (a *App) hDupesDelete(w http.ResponseWriter, r *http.Request) {
	rels := uniq(strs(body(r)["items"], 5000))
	var df DupFileJSON
	a.load("dupes.json", &df)
	inScan := map[string]bool{}
	for _, g := range df.Groups {
		for _, f := range g.Files {
			inScan[f.Path] = true
		}
	}
	sel := map[string]bool{}
	for _, x := range rels {
		if inScan[x] {
			sel[x] = true
		}
	}
	kept := 0
	for _, g := range df.Groups {
		all := len(g.Files) > 0
		for _, f := range g.Files {
			all = all && sel[f.Path]
		}
		if all {
			k := g.Files[0].Path
			for _, f := range g.Files {
				if f.Path == g.Keep {
					k = g.Keep
				}
			}
			delete(sel, k)
			kept++
		}
	}
	root, _ := filepath.EvalSymlinks(a.target)
	gone := map[string]bool{}
	errs := []string{}
	var freed int64
	for _, rel := range rels {
		if !sel[rel] {
			continue
		}
		p := filepath.Join(a.target, filepath.FromSlash(rel))
		err := func() error {
			par, _ := filepath.EvalSymlinks(filepath.Dir(p))
			if par != root && !strings.HasPrefix(par, root+string(filepath.Separator)) {
				return errors.New("liegt außerhalb der Bibliothek")
			}
			if !analysisExt[lowerExt(p)] {
				return errors.New("keine Videodatei")
			}
			st, err := os.Stat(p)
			if err != nil || !st.Mode().IsRegular() {
				return errors.New("Datei nicht gefunden")
			}
			if err := os.Remove(p); err != nil {
				return err
			}
			freed += st.Size()
			return nil
		}()
		if err != nil {
			errs = append(errs, rel+": "+err.Error())
			continue
		}
		gone[rel] = true
		a.rmSidecars(p)
		a.rmEmptyParents(p)
		fmt.Fprintln(os.Stderr, "Aus Bibliothek gelöscht (Duplikat):", rel)
	}
	if len(gone) > 0 {
		out := []DupGroup{}
		for _, g := range df.Groups {
			var fs []DupFile
			for _, f := range g.Files {
				if !gone[f.Path] {
					fs = append(fs, f)
				}
			}
			if len(fs) < 2 {
				continue
			}
			hasKeep := false
			for _, f := range fs {
				hasKeep = hasKeep || f.Path == g.Keep
			}
			if !hasKeep {
				g.Keep = fs[0].Path
			}
			var total, keepSize int64
			for _, f := range fs {
				total += f.Size
				if f.Path == g.Keep {
					keepSize = f.Size
				}
			}
			g.Files, g.Waste = fs, total-keepSize
			out = append(out, g)
		}
		df.Groups = out
		a.save("dupes.json", df)
		af := a.loadAnalysis("target")
		ch := false
		for k := range gone {
			if _, ok := af.Files[k]; ok {
				delete(af.Files, k)
				ch = true
			}
		}
		if ch {
			a.save("analysis_target.json", af)
		}
		a.libInvalidate()
	}
	js(w, 200, M{"ok": true, "deleted": len(gone), "freed": freed, "kept": kept, "errors": errs})
}

func (a *App) libReady() bool { a.lib.mu.Lock(); defer a.lib.mu.Unlock(); return a.lib.data != nil }
