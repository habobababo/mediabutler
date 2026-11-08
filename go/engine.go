package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	videoExt   = map[string]bool{".mkv": true, ".mp4": true, ".avi": true, ".m4v": true, ".mov": true, ".wmv": true, ".ts": true, ".mpg": true, ".mpeg": true, ".srt": true, ".ass": true, ".sub": true}
	partialExt = map[string]bool{".part": true, ".crdownload": true, ".aria2": true, ".!ut": true, ".tmp": true, ".jdownloader": true, ".download": true}
	archiveExt = map[string]bool{".rar": true, ".zip": true, ".7z": true, ".tar": true, ".gz": true, ".iso": true}
	reSample   = regexp.MustCompile(`(?i)sample`)
	reArchPart = regexp.MustCompile(`^\.(r\d{2,3}|\d{3})$`)
	reSeasonT  = regexp.MustCompile(`^Season \d+$`)
	reEPS      = regexp.MustCompile(`[Ss](\d{1,2})[Ee](\d{1,3})((?:-[Ee]\d{1,3})*)`)
	reEpTail   = regexp.MustCompile(`[Ee](\d{1,3})`)
)

const chunkSize = 4 * 1024 * 1024
const lockFile = "/tmp/mediabutler.lock"

type queueItem struct {
	Src   string `json:"src"`
	Name  string `json:"name"`
	Size  int64  `json:"size"`
	State string `json:"state"`
	Err   string `json:"err,omitempty"`
}

type item struct{ src, dst string } // src relativ zur Quelle, dst relativ zum Ziel

type batch struct {
	ID     int
	Source string
	Items  []item
	Size   int64
}

type App struct {
	*Store
	source, target string
	minAge         time.Duration
	user, pass     string
	apiKey         string
	version        string

	mu  sync.Mutex // schützt job
	job jobState

	qmu     sync.Mutex
	pending []*batch
	active  *batch
	qid     int
	qcond   *sync.Cond

	ex *exState

	alerts   map[string]bool
	marks    []int
	runT0    time.Time
	scanDue  bool
	scanPath map[string]bool

	diskT time.Time
	diskV map[string]any
}

type jobState struct {
	Running, Cancel, Finished bool
	Total, Done, CurrentSize  int64
	CurrentDone               int64
	FilesTotal, FilesDone     int
	Current, Source           string
	Speed                     float64
	Errors                    []string
	Queue                     []*queueItem
	Started, Ended            time.Time
}

func (a *App) abs(rel string) string { return filepath.Join(a.source, filepath.FromSlash(rel)) }

func hiddenRel(rel string) bool {
	parts := strings.Split(rel, "/")
	for _, x := range parts[:len(parts)-1] {
		if strings.HasPrefix(x, ".") {
			return true
		}
	}
	return false
}

// pathLess sortiert wie Pythons Path: komponentenweise.
func pathLess(x, y string) bool {
	a, b := strings.Split(x, "/"), strings.Split(y, "/")
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

// walkFiles liefert alle regulären Dateien unter root (relative Pfade, sortiert).
func walkFiles(root string) []string {
	var out []string
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.Type().IsRegular() {
			if r, e := filepath.Rel(root, p); e == nil {
				out = append(out, filepath.ToSlash(r))
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return pathLess(out[i], out[j]) })
	return out
}

func (a *App) safeSource(rel string) (string, error) {
	p, err := filepath.Abs(filepath.Join(a.source, filepath.FromSlash(rel)))
	if err == nil {
		p, err = filepath.EvalSymlinks(p)
	}
	root, _ := filepath.EvalSymlinks(a.source)
	st, e2 := os.Stat(p)
	if err != nil || e2 != nil || !st.Mode().IsRegular() || !strings.HasPrefix(p, root+string(filepath.Separator)) {
		return "", fmt.Errorf("Ungültiger Pfad: %s", rel)
	}
	return p, nil
}

func (a *App) purgeSamples(now bool) {
	for _, rel := range walkFiles(a.source) {
		name := path.Base(rel)
		if !reSample.MatchString(name) || partialExt[lowerExt(name)] || hiddenRel(rel) {
			continue
		}
		p := a.abs(rel)
		st, err := os.Stat(p)
		if err != nil || (!now && time.Since(st.ModTime()) < a.minAge) {
			continue
		}
		if err := os.Remove(p); err != nil {
			fmt.Fprintln(os.Stderr, "Sample nicht gelöscht:", p, err)
		} else {
			fmt.Fprintln(os.Stderr, "Sample gelöscht:", p)
		}
	}
}

// ---------- Zielpfade ----------

func mgPath(rel, cat string, g Group) string {
	show := safe(g.Name)
	if g.Year != "" {
		show = safe(g.Name + " (" + g.Year + ")")
	}
	return path.Join(cat, show+idTag(g.TMDB), fmt.Sprintf("Season %02d", g.Season),
		fmt.Sprintf("%s S%02dE%02d%s", show, g.Season, g.Ep, strings.ToLower(path.Ext(rel))))
}

func (a *App) defaultTarget(rel, cat string) string {
	if g, ok := a.getGroups()[rel]; ok && (cat == "Serien" || cat == "Anime" || cat == "Dokus") {
		return mgPath(rel, cat, g)
	}
	if a.cfg().Jellyfin {
		return a.jfTarget(rel, cat)
	}
	return legacyTarget(rel, cat)
}

func (a *App) targetFor(rel, cat string) string {
	ov, ok := a.getPaths()[rel]
	if !ok || ov.Cat != cat {
		return a.defaultTarget(rel, cat)
	}
	if ov.Folder == "" {
		return ov.Dst
	}
	d := strings.Split(a.defaultTarget(rel, cat), "/")
	n := ov.N
	if n > len(d) {
		n = len(d)
	}
	return path.Join(append([]string{ov.Folder}, d[n:]...)...)
}

func cleanFolder(s string) ([]string, error) {
	var parts []string
	for _, y := range regexp.MustCompile(`[\\/]+`).Split(strings.TrimSpace(s), -1) {
		if x := safe(y); x != "" {
			parts = append(parts, x)
		}
	}
	cat := ""
	if len(parts) > 0 {
		for _, c := range categories {
			if strings.EqualFold(c, parts[0]) {
				cat = c
			}
		}
	}
	if cat == "" {
		return nil, errors.New("Der Zielordner muss mit einer Kategorie beginnen: " + strings.Join(categories, ", "))
	}
	return append([]string{cat}, parts[1:]...), nil
}

func (a *App) folderTargets(rels []string, folder string) ([]string, int, []string, error) {
	parts, err := cleanFolder(folder)
	if err != nil {
		return nil, 0, nil, err
	}
	defs := make([][]string, len(rels))
	n := 2
	for i, r := range rels {
		defs[i] = strings.Split(a.defaultTarget(r, parts[0]), "/")
		if len(defs[i])-1 < n {
			n = len(defs[i]) - 1
		}
	}
	for n > 1 {
		seen := map[string]bool{}
		for _, d := range defs {
			seen[strings.Join(d[:n], "/")] = true
		}
		if len(seen) <= 1 {
			break
		}
		n--
	}
	out := make([]string, len(rels))
	for i, d := range defs {
		if n < 0 {
			n = 0
		}
		out[i] = path.Join(append(append([]string{}, parts...), d[n:]...)...)
	}
	return parts, n, out, nil
}

func showDir(target string) string {
	parts := strings.Split(target, "/")
	i := 0
	for n, x := range parts {
		if reSeasonT.MatchString(x) {
			i = n
			break
		}
	}
	if i > 1 {
		return strings.Join(parts[:i], "/")
	}
	if len(parts) > 2 {
		parts = parts[:2]
	}
	return strings.Join(parts, "/")
}

func (a *App) libraryEps(shows map[string]bool) map[string]map[string][]int {
	out := map[string]map[string][]int{}
	for sd := range shows {
		res := map[int]map[int]bool{}
		d := filepath.Join(a.target, filepath.FromSlash(sd))
		filepath.WalkDir(d, func(p string, e fs.DirEntry, err error) error {
			if err != nil || !e.Type().IsRegular() || !videoExt[lowerExt(e.Name())] {
				return nil
			}
			if m := reEPS.FindStringSubmatch(e.Name()); m != nil {
				s := atoi(m[1])
				if res[s] == nil {
					res[s] = map[int]bool{}
				}
				res[s][atoi(m[2])] = true
				for _, x := range reEpTail.FindAllStringSubmatch(m[3], -1) {
					res[s][atoi(x[1])] = true
				}
			}
			return nil
		})
		o := map[string][]int{}
		for s, set := range res {
			l := []int{}
			for e := range set {
				l = append(l, e)
			}
			sort.Ints(l)
			o[fmt.Sprint(s)] = l
		}
		out[sd] = o
	}
	return out
}

func (a *App) seriesInfo(rel string) map[string]any {
	if g, ok := a.getGroups()[rel]; ok {
		return map[string]any{"name": g.Name, "orig": g.Name, "year": g.Year, "rkey": "g:" + skey(g.Name),
			"key": "g:" + skey(g.Name) + g.Year, "merged": false, "manual": true, "season": g.Season, "ep": g.Ep, "eps": []int{g.Ep}}
	}
	ep := parseEp(rel, false, true, false, nil)
	if ep == nil {
		return nil
	}
	r := a.ruleFor(ep.title)
	shown, year, ryear := ep.title, ep.year, ""
	if r != nil {
		if r.Name != "" {
			shown = r.Name
		}
		if r.Year != "" {
			year = r.Year
		}
		ryear = r.Year
		if r.Year == "" {
			year = ""
		}
	}
	return map[string]any{"name": shown, "orig": ep.title, "year": year, "rkey": skey(ep.title),
		"key": skey(shown) + ryear, "merged": r != nil, "season": ep.season, "ep": ep.eps[0], "eps": ep.eps}
}

// ---------- Scan ----------

func (a *App) isCopied(cm map[string]sig, rel string) bool {
	s, err := fileSig(a.abs(rel))
	return err == nil && cm[rel] == s
}

func (a *App) scan() ([]item, []string) {
	a.purgeSamples(true)
	cm, bl := a.copied(), a.extractBlocked()
	var items []item
	unclear := []string{}
	for _, rel := range walkFiles(a.source) {
		name := path.Base(rel)
		if !videoExt[lowerExt(name)] || strings.HasPrefix(name, ".") || reSample.MatchString(name) || hiddenRel(rel) || exBlocked(rel, bl) {
			continue
		}
		st, err := os.Stat(a.abs(rel))
		if err != nil || time.Since(st.ModTime()) < a.minAge || a.isCopied(cm, rel) {
			continue
		}
		if cat := a.guessCategory(rel); cat != "" {
			items = append(items, item{rel, a.targetFor(rel, cat)})
		} else {
			unclear = append(unclear, rel)
		}
	}
	return items, unclear
}

func (a *App) busySrc() map[string]bool {
	b := map[string]bool{}
	a.mu.Lock()
	if a.job.Running {
		for _, q := range a.job.Queue {
			if q.State == "wartet" || q.State == "kopiert" {
				b[q.Src] = true
			}
		}
	}
	a.mu.Unlock()
	a.qmu.Lock()
	for _, bt := range a.pending {
		for _, it := range bt.Items {
			b[it.src] = true
		}
	}
	a.qmu.Unlock()
	return b
}

// ---------- Warteschlange & Kopier-Job ----------

func (a *App) enqueue(items []item, source string) (int, bool) {
	a.qmu.Lock()
	have := map[string]bool{}
	for _, b := range a.pending {
		for _, it := range b.Items {
			have[it.src] = true
		}
	}
	a.qmu.Unlock()
	for k := range a.busySrcJob() {
		have[k] = true
	}
	var nw []item
	var size int64
	for _, it := range items {
		if !have[it.src] {
			nw = append(nw, it)
			if st, err := os.Stat(a.abs(it.src)); err == nil {
				size += st.Size()
			}
		}
	}
	if len(nw) == 0 {
		return 0, false
	}
	a.qmu.Lock()
	defer a.qmu.Unlock()
	a.mu.Lock()
	waits := a.job.Running || len(a.pending) > 0
	a.mu.Unlock()
	a.qid++
	a.pending = append(a.pending, &batch{a.qid, source, nw, size})
	a.qcond.Broadcast()
	return len(nw), waits
}

func (a *App) busySrcJob() map[string]bool {
	b := map[string]bool{}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.job.Running {
		for _, q := range a.job.Queue {
			if q.State == "wartet" || q.State == "kopiert" {
				b[q.Src] = true
			}
		}
	}
	return b
}

func (a *App) waitingBatches() []*batch {
	a.qmu.Lock()
	defer a.qmu.Unlock()
	var w []*batch
	for _, b := range a.pending {
		if b != a.active {
			w = append(w, b)
		}
	}
	return w
}

func (a *App) queueWorker() {
	for {
		a.qmu.Lock()
		for len(a.pending) == 0 {
			a.qcond.Wait()
		}
		b := a.pending[0]
		a.active = b
		a.qmu.Unlock()
		for a.busy() {
			time.Sleep(2 * time.Second)
		}
		a.runJob(b.Items, b.Source)
		a.qmu.Lock()
		for i, x := range a.pending {
			if x == b {
				a.pending = append(a.pending[:i], a.pending[i+1:]...)
				break
			}
		}
		a.active = nil
		idle := len(a.pending) == 0
		a.qmu.Unlock()
		if idle {
			a.jfAfterQueue()
		}
	}
}

func acquire() *os.File {
	f, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil
	}
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		f.Close()
		return nil
	}
	return f
}

func (a *App) busy() bool {
	f := acquire()
	if f == nil {
		return true
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
	return false
}

func (a *App) copyItems(items []item, source string, move bool) []item {
	var ok []item
	a.mu.Lock()
	q := make([]*queueItem, len(items))
	var total int64
	for i, it := range items {
		var sz int64
		if st, err := os.Stat(a.abs(it.src)); err == nil {
			sz = st.Size()
		}
		total += sz
		q[i] = &queueItem{Src: it.src, Name: it.dst, Size: sz, State: "wartet"}
	}
	a.job = jobState{Running: true, FilesTotal: len(items), Source: source, Total: total, Started: time.Now(), Queue: q, Errors: []string{}}
	a.mu.Unlock()
	for i, it := range items {
		if a.cancelled() {
			break
		}
		qi := q[i]
		if err := a.copyOne(it, qi, move); err != nil {
			a.mu.Lock()
			a.job.Errors = append(a.job.Errors, fmt.Sprintf("%s: %v", path.Base(it.dst), err))
			qi.State, qi.Err = "fehler", err.Error()
			a.mu.Unlock()
		} else {
			ok = append(ok, it)
		}
	}
	a.mu.Lock()
	for _, x := range q {
		if x.State == "wartet" || x.State == "kopiert" {
			x.State = "abgebrochen"
		}
	}
	a.job.Running, a.job.Finished, a.job.Current, a.job.Ended = false, true, "", time.Now()
	a.mu.Unlock()
	return ok
}

func (a *App) cancelled() bool { a.mu.Lock(); defer a.mu.Unlock(); return a.job.Cancel }

func (a *App) copyOne(it item, qi *queueItem, move bool) (err error) {
	src, dst := a.abs(it.src), filepath.Join(a.target, filepath.FromSlash(it.dst))
	st, err := os.Stat(src)
	if err != nil {
		return err
	}
	size := st.Size()
	cat := strings.SplitN(it.dst, "/", 2)[0]
	a.mu.Lock()
	a.job.Current, a.job.CurrentSize, a.job.CurrentDone = it.dst, size, 0
	qi.State = "kopiert"
	a.mu.Unlock()
	if ds, e := os.Stat(dst); e == nil && ds.Size() == size {
		if move {
			os.Remove(src)
			a.pushHistory(cat, path.Base(it.src), dst, size, "war schon vorhanden, Quelle gelöscht")
		} else {
			a.markCopied(it.src, src)
		}
		a.mu.Lock()
		a.job.Done += size
		a.job.FilesDone++
		qi.State = "vorhanden"
		a.mu.Unlock()
		return nil
	}
	if err = os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	part := dst + ".part"
	defer func() {
		if err != nil {
			os.Remove(part)
		}
	}()
	fi, err := os.Open(src)
	if err != nil {
		return err
	}
	defer fi.Close()
	fo, err := os.Create(part)
	if err != nil {
		return err
	}
	buf := make([]byte, chunkSize)
	t0 := time.Now()
	for {
		if a.cancelled() {
			fo.Close()
			return errors.New("Abgebrochen")
		}
		n, rerr := fi.Read(buf)
		if n > 0 {
			if _, werr := fo.Write(buf[:n]); werr != nil {
				fo.Close()
				return werr
			}
			a.mu.Lock()
			a.job.Done += int64(n)
			a.job.CurrentDone += int64(n)
			if el := time.Since(t0).Seconds(); el > 0.5 {
				a.job.Speed = float64(a.job.CurrentDone) / el
			}
			a.mu.Unlock()
			a.progressCheck()
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			fo.Close()
			return rerr
		}
	}
	if err = fo.Sync(); err != nil {
		fo.Close()
		return err
	}
	if err = fo.Close(); err != nil {
		return err
	}
	if err = os.Rename(part, dst); err != nil {
		return err
	}
	os.Chtimes(dst, st.ModTime(), st.ModTime())
	os.Chmod(dst, st.Mode().Perm())
	if ds, e := os.Stat(dst); e != nil || ds.Size() != size {
		return errors.New("Größe am Ziel stimmt nicht")
	}
	status := "kopiert"
	if move {
		os.Remove(src)
		status = "verschoben"
	}
	a.pushHistory(cat, path.Base(it.src), dst, size, status)
	if !move {
		a.markCopied(it.src, src)
	} else {
		a.markCopied(it.src, src) // Quelle existiert nicht mehr: markCopied ignoriert das
	}
	a.mu.Lock()
	a.job.FilesDone++
	qi.State = "fertig"
	a.mu.Unlock()
	return nil
}

func (a *App) trigger(source string) (bool, string) {
	if st, err := os.Stat(a.target); err != nil || !st.IsDir() {
		a.alertOnce("target", true, fmt.Sprintf("🚨 Handlungsbedarf – MediaButler\nDas Ziel %s ist nicht erreichbar. Ist das NFS-Share eingebunden? Bis dahin wird nichts kopiert.", a.target), "")
		return false, fmt.Sprintf("Ziel %s nicht erreichbar (NFS gemountet?)", a.target)
	}
	items, unclear := a.scan()
	a.unclearNotice(unclear)
	n, waits := a.enqueue(items, source)
	if n == 0 && len(items) > 0 {
		return false, "Alle fertigen Dateien laufen schon oder warten bereits."
	}
	msg := fmt.Sprintf("%d Datei(en) in der Warteschlange", n)
	if waits {
		msg += " (nach dem laufenden Vorgang)"
	}
	if len(unclear) > 0 {
		msg += fmt.Sprintf(", %d zur Klärung übersprungen", len(unclear))
	}
	return true, msg
}

func (a *App) scheduler() {
	var last time.Time
	for {
		time.Sleep(15 * time.Second)
		c := a.cfg()
		now := time.Now().Truncate(time.Minute)
		if !c.Enabled || now.Equal(last) {
			continue
		}
		cr, err := NewCron(c.Cron)
		if err != nil || !cr.Match(now) {
			continue
		}
		last = now
		if ok, msg := a.trigger("Zeitplan"); !ok {
			a.saveCfg(func(c *Config) {
				c.LastRun = now.Format("2006-01-02 15:04:05")
				c.LastResult = "Zeitplan: übersprungen – " + msg
			})
		}
	}
}

// ---------- Reste im Quellordner ----------

func (a *App) dirInfo(name string) map[string]any {
	root := filepath.Join(a.source, name)
	var n, media int
	var size int64
	partial, arch := false, false
	var newest time.Time
	sample := []string{}
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return nil
		}
		st, e := d.Info()
		if e != nil {
			return nil
		}
		ext := lowerExt(d.Name())
		n++
		size += st.Size()
		if videoExt[ext] {
			media++
		}
		if partialExt[ext] {
			partial = true
		}
		if archiveExt[ext] || reArchPart.MatchString(ext) {
			arch = true
		}
		if st.ModTime().After(newest) {
			newest = st.ModTime()
		}
		if len(sample) < 3 {
			sample = append(sample, d.Name())
		}
		return nil
	})
	if n == 0 {
		if st, err := os.Stat(root); err == nil {
			newest = st.ModTime()
		}
	}
	reason := ""
	blocked := false
	for _, x := range a.extractBlocked() {
		blocked = blocked || x == name || strings.HasPrefix(x, name+"/")
	}
	switch {
	case blocked:
		reason = "wird gerade entpackt"
	case media > 0:
		reason = "enthält noch Medien"
	case partial:
		reason = "wird noch geschrieben"
	case time.Since(newest) < a.minAge:
		reason = "kürzlich geändert"
	}
	return map[string]any{"path": name, "type": "dir", "files": n, "size": size, "media": media, "archives": arch, "locked": reason, "sample": sample}
}

func (a *App) entryInfo(name string) map[string]any {
	p := filepath.Join(a.source, name)
	st, err := os.Stat(p)
	if err != nil {
		return map[string]any{"path": name, "type": "file", "files": 0, "size": 0, "media": 0, "archives": false, "locked": "", "sample": []string{}}
	}
	if st.IsDir() {
		return a.dirInfo(name)
	}
	e := lowerExt(name)
	reason := ""
	switch {
	case videoExt[e]:
		reason = "ist noch ein Medium"
	case partialExt[e]:
		reason = "wird noch geschrieben"
	case time.Since(st.ModTime()) < a.minAge:
		reason = "kürzlich geändert"
	}
	m := 0
	if videoExt[e] {
		m = 1
	}
	return map[string]any{"path": name, "type": "file", "files": 1, "size": st.Size(), "media": m,
		"archives": archiveExt[e] || reArchPart.MatchString(e), "locked": reason, "sample": []string{}}
}

func (a *App) topEntries() []string {
	es, _ := os.ReadDir(a.source)
	var out []string
	for _, e := range es {
		if !strings.HasPrefix(e.Name(), ".") {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out
}

func (a *App) removeEmptyDirs() int {
	var dirs []string
	filepath.WalkDir(a.source, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() && p != a.source {
			dirs = append(dirs, p)
		}
		return nil
	})
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	n := 0
	for _, d := range dirs {
		es, err := os.ReadDir(d)
		st, e2 := os.Stat(d)
		if err == nil && e2 == nil && len(es) == 0 && !strings.HasPrefix(filepath.Base(d), ".") && time.Since(st.ModTime()) >= a.minAge {
			if os.Remove(d) == nil {
				n++
			}
		}
	}
	return n
}

func (a *App) deleteEntry(name string, force bool) error {
	if name == "" || strings.ContainsAny(name, "/\\") || name == "." || name == ".." {
		return fmt.Errorf("Ungültiger Eintrag: %s", name)
	}
	p := filepath.Join(a.source, name)
	if _, err := os.Lstat(p); err != nil {
		return fmt.Errorf("Ungültiger Eintrag: %s", name)
	}
	if !force {
		if r, _ := a.entryInfo(name)["locked"].(string); r != "" {
			return fmt.Errorf("%s: %s", name, r)
		}
	}
	return os.RemoveAll(p)
}

func (a *App) cleanupLeftovers() int {
	n := 0
	for _, name := range a.topEntries() {
		if st, err := os.Stat(filepath.Join(a.source, name)); err != nil || !st.IsDir() {
			continue
		}
		i := a.dirInfo(name)
		if i["locked"] == "" && i["archives"] == false {
			os.RemoveAll(filepath.Join(a.source, name))
			n++
		}
	}
	return n + a.removeEmptyDirs()
}

// ---------- Datenträger ----------

func (a *App) diskInfo() map[string]any {
	if time.Since(a.diskT) < 20*time.Second {
		return a.diskV
	}
	var v map[string]any
	var s syscall.Statfs_t
	if syscall.Statfs(a.target, &s) == nil {
		bs := uint64(s.Frsize)
		total, free, used := s.Blocks*bs, s.Bavail*bs, (s.Blocks-s.Bfree)*bs
		den := used + free
		if den == 0 {
			den = 1
		}
		pct := float64(int(float64(used)/float64(den)*1000+0.5)) / 10
		v = map[string]any{"total": total, "used": used, "free": free, "pct": pct, "path": a.target}
	}
	a.diskT, a.diskV = time.Now(), v
	return v
}

func lookPath(name string) (string, error) { return exec.LookPath(name) }
