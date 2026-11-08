package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
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
	"syscall"
	"time"
)

var (
	rPart      = regexp.MustCompile(`(?i)^(.+)\.part0*(\d+)\.rar$`)
	rOld       = regexp.MustCompile(`(?i)^(.+)\.r(\d{2,3})$`)
	zOld       = regexp.MustCompile(`(?i)^(.+)\.z(\d{2,3})$`)
	rSplit     = regexp.MustCompile(`(?i)^(.+\.(?:zip|rar|7z))\.(\d{3})$`)
	rIncompl   = regexp.MustCompile(`(?i)unexpected end of archive|cannot find volume|missing volume|next volume|there are some data after the end`)
	rWrongPw   = regexp.MustCompile(`(?i)wrong password|encrypted`)
	rPct       = regexp.MustCompile(`(\d{1,3})%`)
	rEncLine   = regexp.MustCompile(`(?im)^encrypted = \+`)
	exInterval = time.Duration(atoi(env("EXTRACT_INTERVAL", "20"))) * time.Second
)

type exEntry struct {
	ID, Name, Dir, Main string
	Parts               []string
	Size                int64
	Sig, State, Msg     string
	Pct                 int
	Found, RetryAfter   float64
	Force, Cancel       bool
	PwHint              string
}

type exState struct {
	mu   sync.Mutex
	m    map[string]*exEntry
	done int
}

func sevenz() string {
	for _, b := range []string{"7zz", "7z", "7za", "7zr"} {
		if p, err := exec.LookPath(b); err == nil {
			return p
		}
	}
	return ""
}

func now() float64 { return float64(time.Now().UnixNano()) / 1e9 }

func (a *App) exStable() time.Duration {
	s := a.minAge
	if s > 60*time.Second {
		s = 60 * time.Second
	}
	if v := os.Getenv("EXTRACT_STABLE"); v != "" {
		s = time.Duration(atoi(v)) * time.Second
	}
	if s < 10*time.Second {
		s = 10 * time.Second
	}
	return s
}

func (a *App) extractBlocked() []string {
	a.ex.mu.Lock()
	defer a.ex.mu.Unlock()
	set := map[string]bool{}
	for _, e := range a.ex.m {
		if e.Dir != "" && e.State != "fehler" {
			set[e.Dir] = true
		}
	}
	out := []string{}
	for d := range set {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

func exBlocked(rel string, bl []string) bool {
	for _, d := range bl {
		if rel == d || strings.HasPrefix(rel, d+"/") {
			return true
		}
	}
	return false
}

func (a *App) exSummary() M {
	a.ex.mu.Lock()
	defer a.ex.mu.Unlock()
	n, pw, er, run, pct, name := 0, 0, 0, false, 0, ""
	for _, e := range a.ex.m {
		switch e.State {
		case "läuft":
			n++
			run, pct, name = true, e.Pct, e.Name
		case "wartet", "unvollständig":
			n++
		case "passwort":
			pw++
		case "fehler":
			er++
		}
	}
	return M{"n": n, "pw": pw, "err": er, "running": run, "done": a.ex.done, "total": len(a.ex.m), "pct": pct, "name": name}
}

type archive struct {
	base, dir, main string
	vols            map[int]string
	kind            string
	missing         string
	parts           []string
	size            int64
	newest          float64
	partial         bool
	id              string
}

func missingMsg(vols map[int]string) string {
	if len(vols) == 0 {
		return ""
	}
	mx := 0
	for k := range vols {
		if k > mx {
			mx = k
		}
	}
	var lost []int
	for i := 1; i <= mx; i++ {
		if _, ok := vols[i]; !ok {
			lost = append(lost, i)
		}
	}
	if len(lost) == 0 {
		return ""
	}
	s := fmt.Sprintf("Teil %d fehlt", lost[0])
	if len(lost) > 1 {
		s += fmt.Sprintf(" (und %d weitere)", len(lost)-1)
	}
	return s
}

func (a *App) findArchives() []archive {
	groups := map[string]*archive{}
	var order []string
	partialDirs := map[string]bool{}
	grp := func(rel, kind, base string) *archive {
		d := path.Dir(rel)
		if d == "." {
			d = ""
		}
		k := d + "\x00" + kind + "\x00" + strings.ToLower(base)
		g := groups[k]
		if g == nil {
			g = &archive{base: base, dir: d, kind: kind, vols: map[int]string{}}
			groups[k] = g
			order = append(order, k)
		}
		return g
	}
	for _, rel := range walkFiles(a.source) {
		if hiddenRel(rel) || strings.HasPrefix(path.Base(rel), ".") {
			if hiddenRel(rel) {
				continue
			}
		}
		n := path.Base(rel)
		low := strings.ToLower(n)
		if partialExt[lowerExt(n)] {
			d := path.Dir(rel)
			if d == "." {
				d = ""
			}
			partialDirs[d] = true
			continue
		}
		if strings.HasPrefix(n, ".") {
			continue
		}
		if m := rPart.FindStringSubmatch(n); m != nil {
			grp(rel, "rarpart", m[1]).vols[atoi(m[2])] = rel
		} else if m := rSplit.FindStringSubmatch(n); m != nil {
			grp(rel, "split", m[1]).vols[atoi(m[2])] = rel
		} else if strings.HasSuffix(low, ".rar") {
			grp(rel, "rar", n[:len(n)-4]).main = rel
		} else if m := rOld.FindStringSubmatch(n); m != nil {
			grp(rel, "rar", m[1]).vols[atoi(m[2])+1] = rel
		} else if strings.HasSuffix(low, ".zip") {
			grp(rel, "zip", n[:len(n)-4]).main = rel
		} else if m := zOld.FindStringSubmatch(n); m != nil {
			grp(rel, "zip", m[1]).vols[atoi(m[2])] = rel
		} else if strings.HasSuffix(low, ".7z") {
			grp(rel, "7z", n[:len(n)-3]).main = rel
		}
	}
	var out []archive
	for _, k := range order {
		g := groups[k]
		main := g.main
		if g.kind == "rarpart" || g.kind == "split" {
			if len(g.vols) == 0 {
				continue
			}
			main = g.vols[1]
			g.missing = missingMsg(g.vols)
		} else if main == "" {
			continue
		} else {
			g.missing = missingMsg(g.vols)
		}
		keys := make([]int, 0, len(g.vols))
		for i := range g.vols {
			keys = append(keys, i)
		}
		sort.Ints(keys)
		var files []string
		seen := map[string]bool{}
		add := func(f string) {
			if f != "" && !seen[f] {
				seen[f] = true
				files = append(files, f)
			}
		}
		add(main)
		for _, i := range keys {
			add(g.vols[i])
		}
		var size int64
		var newest float64
		bad := false
		for _, f := range files {
			st, err := os.Stat(a.abs(f))
			if err != nil {
				bad = true
				break
			}
			size += st.Size()
			if t := float64(st.ModTime().UnixNano()) / 1e9; t > newest {
				newest = t
			}
		}
		if bad {
			continue
		}
		relMain := main
		if relMain == "" {
			relMain = files[0]
		}
		h := md5.Sum([]byte(relMain))
		g.id, g.parts, g.size, g.newest, g.main = hex.EncodeToString(h[:])[:10], files, size, newest, main
		g.partial = partialDirs[g.dir]
		out = append(out, *g)
	}
	return out
}

// ---------- Passwörter / extract.json ----------

type pwList struct {
	List []string `json:"list"`
	Last string   `json:"last"`
}

func (a *App) exPws() pwList {
	d := pwList{List: []string{}}
	a.load("passwords.json", &d)
	if d.List == nil {
		d.List = []string{}
	}
	return d
}

func (a *App) addPw(pw string, ok bool) {
	pw = strings.Trim(pw, "\r\n")
	if pw == "" {
		return
	}
	a.ex.mu.Lock()
	defer a.ex.mu.Unlock()
	d := a.exPws()
	found := false
	for _, x := range d.List {
		found = found || x == pw
	}
	if !found {
		d.List = append(d.List, pw)
	}
	if ok {
		d.Last = pw
	}
	a.save("passwords.json", d)
}

func (a *App) exStore() M {
	m := M{}
	a.load("extract.json", &m)
	return m
}

func subMap(m M, k string) M {
	if x, ok := m[k].(M); ok {
		return x
	}
	if x, ok := m[k].(map[string]any); ok {
		return x
	}
	x := M{}
	m[k] = x
	return x
}

func capMap(m M, n int) M {
	if len(m) <= n {
		return m
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return toF(m[keys[i]]) < toF(m[keys[j]]) })
	for _, k := range keys[:len(m)-n] {
		delete(m, k)
	}
	return m
}

func toF(v any) float64 { f, _ := v.(float64); return f }

func (a *App) exCandidates(e *exEntry) []string {
	d := a.exPws()
	order := append([]string{e.PwHint, d.Last}, d.List...)
	var out []string
	seen := map[string]bool{}
	for _, x := range order {
		if x != "" && !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

// ---------- Scan / Auswahl ----------

func (a *App) extractScan() {
	if sevenz() == "" {
		return
	}
	found := a.findArchives()
	t := now()
	st := a.exStore()
	done := subMap(st, "done_sigs")
	a.ex.mu.Lock()
	defer a.ex.mu.Unlock()
	ids := map[string]bool{}
	for _, ar := range found {
		ids[ar.id] = true
		sig := fmt.Sprintf("%s:%d", ar.id, ar.size)
		e := a.ex.m[ar.id]
		if e == nil {
			if _, ok := done[sig]; ok {
				continue
			}
			e = &exEntry{ID: ar.id, State: "unvollständig", Found: t}
			a.ex.m[ar.id] = e
		}
		changed := e.Sig != sig
		e.Name, e.Dir, e.Main, e.Parts, e.Size, e.Sig = ar.base, ar.dir, ar.main, ar.parts, ar.size, sig
		if e.State == "läuft" {
			continue
		}
		if ar.missing != "" || ar.main == "" {
			e.State = "unvollständig"
			e.Msg = ar.missing
			if e.Msg == "" {
				e.Msg = "Erster Teil fehlt"
			}
			continue
		}
		if t-ar.newest < a.exStable().Seconds() || ar.partial {
			e.State, e.Msg = "unvollständig", "Wird noch geladen …"
			continue
		}
		if (e.State == "unvollständig" && t >= e.RetryAfter) || (changed && (e.State == "fehler" || e.State == "passwort")) {
			e.State, e.Msg = "wartet", ""
		}
	}
	for id, e := range a.ex.m {
		if !ids[id] && e.State != "läuft" {
			delete(a.ex.m, id)
		}
	}
}

func (a *App) exPick() *exEntry {
	c := a.cfg()
	a.ex.mu.Lock()
	defer a.ex.mu.Unlock()
	ids := make([]string, 0, len(a.ex.m))
	for id := range a.ex.m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return a.ex.m[ids[i]].Found < a.ex.m[ids[j]].Found })
	for _, id := range ids {
		e := a.ex.m[id]
		if e.State == "wartet" && (c.ExtractAuto || e.Force) && now() >= e.RetryAfter {
			return e
		}
	}
	return nil
}

// run7z startet 7-Zip, liest Ausgabe, meldet Prozent, bricht bei Cancel ab.
func (a *App) run7z(args []string, e *exEntry) (int, string) {
	cmd := exec.Command(sevenz(), args...)
	pr, pw := io.Pipe()
	cmd.Stdout, cmd.Stderr = pw, pw
	if err := cmd.Start(); err != nil {
		return -1, err.Error()
	}
	go func() { cmd.Wait(); pw.Close() }()
	var buf []byte
	chunk := make([]byte, 4096)
	for {
		n, err := pr.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
			if len(buf) > 8000 {
				buf = buf[len(buf)-8000:]
			}
			if e != nil {
				if m := rPct.FindAllSubmatch(chunk[:n], -1); len(m) > 0 {
					a.ex.mu.Lock()
					e.Pct = min(100, atoi(string(m[len(m)-1][1])))
					c := e.Cancel
					a.ex.mu.Unlock()
					if c {
						cmd.Process.Signal(syscall.SIGTERM)
					}
				}
			}
		}
		if err != nil {
			break
		}
	}
	cmd.Wait()
	return cmd.ProcessState.ExitCode(), string(bytes.ToValidUTF8(buf, []byte("�")))
}

type probe struct {
	enc, incomplete bool
	out             string
}

func (a *App) exProbe(main string) probe {
	_, out := a.run7z([]string{"l", "-slt", "-y", main}, nil)
	low := strings.ToLower(out)
	return probe{enc: rEncLine.MatchString(out) || strings.Contains(low, "wrong password") || strings.Contains(low, "encrypted archive") || strings.Contains(low, "enter password"),
		incomplete: rIncompl.MatchString(out), out: out}
}

func exMoveInto(src, dst string) error {
	es, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, it := range es {
		s, t := filepath.Join(src, it.Name()), filepath.Join(dst, it.Name())
		if st, e := os.Lstat(t); e == nil {
			if it.IsDir() && st.IsDir() {
				if err := exMoveInto(s, t); err != nil {
					return err
				}
				continue
			}
			ext := filepath.Ext(it.Name())
			stem := strings.TrimSuffix(it.Name(), ext)
			for k := 2; ; k++ {
				t = filepath.Join(dst, fmt.Sprintf("%s (%d)%s", stem, k, ext))
				if _, e := os.Lstat(t); e != nil {
					break
				}
			}
		}
		if err := os.Rename(s, t); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) exSet(e *exEntry, f func()) { a.ex.mu.Lock(); f(); a.ex.mu.Unlock() }

func (a *App) extractRun(e *exEntry) {
	main, base := a.abs(e.Main), filepath.Join(a.source, filepath.FromSlash(e.Dir))
	tmp := filepath.Join(base, ".entpacken-"+e.ID)
	c := a.cfg()
	a.exSet(e, func() { e.State, e.Pct, e.Msg, e.Cancel, e.Force = "läuft", 0, "Prüfe Archiv …", false, false })
	fail := func(state, msg string, retry float64) {
		os.RemoveAll(tmp)
		a.exSet(e, func() {
			e.State, e.Msg, e.Pct, e.RetryAfter = state, msg, 0, 0
			if retry > 0 {
				e.RetryAfter = now() + retry
			}
		})
		if state == "fehler" {
			a.notify(fmt.Sprintf("⚠️ MediaButler: Entpacken fehlgeschlagen\n📦 %s\n%s", e.Name, msg)+a.linkLine("ext"), "tg_errors")
		} else if state == "passwort" {
			a.exNotifyPw(e)
		}
	}
	type res struct {
		rc     int
		out    string
		used   *string
		pwfail bool
	}
	attempt := func(pws []*string) res {
		var r res
		for i, pw := range pws {
			os.RemoveAll(tmp)
			os.MkdirAll(tmp, 0o755)
			a.exSet(e, func() {
				e.Pct = 0
				if pw == nil && len(pws) == 1 {
					e.Msg = "Entpacke …"
				} else {
					e.Msg = fmt.Sprintf("Entpacke mit Passwort %d von %d …", i+1, len(pws))
				}
			})
			args := []string{"x", "-y", "-bsp1", "-bso0", "-aoa", "-o" + tmp}
			if pw != nil {
				args = append(args, "-p"+*pw)
			}
			r.rc, r.out = a.run7z(append(args, main), e)
			r.used = pw
			a.ex.mu.Lock()
			cn := e.Cancel
			a.ex.mu.Unlock()
			if cn || r.rc == 0 || r.rc == 1 || !rWrongPw.MatchString(r.out) {
				return r
			}
		}
		r.used, r.pwfail = nil, true
		return r
	}
	pwPtrs := func() []*string {
		var l []*string
		for _, p := range a.exCandidates(e) {
			p := p
			l = append(l, &p)
		}
		return l
	}
	if st, err := os.Stat(main); err != nil || !st.Mode().IsRegular() {
		fail("unvollständig", "Archiv-Datei nicht gefunden", 60)
		return
	}
	pr := a.exProbe(main)
	if pr.incomplete {
		fail("unvollständig", "Es fehlen noch Teile des Archivs", 60)
		return
	}
	const noPw = "Archiv ist verschlüsselt, es ist noch kein Passwort gespeichert."
	pws := []*string{nil}
	if pr.enc {
		if pws = pwPtrs(); len(pws) == 0 {
			fail("passwort", noPw, 0)
			return
		}
	}
	r := attempt(pws)
	if r.pwfail && !pr.enc {
		if pws = pwPtrs(); len(pws) > 0 {
			r = attempt(pws)
		} else {
			fail("passwort", noPw, 0)
			return
		}
	}
	a.ex.mu.Lock()
	cancelled := e.Cancel
	a.ex.mu.Unlock()
	if cancelled {
		fail("fehler", "Abgebrochen", 0)
		return
	}
	if r.pwfail {
		fail("passwort", fmt.Sprintf("Keines der %d gespeicherten Passwörter passt.", len(pws)), 0)
		return
	}
	if r.rc != 0 && r.rc != 1 {
		if rIncompl.MatchString(r.out) {
			fail("unvollständig", "Es fehlen noch Teile des Archivs", 60)
			return
		}
		var lines []string
		for _, l := range strings.Split(strings.ReplaceAll(r.out, "\r", "\n"), "\n") {
			if l = strings.TrimSpace(l); l != "" && !strings.Contains(l, "%") {
				lines = append(lines, l)
			}
		}
		if len(lines) > 3 {
			lines = lines[len(lines)-3:]
		}
		msg := strings.Join(lines, " ")
		if msg == "" {
			msg = fmt.Sprintf("7-Zip-Fehlercode %d", r.rc)
		}
		if rs := []rune(msg); len(rs) > 300 {
			msg = string(rs[:300])
		}
		fail("fehler", msg, 0)
		return
	}
	type of struct {
		name string
		vid  bool
		size int64
	}
	var outs []of
	filepath.WalkDir(tmp, func(p string, d fs.DirEntry, err error) error {
		if err == nil && d.Type().IsRegular() {
			if i, e := d.Info(); e == nil {
				outs = append(outs, of{d.Name(), videoExt[lowerExt(d.Name())], i.Size()})
			}
		}
		return nil
	})
	sort.SliceStable(outs, func(i, j int) bool {
		if outs[i].vid != outs[j].vid {
			return outs[i].vid
		}
		return outs[i].size > outs[j].size
	})
	if len(outs) == 0 {
		fail("fehler", "Das Archiv enthielt keine Dateien.", 0)
		return
	}
	names := []string{}
	for i, o := range outs {
		if i < 6 {
			names = append(names, o.name)
		}
	}
	a.exSet(e, func() { e.Msg, e.Pct = "Verschiebe Dateien …", 100 })
	if err := exMoveInto(tmp, base); err != nil {
		fail("fehler", "Verschieben fehlgeschlagen: "+err.Error(), 0)
		return
	}
	os.RemoveAll(tmp)
	a.purgeSamples(true)
	if r.used != nil {
		a.addPw(*r.used, true)
	}
	del := c.ExtractDel && r.rc == 0
	if del {
		for _, p := range e.Parts {
			os.Remove(a.abs(p))
		}
	}
	a.ex.mu.Lock()
	st := a.exStore()
	ds := subMap(st, "done_sigs")
	ds[e.Sig] = float64(time.Now().Unix())
	st["done_sigs"] = capMap(ds, 500)
	entry := M{"name": e.Name, "dir": e.Dir, "files": len(outs), "out": names, "size": e.Size, "ended": time.Now().Unix(),
		"deleted": del, "warn": r.rc == 1, "pw": r.used != nil}
	prev, _ := st["done"].([]any)
	st["done"] = append([]any{entry}, prev...)
	if l := st["done"].([]any); len(l) > 50 {
		st["done"] = l[:50]
	}
	a.save("extract.json", st)
	delete(a.ex.m, e.ID)
	a.ex.done++
	a.ex.mu.Unlock()
}

func (a *App) extractLoop() {
	time.Sleep(2 * time.Second)
	if a.sevenOK() {
		filepath.WalkDir(a.source, func(p string, d fs.DirEntry, err error) error {
			if err == nil && d.IsDir() && strings.HasPrefix(d.Name(), ".entpacken-") {
				os.RemoveAll(p)
				return filepath.SkipDir
			}
			return nil
		})
	}
	var last time.Time
	for {
		if time.Since(last) >= exInterval {
			a.extractScan()
			last = time.Now()
		}
		if e := a.exPick(); e != nil {
			a.extractRun(e)
			continue
		}
		time.Sleep(2 * time.Second)
	}
}

func (a *App) sevenOK() bool { return sevenz() != "" }

// ---------- HTTP ----------

func (a *App) exPayload() M {
	d, st, c := a.exPws(), a.exStore(), a.cfg()
	a.ex.mu.Lock()
	es := make([]*exEntry, 0, len(a.ex.m))
	for _, e := range a.ex.m {
		es = append(es, e)
	}
	sort.Slice(es, func(i, j int) bool { return es[i].Found < es[j].Found })
	items := []M{}
	for _, e := range es {
		items = append(items, M{"id": e.ID, "name": e.Name, "dir": e.Dir, "state": e.State, "pct": e.Pct, "msg": e.Msg,
			"size": e.Size, "nparts": len(e.Parts), "auto": c.ExtractAuto})
	}
	a.ex.mu.Unlock()
	done, _ := st["done"].([]any)
	if done == nil {
		done = []any{}
	}
	if len(done) > 30 {
		done = done[:30]
	}
	return M{"available": sevenz() != "", "auto": c.ExtractAuto, "delete": c.ExtractDel, "items": items, "done": done, "pws": d.List,
		"tg": c.TgEnabled && c.TgPw}
}

func (a *App) exSetPassword(pw, only string) int {
	a.addPw(pw, false)
	n := 0
	st := a.exStore()
	notified := subMap(st, "notified")
	a.ex.mu.Lock()
	for _, e := range a.ex.m {
		if e.State == "passwort" && (only == "" || e.ID == only) {
			e.State, e.Force, e.PwHint, e.Msg, e.RetryAfter = "wartet", true, pw, "", 0
			delete(notified, e.Sig)
			n++
		}
	}
	a.save("extract.json", st)
	a.ex.mu.Unlock()
	return n
}

func (a *App) exRoutes(get, post func(string, http.HandlerFunc)) {
	get("/api/extract", func(w http.ResponseWriter, r *http.Request) { js(w, 200, a.exPayload()) })
	post("/api/extract/scan", func(w http.ResponseWriter, r *http.Request) { a.extractScan(); js(w, 200, a.exPayload()) })
	post("/api/extract/settings", func(w http.ResponseWriter, r *http.Request) {
		d := body(r)
		a.saveCfg(func(c *Config) { c.ExtractAuto, c.ExtractDel = d["auto"] == true, d["delete"] == true })
		js(w, 200, a.exPayload())
	})
	post("/api/extract/start", func(w http.ResponseWriter, r *http.Request) {
		id := str(body(r)["id"])
		a.ex.mu.Lock()
		if e := a.ex.m[id]; e != nil && (e.State == "wartet" || e.State == "fehler" || e.State == "unvollständig" || e.State == "passwort") {
			e.State, e.Force, e.RetryAfter, e.Msg = "wartet", true, 0, ""
		}
		a.ex.mu.Unlock()
		js(w, 200, a.exPayload())
	})
	post("/api/extract/cancel", func(w http.ResponseWriter, r *http.Request) {
		a.ex.mu.Lock()
		if e := a.ex.m[str(body(r)["id"])]; e != nil && e.State == "läuft" {
			e.Cancel = true
		}
		a.ex.mu.Unlock()
		js(w, 200, M{"ok": true})
	})
	post("/api/extract/dismiss", func(w http.ResponseWriter, r *http.Request) {
		id := str(body(r)["id"])
		st := a.exStore()
		a.ex.mu.Lock()
		if e := a.ex.m[id]; e != nil && e.State != "läuft" {
			subMap(st, "done_sigs")[e.Sig] = float64(time.Now().Unix())
			a.save("extract.json", st)
			delete(a.ex.m, id)
		}
		a.ex.mu.Unlock()
		js(w, 200, a.exPayload())
	})
	post("/api/extract/password", func(w http.ResponseWriter, r *http.Request) {
		d := body(r)
		pw := str(d["pw"])
		if strings.TrimSpace(pw) == "" {
			js(w, 400, M{"error": "Bitte ein Passwort eingeben."})
			return
		}
		n := a.exSetPassword(pw, str(d["id"]))
		p := a.exPayload()
		p["n"] = n
		js(w, 200, p)
	})
	post("/api/extract/password/delete", func(w http.ResponseWriter, r *http.Request) {
		i, ok := toInt(body(r)["i"])
		a.ex.mu.Lock()
		d := a.exPws()
		if ok && i >= 0 && i < len(d.List) {
			if d.Last == d.List[i] {
				d.Last = ""
			}
			d.List = append(d.List[:i], d.List[i+1:]...)
			a.save("passwords.json", d)
		}
		a.ex.mu.Unlock()
		js(w, 200, a.exPayload())
	})
	post("/api/hook", func(w http.ResponseWriter, r *http.Request) {
		pw := strings.TrimSpace(str(body(r)["password"]))
		n := 0
		if pw != "" {
			n = a.exSetPassword(pw, "")
		}
		a.extractScan()
		s := a.exSummary()
		s["ok"], s["retried"] = true, n
		js(w, 200, s)
	})
}

var _ = strconv.Itoa
