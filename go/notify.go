package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	tgBase      = strings.TrimRight(env("TG_API", "https://api.telegram.org"), "/")
	progressMin = time.Duration(atoi(env("PROGRESS_MIN_SECONDS", "20"))) * time.Second
	watchEvery  = time.Duration(atoi(env("WATCH_INTERVAL", "60"))) * time.Second
	alertMu     sync.Mutex
	rPwCmd      = regexp.MustCompile(`(?i)^/pw(@\w+)?(\s|$)`)
	rPwStrip    = regexp.MustCompile(`(?i)^/pw(@\w+)?\s*`)
)

func hsize(b float64) string {
	for _, u := range []string{"B", "KB", "MB", "GB", "TB"} {
		if b < 1024 || u == "TB" {
			if u == "B" {
				return fmt.Sprintf("%d B", int(b))
			}
			return fmt.Sprintf("%.1f %s", b, u)
		}
		b /= 1024
	}
	return ""
}

func hdur(sec float64) string {
	m, s := int(sec)/60, int(sec)%60
	if m > 0 {
		return fmt.Sprintf("%d min %d s", m, s)
	}
	return fmt.Sprintf("%d s", s)
}

func (a *App) uiLink(frag string) string {
	base := strings.TrimRight(a.cfg().PublicURL, "/")
	if base == "" {
		return ""
	}
	if frag != "" {
		return base + "/#" + frag
	}
	return base + "/"
}

func (a *App) linkLine(frag string) string {
	if l := a.uiLink(frag); l != "" {
		return "\n👉 " + l
	}
	return ""
}

// ---------- Telegram ----------

func (c Config) flag(kind string) bool {
	switch kind {
	case "tg_start":
		return c.TgStart
	case "tg_done":
		return c.TgDone
	case "tg_unclear":
		return c.TgUnclear
	case "tg_new":
		return c.TgNew
	case "tg_progress":
		return c.TgProgress
	case "tg_errors":
		return c.TgErrors
	case "tg_pw":
		return c.TgPw
	}
	return false
}

func tgPost(url string, payload any, timeout time.Duration, token string) (json.RawMessage, error) {
	b, _ := json.Marshal(payload)
	cl := &http.Client{Timeout: timeout}
	resp, err := cl.Post(url, "application/json", bytes.NewReader(b))
	scrub := func(s string) string { return strings.ReplaceAll(s, token, "***") }
	if err != nil {
		return nil, fmt.Errorf("%s", scrub(err.Error()))
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	var r struct {
		Result      json.RawMessage `json:"result"`
		Description string          `json:"description"`
	}
	json.Unmarshal(data, &r)
	if resp.StatusCode >= 400 {
		msg := r.Description
		if msg == "" {
			msg = resp.Status
		}
		return nil, fmt.Errorf("%s", scrub(msg))
	}
	return r.Result, nil
}

func (a *App) tgAPI(method string, payload any, timeout time.Duration) (json.RawMessage, error) {
	tok := a.cfg().TgToken
	return tgPost(fmt.Sprintf("%s/bot%s/%s", tgBase, tok, method), payload, timeout, tok)
}

func (a *App) tgSend(text, token, chat string, urgent bool) error {
	c := a.cfg()
	if token == "" {
		token = c.TgToken
	}
	if chat == "" {
		chat = c.TgChat
	}
	if token == "" || chat == "" {
		return fmt.Errorf("Bot-Token und Chat-ID fehlen")
	}
	if rs := []rune(text); len(rs) > 4000 {
		text = string(rs[:4000])
	}
	_, err := tgPost(fmt.Sprintf("%s/bot%s/sendMessage", tgBase, token), M{"chat_id": chat, "text": text,
		"disable_web_page_preview": true, "disable_notification": c.TgSilent && !urgent}, 10*time.Second, token)
	return err
}

func (a *App) notifyW(text, kind string, wait, urgent bool) {
	c := a.cfg()
	if !c.TgEnabled || !c.flag(kind) {
		return
	}
	send := func() {
		if err := a.tgSend(text, "", "", urgent); err != nil {
			fmt.Fprintln(os.Stderr, "Telegram-Fehler:", err)
		}
	}
	if wait {
		send()
	} else {
		go send()
	}
}

func (a *App) notify(text, kind string) { a.notifyW(text, kind, false, false) }

func (a *App) unclearNotice(unclear []string) {
	c := a.cfg()
	sig := ""
	if len(unclear) > 0 {
		h := md5.Sum([]byte(strings.Join(unclear, "|")))
		sig = hex.EncodeToString(h[:])
	}
	if sig == c.TgUnclearSig {
		return
	}
	if len(unclear) == 0 {
		a.saveCfg(func(c *Config) { c.TgUnclearSig = "" })
		return
	}
	if c.TgEnabled && c.TgUnclear {
		var lines []string
		for i, u := range unclear {
			if i < 8 {
				lines = append(lines, "• "+u)
			}
		}
		txt := fmt.Sprintf("🚨 Handlungsbedarf – MediaButler\n❓ %d Datei(en) brauchen eine Zuordnung, der Zeitplan überspringt sie.\n%s", len(unclear), strings.Join(lines, "\n"))
		if len(unclear) > 8 {
			txt += fmt.Sprintf("\n… und %d weitere", len(unclear)-8)
		}
		txt += "\nBitte im Reiter „Sortieren“ zuordnen." + a.linkLine("clarify")
		a.notifyW(txt, "tg_unclear", false, true)
		a.saveCfg(func(c *Config) { c.TgUnclearSig = sig })
	}
}

func (a *App) alertOnce(key string, down bool, textDown, textUp string) {
	alertMu.Lock()
	prev := a.alerts[key]
	a.alerts[key] = down
	alertMu.Unlock()
	if down && !prev {
		a.notifyW(textDown, "tg_errors", false, true)
	} else if prev && !down && textUp != "" {
		a.notify(textUp, "tg_errors")
	}
}

// ---------- Jellyfin ----------

func jfDo(url, path string, body []byte, key string, ctype string) (int, error) {
	req, _ := http.NewRequest("POST", strings.TrimRight(strings.TrimSpace(url), "/")+path, bytes.NewReader(body))
	req.Header.Set("X-Emby-Token", key)
	req.Header.Set("Authorization", fmt.Sprintf(`MediaBrowser Token="%s"`, key))
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

func jfResult(code int, err error, ok string) (bool, string) {
	if err == nil {
		return true, fmt.Sprintf(ok, code)
	}
	if code == 401 {
		return false, "API-Key ungültig (HTTP 401)"
	}
	if code != 0 {
		return false, fmt.Sprintf("HTTP %d", code)
	}
	return false, "nicht erreichbar: " + err.Error()
}

func jfRefresh(url, key string) (bool, string) {
	url = strings.TrimRight(strings.TrimSpace(url), "/")
	if url == "" || key == "" {
		return false, "Jellyfin-Adresse oder API-Key fehlt."
	}
	code, err := jfDo(url, "/Library/Refresh", nil, key, "")
	return jfResult(code, err, "Scan gestartet (HTTP %d)")
}

func jfUpdated(url, key string, paths []string) (bool, string) {
	ups := []M{}
	for _, p := range paths {
		ups = append(ups, M{"Path": p, "UpdateType": "Created"})
	}
	b, _ := json.Marshal(M{"Updates": ups})
	code, err := jfDo(url, "/Library/Media/Updated", b, key, "application/json")
	return jfResult(code, err, fmt.Sprintf("Teil-Scan für %d Ordner gestartet (HTTP %%d)", len(paths)))
}

func (a *App) jfAfterQueue() {
	c := a.cfg()
	a.mu.Lock()
	due := a.scanDue
	var rels []string
	for p := range a.scanPath {
		rels = append(rels, p)
	}
	a.scanDue, a.scanPath = false, map[string]bool{}
	a.mu.Unlock()
	sort.Strings(rels)
	if !c.JfScan || !due {
		return
	}
	var ok bool
	var msg string
	if c.JfMode == "partial" && len(rels) > 0 {
		root := strings.TrimRight(c.JfRoot, "/")
		if root == "" {
			root = a.target
		}
		var ps []string
		for _, r := range rels {
			ps = append(ps, root+"/"+r)
		}
		ok, msg = jfUpdated(c.JfURL, c.JfKey, ps)
		if !ok {
			ok2, msg2 := jfRefresh(c.JfURL, c.JfKey)
			if ok2 {
				msg2 = "voller Scan gestartet"
			}
			msg, ok = fmt.Sprintf("Teil-Scan fehlgeschlagen (%s); %s", msg, msg2), ok2
		}
	} else {
		ok, msg = jfRefresh(c.JfURL, c.JfKey)
	}
	a.saveCfg(func(c *Config) { c.JfLast = time.Now().Format("2006-01-02 15:04:05") + ": " + msg })
	if !ok {
		a.notify("⚠️ MediaButler: Jellyfin-Scan fehlgeschlagen – "+msg, "tg_errors")
	}
}

// ---------- Kopier-Lauf mit Meldungen ----------

type ordCount struct {
	keys []string
	n    map[string]int
}

func countCats(dsts []string) string {
	oc := ordCount{n: map[string]int{}}
	for _, d := range dsts {
		k := strings.SplitN(d, "/", 2)[0]
		if oc.n[k] == 0 {
			oc.keys = append(oc.keys, k)
		}
		oc.n[k]++
	}
	var p []string
	for _, k := range oc.keys {
		p = append(p, fmt.Sprintf("%s %d", k, oc.n[k]))
	}
	return strings.Join(p, " · ")
}

func (a *App) srcSize(rel string) int64 {
	if st, err := os.Stat(a.abs(rel)); err == nil {
		return st.Size()
	}
	return 0
}

func (a *App) runJob(items []item, source string) {
	f := acquire()
	if f == nil {
		a.mu.Lock()
		a.job.Errors = append(a.job.Errors, "Es läuft bereits ein anderer Kopiervorgang.")
		a.job.Running, a.job.Finished = false, true
		a.mu.Unlock()
		return
	}
	t0, c := time.Now(), a.cfg()
	sizes := map[string]int64{}
	var total int64
	dsts := []string{}
	for _, it := range items {
		s := a.srcSize(it.src)
		sizes[it.src] = s
		total += s
		dsts = append(dsts, it.dst)
	}
	if len(items) > 0 {
		a.notify(fmt.Sprintf("▶️ MediaButler: Kopieren gestartet (%s)\n%d Datei(en), %s\n%s", source, len(items), hsize(float64(total)), countCats(dsts)), "tg_start")
	}
	a.mu.Lock()
	a.marks, a.runT0 = []int{25, 50, 75}, t0
	a.mu.Unlock()
	ok := a.copyItems(items, source, c.MoveAfter)
	removed := 0
	moved := map[string]bool{}
	a.mu.Lock()
	for _, it := range ok {
		pp := strings.Split(it.dst, "/")
		if len(pp) >= 3 {
			a.scanPath[strings.Join(pp[:2], "/")] = true
		} else {
			a.scanPath[strings.Join(pp[:len(pp)-1], "/")] = true
		}
		a.scanDue = true
	}
	a.mu.Unlock()
	for _, it := range ok {
		if _, err := os.Stat(a.abs(it.src)); err != nil {
			moved[it.src] = true
		}
	}
	if len(moved) > 0 {
		np := map[string]PathOv{}
		for k, v := range a.getPaths() {
			if !moved[k] {
				np[k] = v
			}
		}
		a.setPaths(np)
		ng := map[string]Group{}
		for k, v := range a.getGroups() {
			if !moved[k] {
				ng[k] = v
			}
		}
		a.setGroups(ng)
	}
	if c.CleanupAfter {
		removed = a.cleanupLeftovers()
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
	_, unclear := a.scan()
	a.mu.Lock()
	errs := append([]string{}, a.job.Errors...)
	cancel, fd, ft := a.job.Cancel, a.job.FilesDone, a.job.FilesTotal
	a.mu.Unlock()
	res := fmt.Sprintf("%s: %d von %d Datei(en)", source, fd, ft)
	if len(errs) > 0 {
		res += fmt.Sprintf(", %d Fehler", len(errs))
	}
	if removed > 0 {
		res += fmt.Sprintf(", %d Ordner entfernt", removed)
	}
	if len(unclear) > 0 {
		res += fmt.Sprintf(", %d zur Klärung offen", len(unclear))
	}
	a.saveCfg(func(c *Config) { c.LastRun = time.Now().Format("2006-01-02 15:04:05"); c.LastResult = res })
	if len(items) == 0 {
		return
	}
	head := "✅ Fertig"
	if cancel {
		head = "⛔ Abgebrochen"
	} else if len(errs) > 0 {
		head = "⚠️ Fertig mit Fehlern"
	}
	var lines []string
	var okSize int64
	for i, it := range ok {
		okSize += sizes[it.src]
		if i < 10 {
			lines = append(lines, "• "+it.dst)
		}
	}
	if len(ok) > 10 {
		lines = append(lines, fmt.Sprintf("… und %d weitere", len(ok)-10))
	}
	txt := fmt.Sprintf("%s – MediaButler (%s)\n%d von %d Datei(en) übertragen, %s in %s\nZiel: %s\n%s", head, source, len(ok), len(items),
		hsize(float64(okSize)), hdur(time.Since(t0).Seconds()), a.target, strings.Join(lines, "\n"))
	if len(errs) > 0 {
		txt += "\nFehler:"
		for i, e := range errs {
			if i < 5 {
				txt += "\n– " + e
			}
		}
	}
	if len(unclear) > 0 {
		txt += fmt.Sprintf("\n❓ %d Datei(en) brauchen Klärung", len(unclear)) + a.linkLine("clarify")
	}
	if l := a.uiLink("hist"); l != "" {
		txt += "\n🗂 Verlauf: " + l
	}
	if len(errs) > 0 && c.TgErrors {
		a.notifyW("🚨 Handlungsbedarf – "+txt, "tg_errors", true, true)
	} else {
		a.notifyW(txt, "tg_done", true, false)
	}
}

func (a *App) progressCheck() {
	a.mu.Lock()
	if len(a.marks) == 0 || a.job.Total == 0 {
		a.mu.Unlock()
		return
	}
	pct := float64(a.job.Done) * 100 / float64(a.job.Total)
	var hit []int
	for len(a.marks) > 0 && pct >= float64(a.marks[0]) {
		hit = append(hit, a.marks[0])
		a.marks = a.marks[1:]
	}
	done, total, fd, ft, speed, src, t0 := a.job.Done, a.job.Total, a.job.FilesDone, a.job.FilesTotal, a.job.Speed, a.job.Source, a.runT0
	a.mu.Unlock()
	if len(hit) == 0 || time.Since(t0) < progressMin {
		return
	}
	m := hit[len(hit)-1]
	eta := 0.0
	if done > 0 {
		eta = time.Since(t0).Seconds() * float64(total-done) / float64(done)
	}
	txt := fmt.Sprintf("⏳ MediaButler: %d %% geschafft (%s)\n%d von %d Datei(en), %s von %s", m, src, fd, ft, hsize(float64(done)), hsize(float64(total)))
	if speed > 0 && eta > 0 {
		txt += fmt.Sprintf("\n%s/s, noch ca. %s", hsize(speed), hdur(eta))
	}
	a.notify(txt, "tg_progress")
}

// ---------- Wächter ----------

func (a *App) scheduleNext() string {
	c := a.cfg()
	if !c.Enabled {
		return ""
	}
	cr, err := NewCron(c.Cron)
	if err != nil {
		return "ungültiger Ausdruck"
	}
	if t, ok := cr.NextAfter(time.Now()); ok {
		return fmtDt(t)
	}
	return ""
}

func (a *App) announceNew(items []item) {
	var dsts, names []string
	var size int64
	for i, it := range items {
		dsts = append(dsts, it.dst)
		size += a.srcSize(it.src)
		if i < 6 {
			names = append(names, "• "+path.Base(it.src))
		}
	}
	if len(items) > 6 {
		names = append(names, fmt.Sprintf("… und %d weitere", len(items)-6))
	}
	when := "⏰ Zeitplan ist aus, bitte manuell starten"
	if n := a.scheduleNext(); a.cfg().Enabled && n != "" {
		when = "⏰ Nächster Lauf: " + n
	}
	txt := fmt.Sprintf("📥 MediaButler: %d neue Datei(en) bereit (%s)\n%s\n%s\n%s", len(items), hsize(float64(size)), countCats(dsts), strings.Join(names, "\n"), when)
	txt += a.linkLine("sort")
	a.notifyW(txt, "tg_new", true, false)
}

type watchState struct {
	lastNew map[string]bool
	stable  int
}

func sameSet(x, y map[string]bool) bool {
	if len(x) != len(y) {
		return false
	}
	for k := range x {
		if !y[k] {
			return false
		}
	}
	return true
}

func (a *App) watchOnce(st *watchState) {
	c := a.cfg()
	if !c.TgEnabled {
		st.lastNew, st.stable = map[string]bool{}, 0
		return
	}
	link := ""
	if l := a.uiLink("sched"); l != "" {
		link = "\n👉 " + l
	}
	ts, err := os.Stat(a.target)
	a.alertOnce("target", err != nil || !ts.IsDir(), fmt.Sprintf("🚨 Handlungsbedarf – MediaButler\nDas Ziel %s ist nicht erreichbar. Ist das NFS-Share eingebunden? Bis dahin wird nichts kopiert.%s", a.target, link),
		"✅ MediaButler: Das Ziel ist wieder erreichbar.")
	ss, err := os.Stat(a.source)
	srcOK := err == nil && ss.IsDir()
	a.alertOnce("source", !srcOK, fmt.Sprintf("🚨 Handlungsbedarf – MediaButler\nDer Quellordner %s ist nicht erreichbar.%s", a.source, link),
		"✅ MediaButler: Der Quellordner ist wieder erreichbar.")
	if !srcOK {
		return
	}
	items, unclear := a.scan()
	a.unclearNotice(unclear)
	keys := map[string]item{}
	for _, it := range items {
		keys[it.src+"|"+strconv.FormatInt(a.srcSize(it.src), 10)] = it
	}
	var seenF struct {
		Keys []string `json:"keys"`
	}
	a.load("seen.json", &seenF)
	seen := map[string]bool{}
	for _, k := range seenF.Keys {
		seen[k] = true
	}
	nw := map[string]bool{}
	for k := range keys {
		if !seen[k] {
			nw[k] = true
		}
	}
	sorted := func(m map[string]item) []string {
		l := []string{}
		for k := range m {
			l = append(l, k)
		}
		sort.Strings(l)
		return l
	}
	if len(nw) > 0 && c.TgNew {
		if sameSet(nw, st.lastNew) {
			st.stable++
		} else {
			st.lastNew, st.stable = nw, 0
		}
		if st.stable >= 1 {
			var l []string
			for k := range nw {
				l = append(l, k)
			}
			sort.Strings(l)
			var its []item
			for _, k := range l {
				its = append(its, keys[k])
			}
			a.announceNew(its)
			a.save("seen.json", M{"keys": sorted(keys)})
			st.lastNew, st.stable = map[string]bool{}, 0
		}
	} else {
		st.lastNew, st.stable = map[string]bool{}, 0
		cur := map[string]bool{}
		for k := range keys {
			cur[k] = true
		}
		if !sameSet(cur, seen) {
			a.save("seen.json", M{"keys": sorted(keys)})
		}
	}
}

func (a *App) watcher() {
	st := &watchState{lastNew: map[string]bool{}}
	for {
		time.Sleep(watchEvery)
		a.purgeSamples(false)
		a.watchOnce(st)
	}
}

// ---------- Passwort per Telegram ----------

func (a *App) exNotifyPw(e *exEntry) {
	a.ex.mu.Lock()
	st := a.exStore()
	notified := subMap(st, "notified")
	if _, ok := notified[e.Sig]; ok {
		a.ex.mu.Unlock()
		return
	}
	asked := false
	for _, o := range a.ex.m {
		if _, n := notified[o.Sig]; o != e && o.State == "passwort" && n {
			asked = true
		}
	}
	notified[e.Sig] = float64(time.Now().Unix())
	st["notified"] = capMap(notified, 200)
	a.save("extract.json", st)
	name, msg, id := e.Name, e.Msg, e.ID
	a.ex.mu.Unlock()
	c := a.cfg()
	if asked || !(c.TgEnabled && c.TgPw && c.TgToken != "" && c.TgChat != "") {
		return
	}
	txt := fmt.Sprintf("🔑 Passwort fehlt – MediaButler\n📦 %s\n%s\n\n👉 Antworte auf diese Nachricht mit dem Passwort oder schreibe /pw <Passwort>. Es wird gespeichert und deine Nachricht gelöscht.%s", name, msg, a.linkLine("ext"))
	res, err := a.tgAPI("sendMessage", M{"chat_id": c.TgChat, "text": txt, "reply_markup": M{"force_reply": true, "input_field_placeholder": "Passwort"}}, 10*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Telegram-Fehler:", err)
		return
	}
	var r struct {
		MessageID int `json:"message_id"`
	}
	json.Unmarshal(res, &r)
	a.ex.mu.Lock()
	st = a.exStore()
	pm := subMap(st, "pwmsgs")
	pm[strconv.Itoa(r.MessageID)] = id
	if len(pm) > 50 {
		keys := []string{}
		for k := range pm {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return atoi(keys[i]) < atoi(keys[j]) })
		for _, k := range keys[:len(pm)-50] {
			delete(pm, k)
		}
	}
	a.save("extract.json", st)
	a.ex.mu.Unlock()
}

func (a *App) tgHandle(m M) {
	c := a.cfg()
	chat, _ := m["chat"].(map[string]any)
	cid := strings.TrimSpace(c.TgChat)
	if str(chat["id"]) != cid && "@"+str(chat["username"]) != cid {
		return
	}
	text := strings.TrimSpace(str(m["text"]))
	var rid string
	if rm, ok := m["reply_to_message"].(map[string]any); ok {
		rid = str(rm["message_id"])
	}
	pw := ""
	if rid != "" {
		a.ex.mu.Lock()
		_, known := subMap(a.exStore(), "pwmsgs")[rid]
		a.ex.mu.Unlock()
		if known {
			pw = text
		}
	}
	if pw == "" {
		if !rPwCmd.MatchString(text) {
			return
		}
		pw = rPwStrip.ReplaceAllString(text, "")
	}
	reply := func(t string) { a.tgAPI("sendMessage", M{"chat_id": c.TgChat, "text": t}, 10*time.Second) }
	if strings.TrimSpace(pw) == "" {
		reply("🔑 Bitte so: /pw <Passwort> – oder direkt auf die Passwort-Nachricht antworten.")
		return
	}
	n := a.exSetPassword(pw, "")
	a.tgAPI("deleteMessage", M{"chat_id": c.TgChat, "message_id": m["message_id"]}, 10*time.Second)
	if n > 0 {
		reply(fmt.Sprintf("🔑 Passwort gespeichert – %d Archiv(e) werden erneut versucht.", n))
	} else {
		reply("🔑 Passwort gespeichert. Gerade wartet kein Archiv darauf.")
	}
}

func (a *App) tgPoller() {
	off := 0
	for {
		c := a.cfg()
		if !(c.TgEnabled && c.TgToken != "" && c.TgChat != "") || a.exSummary()["pw"].(int) == 0 {
			time.Sleep(5 * time.Second)
			continue
		}
		p := M{"timeout": 20, "allowed_updates": []string{"message"}}
		if off != 0 {
			p["offset"] = off
		}
		res, err := a.tgAPI("getUpdates", p, 30*time.Second)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Telegram-Abfrage:", err)
			time.Sleep(15 * time.Second)
			continue
		}
		var ups []M
		json.Unmarshal(res, &ups)
		for _, u := range ups {
			off = atoi(str(u["update_id"])) + 1
			if m, ok := u["message"].(map[string]any); ok {
				a.tgHandle(m)
			}
		}
		time.Sleep(time.Second)
	}
}

// ---------- Test-Endpunkte ----------

func (a *App) notifyRoutes(post func(string, http.HandlerFunc)) {
	post("/api/jellyfin/test", func(w http.ResponseWriter, r *http.Request) {
		d := body(r)
		c := a.cfg()
		url, key := str(d["url"]), strings.TrimSpace(str(d["key"]))
		if url == "" {
			url = c.JfURL
		}
		if key == "" {
			key = c.JfKey
		}
		ok, msg := jfRefresh(url, key)
		js(w, 200, M{"ok": ok, "msg": msg})
	})
	post("/api/telegram/test", func(w http.ResponseWriter, r *http.Request) {
		d := body(r)
		urgent := d["urgent"] == true
		head := "✅ Test: normale Meldung"
		if a.cfg().TgSilent {
			head += ", stumm gesendet."
		} else {
			head += "."
		}
		if urgent {
			head = "🚨 Test: dringende Meldung, kommt mit Ton."
		}
		err := a.tgSend(head+a.linkLineBase(), strings.TrimSpace(str(d["token"])), strings.TrimSpace(str(d["chat"])), urgent)
		if err != nil {
			js(w, 400, M{"error": err.Error()})
			return
		}
		js(w, 200, M{"ok": true})
	})
}

func (a *App) linkLineBase() string {
	if l := a.uiLink(""); l != "" {
		return "\n🌐 " + l
	}
	return ""
}
