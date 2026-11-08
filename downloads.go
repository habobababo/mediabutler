package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Download-Manager: HTTP/HTTPS-Links, ddownload.com (Premium per API-Key oder Login), OCH-Parts werden zu Paketen gruppiert.
// Cookies (Hoster-Login) werden dauerhaft in cookies.json gespeichert.

type DLItem struct {
	ID     int    `json:"id"`
	URL    string `json:"url"`
	Name   string `json:"name"`
	Pkg    string `json:"pkg"`
	Host   string `json:"host"`
	Status string `json:"status"` // wartet | lädt | fertig | fehler | pausiert
	Size   int64  `json:"size"`
	Done   int64  `json:"done"`
	Err    string `json:"err,omitempty"`
	Added  string `json:"added"`
	Speed  int64  `json:"speed"`
	Tries  int    `json:"-"`
}

type DLConf struct {
	Dir      string            `json:"dir"`
	Parallel int               `json:"parallel"`
	DDKey    string            `json:"dd_key"`
	DDLogin  string            `json:"dd_login"`
	DDPass   string            `json:"dd_pass"`
	Cookies  map[string]string `json:"cookies"` // manuell importierte Cookies je Host
}

type dlFile struct {
	Next  int       `json:"next"`
	Items []*DLItem `json:"items"`
}

type dlManager struct {
	a       *App
	mu      sync.Mutex
	f       dlFile
	cancel  map[int]context.CancelFunc
	wake    chan struct{}
	jar     *persistJar
	client  *http.Client
	noRedir *http.Client
	pkgDone map[string]bool
	opt     []string
	acc     []M
	accT    time.Time
}

var (
	ddBase   = strings.TrimRight(env("DDL_BASE", "https://ddownload.com"), "/")
	ddAPI    = strings.TrimRight(env("DDL_API", "https://api-v2.ddownload.com/api"), "/")
	reURL    = regexp.MustCompile(`https?://[^\s"'<>\\]+`)
	reDDCode = regexp.MustCompile(`(?:^|/)([a-zA-Z0-9]{12})(?:/|$|\.)`)
)

// ---------- Cookie-Jar mit Persistenz ----------

type pCookie struct {
	Name    string    `json:"name"`
	Value   string    `json:"value"`
	Domain  string    `json:"domain"`
	Path    string    `json:"path"`
	Expires time.Time `json:"expires"`
}

type persistJar struct {
	mu sync.Mutex
	a  *App
	m  map[string][]pCookie // Domain -> Cookies
}

func newJar(a *App) *persistJar {
	j := &persistJar{a: a, m: map[string][]pCookie{}}
	a.load("cookies.json", &j.m)
	if j.m == nil {
		j.m = map[string][]pCookie{}
	}
	return j
}

func (j *persistJar) SetCookies(u *url.URL, cs []*http.Cookie) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, c := range cs {
		dom := strings.TrimPrefix(strings.ToLower(c.Domain), ".")
		if dom == "" {
			dom = strings.ToLower(u.Hostname())
		}
		p := c.Path
		if p == "" {
			p = "/"
		}
		list := j.m[dom]
		out := list[:0:0]
		for _, o := range list {
			if o.Name != c.Name || o.Path != p {
				out = append(out, o)
			}
		}
		if c.MaxAge >= 0 && (c.Expires.IsZero() || c.Expires.After(time.Now())) && c.Value != "" {
			exp := c.Expires
			if c.MaxAge > 0 {
				exp = time.Now().Add(time.Duration(c.MaxAge) * time.Second)
			}
			if exp.IsZero() {
				exp = time.Now().AddDate(0, 0, 30) // Session-Cookies behalten wir 30 Tage
			}
			out = append(out, pCookie{c.Name, c.Value, dom, p, exp})
		}
		j.m[dom] = out
	}
	j.a.save("cookies.json", j.m)
}

func (j *persistJar) Cookies(u *url.URL) []*http.Cookie {
	j.mu.Lock()
	defer j.mu.Unlock()
	h := strings.ToLower(u.Hostname())
	var out []*http.Cookie
	for dom, list := range j.m {
		if h != dom && !strings.HasSuffix(h, "."+dom) {
			continue
		}
		for _, c := range list {
			if c.Expires.After(time.Now()) && strings.HasPrefix(u.Path+"/", strings.TrimSuffix(c.Path, "/")+"/") {
				out = append(out, &http.Cookie{Name: c.Name, Value: c.Value})
			}
		}
	}
	return out
}

func (j *persistJar) hosts() map[string]int {
	j.mu.Lock()
	defer j.mu.Unlock()
	r := map[string]int{}
	for d, l := range j.m {
		n := 0
		for _, c := range l {
			if c.Expires.After(time.Now()) {
				n++
			}
		}
		if n > 0 {
			r[d] = n
		}
	}
	return r
}

func (j *persistJar) clear(host string) {
	j.mu.Lock()
	delete(j.m, host)
	j.mu.Unlock()
	j.a.save("cookies.json", j.m)
}

// "a=b; c=d" -> Cookies für Host speichern
func (j *persistJar) importHeader(host, hdr string) {
	host = strings.ToLower(strings.TrimSpace(host))
	var cs []*http.Cookie
	for _, part := range strings.Split(hdr, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 && kv[0] != "" {
			cs = append(cs, &http.Cookie{Name: kv[0], Value: kv[1], Domain: host, Path: "/", Expires: time.Now().AddDate(1, 0, 0)})
		}
	}
	if host != "" && len(cs) > 0 {
		j.SetCookies(&url.URL{Scheme: "https", Host: host}, cs)
	}
}

// ---------- Konfiguration ----------

func (d *dlManager) conf() DLConf {
	c := DLConf{}
	d.a.load("dl_conf.json", &c)
	if c.Dir == "" {
		c.Dir = filepath.Join(d.a.source, "Downloads")
	}
	if c.Parallel < 1 {
		c.Parallel = 2
	}
	if c.Parallel > 8 {
		c.Parallel = 8
	}
	return c
}

func (a *App) dlInit() *dlManager {
	d := &dlManager{a: a, cancel: map[int]context.CancelFunc{}, wake: make(chan struct{}, 1), pkgDone: map[string]bool{}}
	d.jar = newJar(a)
	tr := &http.Transport{Proxy: http.ProxyFromEnvironment, ResponseHeaderTimeout: 60 * time.Second, TLSHandshakeTimeout: 20 * time.Second,
		DialContext: (&net.Dialer{Timeout: 20 * time.Second, KeepAlive: 30 * time.Second}).DialContext}
	d.client = &http.Client{Transport: tr, Jar: d.jar}
	d.noRedir = &http.Client{Transport: tr, Jar: d.jar, Timeout: 60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	a.load("downloads.json", &d.f)
	for _, it := range d.f.Items {
		if it.Status == "lädt" {
			it.Status = "wartet"
		}
		it.Speed = 0
	}
	if d.f.Next < 1 {
		d.f.Next = 1
	}
	for _, it := range d.f.Items {
		if it.ID >= d.f.Next {
			d.f.Next = it.ID + 1
		}
	}
	for host, h := range d.conf().Cookies {
		d.jar.importHeader(host, h)
	}
	go d.loop()
	return d
}

func (d *dlManager) saveLocked() { d.a.save("downloads.json", d.f) }

func (d *dlManager) poke() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// ---------- Namen, Pakete ----------

var (
	rePart1 = regexp.MustCompile(`(?i)\.part\d+\.(rar|zip|7z)$`)
	rePart2 = regexp.MustCompile(`(?i)\.(rar|zip|7z)\.\d{3}$`)
	rePart3 = regexp.MustCompile(`(?i)\.(r\d{2}|\d{3})$`)
	rePart4 = regexp.MustCompile(`(?i)\.(rar|zip|7z)$`)
	rePart5 = regexp.MustCompile(`(?i)\.part\d+$`)
	reBadFn = regexp.MustCompile(`[\x00-\x1f/\\:*?"<>|]`)
)

func pkgFromName(name string) string {
	for _, re := range []*regexp.Regexp{rePart1, rePart2, rePart3, rePart4, rePart5} {
		if re.MatchString(name) {
			return strings.TrimSpace(re.ReplaceAllString(name, ""))
		}
	}
	if e := path.Ext(name); len(e) >= 2 && len(e) <= 5 {
		name = strings.TrimSuffix(name, e)
	}
	return strings.TrimSpace(name)
}

func cleanFn(s string) string {
	s = strings.TrimSpace(reBadFn.ReplaceAllString(s, "_"))
	s = strings.Trim(s, ". ")
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

func nameFromURL(u string) string {
	pu, err := url.Parse(u)
	if err != nil {
		return ""
	}
	seg := path.Base(strings.TrimRight(pu.Path, "/"))
	if v, err := url.PathUnescape(seg); err == nil {
		seg = v
	}
	seg = strings.TrimSuffix(strings.TrimSuffix(seg, ".html"), ".htm")
	if seg == "." || seg == "/" || reDDCode.MatchString("/"+seg) && len(seg) == 12 {
		return ""
	}
	return cleanFn(seg)
}

func (d *dlManager) add(links []string, pkg string) (n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	have := map[string]bool{}
	for _, it := range d.f.Items {
		have[it.URL] = true
	}
	for _, l := range links {
		l = strings.TrimSpace(html.UnescapeString(l))
		l = strings.TrimRight(l, ".,;)")
		pu, err := url.Parse(l)
		if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Host == "" || have[l] {
			continue
		}
		have[l] = true
		name := nameFromURL(l)
		p := cleanFn(pkg)
		if p == "" {
			p = pkgFromName(name)
		}
		if p == "" {
			p = "Unbenannt"
		}
		d.f.Items = append(d.f.Items, &DLItem{ID: d.f.Next, URL: l, Name: name, Pkg: p, Host: strings.TrimPrefix(strings.ToLower(pu.Hostname()), "www."),
			Status: "wartet", Added: time.Now().Format("2006-01-02 15:04:05")})
		d.f.Next++
		n++
	}
	d.saveLocked()
	d.poke()
	return
}

// ---------- Auflösen (ddownload & Co.) ----------

type resolved struct {
	url  string
	size int64
	name string
}

func isDD(host string) bool {
	return strings.HasSuffix(host, "ddownload.com") || strings.HasSuffix(host, "ddl.to")
}

func (d *dlManager) get(ctx context.Context, c *http.Client, u string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) MediaButler")
	return c.Do(req)
}

func (d *dlManager) ddLogin(ctx context.Context, conf DLConf) error {
	if conf.DDLogin == "" || conf.DDPass == "" {
		return fmt.Errorf("Kein ddownload-Login hinterlegt.")
	}
	form := url.Values{"op": {"login"}, "redirect": {""}, "login": {conf.DDLogin}, "password": {conf.DDPass}}
	req, _ := http.NewRequestWithContext(ctx, "POST", ddBase+"/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) MediaButler")
	resp, err := d.noRedir.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	u, _ := url.Parse(ddBase)
	for _, c := range d.jar.Cookies(u) {
		if c.Name == "xfss" {
			return nil
		}
	}
	return fmt.Errorf("ddownload-Login fehlgeschlagen. Zugangsdaten prüfen.")
}

var (
	reDirect = regexp.MustCompile(`https?://[\w.\-]+(?::\d+)?/d/[\w\-]+/[^"'\s<>]+`)
	reInput  = regexp.MustCompile(`(?is)<input[^>]*>`)
	reAttr   = regexp.MustCompile(`(?is)(name|value)\s*=\s*["']([^"']*)["']`)
)

func (d *dlManager) resolveDD(ctx context.Context, it *DLItem, conf DLConf) (*resolved, error) {
	pu, _ := url.Parse(it.URL)
	m := reDDCode.FindStringSubmatch(pu.Path)
	if m == nil {
		return nil, fmt.Errorf("Datei-Code in der ddownload-URL nicht erkannt.")
	}
	code := m[1]
	var errs []string
	if conf.DDKey != "" {
		r, err := d.ddViaAPI(ctx, conf.DDKey, code)
		if err == nil {
			return r, nil
		}
		errs = append(errs, err.Error())
		// Datei gehört nicht zum eigenen Account ("No file"): erst Klon im eigenen Account, dann Login-Weg
		cr, err2 := d.ddViaClone(ctx, conf.DDKey, code)
		if err2 == nil {
			return cr, nil
		}
		errs = append(errs, "Klon: "+err2.Error())
	}
	if conf.DDLogin != "" && conf.DDPass != "" || conf.DDKey == "" {
		r, err := d.ddViaWeb(ctx, code, conf)
		if err == nil {
			return r, nil
		}
		errs = append(errs, "Login-Weg: "+err.Error())
	} else {
		errs = append(errs, "kein Login hinterlegt")
	}
	return nil, fmt.Errorf("%s", strings.Join(errs, " | "))
}

func (d *dlManager) ddViaWeb(ctx context.Context, code string, conf DLConf) (*resolved, error) {
	// Login/Cookies: Seite laden, Direktlink suchen
	u, _ := url.Parse(ddBase)
	has := false
	for _, c := range d.jar.Cookies(u) {
		if c.Name == "xfss" || c.Name == "login" {
			has = true
		}
	}
	if !has {
		if err := d.ddLogin(ctx, conf); err != nil {
			return nil, err
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		page := ddBase + "/" + code
		resp, err := d.get(ctx, d.noRedir, page)
		if err != nil {
			return nil, err
		}
		if loc := resp.Header.Get("Location"); resp.StatusCode/100 == 3 && loc != "" {
			resp.Body.Close()
			if lu, err := url.Parse(loc); err == nil && reDirect.MatchString(lu.String()) || strings.Contains(loc, "/d/") {
				return &resolved{url: loc}, nil
			}
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		resp.Body.Close()
		txt := string(b)
		if mm := reDirect.FindString(txt); mm != "" {
			return &resolved{url: html.UnescapeString(mm)}, nil
		}
		// Formular download2 absenden
		if strings.Contains(txt, "download2") || strings.Contains(txt, "method_premium") {
			form := url.Values{}
			for _, in := range reInput.FindAllString(txt, -1) {
				nm, val := "", ""
				for _, at := range reAttr.FindAllStringSubmatch(in, -1) {
					if strings.EqualFold(at[1], "name") {
						nm = at[2]
					} else {
						val = at[2]
					}
				}
				if nm != "" {
					form.Set(nm, val)
				}
			}
			form.Set("op", "download2")
			form.Set("method_premium", "1")
			req, _ := http.NewRequestWithContext(ctx, "POST", page, strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) MediaButler")
			req.Header.Set("Referer", page)
			r2, err := d.noRedir.Do(req)
			if err != nil {
				return nil, err
			}
			if loc := r2.Header.Get("Location"); loc != "" {
				r2.Body.Close()
				return &resolved{url: loc}, nil
			}
			b2, _ := io.ReadAll(io.LimitReader(r2.Body, 2<<20))
			r2.Body.Close()
			if mm := reDirect.FindString(string(b2)); mm != "" {
				return &resolved{url: html.UnescapeString(mm)}, nil
			}
		}
		if attempt == 0 && conf.DDLogin != "" { // Session abgelaufen -> neu einloggen
			d.jar.clear(strings.TrimPrefix(u.Hostname(), "www."))
			if err := d.ddLogin(ctx, conf); err != nil {
				return nil, err
			}
			continue
		}
	}
	return nil, fmt.Errorf("Kein Premium-Direktlink gefunden. Account oder API-Key prüfen.")
}

func (d *dlManager) apiJSON(ctx context.Context, u string, v any) error {
	resp, err := d.get(ctx, d.noRedir, u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("ungültige Antwort der API (HTTP %d)", resp.StatusCode)
	}
	return nil
}

func anyInt(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case string:
		var n int64
		fmt.Sscan(x, &n)
		return n
	}
	return 0
}

// ---------- Download-Schleife ----------

func (d *dlManager) loop() {
	t := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-d.wake:
		case <-t.C:
		}
		d.mu.Lock()
		running := 0
		for _, it := range d.f.Items {
			if it.Status == "lädt" {
				running++
			}
		}
		par := d.conf().Parallel
		for _, it := range d.f.Items {
			if running >= par {
				break
			}
			if it.Status == "wartet" {
				it.Status, it.Err = "lädt", ""
				ctx, cancel := context.WithCancel(context.Background())
				d.cancel[it.ID] = cancel
				running++
				go d.run(ctx, it)
			}
		}
		d.saveLocked()
		d.mu.Unlock()
	}
}

func (d *dlManager) setState(it *DLItem, f func()) {
	d.mu.Lock()
	f()
	d.mu.Unlock()
}

func (d *dlManager) fail(it *DLItem, ctx context.Context, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.cancel, it.ID)
	it.Speed = 0
	if ctx.Err() != nil {
		if it.Status == "lädt" {
			it.Status = "pausiert"
		}
		d.saveLocked()
		return
	}
	it.Tries++
	if it.Tries < 2 {
		it.Status = "wartet"
		it.Err = err.Error()
	} else {
		it.Status, it.Err = "fehler", err.Error()
		d.a.notify(fmt.Sprintf("⚠️ Download fehlgeschlagen: %s\n%s", firstNonEmpty(it.Name, it.URL), err.Error()), "tg_errors")
	}
	d.saveLocked()
	d.poke()
}

func (d *dlManager) run(ctx context.Context, it *DLItem) {
	conf := d.conf()
	for host, h := range conf.Cookies {
		d.jar.importHeader(host, h)
	}
	target, name, size := it.URL, it.Name, int64(0)
	if isDD(it.Host) {
		r, err := d.resolveDD(ctx, it, conf)
		if err != nil {
			d.fail(it, ctx, err)
			return
		}
		target, size = r.url, r.size
		if r.name != "" {
			name = cleanFn(r.name)
		}
	}
	dir := filepath.Join(conf.Dir, cleanFn(it.Pkg))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		d.fail(it, ctx, err)
		return
	}
	part := filepath.Join(dir, firstNonEmpty(name, fmt.Sprintf("download-%d", it.ID))+".mbdl")
	var have int64
	if st, err := os.Stat(part); err == nil {
		have = st.Size()
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) MediaButler")
	if have > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", have))
	}
	resp, err := d.client.Do(req)
	if err != nil {
		d.fail(it, ctx, err)
		return
	}
	defer resp.Body.Close()
	flag := os.O_CREATE | os.O_WRONLY
	switch {
	case resp.StatusCode == 206:
		flag |= os.O_APPEND
	case resp.StatusCode == 416:
		have = 0
		os.Remove(part)
		d.fail(it, ctx, fmt.Errorf("Wiederaufnahme nicht möglich, starte neu."))
		return
	case resp.StatusCode == 200:
		have = 0
		flag |= os.O_TRUNC
	default:
		d.fail(it, ctx, fmt.Errorf("HTTP %d", resp.StatusCode))
		return
	}
	if strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") {
		d.fail(it, ctx, fmt.Errorf("Der Server lieferte eine Webseite statt der Datei (Link abgelaufen oder Login nötig)."))
		return
	}
	if name == "" {
		if cd := resp.Header.Get("Content-Disposition"); cd != "" {
			if m := regexp.MustCompile(`(?i)filename\*?=(?:UTF-8'')?"?([^";]+)"?`).FindStringSubmatch(cd); m != nil {
				v, _ := url.PathUnescape(m[1])
				name = cleanFn(v)
			}
		}
		if name == "" {
			name = cleanFn(path.Base(resp.Request.URL.Path))
		}
		if name == "" || name == "." {
			name = fmt.Sprintf("download-%d", it.ID)
		}
		np := filepath.Join(dir, name+".mbdl")
		if np != part && have == 0 {
			part = np
		}
	}
	total := have + resp.ContentLength
	if resp.ContentLength < 0 {
		total = size
	}
	d.setState(it, func() {
		if it.Pkg == "Unbenannt" || it.Pkg == "" || (it.Pkg == pkgFromName(it.Name) && path.Ext(it.Name) == "") {
			it.Pkg = pkgFromName(name)
		}
		it.Name, it.Size, it.Done = name, total, have
	})
	f, err := os.OpenFile(part, flag, 0o644)
	if err != nil {
		d.fail(it, ctx, err)
		return
	}
	buf := make([]byte, 256<<10)
	lastT, lastB := time.Now(), have
	cur := have
	var werr error
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr = f.Write(buf[:n]); werr != nil {
				break
			}
			cur += int64(n)
			if time.Since(lastT) >= time.Second {
				sp := int64(float64(cur-lastB) / time.Since(lastT).Seconds())
				c2 := cur
				d.setState(it, func() { it.Done, it.Speed = c2, sp })
				lastT, lastB = time.Now(), cur
			}
		}
		if rerr != nil {
			if rerr != io.EOF {
				werr = rerr
			}
			break
		}
	}
	f.Sync()
	f.Close()
	if werr != nil {
		d.fail(it, ctx, werr)
		return
	}
	if total > 0 && cur < total {
		d.fail(it, ctx, fmt.Errorf("Verbindung vorzeitig beendet (%d von %d Bytes).", cur, total))
		return
	}
	final := strings.TrimSuffix(part, ".mbdl")
	if err := os.Rename(part, final); err != nil {
		d.fail(it, ctx, err)
		return
	}
	d.mu.Lock()
	delete(d.cancel, it.ID)
	it.Status, it.Done, it.Size, it.Speed, it.Err = "fertig", cur, cur, 0, ""
	pkgFin := true
	for _, o := range d.f.Items {
		if o.Pkg == it.Pkg && o.Status != "fertig" {
			pkgFin = false
		}
	}
	notify := pkgFin && !d.pkgDone[it.Pkg]
	if notify {
		d.pkgDone[it.Pkg] = true
	}
	d.saveLocked()
	d.mu.Unlock()
	d.poke()
	if notify {
		d.a.notify(fmt.Sprintf("⬇️ Download fertig: %s", it.Pkg), "tg_done")
	}
}

// ---------- Click'n'Load ----------

func cnlDecrypt(crypted, jk string) ([]string, error) {
	m := regexp.MustCompile(`(?i)return\s*['"]?([0-9a-f]{32})['"]?`).FindStringSubmatch(jk)
	if m == nil {
		m = regexp.MustCompile(`(?i)['"]([0-9a-f]{32})['"]`).FindStringSubmatch(jk)
	}
	if m == nil {
		return nil, fmt.Errorf("Schlüssel nicht erkannt")
	}
	key, _ := hex.DecodeString(m[1])
	raw, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(strings.TrimSpace(crypted), " ", "+"))
	if err != nil || len(raw) == 0 || len(raw)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("Daten nicht lesbar")
	}
	blk, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(raw))
	cipher.NewCBCDecrypter(blk, key).CryptBlocks(out, raw)
	txt := strings.Trim(string(out), "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10")
	return reURL.FindAllString(txt, -1), nil
}

func cors(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "*")
	h.Set("Access-Control-Allow-Private-Network", "true")
}

func (d *dlManager) cnlHandlers(mux *http.ServeMux, prefix string) {
	h := func(p string, f http.HandlerFunc) {
		mux.HandleFunc(prefix+p, func(w http.ResponseWriter, r *http.Request) {
			cors(w)
			if r.Method == "OPTIONS" {
				w.WriteHeader(204)
				return
			}
			f(w, r)
		})
	}
	h("/", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("JDownloader")) })
	h("/jdcheck.js", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/javascript")
		w.Write([]byte("jdownloader=true;\nvar version='17461';\n"))
	})
	h("/crossdomain.xml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/xml")
		w.Write([]byte(`<?xml version="1.0"?><cross-domain-policy><allow-access-from domain="*" /></cross-domain-policy>`))
	})
	h("/add", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		links := reURL.FindAllString(strings.ReplaceAll(r.FormValue("urls"), "\r", "\n"), -1)
		n := d.add(links, r.FormValue("package"))
		w.Write([]byte(fmt.Sprintf("success %d", n)))
	})
	h("/addcrypted2", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		links, err := cnlDecrypt(r.FormValue("crypted"), r.FormValue("jk"))
		if err != nil {
			http.Error(w, "failed "+err.Error(), 400)
			return
		}
		pkg := firstNonEmpty(r.FormValue("package"), r.FormValue("source"))
		if pu, err := url.Parse(pkg); err == nil && pu.Host != "" {
			pkg = "" // Quelle ist nur eine URL, nicht aussagekräftig
		}
		n := d.add(links, pkg)
		d.a.notify(fmt.Sprintf("📥 Click'n'Load: %d Link(s) hinzugefügt", n), "tg_new")
		w.Write([]byte("success"))
	})
	h("/flash", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("JDownloader")) })
}

func (d *dlManager) cnlServer() {
	port := env("CNL_PORT", "9666")
	if port == "0" || port == "" {
		return
	}
	mux := http.NewServeMux()
	d.cnlHandlers(mux, "/flash")
	mux.HandleFunc("/jdcheck.js", func(w http.ResponseWriter, r *http.Request) {
		cors(w)
		w.Header().Set("Content-Type", "text/javascript")
		w.Write([]byte("jdownloader=true;\nvar version='17461';\n"))
	})
	go func() {
		if err := http.ListenAndServe(":"+port, mux); err != nil {
			fmt.Fprintln(os.Stderr, "Click'n'Load-Port nicht verfügbar:", err)
		}
	}()
}

// ---------- API ----------

func (d *dlManager) view() M {
	d.mu.Lock()
	defer d.mu.Unlock()
	type pk struct {
		Name               string
		Items              []*DLItem
		Size, Done, Speed  int64
		nf, nd, nl, ne, np int
	}
	idx := map[string]*pk{}
	var order []*pk
	for _, it := range d.f.Items {
		p := idx[it.Pkg]
		if p == nil {
			p = &pk{Name: it.Pkg}
			idx[it.Pkg] = p
			order = append(order, p)
		}
		p.Items = append(p.Items, it)
		p.Size += it.Size
		p.Done += it.Done
		p.Speed += it.Speed
		switch it.Status {
		case "fertig":
			p.nd++
		case "lädt":
			p.nl++
		case "fehler":
			p.ne++
		case "pausiert":
			p.np++
		}
	}
	pkgs := []M{}
	active := 0
	for _, p := range order {
		sort.SliceStable(p.Items, func(i, j int) bool { return p.Items[i].Name < p.Items[j].Name })
		st := "wartet"
		switch {
		case p.nd == len(p.Items):
			st = "fertig"
		case p.nl > 0:
			st = "lädt"
		case p.ne > 0:
			st = "fehler"
		case p.np == len(p.Items):
			st = "pausiert"
		}
		if st == "lädt" {
			active++
		}
		pkgs = append(pkgs, M{"name": p.Name, "items": p.Items, "size": p.Size, "done": p.Done, "speed": p.Speed, "status": st, "finished": p.nd, "total": len(p.Items)})
	}
	return M{"packages": pkgs, "active": active}
}

func (d *dlManager) routes(get, post func(string, http.HandlerFunc)) {
	get("/api/dl", func(w http.ResponseWriter, r *http.Request) {
		c := d.conf()
		v := d.view()
		v["conf"] = M{"dir": c.Dir, "parallel": c.Parallel, "dd_key_set": c.DDKey != "", "dd_login": c.DDLogin, "dd_pass_set": c.DDPass != "",
			"cookie_hosts": d.jar.hosts(), "cnl_port": env("CNL_PORT", "9666")}
		js(w, 200, v)
	})
	get("/api/dl/accounts", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
		defer cancel()
		js(w, 200, M{"accounts": d.accounts(ctx, r.URL.Query().Get("force") == "1")})
	})
	post("/api/dl/add", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		var links []string
		switch x := b["links"].(type) {
		case string:
			links = reURL.FindAllString(x, -1)
		case []any:
			links = strs(x, 2000)
		}
		n := d.add(links, str(b["package"]))
		js(w, 200, M{"added": n})
	})
	post("/api/dl/action", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		act, pkg := str(b["action"]), str(b["pkg"])
		id, _ := toInt(b["id"])
		d.mu.Lock()
		keep := d.f.Items[:0:0]
		for _, it := range d.f.Items {
			hit := (id > 0 && it.ID == id) || (id == 0 && pkg != "" && it.Pkg == pkg) || (act == "clear_done" && it.Status == "fertig")
			if !hit {
				keep = append(keep, it)
				continue
			}
			switch act {
			case "pause":
				if it.Status == "lädt" || it.Status == "wartet" {
					if c := d.cancel[it.ID]; c != nil {
						c()
					}
					it.Status = "pausiert"
				}
			case "resume", "retry":
				if it.Status != "fertig" && it.Status != "lädt" {
					it.Status, it.Err, it.Tries = "wartet", "", 0
				}
			case "remove", "clear_done":
				if c := d.cancel[it.ID]; c != nil {
					c()
				}
				continue
			}
			keep = append(keep, it)
		}
		d.f.Items = keep
		d.saveLocked()
		d.mu.Unlock()
		d.poke()
		js(w, 200, M{"ok": true})
	})
	post("/api/dl/group", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		target := cleanFn(str(b["target"]))
		if target == "" {
			js(w, 400, M{"error": "Bitte einen Paketnamen angeben."})
			return
		}
		pk := map[string]bool{}
		for _, p := range strs(b["pkgs"], 500) {
			pk[p] = true
		}
		ids := map[int]bool{}
		if l, ok := b["ids"].([]any); ok {
			for _, x := range l {
				if n, ok := toInt(x); ok {
					ids[n] = true
				}
			}
		}
		conf := d.conf()
		d.mu.Lock()
		defer d.mu.Unlock()
		var sel []*DLItem
		for _, it := range d.f.Items {
			if (pk[it.Pkg] || ids[it.ID]) && it.Pkg != target {
				if it.Status == "lädt" {
					js(w, 400, M{"error": "Laufende Downloads bitte erst pausieren."})
					return
				}
				sel = append(sel, it)
			}
		}
		moved := 0
		for _, it := range sel {
			oldDir := filepath.Join(conf.Dir, cleanFn(it.Pkg))
			newDir := filepath.Join(conf.Dir, target)
			if it.Name != "" {
				os.MkdirAll(newDir, 0o755)
				for _, fn := range []string{it.Name, it.Name + ".mbdl"} {
					src, dst := filepath.Join(oldDir, fn), filepath.Join(newDir, fn)
					if _, err := os.Stat(src); err == nil {
						if _, err := os.Stat(dst); err != nil {
							os.Rename(src, dst)
						}
					}
				}
				os.Remove(oldDir) // nur wenn leer
			}
			it.Pkg = target
			moved++
		}
		delete(d.pkgDone, target)
		d.saveLocked()
		js(w, 200, M{"ok": true, "moved": moved})
	})
	post("/api/dl/config", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		c := d.conf()
		old := DLConf{}
		d.a.load("dl_conf.json", &old)
		if v := strings.TrimSpace(str(b["dir"])); v != "" {
			c.Dir = v
		}
		if n, ok := toInt(b["parallel"]); ok && n > 0 {
			c.Parallel = n
		}
		if _, ok := b["dd_login"]; ok {
			c.DDLogin = strings.TrimSpace(str(b["dd_login"]))
		}
		if v := strings.TrimSpace(str(b["dd_key"])); v != "" {
			c.DDKey = v
		}
		if v := str(b["dd_pass"]); v != "" {
			c.DDPass = v
		}
		if b["dd_key_clear"] == true {
			c.DDKey = ""
		}
		if ck, ok := b["cookies"].(map[string]any); ok {
			c.Cookies = map[string]string{}
			for h, v := range ck {
				if s := strings.TrimSpace(str(v)); s != "" {
					c.Cookies[strings.ToLower(strings.TrimSpace(h))] = s
				}
			}
		}
		if c.Cookies == nil {
			c.Cookies = old.Cookies
		}
		d.a.save("dl_conf.json", c)
		d.mu.Lock()
		d.acc = nil
		d.mu.Unlock()
		for h, v := range c.Cookies {
			d.jar.importHeader(h, v)
		}
		d.poke()
		js(w, 200, M{"ok": true})
	})
	post("/api/dl/login", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 40*time.Second)
		defer cancel()
		c := d.conf()
		if c.DDKey != "" {
			var info struct {
				Status int            `json:"status"`
				Msg    string         `json:"msg"`
				Result map[string]any `json:"result"`
			}
			if err := d.apiJSON(ctx, fmt.Sprintf("%s/account/info?key=%s", ddAPI, url.QueryEscape(c.DDKey)), &info); err != nil || info.Status != 200 {
				js(w, 200, M{"ok": false, "error": firstNonEmpty(info.Msg, "API-Key ungültig oder nicht erreichbar")})
				return
			}
			js(w, 200, M{"ok": true, "info": "API-Key ok" + premiumInfo(info.Result)})
			return
		}
		if err := d.ddLogin(ctx, c); err != nil {
			js(w, 200, M{"ok": false, "error": err.Error()})
			return
		}
		js(w, 200, M{"ok": true, "info": "Login ok, Cookies gespeichert"})
	})
	post("/api/dl/diagnose", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		js(w, 200, M{"steps": d.diagnose(ctx, str(body(r)["url"]))})
	})
	post("/api/dl/cookies/clear", func(w http.ResponseWriter, r *http.Request) {
		d.jar.clear(strings.ToLower(str(body(r)["host"])))
		js(w, 200, M{"ok": true})
	})
	// Click'n'Load über die normale Weboberfläche (z. B. vom Chrome-Plugin umgeleitet)
	mux := http.NewServeMux()
	d.cnlHandlers(mux, "/api/cnl")
	cnl := func(w http.ResponseWriter, r *http.Request) { mux.ServeHTTP(w, r) }
	for _, p := range []string{"/", "/jdcheck.js", "/add", "/addcrypted2", "/flash", "/crossdomain.xml"} {
		get("/api/cnl"+p, cnl)
		post("/api/cnl"+p, cnl)
		d.opt = append(d.opt, "/api/cnl"+p)
	}
}

// premiumInfo wertet die Felder der ddownload-API tolerant aus (premium_expire, premium bool/Zahl/Text).
func premiumInfo(res map[string]any) string {
	if exp := strings.TrimSpace(fmt.Sprint(res["premium_expire"])); res["premium_expire"] != nil && exp != "" && exp != "0" && exp != "<nil>" {
		for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02", time.RFC3339} {
			if t, err := time.ParseInLocation(layout, exp, time.UTC); err == nil {
				if t.After(time.Now()) {
					return ", Premium aktiv bis " + t.Format("02.01.2006")
				}
				return ", Premium abgelaufen am " + t.Format("02.01.2006")
			}
		}
		return ", Premium bis " + exp
	}
	switch v := res["premium"].(type) {
	case bool:
		if v {
			return ", Premium aktiv"
		}
	case float64:
		if v > 0 {
			return ", Premium aktiv"
		}
	case string:
		if v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes") {
			return ", Premium aktiv"
		}
	}
	if anyInt(res["premium_traffic"]) > 0 || anyInt(res["traffic_left"]) > 0 {
		return ", Premium-Traffic vorhanden"
	}
	return " (die API meldet keinen Premium-Status, Direktlinks werden trotzdem versucht)"
}

// accounts: aktive Hoster-Accounts mit Premium-Status (5 Minuten gecacht)
func (d *dlManager) accounts(ctx context.Context, force bool) []M {
	d.mu.Lock()
	if !force && d.acc != nil && time.Since(d.accT) < 5*time.Minute {
		a := d.acc
		d.mu.Unlock()
		return a
	}
	d.mu.Unlock()
	c := d.conf()
	out := []M{}
	if c.DDKey != "" {
		var info struct {
			Status int            `json:"status"`
			Msg    string         `json:"msg"`
			Result map[string]any `json:"result"`
		}
		e := d.apiJSON(ctx, fmt.Sprintf("%s/account/info?key=%s", ddAPI, url.QueryEscape(c.DDKey)), &info)
		acc := M{"host": "ddownload.com", "mode": "API-Key"}
		if e != nil || info.Status != 200 {
			acc["ok"], acc["text"] = false, firstNonEmpty(info.Msg, "nicht erreichbar oder Key ungültig")
		} else {
			acc["ok"], acc["text"] = true, strings.TrimLeft(premiumInfo(info.Result), ", ")
			acc["premium"] = !strings.Contains(premiumInfo(info.Result), "abgelaufen") && !strings.Contains(premiumInfo(info.Result), "keinen Premium")
			if v := info.Result["traffic_left"]; v != nil {
				acc["traffic"] = v
			}
			if v := info.Result["premium_expire"]; v != nil {
				acc["expire"] = fmt.Sprint(v)
			}
		}
		out = append(out, acc)
	} else if c.DDLogin != "" {
		u, _ := url.Parse(ddBase)
		n := 0
		for _, ck := range d.jar.Cookies(u) {
			if ck.Name == "xfss" {
				n++
			}
		}
		acc := M{"host": "ddownload.com", "mode": "Login " + c.DDLogin, "ok": n > 0, "premium": nil}
		if n > 0 {
			acc["text"] = "Login aktiv, Cookies gespeichert (Premium-Status per Login nicht abfragbar, API-Key empfohlen)"
		} else {
			acc["text"] = "noch nicht angemeldet, wird beim ersten Download eingeloggt"
		}
		out = append(out, acc)
	}
	d.mu.Lock()
	d.acc, d.accT = out, time.Now()
	d.mu.Unlock()
	return out
}

type ddDirect struct {
	Msg    string `json:"msg"`
	Status int    `json:"status"`
	Result struct {
		URL  string `json:"url"`
		Size any    `json:"size"`
	} `json:"result"`
}

func (d *dlManager) ddViaAPI(ctx context.Context, key, code string) (*resolved, error) {
	var dl ddDirect
	if err := d.apiJSON(ctx, fmt.Sprintf("%s/file/direct_link?key=%s&file_code=%s", ddAPI, url.QueryEscape(key), code), &dl); err != nil {
		return nil, err
	}
	if dl.Result.URL == "" {
		return nil, fmt.Errorf("ddownload API (%s): %s", code, firstNonEmpty(dl.Msg, "kein Link erhalten"))
	}
	r := &resolved{url: dl.Result.URL, size: anyInt(dl.Result.Size)}
	var info struct {
		Result []struct {
			Name string
			Size any
		} `json:"result"`
	}
	if d.apiJSON(ctx, fmt.Sprintf("%s/file/info?key=%s&file_code=%s", ddAPI, url.QueryEscape(key), code), &info) == nil && len(info.Result) > 0 {
		r.name = info.Result[0].Name
		if r.size == 0 {
			r.size = anyInt(info.Result[0].Size)
		}
	}
	return r, nil
}

// ddViaClone klont eine fremde Datei in den eigenen Account und holt den Direktlink des Klons.
func (d *dlManager) ddViaClone(ctx context.Context, key, code string) (*resolved, error) {
	var cl struct {
		Status int            `json:"status"`
		Msg    string         `json:"msg"`
		Result map[string]any `json:"result"`
	}
	if err := d.apiJSON(ctx, fmt.Sprintf("%s/file/clone?key=%s&file_code=%s", ddAPI, url.QueryEscape(key), code), &cl); err != nil {
		return nil, err
	}
	nc := ""
	for _, k := range []string{"filecode", "file_code", "code"} {
		if v, ok := cl.Result[k].(string); ok && v != "" {
			nc = v
		}
	}
	if nc == "" {
		return nil, fmt.Errorf("Klonen nicht möglich: %s", firstNonEmpty(cl.Msg, "keine Antwort"))
	}
	return d.ddViaAPI(ctx, key, nc)
}

// summary: Gesamtfortschritt aller aktiven/wartenden Downloads für den Header
func (d *dlManager) summary() M {
	d.mu.Lock()
	defer d.mu.Unlock()
	var active, waiting int
	var done, size, speed int64
	name := ""
	for _, it := range d.f.Items {
		switch it.Status {
		case "lädt":
			active++
			if name == "" {
				name = it.Pkg
			}
		case "wartet":
			waiting++
		default:
			continue
		}
		done += it.Done
		size += it.Size
		speed += it.Speed
	}
	pct := 0.0
	if size > 0 {
		pct = float64(done) * 100 / float64(size)
	}
	return M{"active": active, "waiting": waiting, "done": done, "size": size, "speed": speed, "pct": pct, "name": name}
}

// diagnose prüft einen ddownload-Link Schritt für Schritt und liefert die Rohantworten (ohne Key) zurück.
func (d *dlManager) diagnose(ctx context.Context, link string) []M {
	conf := d.conf()
	out := []M{}
	add := func(step string, ok bool, detail string) {
		out = append(out, M{"step": step, "ok": ok, "detail": detail})
	}
	mask := func(s string) string {
		if conf.DDKey != "" {
			s = strings.ReplaceAll(s, conf.DDKey, "***")
		}
		return s
	}
	cut := func(s string, n int) string {
		s = strings.Join(strings.Fields(s), " ")
		if len(s) > n {
			s = s[:n] + " …"
		}
		return s
	}
	pu, err := url.Parse(strings.TrimSpace(link))
	m := []string(nil)
	if err == nil {
		m = reDDCode.FindStringSubmatch(pu.Path)
	}
	if m == nil {
		add("Datei-Code", false, "In der URL wurde kein 12-stelliger Code gefunden: "+link)
		return out
	}
	code := m[1]
	add("Datei-Code", true, code)
	raw := func(u string) (int, string, error) {
		resp, err := d.get(ctx, d.noRedir, u)
		if err != nil {
			return 0, "", err
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return resp.StatusCode, string(b), nil
	}
	if conf.DDKey == "" {
		add("API-Key", false, "Kein API-Key hinterlegt, nur der Login-Weg wird geprüft.")
	} else {
		for _, ep := range []string{"file/info", "file/direct_link"} {
			st, body, err := raw(fmt.Sprintf("%s/%s?key=%s&file_code=%s", ddAPI, ep, url.QueryEscape(conf.DDKey), code))
			if err != nil {
				add("API "+ep, false, mask(err.Error()))
				continue
			}
			ok := st == 200 && regexp.MustCompile(`"status"\s*:\s*200`).MatchString(body)
			add("API "+ep, ok, fmt.Sprintf("HTTP %d: %s", st, mask(cut(body, 400))))
		}
	}
	if conf.DDLogin == "" || conf.DDPass == "" {
		add("Login-Weg", false, "Kein Benutzername/Passwort hinterlegt.")
		return out
	}
	u, _ := url.Parse(ddBase)
	hasCookie := false
	for _, c := range d.jar.Cookies(u) {
		if c.Name == "xfss" {
			hasCookie = true
		}
	}
	if !hasCookie {
		if err := d.ddLogin(ctx, conf); err != nil {
			add("Login", false, err.Error())
			return out
		}
		add("Login", true, "angemeldet, Cookie gespeichert")
	} else {
		add("Login", true, "gespeicherter Cookie vorhanden")
	}
	resp, err := d.get(ctx, d.noRedir, ddBase+"/"+code)
	if err != nil {
		add("Seite laden", false, err.Error())
		return out
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	resp.Body.Close()
	txt := string(b)
	title := ""
	if mm := regexp.MustCompile(`(?is)<title>(.*?)</title>`).FindStringSubmatch(txt); mm != nil {
		title = cut(html.UnescapeString(mm[1]), 120)
	}
	marks := []string{}
	for _, k := range []string{"download2", "method_premium", "/d/", "Cloudflare", "captcha", "File Not Found", "No such file", "login", "Premium"} {
		if strings.Contains(txt, k) {
			marks = append(marks, k)
		}
	}
	add("Seite laden", resp.StatusCode < 400, fmt.Sprintf("HTTP %d, Location: %q, Titel: %q, enthält: %s", resp.StatusCode, resp.Header.Get("Location"), title, strings.Join(marks, ", ")))
	if r, err := d.ddViaWeb(ctx, code, conf); err != nil {
		add("Direktlink über Login", false, err.Error())
	} else {
		add("Direktlink über Login", true, "Link gefunden: "+cut(reDirectHost(r.url), 80))
	}
	return out
}

func reDirectHost(u string) string {
	if pu, err := url.Parse(u); err == nil {
		return pu.Scheme + "://" + pu.Host + "/…"
	}
	return "ok"
}
