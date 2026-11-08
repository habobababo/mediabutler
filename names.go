package main

// Erkennung und Zielpfade, 1:1 aus app.py portiert (Abschnitt „Erkennung & Zielpfad“).
// Python-Regexe mit Lookaround laufen über regexp2; alles Positionsbezogene arbeitet mit Rune-Offsets.

import (
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/dlclark/regexp2"
)

// ---------- Regex-Helfer ----------

type rx struct{ r *regexp2.Regexp }

func mk(p string, ci bool) *rx {
	o := regexp2.RegexOptions(0)
	if ci {
		o = regexp2.IgnoreCase
	}
	return &rx{regexp2.MustCompile(p, o)}
}

type hit struct {
	start, end int // Rune-Offsets
	g          []string
	ok         []bool
}

func (h *hit) grp(i int) string { return h.g[i] }
func (h *hit) has(i int) bool   { return i < len(h.ok) && h.ok[i] }

func mkhit(m *regexp2.Match) *hit {
	gs := m.Groups()
	h := &hit{start: m.Index, end: m.Index + m.Length, g: make([]string, len(gs)), ok: make([]bool, len(gs))}
	for i, g := range gs {
		if len(g.Captures) > 0 {
			h.g[i], h.ok[i] = g.String(), true
		}
	}
	return h
}

// search sucht ab Rune-Offset from.
func (x *rx) search(s string, from int) *hit {
	rs := []rune(s)
	if from > len(rs) {
		return nil
	}
	m, err := x.r.FindRunesMatchStartingAt(rs, from)
	if err != nil || m == nil {
		return nil
	}
	return mkhit(m)
}

func (x *rx) match(s string) *hit { // wie re.match: nur am Anfang
	h := x.search(s, 0)
	if h == nil || h.start != 0 {
		return nil
	}
	return h
}

func (x *rx) all(s string) []*hit {
	rs := []rune(s)
	var out []*hit
	m, _ := x.r.FindRunesMatch(rs)
	for m != nil {
		out = append(out, mkhit(m))
		m, _ = x.r.FindNextMatch(m)
	}
	return out
}

func (x *rx) full(s string) bool {
	h := x.match(s)
	return h != nil && h.end == len([]rune(s))
}

func (x *rx) sub(s, repl string) string {
	o, err := x.r.Replace(s, repl, -1, -1)
	if err != nil {
		return s
	}
	return o
}

func rsub(s string, from, to int) string { // Python s[from:to] auf Runen
	r := []rune(s)
	if from < 0 {
		from = 0
	}
	if to > len(r) {
		to = len(r)
	}
	if from >= to {
		return ""
	}
	return string(r[from:to])
}

var (
	reYEAR       = mk(`(?<![\dA-Za-z])(19[2-9]\d|20[0-3]\d)(?![\dA-Za-z])`, false)
	reQUALITY    = mk(`(?<![A-Za-z0-9])(?:\d{3,4}[pi]|4k|uhd|blu-?ray|bdrip|brrip|web-?dl|web-?rip|hdtv|dvdrip|remux|x26[45]|h26[45]|hevc|xvid|german|ac3|dts)(?![A-Za-z0-9])`, true)
	reEPISODE    = mk(`[Ss](\d{1,2})[ ._-]*[Ee](\d{1,3})((?:-?[Ee]\d{1,3})*)|(?<![\dA-Za-z])(\d{1,2})x(\d{2,3})(?![\dA-Za-z])`, false)
	reANIMEEP    = mk(`^\s*(?:\[[^\]]*\]\s*)*(.+?)\s+-\s+(\d{1,4})(?:v\d)?(?!\d)`, false)
	reWSEASON    = mk(`(?<![A-Za-z])(?:staffel|season|saison)[ ._-]*(\d{1,2})(?![\dA-Za-z])`, true)
	reWEP        = mk(`(?<![A-Za-z])(?:folge|episode|ep|eps)[ ._-]*(\d{1,4})(?![\dA-Za-z])`, true)
	reSEASONDIR  = mk(`^(?:staffel|season|saison|series|s)[ ._-]*(\d{1,2})$`, true)
	rePART       = mk(`(?<![A-Za-z])(?:cd|part|pt|disc|disk|teil)[ ._-]*(\d{1,2})(?![\dA-Za-z])`, true)
	reGenericDir = regexp.MustCompile(`(?i)^(?:downloads?|incoming|complete|fertig|serien|series|anime|tv|shows?)$`)
	reDirGeneric = regexp.MustCompile(`(?i)^(?:downloads?|incoming|complete|fertig|filme|movies?|serien|series|anime|tv|shows?|dokus?)$`)
	reEpTag      = mk(`[Ss](\d{1,2})[ ._-]*[Ee]\d{1,3}|(?<!\d)\d{1,2}x\d{2}(?!\d)`, false)

	reWS        = regexp.MustCompile(`\s+`)
	reDotUnd    = regexp.MustCompile(`[._]+`)
	reBrYear    = regexp.MustCompile(`\[((?:19|20)\d\d)\]`)
	reBrAny     = regexp.MustCompile(`\[[^\]]*\]|\{[^}]*\}`)
	reIllegal   = regexp.MustCompile("[<>:\"/\\\\|?*\x00-\x1f]")
	reSkeyYear  = regexp.MustCompile(`\(\s*(?:19|20)\d\d\s*\)`)
	reSkeyNon   = regexp.MustCompile(`[^0-9a-zäöüß]+`)
	reSeasonEnd = regexp.MustCompile(`\s[Ss](\d{1,2})$`)
	reCRC       = regexp.MustCompile(`\[[0-9A-Fa-f]{8}\]`)
	reBrackets  = regexp.MustCompile(`[\[\]{}()]+`)
	reIDNorm    = regexp.MustCompile(`(?i)^\s*(?:(tmdb|tvdb)(?:id)?\s*[:\-_ ]?\s*)?(\d{1,10})\s*$`)
	reLangWord  = regexp.MustCompile(`(?i)doku|documentary|docu\b|dokumentation`)
	reTitleNum  = regexp.MustCompile(`^(.*?[A-Za-zÄÖÜäöü].*?)[ -]+(\d{1,4})$`)
	reDigits1_4 = regexp.MustCompile(`^\d{1,4}$`)
	reAnimeBr   = regexp.MustCompile(`^\[[^\]]+\]`)
	reLegacy1   = regexp.MustCompile(`^(.*?)[ ._-]+[Ss](\d{1,2})[ ._-]*[Ee]\d{1,3}`)
	reLegacy2   = regexp.MustCompile(`^\[[^\]]+\]\s*(.+?)\s+-\s+\d+`)
)

var categories = []string{"Filme", "Serien", "Anime", "Dokus"}
var subExt = map[string]bool{".srt": true, ".ass": true, ".sub": true}
var langs = map[string]bool{"de": true, "deu": true, "ger": true, "german": true, "deutsch": true, "en": true, "eng": true, "english": true,
	"fr": true, "fra": true, "french": true, "es": true, "spa": true, "ja": true, "jpn": true, "it": true, "ita": true, "nl": true, "pt": true,
	"ru": true, "forced": true, "sdh": true, "cc": true}
var langMap = map[string]string{"german": "de", "deutsch": "de", "ger": "de", "deu": "de", "english": "en", "eng": "en", "french": "fr",
	"fra": "fr", "spa": "es", "jpn": "ja", "ita": "it"}
var genericWords = map[string]bool{"movie": true, "film": true, "video": true, "main": true, "feature": true, "movies": true, "films": true}

// ---------- Kleinkram ----------

func collapseWS(s string) string { return reWS.ReplaceAllString(s, " ") }

func clean(s string) string {
	return strings.Trim(collapseWS(reDotUnd.ReplaceAllString(s, " ")), " -")
}

func pretty(s string) string {
	s = reBrYear.ReplaceAllString(s, "($1)")
	s = reBrAny.ReplaceAllString(s, " ")
	s = reDotUnd.ReplaceAllString(s, " ")
	return strings.Trim(collapseWS(s), " -")
}

func safe(s string) string { return strings.Trim(reIllegal.ReplaceAllString(s, ""), " .") }

func skey(title string) string {
	t := reSkeyYear.ReplaceAllString(title, "")
	return reSkeyNon.ReplaceAllString(strings.ToLower(t), "")
}

func idNorm(v string) string {
	m := reIDNorm.FindStringSubmatch(v)
	if m == nil {
		return ""
	}
	if strings.ToLower(m[1]) == "tvdb" {
		return "tvdb:" + m[2]
	}
	return m[2]
}

func idTag(v string) string {
	n := idNorm(v)
	switch {
	case n == "":
		return ""
	case strings.HasPrefix(n, "tvdb:"):
		return " [tvdbid-" + n[5:] + "]"
	}
	return " [tmdbid-" + n + "]"
}

func atoi(s string) int { n, _ := strconv.Atoi(s); return n }

func stem(name string) string { return strings.TrimSuffix(name, path.Ext(name)) }

func lowerExt(name string) string { return strings.ToLower(path.Ext(name)) }

// ---------- Titel, Jahr, Folge ----------

// titleYear: 'Blade.Runner.2049.2017.1080p' -> ('Blade Runner 2049', '2017')
func titleYear(s string) (string, string) {
	s = pretty(s)
	n := len([]rune(s))
	limit := n
	if q := reQUALITY.search(s, 0); q != nil {
		limit = q.start
	}
	var best *hit
	for _, m := range reYEAR.all(s) {
		if m.end <= limit && strings.Trim(rsub(s, 0, m.start), " (-") != "" {
			best = m
		}
	}
	if best != nil {
		return strings.Trim(rsub(s, 0, best.start), " (-"), best.grp(1)
	}
	return strings.Trim(rsub(s, 0, limit), " (-"), ""
}

type epInfo struct {
	title, year string
	season      int
	eps         []int
}

func epsFromTail(first string, tail string) []int {
	out := []int{atoi(first)}
	for _, m := range regexp.MustCompile(`[Ee](\d{1,3})`).FindAllStringSubmatch(tail, -1) {
		out = append(out, atoi(m[1]))
	}
	return out
}

func episode(stm string, anime bool) *epInfo {
	var raw string
	var season int
	var eps []int
	if m := reEPISODE.search(stm, 0); m != nil {
		raw = rsub(stm, 0, m.start)
		if m.has(1) {
			season = atoi(m.grp(1))
			eps = epsFromTail(m.grp(2), m.grp(3))
		} else {
			season, eps = atoi(m.grp(4)), []int{atoi(m.grp(5))}
		}
	} else if anime {
		a := reANIMEEP.match(stm)
		if a == nil {
			return nil
		}
		raw, season, eps = a.grp(1), 1, []int{atoi(a.grp(2))}
		tr := strings.TrimRight(raw, " \t\r\n")
		if s2 := reSeasonEnd.FindStringSubmatchIndex(tr); s2 != nil {
			season, raw = atoi(tr[s2[2]:s2[3]]), tr[:s2[0]]
		}
	} else {
		return nil
	}
	t, y := titleYear(raw)
	if t == "" {
		return nil
	}
	return &epInfo{t, y, season, eps}
}

// ctx: (Serientitel aus Ordner, Staffel aus Ordner), von unten nach oben
func ctx(dirs []string) (string, int) {
	season, title := 0, ""
	for i := len(dirs) - 1; i >= 0; i-- {
		pd := pretty(dirs[i])
		if m := reSEASONDIR.match(pd); m != nil {
			if season == 0 {
				season = atoi(m.grp(1))
			}
			continue
		}
		if ws := reWSEASON.search(pd, 0); ws != nil {
			if season == 0 {
				season = atoi(ws.grp(1))
			}
			pd = pretty(rsub(pd, 0, ws.start) + " " + rsub(pd, ws.end, len([]rune(pd))))
		}
		if pd != "" && !reGenericDir.MatchString(pd) {
			title = pd
		}
		break
	}
	return title, season
}

func parentDirs(rel string) []string {
	d := path.Dir(rel)
	if d == "." || d == "" {
		return nil
	}
	return strings.Split(d, "/")
}

// parseEp: (Titel, Jahr, Staffel, [Folgen]) oder nil
func parseEp(rel string, anime, loose, explicit bool, stemOverride *string) *epInfo {
	stm := stem(path.Base(rel))
	if stemOverride != nil {
		stm = *stemOverride
	}
	ctxTitle, ctxSeason := ctx(parentDirs(rel))
	if m := reEPISODE.search(stm, 0); m != nil {
		if r := episode(stm, anime); r != nil {
			return r
		}
		if ctxTitle != "" {
			if t, y := titleYear(ctxTitle); t != "" {
				var sn int
				var eps []int
				if m.has(1) {
					sn, eps = atoi(m.grp(1)), epsFromTail(m.grp(2), m.grp(3))
				} else {
					sn, eps = atoi(m.grp(4)), []int{atoi(m.grp(5))}
				}
				return &epInfo{t, y, sn, eps}
			}
		}
	}
	if me := reWEP.search(stm, 0); me != nil {
		ws := reWSEASON.search(stm, 0)
		season := 1
		if ws != nil {
			season = atoi(ws.grp(1))
		} else if ctxSeason != 0 {
			season = ctxSeason
		}
		cut := me.start
		if ws != nil && ws.start < cut {
			cut = ws.start
		}
		title, year := titleYear(rsub(stm, 0, cut))
		if title == "" {
			title = ctxTitle
		}
		if title == "" {
			return nil
		}
		return &epInfo{title, year, season, []int{atoi(me.grp(1))}}
	}
	if explicit {
		return nil
	}
	cs := ctxSeason
	if cs == 0 {
		cs = 1
	}
	if anime || loose {
		if a := reANIMEEP.match(stm); a != nil {
			raw, season := a.grp(1), cs
			tr := strings.TrimRight(raw, " \t\r\n")
			if s2 := reSeasonEnd.FindStringSubmatchIndex(tr); s2 != nil {
				season, raw = atoi(tr[s2[2]:s2[3]]), tr[:s2[0]]
			}
			if title, year := titleYear(raw); title != "" {
				return &epInfo{title, year, season, []int{atoi(a.grp(2))}}
			}
		}
	}
	if loose {
		pt := pretty(stm)
		if q := reQUALITY.search(pt, 0); q != nil {
			pt = rsub(pt, 0, q.start)
		}
		pt = strings.Trim(pt, " -")
		if t := reTitleNum.FindStringSubmatch(pt); t != nil && !reYEAR.full(t[2]) {
			if title, year := titleYear(t[1]); title != "" {
				return &epInfo{title, year, cs, []int{atoi(t[2])}}
			}
		}
		if reDigits1_4.MatchString(pt) && ctxTitle != "" {
			return &epInfo{ctxTitle, "", cs, []int{atoi(pt)}}
		}
	}
	return nil
}

// ---------- Tags, Zielpfad ----------

func qualityTags(stm string, start int) string {
	q := reQUALITY.search(stm, start)
	if q == nil {
		return ""
	}
	t := reCRC.ReplaceAllString(rsub(stm, q.start, len([]rune(stm))), " ")
	t = reBrackets.ReplaceAllString(reIllegal.ReplaceAllString(t, ""), " ")
	return strings.Trim(collapseWS(t), " .-_")
}

// dirYear: Titel/Jahr/Ordnername aus dem nächsten Ordner, wenn der Dateiname kein Jahr hat und der Ordner zum Titel passt.
func dirYear(rel, title string) (string, string, string) {
	dirs := parentDirs(rel)
	for i := len(dirs) - 1; i >= 0; i-- {
		t2, y2 := titleYear(dirs[i])
		if t2 != "" && !reDirGeneric.MatchString(t2) {
			k, k2 := skey(title), skey(t2)
			if y2 != "" && (k == "" || genericWords[k] || strings.Contains(k2, k) || strings.Contains(k, k2)) {
				return t2, y2, dirs[i]
			}
			return "", "", ""
		}
	}
	return "", "", ""
}

func legacyTarget(rel, cat string) string {
	name := path.Base(rel)
	stm := stem(name)
	if cat == "Serien" || cat == "Anime" {
		if m := reLegacy1.FindStringSubmatch(name); m != nil {
			return fmt.Sprintf("%s/%s/Season %02d/%s", cat, clean(m[1]), atoi(m[2]), name)
		}
		if m := reLegacy2.FindStringSubmatch(name); m != nil {
			return fmt.Sprintf("%s/%s/Season 01/%s", cat, clean(m[1]), name)
		}
		return cat + "/" + clean(stm) + "/" + name
	}
	if cat == "Filme" {
		return cat + "/" + clean(stm) + "/" + name
	}
	return cat + "/" + name
}

func showName(title, year string) string {
	if year != "" {
		return safe(title + " (" + year + ")")
	}
	return safe(title)
}

// jfTarget: Jellyfin-Schema Titel (Jahr)/Titel (Jahr) - Tags.mkv bzw. Serie (Jahr)/Season 01/Serie (Jahr) S01E02 - Tags.mkv
func (s *Store) jfTarget(rel, cat string) string {
	name := path.Base(rel)
	ext, stm, extra := lowerExt(name), stem(name), ""
	if subExt[ext] { // Movie.2020.de.forced.srt -> Sprachkürzel behalten
		parts := strings.Split(stm, ".")
		var tail []string
		for len(parts) > 1 && langs[strings.ToLower(parts[len(parts)-1])] {
			t := strings.ToLower(parts[len(parts)-1])
			parts = parts[:len(parts)-1]
			if v, ok := langMap[t]; ok {
				t = v
			}
			tail = append([]string{t}, tail...)
		}
		stm = strings.Join(parts, ".")
		for _, t := range tail {
			extra += "." + t
		}
	}
	var ep *epInfo
	if cat == "Serien" || cat == "Anime" || cat == "Dokus" {
		ep = parseEp(rel, cat == "Anime", cat == "Serien" || cat == "Anime", false, &stm)
	}
	if ep != nil {
		title, year, tag := ep.title, ep.year, ""
		if year == "" {
			_, y2, _ := dirYear(rel, title)
			year = y2
		}
		if r := s.ruleFor(title); r != nil {
			if r.Name != "" {
				title = r.Name
			}
			year = r.Year
			tag = idTag(r.TMDB)
		}
		show := showName(title, year)
		es := fmt.Sprintf("S%02dE%02d", ep.season, ep.eps[0])
		for _, n := range ep.eps[1:] {
			es += fmt.Sprintf("-E%02d", n)
		}
		start := 0
		if m := reEPISODE.search(stm, 0); m != nil {
			start = m.end
		}
		tg := ""
		if t := qualityTags(stm, start); t != "" {
			tg = " - " + t
		}
		return fmt.Sprintf("%s/%s/Season %02d/%s %s%s%s%s", cat, show+tag, ep.season, show, es, tg, extra, ext)
	}
	if cat == "Serien" || cat == "Anime" { // keine Folgennummer erkennbar: Dateiname bleibt
		d := safe(pretty(stm))
		if d == "" {
			d = "Unbekannt"
		}
		return cat + "/" + d + "/" + name
	}
	title, year := titleYear(stm)
	dname := ""
	if year == "" || title == "" {
		t2, y2, dn := dirYear(rel, title)
		if y2 != "" {
			title, year, dname = t2, y2, dn
		}
	}
	if title != "" {
		mtag := ""
		if r := s.ruleByKey(movieKey(title, year)); r != nil {
			if r.Name != "" {
				title = r.Name
			}
			year = r.Year
			mtag = idTag(r.TMDB)
		}
		base := showName(title, year)
		tg := qualityTags(stm, 0)
		if tg == "" && year != "" && dname != "" {
			tg = qualityTags(dname, 0)
		}
		if tg != "" {
			tg = " - " + tg
		}
		return cat + "/" + base + mtag + "/" + base + tg + extra + ext
	}
	return cat + "/" + name
}

// ---------- Medien-ID (Duplikate, Abgleich) ----------

type mediaID struct {
	typ, key, year string
	extra          string // "S1E1,2" bzw. Teil-Nr.
	display        string
}

func (a mediaID) id4() string { return a.typ + "|" + a.key + "|" + a.year + "|" + a.extra }

func (s *Store) mediaID(rel string, skip int) mediaID {
	dirs := parentDirs(rel)
	if len(dirs) > skip {
		dirs = dirs[skip:]
	} else {
		dirs = nil
	}
	stm := stem(path.Base(rel))
	if ep := episode(stm, strings.Contains(strings.ToLower(rel), "anime")); ep != nil {
		title, year := ep.title, ep.year
		if title == "" {
			t, _ := ctx(dirs)
			title, year = titleYear(t)
		}
		if title != "" {
			es := fmt.Sprintf("S%02d", ep.season)
			ex := fmt.Sprintf("S%d:", ep.season)
			for i, x := range ep.eps {
				es += fmt.Sprintf("E%02d", x)
				if i > 0 {
					ex += ","
				}
				ex += strconv.Itoa(x)
			}
			return mediaID{"e", skey(title), year, ex, showName2(title, year) + " " + es}
		}
	}
	title, year := titleYear(stm)
	if title == "" || skey(title) == "" {
		t, _ := ctx(dirs)
		if t == "" {
			t = stm
		}
		title, year = titleYear(t)
	}
	part := ""
	if pm := rePART.search(stm, 0); pm != nil {
		part = strconv.Itoa(atoi(pm.grp(1)))
	}
	return mediaID{"m", skey(title), year, part, showName2(title, year)}
}

func showName2(title, year string) string {
	if year != "" {
		return title + " (" + year + ")"
	}
	return title
}

var articles = []string{"the", "der", "die", "das", "ein", "eine", "a", "an", "le", "la", "les", "el"}

// cmpKey: führenden Artikel entfernen, wenn danach noch mindestens 4 Zeichen bleiben (wie _ARTICLE in app.py)
func cmpKey(k string) string {
	for _, a := range articles {
		if strings.HasPrefix(k, a) && len([]rune(k[len(a):])) >= 4 {
			return k[len(a):]
		}
	}
	return k
}

// guessCategory: Kategorie oder "" wenn unklar (= Klärungsbedarf)
func (s *Store) guessCategory(rel string) string {
	low, name := strings.ToLower(rel), path.Base(rel)
	if ov, ok := s.getPaths()[rel]; ok && inCats(ov.Cat) {
		return ov.Cat
	}
	if g, ok := s.getGroups()[rel]; ok && inCats(g.Cat) {
		return g.Cat
	}
	if ep := parseEp(rel, false, true, false, nil); ep != nil {
		if r := s.ruleFor(ep.title); r != nil && inCats(r.Category) {
			return r.Category
		}
	}
	if reLangWord.MatchString(low) {
		return "Dokus"
	}
	if strings.Contains(low, "anime") || reAnimeBr.MatchString(name) {
		return "Anime"
	}
	if reEpTag.search(name, 0) != nil || parseEp(rel, false, false, true, nil) != nil {
		return "Serien"
	}
	if reYEAR.search(rel, 0) != nil {
		return "Filme"
	}
	return ""
}

func inCats(c string) bool {
	for _, x := range categories {
		if x == c {
			return true
		}
	}
	return false
}
