package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

var (
	metaNames  = map[string]string{"tmdb": "TMDB", "tvdb": "TVDB", "anilist": "AniList", "jikan": "MyAnimeList"}
	metaOrder  = []string{"tmdb", "tvdb", "anilist", "jikan"}
	tmdbAPI    = strings.TrimRight(env("TMDB_API", "https://api.themoviedb.org/3"), "/")
	tvdbAPI    = strings.TrimRight(env("TVDB_API", "https://api4.thetvdb.com/v4"), "/")
	anilistAPI = env("ANILIST_API", "https://graphql.anilist.co")
	jikanAPI   = strings.TrimRight(env("JIKAN_API", "https://api.jikan.moe/v4"), "/")
	metaKinds  = map[string][]string{"tv": {"tmdb", "tvdb"}, "movie": {"tmdb", "tvdb"}, "anime": {"anilist", "jikan", "tvdb", "tmdb"},
		"all": {"tmdb", "tvdb", "anilist", "jikan"}}
	reHTMLTag = regexp.MustCompile(`<[^>]+>`)
	tvdbTok   struct {
		sync.Mutex
		key, t string
		exp    time.Time
	}
)

type errPerm struct{ s string }

func (e errPerm) Error() string { return e.s }

type metaConf struct {
	tmdb, tvdb string
	on         map[string]bool
}

func (a *App) metaConf() metaConf {
	d := M{}
	a.load("meta.json", &d)
	on, _ := d["on"].(map[string]any)
	c := metaConf{on: map[string]bool{}}
	c.tmdb = strings.TrimSpace(firstNonEmpty(str(d["tmdb_key"]), env("TMDB_KEY", "")))
	c.tvdb = strings.TrimSpace(firstNonEmpty(str(d["tvdb_key"]), env("TVDB_KEY", "")))
	for k := range metaNames {
		v, ok := on[k].(bool)
		c.on[k] = !ok || v
	}
	return c
}

func firstNonEmpty(l ...string) string {
	for _, s := range l {
		if s != "" {
			return s
		}
	}
	return ""
}

func httpJSON(u string, data any, headers map[string]string) (map[string]any, error) {
	var rd io.Reader
	method := "GET"
	if data != nil {
		b, _ := json.Marshal(data)
		rd, method = bytes.NewReader(b), "POST"
	}
	req, _ := http.NewRequest(method, u, rd)
	req.Header.Set("User-Agent", "MediaButler")
	req.Header.Set("Accept", "application/json")
	if data != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
	if err != nil {
		reason := err.Error()
		if ue, ok := err.(*url.Error); ok {
			reason = ue.Err.Error()
		}
		return nil, fmt.Errorf("nicht erreichbar (%s)", reason)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 401 || resp.StatusCode == 403:
		return nil, errPerm{"API-Key ungültig oder abgelehnt"}
	case resp.StatusCode == 429:
		return nil, errors.New("zu viele Anfragen, kurz warten")
	case resp.StatusCode >= 400:
		return nil, fmt.Errorf("Fehler %d", resp.StatusCode)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("nicht erreichbar (%v)", err)
	}
	return out, nil
}

func mm(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}
func arr(v any) []any { l, _ := v.([]any); return l }
func or(vs ...any) any {
	for _, v := range vs {
		if v != nil && v != "" && v != float64(0) {
			return v
		}
	}
	return nil
}
func fstr(v any) string {
	if v == nil {
		return ""
	}
	return str(v)
}

func metaHit(src, title, orig, year, kind, overview, idval, rid string) M {
	t := strings.TrimSpace(firstNonEmpty(title, orig))
	y := []rune(year)
	if len(y) > 4 {
		y = y[:4]
	}
	ov := strings.TrimSpace(collapseWS(reHTMLTag.ReplaceAllString(overview, " ")))
	if r := []rune(ov); len(r) > 180 {
		ov = string(r[:180])
	}
	return M{"src": src, "title": t, "orig": strings.TrimSpace(orig), "year": string(y), "kind": kind, "overview": ov, "idval": idval, "id": rid}
}

func sTMDB(q, kind, year string, c metaConf) ([]M, error) {
	if c.tmdb == "" {
		return nil, errPerm{"kein API-Key hinterlegt"}
	}
	ep := map[string]string{"tv": "search/tv", "anime": "search/tv", "movie": "search/movie"}[kind]
	if ep == "" {
		ep = "search/multi"
	}
	p := url.Values{"query": {q}, "language": {"de-DE"}, "include_adult": {"false"}}
	hdr := map[string]string{}
	if len(c.tmdb) > 40 {
		hdr["Authorization"] = "Bearer " + c.tmdb
	} else {
		p.Set("api_key", c.tmdb)
	}
	if year != "" && ep != "search/multi" {
		if strings.HasSuffix(ep, "movie") {
			p.Set("year", year)
		} else {
			p.Set("first_air_date_year", year)
		}
	}
	j, err := httpJSON(tmdbAPI+"/"+ep+"?"+p.Encode(), nil, hdr)
	if err != nil {
		return nil, err
	}
	out := []M{}
	for i, x := range arr(j["results"]) {
		if i >= 8 {
			break
		}
		r := mm(x)
		mt := str(r["media_type"])
		if mt == "" {
			mt = "tv"
			if strings.HasSuffix(ep, "movie") {
				mt = "movie"
			}
		}
		if mt != "tv" && mt != "movie" {
			continue
		}
		tv := mt == "tv"
		kt, tk, ok, dk := "Film", "title", "original_title", "release_date"
		if tv {
			kt, tk, ok, dk = "Serie", "name", "original_name", "first_air_date"
		}
		out = append(out, metaHit("tmdb", fstr(r[tk]), fstr(r[ok]), fstr(r[dk]), kt, fstr(r["overview"]), fstr(r["id"]), fstr(r["id"])))
	}
	return out, nil
}

func tvdbToken(key string, fresh bool) (string, error) {
	tvdbTok.Lock()
	defer tvdbTok.Unlock()
	if !fresh && tvdbTok.t != "" && tvdbTok.key == key && time.Now().Before(tvdbTok.exp) {
		return tvdbTok.t, nil
	}
	j, err := httpJSON(tvdbAPI+"/login", M{"apikey": key}, nil)
	if err != nil {
		return "", err
	}
	tvdbTok.key, tvdbTok.t, tvdbTok.exp = key, str(mm(j["data"])["token"]), time.Now().Add(20*24*time.Hour)
	if tvdbTok.t == "" {
		return "", errPerm{"Anmeldung bei TVDB fehlgeschlagen"}
	}
	return tvdbTok.t, nil
}

var reNonDig = regexp.MustCompile(`\D`)

func sTVDB(q, kind, year string, c metaConf) ([]M, error) {
	if c.tvdb == "" {
		return nil, errPerm{"kein API-Key hinterlegt"}
	}
	typ := "series"
	if kind == "movie" {
		typ = "movie"
	}
	p := url.Values{"query": {q}, "type": {typ}, "limit": {"8"}}
	if year != "" {
		p.Set("year", year)
	}
	u := tvdbAPI + "/search?" + p.Encode()
	tok, err := tvdbToken(c.tvdb, false)
	if err != nil {
		return nil, err
	}
	j, err := httpJSON(u, nil, map[string]string{"Authorization": "Bearer " + tok})
	if _, perm := err.(errPerm); perm {
		if tok, err = tvdbToken(c.tvdb, true); err == nil {
			j, err = httpJSON(u, nil, map[string]string{"Authorization": "Bearer " + tok})
		}
	}
	if err != nil {
		return nil, err
	}
	out := []M{}
	for i, x := range arr(j["data"]) {
		if i >= 8 {
			break
		}
		r := mm(x)
		series := firstNonEmpty(fstr(r["type"]), "series") != "movie"
		tid := fstr(or(r["tvdb_id"]))
		if tid == "" {
			tid = reNonDig.ReplaceAllString(fstr(r["id"]), "")
		}
		kt, idv := "Film", ""
		if series {
			kt = "Serie"
			if tid != "" {
				idv = "tvdb:" + tid
			}
		}
		out = append(out, metaHit("tvdb", firstNonEmpty(fstr(mm(r["translations"])["deu"]), fstr(r["name"])), fstr(r["name"]),
			firstNonEmpty(fstr(r["year"]), fstr(r["first_air_time"])), kt, firstNonEmpty(fstr(mm(r["overviews"])["deu"]), fstr(r["overview"])), idv, tid))
	}
	return out, nil
}

const anilistQ = "query($s:String){Page(perPage:8){media(search:$s,type:ANIME,sort:SEARCH_MATCH){id title{romaji english} startDate{year} format description(asHtml:false)}}}"

func sAniList(q, kind, year string, c metaConf) ([]M, error) {
	j, err := httpJSON(anilistAPI, M{"query": anilistQ, "variables": M{"s": q}}, nil)
	if err != nil {
		return nil, err
	}
	out := []M{}
	for _, x := range arr(mm(mm(j["data"])["Page"])["media"]) {
		m := mm(x)
		t := mm(m["title"])
		kt := "Anime"
		if m["format"] == "MOVIE" {
			kt = "Anime-Film"
		}
		out = append(out, metaHit("anilist", firstNonEmpty(fstr(t["english"]), fstr(t["romaji"])), fstr(t["romaji"]), fstr(mm(m["startDate"])["year"]), kt, fstr(m["description"]), "", fstr(m["id"])))
	}
	return out, nil
}

func sJikan(q, kind, year string, c metaConf) ([]M, error) {
	j, err := httpJSON(jikanAPI+"/anime?"+url.Values{"q": {q}, "limit": {"8"}, "sfw": {"true"}}.Encode(), nil, nil)
	if err != nil {
		return nil, err
	}
	out := []M{}
	for i, x := range arr(j["data"]) {
		if i >= 8 {
			break
		}
		m := mm(x)
		kt := "Anime"
		if m["type"] == "Movie" {
			kt = "Anime-Film"
		}
		y := fstr(or(m["year"], mm(mm(mm(m["aired"])["prop"])["from"])["year"]))
		out = append(out, metaHit("jikan", firstNonEmpty(fstr(m["title_english"]), fstr(m["title"])), fstr(m["title"]), y, kt, fstr(m["synopsis"]), "", fstr(m["mal_id"])))
	}
	return out, nil
}

var metaFn = map[string]func(string, string, string, metaConf) ([]M, error){"tmdb": sTMDB, "tvdb": sTVDB, "anilist": sAniList, "jikan": sJikan}

func (a *App) metaSearchAll(q, kind, year string) ([]M, M) {
	c := a.metaConf()
	kinds, ok := metaKinds[kind]
	if !ok {
		kinds = metaKinds["all"]
	}
	var srcs []string
	for _, x := range kinds {
		if c.on[x] {
			srcs = append(srcs, x)
		}
	}
	results, errs := map[string][]M{}, M{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, x := range srcs {
		wg.Add(1)
		go func(x string) {
			defer wg.Done()
			r, err := metaFn[x](q, kind, year, c)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				msg := err.Error()
				if msg == "" {
					msg = "unbekannter Fehler"
				}
				errs[x] = msg
			} else {
				results[x] = r
			}
		}(x)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
	}
	mu.Lock()
	defer mu.Unlock()
	merged := []M{}
	mx := 0
	for _, x := range srcs {
		if _, ok := results[x]; !ok {
			if _, e := errs[x]; !e {
				errs[x] = "Zeitüberschreitung"
			}
		}
		if len(results[x]) > mx {
			mx = len(results[x])
		}
	}
	for i := 0; i < mx; i++ {
		for _, x := range srcs {
			if i < len(results[x]) {
				merged = append(merged, results[x][i])
			}
		}
	}
	return merged, errs
}

func (a *App) metaRoutes(get, post func(string, http.HandlerFunc)) {
	get("/api/meta", func(w http.ResponseWriter, r *http.Request) {
		c := a.metaConf()
		js(w, 200, M{"tmdb_set": c.tmdb != "", "tvdb_set": c.tvdb != "", "on": c.on, "names": metaNames})
	})
	post("/api/meta/config", func(w http.ResponseWriter, r *http.Request) {
		b := body(r)
		d := M{}
		a.load("meta.json", &d)
		for _, k := range []string{"tmdb_key", "tvdb_key"} {
			if v := strings.TrimSpace(str(b[k])); v != "" {
				d[k] = v
			}
			if b["clear_"+k] == true || b["clear_"+k] == float64(1) {
				delete(d, k)
			}
		}
		if on, ok := b["on"].(map[string]any); ok {
			n := M{}
			for k := range metaNames {
				v, has := on[k].(bool)
				n[k] = !has || v
			}
			d["on"] = n
		}
		a.save("meta.json", d)
		if _, ok := b["tvdb_key"]; ok {
			tvdbTok.Lock()
			tvdbTok.t, tvdbTok.exp = "", time.Time{}
			tvdbTok.Unlock()
		}
		js(w, 200, M{"ok": true})
	})
	post("/api/meta/search", func(w http.ResponseWriter, r *http.Request) {
		d := body(r)
		q := strings.TrimSpace(str(d["q"]))
		if len([]rune(q)) < 2 {
			js(w, 400, M{"error": "Bitte mindestens zwei Zeichen eingeben."})
			return
		}
		kind := str(d["kind"])
		if _, ok := metaKinds[kind]; !ok {
			kind = "all"
		}
		res, errs := a.metaSearchAll(q, kind, yearOf(d["year"]))
		js(w, 200, M{"results": res, "errors": errs, "names": metaNames})
	})
}
