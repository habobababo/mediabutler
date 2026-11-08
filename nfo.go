package main

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

// NFO-Dateien für Jellyfin: Metadaten aus TMDB/TVDB neben die Medien schreiben.
// Abschalten mit NFO=0. Vorhandene .nfo-Dateien werden nie überschrieben.

var nfoMu sync.Mutex

type nfoInfo struct {
	Title, Orig, Year, Plot, Premiered, Rating string
	Runtime                                    int
	Genres                                     []string
	TMDB, TVDB                                 string
}

func xesc(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

func (n nfoInfo) xml(root string) string {
	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>\n<" + root + ">\n")
	add := func(tag, v string) {
		if strings.TrimSpace(v) != "" {
			b.WriteString("  <" + tag + ">" + xesc(v) + "</" + tag + ">\n")
		}
	}
	add("title", n.Title)
	add("originaltitle", n.Orig)
	add("year", n.Year)
	add("plot", n.Plot)
	add("premiered", n.Premiered)
	add("rating", n.Rating)
	if n.Runtime > 0 {
		add("runtime", fmt.Sprint(n.Runtime))
	}
	for _, g := range n.Genres {
		add("genre", g)
	}
	if n.TMDB != "" {
		b.WriteString("  <uniqueid type=\"tmdb\" default=\"true\">" + xesc(n.TMDB) + "</uniqueid>\n")
		add("tmdbid", n.TMDB)
	}
	if n.TVDB != "" {
		d := ""
		if n.TMDB == "" {
			d = " default=\"true\""
		}
		b.WriteString("  <uniqueid type=\"tvdb\"" + d + ">" + xesc(n.TVDB) + "</uniqueid>\n")
		add("tvdbid", n.TVDB)
	}
	b.WriteString("</" + root + ">\n")
	return b.String()
}

// nfoDetails holt Details zur gespeicherten ID (TMDB-Zahl oder "tvdb:123"). Fehler sind unkritisch.
func (a *App) nfoDetails(id string, movie bool) nfoInfo {
	c := a.metaConf()
	var n nfoInfo
	switch {
	case strings.HasPrefix(id, "tvdb:"):
		n.TVDB = strings.TrimPrefix(id, "tvdb:")
		if c.tvdb == "" || !c.on["tvdb"] {
			return n
		}
		tok, err := tvdbToken(c.tvdb, false)
		if err != nil {
			return n
		}
		h := map[string]string{"Authorization": "Bearer " + tok}
		j, err := httpJSON(tvdbAPI+"/series/"+url.PathEscape(n.TVDB)+"/extended?short=true", nil, h)
		if err != nil {
			return n
		}
		d := mm(j["data"])
		n.Orig = fstr(d["name"])
		n.Title = n.Orig
		n.Premiered = fstr(d["firstAired"])
		n.Year = fstr(d["year"])
		n.Plot = fstr(d["overview"])
		for _, g := range arr(d["genres"]) {
			if s := fstr(mm(g)["name"]); s != "" {
				n.Genres = append(n.Genres, s)
			}
		}
		if t, err := httpJSON(tvdbAPI+"/series/"+url.PathEscape(n.TVDB)+"/translations/deu", nil, h); err == nil {
			td := mm(t["data"])
			if v := fstr(td["name"]); v != "" {
				n.Title = v
			}
			if v := fstr(td["overview"]); v != "" {
				n.Plot = v
			}
		}
	case id != "":
		n.TMDB = id
		if c.tmdb == "" || !c.on["tmdb"] {
			return n
		}
		kind := "tv"
		if movie {
			kind = "movie"
		}
		p := url.Values{"language": {"de-DE"}}
		hdr := map[string]string{}
		if len(c.tmdb) > 40 {
			hdr["Authorization"] = "Bearer " + c.tmdb
		} else {
			p.Set("api_key", c.tmdb)
		}
		j, err := httpJSON(tmdbAPI+"/"+kind+"/"+url.PathEscape(id)+"?"+p.Encode(), nil, hdr)
		if err != nil {
			return n
		}
		n.Title = firstNonEmpty(fstr(j["title"]), fstr(j["name"]))
		n.Orig = firstNonEmpty(fstr(j["original_title"]), fstr(j["original_name"]))
		n.Premiered = firstNonEmpty(fstr(j["release_date"]), fstr(j["first_air_date"]))
		if len(n.Premiered) >= 4 {
			n.Year = n.Premiered[:4]
		}
		n.Plot = fstr(j["overview"])
		if v, ok := j["vote_average"].(float64); ok && v > 0 {
			n.Rating = fmt.Sprintf("%.1f", v)
		}
		if v, ok := j["runtime"].(float64); ok {
			n.Runtime = int(v)
		}
		for _, g := range arr(j["genres"]) {
			if s := fstr(mm(g)["name"]); s != "" {
				n.Genres = append(n.Genres, s)
			}
		}
		if ex, ok := mm(j["external_ids"])["tvdb_id"]; ok {
			n.TVDB = fstr(ex)
		}
	}
	return n
}

func writeIfMissing(p, content string) {
	if _, err := os.Stat(p); err == nil {
		return
	}
	tmp := p + ".tmp"
	if os.WriteFile(tmp, []byte(content), 0o644) == nil {
		os.Rename(tmp, p)
	}
}

// writeNFO legt nach dem Kopieren die NFO an: Film -> "<Datei>.nfo", Serie -> "tvshow.nfo" im Serienordner.
func (a *App) writeNFO(rel, cat, dst string) {
	if env("NFO", "1") == "0" || cat == "" {
		return
	}
	sub, ok := a.autoSubject(rel, cat)
	if !ok {
		return
	}
	r := a.ruleByKey(sub.key)
	if r == nil || r.TMDB == "" {
		return // ohne bestätigte ID keine NFO, damit Jellyfin nicht falsch zuordnet
	}
	nfoMu.Lock()
	defer nfoMu.Unlock()
	movie := sub.kind == "movie"
	var target string
	if movie {
		target = strings.TrimSuffix(dst, path.Ext(dst)) + ".nfo"
	} else {
		dir := filepath.Dir(dst)
		if reSeasonT.MatchString(filepath.Base(dir)) {
			dir = filepath.Dir(dir)
		}
		target = filepath.Join(dir, "tvshow.nfo")
	}
	if _, err := os.Stat(target); err == nil {
		return
	}
	n := a.nfoDetails(r.TMDB, movie)
	n.Title = firstNonEmpty(r.Name, n.Title)
	n.Year = firstNonEmpty(r.Year, n.Year)
	if n.Title == "" {
		return
	}
	root := "tvshow"
	if movie {
		root = "movie"
	}
	writeIfMissing(target, n.xml(root))
}
