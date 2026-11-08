package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNFOXML(t *testing.T) {
	x := nfoInfo{Title: "Fast & Furious", Year: "2001", Plot: "A <b>test</b>", Genres: []string{"Action"}, TMDB: "9799"}.xml("movie")
	for _, w := range []string{"<movie>", "Fast &amp; Furious", "&lt;b&gt;", `<uniqueid type="tmdb" default="true">9799</uniqueid>`, "<genre>Action</genre>"} {
		if !strings.Contains(x, w) {
			t.Fatalf("fehlt %q in %s", w, x)
		}
	}
}

func TestWriteIfMissing(t *testing.T) {
	p := filepath.Join(t.TempDir(), "a.nfo")
	writeIfMissing(p, "eins")
	writeIfMissing(p, "zwei")
	if b, _ := os.ReadFile(p); string(b) != "eins" {
		t.Fatal("überschrieben")
	}
}
