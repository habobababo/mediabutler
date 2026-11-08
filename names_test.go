package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

type goldenCase struct {
	Rel    string   `json:"rel"`
	Cat    string   `json:"cat"`
	Target string   `json:"target"`
	Legacy string   `json:"legacy"`
	Ep     []any    `json:"ep"`
	Mid    []string `json:"mid"`
}

func TestGolden(t *testing.T) {
	b, err := os.ReadFile("testdata/golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var g struct {
		Cases []goldenCase                `json:"cases"`
		Guess []struct{ Rel, Cat string } `json:"guess"`
	}
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatal(err)
	}
	s := NewStore(t.TempDir())
	bad := 0
	fail := func(f string, a ...any) {
		bad++
		if bad <= 25 {
			t.Errorf(f, a...)
		}
	}
	for _, c := range g.Cases {
		if got := s.jfTarget(c.Rel, c.Cat); got != c.Target {
			fail("jfTarget(%q,%s)\n  go: %s\n  py: %s", c.Rel, c.Cat, got, c.Target)
		}
		if got := legacyTarget(c.Rel, c.Cat); got != c.Legacy {
			fail("legacy(%q,%s)\n  go: %s\n  py: %s", c.Rel, c.Cat, got, c.Legacy)
		}
		ep := parseEp(c.Rel, c.Cat == "Anime", true, false, nil)
		var got any
		if ep != nil {
			es := make([]any, len(ep.eps))
			for i, x := range ep.eps {
				es[i] = float64(x)
			}
			got = []any{ep.title, ep.year, float64(ep.season), es}
		}
		gj, _ := json.Marshal(got)
		pj, _ := json.Marshal(c.Ep)
		if string(gj) != string(pj) {
			fail("parseEp(%q,%s)\n  go: %s\n  py: %s", c.Rel, c.Cat, gj, pj)
		}
		mid := s.mediaID("Cat/"+c.Rel, 1)
		gm := []string{mid.typ, mid.key, mid.year, mid.extra, mid.display}
		if strings.Join(gm, "|") != strings.Join(c.Mid, "|") {
			fail("mediaID(%q)\n  go: %v\n  py: %v", c.Rel, gm, c.Mid)
		}
	}
	for _, x := range g.Guess {
		if got := s.guessCategory(x.Rel); got != x.Cat {
			fail("guess(%q) go=%q py=%q", x.Rel, got, x.Cat)
		}
	}
	t.Logf("%d Fälle, %d Abweichungen", len(g.Cases), bad)
}
