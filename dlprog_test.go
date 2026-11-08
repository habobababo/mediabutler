package main

import "testing"

func TestPkgProgress(t *testing.T) {
	items := []*DLItem{
		{Status: "fertig", Size: 100, Done: 100},
		{Status: "lädt", Size: 100, Done: 50},
		{Status: "wartet"}, // Größe unbekannt, wird mit 100 geschätzt
	}
	done, size, _, files, fin := pkgProgress(items)
	if done != 150 || size != 300 || files != 3 || fin != 1 {
		t.Fatalf("done=%d size=%d files=%d fin=%d", done, size, files, fin)
	}
}
