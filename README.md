# MediaButler

Self-hosted Web-Tool in Go: sortiert Downloads in Filme, Serien, Anime und Dokus, benennt sie nach Jellyfin-Schema und kopiert sie auf ein NFS-Share.
Ein statisches Binary, die Web-Oberfläche ist eingebettet.

## Funktionen
- Erkennung und Jellyfin-Benennung (`Titel (Jahr) - Tags.ext`, `Serie (Jahr)/Season 01/…`, TMDB-/TVDB-IDs), eigene Regeln, Gruppen und Zielordner
- Kopier-Warteschlange (`.part`, fsync, Größenprüfung), Zeitplan (Cron), Verschieben statt Kopieren, Aufräumen, Sample-Dateien werden gelöscht
- Entpacken von RAR/ZIP/7z (mehrteilig, Passwortliste, Hook für pyLoad)
- Duplikate und Abgleich der Neuzugänge mit der Bibliothek, Codec-Analyse (ffprobe)
- Metadaten-Suche (TMDB, TVDB, AniList, MyAnimeList), Telegram-Meldungen, Jellyfin-Scan
- Speicherbalken, Login (Basic Auth), Update-Knopf (Release-Asset)

## Starten
    docker compose up -d --build

Konfiguration über Umgebungsvariablen in `docker-compose.yml` (SOURCE_DIR, TARGET_DIR, DATA_DIR, PUBLIC_URL, AUTH_USER/AUTH_PASS, API_KEY, MIN_AGE, WATCH_INTERVAL, MOVE_AFTER, JELLYFIN_*, TMDB_KEY, TVDB_KEY).
Einstellungen und Verlauf liegen als JSON in `./data`.

## Entwickeln
    go test ./...
    go build -o mediabutler .

Die Web-Oberfläche liegt in `web/page.html`. `testdata/golden.json` enthält Referenzfälle für die Benennung.

## Update-Knopf
Jeder Push auf `main` mit Code-Änderungen startet `.github/workflows/go-release.yml` (Tests, Build für amd64 und arm64, neues Release `v<Laufnummer>`).
Der Knopf in den Einstellungen lädt das passende Asset `mediabutler-linux-<arch>`, sichert die alte Datei (`DATA_DIR/mediabutler.bak`), ersetzt das Programm und beendet es; `restart: unless-stopped` startet den Container neu.
Ein neues Image (`docker compose build`) ersetzt solche Updates wieder. Für ein privates Repo braucht der Knopf ein Token mit Lesezugriff auf Contents.


## Download-Manager

Neuer Bereich „Downloads": HTTP/HTTPS-Links und ddownload.com (Premium per API-Key oder Login). Teile wie `.part01.rar` werden zu einem Paket gruppiert, Downloads sind wiederaufnehmbar (`.mbdl`-Datei bis zum Abschluss). Standard-Zielordner ist `<SOURCE_DIR>/Downloads`, dort sortiert und entpackt MediaButler danach wie gewohnt.

- **Cookies** (Hoster-Login) werden dauerhaft in `cookies.json` im Datenordner gespeichert. Zusätzlich lässt sich ein Cookie-Header aus dem Browser einfügen.
- **Click'n'Load**: Port `9666` veröffentlichen (`CNL_PORT`, `0` schaltet ihn ab) oder das Chrome-Plugin in `chrome-extension/` nutzen (Chrome → Erweiterungen → Entwicklermodus → „Entpackte Erweiterung laden"). Das Plugin leitet `127.0.0.1:9666` auf den Server um und bietet „An MediaButler senden" im Kontextmenü. Server-Adresse und `API_KEY` im Plugin eintragen.
- Testumgebung: `DDL_BASE` und `DDL_API` überschreiben die ddownload-Adressen.

### ddownload (Premium)

Die API liefert Direktlinks nur für eigene Dateien. Für fremde Links nutzt MediaButler wie pyLoad die Web-Anmeldung. Weil der Login ein Captcha hat, kommt die Sitzung aus dem Browser: Chrome-Plugin installieren, bei ddownload.com anmelden, die Cookies werden automatisch gesendet (oder per Button „Cookies senden“). Alternativ den Cookie-Header unter Einstellungen → Downloads einfügen. Die Cookies werden dauerhaft gespeichert. „Testen“ in den Einstellungen zeigt jeden Schritt.

### NFO-Dateien für Jellyfin

Nach dem Kopieren legt MediaButler zu Titeln mit bestätigter TMDB-/TVDB-ID eine NFO an: bei Filmen `<Datei>.nfo`, bei Serien `tvshow.nfo` im Serienordner (Titel, Jahr, Handlung, Genres, IDs). Vorhandene NFOs werden nie überschrieben. Abschalten mit `NFO=0`.
