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
`git tag v1.2.0 && git push --tags` startet `.github/workflows/go-release.yml` (Tests, Build für amd64 und arm64, Release).
Der Knopf in den Einstellungen lädt das passende Asset `mediabutler-linux-<arch>`, sichert die alte Datei (`DATA_DIR/mediabutler.bak`), ersetzt das Programm und beendet es; `restart: unless-stopped` startet den Container neu.
Ein neues Image (`docker compose build`) ersetzt solche Updates wieder. Für ein privates Repo braucht der Knopf ein Token mit Lesezugriff auf Contents.
