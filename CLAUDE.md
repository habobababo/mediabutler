# Media Sorter

Self-hosted Web-Tool (Flask, eine Datei `app.py`), das Downloads in Filme, Serien, Anime und Dokus sortiert und auf ein NFS-Share kopiert. Alle UI-Texte und die Kommunikation sind Deutsch.

## Betrieb
- `docker compose up -d` (Dockerfile nur bei Änderungen neu bauen: `--build`).
- `app.py` ist per Bind-Mount eingebunden: Update = Datei tauschen + `docker restart media-sorter`.
- Pfade: Quelle `/mnt/share/media/downloads` -> `/incoming`, Ziel `/mnt/share/media` -> `/media`, Einstellungen in `./data` -> `/data`.
- Umgebungsvariablen: SOURCE_DIR, TARGET_DIR, DATA_DIR, PUBLIC_URL, AUTH_USER/AUTH_PASS, MIN_AGE, WATCH_INTERVAL, MOVE_AFTER.

## Aufbau von app.py
- Persistenz: JSON in DATA_DIR (config, history, analysis_*, seen, rules) über `load()/save()`.
- Erkennung: `guess_category()` liefert "" bei Unklarheit (Klärungsbedarf, Zeitplan überspringt). `parse_ep()` erkennt S01E02, 1x02, Staffel/Folge/Episode, Ordner-Kontext und "Titel - 001".
- Jellyfin-Benennung: `jf_target()` (Filme `Titel (Jahr)/Titel (Jahr).ext`, Serien `Serie (Jahr)/Season NN/Serie (Jahr) SNNENN.ext`).
- Serien-Regeln (`rules.json`): Umbenennen/Vereinen mit Name, Jahr, TMDB-ID, Kategorie; Schlüssel `skey()` (GT und Z bleiben getrennt).
- Kopier-Job: chunked `.part` + `os.replace`, fsync, Größenprüfung, Prozess-Lock (`fcntl`), Warteschlange in `job["queue"]`.
- Zeitplan: eigener Cron-Parser + Scheduler-Thread; Watcher meldet neue Dateien.
- Telegram: normal (stumm) und dringend, Fortschritt bei 25/50/75 %.
- Codec-Analyse per ffprobe (H.265-Sparpotenzial).
- Oberfläche: eingebettetes HTML/CSS/JS (`PAGE`), Tabs Sortieren, Läuft, Verschoben, Aufräumen, Analyse, Zeitplan, Einstellungen.

## Tests
Es gibt keine Testdateien im Repo. Wichtige Prüfungen: GT/Z-Namen bleiben getrennt, Kopieren mit Größenprüfung, Lückenanzeige bei Folgen, Klärungsfälle.
