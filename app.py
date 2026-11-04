#!/usr/bin/env python3
"""
Media Sorter – sortiert Filme, Serien, Anime und Dokus und kopiert/verschiebt sie auf ein NFS-Share.
Web-UI mit Fortschrittsbalken, Verlauf, Aufräumen und eingebautem Cron-Zeitplan.

Umgebungsvariablen:
  SOURCE_DIR  Quelle (Standard /incoming)       TARGET_DIR  Ziel = NFS-Mount (Standard /media)
  DATA_DIR    Einstellungen + Verlauf (/data)   MOVE_AFTER  Startwert "Quelle löschen" (0/1)
  MIN_AGE     Dateien/Ordner jünger als X Sekunden werden im Auto-Modus ignoriert (120)
  PORT        Webport (8080)

CLI (z.B. für System-Cron):  python app.py --auto [--dry-run]
"""
import fcntl, hashlib, hmac, json, os, re, shutil, subprocess, sys, threading, time, urllib.error, urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from flask import Flask, jsonify, request, Response

SOURCE = Path(os.environ.get("SOURCE_DIR", "/incoming")).resolve()
TARGET = Path(os.environ.get("TARGET_DIR", "/media")).resolve()
DATA = Path(os.environ.get("DATA_DIR", "/data"))
PORT = int(os.environ.get("PORT", "8080"))
AUTH_USER = os.environ.get("AUTH_USER", "admin")
AUTH_PASS = os.environ.get("AUTH_PASS", "")   # leer = kein Login (z.B. wenn der Reverse Proxy schon schützt)
MIN_AGE = int(os.environ.get("MIN_AGE", "120"))
LOCKFILE = "/tmp/media-sorter.lock"
VERSION = "1.1.0"
BUILD = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:7]    # zeigt, welche app.py wirklich läuft
CATEGORIES = ["Filme", "Serien", "Anime", "Dokus"]
VIDEO_EXT = {".mkv", ".mp4", ".avi", ".m4v", ".mov", ".wmv", ".ts", ".mpg", ".mpeg", ".srt", ".ass", ".sub"}
PARTIAL_EXT = {".part", ".crdownload", ".aria2", ".!ut", ".tmp", ".jdownloader", ".download"}
ARCHIVE_EXT = {".rar", ".zip", ".7z", ".tar", ".gz", ".iso"}
CHUNK = 4 * 1024 * 1024
CLI_MODE = False
WD = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"]

app = Flask(__name__)

@app.before_request
def basic_auth():
    if not AUTH_PASS: return
    a = request.authorization
    if (a and hmac.compare_digest((a.username or "").encode(), AUTH_USER.encode())
            and hmac.compare_digest((a.password or "").encode(), AUTH_PASS.encode())):
        return
    return Response("Login erforderlich", 401, {"WWW-Authenticate": 'Basic realm="Media Sorter", charset="UTF-8"'})
lock = threading.Lock()
job = {"running": False, "cancel": False, "total": 0, "done": 0, "files_total": 0, "files_done": 0,
       "current": "", "current_size": 0, "current_done": 0, "speed": 0, "errors": [], "finished": False,
       "queue": [], "started": 0, "ended": 0}

# ---------- Einstellungen & Verlauf (JSON in DATA_DIR) ----------
DEFAULT_CFG = {"enabled": False, "cron": "0 3 * * *", "move_after": os.environ.get("MOVE_AFTER", "0") == "1",
               "cleanup_after": False, "last_run": "", "last_result": "",
               "tg_enabled": False, "tg_token": "", "tg_chat": "", "tg_start": True, "tg_done": True,
               "tg_unclear": True, "tg_unclear_sig": "", "tg_new": True, "tg_progress": True,
               "tg_errors": True, "tg_silent": True, "jellyfin": True, "public_url": os.environ.get("PUBLIC_URL", "").rstrip("/")}
_mem, _flock = {}, threading.Lock()

def load(name, default):
    try:
        return json.loads((DATA / name).read_text())
    except Exception:
        return _mem.get(name, default)

def save(name, data):
    with _flock:
        _mem[name] = data
        try:
            DATA.mkdir(parents=True, exist_ok=True)
            tmp = DATA / (name + ".tmp")
            tmp.write_text(json.dumps(data, ensure_ascii=False))
            os.replace(tmp, DATA / name)
        except Exception as e:
            print("Speichern fehlgeschlagen:", e, file=sys.stderr)

def cfg():
    return {**DEFAULT_CFG, **load("config.json", {})}

def save_cfg(**ch):
    c = load("config.json", {}); c.update(ch); save("config.json", c)

def _persist_ok():
    try:
        DATA.mkdir(parents=True, exist_ok=True)
        t = DATA / ".w"; t.write_text("x"); t.unlink(); return True
    except Exception:
        return False
PERSIST = _persist_ok()

def push_history(it, size, status):
    h = load("history.json", [])
    h.append({"t": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "cat": it["dst"].parts[0],
              "name": it["src"].name, "full": str(TARGET / it["dst"]), "size": size, "status": status})
    save("history.json", h[-2000:])

def _sig(p):
    st = p.stat(); return [st.st_size, int(st.st_mtime)]

def mark_copied(src):
    """Quelle als erledigt merken (Größe + Änderungszeit), damit sie in Sortieren und im Zeitplan nicht erneut auftaucht."""
    try:
        if not src.exists(): return
        m = load("copied.json", {}); m[src.relative_to(SOURCE).as_posix()] = _sig(src)
        save("copied.json", dict(list(m.items())[-20000:]))
    except Exception as e:
        print("copied.json:", e, file=sys.stderr)

def is_copied(cm, p, rel):
    try: return cm.get(rel) == _sig(p)
    except OSError: return False

# ---------- Cron ----------
def _field(txt, lo, hi):
    out = set()
    for part in txt.split(","):
        step = 1
        if "/" in part:
            part, st = part.split("/", 1); step = int(st)
            if step < 1: raise ValueError
        if part == "*":
            a, b = lo, hi
        elif "-" in part:
            a, b = (int(x) for x in part.split("-", 1))
        else:
            a = int(part); b = hi if step > 1 else a
        if not (lo <= a <= b <= hi): raise ValueError
        out.update(range(a, b + 1, step))
    return out

class Cron:
    def __init__(self, expr):
        f = expr.split()
        if len(f) != 5:
            raise ValueError("Cron braucht 5 Felder: Minute Stunde Tag Monat Wochentag")
        try:
            self.min, self.hour = _field(f[0], 0, 59), _field(f[1], 0, 23)
            self.dom, self.mon = _field(f[2], 1, 31), _field(f[3], 1, 12)
            self.dow = {x % 7 for x in _field(f[4], 0, 7)}
        except ValueError:
            raise ValueError("Ungültiger Wert. Erlaubt: Zahlen, *, Bereiche (1-5), Listen (1,3) und Schritte (*/15)") from None
        self.dom_star, self.dow_star = f[2].startswith("*"), f[4].startswith("*")

    def day_ok(self, t):
        d, w = t.day in self.dom, (t.weekday() + 1) % 7 in self.dow
        if self.dom_star and self.dow_star: return True
        if self.dom_star: return w
        if self.dow_star: return d
        return d or w

    def match(self, t):
        return t.minute in self.min and t.hour in self.hour and t.month in self.mon and self.day_ok(t)

    def next_after(self, after):
        t = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(20000):
            if t.month not in self.mon:
                t = (t.replace(day=1, hour=0, minute=0) + timedelta(days=32)).replace(day=1)
            elif not self.day_ok(t):
                t = t.replace(hour=0, minute=0) + timedelta(days=1)
            elif t.hour not in self.hour:
                t = t.replace(minute=0) + timedelta(hours=1)
            elif t.minute not in self.min:
                t += timedelta(minutes=1)
            else:
                return t
        return None

def next_runs(expr, n=3):
    c, t, out = Cron(expr), datetime.now(), []
    for _ in range(n):
        t = c.next_after(t)
        if not t: break
        out.append(t)
    return out

def fmt_dt(t):
    return f"{WD[t.weekday()]} {t:%d.%m.%Y %H:%M}"

# ---------- Erkennung & Zielpfad ----------
def clean(s):
    return re.sub(r"\s+", " ", re.sub(r"[._]+", " ", s)).strip(" -")

YEAR = re.compile(r"(?<![\dA-Za-z])(19[2-9]\d|20[0-3]\d)(?![\dA-Za-z])")

def guess_category(rel):
    """Kategorie oder "" wenn unklar (= Klärungsbedarf, Zeitplan überspringt die Datei)."""
    low, name = rel.lower(), Path(rel).name
    ov = get_paths().get(rel)
    if ov and ov.get("cat") in CATEGORIES: return ov["cat"]
    g = get_mg().get(rel)
    if g and g.get("cat") in CATEGORIES: return g["cat"]
    r = rule_for_path(rel)
    if r and r.get("category") in CATEGORIES: return r["category"]
    if re.search(r"doku|documentary|docu\b|dokumentation", low): return "Dokus"
    if "anime" in low or re.match(r"^\[[^\]]+\]", name): return "Anime"
    if re.search(r"[Ss]\d{1,2}[ ._-]*[Ee]\d{1,3}|(?<!\d)\d{1,2}x\d{2}(?!\d)", name): return "Serien"
    if parse_ep(rel, False, False, explicit=True): return "Serien"
    if YEAR.search(rel): return "Filme"
    return ""

def legacy_target(rel, cat):
    name, stem = Path(rel).name, Path(rel).stem
    if cat in ("Serien", "Anime"):
        m = re.match(r"^(.*?)[ ._-]+[Ss](\d{1,2})[ ._-]*[Ee]\d{1,3}", name)
        if m: return Path(cat) / clean(m.group(1)) / f"Season {int(m.group(2)):02d}" / name
        m = re.match(r"^\[[^\]]+\]\s*(.+?)\s+-\s+\d+", name)
        if m: return Path(cat) / clean(m.group(1)) / "Season 01" / name
        return Path(cat) / clean(stem) / name
    if cat == "Filme": return Path(cat) / clean(stem) / name
    return Path(cat) / name

SUB_EXT = {".srt", ".ass", ".sub"}
LANGS = {"de", "deu", "ger", "german", "deutsch", "en", "eng", "english", "fr", "fra", "french", "es", "spa",
         "ja", "jpn", "it", "ita", "nl", "pt", "ru", "forced", "sdh", "cc"}
QUALITY = re.compile(r"(?<![A-Za-z0-9])(?:\d{3,4}[pi]|4k|uhd|blu-?ray|bdrip|brrip|web-?dl|web-?rip|hdtv|dvdrip|remux|"
                     r"x26[45]|h26[45]|hevc|xvid|german|ac3|dts)(?![A-Za-z0-9])", re.I)
EPISODE = re.compile(r"[Ss](\d{1,2})[ ._-]*[Ee](\d{1,3})((?:-?[Ee]\d{1,3})*)|(?<![\dA-Za-z])(\d{1,2})x(\d{2,3})(?![\dA-Za-z])")
ANIME_EP = re.compile(r"^\s*(?:\[[^\]]*\]\s*)*(.+?)\s+-\s+(\d{1,4})(?:v\d)?(?!\d)")
LANGMAP = {"german": "de", "deutsch": "de", "ger": "de", "deu": "de", "english": "en", "eng": "en", "french": "fr",
           "fra": "fr", "spa": "es", "jpn": "ja", "ita": "it"}
ILLEGAL = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

def _pretty(s):
    s = re.sub(r"\[((?:19|20)\d\d)\]", r"(\1)", s)          # [1999] -> (1999)
    s = re.sub(r"\[[^\]]*\]|\{[^}]*\}", " ", s)              # [Gruppe], {tmdbid-..}
    s = re.sub(r"[._]+", " ", s)
    return re.sub(r"\s+", " ", s).strip(" -")

def _safe(s):
    return ILLEGAL.sub("", s).strip(" .")

def _title_year(s):
    """'Blade.Runner.2049.2017.1080p' -> ('Blade Runner 2049', '2017')"""
    s = _pretty(s)
    q = QUALITY.search(s)
    limit = q.start() if q else len(s)
    best = None
    for m in YEAR.finditer(s):
        if m.end() <= limit and s[:m.start()].strip(" (-"):
            best = m
    if best:
        return s[:best.start()].strip(" (-"), best.group(1)
    return s[:limit].strip(" (-"), ""

def _episode(stem, anime):
    m = EPISODE.search(stem)
    if m:
        raw = stem[:m.start()]
        if m.group(1):
            season = int(m.group(1))
            eps = [int(m.group(2))] + [int(x) for x in re.findall(r"[Ee](\d{1,3})", m.group(3) or "")]
        else:
            season, eps = int(m.group(4)), [int(m.group(5))]
    elif anime:
        a = ANIME_EP.match(stem)
        if not a: return None
        raw, season, eps = a.group(1), 1, [int(a.group(2))]
        s2 = re.search(r"\s[Ss](\d{1,2})$", raw.rstrip())
        if s2: season, raw = int(s2.group(1)), raw.rstrip()[:s2.start()]
    else:
        return None
    title, year = _title_year(raw)
    return (title, year, season, eps) if title else None


# ---------- Serien: Erkennung (Staffel/Folge, Ordner-Kontext) & Merk-Regeln ----------
WORD_SEASON = re.compile(r"(?<![A-Za-z])(?:staffel|season|saison)[ ._-]*(\d{1,2})(?![\dA-Za-z])", re.I)
WORD_EP = re.compile(r"(?<![A-Za-z])(?:folge|episode|ep|eps)[ ._-]*(\d{1,4})(?![\dA-Za-z])", re.I)
SEASON_DIR = re.compile(r"^(?:staffel|season|saison|series|s)[ ._-]*(\d{1,2})$", re.I)

def skey(title):
    """Vergleichsschlüssel: 'Dragon Ball GT' == 'DragonBall.GT', aber != 'Dragonball Z'."""
    t = re.sub(r"\(\s*(?:19|20)\d\d\s*\)", "", title or "")
    return re.sub(r"[^0-9a-zäöüß]+", "", t.lower())

def _ctx(dirs):
    """(Serientitel aus Ordner, Staffel aus Ordner) – von unten nach oben."""
    season = title = None
    for d in reversed(dirs):
        pd = _pretty(d)
        m = SEASON_DIR.match(pd)
        if m:
            season = season or int(m.group(1)); continue
        ws = WORD_SEASON.search(pd)
        if ws:
            season = season or int(ws.group(1)); pd = _pretty(pd[:ws.start()] + " " + pd[ws.end():])
        if pd and not re.fullmatch(r"(?i)downloads?|incoming|complete|fertig|serien|series|anime|tv|shows?", pd):
            title = pd
        break
    return title, season

def parse_ep(rel, anime=False, loose=False, explicit=False, stem=None):
    """-> (Titel, Jahr, Staffel, [Folgen]) oder None.
    explicit: nur sichere Muster (S01E02, 1x02, Staffel/Folge-Wörter). loose: zusätzlich 'Titel 001'."""
    p = Path(rel); stem = p.stem if stem is None else stem
    ctx_title, ctx_season = _ctx(p.parts[:-1])
    m = EPISODE.search(stem)
    if m:
        r = _episode(stem, anime)
        if r:
            t, y, sn, eps = r
            if ctx_title and skey(t) == "" : t = ctx_title
            return t, y, sn, eps
    me = WORD_EP.search(stem)
    if me:
        ws = WORD_SEASON.search(stem)
        season = int(ws.group(1)) if ws else (ctx_season or 1)
        cut = min(x.start() for x in (me, ws) if x)
        raw = stem[:cut]
        title, year = _title_year(raw)
        title = title or ctx_title or ""
        return (title, year, season, [int(me.group(1))]) if title else None
    if explicit: return None
    if anime or loose:
        a = ANIME_EP.match(stem)
        if a:
            raw, season = a.group(1), ctx_season or 1
            s2 = re.search(r"\s[Ss](\d{1,2})$", raw.rstrip())
            if s2: season, raw = int(s2.group(1)), raw.rstrip()[:s2.start()]
            title, year = _title_year(raw)
            if title: return title, year, season, [int(a.group(2))]
    if loose:
        pt = _pretty(stem); q = QUALITY.search(pt)
        pt = (pt[:q.start()] if q else pt).strip(" -")
        t = re.match(r"^(.*?[A-Za-zÄÖÜäöü].*?)[ -]+(\d{1,4})$", pt)
        if t and not YEAR.fullmatch(t.group(2)):
            title, year = _title_year(t.group(1))
            if title: return title, year, ctx_season or 1, [int(t.group(2))]
        if re.fullmatch(r"\d{1,4}", pt) and ctx_title:
            return ctx_title, "", ctx_season or 1, [int(pt)]
    return None

_rules_cache = {"v": None}
def get_rules():
    if _rules_cache["v"] is None:
        _rules_cache["v"] = [r for r in load("rules.json", []) if isinstance(r, dict) and r.get("key")]
    return _rules_cache["v"]

def set_rules(rules):
    _rules_cache["v"] = rules; save("rules.json", rules)

def rule_for(title):
    k = skey(title)
    for r in get_rules():
        if r["key"] == k: return r
    return None

def rule_for_path(rel):
    ep = parse_ep(rel, False, True)
    return rule_for(ep[0]) if ep else None

# Eigene Gruppen (groups.json): {Quelldatei: {name, year, tmdb, cat, season, ep}} - für alles, was die Erkennung nicht oder falsch zuordnet
_mg_cache = {"v": None}
def get_mg():
    if _mg_cache["v"] is None:
        d = load("groups.json", {})
        _mg_cache["v"] = d if isinstance(d, dict) else {}
    return _mg_cache["v"]

def set_mg(d):
    _mg_cache["v"] = d; save("groups.json", d)

def _nat(s):
    return [int(x) if x.isdigit() else x for x in re.split(r"(\d+)", s.lower())]

def mg_build(d):
    """Eintrag pro Datei aus den Angaben des Dialogs; wirft ValueError bei ungültigen Angaben."""
    rels = sorted(dict.fromkeys((d.get("items") or [])[:5000]), key=_nat)
    for r in rels: safe_source(r)
    name = _safe(_pretty((d.get("name") or "").strip()))
    year = re.sub(r"\D", "", str(d.get("year") or ""))[:4]
    tmdb = re.sub(r"\D", "", str(d.get("tmdb") or ""))[:10]
    cat = d.get("category")
    if not rels: raise ValueError("Keine Dateien ausgewählt.")
    if not name: raise ValueError("Bitte einen Namen eingeben.")
    if cat not in ("Serien", "Anime", "Dokus"): raise ValueError("Kategorie muss Serien, Anime oder Dokus sein.")
    if year and not YEAR.fullmatch(year): raise ValueError("Jahr ungültig.")
    try: season = max(0, min(99, int(d.get("season") or 1)))
    except (TypeError, ValueError): raise ValueError("Staffel ungültig.")
    try: start = max(0, int(d.get("start") if d.get("start") not in (None, "") else 1))
    except (TypeError, ValueError): raise ValueError("Startnummer ungültig.")
    eps = {}
    if d.get("mode") == "renumber":
        eps = {r: start + i for i, r in enumerate(rels)}
    else:                                           # erkannte Nummern behalten, fehlende hinten anhängen
        for r in rels:
            e = parse_ep(r, False, True)
            if e and e[3]: eps[r] = e[3][0]
        nxt = max(eps.values(), default=start - 1)
        for r in rels:
            if r not in eps: nxt += 1; eps[r] = nxt
    return {r: {"name": name, "year": year, "tmdb": tmdb, "cat": cat, "season": season, "ep": eps[r]} for r in rels}

def mg_path(rel, cat, g):
    show = _safe(f"{g['name']} ({g['year']})" if g.get("year") else g["name"])
    tag = f" [tmdbid-{g['tmdb']}]" if g.get("tmdb") else ""
    return Path(cat) / (show + tag) / f"Season {g['season']:02d}" / f"{show} S{g['season']:02d}E{g['ep']:02d}{Path(rel).suffix.lower()}"

def series_info(rel):
    """Für die Oberfläche: erkannter Serienname, Schlüssel, Staffel, Folge."""
    g = get_mg().get(rel)
    if g:
        return {"name": g["name"], "orig": g["name"], "year": g.get("year", ""), "rkey": "g:" + skey(g["name"]),
                "key": "g:" + skey(g["name"]) + g.get("year", ""), "merged": False, "manual": True,
                "season": g["season"], "ep": g["ep"], "eps": [g["ep"]]}
    ep = parse_ep(rel, False, True)
    if not ep: return None
    t, y, sn, eps = ep
    r = rule_for(t)
    shown = (r.get("name") or t) if r else t
    return {"name": shown, "orig": t, "year": (r.get("year") if r else y) or "", "rkey": skey(t),
            "key": skey(shown) + (r.get("year", "") if r else ""), "merged": bool(r), "season": sn, "ep": eps[0], "eps": eps}

_EPS = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,3})((?:-[Ee]\d{1,3})*)")
def library_eps(show_dirs):
    """Folgen, die schon im Ziel liegen: {Serienordner: {Staffel: [Folgen]}}."""
    out = {}
    for sd in show_dirs:
        d = TARGET / sd; res = {}
        try:
            for f in d.rglob("*"):
                if f.suffix.lower() not in VIDEO_EXT: continue
                m = _EPS.search(f.name)
                if m:
                    res.setdefault(int(m.group(1)), set()).update([int(m.group(2))] + [int(x) for x in re.findall(r"[Ee](\d{1,3})", m.group(3))])
        except OSError:
            pass
        out[sd] = {str(k): sorted(v) for k, v in res.items()}
    return out

def jf_target(rel, cat):
    """Jellyfin-Schema: Titel (Jahr)/Titel (Jahr).mkv  bzw.  Serie (Jahr)/Season 01/Serie (Jahr) S01E02.mkv"""
    name = Path(rel).name
    p = Path(name)
    ext, stem, extra = p.suffix.lower(), p.stem, ""
    if ext in SUB_EXT:                       # Movie.2020.de.forced.srt -> Sprachkürzel behalten
        parts, tail = stem.split("."), []
        while len(parts) > 1 and parts[-1].lower() in LANGS:
            t = parts.pop().lower(); tail.insert(0, LANGMAP.get(t, t))
        stem, extra = ".".join(parts), "".join("." + t for t in tail)
    ep = parse_ep(rel, cat == "Anime", loose=cat in ("Serien", "Anime"), stem=stem) if cat in ("Serien", "Anime", "Dokus") else None
    if ep:
        title, year, season, eps = ep
        tag = ""
        r = rule_for(title)
        if r:
            title, year = r.get("name") or title, r.get("year", "")
            if r.get("tmdb"): tag = f" [tmdbid-{r['tmdb']}]"
        show = _safe(f"{title} ({year})" if year else title)
        es = f"S{season:02d}E{eps[0]:02d}" + "".join(f"-E{n:02d}" for n in eps[1:])
        return Path(cat) / (show + tag) / f"Season {season:02d}" / f"{show} {es}{extra}{ext}"
    if cat in ("Serien", "Anime"):           # keine Folgennummer erkennbar: Dateiname bleibt
        return Path(cat) / (_safe(_pretty(stem)) or "Unbekannt") / name
    title, year = _title_year(stem)
    if title:
        base = _safe(f"{title} ({year})" if year else title)
        return Path(cat) / base / f"{base}{extra}{ext}"
    return Path(cat) / name

def default_target(rel, cat):
    g = get_mg().get(rel)
    if g and cat in ("Serien", "Anime", "Dokus"): return mg_path(rel, cat, g)
    return jf_target(rel, cat) if cfg()["jellyfin"] else legacy_target(rel, cat)

# Eigene Zielordner (paths.json): {Quelldatei: {"folder": Zielordner, "n": ersetzte Pfadteile, "cat": Kategorie}}
_paths_cache = {"v": None}
def get_paths():
    if _paths_cache["v"] is None:
        d = load("paths.json", {})
        _paths_cache["v"] = d if isinstance(d, dict) else {}
    return _paths_cache["v"]

def set_paths(paths):
    _paths_cache["v"] = paths; save("paths.json", paths)

def target_for(rel, cat):
    ov = get_paths().get(rel)
    if not ov or ov.get("cat") != cat: return default_target(rel, cat)
    if "folder" not in ov: return Path(ov["dst"])    # Eintrag im alten Format
    return Path(ov["folder"], *default_target(rel, cat).parts[ov["n"]:])

def clean_folder(s):
    """'anime/Dragonball/' -> ['Anime', 'Dragonball']; '..' und unerlaubte Zeichen fallen weg."""
    parts = [x for x in (_safe(y) for y in re.split(r"[\\/]+", (s or "").strip())) if x]
    cat = next((c for c in CATEGORIES if parts and c.lower() == parts[0].lower()), None)
    if not cat: raise ValueError("Der Zielordner muss mit einer Kategorie beginnen: " + ", ".join(CATEGORIES))
    return [cat] + parts[1:]

def folder_targets(rels, folder):
    """Ersetzt den gemeinsamen Anfang der Standardpfade (höchstens Kategorie/Serie) durch den Zielordner.
    Eine Serie wird so umgezogen, mehrere Serien landen getrennt in Unterordnern davon."""
    parts = clean_folder(folder)
    defaults = [default_target(r, parts[0]).parts for r in rels]
    n = min([2] + [len(d) - 1 for d in defaults])
    while n > 1 and len({d[:n] for d in defaults}) > 1: n -= 1
    return parts, n, [Path(*parts, *d[n:]) for d in defaults]

TARGET_SEASON = re.compile(r"Season \d+")
def show_dir(target):
    """Serienordner eines Zielpfads: alles vor 'Season NN', sonst Kategorie/Name."""
    parts = Path(target).parts
    i = next((n for n, x in enumerate(parts) if TARGET_SEASON.fullmatch(x)), 0)
    return "/".join(parts[:i] if i > 1 else parts[:2])

def safe_source(rel):
    p = (SOURCE / rel).resolve()
    if SOURCE not in p.parents or not p.is_file():
        raise ValueError(f"Ungültiger Pfad: {rel}")
    return p

SAMPLE = re.compile(r"\.sample\.[^.]+$", re.I)    # film.sample.mkv, aber nicht Free.Sample.2016.mkv
def purge_samples():
    """Löscht fertige Sample-Dateien im Quellordner (nie im Ziel) und protokolliert sie."""
    now = time.time()
    for p in SOURCE.rglob("*"):
        try:
            if SAMPLE.search(p.name) and p.is_file() and now - p.stat().st_mtime >= MIN_AGE:
                p.unlink(); print("Sample gelöscht:", p, file=sys.stderr)
        except OSError as e:
            print("Sample nicht gelöscht:", p, e, file=sys.stderr)

def scan():
    """Fertige Dateien (älter als MIN_AGE): (automatisch zuordenbar, unklar)."""
    now, items, unclear, cm = time.time(), [], [], load("copied.json", {})
    for p in sorted(SOURCE.rglob("*")):
        if not (p.is_file() and p.suffix.lower() in VIDEO_EXT) or p.name.startswith(".") or SAMPLE.search(p.name): continue
        if now - p.stat().st_mtime < MIN_AGE: continue
        rel = p.relative_to(SOURCE).as_posix()
        if is_copied(cm, p, rel): continue
        cat = guess_category(rel)
        if cat: items.append({"src": p, "dst": target_for(rel, cat)})
        else: unclear.append(rel)
    return items, unclear

# ---------- Aufräumen ----------
def dir_info(d):
    files = []
    for f in d.rglob("*"):
        try:
            if f.is_file(): files.append((f, f.stat()))
        except OSError: pass
    ext = lambda f: f.suffix.lower()
    media = sum(ext(f) in VIDEO_EXT for f, _ in files)
    partial = any(ext(f) in PARTIAL_EXT for f, _ in files)
    arch = any(ext(f) in ARCHIVE_EXT or re.fullmatch(r"\.(r\d{2,3}|\d{3})", ext(f)) for f, _ in files)
    newest = max((s.st_mtime for _, s in files), default=d.stat().st_mtime)
    reason = ("enthält noch Medien" if media else "wird noch geschrieben" if partial
              else "kürzlich geändert" if time.time() - newest < MIN_AGE else "")
    return {"path": d.name, "type": "dir", "files": len(files), "size": sum(s.st_size for _, s in files), "media": media,
            "archives": arch, "locked": reason, "sample": [f.name for f, _ in files[:3]]}

def top_dirs():
    return sorted(p for p in SOURCE.iterdir() if p.is_dir() and not p.name.startswith("."))

def remove_empty_dirs():
    n = 0
    for d in sorted((p for p in SOURCE.rglob("*") if p.is_dir()), reverse=True):
        try:
            if not d.name.startswith(".") and not any(d.iterdir()) and time.time() - d.stat().st_mtime >= MIN_AGE:
                d.rmdir(); n += 1
        except OSError: pass
    return n

def entry_info(p):
    if p.is_dir():
        return dir_info(p)
    s, e = p.stat(), p.suffix.lower()
    reason = ("ist noch ein Medium" if e in VIDEO_EXT else "wird noch geschrieben" if e in PARTIAL_EXT
              else "kürzlich geändert" if time.time() - s.st_mtime < MIN_AGE else "")
    return {"path": p.name, "type": "file", "files": 1, "size": s.st_size, "media": int(e in VIDEO_EXT),
            "archives": e in ARCHIVE_EXT or bool(re.fullmatch(r"\.(r\d{2,3}|\d{3})", e)),
            "locked": reason, "sample": []}

def top_entries():
    return sorted(p for p in SOURCE.iterdir() if not p.name.startswith("."))

def delete_entry(name, force=False):
    p = SOURCE / name
    if p.parent != SOURCE or not (p.is_dir() or p.is_file()):
        raise ValueError(f"Ungültiger Eintrag: {name}")
    if not force:
        info = entry_info(p)
        if info["locked"]:
            raise ValueError(f"{name}: {info['locked']}")
    if p.is_symlink() or p.is_file():
        p.unlink()
    else:
        shutil.rmtree(p)

def cleanup_leftovers():
    """Automatisch: Ordner ohne Medien, ohne Teil-Downloads, ohne Archive, nicht frisch + leere Ordner."""
    n = 0
    for d in top_dirs():
        i = dir_info(d)
        if not i["locked"] and not i["archives"]:
            shutil.rmtree(d, ignore_errors=True); n += 1
    return n + remove_empty_dirs()

# ---------- Kopier-Job ----------
def _run_job(items, source, move):
    ok = []
    run_t0, marks = time.time(), [25, 50, 75]
    with lock:
        job.update(running=True, cancel=False, finished=False, errors=[], done=0, files_done=0,
                   files_total=len(items), current="", speed=0, source=source,
                   total=sum(i["src"].stat().st_size for i in items if i["src"].exists()),
                   started=time.time(), ended=0,
                   queue=[{"src": i["src"].relative_to(SOURCE).as_posix(), "name": i["dst"].as_posix(), "size": (i["src"].stat().st_size if i["src"].exists() else 0), "state": "wartet"} for i in items])
    for qi, it in enumerate(items):
        if job["cancel"]: break
        src, dst, part = it["src"], TARGET / it["dst"], None
        q = job["queue"][qi]
        try:
            size = src.stat().st_size
            with lock:
                job.update(current=it["dst"].as_posix(), current_size=size, current_done=0); q["state"] = "kopiert"
            if dst.exists() and dst.stat().st_size == size:
                if move:
                    src.unlink(); push_history(it, size, "war schon vorhanden, Quelle gelöscht"); ok.append((it, size))
                else: mark_copied(src)
                with lock:
                    job["done"] += size; job["files_done"] += 1; q["state"] = "vorhanden"
                continue
            dst.parent.mkdir(parents=True, exist_ok=True)
            part = dst.with_name(dst.name + ".part")
            t0 = time.time()
            with open(src, "rb") as fi, open(part, "wb") as fo:
                while True:
                    if job["cancel"]: raise InterruptedError("Abgebrochen")
                    buf = fi.read(CHUNK)
                    if not buf: break
                    fo.write(buf)
                    with lock:
                        job["done"] += len(buf); job["current_done"] += len(buf)
                        if time.time() - t0 > 0.5:
                            job["speed"] = job["current_done"] / (time.time() - t0)
                    if marks and job["total"]:
                        pct = job["done"] * 100 / job["total"]
                        while marks and pct >= marks[0]:
                            m = marks.pop(0)
                            if time.time() - run_t0 >= PROGRESS_MIN: _progress_note(m, source, run_t0)
                fo.flush(); os.fsync(fo.fileno())
            os.replace(part, dst)
            try: shutil.copystat(src, dst)
            except OSError: pass
            if dst.stat().st_size != size:
                raise IOError("Größe am Ziel stimmt nicht")
            if move: src.unlink()
            push_history(it, size, "verschoben" if move else "kopiert")
            mark_copied(src)
            ok.append((it, size))
            with lock:
                job["files_done"] += 1; q["state"] = "fertig"
        except Exception as e:
            with lock:
                job["errors"].append(f"{it['dst'].name}: {e}"); q["state"] = "fehler"; q["err"] = str(e)
            if part:
                try: part.unlink(missing_ok=True)
                except Exception: pass
    with lock:
        for q in job["queue"]:
            if q["state"] in ("wartet", "kopiert"): q["state"] = "abgebrochen"
        job.update(running=False, finished=True, current="", ended=time.time())
    return ok

def _acquire():
    fd = open(LOCKFILE, "w")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB); return fd
    except OSError:
        fd.close(); return None

def busy():
    fd = _acquire()
    if fd is None: return True
    fcntl.flock(fd, fcntl.LOCK_UN); fd.close(); return False

def hsize(b):
    for u in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024 or u == "TB":
            return f"{int(b)} B" if u == "B" else f"{b:.1f} {u}"
        b /= 1024

def hdur(sec):
    m, s = divmod(int(sec), 60)
    return f"{m} min {s} s" if m else f"{s} s"

def ui_link(frag=""):
    """Link auf die Web-UI (z.B. hinter dem Reverse Proxy); leer, wenn keine Adresse eingestellt ist."""
    base = cfg()["public_url"].rstrip("/")
    return "" if not base else (f"{base}/#{frag}" if frag else base + "/")

def tg_send(text, token=None, chat=None, urgent=True):
    c = cfg()
    token, chat = token or c["tg_token"], chat or c["tg_chat"]
    if not token or not chat:
        raise ValueError("Bot-Token und Chat-ID fehlen")
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data=json.dumps({"chat_id": chat, "text": text[:4000], "disable_web_page_preview": True,
                         "disable_notification": bool(c["tg_silent"] and not urgent)}).encode(),
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r: r.read()
    except urllib.error.HTTPError as e:
        try: msg = json.loads(e.read()).get("description", str(e))
        except Exception: msg = str(e)
        raise RuntimeError(msg.replace(token, "***")) from None
    except Exception as e:
        raise RuntimeError(str(e).replace(token, "***")) from None

def notify(text, kind, wait=False, urgent=False):
    c = cfg()
    if not (c["tg_enabled"] and c.get(kind)): return
    def send():
        try: tg_send(text, urgent=urgent)
        except Exception as e: print("Telegram-Fehler:", e, file=sys.stderr)
    if wait or CLI_MODE: send()
    else: threading.Thread(target=send, daemon=True).start()

def unclear_notice(unclear):
    """Meldet Klärungsbedarf nur, wenn sich die Liste geändert hat (kein Spam bei jedem Cron-Lauf)."""
    c = cfg()
    sig = hashlib.md5("|".join(unclear).encode()).hexdigest() if unclear else ""
    if sig == c["tg_unclear_sig"]: return
    if not unclear:
        save_cfg(tg_unclear_sig=""); return
    if c["tg_enabled"] and c["tg_unclear"]:
        lines = "\n".join(f"• {u}" for u in unclear[:8]) + (f"\n… und {len(unclear) - 8} weitere" if len(unclear) > 8 else "")
        notify(f"🚨 Handlungsbedarf – Media Sorter\n❓ {len(unclear)} Datei(en) brauchen eine Zuordnung, der Zeitplan überspringt sie.\n{lines}\n"
               "Bitte im Reiter „Sortieren“ zuordnen." + (f"\n👉 {ui_link('clarify')}" if ui_link("clarify") else ""), "tg_unclear", urgent=True)
        save_cfg(tg_unclear_sig=sig)

def run_job(items, source="manuell"):
    """Prozessübergreifende Sperre: UI, Zeitplan und System-Cron laufen nie parallel."""
    fd = _acquire()
    if fd is None:
        with lock:
            job["errors"].append("Es läuft bereits ein anderer Kopiervorgang.")
            job.update(running=False, finished=True)
        return
    removed, ok, t0 = 0, [], time.time()
    try:
        c = cfg()
        if items:
            cats = Counter(i["dst"].parts[0] for i in items)
            size = sum(i["src"].stat().st_size for i in items if i["src"].exists())
            notify(f"▶️ Media Sorter: Kopieren gestartet ({source})\n{len(items)} Datei(en), {hsize(size)}\n"
                   + " · ".join(f"{k} {v}" for k, v in cats.items()), "tg_start")
        ok = _run_job(items, source, c["move_after"])
        moved = {it["src"].relative_to(SOURCE).as_posix() for it, _ in ok if not it["src"].exists()}
        if moved & get_paths().keys():
            set_paths({k: v for k, v in get_paths().items() if k not in moved})
        if moved & get_mg().keys():
            set_mg({k: v for k, v in get_mg().items() if k not in moved})
        if c["cleanup_after"]:
            removed = cleanup_leftovers()
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN); fd.close()
    unclear = scan()[1]
    res = f"{source}: {job['files_done']} von {job['files_total']} Datei(en)"
    if job["errors"]: res += f", {len(job['errors'])} Fehler"
    if removed: res += f", {removed} Ordner entfernt"
    if unclear: res += f", {len(unclear)} zur Klärung offen"
    save_cfg(last_run=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), last_result=res)
    if items:
        errs = job["errors"]
        head = "⛔ Abgebrochen" if job["cancel"] else ("⚠️ Fertig mit Fehlern" if errs else "✅ Fertig")
        lines = [f"• {it['dst'].as_posix()}" for it, _ in ok[:10]]
        if len(ok) > 10: lines.append(f"… und {len(ok) - 10} weitere")
        txt = (f"{head} – Media Sorter ({source})\n{len(ok)} von {len(items)} Datei(en) übertragen, "
               f"{hsize(sum(sz for _, sz in ok))} in {hdur(time.time() - t0)}\nZiel: {TARGET}\n" + "\n".join(lines))
        if errs: txt += "\nFehler:\n" + "\n".join(f"– {e}" for e in errs[:5])
        if unclear:
            txt += f"\n❓ {len(unclear)} Datei(en) brauchen Klärung"
            if ui_link("clarify"): txt += f"\n👉 {ui_link('clarify')}"
        if ui_link("hist"): txt += f"\n🗂 Verlauf: {ui_link('hist')}"
        if errs and c["tg_errors"]:
            notify("🚨 Handlungsbedarf – " + txt, "tg_errors", wait=True, urgent=True)
        else:
            notify(txt, "tg_done", wait=True)

# Warteschlange: Aufträge (UI, Zeitplan) laufen nacheinander; neue werden hinten angestellt, statt abgelehnt zu werden.
qcv = threading.Condition()
pending, qstate, _qid, _qworker = [], {"active": None}, [0], []

def enqueue(items, source):
    """-> (neue Dateien, musste warten). Dateien, die schon laufen oder warten, werden nicht doppelt aufgenommen."""
    with qcv:
        have = {it["src"] for b in pending for it in b["items"]}
        with lock:
            if job["running"]: have |= {SOURCE / q["src"] for q in job["queue"] if q["state"] in ("wartet", "kopiert") and "src" in q}
        new = [it for it in items if it["src"] not in have]
        if not new: return 0, False
        waits = job["running"] or bool(pending)
        _qid[0] += 1
        pending.append({"id": _qid[0], "source": source, "items": new, "t": time.time(),
                        "size": sum(i["src"].stat().st_size for i in new if i["src"].exists())})
        if not _qworker:
            _qworker.append(threading.Thread(target=queue_worker, daemon=True)); _qworker[0].start()
        qcv.notify_all()
        return len(new), waits

def queue_worker():
    while True:
        with qcv:
            while not pending: qcv.wait()
            b = pending[0]; qstate["active"] = b
        try:
            while busy(): time.sleep(2)             # ein anderer Prozess (z. B. CLI) hält die Sperre
            run_job(b["items"], b["source"])
        except Exception as e:
            print("Warteschlange:", e, file=sys.stderr)
        finally:
            with qcv:
                if b in pending: pending.remove(b)
                qstate["active"] = None

def waiting_batches():
    with qcv: return [b for b in pending if b is not qstate["active"]]

def trigger(source):
    if not TARGET.is_dir():
        alert_once("target", True, f"🚨 Handlungsbedarf – Media Sorter\nDas Ziel {TARGET} ist nicht erreichbar. Ist das NFS-Share eingebunden? Bis dahin wird nichts kopiert.",
                   "✅ Media Sorter: Das Ziel ist wieder erreichbar.")
        return False, f"Ziel {TARGET} nicht erreichbar (NFS gemountet?)"
    items, unclear = scan()
    unclear_notice(unclear)
    n, waits = enqueue(items, source)
    if not n and items: return False, "Alle fertigen Dateien laufen schon oder warten bereits."
    msg = f"{n} Datei(en) in der Warteschlange" + (" (nach dem laufenden Vorgang)" if waits else "")
    if unclear: msg += f", {len(unclear)} zur Klärung übersprungen"
    return True, msg

def scheduler():
    last = None
    while True:
        time.sleep(15)
        try:
            c = cfg()
            now = datetime.now().replace(second=0, microsecond=0)
            if c["enabled"] and now != last and Cron(c["cron"]).match(now):
                last = now
                ok, msg = trigger("Zeitplan")
                if not ok:
                    save_cfg(last_run=now.strftime("%Y-%m-%d %H:%M:%S"), last_result=f"Zeitplan: übersprungen – {msg}")
        except Exception as e:
            print("Scheduler-Fehler:", e, file=sys.stderr)

# ---------- Wächter: neue Dateien, Ausfälle ----------
WATCH_INTERVAL = int(os.environ.get("WATCH_INTERVAL", "60"))
PROGRESS_MIN = int(os.environ.get("PROGRESS_MIN_SECONDS", "20"))   # kürzere Läufe senden keine 25/50/75-%-Meldungen
_alerts = {}

def alert_once(key, down, text_down, text_up):
    """Meldet einen Ausfall einmal beim Eintreten (dringend) und eine Entwarnung beim Ende."""
    prev = _alerts.get(key, False)
    if down and not prev: notify(text_down, "tg_errors", urgent=True)
    elif prev and not down: notify(text_up, "tg_errors")
    _alerts[key] = down

def _key(it):
    try: return f"{it['src'].relative_to(SOURCE).as_posix()}|{it['src'].stat().st_size}"
    except OSError: return None

def announce_new(items):
    cats = Counter(i["dst"].parts[0] for i in items)
    size = 0
    for i in items:
        try: size += i["src"].stat().st_size
        except OSError: pass
    names = [f"• {i['src'].name}" for i in items[:6]] + ([f"… und {len(items) - 6} weitere"] if len(items) > 6 else [])
    sch = schedule_info()
    when = f"⏰ Nächster Lauf: {sch['next']}" if sch["enabled"] and sch["next"] else "⏰ Zeitplan ist aus, bitte manuell starten"
    txt = (f"📥 Media Sorter: {len(items)} neue Datei(en) bereit ({hsize(size)})\n"
           + ", ".join(f"{k} {v}" for k, v in cats.items()) + "\n" + "\n".join(names) + f"\n{when}")
    if ui_link("sort"): txt += f"\n👉 {ui_link('sort')}"
    notify(txt, "tg_new", wait=True)

def watch_once(state):
    """Ein Prüfdurchlauf: Ausfälle, Klärungsbedarf und neue Dateien melden."""
    c = cfg()
    if not c["tg_enabled"]:
        state.update(last_new=set(), stable=0); return
    link = f"\n👉 {ui_link('sched')}" if ui_link("sched") else ""
    alert_once("target", not TARGET.is_dir(),
               f"🚨 Handlungsbedarf – Media Sorter\nDas Ziel {TARGET} ist nicht erreichbar. Ist das NFS-Share eingebunden? Bis dahin wird nichts kopiert.{link}",
               "✅ Media Sorter: Das Ziel ist wieder erreichbar.")
    src_ok = SOURCE.is_dir()
    alert_once("source", not src_ok, f"🚨 Handlungsbedarf – Media Sorter\nDer Quellordner {SOURCE} ist nicht erreichbar.{link}",
               "✅ Media Sorter: Der Quellordner ist wieder erreichbar.")
    if not src_ok: return
    items, unclear = scan()
    unclear_notice(unclear)
    keys = {k: i for i in items if (k := _key(i))}
    seen = set((load("seen.json", {}) or {}).get("keys", []))
    new = {k for k in keys if k not in seen}
    if new and c["tg_new"]:
        if new == state["last_new"]: state["stable"] += 1          # zwei gleiche Prüfungen in Folge: nichts kommt mehr nach
        else: state.update(last_new=new, stable=0)
        if state["stable"] >= 1:
            announce_new([keys[k] for k in sorted(new)])
            save("seen.json", {"keys": sorted(keys)}); state.update(last_new=set(), stable=0)
    else:
        state.update(last_new=set(), stable=0)
        if set(keys) != seen: save("seen.json", {"keys": sorted(keys)})

def watcher():
    state = {"last_new": set(), "stable": 0}
    while True:
        time.sleep(WATCH_INTERVAL)
        try: purge_samples()
        except Exception as e: print("Sample-Fehler:", e, file=sys.stderr)
        try: watch_once(state)
        except Exception as e: print("Watcher-Fehler:", e, file=sys.stderr)

def _progress_note(mark, source, t0):
    with lock:
        done, total, fd, ft, speed = job["done"], job["total"], job["files_done"], job["files_total"], job["speed"]
    eta = (time.time() - t0) * (total - done) / done if done else 0
    txt = f"⏳ Media Sorter: {mark} % geschafft ({source})\n{fd} von {ft} Datei(en), {hsize(done)} von {hsize(total)}"
    if speed and eta: txt += f"\n{hsize(speed)}/s, noch ca. {hdur(eta)}"
    notify(txt, "tg_progress")

# ---------- Codec-Analyse ----------
ANALYSIS_EXT = {".mkv", ".mp4", ".avi", ".m4v", ".mov", ".wmv", ".ts", ".mpg", ".mpeg"}
CODEC_NAMES = {"h264": "H.264", "hevc": "H.265", "av1": "AV1", "vp9": "VP9", "vp8": "VP8", "mpeg4": "MPEG-4",
               "msmpeg4v3": "MPEG-4", "msmpeg4v2": "MPEG-4", "mpeg2video": "MPEG-2", "mpeg1video": "MPEG-1",
               "vc1": "VC-1", "wmv1": "WMV", "wmv2": "WMV", "wmv3": "WMV", "theora": "Theora", "prores": "ProRes"}
an = {"running": False, "cancel": False, "scope": "target", "total": 0, "done": 0, "current": "", "error": ""}
an_lock = threading.Lock()

def analysis_root(scope):
    return TARGET if scope == "target" else SOURCE

def probe(path):
    r = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                        "stream=codec_name,profile,width,height,pix_fmt:format=duration,bit_rate",
                        "-of", "json", str(path)], capture_output=True, text=True, timeout=60)
    j = json.loads(r.stdout or "{}")
    return (j.get("streams") or [{}])[0], j.get("format") or {}

def _entry(size, mtime, s, f):
    raw = s.get("codec_name", "")
    dur = float(f.get("duration") or 0)
    br = int(f.get("bit_rate") or 0) or (int(size * 8 / dur) if dur else 0)
    return {"s": size, "m": mtime, "codec": CODEC_NAMES.get(raw, raw.upper() if raw else "Unbekannt"),
            "profile": s.get("profile", ""), "w": s.get("width") or 0, "h": s.get("height") or 0,
            "br": br, "dur": round(dur), "pix": s.get("pix_fmt", ""), "bad": not raw}

def _same(a, b):
    try: return os.path.samefile(a, b)
    except OSError: return False

def _walk_files(root, skip=None):
    """Alle Dateien unter root. Ordner, die mit `skip` identisch sind (z.B. downloads im Ziel), werden übersprungen."""
    for dp, dns, fns in os.walk(root):
        dns[:] = sorted(d for d in dns if not d.startswith(".") and not (skip and _same(os.path.join(dp, d), skip)))
        for fn in sorted(fns):
            yield Path(dp) / fn

def run_analysis(scope):
    root = analysis_root(scope)
    with an_lock:
        an.update(running=True, cancel=False, scope=scope, total=0, done=0, current="", error="")
    try:
        if not root.is_dir():
            raise RuntimeError(f"{root} nicht erreichbar")
        cache = (load(f"analysis_{scope}.json", {}) or {}).get("files", {})
        new, todo, found = {}, [], 0
        for p in _walk_files(root, SOURCE if scope == "target" else None):
            if not (p.is_file() and p.suffix.lower() in ANALYSIS_EXT) or p.name.startswith("."): continue
            try: st = p.stat()
            except OSError: continue
            found += 1
            rel, c = p.relative_to(root).as_posix(), cache.get(p.relative_to(root).as_posix())
            if c and c.get("s") == st.st_size and c.get("m") == int(st.st_mtime) and not c.get("bad"):
                new[rel] = c
            else:
                todo.append((p, rel, st))
        with an_lock:
            an.update(total=found, done=len(new))
        def work(item):
            p, rel, st = item
            if an["cancel"]: return None
            with an_lock: an["current"] = rel
            try: s, f = probe(p)
            except Exception: s, f = {}, {}
            return rel, _entry(st.st_size, int(st.st_mtime), s, f)
        with ThreadPoolExecutor(max_workers=4) as ex:
            for fut in as_completed([ex.submit(work, i) for i in todo]):
                r = fut.result()
                if r: new[r[0]] = r[1]
                with an_lock: an["done"] += 1
        save(f"analysis_{scope}.json", {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "files": new})
    except Exception as e:
        with an_lock: an["error"] = str(e)
    finally:
        with an_lock: an.update(running=False, current="")

# ---------- API ----------
_sm = {}
def schedule_info():
    c, nxt = cfg(), ""
    if c["enabled"]:
        key = (c["cron"], datetime.now().strftime("%Y%m%d%H%M"))
        if _sm.get("k") != key:
            try:
                t = Cron(c["cron"]).next_after(datetime.now()); _sm.update(k=key, v=fmt_dt(t) if t else "")
            except ValueError:
                _sm.update(k=key, v="ungültiger Ausdruck")
        nxt = _sm["v"]
    return {"enabled": c["enabled"], "cron": c["cron"], "next": nxt, "last": c["last_result"]}

def settings_payload():
    c = cfg()
    try: nxt, err = [fmt_dt(t) for t in next_runs(c["cron"])], ""
    except ValueError as e: nxt, err = [], str(e)
    tok = c.pop("tg_token"); c.pop("tg_unclear_sig")
    return {**c, "tg_token_set": bool(tok), "next": nxt, "error": err,
            "tz": datetime.now().astimezone().tzname(), "persist": PERSIST,
            "info": {"source": str(SOURCE), "target": str(TARGET), "data": str(DATA), "min_age": MIN_AGE,
                     "auth": bool(AUTH_PASS), "ffprobe": bool(shutil.which("ffprobe")), "watch": WATCH_INTERVAL}}

@app.get("/api/files")
def files():
    out, cm = [], load("copied.json", {})
    with lock: busy_src = {q["src"] for q in job["queue"] if q["state"] in ("wartet", "kopiert") and "src" in q} if job["running"] else set()
    with qcv: busy_src |= {it["src"].relative_to(SOURCE).as_posix() for b in pending for it in b["items"]}
    for p in sorted(SOURCE.rglob("*")):
        if p.is_file() and p.suffix.lower() in VIDEO_EXT and not SAMPLE.search(p.name):
            rel = p.relative_to(SOURCE).as_posix(); cat = guess_category(rel)
            out.append({"path": rel, "size": p.stat().st_size, "category": cat, "target": target_for(rel, cat).as_posix() if cat else "",
                        "series": series_info(rel), "copied": is_copied(cm, p, rel), "busy": rel in busy_src})
    shows = {show_dir(f["target"]) for f in out if f["series"] and f["target"]}
    return jsonify(library=library_eps(shows), files=out, categories=CATEGORIES, source=str(SOURCE), target=str(TARGET), target_mounted=TARGET.is_dir())

@app.get("/api/rules")
def rules_get():
    return jsonify(rules=get_rules())

@app.post("/api/rules")
def rules_set():
    d = request.get_json() or {}
    detected = (d.get("detected") or "").strip()
    name = _safe(_pretty((d.get("name") or "").strip()))
    year = re.sub(r"\D", "", str(d.get("year") or ""))[:4]
    tmdb = re.sub(r"\D", "", str(d.get("tmdb") or ""))[:10]
    cat = d.get("category") if d.get("category") in CATEGORIES else ""
    if not detected or not skey(detected): return jsonify(error="Erkannter Name fehlt."), 400
    if not name: return jsonify(error="Bitte einen Namen eingeben."), 400
    if year and not YEAR.fullmatch(year): return jsonify(error="Jahr ungültig."), 400
    rules = [r for r in get_rules() if r["key"] != skey(detected)]
    rules.append({"key": skey(detected), "detected": detected, "name": name, "year": year, "tmdb": tmdb, "category": cat})
    set_rules(rules)
    return jsonify(ok=True, rules=rules)

@app.post("/api/rules/delete")
def rules_delete():
    k = (request.get_json() or {}).get("key")
    set_rules([r for r in get_rules() if r["key"] != k])
    return jsonify(ok=True, rules=get_rules())

@app.post("/api/series/preview")
def series_preview():
    """Vorschau für den Umbenennen-Dialog, ohne zu speichern."""
    d = request.get_json() or {}
    name = _safe(_pretty((d.get("name") or "").strip()))
    year = re.sub(r"\D", "", str(d.get("year") or ""))[:4]
    tmdb = re.sub(r"\D", "", str(d.get("tmdb") or ""))[:10]
    cat = d.get("category") if d.get("category") in CATEGORIES else "Serien"
    out = []
    for e in (d.get("samples") or [])[:500]:
        ep = parse_ep(e["path"], cat == "Anime", True)
        if not ep or not name: out.append(""); continue
        _, _, sn, eps = ep
        show = f"{name} ({year})" if year else name
        es = f"S{sn:02d}E{eps[0]:02d}" + "".join(f"-E{n:02d}" for n in eps[1:])
        out.append((Path(cat) / (show + (f" [tmdbid-{tmdb}]" if tmdb else "")) / f"Season {sn:02d}" / f"{show} {es}{Path(e['path']).suffix.lower()}").as_posix())
    return jsonify(targets=out)

@app.post("/api/preview")
def preview():
    d = request.get_json()
    return jsonify(target=target_for(d["path"], d["category"]).as_posix() if d["category"] else "")

@app.post("/api/preview/batch")
def preview_batch():
    items = (request.get_json() or {}).get("items", [])[:5000]
    return jsonify(targets=[target_for(e["path"], e["category"]).as_posix() if e.get("category") in CATEGORIES else "" for e in items])

@app.post("/api/groups/preview")
def groups_preview():
    d = request.get_json() or {}
    try:
        mg = mg_build(d)
        order = list(dict.fromkeys((d.get("items") or [])[:5000]))
        return jsonify(targets=[mg_path(r, d["category"], mg[r]).as_posix() for r in order], eps=[mg[r]["ep"] for r in order])
    except ValueError as e:
        return jsonify(error=str(e)), 400

@app.post("/api/groups")
def groups_set():
    d = request.get_json() or {}
    try: mg = mg_build(d)
    except ValueError as e: return jsonify(error=str(e)), 400
    new = dict(get_mg()); new.update(mg); set_mg(new)
    return jsonify(ok=True, n=len(mg))

@app.post("/api/groups/delete")
def groups_delete():
    rels = set(((request.get_json() or {}).get("items") or [])[:5000])
    set_mg({k: v for k, v in get_mg().items() if k not in rels})
    return jsonify(ok=True)

@app.post("/api/paths/preview")
def paths_preview():
    d = request.get_json() or {}
    rels = (d.get("items") or [])[:5000]
    try:
        return jsonify(targets=[t.as_posix() for t in folder_targets(rels, d.get("folder"))[2]] if rels else [])
    except ValueError as e:
        return jsonify(error=str(e)), 400

@app.post("/api/paths")
def paths_set():
    """Zielordner für Dateien merken; leerer Ordner setzt sie auf den Standardpfad zurück."""
    d = request.get_json() or {}
    rels = (d.get("items") or [])[:5000]
    paths = dict(get_paths())
    try:
        for r in rels: safe_source(r)
        if (d.get("folder") or "").strip():
            if rels:
                parts, n, _ = folder_targets(rels, d["folder"])
                paths.update({r: {"folder": "/".join(parts), "n": n, "cat": parts[0]} for r in rels})
        else:
            for r in rels: paths.pop(r, None)
    except ValueError as e:
        return jsonify(error=str(e)), 400
    set_paths(paths)
    return jsonify(ok=True)

@app.post("/api/start")
def start():
    if not TARGET.is_dir(): return jsonify(error=f"Ziel {TARGET} nicht erreichbar (NFS gemountet?)"), 400
    items = []
    try:
        for e in request.get_json()["items"]:
            if e["category"] not in CATEGORIES: raise ValueError(f"Kategorie fehlt: {e['path']}")
            items.append({"src": safe_source(e["path"]), "dst": target_for(e["path"], e["category"])})
    except Exception as e:
        return jsonify(error=str(e)), 400
    if not items: return jsonify(error="Nichts ausgewählt."), 400
    n, waits = enqueue(items, "manuell")
    if not n: return jsonify(error="Diese Dateien laufen schon oder warten bereits in der Warteschlange."), 409
    return jsonify(ok=True, queued=waits, n=n, position=len(waiting_batches()))

@app.post("/api/queue/remove")
def queue_remove():
    bid = (request.get_json() or {}).get("id")
    with qcv:
        pending[:] = [b for b in pending if b["id"] != bid or b is qstate["active"]]
    return jsonify(ok=True)

@app.post("/api/queue/clear")
def queue_clear():
    with qcv:
        pending[:] = [b for b in pending if b is qstate["active"]]
    return jsonify(ok=True)

@app.post("/api/run")
def run_now():
    ok, msg = trigger("manuell (alle fertigen)")
    return (jsonify(ok=True, info=msg) if ok else (jsonify(error=msg), 409))

@app.get("/api/status")
def status():
    with lock:
        j = {k: v for k, v in job.items() if k != "queue"}
        j["counts"] = {}
        for q in job["queue"]: j["counts"][q["state"]] = j["counts"].get(q["state"], 0) + 1
    j["elapsed"] = (time.time() if j["running"] else (j["ended"] or time.time())) - j["started"] if j["started"] else 0
    j["schedule"] = schedule_info()
    with qcv: j["batches"] = len(pending)
    j["waiting"] = [{"id": b["id"], "source": b["source"], "files": len(b["items"]), "size": b["size"]} for b in waiting_batches()]
    return jsonify(j)

@app.get("/api/job/queue")
def job_queue():
    """Kompakte Liste fürs Laufend-Tab: aktuelle, Fehler, nächste 100 wartende, letzte 100 fertige."""
    with lock: q = [dict(x) for x in job["queue"]]
    cur = [x for x in q if x["state"] == "kopiert"]
    bad = [x for x in q if x["state"] in ("fehler", "abgebrochen")]
    wait = [x for x in q if x["state"] == "wartet"]
    done = [x for x in q if x["state"] in ("fertig", "vorhanden")]
    return jsonify(items=cur + bad + wait[:100] + done[-100:][::-1], hidden=max(0, len(wait) - 100) + max(0, len(done) - 100))

@app.post("/api/cancel")
def cancel():
    job["cancel"] = True; return jsonify(ok=True)

@app.get("/api/analysis/status")
def analysis_status():
    with an_lock: j = dict(an)
    return jsonify(**j, ffprobe=bool(shutil.which("ffprobe")), roots={"target": str(TARGET), "source": str(SOURCE)})

@app.post("/api/analysis/start")
def analysis_start():
    scope = (request.get_json() or {}).get("scope", "target")
    if scope not in ("target", "source"): return jsonify(error="Ungültiger Bereich"), 400
    if not shutil.which("ffprobe"): return jsonify(error="ffprobe fehlt (Docker-Image mit ffmpeg neu bauen)"), 400
    if an["running"]: return jsonify(error="Es läuft bereits eine Analyse."), 409
    threading.Thread(target=run_analysis, args=(scope,), daemon=True).start()
    return jsonify(ok=True)

@app.post("/api/analysis/cancel")
def analysis_cancel():
    an["cancel"] = True; return jsonify(ok=True)

@app.get("/api/analysis/results")
def analysis_results():
    scope = request.args.get("scope", "target")
    if scope not in ("target", "source"): return jsonify(error="Ungültiger Bereich"), 400
    d = load(f"analysis_{scope}.json", {}) or {}
    return jsonify(ts=d.get("ts", ""), scope=scope, root=str(analysis_root(scope)),
                   items=[{"path": k, **v} for k, v in d.get("files", {}).items()])

@app.get("/api/history")
def history():
    return jsonify(items=list(reversed(load("history.json", []))))

@app.post("/api/history/clear")
def history_clear():
    save("history.json", []); return jsonify(ok=True)

@app.get("/api/leftovers")
def leftovers():
    return jsonify(dirs=[entry_info(p) for p in top_entries()], min_age=MIN_AGE)

@app.post("/api/leftovers/delete")
def leftovers_delete():
    if job["running"] or busy(): return jsonify(error="Während des Kopierens nicht möglich."), 409
    d = request.get_json() or {}
    force = bool(d.get("force"))
    names = ([x["path"] for x in map(entry_info, top_entries()) if force or not x["locked"]]
             if d.get("all") else d.get("paths", []))
    deleted, errors = 0, []
    for n in names:
        try: delete_entry(n, force); deleted += 1
        except Exception as e: errors.append(str(e))
    deleted += remove_empty_dirs() if d.get("all") else 0
    return jsonify(deleted=deleted, errors=errors)

@app.post("/api/leftovers/empty")
def leftovers_empty():
    if job["running"] or busy(): return jsonify(error="Während des Kopierens nicht möglich."), 409
    return jsonify(deleted=remove_empty_dirs())

@app.get("/api/settings")
def settings_get():
    return jsonify(settings_payload())

@app.post("/api/settings")
def settings_set():
    d = request.get_json() or {}
    try: Cron(str(d.get("cron", "")))
    except ValueError as e: return jsonify(error=str(e)), 400
    old = cfg()
    tok = str(d.get("tg_token", "")).strip() or old["tg_token"]
    chat = str(d.get("tg_chat", "")).strip()
    if d.get("tg_enabled") and not (tok and chat):
        return jsonify(error="Telegram: Bot-Token und Chat-ID eintragen."), 400
    url = str(d.get("public_url", "")).strip().rstrip("/")
    if url and not re.match(r"^https?://\S+$", url):
        return jsonify(error="Web-UI-Adresse muss mit http:// oder https:// beginnen."), 400
    save_cfg(enabled=bool(d.get("enabled")), cron=" ".join(d["cron"].split()),
             move_after=bool(d.get("move_after")), cleanup_after=bool(d.get("cleanup_after")),
             tg_enabled=bool(d.get("tg_enabled")), tg_token=tok, tg_chat=chat,
             tg_start=bool(d.get("tg_start")), tg_done=bool(d.get("tg_done")), tg_unclear=bool(d.get("tg_unclear")), public_url=url,
             tg_new=bool(d.get("tg_new", old["tg_new"])), tg_progress=bool(d.get("tg_progress", old["tg_progress"])),
             tg_errors=bool(d.get("tg_errors", old["tg_errors"])), tg_silent=bool(d.get("tg_silent", old["tg_silent"])),
             jellyfin=bool(d.get("jellyfin", old["jellyfin"])))
    return jsonify(settings_payload())

@app.post("/api/telegram/test")
def telegram_test():
    d = request.get_json() or {}
    try:
        urgent = bool(d.get("urgent"))
        head = "🚨 Test: dringende Meldung, kommt mit Ton." if urgent else ("✅ Test: normale Meldung" + (", stumm gesendet." if cfg()["tg_silent"] else "."))
        tg_send(head + (f"\n🌐 {ui_link()}" if ui_link() else ""),
                str(d.get("token", "")).strip() or None, str(d.get("chat", "")).strip() or None, urgent=urgent)
    except Exception as e:
        return jsonify(error=str(e)), 400
    return jsonify(ok=True)

@app.get("/api/cron")
def cron_preview():
    try: return jsonify(ok=True, next=[fmt_dt(t) for t in next_runs(request.args.get("expr", ""))])
    except ValueError as e: return jsonify(ok=False, error=str(e))

@app.get("/")
def index():
    return Response(PAGE.replace("{{VERSION}}", f"v{VERSION} · {BUILD}"), mimetype="text/html", headers={"Cache-Control": "no-store"})

# ---------- Frontend ----------
PAGE = r"""<!doctype html>
<html lang="de"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#4a56c8">
<title>Media Sorter</title>
<style>
:root{
 --bg:#f5f6f8;--surface:#fff;--ink:#1f2430;--muted:#6b7280;--line:#e5e7eb;--soft:#f0f2f5;
 --brand:#4a56c8;--brand2:#4a56c8;--brand-ink:#fff;
 --ok:#2f8a66;--okbg:#e4f2ec;--warn:#9a6700;--warnbg:#faf0d9;--bad:#b83a52;--badbg:#f9e6ea;
 --c-Filme:#b97a45;--c-Serien:#5a7cc0;--c-Anime:#a8648f;--c-Dokus:#4b957a;--c-:#9a6700;
 --shadow:0 1px 2px rgba(20,24,40,.05),0 4px 14px rgba(20,24,40,.05);color-scheme:light}
@media (prefers-color-scheme:dark){:root{
 --bg:#14161b;--surface:#1c1f26;--ink:#e8eaee;--muted:#9aa1ad;--line:#2b2f38;--soft:#242831;
 --brand:#8e99f0;--brand2:#8e99f0;--brand-ink:#14161b;
 --ok:#6cc5a0;--okbg:#17302a;--warn:#e0b45c;--warnbg:#332a14;--bad:#f08a9f;--badbg:#3a1b24;
 --c-Filme:#d49a68;--c-Serien:#85a2dc;--c-Anime:#cc89b3;--c-Dokus:#6cc5a0;--c-:#e0b45c;
 --shadow:0 1px 2px rgba(0,0,0,.3),0 4px 14px rgba(0,0,0,.2);color-scheme:dark}}
*{box-sizing:border-box}
body{margin:0;padding-top:env(safe-area-inset-top,0px);background:var(--bg);color:var(--ink);
 font:15px/1.5 ui-rounded,"SF Pro Rounded","Segoe UI Variable","Segoe UI",system-ui,-apple-system,sans-serif}
main{max-width:1100px;margin:0 auto;padding:16px 16px 24px}
h1{font-size:24px;line-height:1.15;margin:0}h2{font-size:17px;margin:0 0 4px}
p{margin:0}.sub{color:var(--muted)}
code,.path,input.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px}
[hidden]{display:none!important}

/* Kopfbereich */
.hero{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:16px 18px;box-shadow:var(--shadow);margin-bottom:14px}
.brand{display:flex;gap:12px;align-items:center}
.logo{font-size:28px;line-height:1;background:var(--soft);border-radius:12px;padding:8px 10px}
.hero{position:relative;padding-top:28px}.ver{position:absolute;top:8px;right:18px;font-size:12px;color:var(--muted);font-variant-numeric:tabular-nums}
.hero p{color:var(--muted);font-size:14px}
.flow{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:14px}
.pathchip{background:var(--soft);border-radius:10px;padding:5px 10px;min-width:0;max-width:100%}
.pathchip small{display:block;font-size:11px;color:var(--muted);line-height:1.2}
.pathchip code{word-break:break-all}
.arrow{color:var(--muted)}
.hero .brand{justify-content:space-between;flex-wrap:wrap;align-items:flex-start;gap:12px 20px}.hero .brand>div:first-of-type{flex:1 1 320px}
.hero .logo{display:none}.hero h1:before{content:"🎞️ "}.hero .flow{margin-top:8px}
.stats{display:flex;gap:8px;margin-top:0}
.stat{background:var(--soft);border-radius:12px;padding:6px 12px;min-width:92px;display:flex;flex-direction:column-reverse}
.stat b{display:block;font-size:20px;line-height:1.1}.stat span{font-size:12px;color:var(--muted);white-space:nowrap}
.stat.alert{background:var(--warnbg)}.stat.alert b,.stat.alert span{color:var(--warn)}

/* Hinweise */
.banner{border-radius:12px;padding:10px 14px;margin-bottom:12px;display:flex;gap:10px;align-items:flex-start;font-size:14px}
.banner.bad{background:var(--badbg);color:var(--bad)}.banner.warn{background:var(--warnbg);color:var(--warn)}.banner.ok{background:var(--okbg);color:var(--ok)}

/* Bedienelemente */
.bar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:12px}
button,select,input[type=text],input[type=search],input[type=password],input[type=url]{
 font:inherit;border:1.5px solid var(--line);background:var(--surface);color:var(--ink);padding:8px 14px;border-radius:12px;min-height:40px}
button{cursor:pointer;font-weight:600;transition:transform .08s,background .15s,border-color .15s}
button:hover:not(:disabled){border-color:var(--brand);background:var(--soft)}
button:active:not(:disabled){transform:scale(.97)}
button.primary{background:var(--brand);color:var(--brand-ink);border-color:transparent}
button.primary:hover:not(:disabled){background:color-mix(in srgb,var(--brand) 86%,#000);border-color:transparent}
button.danger{color:var(--bad);border-color:color-mix(in srgb,var(--bad) 45%,var(--line))}
button.danger:hover:not(:disabled){background:var(--badbg);border-color:var(--bad)}
button.ghost{border-color:transparent;background:none}
button:disabled{opacity:.45;cursor:not-allowed;box-shadow:none}
button.link{border:0;background:none;padding:0;min-height:0;color:var(--brand);text-decoration:underline;font-weight:600}
:focus-visible{outline:3px solid color-mix(in srgb,var(--brand) 55%,transparent);outline-offset:2px}
input[type=checkbox]{width:18px;height:18px;accent-color:var(--brand)}

/* Reiter */
.nav{display:flex;gap:6px;margin-bottom:14px;overflow-x:auto;position:sticky;top:env(safe-area-inset-top,0px);z-index:5;
 background:var(--bg);padding:6px 0;scrollbar-width:none}
.nav::-webkit-scrollbar{display:none}
.nav button{border-radius:999px;background:var(--surface);white-space:nowrap;display:flex;gap:6px;align-items:center;padding:8px 16px}
.nav button.on{background:var(--ink);color:var(--bg);border-color:var(--ink)}
.badge{min-width:20px;height:20px;border-radius:10px;padding:0 6px;font-size:12px;line-height:20px;text-align:center;background:var(--soft);color:var(--muted)}
.nav .on .badge{background:rgba(255,255,255,.22);color:inherit}
.badge.hot{background:var(--warn);color:#fff}

/* Filter-Chips */
.chips{display:flex;gap:6px;flex-wrap:wrap}
.chip{border-radius:999px;padding:5px 12px;min-height:34px;font-size:14px;display:inline-flex;gap:6px;align-items:center}
.chip[data-c]{--cc:var(--c-)}
.chip[data-c=Filme]{--cc:var(--c-Filme)}.chip[data-c=Serien]{--cc:var(--c-Serien)}.chip[data-c=Anime]{--cc:var(--c-Anime)}.chip[data-c=Dokus]{--cc:var(--c-Dokus)}
.chip[data-c=Alle]{--cc:var(--brand)}
.chip.on{background:var(--cc);border-color:var(--cc);color:#fff}
.chip.on:hover:not(:disabled){background:var(--cc);border-color:var(--cc)}
.chip i{font-style:normal;font-size:12px;opacity:.85}

/* Tabellen */
.table{background:var(--surface);border:1px solid var(--line);border-radius:16px;overflow:hidden;box-shadow:var(--shadow)}
table{width:100%;border-collapse:collapse}
th,td{padding:10px 12px;text-align:left;border-bottom:1px solid var(--line);vertical-align:middle}
th{font-weight:600;color:var(--muted);font-size:13px;background:var(--soft)}tr:last-child td{border-bottom:0}
tbody tr{--cc:var(--c-)}
tbody tr[data-cat=Filme]{--cc:var(--c-Filme)}tbody tr[data-cat=Serien]{--cc:var(--c-Serien)}
tbody tr[data-cat=Anime]{--cc:var(--c-Anime)}tbody tr[data-cat=Dokus]{--cc:var(--c-Dokus)}
#rows td:first-child{box-shadow:inset 4px 0 0 var(--cc)}
td.f,.path{word-break:break-all}.t{color:var(--muted);font-size:13px;word-break:break-all}
td.s{white-space:nowrap;color:var(--muted)}
.path{user-select:all}
select.cat{border-color:var(--cc);color:var(--cc);font-weight:700;background:color-mix(in srgb,var(--cc) 9%,var(--surface))}
.tag{display:inline-flex;gap:4px;align-items:center;padding:2px 10px;border-radius:999px;font-size:12.5px;font-weight:600;background:var(--soft);color:var(--muted)}
.tag.cat{color:var(--cc);background:color-mix(in srgb,var(--cc) 14%,var(--surface))}
.tag.ok{background:var(--okbg);color:var(--ok)}.tag.warn{background:var(--warnbg);color:var(--warn)}.tag.bad{background:var(--badbg);color:var(--bad)}
.empty{padding:40px 20px;text-align:center;color:var(--muted)}
.empty .big{font-size:42px;line-height:1.2}.empty b{display:block;color:var(--ink);font-size:17px;margin:4px 0}

/* Karten / Einstellungen */
.card{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:18px;margin-bottom:14px;max-width:760px;box-shadow:var(--shadow)}
.card header{display:flex;gap:10px;align-items:center;margin-bottom:10px}
.card header .ico{font-size:24px;background:var(--soft);border-radius:12px;padding:6px 9px}
.opt{display:flex;gap:12px;align-items:flex-start;padding:10px 0;border-top:1px solid var(--line);cursor:pointer}
.opt:first-of-type{border-top:0}
.opt b{display:block;font-weight:600}.opt small{color:var(--muted);font-size:13px}
.opt input[type=checkbox]{appearance:none;flex:none;width:44px;height:26px;border-radius:13px;background:var(--line);position:relative;cursor:pointer;transition:background .2s;margin-top:1px}
.opt input[type=checkbox]::after{content:"";position:absolute;top:3px;left:3px;width:20px;height:20px;border-radius:50%;background:#fff;box-shadow:0 1px 3px rgba(0,0,0,.3);transition:left .2s}
.opt input[type=checkbox]:checked{background:var(--ok)}.opt input[type=checkbox]:checked::after{left:21px}
.field{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0}
.field input{flex:1 1 200px;min-width:0}.field input.tok{flex-basis:320px}
label.l{display:block;font-weight:600;font-size:14px;margin-top:10px}
.next{margin:6px 0 0;padding-left:18px;color:var(--muted)}
details{margin-top:10px;color:var(--muted);font-size:14px}summary{cursor:pointer;color:var(--brand);font-weight:600}
details ol{padding-left:20px;margin:6px 0}
.err{color:var(--bad);font-size:13.5px;margin-top:6px;white-space:pre-line}

/* Fortschritts-Leiste unten */
.dock{position:fixed;left:0;right:0;bottom:0;background:var(--surface);border-top:1px solid var(--line);box-shadow:0 -4px 16px rgba(20,24,40,.06);
 padding:12px 16px calc(12px + env(safe-area-inset-bottom,0px));z-index:10}
.dock>div{max-width:1100px;margin:0 auto}
.sched{display:flex;justify-content:space-between;gap:12px;align-items:center;font-size:13.5px;margin-bottom:8px}
.sched .pill{display:inline-flex;gap:6px;align-items:center;background:var(--soft);border-radius:999px;padding:3px 12px}
.sched .pill.on{background:var(--okbg);color:var(--ok)}
.row{display:flex;justify-content:space-between;gap:12px;font-size:13px;color:var(--muted);margin-bottom:4px}
.row span{min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.row b{color:var(--ink)}
.prog{height:14px;background:var(--soft);border-radius:7px;overflow:hidden;margin-bottom:8px}
.prog i{display:block;height:100%;width:0;border-radius:7px;background:var(--brand);transition:width .3s}
.prog.thin{height:7px}
.prog.run i{background-image:linear-gradient(45deg,rgba(255,255,255,.22) 25%,transparent 25%,transparent 50%,rgba(255,255,255,.22) 50%,rgba(255,255,255,.22) 75%,transparent 75%);background-size:28px 28px;animation:flow 1s linear infinite}
@keyframes flow{to{background-position:28px 0}}
.dock .bar{margin:0}

/* Meldungen */
#toasts{position:fixed;left:0;right:0;top:calc(12px + env(safe-area-inset-top,0px));display:flex;flex-direction:column;align-items:center;gap:8px;z-index:50;pointer-events:none;padding:0 12px}
.toast{background:var(--ink);color:var(--bg);padding:10px 16px;border-radius:12px;box-shadow:0 8px 24px rgba(0,0,0,.25);max-width:520px;font-weight:600;animation:pop .2s}
.toast.ok{background:var(--ok);color:#fff}.toast.bad{background:var(--bad);color:#fff}
@keyframes pop{from{transform:translateY(-8px);opacity:0}}

@media (pointer:coarse){
 button,select,input[type=text],input[type=search],input[type=password],input[type=url]{min-height:46px;font-size:16px}
 input[type=checkbox]:not(.sw){width:22px;height:22px}}
@media (pointer:coarse){button.chip{min-height:38px;font-size:14px;padding:4px 12px}}
@media (max-width:720px){
 .brand{gap:10px}.flow{margin-top:8px}.stats{width:100%;margin-top:2px}.stat{flex:1;min-width:0;padding:6px 8px}.stat span{white-space:normal}
 .stat span{font-size:12px;line-height:1.25}
 .bar.tools{display:grid;grid-template-columns:1fr 1fr;gap:8px}.bar.tools button{padding:6px 8px;white-space:nowrap}
 #presets{flex-wrap:wrap!important;overflow:visible!important}
 .bar.tools #sel{grid-column:1/-1}
 .sched{white-space:nowrap;margin-bottom:6px}.sched>span{min-width:0;overflow:hidden;text-overflow:ellipsis}
 main{padding:10px 12px 24px}
 .hero{border-radius:16px;padding:14px}h1{font-size:21px}.logo{font-size:26px;padding:6px 8px}
 .stat b{font-size:19px}.stat{padding:7px 10px}
 .chips{flex-wrap:nowrap;overflow-x:auto;width:100%;padding-bottom:4px}.chip{white-space:nowrap}
 .table{background:none;border:0;box-shadow:none;overflow:visible}
 table{display:block}tbody{display:block}thead{display:none}
 tr{display:grid;grid-template-columns:1fr auto;gap:6px 12px;position:relative;background:var(--surface);border:1px solid var(--line);
  border-left:5px solid var(--cc,var(--line));border-radius:14px;padding:12px;margin-bottom:10px;box-shadow:var(--shadow)}
 td{display:block;border:0;padding:0;min-width:0}
 td.span{grid-column:1/-1}
 #rows td:first-child{box-shadow:none}
 #rows td.chk{position:absolute;top:12px;right:12px}
 #rows td.f{order:1;grid-column:1/-1;padding-right:34px;font-weight:600}
 #rows td.c{order:2}#rows td.s{order:3;text-align:right;align-self:center}
 #rows td.t{order:4;grid-column:1/-1}#rows select{width:100%}
 #rows.grouped tr:not(.grow) td.f{padding-left:0}tr.grow td.s{display:none}tr.grow{background:var(--soft)}
 .empty{background:var(--surface);border-radius:16px}
 .card{max-width:none;padding:14px}
 .dock{padding:8px 12px calc(8px + env(safe-area-inset-bottom,0px))}
 .hide-m{display:none}.prog{height:11px;margin-bottom:6px}.prog.thin{height:6px}
 .dock .bar button.primary{flex:1}
 .nav button{padding:8px 14px;min-height:40px}}
.wide{max-width:none}
button.toggle.on{background:var(--ink);color:var(--bg);border-color:var(--ink)}
dialog{border:1px solid var(--line);border-radius:16px;padding:20px;background:var(--surface);color:var(--text,inherit);width:min(460px,calc(100vw - 32px));box-shadow:0 20px 60px rgba(0,0,0,.3)}
dialog::backdrop{background:rgba(0,0,0,.45)}
dialog input,dialog select{width:100%;box-sizing:border-box}
dialog .field{display:grid;grid-template-columns:1fr 1fr;gap:10px}
#sprev{font-size:12px;line-height:1.45;word-break:break-all;max-height:200px;overflow:auto;border:1px solid var(--line);border-radius:10px;padding:8px}
#sprev div{padding:3px 0;border-top:1px solid var(--line)}#sprev div:first-child{border-top:0}#sprev small{display:block;opacity:.65}
tr.grow.sg,tr.grow.fg{cursor:pointer}tr.grow .cv{min-height:30px;min-width:30px;padding:0;margin-right:6px}
tr.grow.hasgap{--cc:var(--warn)}tr.solo.hasgap{--cc:var(--warn)}tr.solo .rn{margin-left:8px;min-height:30px;padding:2px 10px;font-size:13px;font-weight:400}
tr.grow.sg.hasgap td,tr.grow.fg.hasgap td,tr.solo.hasgap td{background:color-mix(in srgb,var(--warn) 14%,var(--surface))}tr.hasgap .gap{font-weight:700;font-size:15px}
#mrows{max-height:50vh;overflow:auto}.mrow{display:grid;grid-template-columns:1fr 90px 120px;gap:8px;align-items:center;padding:6px 0;border-top:1px solid var(--line)}
.mrow:first-child{border-top:0}.mrow.head{font-size:13px;color:var(--muted);border-top:0;padding-top:0}.mrow .bad{outline:2px solid var(--warn)}
@media (max-width:520px){.mrow{grid-template-columns:1fr 1fr}.mrow>:first-child{grid-column:1/-1}}
#fprev{font-size:12px;line-height:1.45;word-break:break-all;max-height:200px;overflow:auto;border:1px solid var(--line);border-radius:10px;padding:8px}
#fprev div{padding:3px 0;border-top:1px solid var(--line)}#fprev div:first-child{border-top:0}#fprev small{display:block;opacity:.65}
.gap{font-size:13px;margin-top:4px;color:var(--warn)}.gap.ok{color:var(--ok)}.gap.ghost{color:inherit;opacity:.7}
.fq input{flex:1 1 260px;min-width:0}
#gprev{font-size:12px;line-height:1.45;word-break:break-all;max-height:200px;overflow:auto;border:1px solid var(--line);border-radius:10px;padding:8px}
#gprev div{padding:3px 0;border-top:1px solid var(--line)}#gprev div:first-child{border-top:0}#gprev small{display:block;opacity:.65}
#gdlg select,#gdlg input{width:100%}#gdlg .field{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.ctx{position:fixed;z-index:50;background:var(--surface);border:1px solid var(--line);border-radius:12px;box-shadow:0 10px 30px rgba(0,0,0,.25);padding:4px}
.ctx button{display:block;width:100%;border:0;background:none;text-align:left;padding:8px 14px}
td.f::before{content:"🎞️ ";opacity:.75}tr.grow td.f::before{content:""}
.badge.live{background:var(--ok);color:#fff;animation:pulse 1.4s ease-in-out infinite}.nav .on .badge.live{background:var(--ok);color:#fff}
@keyframes pulse{50%{opacity:.55}}
.rtiles{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin:12px 0}
.rtile{background:var(--soft);border-radius:12px;padding:8px 12px}.rtile small{display:block;font-size:12px;color:var(--muted)}.rtile b{font-size:17px}
.bigpct{font-size:40px;font-weight:800;line-height:1}
.runhead{display:flex;justify-content:space-between;align-items:flex-end;gap:12px;margin-bottom:10px}
.curfile{background:var(--soft);border-radius:12px;padding:10px 12px;margin-bottom:12px}.curfile code{word-break:break-all}
.qrow td{padding:7px 10px;border-top:1px solid var(--line)}.qs{white-space:nowrap;width:130px}
.qrow.kopiert td{background:color-mix(in srgb,var(--brand) 8%,var(--surface))}.qrow.fehler td,.qrow.abgebrochen td{background:var(--badbg)}
.rcard{max-width:none}
@media (max-width:720px){.rtiles{grid-template-columns:1fr 1fr}.bigpct{font-size:32px}.qs{width:auto}}
tr.grow.sg td{background:var(--soft)}tr.grow .rn{margin-left:8px;min-height:30px;padding:2px 10px;font-size:13px}
.series .chips{width:100%}.series .chip b{font-weight:700}.series .chip small{opacity:.7}
.rule{display:flex;gap:10px;align-items:center;justify-content:space-between;padding:8px 0;border-top:1px solid var(--line)}
.rule:first-child{border-top:0}.rule .sub{display:block}
.bulk{display:flex;gap:8px 12px;flex-wrap:wrap;align-items:center;background:var(--soft);border-radius:12px;padding:8px 12px;margin-bottom:12px}
tr.grow{--cc:var(--brand)}tr.grow td{background:var(--soft)}tr.grow td.f{font-weight:400}tr.grow select{width:100%}
#rows.grouped tr:not(.grow) td.f{padding-left:12px}
#rows tr.grow td{border-top:12px solid var(--surface)}#rows tr.grow:first-child td{border-top:0}
@media (min-width:721px){#rows.grouped tr:not(.grow):not(.solo) td.chk{padding-left:36px}}
@media (max-width:720px){#rows.grouped tr:not(.grow) td.f{padding-left:0}#rows tr.grow td{border-top:0}#rows.grouped tr:not(.grow):not(.solo){margin-left:18px}}
.kv{display:grid;grid-template-columns:150px 1fr;gap:4px 12px;padding:9px 0;border-top:1px solid var(--line)}.kv:first-child{border-top:0}
@media (max-width:720px){.kv{grid-template-columns:1fr}}
.grp{font-weight:700;margin:16px 0 2px}.grp small{font-weight:400;color:var(--muted);margin-left:6px}
.astats{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:14px}
.tile{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:14px 16px;box-shadow:var(--shadow)}
.tile b{display:block;font-size:26px;line-height:1.2}
.tile.save{background:var(--okbg);color:var(--ok);border-color:color-mix(in srgb,var(--ok) 30%,var(--line))}
.sbar{display:flex;height:24px;border-radius:12px;overflow:hidden;background:var(--soft)}.sbar i{display:block;height:100%}
.crow{display:grid;grid-template-columns:1fr 1fr 1.3fr 1fr;gap:8px;align-items:center;padding:9px 0;border-top:1px solid var(--line)}
.crow .tag{justify-self:start}.crow:first-child{border-top:0}.crow b{text-align:right}.okc{color:var(--ok)}
input[type=range]{accent-color:var(--brand);min-height:0;padding:0;border:0;background:none}
@media (max-width:720px){
 .astats{grid-template-columns:1fr 1fr}.tile.save{grid-column:1/-1}
 .crow{grid-template-columns:auto 1fr auto;gap:6px 10px}.crow span:nth-child(2){display:none}.crow small{display:none}}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style></head><body><main>

<header class="hero">
  <span class="ver" title="Version und Prüfsumme der laufenden app.py">{{VERSION}}</span>
  <div class="brand"><span class="logo" aria-hidden="true">🎞️</span>
    <div><h1>Media Sorter</h1>
      <div class="flow"><span class="pathchip" title="Quellordner">📥 <code id="src">…</code></span><span class="arrow" aria-hidden="true">➜</span><span class="pathchip" title="Ziel auf dem NFS-Share">🗄️ <code id="tgt">…</code></span></div></div>
    <div class="stats">
      <div class="stat"><span>📦 bereit</span><b id="st-ready">0</b></div>
      <div class="stat" id="st-clar-box"><span>❓ brauchen dich</span><b id="st-clar">0</b></div>
      <div class="stat"><span>✅ verschoben</span><b id="st-hist">0</b></div>
    </div></div>
</header>
<div id="warn"></div>

<nav class="nav" id="nav" aria-label="Bereiche">
  <button data-v="sort">📥 Sortieren <span class="badge hot" id="ns" hidden></span></button>
  <button data-v="run">⚡ Läuft <span class="badge live" id="nr" hidden></span></button>
  <button data-v="hist">✅ Verschoben <span class="badge" id="nh" hidden></span></button>
  <button data-v="clean">🧹 Aufräumen <span class="badge" id="nc" hidden></span></button>
  <button data-v="ana">📊 Analyse</button>
  <button data-v="sched">⏰ Zeitplan</button>
  <button data-v="set">⚙️ Einstellungen</button>
</nav>

<section id="v-sort">
  <div class="bar">
    <div class="chips" id="tabs"></div>
  </div>
  <div class="bar fq"><input type="search" id="fq" placeholder="🔍 Suchen: Dateiname, Serie oder Pfad" autocomplete="off" aria-label="Dateien suchen"><button id="fmark" hidden>☑️ Treffer markieren</button><span id="fqn" class="sub"></span></div>
  <div class="bar tools">
    <button id="reload">↻ Neu laden</button><button id="all">☑️ Alle auswählen</button><button id="none">⬜ Keine</button><button id="inv" title="Auswahl der sichtbaren Dateien umdrehen">🔁 Umkehren</button>
    <button id="grp" class="toggle">📁 Nach Ordner</button><button id="sgrp" class="toggle">📺 Nach Serie</button><button id="merge" hidden>🔗 Serien vereinen</button><button id="shc" class="toggle" hidden>✅ Schon kopierte zeigen (<span id="shcn">0</span>)</button>
    <span id="sel" class="sub"></span><span class="sub hide-m" style="margin-left:auto">💡 Shift+Klick markiert einen Bereich, Strg+A alles</span>
  </div>
  <div id="series" class="bulk series" hidden><b>📺 Serien erkannt</b><span class="sub">Tippen zum Umbenennen oder Zuordnen</span><div class="chips" id="schips"></div></div>
  <div id="bulk" class="bulk" hidden><b id="bn"></b><div class="chips" id="bchips"></div><button id="gbtn">👥 Neue Gruppe</button><button id="fbtn">📂 Zielordner ändern</button><button id="mbtn" hidden>🗓️ Jahr &amp; TMDB</button></div>
  <div class="table"><table>
    <thead><tr><th style="width:40px"><input type="checkbox" id="master" aria-label="Alle"></th><th>🎞️ Datei</th><th style="width:170px">🏷️ Kategorie</th><th>📍 Wird abgelegt als</th><th>💾 Größe</th></tr></thead>
    <tbody id="rows"></tbody></table>
    <div id="empty" class="empty" hidden><div class="big">🎉</div><b>Alles sortiert</b>Im Quellordner liegt nichts mehr, was sortiert werden muss.</div></div>
</section>

<div id="ctx" class="ctx" hidden><button id="ctxgrp">👥 Neue Gruppe</button><button id="ctxdis">↩️ Gruppe auflösen</button><button id="ctxedit">✏️ Ziel bearbeiten</button></div>
<dialog id="gdlg"><form method="dialog" id="gform">
  <h2 style="margin:0 0 4px">👥 Neue Gruppe</h2>
  <p class="sub" id="gdet" style="margin:0 0 12px"></p>
  <label class="l" for="gname">Name der Serie</label>
  <input type="text" id="gname" autocomplete="off" required>
  <div class="field"><div><label class="l" for="gyear">Jahr (optional)</label><input type="text" id="gyear" inputmode="numeric" maxlength="4" placeholder="1996"></div>
  <div><label class="l" for="gtmdb">TMDB-ID (optional)</label><input type="text" id="gtmdb" inputmode="numeric" placeholder="12609"></div></div>
  <div class="field"><div><label class="l" for="gcat">Kategorie</label><select id="gcat"></select></div>
  <div><label class="l" for="gseason">Staffel</label><input type="number" id="gseason" min="0" max="99" value="1"></div></div>
  <label class="l" for="gmode">Folgennummern</label>
  <div class="field"><select id="gmode"><option value="keep">Erkannte behalten, fehlende ergänzen</option><option value="renumber">Alle der Reihe nach neu zählen</option></select>
  <div><input type="number" id="gstart" min="0" value="1" aria-label="Startnummer" title="Startnummer"></div></div>
  <p class="sub" id="gcount" style="margin:12px 0 4px"></p>
  <div id="gprev" class="mono"></div>
  <p class="sub" style="margin-top:10px">Wird für genau diese Dateien gemerkt und gilt auch für den Zeitplan. Die Reihenfolge ergibt sich aus dem Dateinamen.</p>
  <div class="bar" style="margin-top:14px;justify-content:flex-end"><button type="button" id="gcancel">Abbrechen</button><button class="primary" type="submit">Gruppe anlegen</button></div>
</form></dialog>
<dialog id="sdlg"><form method="dialog" id="sform">
  <h2 style="margin:0 0 4px">Serie umbenennen</h2>
  <p class="sub" id="sdet" style="margin:0 0 12px"></p>
  <label class="l" for="sname">Name der Serie</label>
  <input type="text" id="sname" autocomplete="off" required>
  <div class="field"><div><label class="l" for="syear">Jahr (optional)</label><input type="text" id="syear" inputmode="numeric" maxlength="4" placeholder="1996"></div>
  <div><label class="l" for="stmdb">TMDB-ID (optional)</label><input type="text" id="stmdb" inputmode="numeric" placeholder="12609"></div></div>
  <label class="l" for="scat">Kategorie</label>
  <select id="scat"></select>
  <p class="sub" style="margin:12px 0 4px" id="scount">So landen die Dateien später:</p>
  <div id="sprev" class="mono"></div>
  <p class="sub" id="snote" style="margin-top:10px">Wird gemerkt: Alle Dateien dieser Serie, auch künftige und vom Zeitplan, bekommen diesen Namen.</p>
  <div class="bar" style="margin-top:14px;justify-content:flex-end"><button type="button" id="scancel">Abbrechen</button><button class="primary" id="ssave" type="submit">Speichern</button></div>
</form></dialog>

<dialog id="mdlg" style="width:min(680px,calc(100vw - 32px))"><form method="dialog" id="mform">
  <h2 style="margin:0 0 4px">Jahr &amp; TMDB-ID eintragen</h2>
  <p class="sub" id="mdet" style="margin:0 0 12px"></p>
  <div id="mrows"></div>
  <p class="sub" id="merr" role="alert" style="margin-top:10px"></p>
  <p class="sub" style="margin-top:10px">Wird pro Serie gemerkt und gilt auch für künftige Folgen und den Zeitplan. Nicht geänderte Serien bleiben unberührt.</p>
  <div class="bar" style="margin-top:14px;justify-content:flex-end"><button type="button" id="mcancel">Abbrechen</button><button class="primary" type="submit">Speichern</button></div>
</form></dialog>

<dialog id="fdlg"><form method="dialog" id="fform">
  <h2 style="margin:0 0 4px">Zielordner ändern</h2>
  <p class="sub" id="fdet" style="margin:0 0 12px"></p>
  <label class="l" for="fpath">Zielordner</label>
  <input type="text" id="fpath" class="mono" autocomplete="off" placeholder="Anime/Dragonball" required>
  <p class="sub" id="fcount" style="margin:12px 0 4px"></p>
  <div id="fprev" class="mono"></div>
  <p class="sub" style="margin-top:10px">Wird für genau diese Dateien gemerkt und gilt auch für den Zeitplan. Später neu geladene Folgen bekommen wieder den Standardpfad.</p>
  <div class="bar" style="margin-top:14px;justify-content:flex-end"><button type="button" id="freset">Standardpfad</button><button type="button" id="fcancel">Abbrechen</button><button class="primary" type="submit">Speichern</button></div>
</form></dialog>

<section id="v-run" hidden>
  <div class="card rcard" id="rcard">
    <div class="runhead"><div><h2 id="rtitle">Gerade läuft nichts</h2><p class="sub" id="rsub"></p></div><div class="bigpct" id="rpct" hidden>0 %</div></div>
    <div id="ractive" hidden>
      <div class="prog run" id="rbar"><i id="rbi"></i></div>
      <div class="rtiles">
        <div class="rtile"><small>📄 Dateien</small><b id="rfiles">–</b></div>
        <div class="rtile"><small>💾 Daten</small><b id="rdata">–</b></div>
        <div class="rtile"><small>🚀 Tempo</small><b id="rspd">–</b></div>
        <div class="rtile"><small>⏳ Restzeit</small><b id="reta">–</b></div>
      </div>
      <div class="curfile" id="rcur" hidden><div class="row"><span>▶️ Aktuelle Datei</span><b id="rcp"></b></div><code id="rcn"></code><div class="prog thin run" style="margin:8px 0 0"><i id="rcb"></i></div></div>
      <button class="danger" id="rstop">⛔ Abbrechen</button>
    </div>
    <div id="ridle" class="empty" style="padding:18px 0 4px"><div class="big">💤</div><span id="ridle-txt">Wenn kopiert wird, siehst du es hier live.</span>
      <div class="bar" style="justify-content:center;margin:14px 0 0"><button id="rtosort">📥 Zu Sortieren</button></div></div>
  </div>
  <div class="card rcard" id="rwaitcard" hidden>
    <header><span class="ico">⏳</span><div><h2>Warteschlange</h2><p class="sub" id="rwaitsub"></p></div><button class="danger" id="rwaitclear" style="margin-left:auto">🗑️ Leeren</button></header>
    <div id="rwait"></div>
  </div>
  <div class="card rcard" id="ranacard" hidden>
    <header><span class="ico">📊</span><div><h2>Codec-Analyse läuft</h2><p class="sub" id="ranatxt"></p></div></header>
    <div class="prog run"><i id="ranabar"></i></div>
  </div>
  <div id="rqwrap" hidden>
    <div class="bar" id="rchips"></div>
    <div class="table"><table><thead><tr><th>🚦 Status</th><th>📍 Wird abgelegt als</th><th>💾 Größe</th></tr></thead><tbody id="rrows"></tbody></table></div>
    <p class="sub" id="rhid" style="margin-top:8px"></p>
  </div>
</section>

<section id="v-hist" hidden>
  <div class="bar"><input type="search" id="hq" placeholder="🔍 Titel oder Pfad suchen" style="flex:1 1 240px">
    <button id="hclear" class="danger">🗑️ Verlauf leeren</button></div>
  <div class="table"><table>
    <thead><tr><th>🕒 Wann</th><th>🏷️ Kategorie</th><th>📍 Liegt jetzt hier</th><th>💾 Größe</th><th></th></tr></thead>
    <tbody id="hrows"></tbody></table>
    <div id="hempty" class="empty" hidden><div class="big">📭</div><b>Noch nichts verschoben</b>Hier siehst du später, wohin jede Datei gewandert ist.</div></div>
</section>

<section id="v-clean" hidden>
  <p class="sub" style="margin-bottom:12px">Das liegt noch im Quellordner. <span class="tag ok">löschbar</span> sind Reste ohne Medien. Alles andere ist geschützt und lässt sich nur mit Extra-Bestätigung löschen.</p>
  <div class="bar"><button id="cdelall" class="danger">🧹 Alle Reste löschen</button>
    <button id="cempty">Leere Ordner entfernen</button><button id="cwipe" class="danger">Alles löschen…</button><button id="creload">↻ Neu einlesen</button></div>
  <div class="table"><table>
    <thead><tr><th>📁 Name</th><th>📦 Inhalt</th><th>🚦 Status</th><th></th></tr></thead>
    <tbody id="crows"></tbody></table>
    <div id="cempty2" class="empty" hidden><div class="big">✨</div><b>Blitzblank</b>Der Quellordner ist leer.</div></div>
</section>

<section id="v-ana" hidden>
  <p class="sub" style="margin-bottom:12px">Welche Video-Codecs liegen bei dir, und wie viel Platz würde H.265 theoretisch sparen? Es werden nur die Kopfdaten der Dateien gelesen, nichts wird verändert.</p>
  <div class="bar"><div class="chips" id="ascope"></div></div>
  <div class="bar"><button class="primary" id="astart">🔍 Analyse starten</button><button id="astop" class="danger" hidden>Abbrechen</button><span id="ameta" class="sub"></span></div>
  <div id="anoff" class="banner warn" hidden>⚠️ <span>ffprobe ist nicht installiert. Im Docker-Image ist es enthalten, bitte neu bauen: <code>docker compose up -d --build</code></span></div>
  <div id="aerr" class="banner bad" hidden></div>
  <div id="aprog" class="card wide" hidden>
    <div class="row"><span id="atxt"></span><b id="apct">0 %</b></div>
    <div class="prog run"><i id="apb"></i></div>
  </div>
  <div id="aempty" class="empty card wide" hidden><div class="big">📊</div><b id="aet">Noch keine Analyse</b><span id="aes">Starte die Analyse, um die Codecs zu sehen.</span></div>
  <div id="abody" hidden>
    <div class="astats">
      <div class="tile"><span class="sub">Videodateien</span><b id="a-n">0</b></div>
      <div class="tile"><span class="sub">Gesamtgröße</span><b id="a-size">0</b></div>
      <div class="tile save"><span>Mögliche Ersparnis mit H.265</span><b id="a-sav">0</b><span id="a-pct"></span></div>
    </div>
    <div class="card wide">
      <h2>Codecs nach Größe</h2>
      <div class="sbar" id="sbar" style="margin:10px 0 6px"></div>
      <div id="clist"></div>
    </div>
    <div class="card wide">
      <h2>Annahme für die Ersparnis</h2>
      <p class="sub" style="margin-bottom:8px">Echte Werte hängen von Quelle, Qualität und Einstellungen ab. Typisch sind 30 bis 50 % bei H.264. Das ist eine Schätzung, kein Versprechen.</p>
      <label class="l" for="p264">H.264 zu H.265: <b id="v264"></b></label>
      <input type="range" id="p264" min="10" max="70" step="5" style="width:100%">
      <label class="l" for="pold">Ältere Codecs (MPEG-2/4, VC-1, WMV …): <b id="vold"></b></label>
      <input type="range" id="pold" min="30" max="85" step="5" style="width:100%">
      <p class="sub" style="margin-top:8px;font-size:13px">H.265 und AV1 zählen mit 0 %, VP9 mit 15 %. Umwandeln kostet Rechenzeit und etwas Qualität, und nicht jedes Abspielgerät kann H.265 direkt.</p>
    </div>
    <div class="bar"><input type="search" id="aq" placeholder="🔍 Datei suchen" style="flex:1 1 220px"><button id="acsv">⬇ CSV speichern</button></div>
    <div class="bar"><div class="chips" id="afilter"></div></div>
    <div class="table"><table>
      <thead><tr><th>Datei</th><th>Codec</th><th>Auflösung und Bitrate</th><th>Größe</th><th>Ersparnis</th></tr></thead>
      <tbody id="arows"></tbody></table>
      <div id="anone" class="empty" hidden><b>Keine Treffer</b></div></div>
    <div class="bar" style="margin-top:12px;justify-content:center"><button id="amore" hidden>Mehr anzeigen</button></div>
  </div>
</section>

<section id="v-sched" hidden>
  <div class="card">
    <header><span class="ico">⏰</span><div><h2>Automatischer Zeitplan</h2><p class="sub">Sortiert und kopiert zu festen Zeiten von allein.</p></div></header>
    <label class="opt"><input type="checkbox" id="en"><span><b>Zeitplan einschalten</b><small>Dateien ohne klare Zuordnung bleiben liegen, bis du sie zuordnest.</small></span></label>
    <label class="l" for="cron">Wann soll es laufen?</label>
    <div class="chips" id="presets" style="margin:6px 0 8px"></div>
    <div class="field"><input type="text" class="mono" id="cron" aria-label="Cron-Ausdruck" spellcheck="false" autocapitalize="off"></div>
    <div class="sub" style="font-size:13px">Eigener Ausdruck: Minute, Stunde, Tag, Monat, Wochentag (0 = Sonntag). Zeitzone: <b id="tz"></b></div>
    <div id="cerr" class="err"></div>
    <div class="sub" style="margin-top:8px">Nächste Läufe:</div><ul class="next" id="next"></ul>
    <div class="bar" style="margin-top:12px"><button class="primary" id="save">Speichern</button><span id="saved" class="sub"></span></div>
    <div id="nop" class="banner warn" hidden>⚠️ Einstellungen gehen beim Neustart verloren. Binde ein Volume auf /data ein.</div>
  </div>
  <div class="card">
    <header><span class="ico">▶️</span><div><h2>Letzter Lauf</h2></div></header>
    <p id="last" class="sub" style="margin-bottom:10px"></p>
    <button id="runnow">Jetzt alle fertigen Dateien verarbeiten</button><span id="runmsg" class="sub" style="margin-left:8px"></span>
  </div>
</section>

<section id="v-set" hidden>
  <div id="nop2" class="banner warn" hidden>⚠️ <span>Einstellungen gehen beim Neustart verloren. Binde ein Volume auf /data ein.</span></div>
  <div class="card">
    <header><span class="ico">🗂️</span><div><h2>Verarbeitung</h2><p class="sub">Gilt fürs manuelle Kopieren und für den Zeitplan.</p></div></header>
    <label class="opt"><input type="checkbox" id="mv"><span><b>Quelle nach dem Kopieren löschen</b><small>Dateien werden verschoben statt kopiert.</small></span></label>
    <label class="opt"><input type="checkbox" id="cu"><span><b>Reste danach aufräumen</b><small>Ordner ohne Medien werden automatisch gelöscht.</small></span></label>
    <label class="opt"><input type="checkbox" id="jf"><span><b>Jellyfin-Benennung</b><small>Ordner und Dateien werden umbenannt: „Titel (Jahr)“ bzw. „Serie/Season 01/Serie S01E02“. Aus: Originalnamen bleiben.</small></span></label>
    <div class="bar" style="margin-top:12px"><button class="primary" id="save2">Speichern</button><span id="saved2" class="sub"></span></div>
  </div>
  <div class="card">
    <header><span class="ico">✈️</span><div><h2>Telegram</h2><p class="sub">Bekomme eine Nachricht, wenn etwas passiert.</p></div></header>
    <label class="opt"><input type="checkbox" id="tge"><span><b>Benachrichtigungen einschalten</b></span></label>
    <div class="field">
      <input type="password" id="tgtok" class="mono tok" autocomplete="off" aria-label="Bot-Token">
      <input type="text" id="tgchat" class="mono" placeholder="Chat-ID" aria-label="Chat-ID" autocapitalize="off">
    </div>
    <label class="l" for="puburl">Adresse dieser Seite (für Links in den Nachrichten)</label>
    <div class="field"><input type="url" id="puburl" class="mono" placeholder="https://media.p.pixelpotato.de" autocapitalize="off" spellcheck="false"></div>
    <div class="grp">Infos <small>kommen leise, wenn „stumm“ an ist</small></div>
    <label class="opt"><input type="checkbox" id="tgn"><span><b>Neue Dateien sind da</b><small>Nachdem sich in der Quelle zwei Minuten nichts mehr tut</small></span></label>
    <label class="opt"><input type="checkbox" id="tgs"><span><b>Kopieren beginnt</b></span></label>
    <label class="opt"><input type="checkbox" id="tgp"><span><b>Fortschritt bei 25, 50 und 75 %</b><small>Nur bei Läufen, die länger als etwa 20 Sekunden dauern</small></span></label>
    <label class="opt"><input type="checkbox" id="tgd"><span><b>Alles fertig</b><small>Mit Zielpfaden</small></span></label>
    <div class="grp">Dringend <small>immer mit Ton und 🚨</small></div>
    <label class="opt"><input type="checkbox" id="tgu"><span><b>Datei braucht Klärung</b><small>Mit Link direkt zur Auswahl</small></span></label>
    <label class="opt"><input type="checkbox" id="tgr"><span><b>Fehler, Abbruch oder Ziel nicht erreichbar</b><small>Zum Beispiel wenn das NFS-Share fehlt</small></span></label>
    <div class="grp">Ton</div>
    <label class="opt"><input type="checkbox" id="tgq"><span><b>Infos stumm senden</b><small>Nur dringende Meldungen machen dann ein Geräusch</small></span></label>
    <details><summary>So richtest du den Bot ein</summary><ol>
      <li>In Telegram <b>@BotFather</b> öffnen, <code>/newbot</code> senden und den Token kopieren.</li>
      <li>Deinem neuen Bot eine beliebige Nachricht schreiben.</li>
      <li>Mit <b>@userinfobot</b> deine Chat-ID herausfinden und oben eintragen.</li>
      <li>Auf „Testnachricht senden“ tippen. Der Token wird unverschlüsselt in /data/config.json gespeichert.</li></ol></details>
    <div class="bar" style="margin-top:12px"><button class="primary" id="tgsave">Speichern</button><button id="tgtest">Test: normal</button><button id="tgtest2">Test: dringend</button><span id="tgmsg" class="sub"></span></div>
  </div>
  <div class="card">
    <header><span class="ico">📺</span><div><h2>Gemerkte Serien</h2><p class="sub">Eigene Namen, Jahr und TMDB-ID. Im Tab Sortieren auf eine erkannte Serie tippen, um eine anzulegen.</p></div></header>
    <div id="rules"></div>
  </div>
  <div class="card">
    <header><span class="ico">🖥️</span><div><h2>System</h2><p class="sub">Wird in der docker-compose.yml festgelegt.</p></div></header>
    <div id="sysinfo"></div>
  </div>
</section>
</main>

<div class="dock"><div>
  <div class="sched"><span id="sch"></span><button class="link" id="schgo">Ändern</button></div>
  <div id="dk-run" hidden><div class="row"><span id="cur">Bereit</span><b id="spd"></b></div>
  <div class="prog thin run"><i id="pf"></i></div></div>
  <div id="dk-tot" hidden><div class="row"><span id="txt"></span><b id="pct">0 %</b></div>
  <div class="prog" id="ptw"><i id="pt"></i></div></div>
  <div class="bar"><button class="primary" id="go">Auswahl kopieren</button><button id="stop" class="danger" hidden>Abbrechen</button></div>
  <div class="err" id="err"></div>
</div></div>
<div id="toasts" aria-live="polite"></div>

<script>
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const fmt=b=>{const u=["B","KB","MB","GB","TB"];let i=0;while(b>=1024&&i<4){b/=1024;i++}return b.toFixed(i?1:0)+" "+u[i]};
const fdate=t=>{const[a,b]=t.split(" ");const d=a.split("-");return `${d[2]}.${d[1]}.${d[0]} ${b.slice(0,5)}`};
const ICON={Filme:"🎬",Serien:"📺",Anime:"🌸",Dokus:"🌍"};
function toast(msg,kind=""){const t=document.createElement("div");t.className="toast "+kind;t.textContent=msg;$("toasts").appendChild(t);setTimeout(()=>t.remove(),kind==="bad"?6000:3200)}
function setBadge(id,n){const b=$(id);b.hidden=!n;b.textContent=n||""}
async function api(u,body){
  const r=await fetch(u,body===undefined?{}:{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
  let j={};try{j=await r.json()}catch{}
  if(!r.ok)throw new Error(j.error||r.statusText);return j}
async function copyText(t,btn){
  try{if(navigator.clipboard&&window.isSecureContext)await navigator.clipboard.writeText(t);else throw 0}
  catch{const a=document.createElement("textarea");a.value=t;a.style.cssText="position:fixed;opacity:0";document.body.appendChild(a);a.select();try{document.execCommand("copy")}catch{}a.remove()}
  toast("Pfad kopiert","ok")}

let view="sort",files=[],cats=[],filter="Alle",hist=[],running=false;
function show(v){view=v;try{history.replaceState(null,"",v==="sort"?location.pathname+location.search:"#"+v)}catch{}
  for(const s of["sort","run","hist","clean","ana","sched","set"])$("v-"+s).hidden=s!==v;
  $("nav").querySelectorAll("button").forEach(b=>b.classList.toggle("on",b.dataset.v===v));
  $("go").hidden=v!=="sort";
  ({sort:loadFiles,run:()=>api("/api/status").then(renderRun),hist:loadHist,clean:loadClean,ana:loadAna,sched:loadSched,set:loadSched})[v]()}
$("nav").querySelectorAll("button").forEach(b=>b.onclick=()=>show(b.dataset.v));

/* Sortieren */
const unclearN=()=>files.filter(f=>!f.category).length;
let showCopied=false,emptyHtml="";const lastRow={v:null},lastGrp={v:null};
const checked=f=>f.sel===true;
let grouped=false,sgrouped=true,groups=[],sgroups=[],sopen=new Set(["","d:"]);try{grouped=localStorage.getItem("sort_grp")==="1";const sg=localStorage.getItem("sort_sgrp");sgrouped=sg===null?!grouped:sg==="1";if(sgrouped)grouped=false}catch{}
function stats(){const u=unclearN();
  $("st-ready").textContent=files.length-u;$("st-clar").textContent=u;
  $("st-clar-box").classList.toggle("alert",u>0);setBadge("ns",u)}
async function loadFiles(){
  await loadRules();
  const r=await api("/api/files"),marked=new Set(files.filter(checked).map(f=>f.path));library=r.library||{};cats=r.categories;
  const nc=r.files.filter(f=>f.copied).length;files=showCopied?r.files:r.files.filter(f=>!f.copied&&!f.busy);
  $("shc").hidden=!nc;$("shcn").textContent=nc;$("shc").classList.toggle("on",showCopied);
  files.forEach(f=>{f.sel=marked.has(f.path)});
  $("src").textContent=r.source;$("tgt").textContent=r.target;
  $("warn").innerHTML=r.target_mounted?"":'<div class="banner bad">⚠️ <span>Das Ziel ist nicht erreichbar. Ist das NFS-Share eingebunden?</span></div>';
  $("bchips").innerHTML=cats.map(c=>`<button class="chip" data-c="${c}">${ICON[c]} ${c}</button>`).join("");
  $("bchips").querySelectorAll("button").forEach(b=>b.onclick=()=>assign(files.filter(checked),b.dataset.c));
  $("grp").classList.toggle("on",grouped);$("sgrp").classList.toggle("on",sgrouped);
  tabs();render()}
function seriesBar(){
  const m=new Map();
  visible().forEach(f=>{const s=f.series;if(!s)return;const e=m.get(s.key)||{key:s.key,names:{},files:[],cats:{}};
    e.names[s.name]=(e.names[s.name]||0)+1;e.files.push(f);e.cats[f.category||"?"]=(e.cats[f.category||"?"]||0)+1;m.set(s.key,e)});
  const list=[...m.values()].filter(e=>e.files.length>=1).sort((a,b)=>b.files.length-a.files.length).slice(0,40);
  seriesList=list;$("series").hidden=sgrouped||!list.length;
  $("schips").innerHTML=list.map((e,i)=>{const n=Object.entries(e.names).sort((a,b)=>b[1]-a[1])[0][0];
    const c=Object.entries(e.cats).sort((a,b)=>b[1]-a[1])[0][0];
    const gp=gaps(e.files).bad.length?" · ⚠ Lücke":"";
    return `<button class="chip" data-s="${i}"><b>${esc(n)}</b> <small>${e.files.length} · ${c==="?"?"❓ unklar":ICON[c]+" "+c}${gp}</small></button>`}).join("");
  $("schips").querySelectorAll("button").forEach(b=>b.onclick=()=>openSeries(seriesList[b.dataset.s]))}
let seriesList=[],curSeries=null,ptm,library={};
const rng=a=>{const o=[];for(let i=0;i<a.length;){let j=i;while(j+1<a.length&&a[j+1]===a[j]+1)j++;o.push(i===j?`E${String(a[i]).padStart(2,"0")}`:`E${String(a[i]).padStart(2,"0")}–E${String(a[j]).padStart(2,"0")}`);i=j+1}return o.join(", ")};
const showDir=t=>{const p=t.split("/"),i=p.findIndex(x=>/^Season \d+$/.test(x));return(i>1?p.slice(0,i):p.slice(0,2)).join("/")};
function gaps(fs){
  const by={};fs.forEach(f=>{const s=f.series;if(!s)return;const sd=f.target?showDir(f.target):"";
    s.eps.forEach(e=>{const b=by[s.season]=by[s.season]||{n:{},sd};b.n[e]=(b.n[e]||0)+1;if(!b.sd)b.sd=sd})});
  const bad=[],good=[];
  Object.keys(by).sort((a,b)=>a-b).forEach(sn=>{const b=by[sn],lib=((library[b.sd]||{})[sn])||[];
    const have=new Set([...Object.keys(b.n).map(Number),...lib]),all=[...have].sort((a,c)=>a-c);
    const lo=all[0],hi=all[all.length-1],miss=[];for(let i=lo;i<=hi;i++)if(!have.has(i))miss.push(i);
    const dup=Object.entries(b.n).filter(([,c])=>c>1).map(([e])=>+e).sort((a,c)=>a-c);
    const p=[];
    if(lo>1)p.push(`beginnt erst bei E${String(lo).padStart(2,"0")}`);
    if(miss.length)p.push(`Lücke ${rng(miss)}`);
    if(dup.length)p.push(`doppelt ${rng(dup)}`);
    const S="S"+String(sn).padStart(2,"0");
    if(p.length)bad.push(`${S}: ${p.join(", ")}`);else good.push(`${S} lückenlos ${rng([...new Set([lo,hi])]).replace(", ","–")}${lib.length?` (${lib.length} schon im Ziel)`:""}`)});
  return{bad,good}}
const gapHtml=fs=>{const g=gaps(fs);return g.bad.length?`<div class="gap">⚠ ${g.bad.map(esc).join(" · ")}</div>`:g.good.length&&fs.length>1?`<div class="gap ok">✓ ${g.good.map(esc).join(" · ")}</div>`:""};
async function sprev(){
  const cat=$("scat").value||"Serien";
  try{const r=await api("/api/series/preview",{name:$("sname").value,year:$("syear").value,tmdb:$("stmdb").value,category:cat,
      samples:curSeries.files.map(f=>({path:f.path}))});
    const n=curSeries.files.length,okn=r.targets.filter(Boolean).length;
    const g=gaps(curSeries.files);
    $("scount").innerHTML=`<b>${okn} von ${n} Dateien</b> werden so abgelegt:`+(g.bad.length?`<div class="gap">⚠ ${g.bad.map(esc).join(" · ")}</div>`:g.good.length&&n>1?`<div class="gap ok">✓ ${g.good.map(esc).join(" · ")}</div>`:"");
    $("sprev").innerHTML=curSeries.files.map((f,i)=>`<div>${esc(f.path)}<small>→ ${esc(r.targets[i]||"nicht erkannt, bleibt unverändert")}</small></div>`).join("")}catch{}}
function openSeries(e){
  curSeries=e;const n=Object.entries(e.names).sort((a,b)=>b[1]-a[1])[0][0];
  const r=rulesCache.find(x=>e.files.some(f=>f.series&&f.series.rkey===x.key));
  $("sname").value=r?r.name:n;$("syear").value=r?r.year:(e.files[0].series.year||"");$("stmdb").value=r?r.tmdb:"";
  const cur=r&&r.category||Object.entries(e.cats).filter(([c])=>c!=="?").sort((a,b)=>b[1]-a[1]).map(x=>x[0])[0]||"";
  $("scat").innerHTML=`<option value="">${cur?"":"Bitte wählen…"}</option>`+cats.map(c=>`<option value="${c}" ${c===cur?"selected":""}>${ICON[c]} ${c}</option>`).join("");
  if(cur)$("scat").firstElementChild.remove();
  const o=new Map();e.files.forEach(f=>{if(f.series)o.set(f.series.rkey,f.series.orig)});
  curSeries.origs=[...o.values()];
  $("sdet").textContent=`${e.files.length} Datei(en)`+(curSeries.origs.length>1?`, vereint aus: ${curSeries.origs.map(x=>"„"+x+"“").join(", ")}`:`, erkannt als „${curSeries.origs[0]||n}“`);
  $("sdlg").querySelector("h2").textContent=curSeries.origs.length>1?"Serien vereinen":"Serie umbenennen";
  $("sdlg").showModal();sprev()}
["sname","syear","stmdb","scat"].forEach(id=>$(id).addEventListener("input",()=>{clearTimeout(ptm);ptm=setTimeout(sprev,250)}));
$("scancel").onclick=()=>$("sdlg").close();
$("sform").onsubmit=async ev=>{ev.preventDefault();
  if(!$("scat").value){toast("Bitte eine Kategorie wählen","bad");return}
  try{
    let r;for(const d of curSeries.origs)r=await api("/api/rules",{detected:d,name:$("sname").value,year:$("syear").value,tmdb:$("stmdb").value,category:$("scat").value});
    await api("/api/paths",{items:curSeries.files.map(f=>f.path),folder:""});
    rulesCache=r.rules;$("sdlg").close();await reloadKeep();toast("Serie gemerkt","ok")
  }catch(e){toast(e.message,"bad")}};
/* Zusammengelegte Serien wieder trennen: löscht die gemerkten Regeln der enthaltenen Namen */
async function splitSeries(fs){
  const names=new Map();fs.forEach(f=>{if(f.series)names.set(f.series.rkey,f.series.orig)});
  const rules=rulesCache.filter(r=>names.has(r.key));
  if(!rules.length){toast("Zu dieser Gruppe gibt es keine gemerkte Regel zum Aufheben.","bad");return}
  const list=[...names.values()].map(x=>"„"+x+"“").join(", ");
  if(!confirm(`Aufteilen in: ${list}\n\nDie gemerkten Namen, Jahre und TMDB-IDs dieser ${rules.length} Regel(n) werden gelöscht. Danach gilt wieder der erkannte Name.`))return;
  try{for(const r of rules)rulesCache=(await api("/api/rules/delete",{key:r.key})).rules;
    await reloadKeep();toast(`Aufgeteilt in ${names.size} Serien`,"ok")}
  catch(e){toast(e.message,"bad")}}
async function reloadKeep(){
  const before=new Map(files.map(f=>[f.path,{sel:f.sel,had:!!f.category}]));
  await loadFiles();
  files.forEach(f=>{const b=before.get(f.path);if(!b)return;f.sel=!b.had&&f.category?true:b.sel});
  tabs();render()}
/* Zielordner: ersetzt den gemeinsamen Anfang der Pfade (höchstens Kategorie/Serie) */
let curFolder=[],ftm;
function commonFolder(fs){
  const ds=fs.filter(f=>f.target).map(f=>f.target.split("/").slice(0,-1));if(!ds.length)return"";
  let n=Math.min(2,...ds.map(d=>d.length));
  while(n>1&&new Set(ds.map(d=>d.slice(0,n).join("/"))).size>1)n--;
  return new Set(ds.map(d=>d[0])).size>1?"":ds[0].slice(0,n).join("/")}
async function fprev(){
  const folder=$("fpath").value,n=curFolder.length;
  if(!folder.trim()){$("fcount").textContent="Bitte einen Ordner eingeben, z. B. Anime/Dragonball";$("fprev").innerHTML="";return}
  try{const r=await api("/api/paths/preview",{items:curFolder.map(f=>f.path),folder});
    if($("fpath").value!==folder)return;
    $("fcount").innerHTML=`<b>${n} ${n===1?"Datei wird":"Dateien werden"}</b> so abgelegt:`;
    $("fprev").innerHTML=curFolder.slice(0,200).map((f,i)=>`<div>${esc(f.path)}<small>→ ${esc(r.targets[i]||"")}</small></div>`).join("")
      +(n>200?`<div>… und ${n-200} weitere</div>`:"")}
  catch(e){$("fcount").textContent=e.message;$("fprev").innerHTML=""}}
function openFolder(list){
  if(!list.length)return;
  curFolder=list;$("fpath").value=commonFolder(list);
  const names=new Set(list.filter(f=>f.series).map(f=>f.series.key));
  $("fdet").textContent=`${list.length} Datei(en)`+(names.size>1?` aus ${names.size} Serien, jede bleibt als eigener Unterordner erhalten`:"");
  $("fdlg").showModal();fprev()}
$("fbtn").onclick=()=>openFolder(files.filter(checked));
$("fpath").addEventListener("input",()=>{clearTimeout(ftm);ftm=setTimeout(fprev,250)});
$("fcancel").onclick=()=>$("fdlg").close();
/* Jahr & TMDB für mehrere Serien */
let metaRows=[];
function openMeta(){
  metaRows=[...mergeCandidates().values()].map(fs=>{
    const rule=rulesCache.find(x=>fs.some(f=>f.series.rkey===x.key)),n={},c={},o=new Map();
    fs.forEach(f=>{n[f.series.name]=(n[f.series.name]||0)+1;if(f.category)c[f.category]=(c[f.category]||0)+1;o.set(f.series.rkey,f.series.orig)});
    const top=m=>Object.entries(m).sort((a,b)=>b[1]-a[1]).map(e=>e[0])[0]||"";
    return{name:rule?rule.name:top(n),count:fs.length,origs:[...o.values()],category:rule&&rule.category||top(c),
      year:rule?rule.year:(fs[0].series.year||""),tmdb:rule?rule.tmdb:""}}).sort((a,b)=>a.name.localeCompare(b.name));
  $("mdet").textContent=`${metaRows.length} markierte ${metaRows.length===1?"Serie":"Serien"}`;$("merr").textContent="";
  $("mrows").innerHTML=`<div class="mrow head"><span>Serie</span><span>Jahr</span><span>TMDB-ID</span></div>`+metaRows.map((r,i)=>`<div class="mrow"><div><b>${esc(r.name)}</b> <span class="sub">${r.count} Datei(en)</span></div>
    <input type="text" data-y="${i}" inputmode="numeric" maxlength="4" placeholder="1996" value="${esc(r.year)}" aria-label="Jahr von ${esc(r.name)}">
    <input type="text" data-t="${i}" inputmode="numeric" placeholder="12609" value="${esc(r.tmdb)}" aria-label="TMDB-ID von ${esc(r.name)}"></div>`).join("");
  $("mdlg").showModal()}
$("mbtn").onclick=openMeta;
$("mcancel").onclick=()=>$("mdlg").close();
$("mform").onsubmit=async ev=>{ev.preventDefault();
  const val=(a,i)=>$("mrows").querySelector(`[data-${a}="${i}"]`).value.trim();
  let bad=false;$("mrows").querySelectorAll("input").forEach(x=>x.classList.remove("bad"));
  metaRows.forEach((r,i)=>{if(val("y",i)&&!/^(19[2-9]\d|20[0-3]\d)$/.test(val("y",i))){$("mrows").querySelector(`[data-y="${i}"]`).classList.add("bad");bad=true}
    if(val("t",i)&&!/^\d{1,10}$/.test(val("t",i))){$("mrows").querySelector(`[data-t="${i}"]`).classList.add("bad");bad=true}});
  if(bad){$("merr").textContent="Bitte die markierten Felder prüfen: Jahr als 1920 bis 2039, TMDB-ID nur als Zahl.";return}
  const todo=metaRows.map((r,i)=>({r,year:val("y",i),tmdb:val("t",i)})).filter(x=>x.year!==x.r.year||x.tmdb!==x.r.tmdb);
  if(!todo.length){$("mdlg").close();toast("Nichts geändert");return}
  try{
    for(const{r,year,tmdb}of todo)for(const d of r.origs)rulesCache=(await api("/api/rules",{detected:d,name:r.name,year,tmdb,category:r.category})).rules;
    $("mdlg").close();await reloadKeep();toast(`${todo.length} ${todo.length===1?"Serie":"Serien"} aktualisiert`,"ok")
  }catch(e){$("merr").textContent=e.message;await reloadKeep()}};
async function saveFolder(folder,msg){
  try{await api("/api/paths",{items:curFolder.map(f=>f.path),folder});$("fdlg").close();await reloadKeep();toast(msg,"ok")}
  catch(e){toast(e.message,"bad")}}
$("freset").onclick=()=>saveFolder("","Standardpfad wiederhergestellt");
$("fform").onsubmit=ev=>{ev.preventDefault();saveFolder($("fpath").value,"Zielordner gemerkt")};
let rulesCache=[];
async function loadRules(){try{rulesCache=(await api("/api/rules")).rules}catch{}}
function drawRules(){
  $("rules").innerHTML=rulesCache.length?rulesCache.map((r,i)=>`<div class="rule"><div><b>${esc(r.name)}${r.year?` (${esc(r.year)})`:""}</b>
    <span class="sub">erkannt als „${esc(r.detected)}“ · ${r.category?ICON[r.category]+" "+r.category:"Kategorie automatisch"}${r.tmdb?` · tmdbid-${esc(r.tmdb)}`:""}</span></div>
    <button class="danger" data-r="${i}">Entfernen</button></div>`).join(""):'<p class="sub">Noch keine Serien gemerkt.</p>';
  $("rules").querySelectorAll("button").forEach(b=>b.onclick=async()=>{
    try{rulesCache=(await api("/api/rules/delete",{key:rulesCache[b.dataset.r].key})).rules;drawRules();toast("Entfernt","ok")}catch(e){toast(e.message,"bad")}})}
function tabs(){
  const u=unclearN();
  if(filter==="Klärung"&&!u)filter="Alle";
  $("tabs").innerHTML=["Alle",...cats,...(u?["Klärung"]:[])].map(c=>{
    const n=c==="Alle"?files.length:c==="Klärung"?u:files.filter(f=>f.category===c).length;
    const label=c==="Klärung"?"❓ Braucht dich":(ICON[c]?ICON[c]+" ":"")+c;
    return `<button class="chip ${c===filter?"on":""}" data-c="${c==="Klärung"?"":c}" ${c==="Klärung"?'data-k="1"':""}>${label} <i>${n}</i></button>`}).join("");
  $("tabs").querySelectorAll("button").forEach(b=>b.onclick=()=>{filter=b.dataset.k?"Klärung":b.dataset.c;tabs();render()});
  stats();seriesBar()}
let fqs="";
const hay=f=>(f.path+" "+(f.series?f.series.name+" "+f.series.orig:"")+" "+(f.target||"")).toLowerCase();
const visible=()=>files.filter(f=>(filter==="Alle"||(filter==="Klärung"?!f.category:f.category===filter))&&(!fqs||fqs.every(w=>hay(f).includes(w))));
const catOpts=(first)=>`<option value="">${first}</option>`+cats.map(c=>`<option value="${c}">${ICON[c]} ${c}</option>`).join("");
function row(f,short,extra="",cls=""){const i=files.indexOf(f),name=short&&f.path.includes("/")?f.path.slice(f.path.indexOf("/")+1):f.path;return `<tr class="${cls}" data-cat="${esc(f.category)}">
   <td class="chk"><input type="checkbox" data-i="${i}" ${checked(f)?"checked":""} aria-label="markieren"></td>
   <td class="f" title="${esc(f.path)}">${esc(name)}${extra}</td>
   <td class="c"><select class="cat" data-i="${i}" aria-label="Kategorie">${f.category?"":'<option value="" selected>Kategorie wählen…</option>'}${cats.map(c=>`<option value="${c}" ${c===f.category?"selected":""}>${ICON[c]} ${c}</option>`).join("")}</select></td>
   <td class="t">${f.category?esc(f.target):'<span class="tag warn">❓ Unklar</span> Wohin gehört das?'}</td><td class="s">${fmt(f.size)}</td></tr>`}
function refreshGroups(){
  $("rows").querySelectorAll("input[data-g]").forEach(c=>{const fs=groups[c.dataset.g];
    c.checked=fs.every(checked);c.indeterminate=fs.some(checked)&&!fs.every(checked)})}
function render(){
  const list=visible();
  if(!emptyHtml)emptyHtml=$("empty").innerHTML;
  $("empty").innerHTML=!list.length&&fqs?'<div class="big">🔍</div><b>Keine Treffer</b>Für diese Suche passt keine Datei.':emptyHtml;
  $("fmark").hidden=!fqs||!list.length;$("fqn").textContent=fqs?`${list.length} Treffer`:"";
  $("empty").hidden=list.length>0;$("rows").classList.toggle("grouped",grouped);
  let html="";groups=[];sgroups=[];
  $("rows").classList.toggle("grouped",grouped||sgrouped);
  if(sgrouped){
    const g=new Map();list.forEach(f=>{const k=f.series?f.series.key:"";if(!g.has(k))g.set(k,{key:k,names:{},files:[],cats:{},origs:[]});
      const e=g.get(k);e.files.push(f);if(f.series&&!e.origs.includes(f.series.orig))e.origs.push(f.series.orig);const c=f.category||"?";e.cats[c]=(e.cats[c]||0)+1;if(f.series)e.names[f.series.name]=(e.names[f.series.name]||0)+1});
    g.forEach(e=>e.gap=e.key&&gaps(e.files).bad.length?1:0);
    [...g.values()].sort((a,b)=>(a.key===""?1:0)-(b.key===""?1:0)||b.gap-a.gap||Object.keys(a.names)[0]?.localeCompare(Object.keys(b.names)[0]||"")).forEach(e=>{
      const fs=e.files,gi=groups.push(fs)-1;sgroups[gi]=e;
      if(fs.length===1){html+=row(fs[0],false,e.key?`<button class="rn" data-sg="${gi}">✎ Umbenennen</button><button class="rn" data-fg="${gi}">📂 Zielordner</button>${gapHtml(fs)}`:"",`solo${e.gap?" hasgap":""}`);return}
      const nm=Object.entries(e.names).sort((a,b)=>b[1]-a[1])[0];
      const summ=Object.entries(e.cats).map(([c,n])=>c==="?"?`<span class="tag warn">❓ ${n} unklar</span>`:`<span class="tag cat" style="--cc:var(--c-${c})">${ICON[c]} ${n}</span>`).join(" ");
      const open=sopen.has(e.key)||!!fqs,dir=commonFolder(fs);
      html+=`<tr class="grow sg${e.gap?" hasgap":""}" data-tg="${gi}"><td class="chk"><input type="checkbox" data-g="${gi}" aria-label="Serie markieren"></td>
        <td class="f" colspan="2"><button type="button" class="cv" data-tg="${gi}" aria-expanded="${open}" aria-label="Folgen ${open?"einklappen":"ausklappen"}">${open?"▾":"▸"}</button>📺 <b>${nm?esc(nm[0]):"Ohne erkannte Serie"}</b> <span class="sub">${fs.length} Datei(en), ${fmt(fs.reduce((a,f)=>a+f.size,0))}</span>
        ${e.key?`<button class="rn" data-sg="${gi}">✎ Umbenennen</button><button class="rn" data-fg="${gi}">📂 Zielordner</button>`:""}${new Set(fs.map(f=>f.series&&f.series.rkey).filter(Boolean)).size>1?`<button class="rn" data-split="${gi}" title="Zusammenlegung aufheben">✂️ Aufteilen</button>`:""}${dir?`<div class="sub mono" style="margin-top:4px">📍 ${esc(dir)}</div>`:""}${e.origs&&e.origs.length>1?`<div class="gap ghost">🔗 vereint aus ${e.origs.map(x=>esc(x)).join(", ")}</div>`:""}<div style="margin-top:4px">${summ}</div>${e.key?gapHtml(fs):""}</td>
        <td class="c"><select data-g="${gi}" aria-label="Ganze Serie zuordnen">${catOpts("Serie zuordnen als…")}</select></td><td class="s"></td></tr>`
        +(open?fs.map(f=>row(f,false)).join(""):"")})
  }else if(grouped){
    const g=new Map();list.forEach(f=>{const k=f.path.includes("/")?f.path.split("/")[0]:"";if(!g.has(k))g.set(k,[]);g.get(k).push(f)});
    [...g.keys()].sort((a,b)=>(a===""?1:0)-(b===""?1:0)||a.localeCompare(b)).forEach(k=>{
      const fs=g.get(k),gi=groups.push(fs)-1,cnt={},key="d:"+k,open=sopen.has(key)||!!fqs;sgroups[gi]={key};
      if(fs.length===1){const gp=fs[0].series?gaps(fs).bad.length:0;html+=row(fs[0],false,fs[0].series?gapHtml(fs):"",`solo${gp?" hasgap":""}`);return}
      const sks=new Set(fs.map(f=>f.series?f.series.key:"")),one=sks.size===1&&!sks.has("");
      fs.forEach(f=>{const c=f.category||"?";cnt[c]=(cnt[c]||0)+1});
      const summ=Object.entries(cnt).map(([c,n])=>c==="?"?`<span class="tag warn">❓ ${n} unklar</span>`:`<span class="tag cat" style="--cc:var(--c-${c})">${ICON[c]} ${n}</span>`).join(" ");
      html+=`<tr class="grow fg${one&&gaps(fs).bad.length?" hasgap":""}" data-tg="${gi}"><td class="chk"><input type="checkbox" data-g="${gi}" aria-label="Ordner markieren"></td>
        <td class="f" colspan="2"><button type="button" class="cv" data-tg="${gi}" aria-expanded="${open}" aria-label="Dateien ${open?"einklappen":"ausklappen"}">${open?"▾":"▸"}</button>📁 <b>${k?esc(k):"Lose Dateien"}</b> <span class="sub">${fs.length} Datei(en), ${fmt(fs.reduce((a,f)=>a+f.size,0))}</span><div style="margin-top:4px">${summ}</div>${one?gapHtml(fs):""}</td>
        <td class="c"><select data-g="${gi}" aria-label="Ganzen Ordner zuordnen">${catOpts("Ordner zuordnen als…")}</select></td><td class="s"></td></tr>`
        +(open?fs.map(f=>row(f,true)).join(""):"")})
  }else html=list.map(f=>row(f)).join("");
  $("rows").innerHTML=html;
  $("rows").querySelectorAll("input[data-i]").forEach(c=>c.onchange=()=>{files[c.dataset.i].sel=c.checked;count();refreshGroups()});
  const rangeSel=(sel,last)=>$("rows").querySelectorAll(sel).forEach(c=>c.addEventListener("click",ev=>{
    const all=[...$("rows").querySelectorAll(sel)],i=all.indexOf(c),j=last.v;
    if(ev.shiftKey&&j!==null&&j!==i&&all[j]){const [a,b]=[Math.min(i,j),Math.max(i,j)];
      all.slice(a,b+1).forEach(x=>{(sel==="input[data-i]"?[files[x.dataset.i]]:groups[x.dataset.g]).forEach(f=>f.sel=c.checked)});
      last.v=i;setTimeout(render,0);return}
    last.v=i}));
  rangeSel("input[data-i]",lastRow);rangeSel("input[data-g]",lastGrp);
  $("rows").querySelectorAll("input[data-g]").forEach(c=>c.onchange=()=>{groups[c.dataset.g].forEach(f=>f.sel=c.checked);render()});
  $("rows").querySelectorAll("select[data-i]").forEach(s=>s.onchange=()=>assign([files[s.dataset.i]],s.value));
  $("rows").querySelectorAll("select[data-g]").forEach(s=>s.onchange=()=>assign(groups[s.dataset.g],s.value));
  $("rows").querySelectorAll("button[data-sg]").forEach(b=>b.onclick=()=>openSeries(sgroups[b.dataset.sg]));
  $("rows").querySelectorAll("button[data-split]").forEach(b=>b.onclick=()=>splitSeries(groups[b.dataset.split]));
  $("rows").querySelectorAll("button[data-fg]").forEach(b=>b.onclick=()=>openFolder(groups[b.dataset.fg]));
  $("rows").querySelectorAll("tr[data-tg]").forEach(t=>t.onclick=ev=>{
    if(ev.target.closest("input,select,button:not(.cv)"))return;
    const k=sgroups[t.dataset.tg].key;sopen.has(k)?sopen.delete(k):sopen.add(k);render()});
  refreshGroups();count()}
async function assign(list,cat){
  if(!cat||!list.length){render();return}
  const changed=list.filter(f=>f.category&&f.category!==cat).length;
  if(changed&&!confirm(`${changed} Datei(en) haben schon eine andere Kategorie und werden auf „${cat}“ umgestellt. Fortfahren?`)){render();return}
  try{
    const r=await api("/api/preview/batch",{items:list.map(f=>({path:f.path,category:cat}))});
    list.forEach((f,i)=>{f.category=cat;f.sel=true;f.target=r.targets[i]});
    toast(`${list.length} Datei(en) zugeordnet: ${ICON[cat]} ${cat}`,"ok")
  }catch(e){toast(e.message,"bad")}
  tabs();render()}
const selected=()=>files.filter(f=>f.category&&f.sel===true);
function updateGo(){const s=selected();
  $("go").disabled=!s.length;
  $("go").textContent=s.length?`${running?"➕ einreihen":"▶ kopieren"}: ${s.length} ${s.length===1?"Datei":"Dateien"} (${fmt(s.reduce((a,f)=>a+f.size,0))})`:"Nichts ausgewählt"}
function mergeCandidates(){const m=new Map();files.filter(checked).forEach(f=>{if(f.series)m.set(f.series.key,(m.get(f.series.key)||[]).concat(f))});return m}
$("merge").onclick=()=>{const m=mergeCandidates(),fs=[...m.values()].flat(),names={},cats={};
  fs.forEach(f=>{names[f.series.name]=(names[f.series.name]||0)+1;const c=f.category||"?";cats[c]=(cats[c]||0)+1});
  openSeries({key:"",names,files:fs,cats})};
function count(){const mc=mergeCandidates().size;$("merge").hidden=mc<2;$("mbtn").hidden=!mc;const m=files.filter(checked),u=unclearN(),un=m.filter(f=>!f.category).length;
  $("sel").textContent=`${m.length} markiert`+(u?`, ${u} ohne Kategorie`:"");
  $("bulk").hidden=!m.length;$("bn").textContent=`${m.length} markierte zuordnen als`+(un?` (davon ${un} unklar)`:"");
  updateGo()}
$("reload").onclick=loadFiles;
$("shc").onclick=()=>{showCopied=!showCopied;loadFiles()};
$("all").onclick=()=>{visible().forEach(f=>f.sel=true);render()};
$("none").onclick=()=>{visible().forEach(f=>f.sel=false);render()};
$("inv").onclick=()=>{visible().forEach(f=>f.sel=!f.sel);render()};
let fqt;$("fq").addEventListener("input",()=>{clearTimeout(fqt);fqt=setTimeout(()=>{fqs=$("fq").value.toLowerCase().split(/\s+/).filter(Boolean);if(!fqs.length)fqs="";render()},150)});
$("fmark").onclick=()=>{visible().forEach(f=>f.sel=true);render();toast(`${visible().length} Treffer markiert`,"ok")};
document.addEventListener("keydown",e=>{
  if(view!=="sort"||dlgOpen()||e.target.closest("input,select,textarea"))return;
  if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==="a"){e.preventDefault();visible().forEach(f=>f.sel=true);render()}});
$("master").onchange=e=>{visible().forEach(f=>f.sel=e.target.checked);render()};
function setGroup(){try{localStorage.setItem("sort_grp",grouped?"1":"0");localStorage.setItem("sort_sgrp",sgrouped?"1":"0")}catch{}
  $("grp").classList.toggle("on",grouped);$("sgrp").classList.toggle("on",sgrouped);render()}
$("grp").onclick=()=>{grouped=!grouped;if(grouped)sgrouped=false;setGroup()};
$("sgrp").onclick=()=>{sgrouped=!sgrouped;if(sgrouped)grouped=false;setGroup()};
$("go").onclick=async()=>{
  try{const r=await api("/api/start",{items:selected().map(f=>({path:f.path,category:f.category}))});$("err").textContent="";toast(r.queued?`In die Warteschlange gestellt (Platz ${r.position})`:"Kopieren gestartet","ok");poll();reloadKeep()}
  catch(e){$("err").textContent=e.message}};
$("stop").onclick=()=>api("/api/cancel",{}).then(()=>toast("Wird abgebrochen…"));

/* Neue Gruppe aus Markierten (Rechtsklick oder Knopf) */
let gFiles=[],gtm;
function openGroup(list){
  list=list.filter(Boolean);
  if(!list.length){toast("Erst Dateien markieren.","bad");return}
  gFiles=list;
  const cnt={},cats={};list.forEach(f=>{if(f.series){cnt[f.series.name]=(cnt[f.series.name]||0)+1}if(f.category)cats[f.category]=(cats[f.category]||0)+1});
  const top=m=>Object.entries(m).sort((a,b)=>b[1]-a[1]).map(e=>e[0])[0]||"";
  const pre=list.map(f=>f.path.split("/").pop().replace(/\.[^.]+$/,"").replace(/[._]+/g," ")).reduce((a,b)=>{let i=0;while(i<a.length&&i<b.length&&a[i].toLowerCase()===b[i].toLowerCase())i++;return a.slice(0,i)}).replace(/[\s\-\d(\[]+$/,"").trim();
  $("gname").value=top(cnt)||(pre.length>=3?pre:"");
  const y=list.find(f=>f.series&&f.series.year);$("gyear").value=y?y.series.year:"";$("gtmdb").value="";
  const serc=["Serien","Anime","Dokus"],cur=serc.includes(top(cats))?top(cats):"Serien";
  $("gcat").innerHTML=serc.map(c=>`<option value="${c}" ${c===cur?"selected":""}>${ICON[c]} ${c}</option>`).join("");
  const sn={};list.forEach(f=>{if(f.series)sn[f.series.season]=(sn[f.series.season]||0)+1});$("gseason").value=top(sn)||1;
  $("gmode").value=list.every(f=>f.series)?"keep":"renumber";$("gstart").value=1;
  $("gdet").textContent=`${list.length} Datei(en) werden zu einer Serie zusammengefasst.`;
  $("gdlg").showModal();gprev();$("gname").focus();$("gname").select()}
const gBody=()=>({items:gFiles.map(f=>f.path),name:$("gname").value,year:$("gyear").value,tmdb:$("gtmdb").value,category:$("gcat").value,season:$("gseason").value,mode:$("gmode").value,start:$("gstart").value});
async function gprev(){
  $("gstart").disabled=$("gmode").value!=="renumber";
  const body=gBody();
  try{const r=await api("/api/groups/preview",body);if(JSON.stringify(body)!==JSON.stringify(gBody()))return;
    const order=[...new Set(body.items)].sort((a,b)=>0),n=gFiles.length;
    $("gcount").innerHTML=`<b>${n} Datei(en)</b> werden so abgelegt:`;
    $("gprev").innerHTML=r.targets.slice(0,300).map((t,i)=>`<div>${esc(body.items[i]||"")}<small>→ ${esc(t)}</small></div>`).join("")+(r.targets.length>300?`<div>… und ${r.targets.length-300} weitere</div>`:"")}
  catch(e){$("gcount").textContent=e.message;$("gprev").innerHTML=""}}
["gname","gyear","gtmdb","gseason","gstart"].forEach(id=>$(id).addEventListener("input",()=>{clearTimeout(gtm);gtm=setTimeout(gprev,250)}));
["gcat","gmode"].forEach(id=>$(id).addEventListener("change",gprev));
$("gcancel").onclick=()=>$("gdlg").close();
$("gform").onsubmit=async ev=>{ev.preventDefault();
  try{const r=await api("/api/groups",gBody());$("gdlg").close();await reloadKeep();toast(`Gruppe mit ${r.n} Datei(en) angelegt`,"ok")}
  catch(e){toast(e.message,"bad")}};
async function dissolveGroup(list){
  const fs=list.filter(f=>f.series&&f.series.manual);
  if(!fs.length){toast("In der Auswahl ist keine eigene Gruppe.","bad");return}
  if(!confirm(`${fs.length} Datei(en) aus ihrer eigenen Gruppe lösen? Sie bekommen wieder den erkannten Namen.`))return;
  try{await api("/api/groups/delete",{items:fs.map(f=>f.path)});await reloadKeep();toast("Gruppe aufgelöst","ok")}catch(e){toast(e.message,"bad")}}
$("gbtn").onclick=()=>openGroup(visible().filter(checked));

/* Rechtsklick: Ziel bearbeiten (öffnet den Zielordner-Dialog mit dem vorhandenen Pfad) */
let ctxList=null,ctxAt=0,ctxGrp=[];
const hideCtx=()=>{$("ctx").hidden=true};
$("rows").addEventListener("contextmenu",ev=>{
  if(ev.target.closest("input,select"))return;
  const tr=ev.target.closest("tr");if(!tr)return;let list=null;
  if(tr.classList.contains("grow")){const cb=tr.querySelector("input[data-g]");list=cb&&groups[cb.dataset.g]}
  else{const cb=tr.querySelector("input[data-i]");list=cb&&[files[cb.dataset.i]]}
  if(!list)return;
  ev.preventDefault();ctxList=list;ctxAt=Date.now();
  const marked=visible().filter(checked),base=marked.length?marked:list;ctxGrp=base;
  $("ctxgrp").textContent=`👥 Neue Gruppe${marked.length?` aus ${marked.length} Markierten`:" aus diesen "+list.length}`;
  $("ctxdis").hidden=!base.some(f=>f.series&&f.series.manual);
  $("ctxedit").hidden=!list.some(f=>f.category&&f.target);
  const m=$("ctx");m.hidden=false;m.style.left=Math.max(4,Math.min(ev.clientX,innerWidth-m.offsetWidth-8))+"px";m.style.top=Math.max(4,Math.min(ev.clientY,innerHeight-m.offsetHeight-8))+"px"});
$("ctxgrp").onclick=()=>{hideCtx();openGroup(ctxGrp)};
$("ctxdis").onclick=()=>{hideCtx();dissolveGroup(ctxGrp)};
$("ctxedit").onclick=()=>{hideCtx();if(ctxList)openFolder(ctxList.filter(f=>f.category&&f.target))};
document.addEventListener("click",hideCtx);document.addEventListener("keydown",e=>{if(e.key==="Escape")hideCtx()});
window.addEventListener("scroll",()=>{if(Date.now()-ctxAt>500)hideCtx()},true);

/* Verlauf */
async function loadHist(){hist=(await api("/api/history")).items;setBadge("nh",hist.length);$("st-hist").textContent=hist.length;renderHist()}
function renderHist(){
  const q=$("hq").value.toLowerCase();
  const list=hist.map((h,i)=>({h,i})).filter(({h})=>!q||(h.full+" "+h.cat).toLowerCase().includes(q));
  $("hempty").hidden=list.length>0;
  $("hrows").innerHTML=list.map(({h,i})=>`<tr data-cat="${esc(h.cat)}"><td class="s">${fdate(h.t)}</td><td><span class="tag cat">${ICON[h.cat]||"📁"} ${esc(h.cat)}</span></td>
   <td class="span"><div class="path">${esc(h.full)}</div>${h.status!=="verschoben"?`<div class="t">${esc(h.status)}</div>`:""}</td>
   <td class="s">${fmt(h.size)}</td><td><button data-i="${i}">📋 Pfad kopieren</button></td></tr>`).join("");
  $("hrows").querySelectorAll("button").forEach(b=>b.onclick=()=>copyText(hist[b.dataset.i].full,b))}
$("hq").oninput=renderHist;
$("hclear").onclick=async()=>{if(confirm("Verlauf wirklich leeren? Die Dateien selbst bleiben unberührt.")){await api("/api/history/clear",{});toast("Verlauf geleert","ok");loadHist()}};

/* Aufräumen */
let dirs=[];
async function loadClean(){dirs=(await api("/api/leftovers")).dirs;setBadge("nc",dirs.length);
  $("cempty2").hidden=dirs.length>0;$("cdelall").disabled=!dirs.some(d=>!d.locked);$("cwipe").disabled=!dirs.length;
  $("crows").innerHTML=dirs.map((d,i)=>`<tr><td class="f span">${d.type==="dir"?"📁":"📄"} ${esc(d.path)}</td>
   <td class="t span">${d.type==="dir"?d.files+" Datei(en), ":""}${fmt(d.size)}${d.sample.length?"<br>"+d.sample.map(esc).join(", "):""}${d.archives?"<br>enthält Archive, evtl. noch nicht entpackt":""}</td>
   <td>${d.locked?`<span class="tag warn">🔒 ${esc(d.locked)}</span>`:'<span class="tag ok">löschbar</span>'}</td>
   <td><button class="danger" data-i="${i}">${d.locked?"Trotzdem löschen":"Löschen"}</button></td></tr>`).join("");
  $("crows").querySelectorAll("button").forEach(b=>b.onclick=()=>delDir(dirs[b.dataset.i]))}
function report(r,what){if(r.errors&&r.errors.length)toast(r.errors.join("\n"),"bad");else toast(what,"ok")}
async function delDir(d){
  const msg=d.locked
    ?`ACHTUNG: "${d.path}" ist gesperrt (${d.locked}${d.media?", "+d.media+" Mediendatei(en)":""}).\nTrotzdem endgültig löschen (${fmt(d.size)})? Das kann nicht rückgängig gemacht werden.`
    :`"${d.path}" (${fmt(d.size)}) endgültig löschen?`;
  if(!confirm(msg))return;
  try{report(await api("/api/leftovers/delete",{paths:[d.path],force:!!d.locked}),"Gelöscht")}catch(e){toast(e.message,"bad")}
  loadClean()}
$("cdelall").onclick=async()=>{
  const n=dirs.filter(d=>!d.locked).length;
  if(!confirm(`${n} Einträge endgültig löschen?`))return;
  try{report(await api("/api/leftovers/delete",{all:true}),`${n} Einträge gelöscht`)}catch(e){toast(e.message,"bad")}
  loadClean()};
$("cwipe").onclick=async()=>{
  const sz=dirs.reduce((a,d)=>a+d.size,0);
  const t=prompt(`Alle ${dirs.length} Einträge (${fmt(sz)}) in der Quelle endgültig löschen, auch Medien und laufende Downloads?\nZur Bestätigung LÖSCHEN eingeben:`);
  if(t!=="LÖSCHEN")return;
  try{report(await api("/api/leftovers/delete",{all:true,force:true}),"Alles gelöscht")}catch(e){toast(e.message,"bad")}
  loadClean()};
$("cempty").onclick=async()=>{try{const r=await api("/api/leftovers/empty",{});toast(`${r.deleted} leere Ordner entfernt`,"ok")}catch(e){toast(e.message,"bad")}loadClean()};
$("creload").onclick=loadClean;

/* Analyse */
let ascope="target",ares=null,ashow=100,afilter="Alle",anaWas=false,atimer,P={h264:40,old:60};
try{const q=JSON.parse(localStorage.getItem("ana_p")||"null");if(q&&q.h264&&q.old)P=q}catch{}
const OLDC=new Set(["MPEG-1","MPEG-2","MPEG-4","VC-1","WMV","Theora","VP8"]);
const CCOL={"H.264":"var(--c-Filme)","H.265":"var(--c-Dokus)","AV1":"var(--c-Serien)","VP9":"var(--muted)","MPEG-4":"var(--c-Anime)","MPEG-2":"var(--c-)","VC-1":"var(--c-)","WMV":"var(--c-)"};
const ccol=c=>CCOL[c]||"var(--muted)";
const rate=c=>c==="H.264"?P.h264:OLDC.has(c)?P.old:c==="VP9"?15:0;
const saving=f=>f.s*rate(f.codec)/100;
function drawProg(st){
  $("aprog").hidden=!st.running;$("astop").hidden=!st.running;$("astart").disabled=st.running||!$("anoff").hidden;
  const pct=st.total?Math.min(100,st.done/st.total*100):0;
  $("apb").style.width=pct+"%";$("apct").textContent=pct.toFixed(0)+" %";
  $("atxt").textContent=`${st.done} von ${st.total} Dateien${st.current?": "+st.current:""}`;
  $("aerr").hidden=!st.error;$("aerr").textContent=st.error?"⚠️ "+st.error:""}
async function loadAna(){
  const st=await api("/api/analysis/status");
  if(st.running)ascope=st.scope;
  $("anoff").hidden=st.ffprobe;
  $("ascope").innerHTML=[["target","📦 Ziel (NFS)"],["source","📥 Quelle"]].map(([k,l])=>
    `<button class="chip ${k===ascope?"on":""}" data-c="Alle" data-s="${k}" title="${esc(st.roots[k])}">${l}</button>`).join("");
  $("ascope").querySelectorAll("button").forEach(b=>b.onclick=()=>{ascope=b.dataset.s;ashow=100;afilter="Alle";loadAna()});
  drawProg(st);if(st.running){anaWas=true;anaPoll()}
  $("p264").value=P.h264;$("pold").value=P.old;
  const r=await api("/api/analysis/results?scope="+ascope);ares=r;
  $("ameta").textContent=r.ts?`Stand ${fdate(r.ts)}, ${r.root}`:r.root;
  renderAna()}
function anaPoll(){clearTimeout(atimer);atimer=setTimeout(async()=>{
  if(view!=="ana")return;
  try{const st=await api("/api/analysis/status");drawProg(st);
    if(st.running){anaWas=true;anaPoll()}
    else if(anaWas){anaWas=false;toast(st.error?"Analyse fehlgeschlagen":"Analyse fertig",st.error?"bad":"ok");loadAna()}}catch{anaPoll()}},1000)}
function renderAna(){
  const items=ares?ares.items:[],has=items.length>0;
  $("aempty").hidden=has;$("abody").hidden=!has;
  if(!has){const none=!!(ares&&ares.ts);$("aet").textContent=none?"Keine Videodateien gefunden":"Noch keine Analyse";
    $("aes").textContent=none?"In diesem Ordner liegen keine Videos.":"Starte die Analyse, um die Codecs zu sehen.";return}
  const tot=items.reduce((a,f)=>a+f.s,0),sav=items.reduce((a,f)=>a+saving(f),0);
  $("a-n").textContent=items.length;$("a-size").textContent=fmt(tot);
  $("a-sav").textContent="≈ "+fmt(sav);$("a-pct").textContent=tot?`${(sav/tot*100).toFixed(0)} % deiner Videos, wenn alles H.265 wäre`:"";
  const g={};items.forEach(f=>{const x=g[f.codec]||(g[f.codec]={n:0,s:0,v:0});x.n++;x.s+=f.s;x.v+=saving(f)});
  const codecs=Object.entries(g).sort((a,b)=>b[1].s-a[1].s);
  $("sbar").innerHTML=codecs.map(([c,v])=>`<i style="width:${v.s/tot*100}%;background:${ccol(c)}" title="${esc(c)}: ${fmt(v.s)}"></i>`).join("");
  $("clist").innerHTML=codecs.map(([c,v])=>`<div class="crow"><span class="tag cat" style="--cc:${ccol(c)}">${esc(c)}</span><span>${v.n} Datei(en)</span>
    <span>${fmt(v.s)} <small class="sub">(${(v.s/tot*100).toFixed(0)} %)</small></span><b class="${v.v?"okc":"sub"}">${v.v?"−"+fmt(v.v):"–"}</b></div>`).join("");
  $("v264").textContent=P.h264+" %";$("vold").textContent=P.old+" %";
  if(afilter!=="Alle"&&!g[afilter])afilter="Alle";
  $("afilter").innerHTML=["Alle",...codecs.map(c=>c[0])].map(c=>`<button class="chip ${c===afilter?"on":""}" data-c="Alle" data-f="${esc(c)}" style="--cc:${c==="Alle"?"var(--brand)":ccol(c)}">${esc(c)} <i>${c==="Alle"?items.length:g[c].n}</i></button>`).join("");
  $("afilter").querySelectorAll("button").forEach(b=>b.onclick=()=>{afilter=b.dataset.f;ashow=100;renderAna()});
  const q=$("aq").value.toLowerCase();
  const list=items.filter(f=>(afilter==="Alle"||f.codec===afilter)&&(!q||f.path.toLowerCase().includes(q)))
    .sort((a,b)=>saving(b)-saving(a)||b.s-a.s);
  $("anone").hidden=list.length>0;$("amore").hidden=list.length<=ashow;$("amore").textContent=`Mehr anzeigen (${list.length-ashow} weitere)`;
  $("arows").innerHTML=list.slice(0,ashow).map(f=>{const sv=saving(f);
    const info=[f.w&&f.h?`${f.w}×${f.h}`:"",f.br?(f.br/1e6).toFixed(1)+" Mb/s":"",/10/.test(f.pix||"")?"10-bit":""].filter(Boolean).join(", ")||"–";
    return `<tr style="--cc:${ccol(f.codec)}"><td class="f span">${esc(f.path)}</td><td><span class="tag cat" style="--cc:${ccol(f.codec)}">${esc(f.codec)}</span></td>
    <td class="t">${esc(info)}</td><td class="s">${fmt(f.s)}</td><td class="s ${sv?"okc":""}">${sv?"−"+fmt(sv):"–"}</td></tr>`}).join("")}
$("aq").oninput=()=>{ashow=100;renderAna()};
$("amore").onclick=()=>{ashow+=200;renderAna()};
function setP(){P={h264:+$("p264").value,old:+$("pold").value};try{localStorage.setItem("ana_p",JSON.stringify(P))}catch{}renderAna()}
$("p264").oninput=setP;$("pold").oninput=setP;
$("astart").onclick=async()=>{try{await api("/api/analysis/start",{scope:ascope});anaWas=true;toast("Analyse gestartet","ok");drawProg(await api("/api/analysis/status"));anaPoll()}catch(e){toast(e.message,"bad")}};
$("astop").onclick=()=>api("/api/analysis/cancel",{}).then(()=>toast("Wird abgebrochen…"));
$("acsv").onclick=()=>{if(!ares)return;
  const rows=[["Datei","Codec","Breite","Hoehe","Bitrate_bps","Groesse_Bytes","Geschaetzte_Ersparnis_Bytes"],...ares.items.map(f=>[f.path,f.codec,f.w,f.h,f.br,f.s,Math.round(saving(f))])];
  const csv="﻿"+rows.map(r=>r.map(v=>`"${String(v??"").replace(/"/g,'""')}"`).join(";")).join("\r\n");
  const a=document.createElement("a");a.href=URL.createObjectURL(new Blob([csv],{type:"text/csv"}));a.download=`codec-analyse-${ascope}.csv`;a.click();setTimeout(()=>URL.revokeObjectURL(a.href),1000)};

/* Zeitplan + Einstellungen */
const PRESETS=[["Alle 15 Min","*/15 * * * *"],["Stündlich","0 * * * *"],["Alle 6 Std","0 */6 * * *"],["Täglich 03:00","0 3 * * *"],["Täglich 06:00","0 6 * * *"],["Sonntags 04:00","0 4 * * 0"]];
function drawPresets(){const v=$("cron").value.trim().replace(/\s+/g," ");
  $("presets").innerHTML=PRESETS.map(([n,e])=>`<button class="chip ${v===e?"on":""}" data-c="Alle" data-e="${e}">${n}</button>`).join("");
  $("presets").querySelectorAll("button").forEach(b=>b.onclick=()=>{$("cron").value=b.dataset.e;$("cron").oninput()})}
function lastInfo(s){$("last").textContent=s.last_run?`${fdate(s.last_run)}: ${s.last_result}`:"Noch kein Lauf."}
function nextList(n,err){$("cerr").textContent=err||"";$("next").innerHTML=n.map(x=>`<li>${esc(x)}</li>`).join("")}
function tokHint(set){$("tgtok").value="";$("tgtok").placeholder=set?"Token gespeichert (leer lassen zum Behalten)":"Bot-Token (123456:ABC…)"}
function fillSettings(s){
  $("en").checked=s.enabled;$("cron").value=s.cron;$("mv").checked=s.move_after;$("cu").checked=s.cleanup_after;$("jf").checked=s.jellyfin!==false;
  $("tge").checked=s.tg_enabled;$("puburl").value=s.public_url||"";$("tgchat").value=s.tg_chat;$("tgs").checked=s.tg_start;$("tgd").checked=s.tg_done;$("tgu").checked=s.tg_unclear;
  $("tgn").checked=s.tg_new;$("tgp").checked=s.tg_progress;$("tgr").checked=s.tg_errors;$("tgq").checked=s.tg_silent;
  tokHint(s.tg_token_set);drawPresets();
  $("tz").textContent=s.tz;$("nop").hidden=$("nop2").hidden=s.persist;nextList(s.next,s.error);lastInfo(s);
  for(const id of["saved","saved2","tgmsg"])$(id).textContent="";
  const i=s.info,kv=(k,v,h)=>`<div class="kv"><span class="sub">${k}</span><span>${v}${h?`<br><small class="sub">${h}</small>`:""}</span></div>`;
  $("sysinfo").innerHTML=kv("Quelle",`<code>${esc(i.source)}</code>`)+kv("Ziel (NFS)",`<code>${esc(i.target)}</code>`)
    +kv("Einstellungen",`<code>${esc(i.data)}</code>`,s.persist?"werden dauerhaft gespeichert":"nicht dauerhaft, Volume fehlt")
    +kv("Zeitzone",esc(s.tz))+kv("Mindestalter",`${i.min_age} s`,"Jüngere Dateien ignoriert der Zeitplan, weil sie evtl. noch geschrieben werden")
    +kv("Neue Dateien prüfen",`alle ${i.watch} s`,"Telegram meldet sie, sobald sich zwei Prüfungen in Folge nicht mehr ändern")
    +kv("Login",i.auth?"aktiv":"aus","AUTH_USER und AUTH_PASS in der docker-compose.yml")+kv("Codec-Analyse",i.ffprobe?"ffprobe vorhanden":"ffprobe fehlt")}
async function loadSched(){fillSettings(await api("/api/settings"));await loadRules();drawRules()}
let pt;$("cron").oninput=()=>{drawPresets();clearTimeout(pt);pt=setTimeout(async()=>{
  const r=await api("/api/cron?expr="+encodeURIComponent($("cron").value));nextList(r.next||[],r.error)},300)};
const collect=()=>({enabled:$("en").checked,cron:$("cron").value,move_after:$("mv").checked,cleanup_after:$("cu").checked,jellyfin:$("jf").checked,
  tg_enabled:$("tge").checked,tg_token:$("tgtok").value,tg_chat:$("tgchat").value,tg_start:$("tgs").checked,tg_done:$("tgd").checked,tg_unclear:$("tgu").checked,
  tg_new:$("tgn").checked,tg_progress:$("tgp").checked,tg_errors:$("tgr").checked,tg_silent:$("tgq").checked,public_url:$("puburl").value});
async function saveAll(out){for(const id of["saved","saved2","tgmsg"])$(id).textContent="";try{
  const s=await api("/api/settings",collect());nextList(s.next,s.error);tokHint(s.tg_token_set);toast("Gespeichert","ok");poll()}
  catch(e){out.textContent=e.message;toast(e.message,"bad")}}
$("save").onclick=()=>saveAll($("saved"));$("save2").onclick=()=>saveAll($("saved2"));$("tgsave").onclick=()=>saveAll($("tgmsg"));
async function sendTest(urgent){$("tgmsg").textContent="Sende…";
  try{await api("/api/telegram/test",{token:$("tgtok").value,chat:$("tgchat").value,urgent});$("tgmsg").textContent="";toast(urgent?"Dringender Test gesendet":"Test gesendet","ok")}
  catch(e){$("tgmsg").textContent="";toast("Telegram: "+e.message,"bad")}}
$("tgtest").onclick=()=>sendTest(false);$("tgtest2").onclick=()=>sendTest(true);
$("runnow").onclick=async()=>{try{const r=await api("/api/run",{});$("runmsg").textContent=r.info;poll()}catch(e){$("runmsg").textContent=e.message}};

/* Läuft */
const hms=sec=>{sec=Math.max(0,Math.round(sec));const h=Math.floor(sec/3600),m=Math.floor(sec%3600/60),s=sec%60;return h?`${h} Std ${m} Min`:m?`${m} Min ${s} s`:`${s} s`};
const QST={kopiert:["▶️","kopiert"],wartet:["⏳","wartet"],fertig:["✅","fertig"],vorhanden:["✅","schon da"],fehler:["⛔","Fehler"],abgebrochen:["⏹️","abgebrochen"]};
async function renderRun(s){
  const pct=s.total?Math.min(100,s.done/s.total*100):0,act=s.running,has=act||s.finished;
  $("rtitle").textContent=act?"⚡ Kopiert gerade":s.finished?(s.errors.length?"⚠️ Zuletzt: fertig mit Fehlern":"✅ Zuletzt: fertig"):"💤 Gerade läuft nichts";
  $("rsub").textContent=has?`${s.source||""} · ${s.files_total} Datei(en)`+(s.elapsed?` · Dauer ${hms(s.elapsed)}`:""):"";
  $("rpct").hidden=!has;$("rpct").textContent=pct.toFixed(0)+" %";
  $("ractive").hidden=!has;$("ridle").hidden=has;$("rstop").hidden=!act;
  $("rbar").classList.toggle("run",act);$("rbi").style.width=pct+"%";
  $("rfiles").textContent=`${s.files_done} / ${s.files_total}`;$("rdata").textContent=`${fmt(s.done)} / ${fmt(s.total)}`;
  const avg=s.elapsed>2?s.done/s.elapsed:0;
  $("rspd").textContent=act&&s.speed?fmt(s.speed)+"/s":"–";
  $("reta").textContent=act&&avg?hms((s.total-s.done)/avg):act?"…":"–";
  $("rcur").hidden=!act||!s.current;$("rcn").textContent=s.current||"";
  $("rcp").textContent=s.current_size?`${fmt(s.current_done)} / ${fmt(s.current_size)}`:"";
  $("rcb").style.width=(s.current_size?s.current_done/s.current_size*100:0)+"%";
  const w=s.waiting||[];$("rwaitcard").hidden=!w.length;
  if(w.length){$("rwaitsub").textContent=`${w.length} ${w.length===1?"Auftrag wartet":"Aufträge warten"} auf den laufenden Vorgang`;
    $("rwait").innerHTML=w.map((b,i)=>`<div class="rule"><div><b>${i+1}. ${esc(b.source)}</b><span class="sub">${b.files} Datei(en) · ${fmt(b.size)}</span></div><button data-q="${b.id}">✖ Entfernen</button></div>`).join("");
    $("rwait").querySelectorAll("button").forEach(x=>x.onclick=async()=>{await api("/api/queue/remove",{id:+x.dataset.q});toast("Aus der Warteschlange entfernt");poll()})}
  const sc=s.schedule;
  $("ridle-txt").innerHTML=(sc.last?`Letzter Lauf: ${esc(sc.last)}<br>`:"")+(sc.enabled?`⏰ Nächster Zeitplan-Lauf: <b>${esc(sc.next||"–")}</b>`:"⏰ Zeitplan ist aus, es läuft nur manuell.");
  $("rqwrap").hidden=!has;
  if(has){try{
    const q=await api("/api/job/queue"),c=s.counts||{};
    $("rchips").innerHTML=["kopiert","wartet","fertig","vorhanden","fehler","abgebrochen"].filter(k=>c[k]).map(k=>`<span class="tag ${k==="fehler"||k==="abgebrochen"?"bad":k==="fertig"||k==="vorhanden"?"ok":""}">${QST[k][0]} ${c[k]} ${QST[k][1]}</span>`).join("");
    $("rrows").innerHTML=q.items.map(x=>`<tr class="qrow ${x.state}"><td class="qs">${QST[x.state][0]} ${QST[x.state][1]}</td><td class="t" title="${esc(x.err||"")}">${esc(x.name)}${x.err?`<br><small class="sub">${esc(x.err)}</small>`:""}</td><td class="s">${fmt(x.size)}</td></tr>`).join("");
    $("rhid").textContent=q.hidden?`… und ${q.hidden} weitere ausgeblendet`:""}catch{}}
  try{const a=await api("/api/analysis/status");$("ranacard").hidden=!a.running;
    if(a.running){$("ranatxt").textContent=`${a.done} von ${a.total||"…"} Dateien · ${a.current||""}`;$("ranabar").style.width=(a.total?a.done/a.total*100:5)+"%"}}catch{}}
$("rwaitclear").onclick=async()=>{await api("/api/queue/clear",{});toast("Warteschlange geleert");poll()};
$("rstop").onclick=()=>api("/api/cancel",{}).then(()=>toast("Wird abgebrochen…"));
$("rtosort").onclick=()=>show("sort");

/* Fortschritt + Zeitplan-Anzeige (läuft immer, zeigt auch Zeitplan-Läufe) */
let wasRunning=false,lastDone=-1;
const dlgOpen=()=>[...document.querySelectorAll("dialog")].some(d=>d.open);
$("schgo").onclick=()=>show("sched");
async function poll(){try{
  const s=await api("/api/status");
  running=s.running;
  const pct=s.total?Math.min(100,s.done/s.total*100):0;
  $("pt").style.width=pct+"%";$("pct").textContent=pct.toFixed(0)+" %";
  $("pf").style.width=(s.current_size?s.current_done/s.current_size*100:0)+"%";
  $("cur").textContent=s.running?`${s.source||""}: ${s.current}`:"";
  $("spd").textContent=s.running&&s.speed?fmt(s.speed)+"/s":"";
  $("txt").textContent=`${s.files_done} von ${s.files_total} Dateien, ${fmt(s.done)} von ${fmt(s.total)}`;
  const sc=s.schedule;$("sch").title=sc.last||"";
  $("sch").innerHTML=sc.enabled?`<span class="pill on">⏰ Zeitplan an</span> <span><span class="hide-m">nächster Lauf</span> <b>${esc(sc.next||"–")}</b></span> <code class="hide-m">${esc(sc.cron)}</code>`
    :'<span class="pill">⏰ Zeitplan aus, nur manuell</span>';
  $("dk-run").hidden=!s.running;$("dk-tot").hidden=!(s.running||s.finished);
  $("ptw").classList.toggle("run",s.running);
  $("stop").hidden=!s.running;$("err").textContent=s.errors.join("\n");updateGo();
  if(wasRunning&&!s.running){
    toast(s.errors.length?`Fertig mit ${s.errors.length} Fehler(n)`:`Fertig: ${s.files_done} von ${s.files_total} Dateien`,s.errors.length?"bad":"ok");
    show(view)}
  const nr=$("nr");const wn=(s.waiting||[]).length;nr.hidden=!s.running&&!wn;nr.textContent=(s.running?Math.round(pct)+"%":"")+(wn?` +${wn}`:"");
  if(view==="run")renderRun(s);
  if(s.running&&view==="sort"&&s.files_done!==lastDone&&!dlgOpen()){lastDone=s.files_done;reloadKeep()}
  if(!s.running)lastDone=-1;
  wasRunning=s.running}catch{}}
setInterval(poll,1000);
const HV={clarify:"sort",sort:"sort",run:"run",hist:"hist",clean:"clean",ana:"ana",sched:"sched",set:"set"};
function route(){const h=location.hash.slice(1);if(h==="clarify")filter="Klärung";show(HV[h]||"sort")}
window.addEventListener("hashchange",route);
const dockEl=document.querySelector(".dock");
new ResizeObserver(()=>{document.querySelector("main").style.paddingBottom=dockEl.offsetHeight+24+"px"}).observe(dockEl);
route();poll();
api("/api/history").then(r=>{setBadge("nh",r.items.length);$("st-hist").textContent=r.items.length});
api("/api/leftovers").then(r=>setBadge("nc",r.dirs.length));
</script></body></html>"""

def cli():
    global CLI_MODE
    CLI_MODE = True
    items, unclear = scan()
    unclear_notice(unclear)
    print(f"{len(items)} Datei(en) bereit ({SOURCE} -> {TARGET}), {len(unclear)} zur Klärung übersprungen")
    for u in unclear: print("   ? Klärung:", u)
    for it in items:
        print("  ", it["src"].relative_to(SOURCE), "->", it["dst"])
    if "--dry-run" in sys.argv or not items: return 0
    if not TARGET.is_dir():
        print("FEHLER: Ziel nicht erreichbar (NFS gemountet?)"); return 2
    t = threading.Thread(target=run_job, args=(items, "CLI")); t.start()
    while t.is_alive():
        time.sleep(5)
        with lock:
            pct = job["done"] / job["total"] * 100 if job["total"] else 0
            print(f"[{pct:5.1f}%] {job['files_done']}/{job['files_total']} {job['current']}", flush=True)
    t.join()
    for e in job["errors"]: print("FEHLER:", e)
    return 1 if job["errors"] else 0

if __name__ == "__main__":
    if "--auto" in sys.argv:
        sys.exit(cli())
    threading.Thread(target=scheduler, daemon=True).start()
    threading.Thread(target=watcher, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
