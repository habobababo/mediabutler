#!/usr/bin/env python3
"""
MediaButler – sortiert Filme, Serien, Anime und Dokus und kopiert/verschiebt sie auf ein NFS-Share.
Web-UI mit Fortschrittsbalken, Verlauf, Aufräumen und eingebautem Cron-Zeitplan.

Umgebungsvariablen:
  SOURCE_DIR  Quelle (Standard /incoming)       TARGET_DIR  Ziel = NFS-Mount (Standard /media)
  DATA_DIR    Einstellungen + Verlauf (/data)   MOVE_AFTER  Startwert "Quelle löschen" (0/1)
  MIN_AGE     Dateien/Ordner jünger als X Sekunden werden im Auto-Modus ignoriert (120)
  PORT        Webport (8080)

CLI (z.B. für System-Cron):  python app.py --auto [--dry-run]
"""
import base64, difflib, fcntl, hashlib, hmac, json, os, re, shutil, subprocess, sys, threading, time, urllib.error, urllib.request
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
API_KEY = os.environ.get("API_KEY", "")      # optional: erlaubt Skripten/Apps (z. B. pyLoad n Click) nur POST /api/hook, ohne Login
MIN_AGE = int(os.environ.get("MIN_AGE", "120"))
LOCKFILE = "/tmp/mediabutler.lock"
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
    if not AUTH_PASS or request.path in PUBLIC_PATHS: return
    if API_KEY and request.path == "/api/hook" and hmac.compare_digest(request.headers.get("X-API-Key", "").encode(), API_KEY.encode()): return
    a = request.authorization
    if (a and hmac.compare_digest((a.username or "").encode(), AUTH_USER.encode())
            and hmac.compare_digest((a.password or "").encode(), AUTH_PASS.encode())):
        return
    return Response("Login erforderlich", 401, {"WWW-Authenticate": 'Basic realm="MediaButler", charset="UTF-8"'})
lock = threading.Lock()
job = {"running": False, "cancel": False, "total": 0, "done": 0, "files_total": 0, "files_done": 0,
       "current": "", "current_size": 0, "current_done": 0, "speed": 0, "errors": [], "finished": False,
       "queue": [], "started": 0, "ended": 0}

# ---------- Einstellungen & Verlauf (JSON in DATA_DIR) ----------
DEFAULT_CFG = {"enabled": False, "cron": "0 3 * * *", "move_after": os.environ.get("MOVE_AFTER", "0") == "1",
               "cleanup_after": False, "last_run": "", "last_result": "",
               "tg_enabled": False, "tg_token": "", "tg_chat": "", "tg_start": True, "tg_done": True,
               "tg_unclear": True, "tg_unclear_sig": "", "tg_new": True, "tg_progress": True,
               "tg_errors": True, "tg_silent": True, "jellyfin": True,
               "jf_scan": bool(os.environ.get("JELLYFIN_URL") and os.environ.get("JELLYFIN_API_KEY")),
               "jf_url": os.environ.get("JELLYFIN_URL", "").rstrip("/"), "jf_key": os.environ.get("JELLYFIN_API_KEY", ""), "jf_last": "", "jf_mode": "partial", "extract_auto": True, "extract_delete": True, "tg_pw": True, "jf_root": os.environ.get("JELLYFIN_ROOT", "").rstrip("/"), "public_url": os.environ.get("PUBLIC_URL", "").rstrip("/")}
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

SAMPLE = re.compile(r"sample", re.I)    # jede Datei mit „sample“ im Namen (film.sample.mkv, Movie-Sample.mkv, sample.mkv)
def purge_samples(now_=False):
    """Löscht Sample-Dateien im Quellordner (nie im Ziel) und protokolliert sie. now_=True: sofort, ohne Wartezeit."""
    now = time.time()
    for p in SOURCE.rglob("*"):
        try:
            if SAMPLE.search(p.name) and p.suffix.lower() not in PARTIAL_EXT and p.is_file() and not hidden_rel(p.relative_to(SOURCE).as_posix()) \
                    and (now_ or now - p.stat().st_mtime >= MIN_AGE):
                p.unlink(); print("Sample gelöscht:", p, file=sys.stderr)
        except OSError as e:
            print("Sample nicht gelöscht:", p, e, file=sys.stderr)

def scan():
    """Fertige Dateien (älter als MIN_AGE): (automatisch zuordenbar, unklar)."""
    try: purge_samples(True)
    except Exception as e: print("Sample-Fehler:", e, file=sys.stderr)
    now, items, unclear, cm, bl = time.time(), [], [], load("copied.json", {}), extract_blocked()
    for p in sorted(SOURCE.rglob("*")):
        if not (p.is_file() and p.suffix.lower() in VIDEO_EXT) or p.name.startswith(".") or SAMPLE.search(p.name): continue
        rel = p.relative_to(SOURCE).as_posix()
        if hidden_rel(rel) or ex_blocked(rel, bl): continue
        if now - p.stat().st_mtime < MIN_AGE: continue
        if is_copied(cm, p, rel): continue
        cat = guess_category(rel)
        if cat: items.append({"src": p, "dst": target_for(rel, cat)})
        else: unclear.append(rel)
    return items, unclear


# ---------- Entpacken (RAR/ZIP/7z, auch mehrteilig) ----------
EX_INTERVAL = int(os.environ.get("EXTRACT_INTERVAL", "20"))
EX_STABLE = max(10, int(os.environ.get("EXTRACT_STABLE", str(min(MIN_AGE, 60)))))
exlock = threading.RLock()
_ex, _exn = {}, {"done": 0}          # id -> Eintrag, Zähler fertiger Archive
_R_PART = re.compile(r"^(?P<b>.+)\.part0*(?P<n>\d+)\.rar$", re.I)
_R_OLD = re.compile(r"^(?P<b>.+)\.r(?P<n>\d{2,3})$", re.I)
_Z_OLD = re.compile(r"^(?P<b>.+)\.z(?P<n>\d{2,3})$", re.I)
_SPLIT = re.compile(r"^(?P<b>.+\.(?:zip|rar|7z))\.(?P<n>\d{3})$", re.I)
_INCOMPLETE = re.compile(r"unexpected end of archive|cannot find volume|missing volume|next volume|there are some data after the end", re.I)

def sevenz():
    return next((b for b in (shutil.which(x) for x in ("7zz", "7z", "7za", "7zr")) if b), None)

def hidden_rel(rel):
    return any(x.startswith(".") for x in rel.split("/")[:-1])

def ex_blocked(rel, bl):
    return any(rel == d or rel.startswith(d + "/") for d in bl)

def extract_blocked():
    """Ordner (relativ zur Quelle), in denen noch entpackt wird oder Archive offen sind. Quell-Wurzel zählt nicht."""
    with exlock: return sorted({e["dir"] for e in _ex.values() if e.get("dir") and e["state"] != "fehler"})

def ex_summary():
    with exlock:
        st = [e["state"] for e in _ex.values()]
    return {"n": sum(x in ("läuft", "wartet", "unvollständig") for x in st), "pw": st.count("passwort"),
            "err": st.count("fehler"), "running": "läuft" in st, "done": _exn["done"], "total": len(st)}

def find_archives():
    groups = {}
    def grp(p, kind, base):
        return groups.setdefault((str(p.parent), kind, base.lower()), {"base": base, "dir": p.parent, "main": None, "vols": {}})
    partial_dirs = set()
    for p in SOURCE.rglob("*"):
        try:
            rel = p.relative_to(SOURCE)
            if not p.is_file() or any(x.startswith(".") for x in rel.parts): continue
        except OSError: continue
        n, low = p.name, p.name.lower()
        if p.suffix.lower() in PARTIAL_EXT: partial_dirs.add(str(p.parent)); continue
        if (m := _R_PART.match(n)): grp(p, "rarpart", m["b"])["vols"][int(m["n"])] = p
        elif (m := _SPLIT.match(n)): grp(p, "split", m["b"])["vols"][int(m["n"])] = p
        elif low.endswith(".rar"): grp(p, "rar", n[:-4])["main"] = p
        elif (m := _R_OLD.match(n)): grp(p, "rar", m["b"])["vols"][int(m["n"]) + 1] = p
        elif low.endswith(".zip"): grp(p, "zip", n[:-4])["main"] = p
        elif (m := _Z_OLD.match(n)): grp(p, "zip", m["b"])["vols"][int(m["n"])] = p
        elif low.endswith(".7z"): grp(p, "7z", n[:-3])["main"] = p
    out = []
    for (d, kind, _), g in groups.items():
        vols, main, missing = g["vols"], g["main"], ""
        if kind in ("rarpart", "split"):
            if not vols: continue
            main = vols.get(1)
            lost = [i for i in range(1, max(vols) + 1) if i not in vols]
            if lost: missing = f"Teil {lost[0]} fehlt" + (f" (und {len(lost) - 1} weitere)" if len(lost) > 1 else "")
        elif main is None: continue
        else:
            lost = [i for i in range(1, max(vols) + 1) if i not in vols] if vols else []
            if lost: missing = f"Teil {lost[0]} fehlt" + (f" (und {len(lost) - 1} weitere)" if len(lost) > 1 else "")
        files = ([main] if main else []) + [v for _, v in sorted(vols.items())]
        files = list(dict.fromkeys(files))
        try: stats = [f.stat() for f in files]
        except OSError: continue
        rel_main = (main or files[0]).relative_to(SOURCE).as_posix()
        out.append({"id": hashlib.md5(rel_main.encode()).hexdigest()[:10], "name": g["base"],
                    "dir": g["dir"].relative_to(SOURCE).as_posix() if g["dir"] != SOURCE else "",
                    "main": rel_main if main else "", "parts": [f.relative_to(SOURCE).as_posix() for f in files],
                    "size": sum(s.st_size for s in stats), "newest": max(s.st_mtime for s in stats),
                    "missing": missing, "partial": d in partial_dirs})
    return out

def ex_pws():
    return load("passwords.json", {"list": [], "last": ""})

def add_pw(pw, ok=False):
    with exlock:
        d = ex_pws(); pw = pw.strip("\r\n")
        if not pw: return
        if pw not in d["list"]: d["list"].append(pw)
        if ok: d["last"] = pw
        save("passwords.json", d)

def ex_candidates(e):
    d = ex_pws()
    order = [e.get("pw_hint"), d.get("last")] + list(d["list"])
    return [x for i, x in enumerate(order) if x and x not in order[:i]]

def extract_scan():
    if not sevenz(): return
    found = {a["id"]: a for a in find_archives()}
    now = time.time()
    with exlock:
        done = load("extract.json", {}).get("done_sigs", {})
        for i, a in found.items():
            sig = f"{i}:{a['size']}"
            e = _ex.get(i)
            if e is None:
                if sig in done: continue
                e = _ex[i] = {"id": i, "state": "unvollständig", "pct": 0, "msg": "", "found": now, "sig": ""}
            changed = e["sig"] != sig
            e.update(name=a["name"], dir=a["dir"], main=a["main"], parts=a["parts"], size=a["size"], sig=sig)
            if e["state"] == "läuft": continue
            if a["missing"] or not a["main"]:
                e.update(state="unvollständig", msg=a["missing"] or "Erster Teil fehlt"); continue
            if now - a["newest"] < EX_STABLE or a["partial"]:
                e.update(state="unvollständig", msg="Wird noch geladen …"); continue
            if (e["state"] == "unvollständig" and now >= e.get("retry_after", 0)) or (changed and e["state"] in ("fehler", "passwort")):
                e.update(state="wartet", msg="")
        for i in list(_ex):
            if i not in found and _ex[i]["state"] != "läuft": del _ex[i]

def ex_pick():
    c = cfg()
    with exlock:
        for e in _ex.values():
            if e["state"] == "wartet" and (c["extract_auto"] or e.get("force")) and time.time() >= e.get("retry_after", 0):
                return e

def ex_7z(args, e=None):
    pr = subprocess.Popen([sevenz()] + args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    buf = b""
    try:
        while True:
            chunk = os.read(pr.stdout.fileno(), 4096)
            if not chunk: break
            buf = (buf + chunk)[-8000:]
            if e is not None:
                m = re.findall(rb"(\d{1,3})%", chunk)
                if m: e["pct"] = min(100, int(m[-1]))
                if e.get("cancel"): pr.terminate()
        pr.wait()
    finally:
        if pr.poll() is None: pr.kill()
    return pr.returncode, buf.decode("utf-8", "replace")

def ex_probe(main):
    rc, out = ex_7z(["l", "-slt", "-y", str(main)])
    low = out.lower()
    return {"enc": bool(re.search(r"^encrypted = \+", out, re.M | re.I)) or "wrong password" in low or "encrypted archive" in low or "enter password" in low,
            "incomplete": bool(_INCOMPLETE.search(out)), "rc": rc, "out": out}

def ex_move_into(src, dst):
    for it in sorted(src.iterdir()):
        t = dst / it.name
        if it.is_dir() and not it.is_symlink() and t.is_dir():
            ex_move_into(it, t); continue
        if t.exists():
            k = 2
            while (dst / f"{it.stem} ({k}){it.suffix}").exists(): k += 1
            t = dst / f"{it.stem} ({k}){it.suffix}"
        os.replace(it, t)

def ex_notify_pw(e):
    with exlock:
        st = load("extract.json", {}); sig = e["sig"]
        if sig in st.get("notified", {}): return
        asked = any(o is not e and o["state"] == "passwort" and o["sig"] in st.get("notified", {}) for o in _ex.values())
        st.setdefault("notified", {})[sig] = int(time.time())
        if asked: save("extract.json", st); return          # ein Passwort gilt meist für alle Teile: nur einmal fragen
        st["notified"] = dict(list(st["notified"].items())[-200:]); save("extract.json", st)
    c = cfg()
    if not (c["tg_enabled"] and c["tg_pw"] and c["tg_token"] and c["tg_chat"]): return
    txt = (f"🔑 Passwort fehlt – MediaButler\n📦 {e['name']}\n{e['msg']}\n\n"
           "👉 Antworte auf diese Nachricht mit dem Passwort oder schreibe /pw <Passwort>. "
           "Es wird gespeichert und deine Nachricht gelöscht." + (f"\n🌐 {ui_link('ext')}" if ui_link("ext") else ""))
    try:
        r = tg_api("sendMessage", {"chat_id": c["tg_chat"], "text": txt, "reply_markup": {"force_reply": True, "input_field_placeholder": "Passwort"}})
        with exlock:
            st = load("extract.json", {}); st.setdefault("pwmsgs", {})[str(r["message_id"])] = e["id"]
            st["pwmsgs"] = dict(list(st["pwmsgs"].items())[-50:]); save("extract.json", st)
    except Exception as x: print("Telegram-Fehler:", x, file=sys.stderr)

def ex_set_password(pw, only=None):
    """Passwort speichern und alle Archive, die darauf warten, erneut versuchen. -> Anzahl"""
    add_pw(pw)
    n = 0
    with exlock:
        st = load("extract.json", {})
        for e in _ex.values():
            if e["state"] == "passwort" and (only is None or e["id"] == only):
                e.update(state="wartet", force=True, pw_hint=pw, msg="", retry_after=0); n += 1
                st.get("notified", {}).pop(e["sig"], None)
        save("extract.json", st)
    return n

def extract_run(e):
    main, base = SOURCE / e["main"], SOURCE / e["dir"]
    tmp = base / f".entpacken-{e['id']}"
    c = cfg()
    with exlock: e.update(state="läuft", pct=0, msg="Prüfe Archiv …", started=time.time(), cancel=False, force=False)

    def fail(state, msg, retry=0):
        shutil.rmtree(tmp, ignore_errors=True)
        with exlock: e.update(state=state, msg=msg, pct=0, retry_after=time.time() + retry if retry else 0)
        if state == "passwort": ex_notify_pw(e)
        elif state == "fehler":
            notify(f"⚠️ MediaButler: Entpacken fehlgeschlagen\n📦 {e['name']}\n{msg}" + (f"\n🌐 {ui_link('ext')}" if ui_link("ext") else ""), "tg_errors")

    def attempt(pws):
        out, rc = "", 1
        for i, pw in enumerate(pws):
            shutil.rmtree(tmp, ignore_errors=True); tmp.mkdir(parents=True, exist_ok=True)
            with exlock:
                e["pct"] = 0
                e["msg"] = "Entpacke …" if pw is None and len(pws) == 1 else f"Entpacke mit Passwort {i + 1} von {len(pws)} …"
            rc, out = ex_7z(["x", "-y", "-bsp1", "-bso0", "-aoa", "-o" + str(tmp)] + ([f"-p{pw}"] if pw is not None else []) + [str(main)], e)
            if e.get("cancel") or rc in (0, 1): return rc, out, pw, False
            if not re.search(r"wrong password|encrypted", out, re.I): return rc, out, pw, False
        return rc, out, None, True          # alle Passwörter durch

    try:
        if not main.is_file(): return fail("unvollständig", "Archiv-Datei nicht gefunden", 60)
        pr = ex_probe(main)
        if pr["incomplete"]: return fail("unvollständig", "Es fehlen noch Teile des Archivs", 60)
        if pr["enc"]:
            pws = ex_candidates(e)
            if not pws: return fail("passwort", "Archiv ist verschlüsselt, es ist noch kein Passwort gespeichert.")
        else: pws = [None]
        rc, out, used, pwfail = attempt(pws)
        if pwfail and not pr["enc"]:
            pws = ex_candidates(e)
            if pws: rc, out, used, pwfail = attempt(pws)
            else: return fail("passwort", "Archiv ist verschlüsselt, es ist noch kein Passwort gespeichert.")
        if e.get("cancel"): return fail("fehler", "Abgebrochen")
        if pwfail: return fail("passwort", f"Keines der {len(pws)} gespeicherten Passwörter passt.")
        if rc not in (0, 1):
            if _INCOMPLETE.search(out): return fail("unvollständig", "Es fehlen noch Teile des Archivs", 60)
            lines = [x.strip() for x in out.replace("\r", "\n").split("\n") if x.strip() and "%" not in x]
            return fail("fehler", " ".join(lines[-3:])[:300] or f"7-Zip-Fehlercode {rc}")
        outs = sorted((f for f in tmp.rglob("*") if f.is_file()), key=lambda f: (f.suffix.lower() not in VIDEO_EXT, -f.stat().st_size))
        n = len(outs)
        if not n: return fail("fehler", "Das Archiv enthielt keine Dateien.")
        out_names = [f.name for f in outs[:6]]
        with exlock: e["msg"] = "Verschiebe Dateien …"; e["pct"] = 100
        ex_move_into(tmp, base); shutil.rmtree(tmp, ignore_errors=True)
        try: purge_samples(True)
        except Exception as x: print("Sample-Fehler:", x, file=sys.stderr)
        if used is not None: add_pw(used, ok=True)
        delete = bool(c["extract_delete"]) and rc == 0
        if delete:
            for rel in e["parts"]:
                try: (SOURCE / rel).unlink()
                except OSError: pass
        with exlock:
            st = load("extract.json", {})
            st.setdefault("done_sigs", {})[e["sig"]] = int(time.time())
            st["done_sigs"] = dict(list(st["done_sigs"].items())[-500:])
            st["done"] = ([{"name": e["name"], "dir": e["dir"], "files": n, "out": out_names, "size": e["size"], "ended": int(time.time()),
                            "deleted": delete, "warn": rc == 1, "pw": used is not None}] + st.get("done", []))[:50]
            save("extract.json", st)
            _ex.pop(e["id"], None); _exn["done"] += 1
    except Exception as x:
        fail("fehler", f"{type(x).__name__}: {x}"[:300])

def ex_cleanup_tmp():
    for d in SOURCE.rglob(".entpacken-*"):
        if d.is_dir() and not any(e["state"] == "läuft" for e in _ex.values()): shutil.rmtree(d, ignore_errors=True)

def extract_loop():
    time.sleep(2)
    try: ex_cleanup_tmp()
    except Exception as x: print("Entpacken:", x, file=sys.stderr)
    last = 0
    while True:
        try:
            if time.time() - last >= EX_INTERVAL: extract_scan(); last = time.time()
            e = ex_pick()
            if e: extract_run(e); continue
        except Exception as x: print("Entpacken:", x, file=sys.stderr)
        time.sleep(2)

def tg_handle(m):
    c = cfg(); chat = m.get("chat", {})
    if str(chat.get("id")) != str(c["tg_chat"]).strip() and ("@" + str(chat.get("username", ""))) != str(c["tg_chat"]).strip(): return
    text = (m.get("text") or "").strip()
    rid = (m.get("reply_to_message") or {}).get("message_id")
    pw = None
    if rid is not None and str(rid) in load("extract.json", {}).get("pwmsgs", {}): pw = text
    elif re.match(r"^/pw(@\w+)?(\s|$)", text, re.I): pw = re.sub(r"^/pw(@\w+)?\s*", "", text, flags=re.I)
    else: return
    reply = lambda t: tg_api("sendMessage", {"chat_id": c["tg_chat"], "text": t})
    if not pw: return reply("🔑 Bitte so: /pw <Passwort> – oder direkt auf die Passwort-Nachricht antworten.")
    n = ex_set_password(pw)
    try: tg_api("deleteMessage", {"chat_id": c["tg_chat"], "message_id": m["message_id"]})
    except Exception: pass
    reply(f"🔑 Passwort gespeichert – {n} Archiv(e) werden erneut versucht." if n else "🔑 Passwort gespeichert. Gerade wartet kein Archiv darauf.")

def tg_poller():
    """Fragt Telegram nur ab, solange ein Archiv auf ein Passwort wartet."""
    off = None
    while True:
        try:
            c = cfg()
            if not (c["tg_enabled"] and c["tg_token"] and c["tg_chat"]) or not ex_summary()["pw"]:
                time.sleep(5); continue
            payload = {"timeout": 20, "allowed_updates": ["message"]}
            if off is not None: payload["offset"] = off
            for u in tg_api("getUpdates", payload, timeout=30) or []:
                off = u["update_id"] + 1
                if u.get("message"):
                    try: tg_handle(u["message"])
                    except Exception as x: print("Telegram:", x, file=sys.stderr)
            time.sleep(1)
        except Exception as x:
            print("Telegram-Abfrage:", x, file=sys.stderr); time.sleep(15)

def ex_public(e):
    return {"id": e["id"], "name": e.get("name", ""), "dir": e.get("dir", ""), "state": e["state"], "pct": e.get("pct", 0), "msg": e.get("msg", ""),
            "size": e.get("size", 0), "nparts": len(e.get("parts", [])), "auto": bool(cfg()["extract_auto"])}

def ex_payload():
    d, st, c = ex_pws(), load("extract.json", {}), cfg()
    with exlock: items = [ex_public(e) for e in sorted(_ex.values(), key=lambda x: x.get("found", 0))]
    return {"available": bool(sevenz()), "auto": c["extract_auto"], "delete": c["extract_delete"], "items": items,
            "done": st.get("done", [])[:30], "pws": list(d["list"]), "tg": bool(c["tg_enabled"] and c["tg_pw"])}

@app.get("/api/extract")
def ex_get():
    return jsonify(ex_payload())

@app.post("/api/extract/scan")
def ex_scan_api():
    extract_scan(); return jsonify(ex_payload())

@app.post("/api/extract/settings")
def ex_settings():
    d = request.get_json() or {}
    save_cfg(extract_auto=bool(d.get("auto")), extract_delete=bool(d.get("delete")))
    return jsonify(ex_payload())

@app.post("/api/extract/start")
def ex_start():
    i = (request.get_json() or {}).get("id")
    with exlock:
        e = _ex.get(i)
        if e and e["state"] in ("wartet", "fehler", "unvollständig", "passwort"):
            e.update(state="wartet", force=True, retry_after=0, msg="")
    return jsonify(ex_payload())

@app.post("/api/extract/cancel")
def ex_cancel():
    with exlock:
        e = _ex.get((request.get_json() or {}).get("id"))
        if e and e["state"] == "läuft": e["cancel"] = True
    return jsonify(ok=True)

@app.post("/api/extract/dismiss")
def ex_dismiss():
    with exlock:
        e = _ex.get((request.get_json() or {}).get("id"))
        if e and e["state"] != "läuft":
            st = load("extract.json", {}); st.setdefault("done_sigs", {})[e["sig"]] = int(time.time()); save("extract.json", st)
            del _ex[e["id"]]
    return jsonify(ex_payload())

@app.post("/api/extract/password")
def ex_password():
    d = request.get_json() or {}
    pw = str(d.get("pw", ""))
    if not pw.strip(): return jsonify(error="Bitte ein Passwort eingeben."), 400
    n = ex_set_password(pw, d.get("id") or None)
    return jsonify(n=n, **ex_payload())

@app.post("/api/hook")
def hook():
    """Von außen (pyLoad n Click, Skript): {"password": "...", "name": "..."} merkt das Entpack-Passwort und sucht sofort nach Archiven."""
    d = request.get_json(silent=True) or {}
    pw, n = str(d.get("password") or "").strip(), 0
    if pw: n = ex_set_password(pw)
    extract_scan()
    return jsonify(ok=True, retried=n, **ex_summary())

@app.post("/api/extract/password/delete")
def ex_password_delete():
    i = (request.get_json() or {}).get("i")
    with exlock:
        d = ex_pws()
        if isinstance(i, int) and 0 <= i < len(d["list"]):
            if d.get("last") == d["list"][i]: d["last"] = ""
            d["list"].pop(i); save("passwords.json", d)
    return jsonify(ex_payload())

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
    reason = ("wird gerade entpackt" if any(x == d.name or x.startswith(d.name + "/") for x in extract_blocked())
              else "enthält noch Medien" if media else "wird noch geschrieben" if partial
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

TG_API = os.environ.get("TG_API", "https://api.telegram.org").rstrip("/")

def tg_api(method, payload, timeout=10):
    token = cfg()["tg_token"]
    req = urllib.request.Request(f"{TG_API}/bot{token}/{method}", data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r: return json.loads(r.read()).get("result")
    except Exception as e:
        raise RuntimeError(str(e).replace(token, "***")) from None

def tg_send(text, token=None, chat=None, urgent=True):
    c = cfg()
    token, chat = token or c["tg_token"], chat or c["tg_chat"]
    if not token or not chat:
        raise ValueError("Bot-Token und Chat-ID fehlen")
    req = urllib.request.Request(
        f"{TG_API}/bot{token}/sendMessage",
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
        notify(f"🚨 Handlungsbedarf – MediaButler\n❓ {len(unclear)} Datei(en) brauchen eine Zuordnung, der Zeitplan überspringt sie.\n{lines}\n"
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
            notify(f"▶️ MediaButler: Kopieren gestartet ({source})\n{len(items)} Datei(en), {hsize(size)}\n"
                   + " · ".join(f"{k} {v}" for k, v in cats.items()), "tg_start")
        ok = _run_job(items, source, c["move_after"])
        if ok:
            _scan_due[0] = True
            for it, _ in ok:
                pp = Path(it["dst"]).parts
                _scan_paths.add("/".join(pp[:2]) if len(pp) >= 3 else "/".join(pp[:-1]))
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
        txt = (f"{head} – MediaButler ({source})\n{len(ok)} von {len(items)} Datei(en) übertragen, "
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
_scan_due, _scan_paths = [False], set()

def jf_refresh(url, key):
    """Jellyfin: Bibliotheks-Scan anstoßen (POST /Library/Refresh, braucht API-Key). -> (ok, Text)"""
    url = (url or "").strip().rstrip("/")
    if not url or not key: return False, "Jellyfin-Adresse oder API-Key fehlt."
    try:
        req = urllib.request.Request(url + "/Library/Refresh", data=b"", method="POST",
                                     headers={"X-Emby-Token": key, "Authorization": f'MediaBrowser Token="{key}"'})
        with urllib.request.urlopen(req, timeout=20) as r:
            return True, f"Scan gestartet (HTTP {r.status})"
    except urllib.error.HTTPError as e:
        return False, "API-Key ungültig (HTTP 401)" if e.code == 401 else f"HTTP {e.code}"
    except Exception as e:
        return False, f"nicht erreichbar: {e}"

def jf_updated(url, key, paths):
    """Teil-Scan: nur die genannten Ordner melden (POST /Library/Media/Updated). -> (ok, Text)"""
    url = (url or "").strip().rstrip("/")
    body = json.dumps({"Updates": [{"Path": p, "UpdateType": "Created"} for p in paths]}).encode()
    try:
        req = urllib.request.Request(url + "/Library/Media/Updated", data=body, method="POST",
                                     headers={"X-Emby-Token": key, "Content-Type": "application/json",
                                              "Authorization": f'MediaBrowser Token="{key}"'})
        with urllib.request.urlopen(req, timeout=20) as r:
            return True, f"Teil-Scan für {len(paths)} Ordner gestartet (HTTP {r.status})"
    except urllib.error.HTTPError as e:
        return False, "API-Key ungültig (HTTP 401)" if e.code == 401 else f"HTTP {e.code}"
    except Exception as e:
        return False, f"nicht erreichbar: {e}"

def jf_paths(c, rels):
    root = (c["jf_root"] or str(TARGET)).rstrip("/")
    return sorted(f"{root}/{r}" for r in rels if r)

def jf_after_queue():
    c = cfg()
    rels = sorted(_scan_paths)
    _scan_paths.clear()
    if not (c["jf_scan"] and _scan_due[0]): _scan_due[0] = False; return
    _scan_due[0] = False
    if c["jf_mode"] == "partial" and rels:
        ok, msg = jf_updated(c["jf_url"], c["jf_key"], jf_paths(c, rels))
        if not ok:
            ok2, msg2 = jf_refresh(c["jf_url"], c["jf_key"])
            msg = f"Teil-Scan fehlgeschlagen ({msg}); " + ("voller Scan gestartet" if ok2 else msg2); ok = ok2
    else:
        ok, msg = jf_refresh(c["jf_url"], c["jf_key"])
    save_cfg(jf_last=f"{datetime.now():%Y-%m-%d %H:%M:%S}: {msg}")
    if not ok: notify(f"⚠️ MediaButler: Jellyfin-Scan fehlgeschlagen – {msg}", "tg_errors")

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
                idle = not pending
            lib_invalidate()
            if idle:
                try: jf_after_queue()
                except Exception as e: print("Jellyfin-Scan:", e, file=sys.stderr)

def waiting_batches():
    with qcv: return [b for b in pending if b is not qstate["active"]]

def trigger(source):
    if not TARGET.is_dir():
        alert_once("target", True, f"🚨 Handlungsbedarf – MediaButler\nDas Ziel {TARGET} ist nicht erreichbar. Ist das NFS-Share eingebunden? Bis dahin wird nichts kopiert.",
                   "✅ MediaButler: Das Ziel ist wieder erreichbar.")
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
    dups = 0
    for i in items:
        try: m = lib_match(i["dst"].as_posix(), i["src"].stat().st_size, 1)
        except OSError: m = None
        if m and m["n"]: dups += 1
    sch = schedule_info()
    when = f"⏰ Nächster Lauf: {sch['next']}" if sch["enabled"] and sch["next"] else "⏰ Zeitplan ist aus, bitte manuell starten"
    txt = (f"📥 MediaButler: {len(items)} neue Datei(en) bereit ({hsize(size)})\n"
           + ", ".join(f"{k} {v}" for k, v in cats.items()) + "\n" + "\n".join(names) + f"\n{when}")
    if dups: txt += f"\n🔁 {dups} davon gibt es schon (oder ähnlich) in der Bibliothek"
    if ui_link("sort"): txt += f"\n👉 {ui_link('sort')}"
    notify(txt, "tg_new", wait=True)

def watch_once(state):
    """Ein Prüfdurchlauf: Ausfälle, Klärungsbedarf und neue Dateien melden."""
    c = cfg()
    if not c["tg_enabled"]:
        state.update(last_new=set(), stable=0); return
    link = f"\n👉 {ui_link('sched')}" if ui_link("sched") else ""
    alert_once("target", not TARGET.is_dir(),
               f"🚨 Handlungsbedarf – MediaButler\nDas Ziel {TARGET} ist nicht erreichbar. Ist das NFS-Share eingebunden? Bis dahin wird nichts kopiert.{link}",
               "✅ MediaButler: Das Ziel ist wieder erreichbar.")
    src_ok = SOURCE.is_dir()
    alert_once("source", not src_ok, f"🚨 Handlungsbedarf – MediaButler\nDer Quellordner {SOURCE} ist nicht erreichbar.{link}",
               "✅ MediaButler: Der Quellordner ist wieder erreichbar.")
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
    txt = f"⏳ MediaButler: {mark} % geschafft ({source})\n{fd} von {ft} Datei(en), {hsize(done)} von {hsize(total)}"
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

# ---------- Duplikate in der Bibliothek & Abgleich der Neuzugänge ----------
DUP_MIN_SIZE = 50 * 1024 * 1024          # gleiche Dateigröße zählt erst ab dieser Größe als Hinweis
SIDECAR_EXT = {".srt", ".ass", ".sub", ".idx", ".nfo"}
dp = {"running": False, "cancel": False, "total": 0, "done": 0, "current": "", "error": ""}
dp_lock = threading.Lock()
_NUMS = re.compile(r"\d+")
_PART = re.compile(r"(?<![A-Za-z])(?:cd|part|pt|disc|disk|teil)[ ._-]*(\d{1,2})(?![\dA-Za-z])", re.I)
_ARTICLE = re.compile(r"^(?:the|der|die|das|ein|eine|a|an|le|la|les|el)(?=.{4})")

def media_id(rel, skip=1):
    """Relativer Pfad -> (Typ, Schlüssel, Jahr, Extra, Anzeigename).
    Typ "e" = Folge (Extra = (Staffel, Folgen)), "m" = Film (Extra = Teil-Nr. oder None).
    skip = Anzahl führender Ordner, die keine Serie/Film benennen (Kategorie im Ziel)."""
    p = Path(rel); stem = p.stem
    dirs = list(p.parts[skip:-1]) if len(p.parts) - 1 > skip else []
    ep = _episode(stem, "anime" in rel.lower())
    if ep:
        title, year, season, eps = ep
        if not title:
            t, _ = _ctx(dirs); title, year = _title_year(t or "")
        if title:
            return ("e", skey(title), year, (season, tuple(eps)),
                    f"{title}{f' ({year})' if year else ''} S{season:02d}" + "".join(f"E{x:02d}" for x in eps))
    title, year = _title_year(stem)
    if not title or not skey(title):
        t, _ = _ctx(dirs); title, year = _title_year(t or stem)
    pm = _PART.search(stem)
    return ("m", skey(title), year, int(pm.group(1)) if pm else None, f"{title}{f' ({year})' if year else ''}")

def _cmp(k): return _ARTICLE.sub("", k)

def _similar(a, b):
    """Zwei Schlüssel (skey) sind fast gleich: gleiche Zahlen, sehr ähnlicher Text."""
    a, b = _cmp(a), _cmp(b)
    if a == b: return True
    if _NUMS.findall(a) != _NUMS.findall(b) or min(len(a), len(b)) < 5: return False
    sm = difflib.SequenceMatcher(None, a, b)
    return sm.real_quick_ratio() >= .88 and sm.quick_ratio() >= .88 and sm.ratio() >= .88

def _years_ok(y1, y2, exact):
    if not y1 or not y2: return not exact
    return int(y1) == int(y2) if exact else abs(int(y1) - int(y2)) <= 1

def _compat(a, b):
    """Passen zwei Medien-IDs zusammen? -> "exakt" | "ähnlich" | None"""
    if a[0] != b[0] or a[3] != b[3]: return None
    if a[1] == b[1] and a[2] == b[2]: return "exakt"
    if a[1] == b[1] and (not a[2] or not b[2]): return "ähnlich"
    if _years_ok(a[2], b[2], False) and _similar(a[1], b[1]): return "ähnlich"
    return None

def _walk_library():
    for p in _walk_files(TARGET, SOURCE):
        if p.suffix.lower() not in ANALYSIS_EXT or p.name.startswith(".") or SAMPLE.search(p.name): continue
        try: st = p.stat()
        except OSError: continue
        yield p.relative_to(TARGET).as_posix(), st.st_size, int(st.st_mtime)

def _enrich(rel, size, cache):
    c = (cache or {}).get(rel)
    return {"codec": c.get("codec", ""), "w": c.get("w", 0), "h": c.get("h", 0)} if c and c.get("s") == size else {"codec": "", "w": 0, "h": 0}

def _find_groups(ents):
    """ents: [{path,size,id,...}] -> Gruppen mit mindestens zwei Dateien."""
    n = len(ents); par = list(range(n)); why = [set() for _ in range(n)]
    def find(i):
        while par[i] != i: par[i] = par[par[i]]; i = par[i]
        return i
    def union(i, j, reason):
        a, b = find(i), find(j)
        if a != b: par[b] = a; why[a] |= why[b]
        why[a].add(reason)
    ex = {}
    for i, e in enumerate(ents): ex.setdefault(e["id"][:4], []).append(i)
    for ids in ex.values():
        for j in ids[1:]: union(ids[0], j, "Gleicher Titel")
    by_size = {}
    for i, e in enumerate(ents):
        if e["size"] >= DUP_MIN_SIZE: by_size.setdefault((e["size"], e["id"][0], e["id"][3]), []).append(i)   # Teil-Nr. bzw. Folge muss passen
    for ids in by_size.values():
        for j in ids[1:]: union(ids[0], j, "Gleiche Dateigröße")
    reps = sorted({find(i) for i in range(n)})      # ähnliche Titel: je Gruppe ein Vertreter, Filme nach Jahr, Folgen nach Staffel/Folge gebündelt
    ms, by_year, eps_b = [], {}, {}
    for r in reps:
        t, _k, y, x, _n = ents[r]["id"]
        if t == "m": ms.append(r); by_year.setdefault(y, []).append(r)
        else: eps_b.setdefault(x, []).append(r)
    for y, lst in by_year.items():
        cand = ms if not y else lst + by_year.get("", []) + by_year.get(str(int(y) - 1), []) + by_year.get(str(int(y) + 1), [])
        for r in lst:
            for q in cand:
                if q != r and find(q) != find(r) and _compat(ents[r]["id"], ents[q]["id"]): union(r, q, "Ähnlicher Titel")
    for lst in eps_b.values():
        for x in range(len(lst)):
            for y2 in range(x + 1, len(lst)):
                if find(lst[x]) != find(lst[y2]) and _compat(ents[lst[x]]["id"], ents[lst[y2]]["id"]): union(lst[x], lst[y2], "Ähnlicher Titel")
    groups = {}
    for i in range(n): groups.setdefault(find(i), []).append(i)
    return [(idx, why[r]) for r, idx in groups.items() if len(idx) > 1]

def run_dupes():
    with dp_lock: dp.update(running=True, cancel=False, total=0, done=0, current="", error="")
    try:
        if not TARGET.is_dir(): raise RuntimeError(f"{TARGET} nicht erreichbar")
        cache = (load("analysis_target.json", {}) or {}).get("files", {})
        files = []
        for rel, size, mt in _walk_library():
            files.append((rel, size, mt))
            if len(files) % 25 == 0:
                with dp_lock: dp.update(total=len(files), current=Path(rel).parent.as_posix())
            if dp["cancel"]: raise RuntimeError("Abgebrochen")
        with dp_lock: dp.update(total=len(files), done=len(files), current="Vergleiche …")
        ents = []
        for rel, size, mt in files:
            ents.append({"path": rel, "size": size, "m": mt, "id": media_id(rel), **_enrich(rel, size, cache)})
        groups = []
        for idx, why in _find_groups(ents):
            fs = sorted((ents[i] for i in idx), key=lambda e: (-(e["h"] or 0), -e["size"], e["path"]))
            keep = fs[0]["path"]
            exact = bool(why & {"Gleicher Titel", "Gleiche Dateigröße"})
            order = [r for r in ("Gleicher Titel", "Gleiche Dateigröße", "Ähnlicher Titel") if r in why]
            names = Counter(e["id"][4] for e in fs).most_common(2)
            groups.append({"label": names[0][0], "type": "Folge" if fs[0]["id"][0] == "e" else "Film", "level": "exakt" if exact else "ähnlich",
                           "reasons": order, "keep": keep, "waste": sum(e["size"] for e in fs) - fs[0]["size"],
                           "files": [{"path": e["path"], "size": e["size"], "m": e["m"], "codec": e["codec"], "w": e["w"], "h": e["h"]} for e in fs]})
        groups.sort(key=lambda g: (g["level"] != "exakt", -g["waste"]))
        save("dupes.json", {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "files": len(files), "groups": groups})
    except Exception as e:
        with dp_lock: dp["error"] = str(e)
    finally:
        with dp_lock: dp.update(running=False, current="")

# Abgleich: liegt ein Neuzugang (oder etwas Ähnliches) schon in der Bibliothek?
LIB_TTL = int(os.environ.get("LIB_INDEX_TTL", "300"))
lib_idx = {"ts": 0.0, "data": None, "busy": False, "memo": {}}
lib_lock = threading.Lock()

def _build_lib_index():
    try:
        if not TARGET.is_dir(): return
        ents = [{"path": rel, "size": size, "id": media_id(rel)} for rel, size, _m in _walk_library()]
        data = {"all": ents, "size": {}, "m": {}, "e": {}}
        for e in ents:
            if e["size"] >= DUP_MIN_SIZE: data["size"].setdefault(e["size"], []).append(e)
            t, _k, y, x, _n = e["id"]
            if t == "e": data["e"].setdefault(x, []).append(e)
            else: data["m"].setdefault(e["id"][2], []).append(e)
        with lib_lock: lib_idx.update(data=data, ts=time.time(), memo={})
    except Exception as ex: print("Bibliotheks-Index:", ex, file=sys.stderr)
    finally:
        with lib_lock: lib_idx["busy"] = False

def lib_invalidate():
    with lib_lock: lib_idx["ts"] = 0.0

def lib_index():
    """Zwischengespeicherter Bibliotheks-Index (wird im Hintergrund erneuert). None, solange er noch nicht fertig ist."""
    with lib_lock:
        if time.time() - lib_idx["ts"] > LIB_TTL and not lib_idx["busy"]:
            lib_idx["busy"] = True
            threading.Thread(target=_build_lib_index, daemon=True).start()
        return lib_idx["data"]

def lib_match(rel, size, skip=0, limit=3):
    """Treffer für einen Neuzugang: [{path,size,level,why}] (leer = nichts Ähnliches da). None = Index noch nicht bereit."""
    idx = lib_index()
    if idx is None: return None
    mid = media_id(rel, skip)
    mk = (mid[:4], size)
    with lib_lock:
        if mk in lib_idx["memo"]: return lib_idx["memo"][mk]
    found = {}
    def add(e, level, why):
        cur = found.get(e["path"])
        if cur is None or (level == "exakt" and cur["level"] != "exakt"):
            found[e["path"]] = {"path": e["path"], "size": e["size"], "level": level, "why": why}
    if size >= DUP_MIN_SIZE:
        for e in idx["size"].get(size, []):
            if e["id"][0] == mid[0] and e["id"][3] == mid[3]: add(e, "exakt", "Gleiche Dateigröße")
    if mid[0] == "e": pool = idx["e"].get(mid[3], [])
    else:
        y = mid[2]
        pool = idx["all"] if not y else [e for yy in (y, "", str(int(y) - 1), str(int(y) + 1)) for e in idx["m"].get(yy, [])]
    for e in pool:
        if e["id"][0] != mid[0]: continue
        c = _compat(mid, e["id"])
        if c: add(e, c, "Gleicher Titel" if c == "exakt" else "Ähnlicher Titel")
    res = sorted(found.values(), key=lambda m: (m["level"] != "exakt", m["path"]))
    out = {"level": res[0]["level"] if res else "", "n": len(res), "items": res[:limit]}
    with lib_lock: lib_idx["memo"][mk] = out
    return out

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
    tok = c.pop("tg_token"); c.pop("tg_unclear_sig"); jk = c.pop("jf_key")
    return {**c, "tg_token_set": bool(tok), "jf_key_set": bool(jk), "next": nxt, "error": err,
            "tz": datetime.now().astimezone().tzname(), "persist": PERSIST,
            "info": {"source": str(SOURCE), "target": str(TARGET), "data": str(DATA), "min_age": MIN_AGE,
                     "auth": bool(AUTH_PASS), "ffprobe": bool(shutil.which("ffprobe")), "sevenz": bool(sevenz()), "watch": WATCH_INTERVAL}}

@app.get("/api/files")
def files():
    try: purge_samples(True)
    except Exception as e: print("Sample-Fehler:", e, file=sys.stderr)
    out, cm, bl = [], load("copied.json", {}), extract_blocked()
    with lock: busy_src = {q["src"] for q in job["queue"] if q["state"] in ("wartet", "kopiert") and "src" in q} if job["running"] else set()
    with qcv: busy_src |= {it["src"].relative_to(SOURCE).as_posix() for b in pending for it in b["items"]}
    for p in sorted(SOURCE.rglob("*")):
        if p.is_file() and p.suffix.lower() in VIDEO_EXT and not SAMPLE.search(p.name):
            rel = p.relative_to(SOURCE).as_posix(); cat = guess_category(rel)
            if hidden_rel(rel) or ex_blocked(rel, bl): continue
            size, cp = p.stat().st_size, is_copied(cm, p, rel)
            tgt = target_for(rel, cat).as_posix() if cat else ""
            dup = None if cp or rel in busy_src else lib_match(tgt or rel, size, 1 if tgt else 0)
            out.append({"path": rel, "size": size, "category": cat, "target": tgt,
                        "series": series_info(rel), "copied": cp, "busy": rel in busy_src, "dup": dup})
    shows = {show_dir(f["target"]) for f in out if f["series"] and f["target"]}
    return jsonify(library=library_eps(shows), files=out, categories=CATEGORIES, source=str(SOURCE), target=str(TARGET), target_mounted=TARGET.is_dir(),
                   extracting=ex_summary(), dupidx=lib_idx["data"] is not None)

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

@app.post("/api/files/delete")
def files_delete():
    """Löscht Quelldateien (nur Videos im Quellordner). Dateien, die gerade laufen oder warten, bleiben unangetastet."""
    rels = list(dict.fromkeys(((request.get_json() or {}).get("items") or [])[:5000]))
    with lock:
        busy_rel = {q["src"] for q in job["queue"] if q["state"] in ("wartet", "kopiert") and "src" in q} if job["running"] else set()
    with qcv: busy_rel |= {it["src"].relative_to(SOURCE).as_posix() for b in pending for it in b["items"]}
    gone, skipped, errors = [], [], []
    for r in rels:
        try:
            p = safe_source(r)
            if p.suffix.lower() not in VIDEO_EXT: raise ValueError(f"Keine Videodatei: {r}")
            if r in busy_rel: skipped.append(r); continue
            p.unlink(); gone.append(r)
        except (ValueError, OSError) as e:
            errors.append(f"{r}: {e}")
    if gone:
        g = set(gone)
        for name, getter, setter in (("paths", get_paths, set_paths), ("groups", get_mg, set_mg)):
            if g & getter().keys(): setter({k: v for k, v in getter().items() if k not in g})
        cm = load("copied.json", {})
        if g & cm.keys(): save("copied.json", {k: v for k, v in cm.items() if k not in g})
        for r in gone: print("Quelldatei gelöscht:", r, file=sys.stderr)
    return jsonify(ok=True, deleted=len(gone), skipped=len(skipped), errors=errors)

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
    j["extract"] = ex_summary()
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

@app.get("/api/dupes")
def dupes_get():
    with dp_lock: j = dict(dp)
    d = load("dupes.json", {}) or {}
    g = d.get("groups", [])
    return jsonify(**j, ts=d.get("ts", ""), scanned=d.get("files", 0), groups=g, root=str(TARGET), mounted=TARGET.is_dir(),
                   waste=sum(x["waste"] for x in g))

@app.post("/api/dupes/start")
def dupes_start():
    if not TARGET.is_dir(): return jsonify(error="Das Ziel ist nicht erreichbar. Ist das NFS-Share eingebunden?"), 400
    with dp_lock:
        if dp["running"]: return jsonify(error="Es läuft bereits ein Scan."), 409
        dp.update(running=True, cancel=False, total=0, done=0, current="", error="")
    threading.Thread(target=run_dupes, daemon=True).start()
    return jsonify(ok=True)

@app.post("/api/dupes/cancel")
def dupes_cancel():
    dp["cancel"] = True; return jsonify(ok=True)

def _rm_sidecars(p):
    try:
        for s in p.parent.iterdir():
            if s.is_file() and s.suffix.lower() in SIDECAR_EXT and (s.stem == p.stem or s.stem.startswith(p.stem + ".")):
                try: s.unlink()
                except OSError: pass
    except OSError: pass

def _rm_empty_parents(p):
    d = p.parent
    while d != TARGET and len(d.relative_to(TARGET).parts) >= 2:
        try:
            if any(d.iterdir()): break
            d.rmdir()
        except OSError: break
        d = d.parent

@app.post("/api/dupes/delete")
def dupes_delete():
    """Löscht markierte Dateien aus der Bibliothek. Nur Dateien aus dem letzten Duplikat-Scan, und pro Gruppe bleibt immer mindestens eine."""
    rels = list(dict.fromkeys(((request.get_json() or {}).get("items") or [])[:5000]))
    d = load("dupes.json", {}) or {}
    groups = d.get("groups", [])
    sel = {r for r in rels if any(r == f["path"] for g in groups for f in g["files"])}
    kept = 0
    for g in groups:
        paths = [f["path"] for f in g["files"]]
        if paths and all(x in sel for x in paths):
            sel.discard(g["keep"] if g["keep"] in paths else paths[0]); kept += 1
    root = os.path.realpath(TARGET)
    gone, errors, freed = [], [], 0
    for r in rels:
        if r not in sel: continue
        try:
            p = TARGET / r
            if os.path.realpath(p.parent) != root and not os.path.realpath(p.parent).startswith(root + os.sep): raise ValueError("liegt außerhalb der Bibliothek")
            if p.suffix.lower() not in ANALYSIS_EXT: raise ValueError("keine Videodatei")
            if not p.is_file(): raise ValueError("Datei nicht gefunden")
            size = p.stat().st_size
            p.unlink(); freed += size; gone.append(r)
            _rm_sidecars(p); _rm_empty_parents(p)
            print("Aus Bibliothek gelöscht (Duplikat):", r, file=sys.stderr)
        except (ValueError, OSError) as e:
            errors.append(f"{r}: {e}")
    if gone:
        g2 = set(gone); out = []
        for g in groups:
            fs = [f for f in g["files"] if f["path"] not in g2]
            if len(fs) < 2: continue
            if not any(f["path"] == g["keep"] for f in fs): g["keep"] = fs[0]["path"]
            g["files"] = fs; g["waste"] = sum(f["size"] for f in fs) - next(f["size"] for f in fs if f["path"] == g["keep"])
            out.append(g)
        d["groups"] = out; save("dupes.json", d)
        for scope in ("target",):
            a = load(f"analysis_{scope}.json", {}) or {}
            if g2 & set(a.get("files", {})): a["files"] = {k: v for k, v in a["files"].items() if k not in g2}; save(f"analysis_{scope}.json", a)
        lib_invalidate()
    return jsonify(ok=True, deleted=len(gone), freed=freed, kept=kept, errors=errors)

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
    jurl = str(d.get("jf_url", "")).strip().rstrip("/")
    jkey = str(d.get("jf_key", "")).strip() or old["jf_key"]
    if d.get("jf_scan") and not (re.match(r"^https?://\S+$", jurl) and jkey):
        return jsonify(error="Jellyfin: Adresse (http://…) und API-Key eintragen."), 400
    jroot = str(d.get("jf_root", "")).strip().rstrip("/")
    save_cfg(jf_scan=bool(d.get("jf_scan")), jf_url=jurl, jf_key=jkey, jf_root=jroot,
             jf_mode="full" if d.get("jf_mode") == "full" else "partial",
             enabled=bool(d.get("enabled")), cron=" ".join(d["cron"].split()),
             move_after=bool(d.get("move_after")), cleanup_after=bool(d.get("cleanup_after")),
             tg_enabled=bool(d.get("tg_enabled")), tg_token=tok, tg_chat=chat,
             tg_start=bool(d.get("tg_start")), tg_done=bool(d.get("tg_done")), tg_unclear=bool(d.get("tg_unclear")), public_url=url,
             tg_new=bool(d.get("tg_new", old["tg_new"])), tg_progress=bool(d.get("tg_progress", old["tg_progress"])),
             tg_errors=bool(d.get("tg_errors", old["tg_errors"])), tg_silent=bool(d.get("tg_silent", old["tg_silent"])),
             jellyfin=bool(d.get("jellyfin", old["jellyfin"])))
    return jsonify(settings_payload())

@app.post("/api/jellyfin/test")
def jellyfin_test():
    d = request.get_json() or {}
    ok, msg = jf_refresh(str(d.get("url", "")) or cfg()["jf_url"], str(d.get("key", "")).strip() or cfg()["jf_key"])
    return jsonify(ok=ok, msg=msg)

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


# ---------- Icon / Logo (Butler mit Play-Button) ----------
def _b64(s): return base64.b64decode(s)
ICON_64 = _b64("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")
ICON_192 = _b64("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")
ICON_512 = _b64("iVBORw0KGgoAAAANSUhEUgAAAgAAAAIACAMAAADDpiTIAAABgFBMVEX////+///9//3+/v79/fz8/v38/Pz6/fz1/v77+vr5+/n3+vn0+vr19/jz9/ny9/jx9Pfu9fju8vXs7/Tm9Pfn7vLo7PDm6+/k6+7f6u/j5+zf5+vd5erc4una4efY4ufY4ObX3+XU3+XV3ePT3OLU2uDR2+G/0Nlulaw5c5gidKYgbJ03Y4EkYo4fYpEdY5MeXYobZJUaYJAaXYwlWH4cWYUbWYYbVYIaWIUaVYEaU38YWYYYV4MYVoIXVYEZVIAYU34WVIAoTGgbT3gaUHwaTnkZUn8ZUn0ZUH4ZT3oZTnkZS3UYUX0YTnoYTXgYS3YYSXMXUHsXTHcXSnUXS3MXSHIXSHAXRW4VUX0VTnoVS3cVS3QWSXQVSHMVSXEUSHEWR3AVRm8VRW0VRG4URm8URGwRRW4YQWUUQWgTQWgSQGcQQWgQPWMPO2AZKzgNNlgPGiMKERcGCw8DBggCAwYEAQICAQIBAQQBAgEBAQEBAAEBAAAAAQYAAQEAAQAAAAB+ltuBAAEAAElEQVR42tydiUNTV/r+E0JIyAJJ2JFFUCDUIKjgvjXYOha16rQ60306ndLpVLaEhC3wr/+e5z3n3nvulgTEznx/ZzqKbGLez33Pu7+R33l+a3b+/e9//yv8/PyzfuOf5vkHzzvn/IQ//mSdH3788ccfrPO177xyzprz1tqa85Z9/hpy1poc+1sYf88r6wu/s8/36qgf8is5P/qP8Tm+j/3yi/Nv+umnfzjnl19xfvnHL3Ls188+eLXtF16f30RKf/gP3/f+1GfTOJF2AGgqfzcAxr/TAOAnLwA/BALwyn1MIQUL89wBMAhQxwTAL2SR/3chAPz4vflvU/909cIIAL8QhSAAyICHgN9+cwD43QvA6RE4BQD/anZ+bhOAf/x0FgBMIYUJ80s5SnRv5HzL89cv15oi0AQALwNuABwxawXxPf+6AADwEQ8BfgAUAT8HEfDv39wE/CYEaJmbqkC9/UEAyF/0uwcD/gzqNFP7P3vPO/exBf6T++V5q85PP+EXl+xNAr5q96ytqd/DtEFb3yT48cdFYN8H+PXbkPN92PFQ8Ys+FL5CQDSAl4B/m+c/cpR4wu6BsyLw/gMB+LltADyq0ZD/22Dl/z8DwHcfCICYOz4ChIJQAP51BgBODYEHgFNpfg1AS/nbAPxkvwJKWb51ToDk2wNgzZK75zoIONZn+r5FUwACeAgDwIPK9woX+/6wj42BhYK6EAIvATcGBgC/nw8A788fgEDRtwDA/eivnQ6ANcrty/aOpQ6ct2gpegHwnu/cRHx72qOI0L8qs8B1IZwOgN9NU/C/DkDz598t/7fBAPh8vvYAcMn1y7aPBcCXfzWoaQGAycBfzwgAvtKtHAwGtFn4s/16tgLAZQo6MJzRF/gQAH5uH4CvDfXnsvz9Tp/3EQ96z5fnekIF/9cvTQDU26eWv/py59sY5oHFwNfKGdQnDIDftKXm2Gu/mwSoN84WDzAB0Hbgv1oeS+bqTVPy/zBF/wtvO0hdMAjx9tb+avh5vgs61PMLkZzzoAfpfN8nt8bAJfRvcN68MdzNgPONeZpbitoqVPfBr9o1/FcTBmxV4GVA/f7hV4D2A04DwM/NAfixNQBrgQCot5sYeC1E6lb5/zsAmG9rQ0BpAguAUCXwbwHgt0AC9G9nigVETq3/vTfAu3AAlPyh50JjvVa4xwOAeru1gR923phHvejq7bCLwC9S58/2xwz5uz6qvon517SgxKUEbAcRAPzsUQEuBn7T5sBvQRT8fmo7INwIbGX/N3P9/uE3/s1on9/bc6l3t4m/dnYAXuKICKzfrdMEAI+89J/NLzbk/8b4tPDzTcixEfBGCMQeCI8K2fZAKACnQcAFgBkIav7oq2s/zPU/FwDU22ttABAm25f+4xePDwCfxtC/WV/8TYhaaUHAmwAALD3gIUBbhE1dQkNgHwbA+1MD8LMXgHfvWgPgiva7ZO9L2fzVb/O3AMCUsPz5M/X+L76w3/3Fy7BjAmAC4gWgCUNKy7yxlE3A56hrw7483LaBOzLgABByEfzWFIDfPwiAdvT/z5LtkayPlr7GQGV+dHzXcP+Vz2cZgBLsNzGQq99UAV99GWKorfmfer88v2h+mtDwxrAFA28I/dGQn8JAAX/4JuQSUG+biNmeoSdIrOyBX32v/29Wmpjm4O/+sOBZbgE3AC1sv386APzTB4AWs5K8K85vAtAi5OuO2XnPZ3Jeti36Z+q3z5yv++wz8/uZbFgAhJgI/h8sFACXVjCeffNtM0wUCsCv4QD8OwSA0yuBswDwTwcArfbNVK/I353o+fqUAITp+w8G4LMWAISHi9bcnmUTPRR6Gbz064Uvv/kGISLTGnQD4I8QtwfAHx8FgJ9/Nit+mgDw1g+AKP8QAPw+edhVb+ryVgrfAMD5si+++CKQG7fAHEk67/Vf/yEGxgv5hSfwrrHZsBDAv1dZA/6cMb3CQADMELEHgDPYAZG2/T8TgH+I/H+yAXCL3wvAqyYArAXr2yYAtH28ADQzD+SXoEfaEbctdBeGbvG7jvzZfq/3m6rr4MsvvzEjAyYDIIA5Ah8A//5TATBDvuEAmJF/173vqH4TAFc29k8D4IuWALz0S9gJJ3g+bIjZJf/Xf//731+/5q+e88JL1Tf6NvAGBw0AfNHhQADMeKBODJ0egPYCv07Jpyvno1SApQj8oucJTek7FlaQ0O2325b6s9a+wMvg97rf/9L3jLuEbkj+hRbx69cuyb/2HhcPjjlgZw4DAkO/+KLDxi0QGAY4dZHYWQCwyv200v9JSd4n/69bZHc1ACEG+OmcvA8D4ItmAFh/MJ76FyYAL14EiDwIABcDjtFhRoh/DCwe+sVvBzYD4HRpgdYA/NN3NAA/uQH4ySv/r81yCl/9lVvu6vfQmI0pYApA/frixWfGeWYffpb5pzauiBaUqY+Z97v5NGvpPuf5W9gJ0AOCzjfuAgKpJfXYgkEZIqNU7CMD4BW9XfJvm33B0m8KgJGG8z/yAQLQMheBvjCEGwzAKY5mxflr+PcYSFgCN6T/+jX/b4uSb0C+z/+mZP9YHf2GSQA/js/mm46d8OKFO/74zbe4Cb71uYVBYWEPABYEpzUFI60uAIrd1e4RDoBX95vFFME1PVY198s34qaHAfDixYtg+fkBePKkDbE/kWN+gS3eZxYA+lNfGB8xVbn5m5L830wAnGMwIR92KwTrJlEI2HFir0H4rxAC/h1cG3CaIrFIa/lbALhL/n/yABBw9/sB0KF9O8Py7bdfOkZ2EwBM0ZliXjXOE/tYEm51zO9oSdvLikmGeZmbN7uSe7D83SyYeoFXBggIBOBbNwD/+DmIAC8AygM4AwASZGymADQAOuajztufbMPPLvjgL1ZBrKu81gXAVwKAL/DyMvAm9mp3Ja4nvuec73nStthdBATriBYAvKbwnLefQ6y0AD43xP3IeBuf8NzSBn977hzqALEI/06nUFWciC3wreEShOsAGoL/dhWGuDuG2sEgYniWzYI/ZqcX3v7hR1c9i6tTKvDed+V8RPCuIIoDgpW2cQFgyipIpN73/CXsqBddXnz9qYb6eGb8ah1b+jiG5J6bCt9+5+ehx/2lJgBiEdAYdKWKkURzykXEG7RFH+oJeINC7VkDZwCACkCzGdApFwKAN43vCb68MUxAR/wexR8GgP8YIv/cC4B92tUXLQEwTygBJgVu69ByCkwC/koAHFPwV0cF/NsVEfrTAPhHIADuNrnvQwH4a5D8vaF55yLwiF/J/FQAqBed4v9cAaB++/wvT9sBwADmifl3nhGAR4+8uuCx20WwXcM3chO8+cZbLxJUImCJrR0A/mgKgHOhhANA8f9kRv/MSn+X7E0A/hqS2n1jK3nz9nfH6z8zxd7m7f7krj5P7Jcbf3AJ4alx2gHgL+b7TQACrbxHOJ/6zsOH/PWRHPVTmGahdglcESKjdjDcHfyX2IEBMcEgrdAEgH+HA+AO/QQBIBUfrmffB4D29S2Dz7L7XGUa1lP/0gn3eFy1EDE40rx7l/+3jiH/u+7f7j4NPC75W+qDBx+z/54wAJRs9W8Pg8+nDx48UG8IDB4CnHtAhYlVSMAhIDQixGCABYD72f9YAKig3z/MPo+WABhq37rwXSG4UwFgSuG5+aQHHUrEYUK9bX9NCwCsu4Na23xwTQAe+c7DsPPAIOCRTYADgKEFVFhI146KqWV3k/pVwPkAEFoA9g/v8dwAhMB39zsNMc4d8MbIpobH/Nx+WJDUn7quV/N59woe5w6PIxzc0KaSeBpytNFmoPK5R3N7jlvfK1k/0AJ33jbea36BsiVcgWLlFVnlw2Y36T8DPEF9DbitAfVbO4ZgKAD/DATAI33TBvAAYDVVvLFqZ1+2AsDt12tnzVD26lef+K1zxyNuLX/zPe5H1nyI/aabS1mod4QJ3RG9S/wB5776zf2FKlD8nJbA3yw1YNeMWM6gt1BQ62xbCXwAAP/6d2D2LxCAn9oBgK1w8tyb9XAvffGel668jCF69eibsjcv+zD5awBCj4sMn+b2fC/eF4bdoN9oBwCvxO1jve0HQBCQq8UwBy0AvhMV4LSOBBSM/ydAB7RfLRwJTv/6o/8h+h/H01PvUvum+L8wXXzfo++NwbhMPDHznoYp/GYA3Lr1wLiIXUCYYLjujzuGTXdLzufmxfLINPad763k67zlP7dv3zb/6BiIn37qmASWJaB1wHffuTwB1on9HOILhEHQ3Aw4NQCuNt8AAAzXzyqnU6Udur7D8+x/Zpp8bmPPLX7v/S4q3lT15jFtL+sNDwDu4yEp6BpxKxGv+C2xP2gm/tsuBEwfAYahhYBlCCgz4K/fuXxBAcBFwH/szsH/OOcjAfDTGQBwaukCb/7PPlMKwB/UNV28p+YTb8qmJQBUuPp9D5pdEF5V0gqAT0MBcMv83v179yh29aubAA8AjzQAz5+bAPhUwC8/u+vFbQB++08IAacE4J+uTh8Tgp8Myf/ii/t+5+/nUOZfWHnFF898kV7cgB6L3JS5eZnfudX0mNew92PO7eC6KQI+wWs6BJh8novefs7l3JSzsrIiv9/GbytEQR9tDzy4/9BjDNiWoKobM/MCIU0DdteQVSjyH39ssG0b4J//9ABgKwDz0f/x+zYAePMmDAC3zR9473u9Ot/d7j2OEG4Z9pbv3LFVAt8KMA7MC8T6gANAkK0fAsBN64gGUB/VMNx0KQPHL7RKBl67Y0I/+OtFw3oHzx+AfwQC8H0YAL72qZchpXtOqMdr+AXGc+6cAgD3aeaWPWgJgCua96k7rmOJ/3bQscWvn3b8ar1nRf7TXNy27gILAVEBf3MR8IMnKuzvGtHjAwIB+P20AJiNn//0Wv8BAITOaPB4f2bZ5rPA9H0oAE0lfxoAwngwv/cdFxpNAnvW02/K/Z4haJ4bcpbU70tLQgRvhRVh4B7xuK1/KAUBE0zPXxvu4BupFXRXCfES+DkoKvifIAB+bw2AMaLIDYBt/VljfkwAvmsBQKDlF5zesYPtgQCYgndxoN64bwCg9KqpYV3PpYmECUcoAKaiaEPt3zaFf3NlSUl+Cb8vmQfSv3GTXIiGsH8o0QIq3vw3JyIUmhtyXQSeh983QeiPDwTAPefrB/X0fzAAnqxOEwCCn3S3INoFIEA7fAgAt5sBINK+tewcefqXlkQj3DQAkB/mU5sA2xt8GQqA2xZsCsDvp/EC/qlkrxAwZ32arp9pAfzVHqtoT+z1+XufOYa/O8If4Oo/9cZ13Ea8XwZKtEoAK3K0MPw3s/+GuNXSWrCFb8rblLz7hldHRH/v3tLSyvJyqbRcLq/LKZeXS4AAH7tx78a9+0tyKdwTJ1H/CLwHHpsMqIIpV+eA0TrkcQhdZYJ/eCbKtQXAP20AfLNew3x/lxIIA8AurQqK9Vi5XH8gP+DO18rbCqwF2l7227fbPfebHEv27s8Pv/ctRX/9+g3Iv1Qqb2xsbe9UcHa2tzY2y6USuFC3AhxDRYBWXcoSQKRZVRm/ZuH4y5fOPeAiQHePOQT8RwUFAzRA+C3gvQICAfAGf776KjD20woAz+3/3Ofx+aQfZPI98AlPPfE3g4/LGzs9As77w7/S+/hbANxYWlgoLW9ubR/h1Hdr9Xq9Uj0CBJtUCstLSwvUEjdtHXDbJuBTiwECQAQMAn5wt42YSuA/5nYBb4K4PQCk9VsNMLWt/x9+9ADwlRsAlwEQCoDX+Aty+E3ph1r7fvErpX+z5fEoiDZYsP7c9LNvusR/74Y2/HGWKf6dyhEk39jF2dvd26tXKzs72xtQA8sLS7ctS4AIqb/ZsgXFIRBT0E3At2KN/2KNFvTfAtZw+TbHiEV8CUANgNP0/cOPob7/dz7rLyTm+5nP8reFj9uOv5i3frDkzevdEL117Lib/Xabx1Ift099bvqxW7LEj18WShR/tba7d7K3d1Db3T08xH8H9aOjSgUILC/fvuHYgsBIEfDQqRh4rArHrVtAQeDpIXUQ+FXHA8LGiAWnBdsGwCn+bCb/JgAEOX53rZS8ysuHyB9Sx4PvA2Al/Nw8LQE3z0KAeb/cuMGH2Hr25flfKG/tHO1UD/DkH+9VIPb6yeE+CAAJJ9ACW5ul5QcmATe1LejoALw20oTyQs8naAcAFRU+JQA/e0rALOl//dZU/mb9r4mA1eb1raeTXsX8fbr/iVv3m5V13nC++Hna4mIc1RtjP6WgPSeMmACjUkXvw9w9fP0N++DeF+mXSpvb9aNqvVbbPzzcb+DU9vcP9w/wv5NGHR852tkoLasvvEnPUBASJWARYHWcPdfugN1C+L1pC9ol40RB1Qb8RxeJBBYMNwNAPfjS/6/bvfS0X1f83yP/v7ojv2aBv9HW1QIAM/luGvsBZv5tj6q3gyuGEWb/uuQ78qGbS00AcKzH260tixvyjUTx39O/Li1dXbq1XN7YqUDkAefkpNHY36/BGNhYVgTcvKcBcAh4ZOeGpEjAQ8C37tFSDgB0BcQZOCUArtzvP23t//XXgcWf3weG/73Pv7uvzw9AQHo/MLIb8NBpqVGaPvGq35aanYCPnv3+IE4CgLb8b1y9cQO+X5nGXy1Y/ieHcuoVWwfck9CQcibwL33gugXYTSYEvPQR8KMeL+oFwGcKhhPgB8CRfxgAgZf/l4GTVL4IuPvvBrl+vsvflH7IC3+O58z3yNINw+oX33/xxgLkv31UOarWLInjuAE4Pt7b31cEqO/AXwmAvmiYIzQIIAKewLBBgFYCHgBoDATOD3n//o/mGsAw/37wAfCdBwBvu4db/gFhfwWAt5LLjPM7AFjGnvt5U+p96aMdB69mgrd8fbf8r16/dh3y39ip1hsH9RPjGPI/2QUAx4e7u/AI10tQH9cVAI4laNmCTq3Qcys99I2JwPdmA7k2B/9jVoj5o4KiAgw10AyAH90rb/zxf1fmzy//gKJ+HfZ76q3kM+s43LrfpZQNLf+nnlAADPnfuHEd59oizL8NPP0w9w8DAJC38ZHjEwCwe1TZLkMFIGTodQa0LUgEUKNuKAE3AdoW/IcZFQouETQSQwTgD7cb6Fn5SQQ8iz6a2P9//eu3/k7vYA3w1Kf1XZr/Qavsynlr/tMZCv53GfK/DjFeu7Z4fam0vFGp1N2Xvucc0ylATOAAEcKN5YWrAsB14xpggshUArwJrJJhZ9KkGijzi7VpwIkK/eff/n0zYeuFQgH44ewA+Mt9mwLguffDCitOL/6rHxuSGyYAEP/1a1dXYP4dVarw++D/hwBwTDewfgBN0Dg62imXbi5ev25pgRsaAKkUsQPDn1o9REaxoCoX/F56h08LwB8uADxd4P8wJz3/END95b0Cggfpeeq9rJCfu7wjNMobZIufRvQ41q9/jvxx+eP5R+x/HeZ/43gP0d8mADQYFmrAEMDvG6Wl64tXeX3Y38++B+7b98CnKiZgOwOaACqBgMTAv00IfMtlXHmByL+cgd8KAJ3+aSp/n/9vzmYPEL/TuMvnPzTka8b4PkjrXzWP/wPnLH8B4KqS/ybSflXY+HsEwDQB3AwIAOqN7fLCVQ8ATkxIF4qoWiGni/Sl5xr40VMg8G/HFDAKBIIB+NkEwCj/aglAgP/nLfgN6Nw2AAiI+BoB3g+69a/6j7z3YygAJn2vLly9urgo0T+4/0d1EICnvGaagKb4d3dxCdQZDjhpHO28X+DPd92NgBWRvO/0E5oAOOOGv/nue7Nr4Jd//tNJDisCWgNgFf5oADjux7IAwtJAX3odAPdAVr/7F1DgGwBAQDimjTve+1hfbXXMLz87FerhF8Hxmy5eXYD7B/sfBNR29/eO1SNuuQBuAA5pBcIMPDmuyh3gA8BBwEOAHjvrzBz/zjtE4Jdf/+mqFTbjQf7EoGUDuLx/R/jOxD9ni55b+1vzfVyTXVSzj9VebbV12FK/4w742AnXlWbelvvVv24K/WrbZ3FxMRCHU2NwQ3l9Ins8+wsLlP8mzf+aPNy86Y/3gwJBPLsHsBL3Dw739/f2jraWFxavLdKEuGoyoPwB8QYesuNIXwJWF7EEBa2woB7X460QMUrFPV1jjjGobACz+Ff3f/xoLneX5T7GFkXXw68Hvrw0uvsVAE4Hv8jfKOvyARAW8Wuu5hfakv9iG/rgDABcd559AHB18fpCebtSOTDNvlAAjNOo7pQXrslxAyAIOAWDVnpIDSizhw07dUK/hAHwn8D+YRsAxwRwA+Ds+FRr3QHAd4EKwO352ZP2pOTLbON0l/Xdd1dVee7+9oSwoM9V+UWebvcDvrjoeeKtLwgwD6xDmYYgcd177L8G/792tbS+U6HlXzs5DQB71aP1hUUNgBBgMEAlcNvsHnnsZAf14oFv/6pHjev9k95aYUsLuD0CFwBm8W8AAK8sAALs/xDXT1sApvxdXj8AuH07uLKjtfRtsQU/zwtazCEftT4WdBeY5/pV2zLDCQLA/fnXrtECOBLXrwkAfhhqNAIsAPBTef1BdQ3QGHxo5wac1iEzOWTFhX81h4l4ds75k0ORn31LPwwAvgoEwFf3rUcvGy3+9qwu6+m/4wYgTP4rTcXvf9294rWecf+TrrSBrSsWry22bzlcDRK5+5oRACr13UPT3GsLgHplq6TuAPkRXQRYdeMSEbBjQo/tEoG/e4vGjQHTCoHWAHi2vrjq/776Sq/7cAD4PkAB+Ge62cPabOXvcv1dALge/6Xm8vcI9Kopb5++tz/JAWBRv3VtsZUGOB0AiwRgYbNShYPXoPHPNACz/q0B2EckYNkAQAeFrrt0gIoI2KMEnjvD5V46i8lsHeBaNWGPD/hPmAYQN1APfjUmfyoAfNt9vvPc/2qU9hcBs5gdDdC0rNOWfovLXil/XtDynKvX3RTMFR7/s78oX2TZbNcXrWNc36eDweTJPAsL6zs7uwfwAWqS8D9pNHypwAAAGsoKvCIAGErgeoASsNtI/2aMkrBWUzrNY+49A/9RKWLvGBEDALv+GwD82AoA7+Ov5P9FkPyfhsj/lAA47t51y0K7qoG4bhtxN+/dv8UygzfPVjFBGvEIaMYfMWkL87wfy3dekK/lk6rEv3h1McxSPBsAVxkGZBQIIb7D/dMCsHjliq0ErmuuXTrAUgJq7KAJgD1PxkPAr14A3M5gEADG6F8r8BMGgG3/ewB48kKGO+lxPsFV3iuuVE8L+TueGl8S6gA+y47qX0Ij0Sqbb0rzOEXX4XvwAZTgr+JTbq3g069cWVzk//HEQV/YgrzmEmX7T719rol5IYmAUwNQ31lf4E9zzUHARcANQwdYjcR6/8Brz5hZV4nAr3a14H+aEBBxpj/+4Jv96gbAP+zTBsB59l/brX4hUX9d2tnK3w8y8llxeXXpBl5qtFRAqmvos6LM53Bmp0dG+vvzSfP09/ePjExO88PCg+7GWNAg2OabbX/5AVhs74gLRx1AQ7AlAO44QGO3sjkv95dG4JoPAbNm+L5VNa5njLoI8DcP2QToaVK/eQ0BAwD/7N9wAF76ALAG+1kA3HUX+rtL/FqFeryO3aKlCCi8lad4oCl5iB1CTyajEZ5oNBqPJxL8v3PifGc0qnAYmSUJFgd80ReVQ+AGQN8Rpzn4DjRCQEAZl0ALANxhALSMVDc0AI4WCNABN63mIQsBpAefm0NmPdsmXHbAqQFQS23bBUBNdjQafg0AbrkB8AV8WwAgz6t+IfDkf/r3H5Xsp/vtBz2R6Orq6uzspPDjMTmEIK5R6OrsSskfSQhBmKQ+AAWf3luyDfzFDzlXVRwXv15dKO98EABXHBVgJghVTZLdtmTFBMQddKaJeLZNUPw/OwT8RwEgBBgxwYgx/snJ/emtxnb6x9349ca9KPGZOWf/uVPxe0dm+djVXkE1HgFlNuaDf10jIL+Izi9B9rOQPSWZgIDxS0e8MxKD/PH/SCQmyiDW0RGLiVIACzF8gFBE8GacbICERHJSdAFMgyXtQKpn78qi8YdmeuCacbQO4a2CcDD6P1ANcniAdP++ty4oIBJYO9xFIODKFa8OWFRw3vAEhe55OggfO7OmnZphVcijowE6Nkwd4K8WhAZQ8v/F3fsfBMCXPgBeuAGQ4dpOyfedW8oG8AGw0uzxX/B41zSu5il+WHlFqvxkNJnp7U3HO3li9om0e4BAPJFORCLQBdNzogqWleN4zQ7HkQTKIgSAa0EHdiVzQsvbR43jev0AraDtAbB7WHMBYNiCi0EEWLeAqhNxtQ1YBHxlAGAnB5yOEScvIFeAMf/BBYBv7YsPgBd+AD5/4pR837nlrvd03L5Q8dNTo2W/oDTAIuUgzz6FL0ofz3wmk6RCPysAVA/RuLIPqEdGCMHyKiGQv++KtvyULBbN5zxE/JbtdoWBqdLW0T5Kvg526wEABOUCcFkgEuQDwFECNzxhQXugyEM1SsBWAS+dvoEfvhUC3AC4CbCsgIgx/jEUANv9MwFwduk8s8bsfy4AWLlfV9LPdPrDnv7rysFbkmDtdYR5rl6H9Fep9vup9OWKjyd5mUdjZwYgEu2IkoCOjs5u8ESrYHauOC8MMLSER/nKov6f+UC6ZO47fBevAABQOwwBIKRPqIaioPkrgTrAcw1YZcOSI5YuUmu2oDFPxAoOG72Dv5olIi5TsH0AvgwB4JkbgM+tga53A4M/rlqPQAJs5U/dz+kafPSjkXgqEaP6jonMYfbHTOmfEoBYJ6yARAcA6oIqSWaSShFADzwBfDcCxHslROyeT8LPPL9xhFx/jU3AXgBC0sGNWnVnPQSAxatuBJbswnGdI9YEPNfG4AuDgO993aP2igE3AD/94K7/ahMAQwG8Nsb3B4tfB39axXwsG5BmuWj+4twYvLx4orsb4u6SZ16ucZp8sbMCAGuwA9dAFJYidAqvAcVAP/UADIIrZz28BABAbbd2sOsHICwdvFc7qmzM+3GzoxJuj/CGcQ/YsWEJC702pkp9YxUJWJmBX40hIgYBAOCnswHwwgTgtbO+wSn88pf7NQeA9R0UPrQwnf23y8XiNA122Gwd8vDDju/Ugubjryy6swAgXxjF/2LRDjgRdCQSmYRmoFQGAgtnRmB+Ey2BNdb8NxptArB3dLRVmv/kk2AAFq9fDYoL3pZbwEkOOM6AOIP4v5Ud9ALgTBfXAASN/wrr/jcLwJ0ti6+V6/+50+zrCf9JW394zNcuykDAfvEGzIDl1TKsvv54PNWJZ5UhHn06OuIeOQYhYL3dDI1oR0ypEufEwUBC7oLS2dXAPNyAek0V/Z0EtYZ57QGUj9Yr28tQAZ8EGoKLji1w3Rw2eM/dQuh0kf9dAUAEfvTNEdE2oPz6uxQMR370r/9pBwDDALCi/xYAdgTYavRqnvTRpRc3GOi/d+/T+yurMPvm+kUxJ6isO2L66u6A7R/vaOPpjrWlA7wAMEYgamAEEQLEB3TC4JQAlABAlXUhbQOwjxZBtxVoIHBt0UgOWGrADgy6bgGnUOSbb+16YWiAHw0l8B/vaQuAgNmfLg/gtQr+KwCMlh9D/KHyNwL9qK5eunP3h7el4rREeuI0+yH2zk47vNMBjXBuADBMxOMA0B2R6GEyzqtAnILTAzCPuiBMhNpvSGVoawBODvdQR7wxPx9ieVoXgVkn4Ayy0EPHnVIhV2rA3jdjE9AcgKbt/74IsJP7fa1numsAXA2+KvjbUvyi4zBVabVcnBvBs58WR51BO9z3nR22XGPtiL9tAHTgmIx1dqhvD1szjkBhJDk7N7+8cOqb4JNP5jd3do5qe3AF9w/C6gBNADgo4KgiRsAnwQgYENg6wGpY1QOtnKFC+hp4842zcujHpgScFQAz+f/UWJ/2+JG7w7tN+VP/X13Ruj+ThteHB7Kzq9sy9kzJiTXQVP7tGIbqCjA+VWHQKaDFuhNRsQaWrnxyWgBKqA2uw7RrEwD0D9XZJDx/JRwA+yYQl5AlyVbLstlApipFdAfpN4EE2AA4c6VbAhA8+dVarazC/+b+PMf8u33f6/u7bn7J7kt9jyqnWP6hVJyU5A6Vfke3pZcppw5DmLb8YzYREhiw5C/OHZMBlmDxS6fz9fo5j6lfOjtUOEm+XOyNWIeEG8QkTM4VlxfmTwfAFa0C9hq7+20AcMAuoao4gp80JcAoFdA9EjfsBJE3R2zkh51yYZcVYAPgXAG+ATAifIXAX336X1kAag/zU2PO0x276v+BO/Xjt/wIgCrfWrx2/eazr0rFWd78iNBYN7MlRbkEouoOgHgpfvXOOEUf64oiqNuVgMziiZSZCkYmkJlBfFYX8kD8fgj+dcaUvicAlLjmrCOqwYpZJiedwyiMgeWVxbalzzM/v769U63u7bFANFT01hu1+lGjsVc/2hEVAAS8FHjDz9ogXLq+ZM2ZtTrIbACUJfCNq0rI8AbsdIC8YQLwXSAAQTUg1j715y75y6Av6/G/3RQATzHv3dUSrv5oUgL0FgBebc7fJIITjagwkLwtRQAJbTSo7L91RF0w0sPsAeMJ3V2dqa6YJXI8/doEAAYxjw9JAKKRLhUaWGnvIvjEIqC8VcGEkL39NgCAs3B8uLfHYTEL+uuDtICRijL8ARkqpCcN2xniAAKcGfNKCZiJgYhP/t+b8X8HgLXgAADMP9cK1Vse+QcmfuzKTVVMx3A/gv3I0TEyF+1wx/gs/R+znlptuDO/K0UAImoYbiz5GBmbnJyWMzk5JjVCUiSUkICfSgAlojHD85MT7WAq2WVFaI3AjHOS5uB8u+InAPNlAFA/bkcD4I3D/b3d+s5OubgghuAnAbRdM4rWzOjwPXO8oHEJKAQ8ALgGy3oBsGtAzBFAXPvj2/vnAGBn/7X87xqrfJrW/SyYACwsQPwjeFA7uzoZn7Wvb/vYZR6aAMaBE90QaSIewdWQZKHPnC4CLLmOXSWITxhhZEEFfgECroJOhgC1eol1mipAAIhLjBAXDKjpJwI6OdRC+gqB5Z0jjICotwOA9Afu1nbqpZHiMn7gYC1gBod0PbSKCdmlIjYBHiVg9w6qKkEXAWQg4hr/4wPAJ32X/FXy15jw7E79tABAaX8E/CfjyRQkAOWMq1rcfQMCBP94YtbzKqlc6vpkvn9El3ipuk9Wfq4ue468VxWNFqWQiBoBdSTplKof0lED5Wqq60D9tYlElG4o/95UBgggNNSe9OkJsCqgXq+2pQFwAexxXFQxMs1qtRIjAsEIXLl2xdU/ckNGkt60CXho6gAnLvzNt44OMCcJqZDgqQF44QHANfElDAD3/a8efl3mg6hPPEF1bFnonR0uAsR2i8aV1sc9nmQtB8q6dC3HsirnUGd+IfgoRBQIVAeTKCuJ8mLotPJLYhR0mADExaDAZ8AX7USyaAQ+4UI7AIgG2K4jENCeBtgDAdXKyUYBf0dpe3trnQiEEHDFZQteXbLzgxYBjgpQa4feGB3kbgD+ZQFgeACh2R+3/e9kgJ+b1d+e1G9w3Y/TSoVuWsxSL85JwE9H/E1HDxoYyh5pQNhqTNx0d9Ivi0eTlP18SUveEnlJ3mQduC1y5yPmgU4ol1lfglQD/iYIvruTjqQkG2FtWNEhRWGHmJasN4QhOV1UOYJPnLD9J8EHVwDzgcFhQE+JOIaJwAasVuYi8UykCFtgRxD4JNTstMV/9brVN69HTYsOeOS+BKw5At9+7w4I2bWigQB4Nr/5n/5nxsT/u271H9Dp7ZP/opqns1ya60diPkpnzhvdifKiltPRmcavaStrX+T6jdVl70NuqXx1+XsvAoOCJfncNYFgOg+i4nAMYkw6MgYkboMBgF1jHE2kVVhAO2vh0icA5Z1DzgZvBwD0BdQblZNSpDOejhRPgEKFc6TnvQli0f8WAc4tYOQG9DJKtyH40gbge1Um5koO/SdQA4QOgPJWgOjyH7f69wKw5FcA4sdS+yPom+6GMR9ldK/DFbpT6pcOO+y2dBouYj+Ds+X1t7zT79xaoSmxZAvdMfakR2B2Tp+ibggoaWtAAJD/EwNCMJKMRLu6cLM4fqNgp2IE1EKiBXAXpegTlp4uNH/6NQAHpwCgWm1sjOKuiRU2Tio1dIse7WxxocC85+jmN6v7mWPJzDqRe+4KAWUHvHz5xuwcPAsAL701wM/MqZ9O53eQ/AOLftAAD9fv7fwcbuEUVD8eQonAdLgAgCfOez+uSr/x6M+X371VNt6C/LequwOkPSCf5LdRleCwE8RuTIjrJ+m9aUEBF0d5bVXrg3neGSVhYCwpjQMoFfMBoBPF+N6wBVNQApMIDDWXvgBQQUb4oAUAdmdQo7pXjHTHUpG5arV6ginSMAnJwMYmdgyVHb3mOqyWdzIDYgao0XIuX4A6wIwK/+IjIAyAgPm/Sv7+sX+u1L8JQNBQH15e98T4G8Hlz8qOCO1/M41vumNpq1yrvL6m1bl6eKVAHJKHWRCJxCG8eCyRkCJwy3XU2ls3D7AAVJmOYmore0FsBlVxmqTPH3MDYOaKU6lOjSNdwk9aAoA4QP2gHQA4MPSknGXkO1uGAtir7WKvDCYNy38wCWAXus6GnM33JGNJM6B9gXuGEnB8AdsMAAJtAxA0ANYHwNO7YQCERf+uS84PmpuPfzLeKZetjvhH5OWPRI2MThdSMlKkQ+29ArUP0a/Jcy9+fVQq/EVDQ2hKhRuBQ9UK0kEDUuKESdUjJM6jUFByGCjJXQAEFADieiasYJHIPyWhJxqD/cXSwnwbABwcqLGwmATVqNXCANhF9dD2TAQAROb2K3s1VAfs1qqV6vZWhfZApVo9CjzcP1WGY2KtKFGLKBUAD9zOYHMAZAZQQAzwjZ7/88a9+kMVgSPrKN0/j72LnFdCC7+uqlJfZn8k8JdMdOoXusMOy8nN3yEmeayzOwXxz6JMD48+1nvL0/92TboD+nVHEF35wMygGUY0flcNQxI0FAjKji1ZYgFiv0SX4owbSs+J+AgIUOE/C7MOpQRKzQhANuCoysf+8LiBhTGYDN2o40LYlxu/oUqFJA+wX8M4GUwUwwXQ3RXLbpzsIjkI8UPw1e2NbQBQw7RBjhU1D6wEbp3h3plyaUGPq7rBfJtrqJiTF/IEBd2DhBwAPD7gG3MAlHf2swZAVf+4qj9vhqR+9VQt7FNQt38mqYtwIPGYu8KbJMS6EBVUD//y8t0VWnur76D25+asljDaZbF2ksK+uKLIFrqjX1wK7U3OKwSYjszoVJJc/l1sLsP/4pZDKB7hSHMlML+OjDB0AFbF1GU2fK2KuWAmAGIE1iq1EzzsJ6Wu7lRnFi4AYIH8a3skYEcSChUCUWuYlwhUygEQgLlQOSIBakoCFtRZ9QFCwKfOVLm/uQlwQgG/ugDwWoAagJfm8C8HANX99/jxI8+8v5DUv/oRl5jIfEvjP5PMpLTQ3YFfafGCpoW/L4VZJeXUw+JTFruu4E3oKO4ZAIhJPokUxCNR9gmiEtgyCJQa4F8RTzECEVN2RLyriz4JNUNM6wBaAk29gB2q6QpcvD01GFAD4LIJ8PxXahUYALHurlTnzNZ+FSPED5FIJACVDQwdrHLXGMTtqSDBWBHsG0ESeWcLBNhhYddkwYdWpejfdVT4zTeqbdCpFDcACFgB7p8AZkz/0t2fj3y1n6HyV01eUP98/HuTCTsXo35V8X4JyvMZk5osHczh3k1YfNNMFyWQ0GNCJxZtXvIRIHfPB5Htg2Jnc1C/xJV0TImuISxC8NndCa2svYq48kkVdeonTMASCPMFUBi4IcbaEXX3McdGWwCoXSHWeKh9yPlkYyLSFenOxkonlZOt7ZNqtcbIYKW6galzuEB2YRbse2ZNwneoUwfUK5wvhGy6qNgls1DMWDukM0NvvnFKhQ1DwAHAUwFqAeBc/56Njy7zz7r6ZZ+LZf6pwgVx/K8pAGD89yczWTz9KQ2AxGBj4gpaAMQpfivEW5KnclZ1g6a6VfUGRNe83qcVABEp/VLxZagD1RGgo0XEbRZ/GXLHXXElfO0Y6KQU7QL8kFACC+FWAC1MdAlVMBK6wY1xeyrxZwAAuxBPem1rDgZABBYgIgC1IgMBSA7hA9WtMtYO1AnA4b45bvhYTqN2wFoCSSMvLspDpgoE7tkjZh+6vEEVEhJn8BezWDzimgTqXgFkAfDMBMCa/8HuD+/QL0/bl56izNzVNbkCEPrD4x+Hdu3u7HJysh0638fuXjxvzL3piI3EaqaTkUgCpR548Ch3XhLdRvbuDAAojKL0QmHmQadEVGfQsgsBuWkgajqXKhwgTUnqB2d8arq4EKoD1JWyjSaB4+OTgwMm/XwA1GDubc9FWLUSG4ToT8ozWyeVyp4AAAuvLHfA3l4QAPuH+HPjpI6uopK01MgLLABYyUGLgMdGTNDODLoBeBVQAGiaf+b9b274tgBwD3k3LL/rV7Xvh3vqJqw/PlkpqwY7HrPjLLAD6Q5GxemftwWhrmTUiTBaII8gTP6Ojo6WVZ+hAHTErGRPh241kwRAF+qBM9O6QdD5m1FIIv5/V6enE5G5KzgTTa4BXRmyg/JAEACLUFeAHcomEYuASg0OAH6O7ggugP3K3MwOZV/jrHHsFIMZiMGDWDvnmjwo8meM6eAA322P08YxNeWq2TwmrWMP7DkS2h1488apEnQKhJoD8EUQAE+9ANx2KQBD/qrm/4YU/eO5ovpPxnUVnqVZcRfrlFuX3P3zOlCH5xEPfwKvc5wGvxTsMdjnNAqcAYCo8jtViXlM5534gS7aeRJzsNIMRAA8dqqMlF2kFBXPU4IWrBqcKy00JWDziAWijQaaRUTijf0qQn/aC6xICgAeD2KAOzX+gb9RMxCAow1unoMCQGlRGABwIWtoK2JLldk7plrHqALMIeN6lFQAAK8CAPDJH/3fz+yF74/N/j9z3Jc386tGenIgC9U/o7MJy/uTUF2nNrASUoPZbz/9yh5nTTgNBCtZJG3BHaosTN5oR/SuD4v8O9gRwC+OanFKUWgslWGjsHMTrLJBgUkCrbFEXUTjlk3Y1RFDdmB2fqlZVmge62P2JCxYo+le564Q2RQgANTKOZY+p2LwAGANDsaKe7D+Tmr09LlIYJ2rBxv1vf1DAwA1flAB0JDNU5slpgaW/HME1C3wt0+fCwEvVEjQUyMYkVGQ7QHwQstfrf64FQiAX/7Xr8ptXppDzZd+6tWVL4++vEfV7UH8iHxLuk4u4Wi0u4si5efEnHhPLKYe3DMB0CFup7ylCwopfyoXDg0QF3OkaFHIi4CepyIgrnOEil2EqRKpNJQW6nhYxRVOAPfGY2V07eCAwq8ysqcBqNVgAEIFRBgDriAhrLKB+wqAWmUT22dqGCTZOHQD4Ly9hwYEklJyqQClBG7SH/TsGXhjtYs4aaEwAL58eSYAXIH/BT2JEQrgq/lp9fB3QY6dVqa30wAARnUZMb+nMP/fifgT8U5Vrg1SOqQlXFd9K80dORMAnfa7JPsoykTqzlkfxgiR9IYh8aQQmEfMuh85hk7LWe3QketYqhMJYrSt87MXmiSHhAB0AO8eCABYIF8RACC+6slGH+zZyPgc5F45KUYYC6rsVSF/KAtcG7gDjiDguhcAHU2SztJ6o8be0qWrize8kwQkIKBCgo/dAJgENAPAs/zLAkDq/+7eCgLA0+yrR3gvLK8x9cPoTRdinqzeNgCISQn+SLG8viwZXraGJfhoxvU4AIjafoDFZrNEHWQLtAKg0zAItTkh6iTeJT3jUnOkLFGdL6IpEKXTIfcFC8thEmAolVSMdcXk4lpoYgjQFajvYWX0IdJ8RyJUSJuyrOLShwc4LjripJzrTtEURDqA5kLjEIsEYAScyG4R0wY44FEAYAwRLhXZPrjkDJSxVxDaA4YxP0CyAi9MADQBETUJHkEiuSDe2EWg9vh33QFuh/+sh19bgEbzz01zmr6FAYatINWS5wscl24fhFcSyrCil5XqSKk8u3rmlPKPpFN64FMsoJ/nQ05or3BcV4DEmDbsFQTgyit3jjWrndJRjhyFsNvVZXcfqLDgQnMzgMGd4zpdO5YM7yDa0zim2x/LRmY2IFcYAEO46yK8CqrVXQ4PQ50gHMFtrJy21gsFTBxjlqEOZ3KrVLp6TVcLq7j7kmUKOobga6NK9Ht7dIQA8FcLgDchALx+bUx/swHQ/V8uA+DminYAdfUnxufh+peKCvU8y1NP75tFGInOVAK+1uw8k723IP6yNIZ2dX2gnE8LQKTDOGKeykWwrN35ebFf1ViKLkLcaYk/xZNPTjcjAHHhigrvbqn8Y7F8cgJn/2R7Ltbdx9APLoMZxBdiKfwBRgLjRlXMjdhehxFAw+GoCQBYTbQra2esJnIbAEXAw4ceAL7xAbAmABiyNyOASgG8cMn/rm/fh2/Uu7VK46oy/5MJ04VWpqAkXFMpSa+WpbyDnaGSu49+8JPeHADzboiZJYByol2sUUiw+ENnjEtiDMqVpaaMJVwAiOW40MIORGJ/Xaq+50tzGUT8Tqq1jVR3unzCnCDk343/DQkArBLeY5QPAHDuZBMNgM2DAOCwVmVWyGodswsF70k84KGYAc/1/AArHGgVh7QGQAUAnPmvQQAY8f8bTggIlX8c9lCaZdGGGURRdnSMN3o3HSkpesHTX5rLq/AbXuToxwfAkyFyAEjQN++MZxjrsxCYn8uAVzqQ+NG7jCsAAHQlSHFoepCZAVR1bM4X5xUQc8lIHxEox7Il+HwntZNyJIXIc4SBQMq/gpQQ9MVmGdHgKsLBriW07i3UuAIO9k+qO5ITkNGGV51FtCoc4KgAG4BvnbxwRMw/uQHevPSv//xMB4BcCsAFgCf/by13wqwPzk9F8mcSj7/ux7QQ0J6UZH2Ky2V6XSVo/xEMakG8H3oWBtd/DwCySe+TheOoA9VlI9K+kFR1ZzYAaR7YK0JAaI2ApBnnraaP4nwxmU1Hc8WNIjPA+wRgayaSlnAQqsP2uHSQlaEoB2NCUOaNhAFwIJOJkVTa2bQIWDSnCNy/fd/xBNg4/MYiwIoFCABf/tWWf+ASmFAARAGY9X/Wdq/ratjewipzv6kOqxnXUQK0/6TrCuJnul+0P4xD6QpmSuC/BgAjPfzErkhUTAFWf8xLekcFh8UGdAGA1HFcDNlwAuyWn/liCYJPZ1PZGVQp4vqvEQHEANLdrAmsV1WZOJI85TJmjyPj1wjeQm0BUDvmjYECobLaO2IMk9ElQjo1zO3Tr1+8MQgQKyCivD9/A5h7/6cxAdaj/z1jH6/f0Na/zHxcLfYnEnYU3Zre0C23QAde3SJrHldY4TeXh8qVdDs/PRoLL/P54MkAus3Q9UVuDRBV7eIsG8iIKcDWQFoCk1LI1mldACn7ElDN5KX5T1pVC+I+OCl2grDSTmW7gmAAqwKqDAIgH1FsVAEAMgeVbap/LqFkl/GedwONnRpGxIgAHOJLjugJXCEAUnplx4Pu2wioCTIv35jVQb+cCgCX/NUA6NveqX+W/G/QB3hbzCfSqhk7FjO0gCQAk1H4/pL1g/E3yTBBKm4lelRp4H8JADvZJJFCpac4xEm5A6KnaAemDABYNAJztjUBxWVIdZv1DfZKWQizyqpQaIDiSXWvcQCzn+Yf5E8AYBRaNqAPAMBTPwQA+4cYNVdBYhi9A2qJ6XVfVsAC4MUbxxNoBYAxBcSvADD+55bW//76r6tXb2I9A7I/mNXNouqYp+yHVTUJ+P4lxlvLzBLEU9o94OMH89AV5LNGhDU95t8Q9Z8OO4tkTgUJAsCaSyGFpewHSMcR70VLiLgDuAYyCQ1AwroIUhxY3kVSWhAwj40SlWMKr6pCfrJjrkY3EDEhAACjnjY9zvrmBspKtllaVG0EAsDpAuwtgwpgkElKA67JjN1Fe9OABuCBuWXkG6NXKAgAdwbwmVUC4r7/72gHUOTvLf9E4b9c63ixRNodHTF3NpWhXzz+JWZ9yhIl7LYA0N3anWEmWlsAhH5OxKUmYqEAdFjZPxYvdEraTxWOqUoBWDAJA4F0miPrOyUkNN+8XHSrWpWCL8T+AECFuT8WB8IIgC1ZAgDsFEJPwGZZH5k5FQjAvgJAmgvqjDGTAGm6dDUNGWaAq0pYGYItAHgSCoCz+mElaObzjRWEf1m/GVNF9u7Rvij3Vuofxj+jBJkO6QCMW3LsiH0kADrsslP7QtL9Zx1OnofxHqspgMZgtzSmjXBajMoO4J+VUg1DYgSmM5m0DLBmMqm5Diji5mfCr8GaryprQolCrbZzwrqA7vLJLuVfazQq7Auh/LGNst4IvgJE8EwN0hescs4MXYGr4guaXWP3rMWj2hH4u9ku+EtE5sq+CQDAWADojIAOAuBmQPPPEu0/JtdU1Y09iUWLJzqL2M/KzZXlVXn8EQ/CyycDwWwBhsozbhxTei0A0MGHWMxf2+EBQO6hqP6OMFeQudDhaqYqUT6Mfxnqk8QIwCEC3SqRmGxBQBE3AE38+h6TxJWT7fIW48CoDEVleGekUD6B0c/V47QDUPqPSEBpo4p3BAGwbwFwIJ9NHbCzszm/4N1Bq5sGZZCcHin8jVMZ8EtEKQAVBfYGABECVDkAPQPWqQBV8tf6/4ZRAsIYEIq/l8vz9P+sDmzrSEQYbhZey3ll/eWR9YtIvWWHbgW3wjHt3PtnOR5CPDXJnBYkoOi/PdEdg3GaSiV0TxAyfwuMC6Zp+MWR21IEJFTzqpon0iQmuLGFIH+Fuh+Pf3muMDNXLG/RDihB1yAQCA2wf8wBg43aLpT6xnJpo472Aj8A1roJPWgIlkNjT+rI5rmJ+rosHLE2EN+zG8ZceWEHgC/DAXgtU2B8CwDvhACAQLSUqdP+g54U+1idbnnM6BKi3WMO8bUFFSROxMPicR8LgCY3h8pAseFI7gdxVlk8pv4JDPaogI6MsqTXEkM1RwpWYVof+ay5hbBgwMb25hYaf6r7YKBSKXaJodk9xP4ElAZGRrdOnNqfQwBwtFkqb9XrTQAwmwzRfmARoBfO6Dky99xtw9aiORuAL5sB8FwDEBgCvqksAHu1O1JAC0xLL7ydp/53A6CKceD954tS8F2C9Rc1kgT/HQDinW4CGIZk9XennhLBxzwRtxN/EhXS/mAmhZAgqkIAQMYGIJnMh+gA5gS2ytguW0N9WHXnpNzdme7qSnVZrml3ZKZiAgDXDmne8jae7r32AED1GKYLwLS+opYNXDc9gVYASCTQvwWa8r97114BbN8AwQ4AEhE38dcuvC1JC4fykBUE3awGY+0XHiN0PqPmr1zMRBPR6KmNvXMVvypLdHqSmKXs4rwiKHet3jmt3DINkvHpeZ0cmJPRU6whSRsAqMaVIB0A+aMaYOOofoDoPh5VRH9T8jd3sUIaUyq6InOG/EEAF0mUNjls6LAdANgpcMIUwrLkhbh67IYaJ3jv9n0vAW/sLoGIMQYgEICnFgC3PAZgQACABYDwAFeR/0FUP5PSAKQ0ACgITUQn50X+LBGL60Kr/xkAXBMlrUQxHmsFAHrZU+R3WecG8mroFP6hNgHcaAMVF5AXEPlvc6WUDISondDqE08Uv3N2ITTA3EnNAOAA5Z7wAgWAw3YAgCF4fHwgvSKycWNRLVzlMDlpHH9oDJJ0KoRbASD2vzIAHtkVAPdDOoBl6veCXO3dWcmU2gCo8k+k/kvLquYTJYLd2ir8rwFgORJOpDKSnXGfuaFYoiNhOR0cEdCv00PS4cqAUIoqQCEAGBKpHhoL874IMNo5N7BUDg09x5Az5I8H3xltzNqwogkAy0fhCG4dNZgObAOAfYaF0WyswgEkwCoO0fsl3PWBbgDcEWB/CNgK/toVIMH930tXWf69PKfsP33EV+JcN6rLOZH//HJxLIL6cM7ibAcAZT18uLC97+qwPUkNAIa0uJvxt7bL2RQQ0N8CP7IEhVRen44u21XkDrCCAimmND0EIBGMTk9MA904QstP/aRSLkb0XCxd5ATrQwBw+r9QC4hy3/IOOKju7rdxUHbIrdUcOKYCQtes4hASoM0A3wQ5LwC+NkDdAxRg/vkB4PCVq8vlObGKDADwdmeaT0p8DtGflSXMhsJsIKn0jP6pAMT1Ze65AmwCursKGyeeU0HZTtQqF4uqKOaczg4x1clxgrgE0nErKoRLoUtqi82KAOT19tDtt4UtkdgruKPmAXjKVRkJrtkA4MHfQRTgqMYroB0ADmXw8CEiyRIQUl3DN0xX0AoHtgbgWSgAdx6EACBzn7nSt1xM5jOSJDUP78ZsQuSvOkQSzPlFWzTyxnwzPe13eT4c+B7XV/o/0GHEktSn4RrexhXqnI06e3fSUdtjlG4mRnuEgOX5ETWG1LEDaQOKMzg975J/ZRc2fXkHXaKYAlOZg/Uf8643YSTYAQC2IlICW3VEBhrtAbDH4QIoF61X2C+2vGCsHlUqwC4NMaoDfzAB8LeBW2sATf/f2f7qtf9Y1fmuSP8o7boEUjAB+UJB/st3mSNQ0yFi8XiYuM9ymn+LLv+7dGuK8R1Yl71fgjQy+Xx+YHg4P7eNRF1XLG5HDKQ7JGW4g9MyfzSesAjA7/ynouDRDgkiAYC83i5c9I0jAnDUAFSoADJikhFaggKAfQk0xATYbuyiD+igLQCOBYB6RTWVlJavqtWTeqiwExC2U0JNADDGQD01AwDWBoAg+YsGWJ2nZSRdE91a+mkxBFX2B4vgyuU5NRG+M2ZkCTs/+pH1wizlUIuGFQD2LLou/UvpZGeus6cwdvHiJcwamx0onbBaR9kAalgEkkMI/SQnVcUoCcgoAqxgkADQ7aQG5zk/viK9fqgPhhuI1I6k/13cdkWkJlhGyWgb4GhjmdPGpPK3HQIaevsEigRVs4i4gtZQYdUv+NCJBoYCYMo/FICVYAWwVEL/B/yl7m67YpIAIG7eS/mv3F5ahfzVYNBo3Jjr/18BQKI7xke7I6MbJ1sTXYkeAMCh05eTRdRkIVGnABB3BtE/HBkSgsFtuAqm5RLQJgAW3AGAdIoAJHUNYImFHaKWy+sb23XEawhAt/ws3WlrAR70jgyJs1TAYW2HUQAMmDk8OGoHANoKx9xXiLDwYR3hgIXrKitg3QGqSDwEgDdmEcgXPgAeSRX4ndvq6fcBIFUoV+ndzUUyCJ2xSCrVnRA7QAXJM5Q/9P9bef5h8HTpziwbgJhxH1M6IXKMB3/ALjX1Hw8GXgDUJzH8AwDmaqzUTPaNj49j7PjYdH5ui9U6cU4s7lDpX86LwScnJSDAWRAWASlmBNLduAQ4XqJbdRhZ40KYquPoPyp1DoWJYCpIN7QJI4AMBTAakEYqoCbpYd4AtTqjACgRQv+f0gA16z9PJsBSABhFcMz5MQdUH9WKOIOozLluVYbcu20D8NqxAiKeShC7ENhVB/y5hAAehMwAvKqqUGDdzanQqc6SZUmAjo7x/r9F/c8JzH6nTP3REJs5oy1mvr/DbzRY6yXCgr0hJkLCe1iSw4czMTh24cIYGRgZKJ80St2RLg6wMD6zM5FQBFzBA05LEBHtFApFlBGAo0LH0+IubrKwBxHgqsz1oicIyrq6s10YkjlXHAd3MRl40bcFANhILrU+rPUvc2DIEfs/ZeYUmolQQbKnJwywOfjAEyI0gwiKAHaMqS4BPVHc7Qh8803EUwlkG4Gm/B9LGvBBGACy9uUqK8DgFcVt5a8rJjsSWv7L6vmn/DsCntQ2AQjwApxug5YaoD0A+i7gjOGMJEv1xsZghO1sUdfn0tYTApQ3iMonejqZbg2AjguzPgQpgCqbQqViA/UdjNiiERiR5q7IzPZJuQ93CnsWI0Nbh3sagMN9PMK4MnaOEd/bZc1QRdcAGk89W8TCAAA3VVUovHjVqgtQayUeNgfA6QQ0ykDvqhhgOABLV3UFqITG7NufprHq/RH5l2j/ZagjAgGIfwAAnecCQCJekvrcRGF0mPIfGRlJFiuNrQl47XErI2TVASVNAvqTXSxz60l2ZTIOAcgLYYUBYkC0ywEBK323mQ4+wVyYNEqDceXUJCYgRsDE9p4CAB5draoSAfxzBe5oBebBidb5tdYAnMggOWkZRHDecgQ0AJ+aCyUMAMw+UMcBtDaBOwCEjABHBHjEkr+GgMofTwblD6+kxBARwgHcAx1veU4DgOPVfRAAqQTatLcnIql4bnh4RJ08SvV30MSNOJ/CWv3anVAVQJYdwOJHrK7IJDPm4Q4TELBTPZT+Lob2MTVoj+HaObgBqS5MBmNTKDxC2gAz2xA8UvvcJosRcBgAtgF00DYYiQzOlaACcBFwuFzdGRd26J89rAFocL4khofAOLf2y9y8qTfLtALAWwSkcwD3DQC8NYDc+o4MQCaqCuathyTZJWGTaXZ+llSIEDWWluHVVD6xsLdj7Xj9bR2P+CFcmGFbo7FUvHdwYOQC/jcyMgAA6izal54V42Sw26qzRxHAqLDcbPFMjwlAurefvUU70sRfteQPAKQMHFXA3Z1zRaoXpoSRDNyhEQgAaqwawhVeRvwQqYO5WLYr0jVX2lZVxJwdIAzIqKAQAGqcVIq/ktsvnTYBa3aUJsANwBehANAAuHPnvgbAp/7lr4ACKLJ7Trt/FgWc94k1COvS+iX2f1cM8YG2HtDz9f9aAwAuMatzazzSDQBEBYCAvrmd+gkAYITfFdzMpq0W0gVNAKcIdGezBgDZRKYfYePKwd7hQZWlvkgINtD4sytuANPNWcaAuwTnbgJADYApYQgbV6QWgPNCSphF052DpTiHtmI2kmkA0EIUDgCnjCDmyMygvgKUGRAMgC8H6OwCeCqzIBEBfnA/YAw0HH/+wv1vCO+zf7bbeZ6QScefuzFQ7y3sf3n+Wf2BGghzt1ui62Of7u6QD/hMgA5ogArS9LHeoWEhYEyugAYCAd0M9KTS5kFfWzSWkHgAq33mGN3q6E458u/NpnoL/f2leu3k+AAT4VngC1cdQ8MQXix0yZQB+qRKlQGACoy/Wu2gtleBwoAJgPVzlV1cQGnWpqdSGCACsR5a8yE4emjP3zKkU0lgjlPI2Da8xOkx18UMuC1zhK2MkOSEmwJwVy+DeGAPgguaA64MgGn4f3zgjUMN3ZWk/G9J/DfTJUGU7v8RALqNowBIoG97DnsIe0cHBwagAQbGe4qNOkY5yxWQTrkIYAyY7Y2TRdU8OBfnYgspDpCboLc3l8sW+pPI8aGJe7e2Xd7cwbQXArB3sjWFzJN5l8UEgD0FwB4bfWACYFb4YbGzm7PpcEvMlDZgCJ6wOABlw9uNPc+CchOAfRkwCgdyZxPZF3ZqWAAYKcGWAHxuLYPwAuBNAy+Ifod4u20AVBkYCkDnytzrsA75JztinT4Auv5nAGAtMIRVwm2cg/wHaAOO58sn+xsF0QByBdjiZ7qDES2MCJnWg2YnmduIMzncowAgAnAETrAgvrG7WymzHATBOtR9cjpQ2shURXgFFGsWAMd1iQJgXvj+xjjb0bsp/h3VSCTVIY3G1tbJkTk7xA3AgcySwZwBtiGtLK1IedjNe7eNqoAmAJjbQFQE4HbYJEB9FfD5hkrrTmSd+H9HHAW1kgC+Bfn3qzuSt0O3qXwphv8qAOptVa5ItwzPeyo72DcMBAbGM8gOwmDrwo+ctsJbVoojLXFkDIyMz+m5D8xxooA0LQBQ/vlcri9ePNnlnKdd5Ha2pLgH2T1+U9r+MSsbGJNyAMyVbxxwuHTjSMJAlap8Hj5OE5BdhAgEHCoVUN3YalTCAaCDiMWFHCaL7RorMqdZTY351ADgTSAAphEoBqAj/2AAxANMq+fIBgBWMTyzkdIqUkSsEU9wDKx88PQAmKL6iADEIoNQsuVCpIArYBihgP6JMrp0OMvVCwB/5bAgySipChEagkmOj4D8e0T+vfl8oRBFodfR0cHuvowHAgAQMip/oGg6XV3LHgAqbAk52tuZYbR4roR6YQYCamwlFADqDdaXVUMBYDPR4V6DEahNLJZQhSHSI/CwPQDuqkpgnwJYCVAAvAB6+XroFwn+FPGGmZ0vvlq4g7FP09GEXrvwPwqAqkOmEHZOSoVIH8R/oS85U+YfIapuhoG6bdHrf0d3twySjuq+YCkUxbShRDYn4u/N5HtzfbhWdo8w7Y0jPVHfwx0CjUbtcMNbeygAMPdPHcFdwogC0FuI5OYQBqpVqvsqT6AAgC/QkAIz0HToLRm0AdivsGFkm8UBN+0+MScWaAHAZSDOMBALgLu3PMfeAm5ugMbjf2MJNQCZbFY6ZFKSEOvmKjbYf8wA3VpBAhAbcTpsx8vtf3X9qScRclQouqsTI3t3TjbmejCdODNWlD+M4qLulsyNedIKgC659/EPlXAAZwnhz+neLB7+fD7HKwClnvV92ICH9R2O/OHYtzq8va3BmF8D4LmG3t5l4JBBg2pta2KQA4V22Ecq8rcGgwAAMAICjtAZuru7v+fdSVKTGqEG7Q3ak0oFOKGgv+gGEQJgyf+LFy753w2UvwuAq2pANTyAZAYvSSatr0ZgQABSMADghi6vz0UT0vcf+NL/zwAg9SEypeUERXXo1dhoNHbYthuzfNtu30kIAZw5zxlBsANGuM821k3tTwuwN48rAE/1CRZI1RHawR3AKbBsC52LuACIKQAOCAC3jrIetHqyVeIwoeqRI37Gf/jMN9hETgKqeygEwztlFmlwnphpoQUJBqzI8EgOD7XTAV4AnljybwOAJU4BWFmBAsijCjhtVUWJBsDIhPgs6r/FAcjEI7LnOXUKAE6t9l2SMd/fDgAKgg4EdOcQC9C2FcOw6NhJWFOhAgHgLglcApOcE6YqhXGtZUGAAAAbYGZH+W4HfKqRCuAQyBrLwrtcAKRMDXBU35B60Bo7Bw8BgCFPGwBEi/FJqDE8OmoGAErEEFRauOoCwIkGR2QbVAgA7mVQfgDYBLCwBgswn5UQWDqriyK6OzoQJJtn/z+ahCT/05lIBZ4QsX0gAN2+97YGgAggQTNVrhxzZDvicSdlZAbQAdbt/1FtBEAA1UB8blnmyID2DCaIyP2foxEYH9s62YPIDo5xB2CDQOOEw6KZEXYvLOfWSBgBBIBLw9Y5H+oAAUTUhKPKy6j8UnYCAKgyXIzgEpeMsQokdC9dlZ6A1Ie7sgE2AC8MAIx1gOY2wFu3gwG4vihV4HmKvifTAwy6tQqQ+S/LdBAmo92dsoEzUP6J7o9xwjSNOdwtSB3x9orHixAZIrYn1WKaQeAmX5BWvzGSk1SXADMiqUhXj7oCCoW+BMyKGq4AAADXbnOnjqhAjctBZuw7QCJBsjPokAAcVlnRxRJyRHPr2Deyb8wHEAcAc+Ibar2gRIuOmGoMXUvWqEp1iA3A/WYAPPECcKs5ADduLKzhAuiFCdibEQDkDujCuleWgHL041w0jlRpp5lJdb/kfxIAnsSPR576T1lWdyNLSwL2KxzmxKReoicdcjISDZDGdskKoHWYXdGJRE9PvrcHAPT1JfvQ9A3RYvgnVfY2G8MxOprbwrv9AMATPOHTLYmjIwyDRzQPVV62nyeZXgJwWK1xcdg2Cagw17gb1jwAhYOC1HWqgHs375kdQvYVIEbAM2MluHcbYNAwYDEAbzMGnMxleeD+yu+9ErpmC9DSChoAZUhcp7zALa+AcwYg9EF3qSBj3pf6d+QKkSJGdWJ4n57+EH501jMhpcIYF8pS8WJ/hv1hPWID9hUSGSn2BQCH2rfD+HjuCgMAogJiHEPR1ZVWABzDrZPJn7DvEMmrcUhcre5O9atkEFfOcj5gaUPugN3DsMYh+AlQAfAEV5bsuYGfegEwPUCOAn/8yLcNNmAX3K0VqQLKqtOjf0/LKqZ5XgDv0EDHgW9NAAg7foG29WVW1t74guYApJzqpbT+B+S6J3hvF2O5hC7zVieMgHSKjYMoe1AlYsx6o2U0l++hDZDhNij47hKYp1wrFdEAGBUe4UhSzh5FzqyLGmCXYUIWgeITEQbCdhHI2guAlQ1s7MklsFGCYSHTJA/COscOTw7ZKwL1HQqAGQOS5/+xfx+4V/5IAa1IGXhv1j5QAnxR6BgjBbyyWppk8x8WdIcCkGgHgEQ7AJjS9XxByBUQAkBvIY3yvBqGdmSzzQFIKAIkMYSEcb4ohiCqRPO8BQBArpDv4Qx4uOQyGggeOQL8ECoCOzvcFsSAMBZGaABquyfHnAvIvnAUgwCABu/2wwAAYAvIQEEWDoGAI0wXC9MA2FAP9raYE7hxzx4gbgPwwrMTXtUAKAAePDBLQX0AqD7A3pwJgOqRhwHAIrD1OYxXYZwkDIAmYv0TAEgEA5BGUSAMN+RrctlMMwDk4pBvx8/hHKH55SvaF0TpGxQAroEetQ8M5jzSvHgOmROuYz+s5BhYEo6QMLyMbgJAiSNMdKirgVA6dHKEy+OwEZDwZ4EYDEGBilmDaiP0CsD2yUPuFVk2APg0AACf/O8YAHibwTkHHINg2SMNDdDrAJBiy3Q/akCeLuMC0ABkUqZAjBqccLOsBQDht3wAAKnge994p/u5ThQ0ADRtHQDcJ5PQC2yTUgrOOQGyQuiKXAKJrliGnkBvJorYUnVX7foUMxAlwhwRs7+RZk14d2RiLgbc5QrYRfQeRYP8tC0B4PgIcx+OAwEgAXTxABW/6d5eCACN4yqMicP61vIyroB7951dUh4AnhgAPNIK4L4bAFcfKCrBl5bnEkk+ODYA6JKNoQi0+H559enPCBCkmBJO6jJpHwCJPwuAVAgACRcAWWvoVyK3gdmbc5Eetw3ABUL89yZF5zunGy4w2oMIRHGZrqDMR46nC/AEejMJdJtUd7ktoLpPhc31AVgHh1KPGRYDd2XL28VYSgMg8/93VSoQtz9vBPiFYQDs72KqMHuBYTFgxKS0iHMdTaPuigMgcAAAAMr9ey4NIM0BEW8vsGoEkZ3AnAXp7AN1OYDsA8YsQJTCIQSIVw6KM0UfsJeuXnK6/Bbj33EB4FXpNooETmP4GUlXx+A6k9sQxI2qWHeeeucWkw7vboQDOboPetys8TNUf8Z3pAGCvqAMj8jEo53pfG+mkIwDAK4D5xz4is7y8plkf1A6yrkgJ1uDeBVVJBAzH6t021E8IqGdhu+xdhd+NbgwBg332C8FqaOQoLrLehIjLIicELuGGYVYkf4giN9oELIBcI0DbAMAKQOapPyz8gLKiyhCytACXEUIOA9jOJ7yS+E0AKRcz/m5AGC3LKQDAOAf0NqCNBABSLkEbdz8fgCyGbkI5uwBMvguzAhnouNs+cJYgF0W+24pFXCMPs6TcjTD3hCE/AfZJFJkMS8qw6WeG1kDI7YXBgD2ECJotMyJW2g/qcqYEHznvaM9wwaUtvFjxhaWpEWsCQDmQkAbgNuhAKyyDIT9/zYBGAQRz8TnWAT6likijglil1j3WTVAioF4/UElujMCkPA7fWlD77sUQDaRVRogkQp40kMO7kGWhSdlfoh0CsSiqAroyTCshEAALHEQsENPkHNiDg9lYwjzZ8XyHEYTAQAocLU1FBcFXDt7GmA4AAhYHuBakUHG1AG7u1wgsqv1hv2VMjcAnSYLJID5YD8AT9wKwAEgOALIadAL6I5PyIAMCwCYzx2oiRqRBSDMASUTifRZfX/riUWxtbYHumRo05kACLP3LLs/bTz//GeMbmAEP2yzVPYUAKQz8aRMh7FmSEUTPTCOlBcAAEQF6Lk/DSyBqO5LcXgW28K6OqUkrLanABBTsd4uADvrCD9ckVugoiJHu3uH+tlXbWYMJx4eHW0uL93UW2QeegB40gYArhog1JjBAoimY/ZkDF4FKTRYJiQEsLxe4vyUdCqd/jAAOLMz7rzzbAAY5p4heuPiz7qgSKFHY4+TIXLZ9gHADdCb5hicOatCENPvUSYuu+AEAK4BRqnPRnl9+4ilQRgRXUCYPJ2C/ldVwQRgV3I8CAPVDLmHAXC4x+zxJgAgAWxDp3WhAZB2QgcAZIUFAKiABy0AkI1gCgCnDMwDACwApPm6EpmE8YpiyWYqPlvG9h8UgcQzHXFUiMTVq35GAOJsInYA0Jmm0wOQCrzwvUf9KzpYf13ZOWYpQLqnt6enR1uCjkHY2+uRPj+EynHmvPJF2S9R5JjcrmwuLYGgyj4AqMtieM79wJgA9H+zKCDFTdoxZi3RFyDPv0T3kAlq7J+0BACFIGLdcW8hJphu0hI8oDOpv8bqFAUAuwwHLy3Zi6SCbAAXANIL9sAGYMk7EH4BaR5J+lllshgBE2WtLC1AXgBJXv22pXVqANSXofQeo3mkalYN5e0+pQZgtlbfT1mjrjfrvvdTNgCJSGJ0rswRzmjhLM5kuSLY8g+9BgGDAYKD/FJAKhSdEIm46gmen4N+BAHdXAtaRcEvQroc5ql2AiN7t4uUYDHGIYFdMqGQjSFVmSW+QxPANSHcEL1rjzC8vgaDC1f0ynIsnGfpSd3cMqXCySdMMGA8sz0tqAkAt9oAALNASv14MlNmfEwaaJEEXEUb8EhSauhOIf1AANJ4/rG3Q+YpMl6WPjUAuJS67O2AEbUSpkviDFYRQEfU2E6BLrytYwgCGzxRcbmNTTZj6XjKySXbqgRPPMcCRKPOUttImtVBiXhRCKAdGIlnMXQCYwHFGeM8X2hsqgAGg7BHGo1oujVEegNPlAbgvsBqtR0ApHqMbgDvAGaitgAAO4oNADhBElmmQwKwFAjAsyAAPAPBlvyV4HMJFkoYubIUV6zkZQfYuvSJnQMAWW5pQoGeVF9Kuv7UGgCNHjNYTNfGkbFQ6M9ipEZt7kHnzc5Gm19ZKs50ZTFIoCPeL5eADEKO0gZkVz/LudGqsSepPinlQrVPXVKCnXpI0AQA2NUA7FSqJ0EAmFeAAMBltDtlDcACro5dNwBqhOjBASvNN8ULOCcA4ALE3Rcr8MdrLY3g6ywT1wBkzwaAhQ60SmwGNQWZhFrZnDr9FcARnO0fGFKyuk22d1Xxp3a+SFQuWsiy3fSL2CjwifQJsMqQJgD7PdgSxFwfEjOS7EdhWH1/o7tbrSfp4nwAAYB9wTQBjkMAMN/erXMo2NG6AmBxAVeHpBMOT9wA1A+OOXk4FIAnwTPhnJ1gfgWwWoxnU1IRYXnS3Xj+O/tLZekDjaYxUtEB4NSBIJutjg4ObywORnqSrCvzlX61oQG6Ex1zsLtOGpjQUAk+O/bhqu+9ihK/tGkbHwz5usrO9s5JZXsDTlEiyxUZVAFSHATNleT0P7T0KgCkQ6R+pOZENPYxMRBTCDmeFgtJIwUCgG96RBtw7zgIAC8M/DnRBLyhAcA0mirl7ywaVBUhR1g2zBgUKkOtJUIGAM/OAgCCgAmpA0zrBBo24cVQGsdJQMvv5vPcDGrNiZEK6tMB4BhrHR14gLe2UFiCsIwsHz69EZhIo7OqiCLvhutxRnMt2uvQhbG3J8vbkLKj6yVjt1kRiNgt6sKO95Q+4PHG42UHfFXMxeJcLoJgAEwD/Lvj2hVEcVAmXkZpwT5addAXDFHQSz+SAnFkcPdY+N8trUFoEUf0iQBwnNjWEVJAfgD2AwHYRdxAjEA4gviR5e8wx0vTJqSiQCg4CACpBHQKAXQUmJdASCsIxsGusNezl7LvEQlnYQcnUNuARqBlLoBmyXMs5fKvwwJxaf+7XQnaTmiAua2NrY3SbARmtcxW6+Z0LfySSrc8KpyMN/AaDxX34JHv1yw7CkWfxw166MjTyjJ2il9PaeJGF3zgWCuBPVZyV7GjdY89uWjfOZAO3TpXv0D++AdHopkCnMOe3l5EP3VKgLnyZB6jnw5QD8CuUGn84IKPLVoBIKKyv3UphgExXVxTGGM98i4A2ERBcK3hXxToqvuvqeGQoGgXu6PnlR+IjUQQ96G0itigUO8AvW1sZ1fjolAY6myPiLxwmsFcADwI6wXCuIEVafZOqQoQvMIAICkbAYqsA0aVIBa+x7pD5BwKgBmNTVkXS0IBgFUbG8WBiB7q1s3i01Qb8jdMlL4crmM02GNXo1uNs+miql+tmny4pnf5VKw7g2YB1rpUZdOPc9THTnaKvfG+QqFXn4wsjWBAeIHdghcw+mm/7rLdakfSIlBFbRi6f1V5OA0BBQAmxGJP0OFJCwD0dFBoFSwfKAsA82VkHGAAqlYhG4A614412CiuV0q3B8AduwzIcwOAoluyEEjSv7oOLM3FL7HpErarLS/PRlEFGj+VfAIkpkI2NgBAgA9aCmETuQfSnvhdK1VABCJz5X2/BacartSrqnWD86YOudb2ZbVTwKnip0pkdDcwYv8gABFhqAB6ZQgIc+6HB4CGmgIre6PYe0wjADNAMChyB9q7zomitZMWAFjFHrI+vrKpANiQXeMNT6dYHXcYtgtDAVznnJjWAKgggLMU2gvA9du33s4l+3OoBMZRAHRzWEIcMaAS60DjMcMHbCVoXyg27SSXXADglGbwUsVTkQ7kBbKnI0A+PZLFLuCiHgRcVAfPnf1KV8ShQ/Jmv6be1GeLd0cFQ75L3lOcK0RQJ5CxHv9Mb05SAlABnCKJ8QD1A8xvrbk0QJ3NvxsM3GJNNJfFyG5lmRQKALbYRdweAIdgq8rY0jzlv4waAm4j8ALAETWwE5Zv6omRt+2Zse0B4A0C3sBGCPSCFArsgVQaII1gFnZBlN4BgNJsDPYAmyizpwAgKC7rBwD3QAGhOboD3anAQH46ECXrrVxK1lCae4QjMyBAX7h4HqFkuue2ucmrPAh1w0BRggNEueVpYyKidsdLjYpqJkOMIpdJovAnJ+LHXKBcT1JtE+f8sDkMAN71ArDfUNG+nepBFUnhSExvjkD16AECRW0DwMVCB8j9s96cCUGUB6MFsX7iBeCQ/iyagzgmpD0AzFYgLwDAaImlwJlMoYAGSAsAzMhKMAtEBYDxpwyfngaAoMB81g8ADlzMCKd5wBLobhMAHQnOumBTQGCGBwmoKvlA8dbQG4yZ4azZ2xiNZVmOgljODowD9gnja2XyRy5XwMkVUPJVUE89/5OkIKvAOEVQGoYX5jNlAaDhsungODDjtyMBBykOBQNdAKCOBIFaK2HsjgiXvwYA1eGIBGAmeRX2H0ISKC53A0CqZHq4Hhn7wATA2xBqFQPfD7IBuILi3hJLQXv4MrADTt0AbJFVMYDSNOe+dppP3tlsgGwwAOIPxNOcr9HVbWuKZt9J5JKGaNLZgL8jl42OogEXgdl9qbNkOygTc6jcx3SOXgQ6eiNzx1UWchdj8HuTvdL/YN349iwAnR5KszE4k8rodtHSnMx/ZZeuYcBzaiQzPmIFSP85h+dKxGD3gIuFjuyh4a65Lz5nkGwxFog7gAUB+PJDDBhAFanxBegf2EU92MLS4nVjZrBdFtgcgBU/APjDV8X+PIwdkb8yArq5R0GCgOwEwZ9QShF0sZ8RgA3XEX+AI6hiqbQGoJX8ObOBJ2v+FJZGyGVTRVH6nK5WkymOKM4AAtABqAuFjigeM1AAAPC4KwD09+uRb2wCQHXQk0mpTdK8BGT+qw8A6GRG/OlG7G+je57d9KwL30Mh+MaGsygsCACTAPiUPHQERQEg4QQAanXzCxo1mRenBoefDgBtBLru/5viA+ZzPUr8eAp6cgXosKQVBOTgSwzUCNftpzpBAGCPy1wyklEzurOtSMuYBLhzuDw5qLJ0BKMgxLOm9Qe/LsL7mLfAWBx6no8ma/aLETp7Pb7jBwAXh7YCykcc/dcwRzqympsRfER84V+iBbnE7SGyPLzGef9YLunYDEEAmIsjD2RvcLVShg+IRBBMAD8ArDGbx+jw0wEQ1AwEK2JphXnAfIEA8C7kL4VerFOKaAWQQRCwI22FCM2WkaynvMJ8DV2fmlVPWBgAwgDMK6ygQvTZjhiEWRWuv8379/Mf0dtbyMWyRW5zhqXPOMBJaSqHmiBu9R5KF/pisswH8Z5cD8werfzle5kAWATgWkBYrFutFprfqEP+R8ey0cXQALgEdhAMwOBI3AKwAriimFvDWL27sd04aJwchhkAZkqI5b/H0iO2gYm0R1wXQNg8rURsIJNtkucCwNIyTMBsIcfxF3JgCSY5F3j+nVIAWAaE3kgUSWZ6PuwICQk0zvsBkHuAayeZgJSqRLfvEE6A9+Qgv1wmk2PAWeKEjMXyGigyOIwu7sHoYEx6dmqYGRvv681bei/jBUAhIJj1cptwv0yKPkLs3wcAxocz2bjBRrEauxC7uTUMs0QYBtiAb4hanyYAnFgAoMXkkKGAqpoXI4kgZBs8AKAnERZg2wB4BgK4QwBYDbq8jHZQtr7nLQAySVrK5VUMC5rHqIhYRxbeob4fz3iUpm0CABHAJJcuLhdNOFHEoNPTBABGbws5Golo0CjTEKgxMojZzBvblNUeR0RghnRtH/m0k1KikJdi/55QANAQxn86dklJhTAuZnr98NdNAJgKBAA7DPlIsph5RGi6ugKg0RKAEweAXQXAzja7ybmV1AcAzEooAAOAex4A9GRoLIh95O4HdN//Vh7wXREeoPhBSv4FAIC50MVNlQbENLCUS0EaKtd5Fu3fAi5TfHWvIEDpdRKArWACthAXiiZ0cNjQ/VQePfoXD4dBGICBnhS9ve5iAwoAPdk1DHNVTxliaNulErM5xyfH+1ABBfxgBfWDa23fK0HAHl4ltIvUvwwNEZnECADABA9c+PD2zLnObBOTeoMtrgbG+Pgyf+zIzA5mCdW3NhosHToMswHMujCZFbEvyR5cBDXYm5w+ccjUUx3lgqwGOq7JzGhYAFzqbgFgTQ0nAE9sAB4FABA0Eizak7PUf643P9CHukCUAv/2/VoZVUKohk7nsj3Nyie1spTfggDQnyQAxDtCAZB7IImYUwcHAneLcHNBRloLAFTJP6c9RIrbtQqLKrm5w1KgzvMGdQDws719EvHT9GIqoNoTkZWfH8GRDO6VuMSY5ubp69PZd0/1RU04J8PX5RJApQa7kJCvYkUIJ0DWpWy8KQD7vjiRO3Z9QOgOd0/oJO5sLEP+4gQsWfvDHgRqgPYAKI0lentsAHL5PswHySTm1l/9soZCII5LyfT2NAUgQAD+D7QDgOQHolJUE4kpJzx3NgD05kcxBAQAW9XuS75PF2HDEcC/GvW/8HxoAPegcwg3fiYNEDAcrgdVoSrMmBufmcOBab4jHSHuwe6HQkBdCNjAVFDZIJGKsYUIAGzL3qfTAuDJVOObH+/vMUzELgNRAOYCubYACMgDYCwcu0EyBccAzLEVOpmcX1/9er08zXk5GJHkB6DHVULvcg+c28J8t3UFNAeA+YFLEc6f6YjG0xKgyZ0NACqBdEoCw9WKGYerneg/YYo3Dfa+Xj7rOX0J9hSyaaskMNozOAa5I79QKm8g0HuEShEAICpl1wMAGYCgkRfcQD9olUGmTgCALYFbHO73oQBw1RgcBJSHyfKgRRcA9wIBMKLAIQCgHfD6VUQBo5mUfQfwOcgmpB387btN9AqnuhJaqE3M+3ZOjwVAugUAuAfykQQ+EWu9sjlhwMVfKACmZQKlTgcCtA3BFNzbP3G/wlZSEO7BRCSOz49z4ZWMF45GerXQIXUaJjtWZuEEPt4RtwXJOXAP+uJNDS8ODyi8PlzfaA/sKnCYTB1NA2gKOWyxLDLox3MBINMDoWZoASxdU0EANwB/awFAQCYQTgCmwsLrsuXPjFBKugFXV9++Ry9ACvFZ66X+oAiQBiDREgC5B5JY9ILhXKlULidaJQSATBMARE3BFMuhhMcuGnFX4TNOWJ4ZjcfSl6jg59R66Q1VLlY1RjejsHOLk/4w36tWCwLgWF0vyOWh6rSCFT9cIZEGe436znkBUOeGOs4FoAl4TcTvJAM+PRMAi9fZD5imjncA4FZI7IRAL0BpJI7sSVZdw6cGoOeMGsCKC0XRZgVcctQCtoCbXgcZ05OzlVNPFvO999T2tv1D+X3fiqgwOogg/iAChTssCNzG2UFuTz/z9R3IHYJHpnlunEYg7xP0BKE1xzXFj6Vo+Ha7vF4ajW3EcCs1RoMQfpQoIcoFPwwAjhKEHchity0lf3UD2AA4A4Mjz+yxULfcY+G8YWAZDYpqcDR86FRIL26AQi7agaXQ6+gGeo8cIeSVs+2w00jf+9k6yIyawFRrADY2UJI5FImzlbCL90CPGa8/VfQh0zuI2D/iAHT99vdct6+M6wQCeFonNvaqDSm1q7JeaEOVF1ApTIwWUirbjNDOzAYrCuHouVc/HrMS8ZjBJRjv1fo2C5XgCsa4PRphYO4BcLsN++3SoL3DA5k4jPuHQWDK/9qid42wA8BzuyHIAWAleCgUfYARToTIKJ+NSiAbi6qJQNwJmEaraDbXMsTXBgCWBmgTgI0tIJBh/0A0iqCATK3JnRoA4Iyfo1AQZ6AqEjcBoO+GEsKdajmODDFKh2uqYrCC1YL22E80x4DBbIrD4FNsCqgog98FwDFXCJxwaPABfYualCIWI3N7yEWokt4PAmCfAKAKjIvKSgvXNABmJNALwF0LgFs2AIHFIKj3yydRAZFWjwt3YqQwIZ+VIKsIAqASIJ09HQAury3nUgGnAoApItZhY0IN9jCL/E8PAFUajAGM+J4py9qdA5ctLt4788RbMzE2fB2r0nEu+JljSrdLqlVT3ZyLx2UQ+DmYVCIAZjZQ1v2glG+r0gBBUAeY7wWj43Crbw4zSg9VSe+HAMB+cCSJOHMcTUOLAsD18wDgJvuBUO7UIzpWSQi1QGiIXdfdQCom64nr+I7rNQ/7Q9tuoDgCWzi8B/KRNOfwd3WZP0fbR4o9EN/L9SVSKBVDvUXtRM3YsMatcalP9XiHCWORrNQII21UlpoOWWHZxbXgsggK6jA+hbUOu94xrvw6mH0w+tEdAq64C+pg96Q0s31QrR2I/g4HwOX8BVUpqlLw2sEuMo7vkZIWBXAtFIDXJgD2YOgQDbCMfidEu60oq1UJIA3h3AodT+WMV763nRMQCbQ+0jYASgMQg/JcDAt+pJUwne3x/w0tAMDTL7P+Ydt2jRKB3Yoxp1st6oMtuFeUurHanoq/0TyYs1Ygs7yL6V02MyZRvsiSsN0976b3RnVvY6y4zSj+3qHa8YWiU/yNkNvhhwGgikCYVthlCIAAYI+8CgXf0H0BHgD0aggbgJWVMA3AsYCYfob8CYfDQUTIoqERhpUAHAiAeIwDgCNh59dTASC1Jk1yAY7oXfVCaOpOSK+NWCPO39KGTqANyOww9EABNcTZIhaISIcAbv79hixrYUUOR32nJzbEhLOSc0joyQLyLik11DsNuWt8bkeiQO5AEOKLKASaYxn3MZ9/PrCHtWPsAarxCjhoNK8EawIANok1WHS4x8GUMjuEGmBx8ZpLARh75BUAajeULgcNWQ6GciA6gQz/92Zp/WWZSIem416gW3IDxOPZ3pDgW6BH1hqGrKzT2t5o/2yJMYim2w4WjGWsshVdvRpu/ztH4pu4CdLxCC+Cw8ruXo3R3F07mydOOyoJ7RaTGhZOdqHNv1svAbR2WzMuLDnGhr3xQfxJxJqRWJosblTrLNlGWR8Xf+yxMm1Xer7pxvmnAYREBW2sKH/VB8T8AzaGUf5yBVgK4OZNDcADOxL4GvLXANxpDgBMAIyFyzuVAL0YhYk1i6ofjInAeMqQpEueva1kH3JOCQBbiFgvhJkSrEvppiWI5F3Gtj3bkL8CII8wf7w3EykUZWVDtbFnD+ITDZCLsIaoZhWT7sOGxyrY7q5O13JztQhEYsvmA8zATzmd7y8iW8zerl029B+yeq/O4ECdlb1nA+D4WG0VpJFRkVphLwA3rd4wSwM8dwFwqwkA4gRm8NrkcxoCvKiwdDgXfnkNlwMAyNoAGA+0g4R5KagXO3d+AGxZ98HWVvFCRLYU0SHDj6nUT9a2XVrAKDUfCHLhI/Dps5weX2EXoQmA2PdOfQYWfHZmOe2hO+Labo9a36lttBTZG59VwKh2WCkmsVUUyaIK1MsuPX9IXHuLKCCoHR7ufxAADU4S5/TqEADsSNBpAFhBNWh/TgAQCPCY4F8ZnS6tig+Q4EgUuz/GfXL+9yj5uwDIGacJAFvK6t+yLn/7rS3rbfgD8UiCs5i73Zo/yCwMRq/QA83BOfhI05Y4u0N3a55YGqB0UnPqM1A8NsFO5IiWvw4K0CNJ8Q6ouu7sBj49l+9Pzs7Pbx6xmZf5YtkVBAAOahz+XdsPA6DpOdTnhBNElfw1AXqDsALggQkA14NZANxqBsDKXUg5z24ACwDIB2POWQzIxVCYrpI1pGfJ0H5P7gwnG3MA0OINOY4nwLO9wz6yTtlMjNBgrrf5nROoh+AS9EqVSRT/RE4NseRwwvFeuAKqjoxqNanqiXWZ4lcMyB1QrZnyZPYv0d8vu8UxLKSKoQ7H6ibAiB9ugjiqH5wegIYFDZRHlfPorhjyNwEwGwP+dgoAZD9oBhVACgL+nu7EgFTshl3mXHAsX865AHA/0LkPBaCZ+C0KhAT5bXu7OBaJdrJNNS2mQDP554L9EyT6sQEa/48kENY/qdT0jY/hTmlp5NIyQjwXXQOd7I/pNOXPpj8OfzKvAHz6AQvOewWAhXnW8h3uWQCwb4wzhev7pwegbnur8AJ2tAFgyT8cAFSCSS2YvR1iJWxDrHSEEYBCwYIgHotHx0q6HYClQD1+fX5G0evTGVEAbJ3+4B7oidAo70ike1SGwi1en+9h/NmORYgF25uLDNIahNV3gBA+K/ljMvvJUQGoGe5mQbx3HyC3UcJacNIJiANVG8XIYB53ALvIN1SDEFbDyJA/aToGA/vBHn7QtBAPALJ2cKus5d9CA/z9FAAsrLMhJJ83AMAEpqguB4+jW6sn1/u/AYClMXgPdGGJbTQtpULuBzwIBr8a0FmvvkJkQhwCTnVAw0gn5kmbeh2O4FCMm+q9AEjXb9Uo26idsAAoM0AAOEiihNnBkhwQAKRknNOkT84IwKHMGdu25X/t2vkB8EcxKZXxBXVQDtzRnbTCgFy6Zvnc/xsaQGyGEtKy3Zxelmb0ql3v0wsAlv8UhjORPiaJ8MpvxFTD6L5pBVaYD4j55B9JcQbsiWPUoaqgWoz0c7MkKsc54xXZJOaLLAAqWxs71crBGQHYxSahHeQA2wRArgBrR3QLACDlRDZTUPLPEYB0B8qBSwQA1aDoBww06c8DgK0zAcCzw35iWeyU6Mm1cjoD/UGaiCx8wP6fNOa9bJfK4gXKhsl9p4GndjIXCZI/IwGV/X0nDlDjpgBIvzffj0gAFkygpaNCANDlI00j3CzkGvTWEgDtW4j8q6hD2kGroJob1hYAd9sFAOOfs1n9+FsAxEe4GwS9IjC4s/l83m8EhgLgd/3CATj72a7gHoAKwDi/rqzL6G9uoSoA9L+Hao9NUCzZKGY3aAPO7ey7GnjpG8biXgMApzsmEyC1DdBgy9lclM9/P5ZtAIBPiiwd5iQv3TVU2ZApYWcCAPXgWCa2XeL04CvnDAD3A2VsAPhIFFLYlTX3XmUCUZ/f+78HADUHDOJRIBBhC0lv1rH62wDAqnzt6euD8cs20RNE/YuDHTIBeB+9Asdsxd7XAHS7PUBxCTACdHRLboADeHjI/iMLEM1k+Phj0uicLBvcEgCwYFq1jaGW9Mg1IaA9AOj/IZhEDVA+DQA4j6094eaGOE9BkCwIy6ft1vi+fH8P6rGjuiEEtdmJfD77QeredTRLmA2I0T5bH3bgDySQJ6aXzpIx2oNoaGnyl3vBVGT3FDjOdWMGGeesSgcfy8H8P3Z3lNScD+dEs1m+p5Pjv47RtAk3DyEfuoD5TD+MgF5unP1EqQCknnePUW1QZ9fQScOe9urpB7Tve3VcVQaH9ABRBSLDB6wbwPQCrgdUhZ4CALp6+WzBAgB3Yg+35c2vL8t60M5O7MjJ5s4dgNg5AKDiQtGE5ImzRMDOZ7QLgKQ+cizd55T/dDcLOPcaFgCcMwXV0OnaCB6NZEcxOQeWIcpMMdWZQ0Ix/W17Lg4PAA8QFEtypChzhbeqMuR9T5q9MSIS+cD6STMAVGrJDYAeBsAtghwYceWcAZCxEH2pnAoDCAXpzu54f/kPmAAYDNjd2ZNv8qoWzgxA6oMB2MYVsrNdHIUxyIc00cYP4wVAmIC3CFmyGiQSGZf1skr+HALLaQIeACKR7IxslUXUeI/5vSpzf8fFeB9dAI5XyWKYFCf8IRqEDfHIOLGQkyNC1aC3JgBoPRAAAHuFORPwIwBQms73ZQsKALEBMLoXUYD3q4wCYDtUb1MACv89AOx7AEOiEK+PS8Qy29MCAT8SBe4UPWlssPizKPNkalbobY8ApCPmSnh2DMzl0Conkx8aNZnizRhwX78FQLonOcdB32IFICcMO/BE9QzYBDQFwH0OFADIBbMbaP6cALhp2oC0hXUYoA8vB5YdROY2f19lKhghAdELbqG7zukA0F8Tj58XALgHSowLoXkxJh0kp/pxVJFyLo5xQbpGvMH5rzDs4L2xl3B3f2tUjXvS4sfJz41i0jn7vjFzFH1ACO8hB8AQkIQBerOYIzIvKmATphs+4UAquvl/DHgPB+BwP2hXOC8iTCNgUbG0A11zAbD4oRqAxSD9PX1WGKgv14dUMOb4bv6GKMAs/50GAIXAcyrR6y9Bie15AYCiQQ4djqSwgiDenes7tfx70BLXVZIKEMx4aexhcihDN1z+wVmT284+8Eisg4PpB+Zm0D6EpwR7XlFPfnRSK3ZmBiD9fiEA3xYJQQGgtF2VCW8ncv8zObx7cloNoOxCFqqeVGSEfLsAYE/kXWsuwG03AI4JuLKEAaD9vfoKAAAwASJdcGQ3/yX1wPHOdJ/1sfM852MEOkGBjSKWu2PRQaQ7TXsWU75Q++MzCvL2Md8lRA6y8Y/73djfy0kfMp0VLUDV7bmIlQmIcYJcIjo8N8u9Oej7xmR4JOgRQEjlB/rlcNBAjlagAMAZj/sHAsBBA15gNag3UAccWJJEv1KVKLoBqKmqcwQDNuevtnsFtAfAMjWAColoAFIRTgbb/GOVmSDMOv4/AAA79+gPdHWJP0AEAoMRfgDyCgA2wkQi3CklFeK0tzhRRP6TYU9K/2sARubmMgQAgYAqAtPcspjoHVAESD6lR1uBn8xzSdAut4fjbtlAKPiodhgCAHxEaHlulPUCYJWvoragLsGgDwfAugJu3l6hEwAToM+ScV+hO5LAdOjNP9bKRc5T7O0rOHmC/1kAGBcqDsMVwHC+rqwl3N4WADh/5uU3RFNQFQNi2MsJHHe0YFSO4B50d8QZ++FaEvaOTs7NoY0iHUU+EIPcSuicindltAbAi9Xf35tPynoR3gGY9C91ARgRUcbIR28cwJkzW9UL5H0AOFUhBxgLsznf/Ap4eCoAHtxiIJj//H4tmcFCV0ciOlt+DwBQEI5EQF/u/wQAMAbpD2ClaYwTzfv6Cnmf++J9jyF/vAR9XXNb21qfVHQ3IKL3BCCm5E8AuEF1bm4ApdJxAaAkAMSSeE4KWgNIRlgBgDsATzUB4DoBEOBuJnMsAVkYDWe/vh9aKwbTtMqFok0AuH9aAB6uYElYntXS/f19ousHCxgGhVKZ9T/QE8ZVOn2F/xsAICqwU5U8Mav30yLewmkAwKuQmUFn8NT46AaWvkyyT3i2CLnghu9wAYAPjHG9NPq+CQDKJjFRl9/EAqBfMsIqFCCFgBYAnBYbMh8M5SLVHfBRb4QDgHgi48HXzg2ApYfLBKBgA5ArDOY6OlEMsFn+g13BGAtyDgDQ01b/2b7A+QHATt5t6/fKNv2BuAiq15531RQAbeH09XFCrFoV1bVxdFLMS6d4ES85CoNlYBG+KX7BDlW8f5oAwHWo4ArAAglUKmoA8rwCDABwBxzvOgBweJCUd+oF0I4G4E4ZjhKUYUABANA1raPO7GhjoW0NYDWG6zVh1pYAIx90f+VtMT8gtl8fASj0DfZGWQ62ufbbOveHRdPa0ztfBZDjiJhiZevczzZM8liEWeJoPKuNF+MpZ7DTsgFsJ9b0ZpFRzGcG0dpf7LcAQJY/y+aQeAd7xFCMRjToBnTGNADTCcDRWxACCsoU6B+Zv6LMQMyIZ1cg7AkAgNgKUoQcJqZi/g3J9h8fw+k42uYVgUKy/RM1d4RbTxiONNtDEHPAdFBvTdj11gDcDgHgAb3Agf7CAMUvAAz0AHRUg7x6tz6fQRl+z8eQ/8cDQOJCHDKGDeFdnTm51doDQOkEBAXyiSETgHrdA0B0QACAfRQRALhKEB8VACzx41gArGPt8yEbOwQALN/m1AlWiqCpRPYCIJXYONo52tosr2/CSMAH1eSZYABq7Ay/9oEAOD0hD5dW55KUvnr+CUAGSq6/tP7qFXLBWKqXK3yE01fghJBzAGA7KKNc2Sr2wQpIQVxZXG59BRcA5h/84SwEhgrJ0S0HAJjtAKBTAcAMQEd8TABIoF25eMIajdJctKsTq2UKLgCKGoBlTI/nkkeulVvF4R5QdiQhRwz3gLPEUC/OAbPl8jr+xxRy9VADINFjEwC0Gh1ZCYFrLgJOA4ATBbh5nxXBA30D1gWA37EfPT5QXl/DaCCsC+s6d9F7AdhWt3cbZ2uryYfUt1JMbFc3OGQM+EYTveqvdAAoBABgpgp6C4lL2EVmAVDxABCLJmb5gZ64AQDchGjSAUA8wuKiAkC2hdZZE76xTACWOU2cZWIYAS2tI2z45xbDddwAW+uyffT44CAQgJP6wV6jYoUCTgHA7TAAbj5dnrUAkBdqsI+7HKfL5bV/baIprDNxzvK3QLMA2D7/IxNeKmwmTXdhBUV31gWA601/LBvlj32JmW0HgB0fABn5QN4AoJhIgQsvAFc/sYyAihr8DACera4+XWVhEFLIqBVjo4isHuezX17H9cWtEtgtyMiPAuDAVUR4KNfF+ry/JqR9ANxNISj8Tw4MDOStS2Cwj6GuufXy2rt1hEC7s+eu/RUAncw3VD8KAOp7HjE/kODyvkTafY3lHRiDchn5AADSCoBOAwAWS0aKDfRpYIkUa+fjWv7wpyQoyKIgHQxEjKfG2YHQAGurq1/RGcScsSNWiaBoHHaEevy3peyvjOZy3Qqq2oJd84GRd4Y1Ob/YGoDnzxUAjg9oDYizrABuiVj5YX4k0+cchAFgnsUJwG/lWaTY0oWcqhM5Zz2ADk9qgO2PBsD2TpV9ZJ0piKxLDcEOtUiNfBUMhsHEHAEYUQBscw8sjECWhrMZoUtsQESCopwDfsRGDewSRaAwnsM/y37+8w4AJW6Vl+3xsAG+evv27bpsFKhWD8QAaCj1v445gogFovV7gwMFsblu78QEwBlBwE0yC/7ekHvuKUGR50/bAGBl5WeEAVwA5NSWqPLaH+Xpj6EBNAEfEQDnLqiWZ1g8HotGu3u17tFGSDAA6oIYxEawAADUZjt2zMgHhqOcA17RAGBgAADo7w8CgAPfFQDLa29h6r17i18xAxoLABDY467B9fUy0wSc/VDnekmqh7ruWXQDcMiViEdWLCjYCDgdAP8qugDoAwCdBGB19X15EgDk+j7gtrex+q8AgDaKCuuFsPgZD246Z5kfTQ8MhCETgLntxol2A3kJdCMdJDbg3Eg0YgOAfcu0lgYsAJgZzk/Pf2IZAawN3eXiyLflTVz3717h922sgajtifO/rkLEDe6EQQx6s7y9U7HHCrsBgPy5n9AdCwrWAE+ftnUF/IZcYF/fgAMAG8MTBGCzNBLt7MoxPtB3BqGHAaCk0JcgANXtj3y2to+w+jcSj9N47833Bf9LTM8QnzGUmKt4AEjz9o8jFAyTomd2bnZ2du5CXNoDFQDT3DlWGFAP/4DyPEdsADZlGAmfbd7+EPC7d+tlOIOIMW2pLeXYDFoVs4+1v7JxhJfDyaEPAA6h2t+z8gHXzgGA9QAA4pwQvLaJQODpAOj7nwOACOzwHkihpz+RyfW1/KfgM4biRQAwpwHYIgAMKUgtCEAaovxn58YVABUBYA4jyZMEYEA0AAFIOgAw+Fc73BNbD/YACIDOhw7Y2VYr7De3pM/8kDMA6uISct3A3nEoAJXt1fPQABD/0i0AAAlrAAYG+gZRERpDVxAA4KrorkJbAPQ1B8BkQf6DFxD7kwBAscgWhk1i1Bsi3D3wcloSMJwEAA0bANoAKSk0AACRKMJABGB2UgHA1X4IBEAB0JlSACCuCgCsSBCsQACAaDAB2MQO0aOKlvtGeZm/83mvYpoYwoXYb81NUDQRjggAx0K6NoUdc+/UXsXjCAYCoAZEqXqwB/a6cHd3OMpB3s3F3TYAegISeQGAzk2q9QN9thOP/jkAqKGvKNnBDvguVKKCACncDCpx0zfBQKIIi80GoKo6Q2JR1IOhPSB5SeQ/d5mjwuZ26goADNlL5jUBA6o3wAFArMB9rHiDsKsY9UwC1t9+hfEba2VZNQujH7vBZXTACexCthBVOIxeSkHN+oF9FiGjDo2L5V3pIGdQ2EPdG2ICcCcMgJvsC3QDAN3ciXqg9dVXmBCbjKb7Ch8DAISb/jwAEBeqV9hE1M1Jg9g13GeJvs/UX5YpkB+IIvy3MzdpAqD2kqI2NJqbVQAgGZC2AFgQAHoH7DAw1+0YAMAKbOxS9Zc3WEDO5YJIC9xdXZWLQMkflaM8eEtUwFZ991iiAVJNbNcMYm8MdhXt1ZUR4AbgxukBWJHZEH4AOCOaqYBMNPv/AwAMDTa20EyKYW/wCNKS+5TMp2GWWAD05AYjxXp9a3aE1v6sA4DMBOiIDsxOCwHTLQDI2ABIbXCjtk0AtursE6sqAnADcNEwsoW7+0wQQ/5YBgwXkGmjbdSREAGlAdwAYPR42VEBHwCAdAZ7AYAROFl+t4o1IfEMfNszK/kWAMT+LAAof/y/zjVRKBnDk8z8QB+zgM4/zrkAenr6IkWUb6nnH3GAigJAbyVOjM1OT5OB6UwyY9kACAVmsFjYC8CCBcA6Fj/t8eLf5P743cquCv5T/R8xRlDb4yABVSLAbXRiB2IjxbEQYBYN1xQAR+akuCAAPv0AAOA0TZffagCyfX0fEYC66OfgYP4p3t8SAM7938ES0dIE4kII5cRSOa/d6gYAQ+N1ohFfaAIQ7b04PT19eXr28iRWURCAbQUAykMyA30GAHkXABg8T5EjCXB4IDKU/ZKI/SIeiD027EUSANAZwAwxXEF87gHqSA6oAVyNgrtqX9B5AbAcAADagsq4ArAmIvrfAGDHEvSO//jf31zyxmdjvMIR74EudnnEERcadF9rGgB0DMdKNYRu8JzJ6D/O/kzZbUF9Fycnp6kDJnozvYYGAAA9oQBwXtD+PoI+W0f7x/WaBPwQ8dk54h4jrASTRTOSFzzkkPG9PbkEto440xoAuBtF92WrMHbGtgDAnBNP+VsOoMWAKg2/KwAMqhdjcHBwALVaXQ4AHVl+YOAjICA2QF2e6p2wE4pB848GvXN7R10Fcg8gpofezrT2SK0EtaodYn9ngtOecU/vUAdUzCsglh4dHwMBFyenxlgXbrmBnKYazfSFAYBsACbI13m7ozSMaWA4e1XZH8KJ4nj2a4d83I/1zpnDRpXBgu0jqAbvWiraiQ0ujGFCyNUhrtyAh7pF3ATgNsDwAbAUCAByQawIBABrBEBdAQYA/JRB/RB/kApQAOz8SWfbogDx4RLzA10YO5wNCHDAlcO+vDnrzEwg1INsYBf7w+kK5sYujIzxTI1hl1i3owGwZShR6HPKQTJOMuCTT1gSsM8YDzjYq4vc4eGxBUBUP6N+0AQ2AKgh3TuSS+CIA+hrLgDYJ3Io42IXPTVB4QDcui8qIBiAWRuAwTYA6LMAGPw/BYCtGfD/k23GhdKoFmHRoAcBFczHiA9ujUuw+WebAwK6uQIWXmRicOzC2MTkxIXxiaEEd8foSKAAkDIB6DUBmOdkENztXAK7J67dsVpTIPIXAKRu0GpKBhcSOMaXeQDY5xLhAwBwpAeGhwNg2gD33TaAEQlc1gD0mQBEkA1efVUOBKBPATD4fxEAih8QoN+32B3JMrqf6DG92r4+S4kjniMeYU9cAIhlu7u5MSKdGxzqQ/d/EiHFCzNSLXAUDMBAvt8DQJ07CPDQH2sAalwzqQfA1jUAJxL3EQ1AJS9BoupezTQBCMBu7Rg7g2AENNUAnBF151GgEWgCwLYQrwbwADDgtgEo+kH7nFr06rv9NwDQNwGKLk4a7B+Iwh1IZHrtcJD68foK/dLnj2AeVulGpTTAag6NFyYodW6SK3ObzU5VJ4MAQDRuADDgAWBTAEAm7wDPdu1Y3LtaTR5/PPe85ms12QbDcyDLwqv0BNZRN1BxAVDjdXGAaDCMAKsgZNEE4L4JwCOVCwgFAJHA5elMHmKU/xQAMbaFlJdfrYcDMPh/FAARP/tv6rgHRtntFY0iRWQAMFAYHMCQnAGtGHJRsQEsuZcpdERuG3QRuDUIFaNb8xqAjoQJQL8LAEyMQ35/TzaLHiDAI0E+xHgOMGDmEJtA+YsAoN6NzWBVKSBBoNCeRmsAAA2AdIBdEXStNQAPNAArXhvgVmk6U6BIJY5tAKBtgBwl5pHywAcBYHsBKRZU/RcIEAZkRZjsh4yy9cXQAMyM9SvxwyeMIjPMfo0dXZVbqWiKZNxbhe1jFgARRwMMKA2w9ImRD8SGwdphA2Y96j0h7rpsETuoHUgjIJFQmR8FAHbaAIBtFgpwQykvCOkiOdzjr1glLZMCFh0Ablz3ro+NUPqPzGSQ3RZkdgffJQADgwNKmpA1KsJ4BaB0jQOEO9qMA7YPw6ANQPq/AwCvANR7bMwVYl0EIC5J4r6AmEChMJCnBkBVIAq0rPlukJFsj1Tr5C5hGJCKA0Qj6T43AEUHgOVt1gEzxHNy3HR9vOXz7R3W1CUgBNSRQtqX1nVGi9hmwISB7A1b9DUHPbTiAGcHwPICFAAD5wnA4H8bADqCdAQKiAon2OjXUwjNdfQVehNzdZE5p7TauyPn5sYHcxnZJh3PawASBKDfygf6Aai2BYCt6vcO0TskGQN2EzZ0nwAvCTWVEHUj28tXrgV1hwUC8MADgH0FPIUR6AAwaAOA+vWPAYDzSRjv9l8AQLaCigkYT6Up/t6geIaRHsbaHFkdCanPjA9x6ACyPlyEh+JhTBhNJPstDYClVn0D6vgBQCSoqgE4aQKAE/KvcTlIleVhSAthoP3usbSN6/lhBw3EkSrSIOTdHRgKwP0gAG4usS3ABUAfikDECAQACQIwcI5xwEEPACfOnayPT1w74R8+ndztI8FAlAagQCSXC4xnWPYAbAAkRCUPGMVOrb6BoeHhYdiI9BJoIeS64jYACQOAgWEvALI94BQAqJEwNYyWhCTWdyp1BI5gOhzqUSGHMrhgE7tjr1hXgFMW2i4AKwLAMzcAeDkSKhAkAPTEzxWAwdYAuASt3gh8f8Vl1gd9g4B3iPi30TbGEQISAxj0OzkGAESgb2B4sG9A2cj4c54BAmkC75dJVwqAEifqRjIGAMP9JgCftAeAORUGdiFUgJgByygZQiCZ1QK6nfiwJuXEnBd1rU0AHgQBAGfgCULBfgBm2cX4vpjo6YBXNNCu8h88GwAtTstPOMXZxkBYBoJZ2JHIweGDaAfD/335vFQPZThcOsPnvt9O+AKAgYFCLGkDkOiM9gw0BwAeQFMAXGOBuGOKc2pUJSnCQVVZGbivbAAMsXEBYC6PCwBAxQFIgEWBAEBn8K4HgCFcAZ2RSQKAdHBvPEv8BwcHXGewnTMQcvjI4TskOG79ZOc8xdvscOgTfL+Nogx6hvGWkWyg9SP1NatnpUYYMKK8/ar9B5MAoAFG5hUAqDzvGeh3/pkeABAKPODmp3rIWngPEnD1dndh6Z/UxRdkFwHSk2gzPpbJ0SeSV9guLyzaADQ1AoMA0F2Cd8s+DYA1DCMlAQCTT7KDA6cAYKCN4wbgTxI/4yoYsla6EGFxdzTWnTMCU+EA6MyeKH0/AP0FlJkqAOaSmEnRY/4z8z4AGgqA4/C9oOZhypjjhaU+TNWQo2hs194dgrMDIyBgfeQpAFgJAGAgjeka/aV3q8sfC4CBPx8AGtTQ/nMsD0foByE7w2cJB6A/9KiP5ePJ5LQFAAYEGM+/AHDVAQCtAQz6wpcPBmA/CAAuoK2rdoINVo429mwAUB2OeQMlBcDVRTMWCAAetgcAf7vzDoOCDQAGpDEkz63hLArtPH8ABkwA/qwLAAU5bBTE7tnORFePFY9QEISrAI/M+8w/0NvPJ5KyHMYCoL/fDcDCJ0Ys2ANAiy0hhzIt7gQh592qjgYcwTF0lg4iWqxaBG0NYBcGnwqAW+tuDTAwiH1Msi1k9f18PtmZ/ugAVD/Cfe/7nlINlmLkBzVOg646xyArwGqTcMvf/my5xyRrjJGwJQVApwFAvx+A8g6XyIYC4J8Oy8d9F2MBUUdwJGYAdpFacSAB4LCGWTHKClQrpJ0Nwu0AcFMMwdvoDJpL9psADGUJwDx7A+f7E52pwUDx/Y8CULWPy49oiOufTXDFUIG5zAEvAAOu62DA7pNx9L0bgAENQB4AXLEAyLsBGJm/6gZgj+U8gQD4LgBUhu4d4xqosoKEu0KRGERLiTVHnuFEBIWMhKDTIm4D8PQxziNrXcit2645YQQAWyaQDsbCKEnuaKkODWU7MPCouL726j2GhGW7+4bwzvbs/lOdeDSltu9+FJNfIbCnAgBw/XNYNRpTYX8t7oDH3mWpNj+S78MVAAAWuB1oGm5gvN90AdgbeMUBoLQDBY5ZALrM354OGr43sOG0BdUaYgiiFLBGyUvOGKNn6+wPciIBN2wVQCPwU7SGNQfgJgBAUSiaQ/vcAKCLDgC8WlsvT8a7uwqDzgc/DgBV48F1P7r+d1XbPlyyvFdBvl65/hzujsjP8NBA33kBMDDQm8oki8tYETg/iYlBiYFmAGyfFgAnPyT1P2WpDzqReiEFAFoIUBamswFXTw/AyhIAWFmW7mBDMEM5B4DZaLo795EA6NIAVD0ABJ5TiZ7Sl6PW9G3MdbGiC3OEC/x3DA86vshAKACtYLAA6CUAVxYwKJAA9LsAmHYDUJcdomcCAGMErLSQAKDKRw/pBljNQYtBAGBQZDMAbt7ApOBb/8beaBcAfayZneOUuPW5aDadU0nCjwXAnqxTPK+zZ5wG2nErJztI+0W60AyCrD9dv4EgQ6Wp/JsAkGH3BzTAQpEb1jNuAORyMJMB+JF8AITI3DUi8lDKiXckHiTRQFwNCoAK3QAbgOsBANy90wwA/nLnX5wQArnrCwBvYDk7AEALO4ZEKQ1wOgDa++wupoO5nwlP6p7veOUZJOEWByaAcv3TnR1R2v4q6KsBGPSKuE3r1XYPKGaUjgoAywQg1jPsBeCTT8xIUGPvZLd+FgCkE0RKBDd3jmwCcA1UuDzAlRE+LQBLK3d+AAADGoAhAQDpQCQD1t/9tomm584zXAFtEXA6APbOAMAJtH83phxhUliiD1m8AZX5CSCgfflrZdGnAYAGmBcA0BcS7XUB0O8DAOr7jADscsKMRAQ3GA8EARoA6RAMB+BpEAD0BfE/WwOsYUoYBDw0rAAYGhpOI6o5gvbl39Efno4lWAH44VcAv7MHgG4LgHM+lRrmK+HyZxdQHJMdImLINnNJ23dfXRFNhgFG5pcEgEw8mh92Yp39skDaBKDMnaGo5Wq1K9wPh8QEa/tHesAICZBZEnVpDrhilwQ4dcEKgCdPFAB3mgLwYBX5YApoWMt/aKgnHouiP/wdAUh0Jfo+lhFIAKrnK3upmao2KhywBdsfuwlR+Jnus0IUTqzCp6zOggABmC4tLCwgDICawMKwyw3o9wCwg8VCh9wdcgYAYAWwQGhD0kIVjpdCdSjzBBu2ERACwN0WANx8wGSAAkDLf5i745PF92//QCQomU4WTvOUtw+A9AaeNwDqjqgw8AvbP4VSbcT9h4ddwg6OZJ0VgDkbgHjfkAeARRMAFIXtEYDTa4B91Q4IU0DMAM4TYKS4Vtuz6gJdANxzAHhyV3UH+QFgIogELN38dIWjYgmAlv/gaEEA2Hz1B6eFpxMFR4dbAjbfNnV8s3cbJwSAhnECHm7fRwM+FX84pvbnzPhYugOJzZ7BUY+8QyyUMwEAS2+ZACAOFE8OeAAwvEBVEYA+v2AAArYI+fdG0e/f0gNlZIQot9VuWyUB18IBuOUDYGXptgIAbz3AuHgXAEOjAzCaEsXNd3+sl6ej3XFagUPBkvQc1EuFfGRUjgsCA4DGeR3U0KB6V9n+CcxzQ5HW8BA8/z6/uD+cgMGBnqSEARYW5ukEZPAqGQS4wgB6ajwBODg8NQAyPgx9I5gdoapDKnpqTA0x4pYAuNfGuQBYWbkFAN4VUexmyPfCACe5z62vc2UMpu4rAFrL3jn+D5i6QH2CA8DpRCwn9MN4Jo6R9utE3ifegT4ecq3smw+oYAkFANWhAoA4AfHMkAsAvTjQyAfCDfBrANT6cfpjUwDqkgLaZUVIZbPEKaN1aSE82eOciFAAxA1UOsDcHu7aHPJg6fEy9kb2D/UNjQ6JYIbxwMhGvPL6v1cZCYr1tvf4n/JYRmBtv/1jvi7OyyS/ifJHMXVNaX+U/EXjCPx6JD/ke8N9TzVJdHmkP8z/MBKiXwPQGYnlB+hq0r0cGcG6CHqBpgqYf7ez0ziQ1SGG+Hf3OSAGfn2trtT6SZBBoJ/3fRUNwLh5sIAlA4fHNU4NNrKB2gh8qJJBTQBQHgBVwGusjBnppwswPMJnc3RweCiB8VeTpfWfUROUSKcy//MAUEvuVblbTbQ/pvqi5i8dZKQ28VFtAuQYbw4oveUHoJDIohxkgV5gXAEwpABgOGAkiQlBrjsAvSFVArB3bOR7aN/Tr5eO0RYA8BypaAAr3PCN9vZUXWAYAE/DAFixALi98nSZq4OpKAWAYSiCoQy2YuRL7wHAfDLT2zPwUQDQV0AtKBl6GgBO9PxEqfnZKsL2Z8lfujA4OoqfezRE4mGWqha3yULQEVGjWDTOaoCF5bk4Rgj3D0iaYVA3hrAeyGMEVFDTx7E/zr8BMR4UC0P+7WgAuQ10dQjmDp4wglZFd8jihwFwF1OCEAocHbLu7+Gh3iiHxW6uc3NoJpeRhPDwxwEAY/FPmlbKe4Qe9qF9rF9B3H8Qjz8jP1mIf2gw/Pb3uCzGz9UmAFQIPfQClwEAnIDOWJeoBXUFiBsAL9ANAMeEYPbbnguAGiq9uEqyGQBG0QA6gkjAJgdPomTgsLr9dj4MgOcWAT4AVhwAbj1eXn5XREXImAXA6PBoAZPUIgqA2WRvpkAT8H8cgJou+RPtn+qTp79FXLLFsZW/fmG8AAwPIBAMGxDVIHACOmNpvpMahDbAwEg/nQA3AHAEj5gNMv4Jh6z5OeI0yDYAkKsOUwSZFto4QvcoioYqO+8VANeDAXgaAIBOBsivD5du3V1+W+wfwc89PCKuGv7fF5ERAevcHp7IJHLiyH0sAE7OAwC4fpz5EMeym0g8N2rf+OIEBAPgd1jCAAjQCGIDZlAsTACW2RQQyxiMEIA5txdodQfVGw3zX4D6/m3OhZam4FYAnMgW0YpEA7bZJlADAGwPuhYMwPNgAORoDXB/ZeXu8hrcANwBYgEAANybHKeIdJAqDE5koRUGzxuA0VTEBMB70Qcu1/WSAPVRwxsnePx3MAi0M8E5PhnY/o7ZOhzksfodVxHe8IBX6CYh3jtgBCYABmouKC8wHslRKfCbKwT6zWSwFQpiLs8C4MRK9W5JggfTyPb9hcJ+APYgdLWBtFprYGowK4PdjSH37uv28KYA3FY3AKZH3bm1WoIb0KcBgKRHh7tjXQlsj34FAPqTvenB4fM3As8BAOUaUfyq2w/73NDtB4IH2wlXDfsIOB0Aw/lMv8QB5xFKTUajhSEFgNYB/cWVT7wqYMMPwB5m0jLBg/BAvT0AdnexzJgjR7FwpspZUstX3eUApwPgNgEoz/WP9I2O0AAgAEOj2VhXJjm/+W55HanC3u6+dhWAEwjyvDH8MQCgAhDtv4WNwTHsi412QfyD7tvK/OsDH32HAN9NP+B7h+0Z4gO9nAMGAEql2XgymuhTLoRWJa6uEAOAmgeABneJIcFTxwToRjsAHGNQZJXRAAQDqlVUGW9LZfD10wKgrgBqgFu3cNPDbxkd1gBABWCYIneHrq++Bd293egNOA0Azms85HrT+bRzugJk3epJDWPfUPHZKba/mBemqeECwPUThXHgAyDo/TQBevIqDMR1IYk0/tYBfQEMD3Bv6IIPAJRz1PZc/7BjBQCMA+SJXDaA3ybSAGCg0IGYAdwxwmFi5QV3OUA4ALfcANy2AYCiH2AciwDQBhjtwxTF5NwmxoTACOjtTpwOAFdUOCA+PHxeANT2tfbHCC/8UhgVy8+lxD8OABAxTYA8q0FUIDjRM2QAgM0R06UrPgDWEfXDEgATANXki2IRWAMHbQCAQOLJLqdDcJo0Y8u1o/WFxQ8GYN4BgDcAAIikkyPlP16xOyiTig8MtyfuZsdzsZ4LACz6yHIVBDeE0vVTCJhevvtHMNXTh2mAkXyiNzlbWgIAzAXHehFI4cskHy8M43LwA1CueDXAiZoNL9VC2iFuBQDnxLHYnWlBaIDa0SY7RIOuAPECH8m54wMABNy+fQvdInj38mppGmVhIzABR4dV3i7bkUkiFrj6FhlhAJCDeRj4avEr2vnAqHX0H4FaQgCQGXihTXL+FrqGChzAf4Y1dFKVJcHY5BCnp4LHHb94RHw2Wn0Ogv99WDFMG5DDlhNYtjtgAoRxk7AB/QAgho8BYOa/E7ujcaEjUXiyHzo6xgXG8d4+aovreqsIUmmqJkTNCNE7Q3RzaHMAeO7z+ScBy3NICI6MWHJCKAj7dbA/mlvO5pKpjqwhwnM5+HapMwFQq++pMjlL+6c554slfy0UT5NPOAsAQ30ZAFD8AgoAccB0Z2J4hJH0EfnsQs7VGGo0h8B5Pzhw5f7gyBGA/VYA7DsA1AhAVRYL1Y+lQXDRvTjutADcWUZ72MCQCUBfN6ieKy9je2gRoSA6gh8HgNppATjkrbGHQWl6zhMKvrsKo6NDzQEY/pAzEPC+kQJLwue/VCZAqjNjADDEbvL5KwEAbGOFbBAA5SOohf32AMCI0RpXCmDBJKJBx1IaflYAblsaYL2IAclDo84VMJRNofO9VH77VoyARGHUrur4CACEijugUOJkHwU/cP0PJfCLdXBxxv3DbiIPAKPNPvN0TOTTTAQ8QWsVtiok4vkRAiAQ0AdMBpgAOhngLgiBHchCrwo0QK0ZAK5h0XgEtO2AtgrsK1hYdG+PbR8AMQEIQAnNIRTw8MiFUYkI4w5I4g54+w4fQjCw16jr+W8DUK2K9u/i5gdMm+TPHfSEj5rWR+iHz3qGMmkUg7zF0i3agB2JAQcAIEAArgUAIBEfHwCw53bY798EAPe0cO6bVp4AYgHcIso5IR8AwJ27X5Wn8whk4EUZuYCDV2ywIy7zQt/BCIj2ipd7XgBoqZwNAL4YVdH+iS6M/E/khnWlmd8K9T/0o6cFYERL1Cf+kYFkNpMpvl1YWsUDgikK8qnWp0MDFJf8GuATWR7W8ADA9T/oG2sKgKuE6AATRlEKIHYgAggagAANIG7g549Vj7g1KuqB5w4QMO5wJOAANemFEUofOmCIO3JRFEJHMJ5JxweH/Bb96YRuugZ8VzpiZgMDk99uDSjir3HGa2mOkx4w5omRH1QwhP5sFgijfu/kwzTAaCHNRACcgDVGAToKF5QCGBAlwILQQAA2pKbftQoYVT4761ANNetRaDo2RAPAsjj2i22gXbSqOkQDZgQJAI8fOQDcCQHgzgrjPSwJAADWa4dFedwf+nZN7oB4QanZ0TO6A34n8dQA8BXCcCxq/85IWk964OM/7EbM8/eaAIyeGwByA8yV1cqlREfHwJjSAHILwAaYK90IA6B26AIA9hzq/La4M84HwH4YACyNPJBeIVgVqAq76lod+eBTDwB3WgAADYAGwX7RAHyFRAvkEF2PwAhgNDiOovdBS6+e1xWQlgER7QGgUn81Pv7U/lhvzzk/rPmg+N0PeKDmOW8A+hJJaQzHtP3JeALdU7YJgDcIwPK1IACwPc4zKpbixNRnOHQHPg0QVhhV520BANAhsrEBnagAMDSACcDnWv4kIBSAp6/XEArCHS/XP34FB4Mx9AdhedgXy5tMCcf7RofPKRhwNgDw+FdgK9P2x3JzNHuC12ExS5rfS87H3J8kxo51DGUxHKq2zJNDmKQf5YBLXxXziUQkC7GP0IRWADAVGAgAlodh24sHgL0qAcAQyFqLsUHKBtwjAKgilfVyWxtVBgLcGsAB4HmbANx9ffftXH50UP/DBYChbnTVJEu/fUsXAdfceQIwehYAqpUatH+KS1yx5qFP/6Baei3/Nv/xAtC2AcMbIJuWMODVBRbOd0QKLgAkE3QtgADGgus1NwCo60IsD8M/GrVa87FB6vOPjy0AUF5WryIcLHWhi0Zr6BkAeMKysOHBC6MS7xEARnsQZI0X/3iC7aa4A5K5C+cVDdTf5hQAyAXAoo8hTvrA4p7c0GlxU0+9nAv6twteBMJA8HuTg+lsnjfA4lJpNpKIxMQJdDQAbgCMbwsCYIcJvGAA9jwA7IcAsCdLow45KRARwa3tfY6KWrx6w9MbrAF4/lgAeNTcBrh7d/mr0mT/oDaohkXWfbhmI7OYF8myIDTCD+nHhq/ThZAnKeS4X9sR9XY62gIApfjFUbQKvmXlXyw7OG6o99DHV/+dF5w3XYIfGXGBoP9xQZrBVH1y11/I0QeYx2SQ1WIG0zQSQyNCgPIER5gIgGPuJ4D9gZ5RsYcHR+zwLKO+p15rLX/ZG1jjdhkCgP1T2CdzhIoAFITYbSHaBvxb5DUIeCzHsgJ98mcwWBi4y6IQBgP579PPaAJ3QP88hka/wx2Qps01OjqmXhdTa144zRl1XudMtFlNoOqEwKnyVyn4Rtqvs4PNnoEyt7+3/bPZP2qbmj743+P6mBCDFwilAAnpCi1jm1A82iMPvjrsCsF4MPRs+3UAADjC5g8ztMHdMAQAe2kajabJoIAPSKfUoUyMvXbVvAHU+ngC8LQFACodsILK0HVOitHqTgGAQbiJaBEAvGJVSCI7Oma/wP5n5RQI6C9OR1oAIM/BCa7MXWr/cdXsmcgPXwiTW9CPNNoWk6cD4AISQVIPjETQJFooon1MAjgEoBxw8YqcgFAgFj75ANgqwZ07EwA12ISYIogr4HoAALYKuKPnBd7xA6DSASDgbWkE0y/gB2oA4AdwhSjHhq8hH5DPYmXmqE+SZzz+OECI5c8QuS747kbFLxpV8fgPtxbb6Zlsiw+l6S/0Ij82WVK1IAlsHRi044YXxhAQQJ1QCACoC8aiYA8ArAiRCr+zaABsFkGZ+Pz1qzdcNqADwHMNgEIgAAAxA9AixrqwvjxfXgIwzKqKDKxtToz8B7qE0R7QN3bhvI4DQLUWPjmbM9GrO0r7d3eh4DPeR/Nk+LwBaE9DKLsB/x8dxJ5wRIF4A2BCbCzaOyRpQAGAmqB/fkUugAAjAIn/w0MXAFz+slVmJKh+mi45C4CT/UOMj5q/cfWGEwjWBESeyR3AoqBbd7lB7POQfAD2it6+syJ1YX3DOm42yuog9odgdcTaP36GCsj15LA19xxfVxuA/RAAaAAw8oOCbzgMHVFUXSnTzTLh/hwAjFtE/u7h0TyDAMXVJdwA/XFYpX26r1LlAXADcIJvGACVmgsArIapI7Fb4mLJMwBwgMoIALBgAvAgFIA7wQCsAIAHt5ZxB+juAOtFHcLqiDhywv/6BaNCMr29Q2MXRv9cAKzIT2e8K8q/Hlt7ka0YkwfxTwPgggkAXRgsDcJkmOUVmIAYCxBloswFQHE5FIBNBLP2fQDsfAAAJ2IDBANAAuwGQZYGfu4rDldXwAP2h9AP6C8Yr+nQUK4DSeHi+3//8hbJonxPftTSAGOuE/Rahbzb+FAm2hQAFfhlwTciP7FYohDwDU0ArB/F+6ONhR3Pj9TyX6S1DkxAREVgAnLp6hwrEgqqEEQHmAfgA9zRN8CVoEDAwe6BK6yLFk8BQKYAB3QG+Ysl7dcI0cPGIUvKFrxXwN8UABgR8dwB4PNbd4MugfscIQIj4Ak7xPJDo7bfru6AOFrE/rW6WeTWhNFxfQm0fMHGmhx8lJ+SbQaAKviuwPaPdLLdpyfwIQ8AwPeXnRqAFhpgZFR1BMEEXCtm4qifZy2wlTaWliD7BggEoHrgBuBgj/lgRIIOTgmA5JJPZJH8gssIDAXgblMA6Af05wdHjcDNMNZrY17Y5rtXMAOxXrPPenzHPuxcaAmA1MdIuw/crHhmUMvf+upTItfqR2kfACgA9ITqalDcTtgqn2EpuKUCZFHM3TD5q5qgwwPXUPD9PeaD2e5bOxUAqpiAm8cRCTQyAYEAfK4BuGsBoKbH2wDc0bGgfP+g4RuNTfRhSlwcDSLvoAKSWKY9OnZ+R10BFSMAapb+nciUvxSu/1iqoKN5Y3/aCbvOYHZc6E/me6UfAB11MAGjhVH2CY3YxUD0AYLFr0pCGoEArO8c8WoIqILxCd0cjSEAsDvQbwL83QXA5/qXEAD4/s91LMj+Z4+Pjw9hCEp8uvz+3dofpZGefniC5sGFcLaD7zx+oSccAESHKij47kqj2bszOzpuPKSmSP4sApyLi6EAGQ1WLi1xuFYCs6igE4atarDRoQH7BvgkBIB9FwCc9ILdP4wEnR4AWUGNRsHSVaskXAPwNz8A9AI+vxV2BQgAt5ZLI/k+cbLxPwppbCyXRe0jBoatvaIKoCc4fh6vO793j+QCqg4AGKK5x71KJ1r7p7MYWZodsj7fLyIDp/GPSYDzF+ISgPzRELS2sLysggC5EYQmLlhWwCB9gCtNAICx587tY/dDjZOftiUOEAwA0keNA+U9clocxqGc7DMVIHOxYELiBlhUANy2FYAA8Ew7AtoIsJYIrqx43EB11KiQAf57x9XrOjE52N2dRShg/dXbTQ4L6e6bGBsZvzDqfpHOpgJ645wV7Fx83IvHwTd7+0r7p9JpzN++MN7kZm5+mbeU8ClAoDU5yn/2CNIAybn15WVMVchgMIxUg46gMEF+w3gQNARcaXIFrB/V3ckgxoKgxLFOQH8k4MbnWNi6bBmvHe1UsIC+votq8vpJnXPxWBZ+7frSvXs3JRPUDgBuJXDf7hxbLs/3458xpv6pFNLEOLrukogGvnv3bnMumcspK8B57dp9zcc90sfJJTQAltsPfwitkqj5gvaPp1KpRLrvwgTOhbaZOn9FYIIl3Pdjg2Ry/t0yFUC8qzOaG1bOoQVAMiwPYE8M9gOArH6ptNUMANoKGoCNrUMM1kNjFLvJOCRTTIDrN88BgJXlMvvEhy8g2TquXtGLU31dnViMtb7+TlRANttHWY6eWiTqjXE3AFk7GaR6vY/3qlazZ1caa51HJ6bGLnrlP26csPecPwIKgAvjI/meAiwAyP8dFiphoOaw+Aajkgke5iKh4sqVZgAs71QDANhqEwBuits4qWB9IDLBDQzGQqs4R0VeowlwGgDu+3pDVE4YDSIKAEsDQAUgBI/J0eu/veMyceQEx8eaPvXjAcd4r75bJsYncl1Z0QAnrpI/av8uTPiOJ/rwWRNT46DGL1d+yPN3TMgZDz1+ZRRCUthtIRwM9OTQE74uCoAdgQUrP04HcajQh/GgS1eaXQFoDqr7rgACsFF3tsp6TT4NAFtJAQCGYWN+NNaPYboECkM3OCLm+tINDwB/bw6A5QVY9QCiAeAIojRweHiCAOiXZqrQFe+Jo1P83RpmxuV7UrACLrge6xChNz2UldYATs1vpXJY43afTo557BnC/TNGgY5dsLlRXzn+5xw/AGPjo7lsHxTAKswl1EjEE5mRMfFQJUdwYZib4patKGAIAFteAJgQJgCcJh8KAJpBLADKFbQW7EppMfpjEQeev37t2g2XBvjbaQFQZiCCgVABCgDrRRifGsFGxMRIafPt6juYvZlUDq/EhCP3M8k/CIB9xP2l3QdjniJdgxOiNkbxvF4Y8ygRU5U0uw7OG4CRsfHBbG+ePbO0l/FaJAsXxqRIggAgbkb/YPnKlSvN3YCjhhcAhHIQCWoNwLEAgKqifaYU8cFdmRePnhB6AG4AcAW8ePHCTAcw3qPlb60RdKaGqfc/W/0DU0P7Resq+U9PTua7OrPR4ntZJZtPplO50QvjzfR9u6cnTgCqKuvPmi+udY/B88egl9HxsXMX6OkF7zkjY1QA+bnyq1tPWCSFXPnw2MgEE2QjRAT9IPk55oE+aaYC1ApZ+3D5G4O5qAnyAWCDAADEDUTYl00k2IZActgmwWHRVxevLy0pDXBfAFAKgABYGuDp5xYAXg1gbZO8JYGA56tfrc/BDRA1q/zAyckL8c5uTIzCvVcm9p1dQ06A/cMBqGkDQJf8dXah32NwTKmg/4r8QxFAFnIw28Om+dVltlMnuzJ5Xk9yA/AKkIXxdxauNVUBnBLiAoC7v5oCoOoAWS50zKjPOgeOHrKuiKOjdzZKS1ch/yVrZeSDhw//ZgHgXAFPJRLYCoA7ogKK+NdYAEyMA4BLPZFUEkukyqucJIIdMtmJsfMAIMthlCdV/gMxII1T/lJdCKz09I2O6ThUkyvkv4DAhfHhZA8LQSD/dyWsiUtlRibHJsZ0ncDomDQEwSJrDoCskDU1AAFANohFYS0BICrziBrv80Lg120tLy9R/kt6QuCD5gA8DANADMHPH73+YhUJAaoAB4Cxi6NoEsM2afg+aCFEG0x0aOIcABhLR9JiA9Sq0P4o+E6lu1jxOTYhlnorC+JPRwBIFjAaNF98J68De2UyY3ihGCFWZQIDUgmweKWFFbjtKv1qC4B9DpfD4mECsMzK0n3uka+zlGhh6epVij8YgCeMBSsASMCjTx0Abvs0AO6I108BQLF/ckBeX7zKY7gDxsZzkQ52CvMfXupPdMV6Q6+ACfMEOGnG+3VfAPc6U/vHUt2YT5wbnRDrb5Kf6vuGTU/Yj3FGlREAwFAi05vAy/D6CV0AmIADAEASkwRgfJjTYZetMFCoFVja0m3A1ryHQ9kJiEBAIABW0hgAIFAmAMAIQNR0F6MhcHMsIREs8tcAPHz4aRMAHgXEARwA7v7l+ZMf6AmOXBizPK6JicmxC9gjl0jQ+F1eWy9GEmgV9ttobYnEAwCSQfs7+1Lyl+3uRqv36NS4SjFNTk6cFwBBHz81ACR+VLWDvbvFuYpJNEz3Tlq1DaIBhqAAvoJFZkk/NB3kCvigyfNI1kAEA7DvAgCBsm1MJ9/gVICqev51Q8D1YACeOQBIdbDTKH7LlQhAufidO3efPn3+5NkqPUHgPnlxasLy81C8o4YFrFIFwBFI4eMXxsZDDDXzJQ/EQEV2uglATUr+ot3Yu5oYnMIHL7aQ5MWLPpm2I+AAIibl+CEx3tRO4MQ4BIztAAnGRJefwhTCWLhEevSiiiqpuBggmJ5/fHURc5ub6gCmgyhPXf8DANQekOWNo3rogAT5gr3aLubBLFzBuEFZiU0H8KoeEXzj5j0M/XLJ/xsbgOcGADYBt28aIyJwddy68+ju3adPnqze/bk0jX8N/L8JKwiHJUKxDrYI0PxFp2h3Vx9enclgAFwvZ9CTOTk+KQ6GugJku08cHQiF8UvtPMoXJ/5cAMYtF3AwA/n3iyWEfkAso4S1Mm6UnaEYeG516QwAcC0s+oObAMCtEg2uCdQAlLE3bGdrnQantTScU/9u377vBAEIAL3AJ6/DADBnhEB13LqlAIAKeA8VMMK7347CXshFsYFpEioAEbDpZLa7Z3R8anKyxUsdpsEVOlPZjq7IXFENeoknhy5d8qvriY95Jo0TAoCViRyRCwChX7TJIAaEOgCYq6MTZoqLuaD5p+0AsOkFAIfpwM1wAA7lCzBaBJsD56/ML29tb+POwHVjAHCP8g8CQAhwA3DHkw66JeHAO7ASaDRgIgS7H8bHnCj8hWEM4xIrQDUKYqP0+EW+hC0ACHvl5RMx3rezGzNeO+Po9Z24FHBhT/wXATDugHFU+4/1daZwAZSWmQWYxWjCSIF+kjYQJlkoAAtghQBcawHAeqUtAMylYQBAmkD2ZRrIlYXNTXQlcTKUNR9SAYCb/FMXAC+aAAAC7t/0AQAC7q59RRUwMGoAMDpWwAY+5oRWxRXMpNNDuI1NDXqKZ3ZSCJiAYYHLH+s9ollkfWFyeI33JgbfxwTAeo+hARDnzQIA9AJQ/hiZA2aHx42cCB4RFAOvLngUwJXgQIAbAG4KQG9Qab3SDIAaATjhqlhYmQvz8/LXuAFwqsEMAJ69FgI0AGEqQFeG3hIV8Ozbn0uTWHoiUXgrIT6ajskOidUnT94jCJLP5ManJnx3aNuvPF/eHnSfR7FsCQbFxQm6fpNuAJoZ/H8GABOOL3BhvBBPMQa0jEIgNIN0dkZyCFeN6M/AdSkWwMrN9gA4MgBgfLdxwGQA3h/WH08ADhkKYheY9b2vXXMDoFwAJf/XJgCWDfDIOlajqKdDVDTA3ad3v/l2VcYGjlhpX2bBxvuiHZwZVF79TNWHJvouTl2cnlSZ2VZPbAAAE+O5aBQ1lfECU0sT8rqHfcHUxeDjen87f7Pr0yYDz4TnBpBmv3GUR7NFZnVlgXUgACBh1qiOKwvg5dV716mWTQICCoMRx6mxwse63xUAEgsOmw/D2i+UBXFNYHnBlv818wq4p30AKxHwzTetADAJMAFYfvHNUxaI9/fb5X98YzjNsWGIg2KAOOJg/T258UtTcBXGg1T2RZ9kzHdMTsr7etB7HEXkR9K+E175uwQ91Vr+F0Ng8P4QjiPRHgCiAsd7k+wFKZdWlt4VUQjYGSsYAFgKAPGYq6cH4AAAHAoA2+EASPAQAFQNABavLloDQi0b0K4FsQB4EQaAhwCrKEBljZ8/Y33wSP+okewdGxvs7uigHbj2L9QGgY+ewgQImAoTWouDz+5FOn1oCu7ARZV0mgz9RlNBBExNTbX8O5r+/Y7QxZoJBoCvwMREISOVwDAAMFY7is0U6VGjFm5qUobCEIB2NEDJCwCCvALAVisA9vZZ/iPSFwCuXjcBcBoCDABefKEAeG4CYJcF2AQ4VSHUAVgkx/Hh/YNjRtIdr0IHWvRGSut//PEOvaL9uASmLrkACNHTIXKayif7xmZsSZgPbrCsp+QEAOD5QOhxf9aEIf9QANS/fSgja+JXl5/AAoymsJlEDCQnHq5dAMjCK/8mAFgTQggApsUus0M8BABkjA9xDxzucxiIdQEsXl20rwDJBN83TMA3FgAvNADPH/vvABcBNgBSPSCLxPqG7CopAHBxFGlhxMLXf//36s9sFcxlh6cuT14cDwBgynPc79ECGLgwgzenLyLoOKk0uvltwr5FKAD+v8H8oPGG++rQ7FlvewHA8z82nM3C6Cm+gwH4FhZgd2ekR4wDGwFmAUrPmJJbbA3AJ2EAIBLUFACuRq86y8JdNoAbAKUANAAvHAD8l8CtB/7S4KdYJaq3yPSN2fEQ1OZODHZ2dibzxc23X67iEsgUsj2Tlycv2wQYL+1U06M+ZxwVn1OXJse1BILlZn6VS7a+t4MBCP77zc+YdMPgAQDJ3onxvlQuTwNAWgGiWE6TZvqf9QHaRmAa8KulhatXxQS40gqALQGgZgOAHTAtANgXDYBqcBkIdi0EAMMEoPy/i3yBY4eDnz93PMFHTlaIWuCWqzbwjh4gDxVQGGYsX9w9CmwmF0df7HR5fe3736VNJDNwaXr84oT9EIU/h4HX8MUJU/e6j0swYd8r8Ot4/H+/VizmO03l7/9+diB4YiibpQGwurBUKhcTiQ5MhBlXjQIqSsjJoNOlO/QAF/0awEcBknncHbgns8LQ30351+DesSis2kDlL3WCuWBa7ZYjL7sEIOD5ZyLotlkPTAC+UwAoApoBgN5wT3GoHh+tKsTFMZ6G3X7x0sWxpBjDm+++/vV3dAn05zMDU8qBmzwDAGEynDRl8uEAuKxUPwBh30/HwMfGh3p6uRto/c2thXJpoJMjoTgsSwdJoAtkQ5iKAZ0VABb62AAcHKDg29kvzHnimAsMA6ABADYWWgDw2LoB3ACoYNDnwQA8MCfHWwCw+gfbkEUPTk4BgKlLFy+NclwcCHj19Tu2C+czmQtTY+OGuM4PgItnBMB4bwsAmn8/7QJOjPb2sA50c+3XJ7wAUpgJx8Y5p06MAGBaxJIFwJWg460L9gOAdCD2wAGAfQJw4gIAzWPUAAd7XBZvAGAlAjQAogCeBwBgq4C2AUBumGVvWCh74dIEjTS+mKBgBsNRMuiNxwjpd5sAJN+LNgFlxp8RgItNAXB9r6a2XtB39OsBHwBTUyE/ixXkHcNmIAz/Xy//9AuTQEkURw1eHDMbowZUIdASEjNnAQAxHgLAfPD6DmqjsEMHoeF9rwaoSeVs9UgGg/sUwE0LAMMJBAAvX2oAnj0LBsDjDKqdwtoOYOU7RsYMT12amrion6yLlyZ6SQAahV69ekczoDeV0zneswIQhoDnu50CgItuGpsB4PlOzt9u5fkmCume/jyJ/2mVBZGJeKQwMTZpBchAAjbEYVhASd8A7QFQDQBgAxbmNtNBGBiKx91lAxCAAwXA+hkBePIX2wqwAPi8KQCP77BNCJ2CaM8SAPAiTV+eujSWSmWkXfjdK3YK5bOI5OvKj/8hAAKczgAAfJ9kWoC6bCmXY7PH+urPbzkpDTuis6NGEhjZICwIG+GwgOtKAVy5EgLAlRANcCIAoLq3KQB1AQAFQevzbhPghg3AA1cxiHMFKCvgLzjW0MBHj20NoJIC900AtCfw+NGT5TUODRpAncYUbHJogunpS1MzQ0yL5rFWeO2z38qzmoCxcUufNhFXC7fwoi9YMNUKgJAzHfp3tPj7LQToBCI/dXFitJCjB4jtee/K6xyYnOhGeHSSFWviCKAXhKXgqyVGALQNGKIAHC0ADSBt3TIGpU4zgDVBqAhZxvoPdPseeABARTA0AFKBxw0sC5x3wgDXnYXhZiZIRwGoAb5wrAADAKUE3ADcdwGgDIS7LH6GJzAxQwAuXcIrOz11aabQhdIozoz4yxMqxUx3ZnjCevz9APDLpgLetoWFM/WBZ9r+Ttax/j4e16deahoTMAEA1BNjA7leGgCv1uTCQzwwPjihPmdCOsXGR/ulFHxp6bp1A4RdAc49MC8AHCsAagCgJguglgUArhU8ODcAXrpVwF8sAB77ABACFAD3FQF37qA2aBOXwADitRcnlfTwys5MpTrTGbQJrD/5jHtGUB2XGZNiPnHdfc/rpRbCOy/5B5wAAALji24AJvh4qzxYXy8NwPK71Z/eSi1kJlnAZ2sbcYTy50ggXACQPwi4ejYA6gqAMgGoyEYhAHB46AegwTLgKy4AbpijoU4BAAnwAHDfBuA+A8J3Vh5//sVX62wSGJhQN4CIa2piZjCS6kSBIC6BtTWZIx7PSU74otd3/2+f1gB4XUzr6VfR7wE4AHmsTVr9em0dt12mK56ZuITXAozoLNlQH7ZDvUUMGA4ZcjPhAJiWoBuAYwGgLgBsiAbgOHj31JxjB4BlBwC3/B8EAvCyLQBQS+QC4A7rQp5/jozC23VOjLgwYclzeprxu1ykk3nBzbWv18psks7Ec3QGzwzAuUBzMQyAZhiEAkAHYBjLoREBer/61do6q6AyXdHRKR1rUvHx0UFOBFp9LDEAHQhuAsCVZgCoWPCRBqAeAMAhbEAGAgMAuB8EwPcEwCKghQZ4wECgC4C7Lz5//vnLtTI8Adg8fJ3Ehp5mSHg82xFjecT71VeyTKInGc2ppN6ZADgPrRGQ92tHD3gAcJWCDffKSPDN1a9+XBUDINHVd0mCopQ/fkEZGAfCfKUtgPYB2DhyA4CKME6LXS45GiAQgPpZALBugSdPVJOIQ8Ade5GIeUAA3qfKAu48WYUdON2PgggYAePah0ICfzTRIbsk1lfZKDCWQHhk8BLlP66Kd3x2X4ub2PvgtjAaPLbe2W5/NzcKXSX9CfHvRrOyGxj/xDdrmJIJAzBRmLk0ZbmKU5NTY9ggnpx7K315ogIWWwDg9AdjMtheYx8jP+gG1g+oAXYIAPtG63VOBDpw5YIYFtrleBADgOuGDXjf2hT3t9evlfy//f777yNvbPkb0QAlf3zyo0AAbgkAjx5JWvgJBPx7kSkB1P6Mq9dLXqN8FI28qBB89cNXq+9hHiW6OwcmIZFxbzDABsAUiV88lzznVABcCvt+p9AdLgDGxybHhwrZjDSCrn65+gd3JiEGPjaLTx+3CLg0JkNBl1kGctUOAzUBwKkL36lCn6PNY/dQxQEwKw7DQpcRC65j9DN2yjRqB75FQbu7NVcuyCV/BcBjOxHwbTgAujw4FIBbFgCKABSAzbFRiI+2UgAXx8cuokYSWRFull17xQLCZKo7PTQ5fWniFACEvUe/99KpLD3PN7x0qW2kPHVC46ojFjthUhn0Aa4tr2JfynSSTSEXYADaGmBy9qLMhV8jADeuXnVSQddaAvB+5+gQq3/cAFTWBYDdJgAcsHo8GIAHakS8FwCHACMeaEcDmwCgLgEA8NkqDeD+seGxS04lHnTBpQxGeHKO9NrXL9FIgk6BbHwQ2aIpHwCXAuXR8kyFCa2NLz0VAC4MLABGkOToQQFkefnWE1o5aIZLDM3yXwczWHTF9CXpBHi3vHJz6cb165qAVgDIL5wQcEAh7+6eWADAwN9E9gXTQjk58iRUA2w45QAaAPd00L8ZN8CPoQAYVSGhACgCnnyx+lWZnsDw0PiUlT3B6zU2dSGF6pDO/Pz7tW9ukYAEujuHpqYunhcAl87+lc2+rA0AUKQ6nMcwwOgs2sBuSQ1IorOzo3dm9uKl6YvT6kGYnqb855ZXb6ElSxHQPgCrGBiNZ98CoHZQO8LDvSmx4EMB4LgeCsCiJwrgng5KBfB3Lf8wAJ5/3jYAaBhfXVV9IhgcZL1IyA2MTU6NQv5dUaRJ0CnAgFAil82OXgp4Xs8s/zN/abMvawkAbjHY/z1cCFBGDSBSgIlUZ0dHFuUQKF/ThvDkpUkpBP9qBYNZbt5wA9AsH3BFlwVj31cwAHhDJkG4JopbALCFMBgAV0OIG4CXJgDPbQDuhANg3AEYHLe6+hV2x/aj7ElsgHGrqhpJARIwXVpf+2qtTD8pW+gZRpHn/zYAl5oCMM6JCBO5VJbz4DEK6hZTwHEUw6ZHL10SWxF+4DTdXdkMtL58/96N27cdM7AlADxXmA06gBtwsG8BUJXmMMkGEYBjDwA1B4D50wDwzRsg8MYVDtKlQS4CPBDcMTTA549XeX4usTZkjLp9QmsBvFQD7I+Iz5WRGVQE4MEZxj05ZWlh/eul/5nTNCRsaYAL+d5CLjGCOQB3RbVhWUUkMQK3zzH/J/H8T/ajTPzO/Xv3aATYOuBaEwSshNAVDvmB/8+pX7gIavgP/r/OBtVrVQKAOlFzdQIAOEGKoO4AcN1lAngB+O7bMAAkJ8RhEeYl0AwAeILPZHhcf//wGA0A6xqYmJoZiMET4CDh8ru366wOKGR6Ri7pqNz/NQAmxA1EEKCQyRfiKAFbvvsZCiMzKALsjA/AMRg3EtXiAZbf3n3gBaCZDrDSAVe4N6Ym216OJS1YoxdwhHywbI5CKCgYgDqrBuavhQEgJqAFwPctAPjcA4CJgAkA7cCnT+7KNpmRgaEJ2xVAgRjccI6OyXCjSPmtENCfR5XgpcsTU/+bAEy1AICVz6M9iXwyOUD5cxZCPoZ50JHBiyNjUxoAfKoyAEo/Lz9G/ByN+QgFXW8NwCcOAOuVyi62vfBJ1wCIfacBoLjZCOoBoN4EANsGVAB8L/IPAOCZLgsgAo8euwlwZghab1uLRp4+vcUV8qgSvzBlXfHSFzg1wf5OkwBslUn2XfwfvQKaBYguimkzNT7Yyx6gkXnOgqX80RGYiPaxLG5yQhFw8fIlJAJhAPzx5NN7jJ7fuCEAXL1q3AEtNADWhELVHxoA7CoNwCGSGAnL9x8YE4NtANAb7FQDGrtiza5wBQDk/2sIAFoHfN4SAIUAEwMvniHiO4IRohcmLtppF5SKzczkoolMHgS8X/35nSYAlcJT/0cBmBjNIAGURAJw+fUtFsZHMQ02XqBveOmiBgDJgIHhCywSenjvxk0CcP30AMDYq9U9AGwtq1iwBqARAEDVD8CNYAAo/yAAXjAhAAD+wm5BAwC7Z9xjD2gdAEdP+gRQGzDFUAhesUm562emMgmWScxxlrQmoJDDtIeLUzqc879lAYSWiKiiscF0DvKHY7P86BGqovMJTC5hnkvGpiv1j5znCDcDltdu4f4HAMwE4P9tAHDFBoDGHuq/1WyAxv6hXAECADZHYQKgcGHODGfhIIxAmQ7gBkCNh3W3BH0rJgAA+Ou3b+RoZ/CZdSQvdNdZJmf1g8hm0QcmAI/056w+ufsEBnH/wBDdoOmLOuB3aWZmohez8zA6Y3P91U+agJ50YRylQ5fl8Z++zJd8ZsaUxOXLzSV12XtmjHO5neN8H+t3lTxoZhFcxBzQbA8KQMT/g91bgPZPxDGzHmHwCckCM9Y9RfnPll6vsIpC7n81pO26GQ1uSsCVTxa2GO/dRQMIt6Sg9qN+ICVBGBJydEAFwNYQ15AYbIrnpGAbgGvXzS1R1nhYCQN+K2FgHwAmAQLAXzwEBALgqABOjkFSYHgI7tD0pMqj8gGfuZiXXgEWCr97BW8QhmBPT9+4foUvzc4GCfh08v8AAOw3QgFQhS74p4z35QpSAIT7/wmff+yEhPzhF47rDqZxZkTGsRewf/7tirL/BADTC2jqCFrRAM6JUgDULACkIgStIdAAJ00AeK8BuO4BwFYAFgA//nJqAFgFdscDgO0LcHzYOl2BvuFLl6AEaAurJ3vmYk8G9wBqxDbfijfYjzKaXgQEoAEYE7jcDgC+B/ecAGiGnDvHePkC6z+V/DEH5j3sv2RnR7SXfTEaAEaJp9gIyggQ2ulWLABunA4ABgKOqnuUsgkACwK4PLDRDID1IAAeeAD4PhCAl194EPhcjo2AMgYecmDgHQ8AT54KADAEp/tHB0dnWBah7tMZEnCpF0MemTp7//Y/b8vv56eT/b3d6YHJS+rpOgUAxpltB4BZdcIAaGESOEkLvHWhkM31oQV4/d3yyzUkN3j/d0Z7Obti6qKOgo9PIgXMNpD3q7dUCb2hAFwp4VYAbO4cKQMQmz8BwKEFANNEAsCBGwAMkjpkSVgQAM4NoEwArQD8AHjNAAMAa5I4AHgYAAD6xQEADcF5dEIOjs/Y6hQigHQm0lgs1MOY4Fuc9yW0C/TmMv3jkg2+GCTolgDM/jkAqKICmHbDsF1R/1V8//bLL5EAxToozC6M9kzMjEvbuspxIfo5KhGg1bu38PjD/rMAuG4A0IIB7QceaUtfAdCwANhpBsC+C4AbzQH49XQA3G0NwNO7MkR0YHRofOYSxX/ZerhnpjD4WxaqMyrMqyLZ25fryY9dmlGR4VMDMHvuAPjfL7ndackcTAz35HsH8iPz79+++ism4CC1BfsvkVeRT13lghTwxBDljwjBHej/lQ8BYPuo0dAANBwAljkyXgFwEgxA+ZQAOAQo6cvQGDsxZMjfAkBmxroigxoBuQNWVbMQIoJ4KS9fmhTZXsZFcBFTFEHAbGmz/IozhFBCVUBT3RBeNnEEp1s/kM2u9FPbAO2oAKQ1psEOfv4JWQUI839z7e07mYCEf008PqAyH3af0uQU+gDRBvbVyoM7KyuWAei1AQw7ILRHhN1hVRn7htEQ/PV4t8GasGU1QCwMgOMjCwAjDng/AABEARwAvnX8wC+emWaACgcFA+CoABOAN5+tYp2AtItdwms3pV9mCGX2Ul8ijerggSJigmssJcUkOexX6xufmWkrq99cnKEa4EMQUP0El2YmBxC9Yn9H+f2rf6EBCPorGe+OJodnAbiaH8QowfhFlABcQB/w2srD2yt3bn4IAJwRge6wAweAmg3AEVuDAwHAklgsC7gWCMDfmgDwreEHOIlhiQeFAWC4AUoFEICnT7/AiLi3HCWNrABffP1gz8zMTs9OgwCsGcYgDZknu15C1xiKKvPDuAYuzYQr4gDR+XV6kyugGQDNIUCrI5reZiZY/d+PGZDr62/RALZZmk0k4ymsg5tlJ9zUpGo1ZLXgCPT/SOnd8m3a3WcGQHICsjiIyt8C4EADsB0CQKPOaWIuAFwFwT4Afv1QAJy4sAXA3TtIC67i8WaV6MhFvPbWSwkpTM/ODkax7A8hIRCAQjp5kJKorB7Ai3xaAGbPAYDLgc6f+a5pgDkxmktS/pPz2I77DvKfH0n2JtKp9NjM5BSdXVSBTzEKeOniMOWPjZF3b68sGTEAFQY6FQCSEOasSAeAw4OWADQIwPK8vyCYPUFmNRAB+NUE4FszHPiF7Qb+5blKDPsBuOXEhBkmtjTA49dwkFYlL4RyiMlL0yq6cvnSFC2xmZlBNg1mIrPcLvQlGgaKI3GMkUnmL8zYAeGmin7WOe4/zTYzAlvaAgFXg/L9JvH492V7ELbIz5Xev/3XL2K8MKqFAuAZgYV+giQ/Lk2NoQuof/7nZTwaKw85nNsJAoUCEKoC6Ady6SMXwUlaGPO/dtYVAEftA2DL31UMEgLAGwMA3SMQAsAtPwAk4M7j109Wv1hbXZP1olgrM2sBMDMr6mBmPMk1Gsylv0I18dom/EGk1qAELiodgM8UCcyEyn/G+g9/nIHc5T+X/P1eQCsCApiwYoDDuSz2QCZHiuvv3/0MB5Z7IFj/W5iRHxifqmrPJ6cuj3MxPAJAK49QOH/73o1WACw2B2BdzH3uflNtwicagC0BAMUf7nHhkh4+AwA8X75xhwPtO0CtE5GhcKpd+I4xPcx7LFPwNbUAdQDWpU+r1x8IkAREhGawWKIDAfQMDIHVtTcgBeXC+Z5eEMCI8MyZj6kMZsNOyBfYOoUinYbpwgwGelwRvhjO9GQhfz7+7/6BMDaBhQOQHMBnaboZ/+fXcTU4a4CXpXNafADHBtCRIMkIXr/uUgHXQq4ADIam/GHwyxTYk1rdAkCmxDTq+4fmoDB8GirCalW0Bi562oL1aKjHRjWYRIF8AHgqRAGAjI55ynO3aZGokxN4+vg5lgrh4S5RB2BKIBjgf/SnL19GneBYTwSDxOIJFlTj8+ANYKgCMsT5QSqBqanZmcsfBoD7amgTAG0rqEuI0atLU5dnLg5h9hMWgfcXMfXs67erSGOM4PlHVHsYkEzPWs0n+ORLs2P9LAHEpPSnd+9IE+XtFQ8ANywAFlsBwLSwAMDYXkMBcFi3AahJe7AXgEOUBNY5UtoE4J4BgBkFaAnAF14Anpplws0AeIrPJwFMC0xicsAlAiDPyqVZTA1F1K8Q5/YP1oqiqWa5DK3anxxAwXDfKDW/COIDALh8BgDcfuEMBMqH+kI+3duDhq85LEZfffUK3h+vf9X/c5nxARuA2UuTVgJgGVelXrLkA0BFg68ung0A0QAlArAXDoA0BgUC8Dd7ONzpALAJMAAIKhHn7aBMAdgNTywCoAMuQiCXJCoMlwlu89Tli4ORDkyVjudxDSxDV7xbn4dDWOjJoUqA4oASnj01AKbN/0EASEwC6n1sACnLnp4knH80/719q9V/PJXIT8KcmZ2e1OK/zEsDW4FHRij/J3dvPZJ5Oh8MAHYGHJsANM4KgDEi3vEBQgAQBkxf0KoUh4H/2DU/NByAz58/f/Hk7urXQsAYYoJiLElIXfKDeLqGOzlTOBFFXr2MCQLlTfiNQADRlpFJfRufFYDLZwJg2tAAjExPXbyAHyaHHZCz81iAsYYcB4IbuP1THfE+9TXTFgHUBKwAHyn+dvfO0yePHzzQs1Runh2AKw4AdQeAKgfFybzo/QAA0BtaO7JbA90AuFtCWgHw0guALJWyAbjVCoDPsVjsyRcv15bfyiBJ5gUuqdyQWFkIr8+MdrNhIBHBQ7NefvVqbR2PF+5aeFtYDTAze3YNcPlDAJi2wpZTQ7neXow4lNufXS2IWs3B9I+jAHhYfeX0ZcsIxM3G+A+LhJ8/uoX7/8FtE4CbZwfgqHEqAA4EAD0l0KMB/hYGwHffAYBvv7UdwaDEsCJAD49ylwl6SND+4uefMyqMR1sIGB0cU2l/iwNJrwxgxVS0M8roOsJrKBJYL86inGigkCuM8Rb+AC/glABQ8lIOooy/i7OXLvSls+hgSPL2f7v6/Rs6//3RRFc8kexVNqK++0X7X7o0KvGf9dWXd289fKAXrq5oN8ATCuLxAnDtlABgia66AvZdgyIRMuByOdUW4MoF67Zwa0CwB4DvNADfNgMACJwOALaNr/16RzJDQ/kLqAm5yFjPlLo0WSwwnImhbygKJbD5fv3tz+vruAdGgEABZ0xfAZfsXzyi02+cFwCXFQXSLzQ13gfPr4Binzk+/qufPUF3+1wCGwAwALJvXCe5LZDpAmBdFPaB/rb85Mlndx88/O8BgJWB3C44fy0IgL+FAPD990CgHQCefN4GALo+8POnanAANlF8xX6Rkf4Llzg6akpeOP7C63OigKnaXek4U8TrKBddfy+mABDI51FZyjhL8yf99CfMb1Q3wBQi0lOjmG4MaySZnIX4UcmOBVgAk72fnZH4qPzglxxNxlwA5T9X+n35L4+erD69QxVAAG4GAaAJuN4eAJWwK+AgGIB92TAsAFz/EwFgZFCdhw/vsJ1Ix4MwQGwVISFUTuB+7LuAlX/6yWGs5TKKRmdmhjNYMsy5skUgwLMJUwAI9CP5hgG0YY//ucjfHzigeXKBaT80LiQnixhyh/E26vaPQ/6xCBbgXHQBMDt5eXaEDQAwZldffP7476t37zzCC3FfFwPeXPECYBDQhgagG9gIsgECAWgoANbFBmgNwK8WAN+fIwCPlCVIBbD6+nO0DcMbXCUBw30oEJhSV8DUlH66L81eRFVVgqN1aAm8e8eq8fUiERgY6MsXhqdmRGiXPw4ALrdBHMgL0D2FfF8yOlLkqOO3X6/S909GEnz+O5CyQhTD0vzaAWADkHIUsXrt749vPb7LV0UBsLLygQAceQCoWBpAG4EBAMjCoJYA/OgA8KNDQGsA2CvUFICHKBd6pOtDn/7tNctDOCmwNJ0f6RuYuGTNXFKVgqgZmp29wMaaTCbaL/cASkbfbq7PA4G8eASiBqbkky+fNwCm38iI9fgwn/4BmP6TEP/a8pOvvkLkvzgZTcjjnx2bVsUtl5zxM7OzqgEI4YyXb56+fvz3Tx88ukv5QwV4AFjS25tF+gqB1gBUGd0/ouwFgJqlAbAhGIMBzfkA3BeA4iEXAEYq0L0l5PtvdS7411//E/ka53uB4DtxCL9ULqH2BG0I1Pwo5HuZFlBto3d8x0kN35XagKeU/130zyPzDzsA0sQdq58cW5aXsWynh+E1Kt11EvCHhQBKxhK54YlpZQ+qVIFh4TVx98KONvdsI5JPPtGanR4b7gdvA3D1p6n8l598wUjW/Gw8mmHlT3Look76KABm+d+UTADpL/4LtLx5+hTqUT0I4gXKDbDixAFMVxDOYMuUIEYEoPQH9f8yFpKFYSeNqgoEVdW7GvueI+tFsTDIUxBszYay5P8NawG0BlAAfB0GgKs2QDMgBDwKJMAAQNUH4T+0Tz+R9DALahDqZdcQAbjsPMoXJb2akZcepsDv6+t/vBcEuHIIg/iHRycoeBUfnG0r6dMEAHnoqVSMgMHFsf78wADF3z9HDNfWvvvpFX+GZASuH+Sfvzgj3S6SILSCPxL9Q/jn9++/fYmq6OePHz3U8r/vAHCzPQCuNQXgYH9vH70ABgBoBWtgVKgXgH02Dy1YNuA9FQc+EwB2PNAoDrAAeKoBCNIBRsvwUwULywTXlpn06x8eGIUnMCnlG+bTe2k4k1Rnrri5+f79H7//Dp9gHk4hSodxenJ9Fy4qPT3rSv7OnpEAVaMmSOEPiPn295FPiH8eCL79+d2aRKZw+WNndTSewEQD9AXqAIYAwPj2RP+AMv+erL58ggvAAcDSADddweCmAFxrA4BjEwCaAG4AsENAusdwBbgB+NRzAwgAv/oB+M4NgGt6GLfFPrMAcCeGvOeRTYAC4MmTl0z5wMPrHx4cHZua1SbYrAsBij/dG2fT1eb62jvxvkrFaaiBHA6i8gNjTCtZMeIWEb/mDKg6JfIEH24Ylf7q4efVDzvkXz+svfrjfQmRnwh2VmMfdt+Emg6ix85JvhDsKfOv9Mcyrrkvntx9/PjuI0P+CgBXMNiMBQgA100ArrUNwHa1JqMD64deABr7HBBz9Yq+AjwA/N3JBVvy9wCgCPCagtZSKROAx2EA4BJ4dNcggHfAk1UUi8MQQDcItg0r784nFmwITKSyKBVh9G2T6xeXxQOfG4GNyLB8b09hEIWmlj1wdgLEDJglQlMXBguMOQz050X3o+QDNsjq202KP5HsRrg6mihw6AkMgClt+0/pKnd1/a+vr755hljxi6d3FQD2otXzB2AfAJQFgJMgALg+fp8j5d0A3D8lAH9tAsAzBwDPejmzeVinBLQKgN24CgDuvnj1liWAIxifbwBgKoKxHHytdIIIMASDxDomTf+xXqJbiLsA8cFctlAYRj7Jive1BcBlb9JfxH8JHYx9yEDnSACDPqWy8kPfIhwl4sfeh1gkVphQzetTqlJQh/9mZkYHkf2dxIrUtRevQfnrW7eUBrj/8QCACjjSAEiloE8D1GvHh1gdv7DoAeDThy0B+M48cg986SoSNRJDzyU3/LmzVESngUx98OiOtV1UKQGeZ29oVeMaGBsaZX1YgOxmsH05nYgmkxGJwq2/e/X1Z6uv1nkVzNIi7MPzmsv3FfqGx6bNIo4WAOigMtT2jAj/8tTY6BDW2aUh/RwD/iM0/DbRu/4bwhCbiPtA/Bno/ihWf82oWkUV/oUFCMefzuAFyl+u/2fYtcWr7jH+0XcCrwAHA28koCkAaBDfqdZPXEZgXQDY4YJIlQwyZgVzbTCqBivvuTQycDzghwDwhQeAx00BuHPHbhnma6MJePES9eLz8AcvqJRvwJkaSESYJk5E4gzFbL7DEHZoAmFA9ABm0DBElO/DqrKJqZk21P20K4p0CeP7ByH2Qk9PAem+ZFSkv/7+j5/f/utf7969pwMKFZRJpFKxaC8K2makbnzKbnNjyyO6/1H8j8DFz3c5SOO1rpi79REAqB02BWDfBcDJvpSNtgDg25ArwLoFHAC8t4CzWUoAeGxcAQ4AjAVZ44NU09gT0QGfffYEkbKvpESA9cJhxTsX+xKRSASZ4mRUm+Sr0m6EsiEwQJtQGBjo60PWcAj7OKYuhToEly/pIlIR/dT4hdFBfBWUfq7QR72fSM7OATM8+tj2scanH38F6316EokU7v4ZZWhI9FrET6Pxshh/I3nksVeX1TAd9UDcag7ATS8A170AeBHwAXDcHABMjSMA5QXXZID7vskQ35pOQFMAAnUA1ww/f+peL+kG4KEyBTlr3mwb/uzZa8kMoAQQ9eJjk97rWf4AaWLEMIauxTB4JZIU+bxbZS82+kjer5fmYRT2s0lneGRocGiAscKBgeFhcDAV7BlCflPY3DJMYvB1hT6cPKdV8NGf5/eEk/pqld++PD83kkTvGvvXEgUpSrAuGVUnMM1vdvkSe/+h/t+j+xMD0p7oNWt3zh+AkheAPSyPLZUAwB4jga5QME0ABAb3VBzoWuBwKD8A/2kHAA8BAoBrp0ATAG7ZvsAqCXh29yXDq7hkxyYxUmvGV5M1g0nCaCLsy0ZBQFcqGon3kwFmZdCR8w722XvYEUoTZPIDw6ILCpK+wa0wIDDgYGknfxsaoqboo9xF8AX14EOJTOO7lpl8erX29T+g+hF7hJ0BP4ThqGQ0kR9z/2zS5or8FcpEh6X3o1h+t/zNi9d/kf5ZCY9/FADcRuA+AWB3cH03HADZG7zoago7bwBemwA81rXiFgAPHADUELGnd61oEBBY/WL1xx++Wmd2aHzkwviU2zVjBykcromZy6gVwD3Q2dndCUtsGk8qBP8HTDR1NkUTzI3I2JG8dVC+19uDORTG6eFC17x09fETqPTzI3Py5K9T7X/99S+/vP35j01koWlhJDjuGdWqfRc9RqqeIQPLcwKxrIE8B8QsP/vL3/+Cf5YJgGu/3k0/AecEQKWxu7cfAADkXxMArjQFwIkD/YcA/PQDz08/fW1YgzokSAwMBhQA0jusl8yqxtHP6fbh0g+KC+BDj6VxXDkDz548e/nmDYtAUSg0fmF4dMZy6LSVNYN7mw430vKFRBQMMBIXyfOBhX3+DvWDb1+V3zFtTLuQFCTtA4nnlTbIsaakIGLvz6uHXgxIyB7fCNJ/JVkqBBrwozDWMA2rU3X7xnsGJ0LDB4gajdL554AQBDj+8hdeb58/AvMKAGaB798PA4Dzgm/caFIX4DUCS9Ifzragg30MDUZi8IhXwPpRHdWfLBa16sHEBOAyYSwSQBjA1AD37PmQdirwe6sv9D8KgB9//OGHtxYAr169ckKCHl/g2bNnHlNQdYyozqFbQQBQMTx+/NQ+T5599sUXn715svwONRZwBzBg3jAFjMpc2u4TfZA96nA6u+OReH6Szy1vgDJGjpbLShuUy9AFwGAaU7nyyYCD+C4MjjlKnqIvw9VnUyLjUrz2tXcRQblHrEsF/YKsyWlF6fjo0KQ4f8u30P/EmAj+3VCCvPYePjCDQGcD4FoAAGgNOQEA2BIImR8IAJscEycANI5NAHYFgE1sp3QD8KkLAPX82wrAC8DXPgCCCXiuDwlQ3aNNAHAIoDcAAtZ+ePsHLQEsmRgYY0j2skmAtQsA98EFqgHsHkLNgDLbqAikdOTdO1zdvzFx9P79+zJBAAk8c87hH+fnYTVBYWyqr2OkByUKlvBRhYgZH9EuVHtFIvH0aEh2QWIIMxe4/GGSLSKrb75Y9QDw4M8BYIsAoDU0FICKB4D7AsCnHwLAmyBnUFkCLgJCrgCXEuAt8BkfwF/frr6HO9CPbPrQOBT/jDhZAR3fU0PZOLQALmdaZ/H+WQ3B5h8Q5qtX/2Dw7tW7337/DQYi0kib6855v2kdpJeQYfzj9984sfod7wCY/MW5C/FIBFo/htLEaDReGL0U4k3KzzHGyD8e/823a7989RWHIz8R918BYJdE3L9/P5yBVgBcawGAPSeQtR8+AHgFIFkwrwC4YQJgyv97wwf0APBTMABBOuC1RQAvwZYAPLrrALD6GRaMrK0++fvLt0oJTA6PjrE849JlTwRXpYxnZjCaj/V4sAZgo8ej2oYXyf72am3VOmtv367RWXj7To7Nwbuv5V1vX0Fj/AF1sV4WZzKfhPS7Orvw6EcisdzghPoZAuKKdC7HJfI/x9A/GJa/DwA8DgTg/vkDgBFhtV3W+2yVTACOXQDUBIDyQmsAfm0CwNcBAAgBL1wEvH6tEWgBgKMEHAS+AAAvYZG+RIRvXsJCA9QCM4EhXXkkWabfHe1ioDiRQX1eXK6D4jxudTqGtAuWjfP/mjsP97aqbNHDMMybYfjezJ2BISEBBpLLe3yThCSENJLYce+25N6b5CLLJbbcS/71t8oua7ejI9vh3Q3YsmzLB63fWXv1rYDohb4znEQBR7quVnkPINmDuwe3/sdwhhWU+cKDv/z9n1//m8ZU6moRJ4WA8r+Ftz9UK4D1+LJrgnwatGmbqWX2rZD/7wQA5gIuPCPwFFKEx2AkYDXAIzEcqAENwASwJxDfAoxH2NNPi2cI1AcAEiWdQgm0tgIAhYGR3iE0wdFO++prHBEB/VbUcoeGgA/C97f/SaU5YBPifgBHtH8ECVy27tinJ6vQu/3VHqCNxR+/gF8DU/9PeJQN3vuffk4ZRjjp644uFrrLbSKKAJD+v+EkcHT9721CmcDgVFcXzNJtp7QYJgDk7e8DwGXBQVooXRsIQVwXgAO1BUA52P4xHAzP08JP3h8eYDf4+3M9KvyYR8UeYByIugJ+kU1BIhM05YQBIzaA4woOD4fRAAZAyZ+niBgAkgQAACY/2NemlfZIaYyKLr8DBG78C5TwHSQgo6Lvzo2//Rkm8/+R0gV/hbzRRyTDL27+IEw+WC246NFPbBZC59YXf8bsDtT3/ekT3PShxvOTP/3vf33zPVUG3XGHz2CxH3/E7AFW/WHkDzJ/AMAA/h/rMboEwOskAM+vF4BjAcAJAfBeAoAj5bgeyAHgrQeArwEW4etsACIEqAIhC0BHskyMg0LNNjfUZwiAY2Tg4zgE4eAdhvTsjW/qpnfBMfjy75+j5f4pHETyRxg9+Bd4DBiAaJXnh6U9vJRjCNPcUPCf/fWvn/3pUxhQ8in8/Md//cc/v/5GZQmDQVJU80fNf7dgCOh3EED4CcU/PDUwMDgAWQ1IAfAY5aY6ABACdit4GO0RyQvAyYk+P/7kggA4cQE4P6/hHvFAAmCbgpIAlFAlAAALhoEAADtF0BiDbYIAHRNqjleJUJ2gAgA+QR4BwgFkR8G72Q01w08YAdhm4cghnitZB4Rvbvzjc7r9QRv86VMc10+LuKDOc/wHGvk+JQBAVXz62Wd/AE/ijxjo+/TPf4OTbb5XdaF3YmPluHvpR+z4/A7v/p82N8HAbIV3ob2dtn81NKWjicamZgHgGAMPYz0ij3MAsE8AnEPXD5YD4DCwCAAXhzVQET89kDaAUxDqGYEbAoCFEABe00UfAU8DqJgAAxAlwI6QYZsZfEKDTzt9xs5b0gKIwO18ZT0wt/vGP//2l08+/gPtA3/49BNKIX2Cnz41QHxCPHyMXOATn/0N2k2+v6PbQdHoiEwk+oGLDVD8pPyhQGlsaA7Uf+tATzsSoCIAGP70FYALwKsAgKdXBGAV9jcBwEUEgJ9DACZiAGwoABZL+QAoOgAYGwAPmFAEiORQUCqsy8QUAJ3YQo57QVs7eIT9XW1DqAV++OLL72DM3nc/1Evy4zwOzv19+83XUNcDU2jJoNfr4z+oLz7+BGKJ/+vzv/39H3ic1Z1UkSgZAUYPULsH1PtDwgoyByD+8d65+eI0Ov/k/ndzraMC4HUWAPqxA8AvVwAAUgHQLgLDQgiA01PXBoDjYigO9MjGAdMawAJAqQAt/yEKB8MatokhBmBQAqAe29bhLgKgo7kpdsSIOV6KGocIgObmvi60pVp7wYiYnMIp01oL3LydtAXumsjcj1jOf1f3jcFxLZDr54wfZgHoP6gg+vKrG1/f+pYGVIEpmHrNu9zofRc7vtH6h60f/b4vyfGvQqihd2Z6oH0QpK9j4Gj/YwdIU5PnBPAR668SADzlcyMepgF45AGADh9Iev8Y/L6T/bMzHhV8eEBu4MmpswUcHp1jOchjVACPfSfAVgT7ACzYpbMBvNDmZUOAPcLBiC0oBst3aUOgOSgOsjMEOCagHEIuNGy1a6yFEbh16+tb3yTvfnPb6gd3frzqusvzHn7gwnPYXG7CtK//wjLRKhmrUPYJ/7Ptshzm7ds3JgXgEhDEhCUBv4ShAK9J6JEEoLb/niS9D6fIXpzv1/bAt9naq9XgXGkeHiaGhJ3Xzo8OsR5IKQCTCprwy8FKjg3gA2AJGDIEDF8vABwUgh1ALcgTDwwQAuOYnAF78Ifbt27fvptd3H9XPpAFRnJAdF4AqEMACsCwXvDrr27BYNCbkPRZ5YAS5S9g8aiUzkwAXl8CgJ+TAJzt420OU6IJgEMsCiYADiMAwGGSAEBLGgBOBc7XBcAhYFgQUEwAoAhoFABdLdqmvMLeQdC1Q9QcCgh8dxvWd//+72gDUN2K8LuN1ozfpbmDkIT+6usbt7/6M5WhjPN1wWWx9O20pMYBeH4ZAKAq+OAEuwOVBjisYTIQzpE4O4oBcHhu64EyAVgJAVhcXPRVAH/S7uCwyQxGAJCT5VMAMAJubqhNEjDQCtvsULk8sra5eY/Dg7dv3fzm33euPBMgb9sonPR6A0e9fE7tYWMv22DHxEuDzB8AwFMzTUEsA5AUPxuEIQENAYBjwVHGoPwJgBolA2lMJANwISeFQhhI1QPZo8JCALw4oAeAtAJ0cYDVAcXgnGkHgM4OUyLyMjhbzgEAM8QqNkztg2AJ9MAro7IdHpieK0GNDhZpfIF6AOpvv/095P89OX1ggUDI/x5mfNsKk13dreT3k/InAJT8rxeAVEr4J54Lj8JFAE4BgK0n6AWeYY1QAMD784tDAkAMiCQvwAdgJQOAhRAAS4DbMdjTKADPPAC4WJAsgW6cMD0wUARPCybXT4HPvcoNIaQHbn/17fc/REa6NdQS8iM3gsd3j7s/3LpF6T5ILkDGZwzqBXq6CgNcxdbWaobk9KmTFJsyAXjeOABxLwDtfSYADwfB4Q9bWBJ6FgcAkoPUFtQoAIuLjvS9xQBwelDOEOOPaB3pvqE2VSJijhihFiF/lICpEVEEgPyhsL5QIIpaR0Z6QdEMDvZClfYQ5+yJAfALbt36RnX1c41ebhPPSe7RFCjv+3DSAzp96H5QEXILJny7O617wttUH6c9dbDrzZssG4BBcGKCIQC/RL0AAQCcEFnDU4JPcUDE6T6GedAEoH2BQgBOOhieqe1t3X/snhPyxgkDTQXyzwHATADAQABAtwVAnDARtoswAKZKgN7RTiIAGWiFVx/sbQfnY2hkZGO+VNmmZgBg4EuYNwn5oq+/J3vgEj2Bd+PDIu58e+sWHfP9BRUZrG6u9baOTBZsvkoZKSbvzeJ/++bN7wEAHCBPAIAhCHOBwQmE8RCYDD48f+8GgRmA/fcUB8oCYLphAGYDAIr1ALAExFuIbR05lhMCADhZlJVAAV63l9bI9PTk9NxQryrb+w6aN3EU878gsHP7uzuXHQsgQIA2L2gXgB4lDPZjnREVnfb2rsxPTxb03d/WZwDAvb+DDlIVt/+HAOCRdQLgxBAAAGfAw5iQg/0aVgTCrHg8RS4CwAEAgHEg57DANyEA8x4ApdKiXg4BbAlEACACWA9YANhFlrtAh5ovLprHdF4ACsfpK5onR7Hhvk6uLyl0t8OgcQq7K+egF9OFyMCPODLky5twIhOGCr9vVP4Y5tG7wPdwzhu+Cld5fYFl4lAoBPNp4DSowWK/Uf7g2HR1uw3xdN0o9w8HgELgEZgAe0dwZuDhPlYGnx0qAPbw2FA+KvbUaQ49QgBa7kdPjJ/UXWGyIyAPADM8OCIPAN0+AMZeEksDoAhosvkhJKCvS0cGewdo3jjLASp8xlQdFyZ5b966jXv2LYjt321oC0AIQPj8y7dv04v9H6wu24Rof2vr3AglfDDka7R/l+2GFq3QKPYPDgAfHHkK1YCQBGAA9naoIvQ4DsApFIy8IwDEgeERAFYCAAwB0CIQaIDZGX9wAFkBxRCANmsJdmkEogBw/6APABDQp0PD8AHPHSEAXnZz7hjSRS2kCaAAHGpJcd3C5KEa1n83M3UEQ6rxp7Fz6CYZfNAw8AMKHwrNN9cgywMTsuCf4YFeb/NX8vcAoCNTrgzA42wAnoAJcAr9IIcQBjo+P6sBAFsYBUgBAMdL16AcxGkMDwCYDQFYwsVmwBzWhkgIDAcqL+DXCfZwiFTnSG2GmN871S+gE2cvHItANRYp57FTWwIF9evtIkfQiiPMRkYWxsY2qmPUD/LDF+wd8Prya2jYuK3WN3B+Dx7fBIufgEITaBT7Ui1qLMQ9///eG69uVsaXFsZGF+ZLZHhgLIL+XxAALnt0W6CceflmJMKLDAhe+WUhcT/wkb8oCrBb24csEMQB9w/Oj2pkA8JsgEN9Wrg9N5zrQS4OzjQAv/3m2IDaApi2s4HKZZZ/xQBAhmAIgEJAEhAMlpcAyCVOHHxpYgAeAOwKdHB2gABQIEkAMBgH5SODqBWghXtTVfjdNLU/N2lLp62B140b8uGNL+xPUncQtgWM93JV0sgI/H8p8bdSxJfSfih9CYA3DuM6AfA7xH4G+f8MHWB7h1AKDpWeOAQeAXi3SqEBknsEAKgI3HrywAKAXWFeV6AxAcrldV8DEAJJAHRiyAVAI4AEOKJXgwUFAf48EQsA+QKiechJEHgLxYWj+yFOhO7BPar4+/GmJUEsOuePT/sDQLhqkLpEVrE5hEwLZWXg5YPZqQBA9rrbdN27HpDc7F3+M+3PNAjA09gW4GWEf/4PAoAOPwEAKgAB2NuD8+AgNOB0BDoAYJzggdgBHBvQ9QF8AMYNAIDAaEIFDDtTxGSFCKkAOU5OGQSCgKZgokyTAYCay3wEumMAjMy1DkxB3Lh1cKR3rLyk28Pu/eQ3BbntQUrw1A1Mr1MsdBb6Kc/TyuelkvQx5uvu/freD+RvHdrLAyCNgGALeIAxfwLgkACAah+0AfHJDAC271kTgE6KcnPBaQCWbTQgBEAjIHIC/uECKltmAbChUxMUananiuiI8DO7TbgIiECzlv/ASBHmT0+CxTYyB1FD6BSFng/bArSpgKDlNAdtVtbKYxRfGJmbLvZ0TsJU10ngYHqgFS9VOf52HqaVvgKgieNXsaYXKgpJmYNZZuDjLADuP4HD4aAhFOaBwjxAGAgFGmDnHh0ZmAAAuoWhZPCBTAQqACaDTFA5AGD5wwDQlwMAQYDiIDAmyNmg0HxxBCgABdA7Am3mIM2VFdi0hqi6GAOIY2NLS2trq+sw9QP+rVTWy+Dcjy3CYOz56dlpKIfBgbgDPW1t0wM0xRRykPDKkwiDufv7PACaFABmy78uADI1wP2W3b0zaAiGo0Bh9hd8ONoDJwBMgNrBQRIAiANFAJicDBIBEQBwLS2ExQEkfpEaHuTDBYIDZtrD9GCbHC/pGFJaqb4QX6qlZ4wqIeggMaVlrCmg10B/PzSpGMetm81FrjgGm3FkBHQFL7riyYF2b0/RPU5twol173/3spusN4Mf2dHNjAlcagv4z31w+M/OTvDMcDoxDvaAPbi/t7Ac7MCfEKtHxJy9w0BgNAw86dSDGx9go1qVNoCJCacA4MzgtEtAsSeaIfYA6HDfSZVVESUCloBOsYR4WpmDHkUAqB6UP6weztniH4UAhZY4SX1afWYAoBupZ6C7Vewp3TEAKO3DF2NLnRMA1NMAlwTg0X2yAdHVP8Gj4iETCJPCW+7t7NVC8QsAWh7qUoBX9qCosCB8Rcs/BGAxG4AZA4BAYCCVIL4kAA4BMc8Aysnb+S9yaXK7WQNszLXiZm+XbnIvOBdpAeijnLSOYzYzAFY1ZQDQdE0AeG7Ao5/vo7mHOb/Tk5OTCzwyEACAVDAAcJoA4Bx7hx8+llHgBgAYzwGAnifsA1BMlQgkATBRwggAvg4wEQINAI5l1CYi/0Udi3L2CCl+5a/09/QjH6q5VR6HZGLYfNubMPabt578LwlAPRvgsQwCEABPsCIYAYCzQBEAOg1sG4LD+5F73wLw9LFNA7kuwNTszGyQChQAGARCALQVqCdKTxsCmAJnnKR76GAWAB3yfXQBeKkL7xUKlCowAHDImIRnRpgqK3RANa3C9jBYpC0ABtL0Ukn74ABuHyrU2w6NDTDg1QYv1M2vPVMj2iCbEQHgRZMEwKchHwB2D+BDhcEGPDuCMjAAYH//PQOwheWAdFKAEwiyANS2nvymAXjz1p0MxDaAL/8YAIv1AcB/iIBpc+y8kXsCgLYIAM1pAF5qAPRptKADDACd3WY30MYmAtBfKPT3F/ppR+eSFbq2IlmLSvL4SBGAVUiWgDgAb/IA8OI6APC3ADo4Ggt+GAA4DAqjPDt7R7XTYy8UbKtC9wCAny8HAPoAi4vsCywTA6PwbxAV5KCg2AZI/oNiOaMkpCeHtpVODnXIZcWM43YREpMeUuofrTJbPypTDq7yp2+2WmOBvtfdHokndZvXYBOwoCr9pNHfrM3AMK3l+IBegMvpFImXhj+tUxWIZb1Y9/FeFfpA6geSAod7LTgh8vR9ags4Pdzdvv8bmwDPTUOQ7QixPoAGoFKtSgAMAUYJJADAsCCcMUObAXSPYg0Xr14BQM9lAXj5wgNANBDIYLMTIYqudmkeSlTUlRX01l/QIb8MADquEYCn4ZgIuQcoAMTh8BdQDwQm/lYNQUjbABAGAACeOhPCYwBsOACUyyYOYDSAImA+kRdQHoEYJBJqAHvOhKkWMM5Vk1TvAQduUMgrIPeWlHeCkW4XAJG26hOTrgJ/PwCgQ3v98WWQiHUL+eNCIgA8NibAAyDgPlYDSQBgAtQOnhq+j2cCxAF4D20jDzUAvgsQ3QCq9QCIFIkNqd7BmQQB4WBJAYD2r2MAZCPQdpnlAuBhApfDws8NQEdTR5qAF/UBiGmAXzwAYCvAk2Uf38eR8BIAMAR2sBoMiwMTAFxAOcivv4XHhKUtAAFAOQFAyMGQHSUWQ8DpHfLrxjUAjsTlZtCRpQUapqA7llbU3+oj44I+duYEQG0EkgLzpA/Am0wC/JwwAvDYAvDg4X20AaV6398/34JDxPAs8YMEAIdQDvKrf07cpDwmKLQAGQBAAP7LCYAuFp0NABgOAND9493iqImONABpNdDZaTqy3SBBHgAii+969bEzC4Am32rhxz4ALwIAHD1Qr0PQAKAYgECwB8Dp/uEWxAZhDtBxSgNgGAABeOr1BHs7QDkKgJE8L+kLpACYNfOFh6UWkFliMUNAFozpolHv5upwaUhqgpfJvSKPWggC/WG4P0sDeIv/Z5yYYTQqlDQChC9orcDHDzET4PT9HR/vtuxAgfB7+axckAsCL5C2AIgCv37rHhc/a5qCywKAigGg7AAAIQHpC8Aao9UYAAMRANrcotEAgI4rAdD2/xsAERZ8cwUAHj2mTICrAd6/23p3eOy2g7oAQFMAAPBbEoBA/iEAakFQaFnkhzM2AvYLVMiVAi6D/jAZ79xBYXjzagCAOqsz76on+qaO3Ithdn75TZKAxNDAkID792EQ0PmB1AAIQO3wNAsAOC3oyVM1FCIyFWIl2AEq1TQAqAMuDUDRGyjlnDvoExAC0NUAAFKSnD3MkjxTwj8N79B1AdB0NQB+sZWhP6tMELaAnp9K+Z6+38ODhAGA0yQAu6tPfjMAyDTATDQIWMkGYDHUAXkAGJkKRorFAdAMBAB0XQWAQiEq9pcmvSABSPr0VwHgRbxnKAnAQwPAY5UHePTg4b3VdzXHxYcvzs5qJ5kAQKToCXmBr56JRPCUAqAUKoAQgDI/1gAs5wRgRgDQO8wBeJN/0wCYHkuMvGUA0BUDoDnP4qRhRPQCAKP2314PAGGQuCkOwKvU4FgPgJ8fgQ9wf/Pd7t6+FO8RDICpkROYBOCIAKCDIp+58p+aieSBtfyrH62tUTFQ2S6pDZa0YwD9I/E5MsNyplCvKBjzTAERFehXqSLRUWYMqvrmIYuWxev9XGfMp+zQhHRkGn7yvu7Ssy6azfQzu2eExqG1Gpt0/yADIJrjXz/nWn0UunOGGH3GSk62Av5zH3xAkP/ePp4SdHAMHcAXeDTIAbaEnmJ26L27NdC54tBAhNMjXvjdIFNaARgncN2T/yYBUB4vJwAwrkEagOFhCUAxAUC/DAp0WwIMAH2ZALzMBUBHJgCZlr/8vS4LQLOqEWiemMgDQLMA4A0BYE6SeY4EPP3NP0CIGXj4+MFjXQnyZAvED1kfOBjqEEcCn+JsmP2TGlSHnvsAcMkYJI0PDgGAJwAAlKvbKLCWvwFgIwrAmlb+UQCWowAIDHwVYFcxCwAVIy58EADkFtCRZ//oiADAKgCuSquABgHwBsjR4EgYHPxrEAhCAh48xP0fQkAtO7u7e4dHF0dHRzWoCoOzIPdPD+BBbbcWugAEwIUCYOtJMwDwrMk9IYY2gKAlVLkACoA1F4DoLhAFINJAJuTfKxWBnCwpV0FYhX2ZN3IagBCEJABu5YEnzz4JgKoOsGUCE84vyMv2XsgHgCYHvn5OiuBXWk/FzU8mAMj/IaQAHij1f3BUOzpnAI6gHgC6As92t3bOKT10EQXgBMeHEABNHW4WYCaYDGijgPkAWC4FBaMhBlYFsDNAH+VW0C9XQwD4aaMYAC/zA9CUBMD8fWWSGDnHfqEOAG99AF49kwDw1FA2BfkoYSDgIdz+oP4h5Qu3/2ENbmqYB0fi39uFXPBe7ZAnwfg2AGmAs7Pd7SfNr16h/L0g8Ox8XQDK5UwClkMAwrpBhwD2CaJ1g16OoL9QyAYgcP30PR8+6wPAFOQCwBMu26R9fRMTfGETlwPgjQBAFAcwBAQC2QK8Czy59wRufwAAdv+LowMcDQHbAMl/b6cF60HPg9FwEoCz7ScTb500cHQ6uCt/BYCDwHK4gprhURkYlibAkHQJvQYSbRO29wg9gKPXzZIYuLMmIqL2n9Fo0DNu4C8S4q1DgFldRsa8sdYDQFmTcoYUPYDoDDqCTABpAjAHHj78jRB4+vRXuPu3d/ZAjKD8YfQPuAC1Gsj/eH9vd3d3CwJD2BEQAYC6BvAskd0dBOD1hJ0MzM0gsZkAHx6AkSwAevqzATCVYNkA+HvDS2kwUGv6tQLQlw+AjhAAFvqbN69UJOC3578iAK+f0/DoX5+8wGNuVreg6wsLfmgqDHT6HdUO4MtTkP877ApHy/DgJAXAAZSMwuE4E28njAFoWsKnvdGgdQFwOOC8wHJQNC6zQzEAiIFiMYKABYBXHgBy+AaBxWi+q7u7Mo3AJAP2sq4GwGs6O+Q5HxiAh4gAAg8fwn90fuHWDgx/w15QaAI7w9lwMPUTzwmDmQC7O6skfxgYlgIAhsNALrjlSXfzq4lmIf8BFP+0GQ5cvgIA4AuMKwAWAwAcDobFKvprgGvI+6ML3umCYxLWAyCSUe6I/YJb71df9vQWGukbABgBvRXkAUCXCb54/eLZc20A/qpOjXhI51tBE+vW1juSPt74dBQUPjo8grMBa2dg/e3gYChIBIANeHJwcnrqRQIxNgBzRGALaHmCBRKFycJEPwp/Et5vPiOwFIaBrQ2YA4Bxt1Ak3j7mASB6yKaLgSLoTxFQCAhoLCrb0ZcEoPlyAPRfCQAdIH72AsWPcxKfvewsFHFSQ3kNj7Ta2gGv7+wMLf9j7ANFadLgFwgBHZH8YTDc9rs9nA99jirgJAYA2AA4PmRsZX5lHoacDah+jeIUBQD0BhB3AvIBUE4AMBZGhVR1EH2WABgQ0gD09//PA4AJ6MoGYCIOwNu3XOsMQofdeG6eRbCygZ+rJH0c+kl7Pp0MjAbAIQJwdITdAAcgVDoo9B3yAV/D4ZFwPnhMAxyfgqe4tUpyxb8A/9Giig3QACW/EiAEICRgSdWL00f1lZkkQhvB2BjvBZ4eYA0wwwaIBiBoHnDOHlFR4TYPBo8HI2TYgeOFGn2dndnyV/u32cWNt0ED77v4b+Kfpr/fRpdhL4CvB3DoUrMtzSI0Xr+AHajpLf1RcvpfQifK6MIS6d4q/MMLhDG/XiWjD8R/AseDn2IPEMjx5AhGApzgYFD0/uD+h6FQGBjGOREAwLGqBgQNQfsEbP/H4AHS4Gg4K2Rrdb0CC6N85m+tQ+S3UlZVfxgFXpcAwIGqDEC5nARAf7Y95EwAqQEHgDlepnfEBaDohIhlN5FwCNr4dHLvaMo+r4TAqyhpsP6DJM8f5cvJgRT2kjylJE1W2GYLOGVi0hLw9llHAb+FIw/a2gdnl8trFTyqFN9rFM0aflgnEVRWt3ZR92N4/wL/4wWzwA7RrQe3rnZGtz8Ghs+OjuQYgON93CHIUASNADEDdA+PzgGAig7w27XJCy5gHW/z9XX+++JbSQBY9CkAxsetOxADAArFpqaGnQNHAgAGLAAyMpQNQFBS1PCSFaF5AVCqgQDgC/YA0D/w8iW80uDsCr/J9M6zUCpqbaAM1lZbdvbQr4N47/kxTP5U66R2iDmAI/AEznbgtMuWLTT/D4+cORC8DqA6GDqHCQA4QAgOi4kCYOS8qUgEEtbX1iv0PH3vo3V/C1iTCMQBWFxY1AT4AMjDR80aHI4CQAMn/eiwC4CLQafvJFwWAO9lqPM0zFOonQg2fANAvwCA5sxYAiYHaLzhWFlvsJsKgapHwMb6aguG/CHOe4QpP9jdMe93CBMhDzD4g9b/uy0w/regRRyHhcKscAEAan47FQR2EOAI3MDd7SgAm+7aZnWAP6k1AKgmzwZYj+gBv3KU2kljANgRwzOyd2DYA4B9wqKTJkgDYDAohGZioxhYoQv594cAOKaIBUCvSbEKfWqMSe/YxqYW+6a6x3wAYJOGCTao2fco1ndwsG8WTAKAkM7ZLol/G2zE2j7aB4e1EwsA/AAGik7QGIAU0OHFKRqQCoCUAqAT1LftaerVSrWivhcBwN0PfACYAGooXkwDMOsjEAIgx02KmpFYytCtIHAL/BsEQBh0soUoCUCeBQCA+HuXNoIbbpNRsADwp1WK/Ozhqh2eHdqFk752d7ZQ+e/s7WFkiPJCDgBnaDySEjjHNPEFnCYG8aMoAOLO99dm9dIALDMASwtRABQGLgDDMykA3FNosgHo7g4ACA2FcLVFnzUTIljylwOg22iJztaRDVaw7o4rHgpNsAExgC2KASACEAJGoRIPe3z3b72DowIO4LhAcPOOzs/VbFgGoMbEoOVwCAdFXaDLeAoFYatJALZjS5kAxg2MWYHCF/CTBLwDqOAwtw04AHgnUIrwoA9AcCytZKE/JflwMG1yFWLWBK4IXMIspTETVNSolntckv4JpQGKcxuxe98TvkZgBeMAEATc3gIMYL2jhY+2+O7HQgB9LtSJNxIY6sTO3xMD8BDu/iMYJA8TpHZWkwbgdpwAVwOkCEhkicb1SAmbGpgPAIgdPxYBoJgBgLQO5AS6AIDot/15g93d4VP8F3piy70kFwAWPVswhY7uFfVOh/d/DIB18ANxjcuZhupxC6cFjqjSgwdEeDOhqUSgdoZpYogQ4QhRMBp3tjfT5l9CBWgAKmv5EMgAAAmYn08DMNQrVUBOAKIFJO4tL+ZROQeaXxkANRFfXJh3kWy9shE7Odk2V3XlH5W82AUwEEwKdpXeyXFWor3jLTgIDuxA8A8POTCMALx3jgXY34edYmdnl22IGqQLdra2V1cblL8EAMMT62tr+ThQuSE9WsykhxbnBQHZSaLBWJbIO6e+O1FBdI1L/gX3tg9W+DRfLV/y5GR/7wa8o9vBHpBEAABYh5jgPLhcFKXHwO383OL46ru9g+OLI6z9QAAuUAOcCr8PPEaYCo9eYgsmElD2IHxUIesbUvBVx/PLBcB6wwDoKfPqYwqA0RwAFEMA5P3oTPnoSUjT/ZxD9D1JvS/krILXgfciPdnJ/p7esYqSv78NROVPH1DsEHXhsH0JMVhf3YJ6cMgCnJ0fH9GpUBTzJb9PAwCzYqD2o2V8dXyVF+gP1CcJx39zOwGA/jYD0IAKUHc/LguAMgYXpCW44BDgxIWsMzAYUQT++aQ+AD1ZADj9Byn10ZO979uLMeXNdK00dMxRWXy1U7AXtLaWq5uBDrAQeJ4AfcLY/BqEZvFt2Rjx0gAADalJREFUxY/gHEBRENz0kPQ72mcT8FyZ/GAJwrGAcHDYGWz3LatrqDfWx8chxo95XmH/O1Gf7eQyP/PRWuZyvQGbEeKdiyeL4fnz89RJrr3BeaEHhuRAgeHhYZ8DCcCg32Tck1gRj8ysdjO42LnRpUpp70kuLf8wd4XHGvYa1TAlLhtLb4qDrb1z5Wos7mYE4/qCpHZxnPG6WRgg2sNz4mA65CFHhyk6AAAc8Lzod7jdj69WN/S8Jz3210QBszd+xwFw3cBsABwCljQAHBMozYsCAZK/WkTEkO0nbwAA96jqkISIUx6eaZsEIEv8IQCaxqKeOUm59ikJAH4cGcRQ4Fo1Fg1S+3IQFVzn/IwFYBuDAxQW8BaWhpH0wd2vVtfLG87U30D8WQi4V5UXAJeAJQ2AXzA8ZsWvAHAGCvhRgQwA3AiBj0AdACLPd/fk2fMtAM7MA14j4lKx4NISQHXPAzyQcGick4DBW+0pAF4OAGubVCVCa8dZ8MT2Nm35ZD2sb7irUs0l/xiWuQFwUTAAKAb0iRML8/MSgIUsAJzJw7QkADJ6bLtNM7aFOqu7ruBpaWkPOkoKa1uGucBlapr6bYanLAJTk6gDYGfBM0+5NHIRUkJsgW1HTUJBgFzwndXYUqcfwIuArVCpVnW2f112eVU3g9SPK3xdA1BtAIC1RKKY5c8+gTUGSmD6oVVYCqvGEIgAAHdLGDblRPhoeNj2mSSjBSlZXhYAoYycC5OUTomltQESQC/Qzq5DsRenkvUuwfZcdeRfqXIepspKu+IuECgbBvA9KO9Y5z3CWnig/SnPvFFmtU/lH/h1wgNgBOTfV48+AADL3D42J0+ilgDMijVjAJhJA9Ar1qAXMh7IkmVPHID2xO8kPFIXgOE6AExhCe5UkeKDPSpIAMzxdNoVPJlvaSN6/6PQK5V1nSgsr6sKTri95Ru+hjs/wrNe3ayaoh4DQMoFrDBvnIaq6jRlQ1tAjABZGzCuV5ntwZLbQRRtJBwNcsUxDEIAGIIi7wlSjJ7HqB+2KqWeWn69qvO1RlGdlDQ9PRyRv6JAqhCdKDZ2Qj/mE+CqoF1qdE7UZ1eV7Jy9HN5dU8DJZoHNImspi7I+EOuGH2OgJ1iRrKkEZAYl29sfebtQHgQMAEb8OF1iKTxzQOeJvMLBoTwA0ENf/lo0WfahdRuuCABfAXS/8wnqKfmbRQDYKoGieDzp5jOmR0eGRkdKJSrX3Gh0kZQdelZILPR6KnmnbYSKpyV8M2GbA0GN64BllwCvgyhCgDtTJBOA4WjIyL03iwkbTgRtLw0ACVpfQa8KYs3M1BG/QmCySB05k8GCBPTklKoiiuQsyV8psg8NzRyzOqyqDWrs8MUSb/oCnqejNIeGpnUbDnTpUwvAItf0iEEgLgCbUQAaJqC8XCo5OSHnC4cDHwCeO++7BCESsZ4Ckn9vMIfAjdfYAG5iaa7ky8vnhy0AMzMqigmXNTsrZS0h9UyConnklA1B2WhBte1wUTn2tbzsoCMom/Rx6uHqeKn6X7AevQCKZEC9Lv6PTnPrnyKE678xqlwOlUZC/tcNwHIAwEISAN8grAcAf5kEIBVQ8IRvFUs9AOhi4HI5ijmbAgCXIMDRXjpOMFXEu3+ir6PPYKB7DN6axY/xNN2uPmgsKOAspUKfLknVIFG/F5Vcc901t3+DhyUQcNsAZSNY6CNcAwDjCoCAgBgAQ3kBGI4AMG0B6M0CYDop+nwAiEtCAMhshWsdDgCQ16oIEH8ENhB7YVQ92meKyEGUE2IJDLBiHZ+CJi8qOZ0wsqdf6x8wnb+m+58bQFbEynP7SwAcAqJAUKkYLZEOZCfABoZkK6nxB+OeQLSvTDLAD4xcTY+BEyEajNadRyrRNQDy1TyytJs3rOSulr1Cx3mZ9RY+g89POyGDwSlvA0NzUFSaQpU5CFVT0Ge7T/smvGVtS8Og/rPc/acGgfKSw+AqKfFznOgjU6IgAFjLAoAQEBPlxmV02BkoYbSAELv+MgrArCBgSJMgLDJ1Xw1LXe4bjPoHPH0fiehINKSGGAqlb52X2YwVAiDjxRoBtx+yUJjIsYQtYeSPF8PzX7TeJwBE+4+KFmXd/wIAI/ZKahMQXykAVAig7AMQ7gPBYJE4ALaWNATATSGkAYh5EtGQXsr1SMgfNoLZ7BUFwEdg0kthuFtBIHhH+Fb8s4bFean6y2XP/LNRxBwAVLIAqB8ckgAsR4zBTABkytBsvuakSr0hyFaTQLgJ+TuQpACYcQyRuPj58tSGewUAJn0AJtnG0x1msdvek7/S/WT3zUvp+9b/RqWaDwC7C1QqlwIAGeDosJgrUkpAkASAA8ajAgC56c5kxY4zYwmxp+1L8cvavzSaWjLRJQWvv9JbhHmlMGpMscx+2SdtW4yyl32l2Sn9Z727PwgZVbLk79gA6fxUwwCU/fFipQYACIpJpdEVCRzlAyAuf5GTYkf/CgDM5weAEBDlRMXJHMK38jdWiBz9EFP+WRvAdgwAbyNoEIBxt4GQJw9LDRA5l5QfRFStU0nW4BoW24X3IPKjHgzqvY1ezazOaKZWCMbsdLCCTWFAMjGVeeez4TdDQ18cn09Z/xtp/R8iYNYlAQAExsfrAlDKA8DoNQPgP6oLwEwuAFStA3x7zhX83NxVAHDWFN3sSuah/JUuvAYAtrMAqB8TML5AqA50mpC3gIAAuQ14KCS3Atctr4fEcPBwOPWjQ8GKy91cLYfhZf+DjNbnAoAJmBbnbiZ5kKI3+0pk7w/1P8b/rf+XNP9CACKWQD6VEFSNc2ywFBIgMLBBgWTqWIlktgEAGlhS8PUAWJAAzKflH9siWNZzgoCkTuBHYZbJCT758t/Y8MVfESUA1UzxBwBULwWAQMA6gQxAabEkVsQtHM2qHfhdABjNWvJSJQDzOi3XKADTHgD1c4wq6Cc2mpWVVNhXA5CeEODXCn4U7VxqGACDwJI7YdxbydhQunqEnMNUMUFi+d3pxol01mieFQVg3gIwnxeA+bm5FAB1UJjhU//Y7Ic1X8qSvweAJ/pcAPiVapkIZE2WiY8cVZ0kZicQqQG/iET9m0tSozyrcih71X2JUdcmEQJ2hS2x5m9HOHCZ8CPJjGEEg5SHobw++CTfbz0Myh8Bmef2TwBQvRIA5ToALC7EI4QhANFyokyn4coAjKYBmM8GYL4uALP1AQgTTfJFVRNZqVT+0ABU8wMQjwunAPBixJ414AAgQcjaHzyfwQsljTaw7B/x1H5s1QEgyUSjy/6FlZL0+Vz5x4bAUjvaNQFQaTAsmAWAw8BCzB40Qm8MABa7C0Bj8k8DUEqIviECfAYi30mBZsy+8ooj+/JGovSnvvuXDUBjOiDVPRKKnr0EaxHq1lKHBAlAfQsxqsMbF78FYMEBgH097HjQNkCplIlAUnvYkPFsLlrky7sRnyDpn0/9x3vFPspsYa5emoAlb9mD6RxFMJ4FAI+ibAyBOoUn2ea+/oXAQZFdT9KfMd5OTP5+m9RoXgCioq8nfWH5V1LlXw0B4PewNaoCyqZSJDFmyDmXTr3PgRHIz/N7wTfjWL27P1MD+KalFnco9MBWWcy11IvxF5Iv8GU9s1AKPBD9ir886ZcjwZ/s+79RDeDvA5cg4OoAOHpY2eLXDYAbjorK/8oAKO0ym7zjkwCUcun+UP6/MwCZM8aWsgzCZX1iOVeXugCYjTh805Q6EGsoIefk8uM8afkv5hW/2cu8jc08Til89UhsKsrXC87zEs3ENBfaBn+zUv/XAEA2AuWMecPZAGQ5hnpFbO0QgNErALAopR6/20upzIb3Y/W+H1vk3NvX58exO8pKPrH9Z4h/c/tDAgDST4YF6wGQQcBiuAVkA7CQCwAvJb3g6vgIAF4su46Ao09nAMDRHYtBXPzrapBTIH+Z9t1sVP7XB4DWA4nNgP/36wAw7gJgvsq1BVwjAIH8loP09rhY8ieXIkyT2JP5ER3eXREAlNPy51Yv7fRtVDac3b9h+W9/VGegmR1pVBU1Ajlig8tlRycs2dli7pgR52CiQBMo3Sz1o2QgYeG5c2tTS3DmxCSkkBalFYMf9WUsiWsVtfFLpeWEmtAaxJd/uf7iMVJ6A3Bu/LrjwevOC/owAKA0NQrSSowRIFYYJAwNcGsLOJPI3Ls+KXRvgIkEwNz5pTgAS86pKUkAUhtFKbEaACDU/KHxX21Q/h8MAKsLMgAICHDVaXRLLkkCgiSi+nQlAJyVBsAgsMTyL1sA9JK/EMo8JwAbZlbAhmz6rlTqJf4bASB7rGHlMgmCDD8xC4AEBe6dZKLzKS8uJmcZ2neKE3CPl39hXPY7ZS/5YzoPKqId8VBozrXuOnx+xU8uw6+e9B0AkgikAPCqBy8DQBwBY3JF1akCAD+WFiPuu09AxKGULywBUN/IA4CvzWwiXBBwSQDWvdVYxU9D4m8IgKzgUEORouV6BMgWo+V4uEV/lNLVv5s/bGMAWJZOiryMNAAGASN3ToRnpkUaAMCe+xWMi8kpfpoUulkfAHm4VH2nUPYRNbAPxN+U8fRK+Yz6zBrdgBqJ3GXvIWZjt8WL3pKXEbIaue+dc/ay7vR6CKxHVqh4nZT/Ve5+rQEsR783AEuXBsA2oafi9HX1AL9Y5C/kAKDOLO3yJZczNdJY3EnxX9ryE+v/Ac3BppMribOkAAAAAElFTkSuQmCC")
MANIFEST = {"name": "MediaButler", "short_name": "MediaButler", "start_url": "/", "display": "standalone",
            "background_color": "#16508a", "theme_color": "#16508a",
            "icons": [{"src": "/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any"},
                      {"src": "/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any"}]}
PUBLIC_PATHS = {"/icon-64.png", "/icon-192.png", "/icon-512.png", "/apple-touch-icon.png", "/favicon.ico", "/manifest.webmanifest"}
_ICON_HDR = {"Cache-Control": "public, max-age=86400"}

@app.get("/favicon.ico")
@app.get("/icon-64.png")
def icon_64(): return Response(ICON_64, mimetype="image/png", headers=_ICON_HDR)

@app.get("/icon-192.png")
@app.get("/apple-touch-icon.png")
def icon_192(): return Response(ICON_192, mimetype="image/png", headers=_ICON_HDR)

@app.get("/icon-512.png")
def icon_512(): return Response(ICON_512, mimetype="image/png", headers=_ICON_HDR)

@app.get("/manifest.webmanifest")
def manifest():
    return Response(json.dumps(MANIFEST), mimetype="application/manifest+json")

@app.get("/")
def index():
    return Response(PAGE.replace("{{VERSION}}", f"v{VERSION} · {BUILD}"), mimetype="text/html", headers={"Cache-Control": "no-store"})

# ---------- Frontend ----------
PAGE = r"""<!doctype html>
<html lang="de"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#202734">
<link rel="icon" href="/icon-64.png" type="image/png"><link rel="apple-touch-icon" href="/apple-touch-icon.png"><link rel="manifest" href="/manifest.webmanifest">
<title>MediaButler</title>
<style>
:root{
 --bg:#202734;--surface:#2a3347;--ink:#e0f8c7;--muted:#a9b8ae;--line:#3f4b66;--soft:#36415a;
 --brand:#b7d894;--brand2:#b7d894;--brand-ink:#202734;--hero:#36415a;
 --ok:#b7d894;--okbg:#2e4234;--warn:#f0c46a;--warnbg:#463c20;--bad:#ff8c9d;--badbg:#4a2a37;
 --c-Filme:#f0a45c;--c-Serien:#8fb8ff;--c-Anime:#f58aa8;--c-Dokus:#6fd6b8;--c-:#f0c46a;
 --shadow:0 1px 2px rgba(0,0,0,.3),0 6px 18px rgba(0,0,0,.25);color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;padding-top:env(safe-area-inset-top,0px);background:var(--bg);color:var(--ink);
 font:15px/1.5 ui-rounded,"SF Pro Rounded","Segoe UI Variable","Segoe UI",system-ui,-apple-system,sans-serif}
main{max-width:1100px;margin:0 auto;padding:16px 16px 24px}
h1{font-size:24px;line-height:1.15;margin:0}h2{font-size:17px;margin:0 0 4px}
p{margin:0}.sub{color:var(--muted)}
code,.path,input.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px}
[hidden]{display:none!important}

/* Kopfbereich */
.hero{position:relative;display:grid;grid-template-columns:1fr auto;gap:14px 28px;align-items:center;background:var(--hero);color:#e0f8c7;
 border-radius:22px;padding:22px 24px 20px;margin-bottom:16px;box-shadow:var(--shadow)}
.hero .brand{grid-column:1;display:flex;gap:14px;align-items:center;min-width:0}
.hero .logo{width:64px;height:64px;border-radius:16px;flex:none;box-shadow:0 2px 6px rgba(0,0,0,.25)}
.hero h1{font-size:28px;font-weight:800;letter-spacing:-.02em;line-height:1.1;margin:0;color:#e0f8c7}
.hero .claim{color:rgba(255,255,255,.8);font-size:14px;margin-top:3px}
.hero .flow{grid-column:1;display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.hero .arrow{color:rgba(255,255,255,.6)}
.pathchip{background:rgba(32,39,52,.45);color:#e0f8c7;border-radius:10px;padding:5px 10px;min-width:0;max-width:100%}
.pathchip code{word-break:break-all;color:#e0f8c7}
.ver{position:absolute;top:10px;right:16px;font-size:11.5px;color:rgba(255,255,255,.65);font-variant-numeric:tabular-nums}
.stats{grid-column:2;grid-row:1 / span 2;display:flex;gap:10px}
.stat{background:rgba(32,39,52,.45);border-radius:14px;padding:9px 14px;min-width:96px;display:flex;flex-direction:column-reverse}
.stat b{display:block;font-size:24px;line-height:1.1;font-weight:800}.stat span{font-size:12px;color:rgba(255,255,255,.8);white-space:nowrap}
.stat.alert{background:#b7d894}.stat.alert b,.stat.alert span{color:#202734}

/* Hinweise */
.banner{border-radius:12px;padding:10px 14px;margin-bottom:12px;display:flex;gap:10px;align-items:flex-start;font-size:14px}
.banner.bad{background:var(--badbg);color:var(--bad)}.banner.warn{background:var(--warnbg);color:var(--warn)}.banner.ok{background:var(--okbg);color:var(--ok)}

/* Bedienelemente */
.exitem{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:12px 14px;margin-bottom:10px;max-width:760px}
.exitem .exh{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:4px}.exitem .bar{margin:8px 0 0}
.exitem .prog{margin-top:8px}.exitem input{flex:1 1 180px;min-width:0}
.exrow{display:flex;justify-content:space-between;gap:8px;padding:6px 0;border-top:1px solid var(--line);font-size:14px}.exrow:first-child{border-top:0}
.tbox{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:10px 12px 4px;margin-bottom:12px}
.tbox h3{margin:0 0 8px;font-size:12px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--muted)}
.tbox .bar{margin-bottom:8px}
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
.topbar{position:sticky;top:env(safe-area-inset-top,0px);z-index:5;background:var(--bg);padding:6px 0;margin-bottom:8px;display:flex;justify-content:flex-end}
.nav{display:flex;gap:6px;max-width:100%}
.nav button{position:relative;border-radius:999px;background:var(--surface);white-space:nowrap;display:flex;gap:6px;align-items:center;justify-content:center;padding:0;width:42px;height:42px;min-height:42px;font-size:19px;line-height:1}
.nav button .lbl{display:none;font-size:14px;font-weight:600}
.nav button.on{width:auto;padding:0 14px;background:var(--brand);color:var(--brand-ink);border-color:transparent;box-shadow:0 2px 8px rgba(0,0,0,.35)}
@media (min-width:700px){.nav button.on .lbl{display:inline}}
.nav .badge{position:absolute;top:-5px;right:-5px;min-width:18px;height:18px;line-height:18px;font-size:11px;padding:0 5px;box-shadow:0 0 0 2px var(--bg)}
.badge{min-width:20px;height:20px;border-radius:10px;padding:0 6px;font-size:12px;line-height:20px;text-align:center;background:var(--soft);color:var(--muted)}
.badge.hot{background:var(--warn);color:#202734}

/* Filter-Chips */
.chips{display:flex;gap:6px;flex-wrap:wrap}
.chip{border-radius:999px;padding:5px 12px;min-height:34px;font-size:14px;display:inline-flex;gap:6px;align-items:center}
.chip[data-c]{--cc:var(--c-)}
.chip[data-c=Filme]{--cc:var(--c-Filme)}.chip[data-c=Serien]{--cc:var(--c-Serien)}.chip[data-c=Anime]{--cc:var(--c-Anime)}.chip[data-c=Dokus]{--cc:var(--c-Dokus)}
.chip[data-c=Alle]{--cc:var(--brand)}
.chip.on{background:var(--cc);border-color:var(--cc);color:#202734}
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
.toast.ok{background:var(--ok);color:#202734}.toast.bad{background:var(--bad);color:#202734}
@keyframes pop{from{transform:translateY(-8px);opacity:0}}

@media (pointer:coarse){
 button,select,input[type=text],input[type=search],input[type=password],input[type=url]{min-height:46px;font-size:16px}
 input[type=checkbox]:not(.sw){width:22px;height:22px}}
@media (pointer:coarse){button.chip{min-height:38px;font-size:14px;padding:4px 12px}}
@media (max-width:720px){
 .hero{grid-template-columns:1fr;padding:18px 16px 16px;gap:12px;border-radius:20px}.hero .stats{grid-column:1;grid-row:auto;width:100%}
 .hero .logo{width:52px;height:52px;border-radius:14px}.hero h1{font-size:23px}.hero .claim{font-size:13px}
 .stat{flex:1;min-width:0;padding:7px 10px}.stat span{white-space:normal}
 .stat span{font-size:12px;line-height:1.25}
 .bar.tools{display:grid;grid-template-columns:1fr 1fr;gap:8px}.bar.tools button{padding:6px 8px;white-space:nowrap}
 #presets{flex-wrap:wrap!important;overflow:visible!important}
 .bar.tools #sel{grid-column:1/-1}
 .sched{white-space:nowrap;margin-bottom:6px}.sched>span{min-width:0;overflow:hidden;text-overflow:ellipsis}
 main{padding:10px 12px 24px}
 .stat b{font-size:20px}
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
 .nav{gap:4px}.nav button{width:38px;height:38px;min-height:38px;font-size:18px}.nav button.on{width:38px;padding:0}}
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
.gpath{margin-top:6px;font-size:12.5px;color:var(--muted);word-break:break-all;line-height:1.35}
.ctx button.danger{border:0;background:none}.ctx button.danger:hover:not(:disabled){background:var(--badbg)}
.ctx button{display:block;width:100%;border:0;background:none;text-align:left;padding:8px 14px}
td.f::before{content:"🎞️ ";opacity:.75}tr.grow td.f::before{content:""}
.badge.live{background:var(--ok);color:#202734;animation:pulse 1.4s ease-in-out infinite}.nav .on .badge.live{background:var(--ok);color:#202734}
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
.dupl{margin-top:6px;display:flex;flex-direction:column;gap:3px;align-items:flex-start;font-size:13px}
.dupm{line-height:1.35;word-break:break-all}.dupm code{font-size:12.5px}
.dg{padding:14px 16px;max-width:none}.dgh{display:flex;justify-content:space-between;gap:6px 14px;flex-wrap:wrap;align-items:center;margin-bottom:6px}
.dgf{display:flex;gap:12px;align-items:flex-start;padding:10px 8px;border-top:1px solid var(--line);cursor:pointer;border-radius:10px}
.dgf.sel{background:color-mix(in srgb,var(--bad) 14%,var(--surface))}
.dgf input{margin-top:3px;flex:none}.dgi{display:flex;flex-direction:column;gap:2px;min-width:0}
.dgn{font-weight:600;word-break:break-word}.dgp{color:var(--muted);word-break:break-all;font-size:12.5px;background:none;padding:0}
.dgm{font-size:13px;color:var(--muted)}#dk-dup .row{margin-bottom:8px}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style></head><body><main>

<div class="topbar"><nav class="nav" id="nav" aria-label="Bereiche">
  <button data-v="sort" title="Sortieren" aria-label="Sortieren"><span class="ic">📥</span><span class="lbl">Sortieren</span><span class="badge hot" id="ns" hidden></span></button>
  <button data-v="run" title="Läuft" aria-label="Läuft"><span class="ic">⚡</span><span class="lbl">Läuft</span><span class="badge live" id="nr" hidden></span></button>
  <button data-v="ext" title="Entpacken" aria-label="Entpacken"><span class="ic">📦</span><span class="lbl">Entpacken</span><span class="badge" id="ne" hidden></span></button>
  <button data-v="hist" title="Verschoben" aria-label="Verschoben"><span class="ic">✅</span><span class="lbl">Verschoben</span><span class="badge" id="nh" hidden></span></button>
  <button data-v="clean" title="Aufräumen" aria-label="Aufräumen"><span class="ic">🧹</span><span class="lbl">Aufräumen</span><span class="badge" id="nc" hidden></span></button>
  <button data-v="dup" title="Duplikate" aria-label="Duplikate"><span class="ic">🔁</span><span class="lbl">Duplikate</span><span class="badge" id="nd" hidden></span></button>
  <button data-v="ana" title="Analyse" aria-label="Analyse"><span class="ic">📊</span><span class="lbl">Analyse</span></button>
  <button data-v="sched" title="Zeitplan" aria-label="Zeitplan"><span class="ic">⏰</span><span class="lbl">Zeitplan</span></button>
  <button data-v="set" title="Einstellungen" aria-label="Einstellungen"><span class="ic">⚙️</span><span class="lbl">Einstellungen</span></button>
</nav></div>

<header class="hero">
  <span class="ver" title="Version und Prüfsumme der laufenden app.py">{{VERSION}}</span>
  <div class="brand"><img class="logo" src="/icon-192.png" alt="" width="64" height="64">
    <div><h1>MediaButler</h1><div class="claim">Sortiert, entpackt und serviert deine Medien.</div></div></div>
  <div class="flow"><span class="pathchip" title="Quellordner">📥 <code id="src">…</code></span><span class="arrow" aria-hidden="true">➜</span><span class="pathchip" title="Ziel auf dem NFS-Share">🗄️ <code id="tgt">…</code></span></div>
  <div class="stats">
    <div class="stat"><span>📦 bereit</span><b id="st-ready">0</b></div>
    <div class="stat" id="st-clar-box"><span>❓ brauchen dich</span><b id="st-clar">0</b></div>
    <div class="stat"><span>✅ verschoben</span><b id="st-hist">0</b></div>
  </div>
</header>
<div id="warn"></div>
<div id="warn2"></div>


<section id="v-sort">
  <div id="exbar" class="banner warn" hidden>📦 <span><span id="extxt"></span> <button class="link" id="exgo">Zum Reiter Entpacken</button></span></div>
  <div class="bar">
    <div class="chips" id="tabs"></div>
  </div>
  <div class="bar fq"><input type="search" id="fq" placeholder="🔍 Suchen: Dateiname, Serie oder Pfad" autocomplete="off" aria-label="Dateien suchen"><button id="fmark" hidden>☑️ Treffer markieren</button><span id="fqn" class="sub"></span><button id="reload" title="Dateiliste neu einlesen">↻ Neu laden</button></div>
  <section class="tbox" aria-labelledby="tboxh"><h3 id="tboxh">☑️ Markierungen &amp; Ansicht</h3>
  <div class="bar tools">
    <button id="all">☑️ Alle auswählen</button><button id="none">⬜ Keine</button><button id="inv" title="Auswahl der sichtbaren Dateien umdrehen">🔁 Umkehren</button>
    <button id="grp" class="toggle">📁 Nach Ordner</button><button id="sgrp" class="toggle">📺 Nach Serie</button><button id="merge" hidden>🔗 Serien vereinen</button><button id="shc" class="toggle" hidden>✅ Schon kopierte zeigen (<span id="shcn">0</span>)</button>
    <span id="sel" class="sub"></span><span class="sub hide-m" style="margin-left:auto">💡 Shift+Klick markiert einen Bereich, Strg+A alles</span>
  </div></section>
  <div id="series" class="bulk series" hidden><b>📺 Serien erkannt</b><span class="sub">Tippen zum Umbenennen oder Zuordnen</span><div class="chips" id="schips"></div></div>
  <div id="bulk" class="bulk" hidden><b id="bn"></b><div class="chips" id="bchips"></div><button id="gbtn">👥 Neue Gruppe</button><button id="dbtn" class="danger">🗑️ Löschen</button><button id="fbtn">📂 Zielordner ändern</button><button id="mbtn" hidden>🗓️ Jahr &amp; TMDB</button></div>
  <div class="table"><table>
    <thead><tr><th style="width:40px"><input type="checkbox" id="master" aria-label="Alle"></th><th>🎞️ Datei</th><th style="width:170px">🏷️ Kategorie</th><th>📍 Wird abgelegt als</th><th>💾 Größe</th></tr></thead>
    <tbody id="rows"></tbody></table>
    <div id="empty" class="empty" hidden><div class="big">🎉</div><b>Alles sortiert</b>Im Quellordner liegt nichts mehr, was sortiert werden muss.</div></div>
</section>

<div id="ctx" class="ctx" hidden><button id="ctxgrp">👥 Neue Gruppe</button><button id="ctxdis">↩️ Gruppe auflösen</button><button id="ctxedit">✏️ Ziel bearbeiten</button><button id="ctxdel" class="danger">🗑️ Löschen</button></div>
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

<section id="v-ext" hidden>
  <div id="exno" class="banner bad" hidden>⚠️ <span>7-Zip fehlt im Container. Bitte neu bauen: <code>docker compose up -d --build</code></span></div>
  <div class="card">
    <header><span class="ico">📦</span><div><h2>Entpacken</h2><p class="sub">RAR (auch .part01.rar, .r00) und ZIP (auch .z01) werden im Quellordner entpackt. Erst danach erscheinen die Dateien unter „Sortieren“.</p></div></header>
    <div class="bar" style="margin-top:8px"><button id="exscan">↻ Jetzt nach Archiven suchen</button></div>
  </div>
  <div id="exlist"></div>
  <div class="card">
    <header><span class="ico">🔑</span><div><h2>Passwörter</h2><p class="sub">Werden der Reihe nach ausprobiert. Fehlt eins, bekommst du eine Meldung hier und per Telegram und kannst es dort direkt beantworten.</p></div></header>
    <div id="expws"></div>
    <div class="bar" style="margin-top:8px"><input id="expwnew" type="text" placeholder="Neues Passwort" autocomplete="off" autocapitalize="off" spellcheck="false" style="flex:1 1 200px"><button id="expwadd" class="primary">Hinzufügen</button></div>
  </div>
  <div class="card" id="exdonecard" hidden>
    <header><span class="ico">✅</span><div><h2>Zuletzt entpackt</h2></div></header>
    <div id="exdone"></div>
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

<section id="v-dup" hidden>
  <p class="sub" style="margin-bottom:12px">Sucht in der Bibliothek nach Dateien, die doppelt oder fast gleich vorhanden sind: gleicher Titel, gleiche Dateigröße oder ähnlicher Name. Der Scan läuft nur, wenn du ihn startest. Gelöscht wird nur, was du markierst.</p>
  <div class="bar"><button class="primary" id="dstart">🔁 Duplikate suchen</button><button id="dstop" class="danger" hidden>Abbrechen</button><span id="dmeta" class="sub"></span></div>
  <div id="dmount" class="banner bad" hidden>⚠️ <span>Das Ziel ist nicht erreichbar. Ist das NFS-Share eingebunden?</span></div>
  <div id="derr" class="banner bad" hidden></div>
  <div id="dprog" class="card wide" hidden>
    <div class="row"><span id="dtxt"></span><b id="dpct"></b></div>
    <div class="prog run"><i id="dpb" style="width:100%"></i></div>
  </div>
  <div id="dempty" class="empty card wide" hidden><div class="big">🔁</div><b id="det">Noch kein Scan</b><span id="des"></span></div>
  <div id="dbody" hidden>
    <div class="astats">
      <div class="tile"><span class="sub">Gruppen</span><b id="d-g">0</b></div>
      <div class="tile"><span class="sub">Dateien betroffen</span><b id="d-f">0</b></div>
      <div class="tile save"><span>Doppelt belegter Platz</span><b id="d-w">0</b><span>wenn pro Gruppe nur eine Datei bleibt</span></div>
    </div>
    <div class="bar"><div class="chips" id="dfilter"></div></div>
    <div class="bar"><button id="dmarkall">☑️ Vorschlag markieren</button><button id="dmarknone">⬜ Keine</button>
      <span class="sub">Der Vorschlag behält je Gruppe die Datei mit der höchsten Auflösung, bei Gleichstand die größte.</span></div>
    <div id="dlist"></div>
    <div class="bar"><button id="dmore" hidden>Mehr anzeigen</button></div>
  </div>
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
    <header><span class="ico">📦</span><div><h2>Entpacken</h2><p class="sub">Gilt für RAR-, ZIP- und 7z-Archive im Quellordner (Übersicht im Reiter „Entpacken“).</p></div></header>
    <label class="opt"><input type="checkbox" id="exauto"><span><b>Automatisch entpacken</b><small>Fertig geladene Archive werden von allein entpackt. Aus: nur per Knopfdruck.</small></span></label>
    <label class="opt"><input type="checkbox" id="exdel"><span><b>Archive nach dem Entpacken löschen</b><small>Alle Teile werden gelöscht, sobald alles fehlerfrei entpackt wurde.</small></span></label>
  </div>
  <div class="card">
    <header><span class="ico">🗂️</span><div><h2>Verarbeitung</h2><p class="sub">Gilt fürs manuelle Kopieren und für den Zeitplan.</p></div></header>
    <label class="opt"><input type="checkbox" id="mv"><span><b>Quelle nach dem Kopieren löschen</b><small>Dateien werden verschoben statt kopiert.</small></span></label>
    <label class="opt"><input type="checkbox" id="cu"><span><b>Reste danach aufräumen</b><small>Ordner ohne Medien werden automatisch gelöscht.</small></span></label>
    <label class="opt"><input type="checkbox" id="jf"><span><b>Jellyfin-Benennung</b><small>Ordner und Dateien werden umbenannt: „Titel (Jahr)“ bzw. „Serie/Season 01/Serie S01E02“. Aus: Originalnamen bleiben.</small></span></label>
    <label class="opt"><input type="checkbox" id="jfs"><span><b>Jellyfin-Bibliothek nach der Warteschlange scannen</b><small>Sobald alle Aufträge durch sind und etwas kopiert wurde, wird einmal ein Scan ausgelöst.</small></span></label>
    <div id="jfbox" style="margin:6px 0 0 30px;display:grid;gap:8px">
      <input id="jfurl" placeholder="http://jellyfin:8096" autocomplete="off">
      <input id="jfkey" type="password" placeholder="API-Key (Jellyfin → Dashboard → API-Schlüssel)" autocomplete="off">
      <select id="jfmode" aria-label="Scan-Umfang"><option value="partial">Nur geänderte Ordner scannen (schnell)</option><option value="full">Ganze Bibliothek scannen</option></select>
      <input id="jfroot" placeholder="Pfad in Jellyfin, z. B. /data" autocomplete="off">
      <div class="bar"><button id="jftest">Scan jetzt testen</button><span id="jfmsg" class="sub"></span></div>
    </div>
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
  <div id="dk-dup" hidden><div class="row"><span id="dsel">Nichts markiert</span></div>
  <div class="bar"><button class="danger" id="ddel" disabled>🗑 Markierte löschen</button></div></div>
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
  for(const s of["sort","run","ext","hist","clean","dup","ana","sched","set"])$("v-"+s).hidden=s!==v;
  $("nav").querySelectorAll("button").forEach(b=>b.classList.toggle("on",b.dataset.v===v));
  $("go").hidden=v!=="sort";$("dk-dup").hidden=v!=="dup";document.querySelector(".dock .sched").hidden=v==="dup";
  ({sort:loadFiles,run:()=>api("/api/status").then(renderRun),ext:loadExt,hist:loadHist,clean:loadClean,dup:loadDup,ana:loadAna,sched:loadSched,set:loadSched})[v]()}
$("nav").querySelectorAll("button").forEach(b=>b.onclick=()=>show(b.dataset.v));


/* Entpacken */
const EXST={läuft:["⏳","entpackt","ok"],wartet:["🕒","wartet",""],unvollständig:["🧩","wartet auf Teile","warn"],passwort:["🔑","Passwort fehlt","bad"],fehler:["⛔","Fehler","bad"]};
async function loadExt(){
  if($("v-ext").contains(document.activeElement)&&document.activeElement.tagName==="INPUT")return;   // sonst schließt sich die Tastatur am Handy
  renderExt(await api("/api/extract"))}
function renderExt(r){
  const pwFirst=(r.items.find(e=>e.state==="passwort")||{}).id;
  $("exno").hidden=r.available;$("exauto").checked=r.auto;$("exdel").checked=r.delete;
  $("exlist").innerHTML=r.items.length?r.items.map(e=>{const [ic,lb,cl]=EXST[e.state]||["",e.state,""];
    const act=e.state==="läuft"?`<button data-a="cancel" data-id="${e.id}">⏹ Abbrechen</button>`
      :`<button data-a="start" data-id="${e.id}">▶️ ${e.state==="wartet"&&e.auto?"Jetzt starten":"Jetzt entpacken"}</button><button class="ghost" data-a="dismiss" data-id="${e.id}" title="Aus der Liste nehmen, bis sich das Archiv ändert">Ignorieren</button>`;
    return `<div class="exitem" data-id="${e.id}"><div class="exh"><b>📦 ${esc(e.name)}</b><span class="tag ${cl}">${ic} ${lb}</span></div>
      <div class="sub mono">${esc(e.dir||"/")} · ${e.nparts} Teil(e) · ${fmt(e.size)}</div>
      ${e.state==="läuft"?`<div class="prog run"><i style="width:${e.pct}%"></i></div><div class="sub">${e.pct} % ${esc(e.msg)}</div>`:(e.msg?`<div class="sub">${esc(e.msg)}</div>`:"")}
      ${e.state==="passwort"?(e.id===pwFirst?`<div class="bar"><input type="text" class="expw" placeholder="Passwort (gilt für alle wartenden Archive)" autocomplete="off" autocapitalize="off" spellcheck="false"><button class="primary" data-a="pw">🔑 Speichern &amp; erneut versuchen</button></div>`:'<div class="sub">🔑 Wartet auf dasselbe Passwort – Eingabe siehe oben.</div>'):""}
      <div class="bar">${act}</div></div>`}).join("")
    :'<div class="card empty"><div class="big">📭</div><b>Keine Archive im Quellordner</b>Neue .rar- und .zip-Dateien werden automatisch erkannt.</div>';
  $("expws").innerHTML=r.pws.length?r.pws.map((p,i)=>`<div class="exrow"><span class="mono">${esc(p)}</span><button class="danger" data-pwdel="${i}">Entfernen</button></div>`).join(""):'<p class="sub">Noch keine Passwörter gespeichert.</p>';
  $("exdonecard").hidden=!r.done.length;
  $("exdone").innerHTML=r.done.map(d=>`<div class="exrow"><span>🎬 <b>${esc((d.out&&d.out[0])||d.name)}</b> <span class="sub">${d.files>1?`+${d.files-1} weitere · `:""}${d.pw?"🔑 · ":""}aus ${esc(d.name)}</span></span><span class="sub">${new Date(d.ended*1000).toLocaleString("de-DE",{day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"})} · ${d.deleted?"Archiv gelöscht":"Archiv behalten"}${d.warn?" · mit Warnungen":""}</span></div>`).join("")}
$("exlist").onclick=async ev=>{const b=ev.target.closest("button[data-a]");if(!b)return;const id=b.dataset.id,a=b.dataset.a;
  try{if(a==="pw"){const v=b.closest(".exitem").querySelector(".expw").value;if(!v)return toast("Bitte ein Passwort eingeben","bad");
      const r=await api("/api/extract/password",{pw:v});toast("Passwort gespeichert, versuche es erneut","ok");renderExt(r)}
    else if(a==="cancel"){await api("/api/extract/cancel",{id});toast("Wird abgebrochen …","ok")}
    else renderExt(await api("/api/extract/"+a,{id}))}catch(e){toast(e.message,"bad")}};
$("expws").onclick=async ev=>{const b=ev.target.closest("[data-pwdel]");if(b)renderExt(await api("/api/extract/password/delete",{i:+b.dataset.pwdel}))};
$("expwadd").onclick=async()=>{const v=$("expwnew").value;if(!v)return;try{renderExt(await api("/api/extract/password",{pw:v}));$("expwnew").value="";toast("Passwort gespeichert","ok")}catch(e){toast(e.message,"bad")}};
const exSet=async()=>renderExt(await api("/api/extract/settings",{auto:$("exauto").checked,delete:$("exdel").checked}));
$("exauto").onchange=exSet;$("exdel").onchange=exSet;
$("exscan").onclick=async()=>{renderExt(await api("/api/extract/scan",{}));toast("Neu gesucht","ok")};
$("exgo").onclick=()=>show("ext");

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
  const xe=r.extracting||{};$("exbar").hidden=!xe.n;$("extxt").textContent=`${xe.n} Archiv(e) werden entpackt oder warten darauf. Die Dateien erscheinen danach hier.`;
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
/* Zielpfad einer Gruppe für den Kopf: gemeinsamer Ordner (Kategorie/Serie), sonst Hinweis */
function gpath(fs){
  const t=fs.filter(f=>f.category&&f.target),u=fs.length-t.length;
  if(!t.length)return`<div class="gpath">❓ noch kein Ziel</div>`;
  const cats=new Set(t.map(f=>f.target.split("/")[0])),dir=cats.size>1?"":commonFolder(t);
  const txt=cats.size>1?"verschiedene Kategorien":dir||"mehrere Ziele";
  return`<div class="gpath mono" title="${esc(dir||txt)}">📍 ${esc(txt)}${u?` <span class="tag warn">❓ ${u} unklar</span>`:""}</div>`}
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
const dupN=()=>files.filter(f=>f.dup&&f.dup.n).length;
const dupTags=f=>{const d=f.dup;if(!d||!d.n)return"";const ex=d.level==="exakt";
  return `<div class="dupl"><span class="tag ${ex?"bad":"warn"}">${ex?"🔁 Schon in der Bibliothek":"≈ Ähnliches vorhanden"}</span>`
    +d.items.map(m=>`<div class="dupm"><code>${esc(m.path)}</code> <span class="sub">${fmt(m.size)}${m.level!=="exakt"?" · ähnlich":""}</span></div>`).join("")
    +(d.n>d.items.length?`<div class="sub">+ ${d.n-d.items.length} weitere</div>`:"")+`</div>`};
function tabs(){
  const u=unclearN(),dn=dupN();
  if(filter==="Klärung"&&!u)filter="Alle";
  if(filter==="Doppelt"&&!dn)filter="Alle";
  const kinds=[...(u?["Klärung"]:[]),...(dn?["Doppelt"]:[])];
  $("tabs").hidden=!kinds.length;
  $("tabs").innerHTML=kinds.map(c=>`<button class="chip ${c===filter?"on":""}" data-c="${c==="Klärung"?"":"Alle"}" data-k="${c}">${c==="Klärung"?"❓ Braucht dich":"🔁 Schon da?"} <i>${c==="Klärung"?u:dn}</i></button>`).join("");
  $("tabs").querySelectorAll("button").forEach(b=>b.onclick=()=>{filter=filter===b.dataset.k?"Alle":b.dataset.k;tabs();render()});
  stats();seriesBar()}
let fqs="";
const hay=f=>(f.path+" "+(f.series?f.series.name+" "+f.series.orig:"")+" "+(f.target||"")).toLowerCase();
const visible=()=>files.filter(f=>(filter==="Alle"||(filter==="Klärung"?!f.category:filter==="Doppelt"?!!(f.dup&&f.dup.n):f.category===filter))&&(!fqs||fqs.every(w=>hay(f).includes(w))));
const catOpts=(first)=>`<option value="">${first}</option>`+cats.map(c=>`<option value="${c}">${ICON[c]} ${c}</option>`).join("");
function row(f,short,extra="",cls=""){const i=files.indexOf(f),name=short&&f.path.includes("/")?f.path.slice(f.path.indexOf("/")+1):f.path;return `<tr class="${cls}" data-cat="${esc(f.category)}">
   <td class="chk"><input type="checkbox" data-i="${i}" ${checked(f)?"checked":""} aria-label="markieren"></td>
   <td class="f" title="${esc(f.path)}">${esc(name)}${extra}${dupTags(f)}</td>
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
        ${e.key?`<button class="rn" data-sg="${gi}">✎ Umbenennen</button><button class="rn" data-fg="${gi}">📂 Zielordner</button>`:""}${new Set(fs.map(f=>f.series&&f.series.rkey).filter(Boolean)).size>1?`<button class="rn" data-split="${gi}" title="Zusammenlegung aufheben">✂️ Aufteilen</button>`:""}${e.origs&&e.origs.length>1?`<div class="gap ghost">🔗 vereint aus ${e.origs.map(x=>esc(x)).join(", ")}</div>`:""}<div style="margin-top:4px">${summ}</div>${e.key?gapHtml(fs):""}</td>
        <td class="c"><select data-g="${gi}" aria-label="Ganze Serie zuordnen">${catOpts("Serie zuordnen als…")}</select>${gpath(fs)}</td><td class="s"></td></tr>`
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
        <td class="c"><select data-g="${gi}" aria-label="Ganzen Ordner zuordnen">${catOpts("Ordner zuordnen als…")}</select>${gpath(fs)}</td><td class="s"></td></tr>`
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
async function deleteFiles(list){
  list=list.filter(Boolean);if(!list.length){toast("Erst Dateien markieren.","bad");return}
  const names=list.slice(0,8).map(f=>"• "+f.path).join("\n")+(list.length>8?`\n… und ${list.length-8} weitere`:"");
  if(!confirm(`${list.length} Datei(en) (${fmt(list.reduce((a,f)=>a+f.size,0))}) aus dem Quellordner endgültig löschen?\n\n${names}\n\nDas lässt sich nicht rückgängig machen.`))return;
  try{const r=await api("/api/files/delete",{items:list.map(f=>f.path)});await reloadKeep();
    toast(`${r.deleted} Datei(en) gelöscht`+(r.skipped?`, ${r.skipped} übersprungen (laufen gerade)`:"")+(r.errors.length?`, ${r.errors.length} Fehler`:""),r.errors.length?"bad":"ok");
    if(r.errors.length)$("err").textContent=r.errors.join("\n")}
  catch(e){toast(e.message,"bad")}}
$("dbtn").onclick=()=>deleteFiles(visible().filter(checked));

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
  $("ctxdel").textContent=`🗑️ Löschen (${base.length})`;
  $("ctxedit").hidden=!list.some(f=>f.category&&f.target);
  const m=$("ctx");m.hidden=false;m.style.left=Math.max(4,Math.min(ev.clientX,innerWidth-m.offsetWidth-8))+"px";m.style.top=Math.max(4,Math.min(ev.clientY,innerHeight-m.offsetHeight-8))+"px"});
$("ctxgrp").onclick=()=>{hideCtx();openGroup(ctxGrp)};
$("ctxdel").onclick=()=>{hideCtx();deleteFiles(ctxGrp)};
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
  $("en").checked=s.enabled;$("cron").value=s.cron;$("mv").checked=s.move_after;$("cu").checked=s.cleanup_after;$("jf").checked=s.jellyfin!==false;$("exauto").checked=s.extract_auto!==false;$("exdel").checked=s.extract_delete!==false;$("jfs").checked=!!s.jf_scan;$("jfmode").value=s.jf_mode||"partial";$("jfroot").value=s.jf_root||"";$("jfroot").placeholder="Zielordner aus Sicht von Jellyfin (leer = "+s.info.target+")";$("jfurl").value=s.jf_url||"";$("jfkey").value="";
  $("jfkey").placeholder=s.jf_key_set?"API-Key gespeichert (leer lassen zum Behalten)":"API-Key (Jellyfin → Dashboard → API-Schlüssel)";$("jfmsg").textContent=s.jf_last?"Zuletzt: "+s.jf_last:"";
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
const collect=()=>({enabled:$("en").checked,cron:$("cron").value,move_after:$("mv").checked,cleanup_after:$("cu").checked,jellyfin:$("jf").checked,jf_scan:$("jfs").checked,jf_url:$("jfurl").value,jf_key:$("jfkey").value,jf_mode:$("jfmode").value,jf_root:$("jfroot").value,
  tg_enabled:$("tge").checked,tg_token:$("tgtok").value,tg_chat:$("tgchat").value,tg_start:$("tgs").checked,tg_done:$("tgd").checked,tg_unclear:$("tgu").checked,
  tg_new:$("tgn").checked,tg_progress:$("tgp").checked,tg_errors:$("tgr").checked,tg_silent:$("tgq").checked,public_url:$("puburl").value});
async function saveAll(out){for(const id of["saved","saved2","tgmsg"])$(id).textContent="";try{
  const s=await api("/api/settings",collect());nextList(s.next,s.error);tokHint(s.tg_token_set);toast("Gespeichert","ok");poll()}
  catch(e){out.textContent=e.message;toast(e.message,"bad")}}
$("save").onclick=()=>saveAll($("saved"));$("save2").onclick=()=>saveAll($("saved2"));$("tgsave").onclick=()=>saveAll($("tgmsg"));
async function sendTest(urgent){$("tgmsg").textContent="Sende…";
  try{await api("/api/telegram/test",{token:$("tgtok").value,chat:$("tgchat").value,urgent});$("tgmsg").textContent="";toast(urgent?"Dringender Test gesendet":"Test gesendet","ok")}
  catch(e){$("tgmsg").textContent="";toast("Telegram: "+e.message,"bad")}}
$("jftest").onclick=async()=>{$("jfmsg").textContent="…";try{const r=await api("/api/jellyfin/test",{url:$("jfurl").value,key:$("jfkey").value});
  $("jfmsg").textContent=(r.ok?"✅ ":"⛔ ")+r.msg}catch(e){$("jfmsg").textContent="⛔ "+e.message}};
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
let exDone=-1;
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
  const ex=s.extract||{};const ne=$("ne");ne.hidden=!(ex.pw||ex.n||ex.err);ne.className="badge"+(ex.pw||ex.err?" hot":ex.running?" live":"");
  ne.textContent=ex.pw?"🔑":ex.err?"!":ex.n||"";
  $("warn2").innerHTML=ex.pw?`<div class="banner warn">🔑 <span><b>Passwort fehlt</b> für ${ex.pw} Archiv(e). <button class="link" id="expwgo">Jetzt eingeben</button></span></div>`:"";
  if($("expwgo"))$("expwgo").onclick=()=>show("ext");
  if(view==="ext")loadExt();
  if(exDone!==ex.done){if(exDone>=0&&view==="sort"&&!dlgOpen())reloadKeep();exDone=ex.done}
  const nr=$("nr");const wn=(s.waiting||[]).length;nr.hidden=!s.running&&!wn;nr.textContent=(s.running?Math.round(pct)+"%":"")+(wn?` +${wn}`:"");
  if(view==="run")renderRun(s);
  if(s.running&&view==="sort"&&s.files_done!==lastDone&&!dlgOpen()){lastDone=s.files_done;reloadKeep()}
  if(!s.running)lastDone=-1;
  wasRunning=s.running}catch{}}
setInterval(poll,1000);
/* Duplikate */
let dres=null,dsel=new Set(),dshow=40,dfilter="Alle",dtimer,dWas=false;
const dIc={Film:"🎬",Folge:"📺"};
async function loadDup(){
  const r=await api("/api/dupes");dres=r;
  const known=new Set(r.groups.flatMap(g=>g.files.map(f=>f.path)));dsel=new Set([...dsel].filter(p=>known.has(p)));
  setBadge("nd",r.groups.length);
  if(r.running){dWas=true;dupPoll()}
  drawDup()}
function dupPoll(){clearTimeout(dtimer);dtimer=setTimeout(async()=>{
  if(view!=="dup")return;
  try{const r=await api("/api/dupes");
    if(r.running){dres=Object.assign(dres||{},{running:true,total:r.total,done:r.done,current:r.current});drawDupProg();dupPoll()}
    else if(dWas){dWas=false;toast(r.error?"Scan fehlgeschlagen":"Duplikat-Scan fertig",r.error?"bad":"ok");loadDup()}}catch{dupPoll()}},1000)}
function drawDupProg(){const r=dres;
  $("dprog").hidden=!r.running;$("dstop").hidden=!r.running;$("dstart").disabled=r.running||!r.mounted;
  $("dtxt").textContent=r.current==="Vergleiche …"?"Vergleiche …":`${r.total} Dateien gelesen${r.current?": "+r.current:""}`;
  $("dpct").textContent="";$("derr").hidden=!r.error;$("derr").textContent=r.error?"⚠️ "+r.error:""}
const dSize=()=>new Map(dres.groups.flatMap(g=>g.files.map(f=>[f.path,f.size])));
function drawDupSel(){
  const sz=dSize(),n=dsel.size,tot=[...dsel].reduce((a,p)=>a+(sz.get(p)||0),0);
  const full=dres.groups.filter(g=>g.files.every(f=>dsel.has(f.path))).length;
  $("dsel").textContent=n?`${n} markiert · ${fmt(tot)} werden frei`+(full?` · ⚠️ In ${full} Gruppe(n) ist alles markiert, dort bleibt eine Datei`:""):"Nichts markiert";
  $("ddel").disabled=!n}
function dupFile(g,f){
  const keep=f.path===g.keep,parts=f.path.split("/"),name=parts.pop(),dir=dres.root.replace(/\/$/,"")+"/"+parts.join("/");
  const q=[f.codec,f.h?`${f.w}×${f.h}`:""].filter(Boolean).join(" · "),on=dsel.has(f.path);
  return `<label class="dgf${on?" sel":""}"><input type="checkbox" data-p="${esc(f.path)}" ${on?"checked":""} aria-label="markieren">
    <span class="dgi"><span class="dgn">${esc(name)} ${keep?'<span class="tag ok">⭐ Vorschlag: behalten</span>':""}</span>
    <code class="dgp">${esc(dir)}</code>
    <span class="dgm"><b>${fmt(f.size)}</b>${q?" · "+esc(q):""} · ${new Date(f.m*1000).toLocaleDateString("de-DE")}</span></span></label>`}
function dupGroup(g,gi){const ex=g.level==="exakt";
  return `<div class="card wide dg"><div class="dgh"><div><b>${dIc[g.type]||""} ${esc(g.label)}</b> <span class="tag ${ex?"bad":"warn"}">${ex?"Exakt":"Ähnlich"}</span>
    ${g.reasons.map(x=>`<span class="tag">${esc(x)}</span>`).join(" ")}</div>
    <div class="sub">${g.files.length} Dateien · doppelt belegt <b>${fmt(g.waste)}</b></div></div>
    ${g.files.map(f=>dupFile(g,f)).join("")}
    <div class="bar" style="margin:8px 0 0"><button data-rest="${gi}">☑️ Alle außer Vorschlag markieren</button></div></div>`}
function drawDup(){
  const r=dres,gs=r.groups,has=gs.length>0;
  drawDupProg();$("dmount").hidden=r.mounted;
  $("dmeta").textContent=r.ts?`Letzter Scan ${fdate(r.ts)}: ${r.scanned} Dateien in ${r.root}`:r.root;
  $("dempty").hidden=has||r.running;$("dbody").hidden=!has;
  if(!has){const done=!!r.ts;$("det").textContent=done?"Keine Duplikate gefunden":"Noch kein Scan";
    $("des").textContent=done?`${r.scanned} Dateien geprüft, nichts ist doppelt oder ähnlich.`:"Starte den Scan, um doppelte und ähnliche Dateien zu finden.";drawDupSel();return}
  $("d-g").textContent=gs.length;$("d-f").textContent=gs.reduce((a,g)=>a+g.files.length,0);$("d-w").textContent=fmt(r.waste);
  const cnt={Alle:gs.length,exakt:gs.filter(g=>g.level==="exakt").length,ähnlich:gs.filter(g=>g.level!=="exakt").length};
  if(dfilter!=="Alle"&&!cnt[dfilter])dfilter="Alle";
  $("dfilter").innerHTML=[["Alle","Alle"],["exakt","🔴 Exakt"],["ähnlich","🟡 Ähnlich"]].filter(([k])=>k==="Alle"||cnt[k]).map(([k,l])=>
    `<button class="chip ${k===dfilter?"on":""}" data-c="Alle" data-f="${k}">${l} <i>${cnt[k]}</i></button>`).join("");
  $("dfilter").querySelectorAll("button").forEach(b=>b.onclick=()=>{dfilter=b.dataset.f;dshow=40;drawDup()});
  const list=gs.map((g,gi)=>[g,gi]).filter(([g])=>dfilter==="Alle"||(dfilter==="exakt")===(g.level==="exakt"));
  $("dlist").innerHTML=list.slice(0,dshow).map(([g,gi])=>dupGroup(g,gi)).join("");
  $("dmore").hidden=list.length<=dshow;$("dmore").textContent=`Mehr anzeigen (${list.length-dshow} weitere)`;
  drawDupSel()}
$("dlist").addEventListener("change",e=>{const c=e.target.closest("input[data-p]");if(!c)return;
  c.checked?dsel.add(c.dataset.p):dsel.delete(c.dataset.p);c.closest(".dgf").classList.toggle("sel",c.checked);drawDupSel()});
$("dlist").addEventListener("click",e=>{const b=e.target.closest("button[data-rest]");if(!b)return;
  const g=dres.groups[b.dataset.rest];g.files.forEach(f=>f.path===g.keep?dsel.delete(f.path):dsel.add(f.path));drawDup()});
$("dmarkall").onclick=()=>{dres.groups.filter(g=>dfilter==="Alle"||(dfilter==="exakt")===(g.level==="exakt")).forEach(g=>g.files.forEach(f=>{if(f.path!==g.keep)dsel.add(f.path)}));drawDup()};
$("dmarknone").onclick=()=>{dsel.clear();drawDup()};
$("dmore").onclick=()=>{dshow+=40;drawDup()};
$("dstart").onclick=async()=>{try{await api("/api/dupes/start",{});dWas=true;toast("Scan gestartet","ok");loadDup()}catch(e){toast(e.message,"bad")}};
$("dstop").onclick=async()=>{await api("/api/dupes/cancel",{})};
$("ddel").onclick=async()=>{
  const sz=dSize(),list=[...dsel];if(!list.length)return;
  const names=list.slice(0,6).map(p=>"• "+p).join("\n")+(list.length>6?`\n… und ${list.length-6} weitere`:"");
  if(!confirm(`${list.length} Datei(en) (${fmt(list.reduce((a,p)=>a+(sz.get(p)||0),0))}) endgültig aus der Bibliothek löschen?\n\n${names}\n\nPro Gruppe bleibt mindestens eine Datei erhalten. Das lässt sich nicht rückgängig machen. Jellyfin merkt die Änderung beim nächsten Bibliotheks-Scan.`))return;
  $("ddel").disabled=true;
  try{const r=await api("/api/dupes/delete",{items:list});
    toast(`${r.deleted} gelöscht, ${fmt(r.freed)} frei`+(r.kept?`, in ${r.kept} Gruppe(n) blieb eine Datei`:""),r.errors.length?"bad":"ok");
    if(r.errors.length)toast(r.errors[0],"bad");
    dsel.clear();loadDup()}catch(e){toast(e.message,"bad");drawDupSel()}};

const HV={clarify:"sort",sort:"sort",run:"run",ext:"ext",hist:"hist",clean:"clean",dup:"dup",ana:"ana",sched:"sched",set:"set"};
function route(){const h=location.hash.slice(1);if(h==="clarify")filter="Klärung";show(HV[h]||"sort")}
window.addEventListener("hashchange",route);
const dockEl=document.querySelector(".dock");
new ResizeObserver(()=>{document.querySelector("main").style.paddingBottom=dockEl.offsetHeight+24+"px"}).observe(dockEl);
route();poll();
api("/api/history").then(r=>{setBadge("nh",r.items.length);$("st-hist").textContent=r.items.length});
api("/api/leftovers").then(r=>setBadge("nc",r.dirs.length));
api("/api/dupes").then(r=>setBadge("nd",r.groups.length)).catch(()=>{});
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
    threading.Thread(target=extract_loop, daemon=True).start()
    threading.Thread(target=tg_poller, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
