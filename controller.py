"""
zeitkapsel – Controller: Anmeldung, Benutzer und Rollen, Host-Verwaltung, Rechteprüfung.

Der Browser spricht nur mit dem Controller. Aufrufe für einen Host gehen an
/api/hosts/<id>/<pfad>; der Controller prüft die Rechte und leitet an den Agenten des Hosts
weiter (<host-url>/agent/api/<pfad>, Token im Header X-Zeitkapsel-Token).

Rollen
  admin  alles: Releases, Notizen, Zeitpläne, Master-DBs, neue DBs, Import, Hosts, Benutzer,
         alle Entwickler-DBs
  user   Entwickler-DBs für sich anlegen (aus jedem Release oder Snapshot), eigene starten,
         stoppen, zurücksetzen, snapshotten und löschen; alles andere nur lesen

Daten in SQLite (ZK_DATA, Standard /var/lib/zeitkapsel/zeitkapsel.db).
"""
from __future__ import annotations

import json
import os
import re
import secrets
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

from flask import Blueprint, Response, g, jsonify, redirect, request, send_file, session
from werkzeug.security import check_password_hash, generate_password_hash

from agent import env

DATA = Path(env("DATA", "/var/lib/zeitkapsel/zeitkapsel.db"))
STATIC = Path(__file__).with_name("static")
RE_USERNAME = re.compile(r"^[a-z0-9][a-z0-9._-]{1,31}$")
RE_HOSTNAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$")
RE_URL = re.compile(r"^https?://[A-Za-z0-9.\-\[\]:]+(/[A-Za-z0-9._/-]*)?$")
ROLES = ("admin", "user")
CSRF_HEADER = "zeitkapsel"

bp = Blueprint("controller", __name__)
_db_lock = threading.Lock()
_login_failures: dict[str, list[float]] = {}


class Denied(Exception):
    pass


class BadRequest(Exception):
    pass


# --------------------------------------------------------------------------- SQLite
SCHEMA = """
create table if not exists users (
  username text primary key,
  pw_hash  text not null,
  role     text not null check (role in ('admin','user')),
  disabled integer not null default 0,
  created  text not null
);
create table if not exists hosts (
  id      integer primary key autoincrement,
  name    text not null unique,
  url     text not null,
  token   text not null,
  created text not null
);
create table if not exists settings (key text primary key, value text not null);
create table if not exists audit (
  id     integer primary key autoincrement,
  ts     text not null,
  user   text not null,
  host   text,
  method text not null,
  path   text not null,
  status integer not null
);
"""


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DATA, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    DATA.parent.mkdir(parents=True, exist_ok=True)
    with _db_lock, db() as conn:
        conn.executescript(SCHEMA)
    os.chmod(DATA, 0o600)


def setting(key: str, factory=None) -> str | None:
    with _db_lock, db() as conn:
        row = conn.execute("select value from settings where key=?", (key,)).fetchone()
        if row:
            return row["value"]
        if factory is None:
            return None
        value = factory()
        conn.execute("insert into settings(key, value) values(?, ?)", (key, value))
        return value


def create_user(username: str, password: str, role: str) -> None:
    if not RE_USERNAME.match(username):
        raise BadRequest("Benutzername: 2–32 Zeichen, Kleinbuchstaben, Ziffern, . _ -")
    if role not in ROLES:
        raise BadRequest("Unbekannte Rolle.")
    check_password_strength(password)
    with _db_lock, db() as conn:
        if conn.execute("select 1 from users where username=?", (username,)).fetchone():
            raise BadRequest(f"Benutzer „{username}“ gibt es schon.")
        conn.execute("insert into users(username, pw_hash, role, created) values(?,?,?,?)",
                     (username, generate_password_hash(password), role, now_iso()))


def check_password_strength(password: str) -> None:
    if len(password or "") < 10:
        raise BadRequest("Passwort: mindestens 10 Zeichen.")


def get_user(username: str) -> sqlite3.Row | None:
    with db() as conn:
        return conn.execute("select * from users where username=?", (username,)).fetchone()


def admin_count(conn) -> int:
    return conn.execute("select count(*) from users where role='admin' and disabled=0").fetchone()[0]


def hosts(with_token: bool = False) -> list[dict]:
    with db() as conn:
        rows = conn.execute("select * from hosts order by name").fetchall()
    out = []
    for r in rows:
        d = dict(r)
        if not with_token:
            d.pop("token")
        out.append(d)
    return out


def get_host(hid: int) -> dict:
    with db() as conn:
        row = conn.execute("select * from hosts where id=?", (hid,)).fetchone()
    if not row:
        raise BadRequest("Host nicht gefunden.")
    return dict(row)


def ensure_local_host(port: int, token: str) -> None:
    """Im kombinierten Betrieb den eigenen Agenten als ersten Host eintragen."""
    with _db_lock, db() as conn:
        if conn.execute("select count(*) from hosts").fetchone()[0] == 0 and token:
            conn.execute("insert into hosts(name, url, token, created) values(?,?,?,?)",
                         (socket.gethostname(), f"http://127.0.0.1:{port}", token, now_iso()))


def audit(host: int | None, status: int) -> None:
    if request.method in ("GET", "HEAD"):
        return
    with _db_lock, db() as conn:
        conn.execute("insert into audit(ts, user, host, method, path, status) values(?,?,?,?,?,?)",
                     (now_iso(), getattr(g, "user", "") or "-", str(host) if host else None,
                      request.method, request.path[:300], status))
        conn.execute("delete from audit where id <= (select max(id) - 5000 from audit)")


# --------------------------------------------------------------------------- Agenten aufrufen
def agent_call(host: dict, method: str, path: str, body=None, user: str = "", query: str = "",
               timeout: int = 30) -> tuple[int, dict | list]:
    url = host["url"].rstrip("/") + "/agent/api/" + path + (("?" + query) if query else "")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "X-Zeitkapsel-Token": host["token"], "X-Zeitkapsel-User": user,
        "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except ValueError:
            return e.code, {"error": f"Agent antwortet mit HTTP {e.code}"}
    except (urllib.error.URLError, socket.timeout, ConnectionError, OSError) as e:
        reason = getattr(e, "reason", e)
        return 502, {"error": f"Host „{host['name']}“ nicht erreichbar: {reason}"}


# --------------------------------------------------------------------------- Rechte
USER_GET = (r"state", r"jobs/[^/]+", r"snapshots", r"usage", r"devdbs/[^/]+")
OWNER_ACTIONS = (
    ("POST", r"devdbs/([^/]+)/(start|stop|reset)"),
    ("DELETE", r"devdbs/([^/]+)"),
    ("POST", r"devdbs/([^/]+)/snapshots"),
    ("POST", r"devdbs/([^/]+)/snapshots/[^/]+/restore"),
    ("PATCH", r"devdbs/([^/]+)/snapshots/[^/]+"),
    ("DELETE", r"devdbs/([^/]+)/snapshots/[^/]+"),
)


def authorize(host: dict, method: str, path: str, body):
    """Prüft das Recht für einen Agent-Aufruf; liefert ggf. einen angepassten Body zurück."""
    if g.role == "admin":
        return body
    if method == "GET":
        if any(re.fullmatch(p, path) for p in USER_GET):
            return body
        raise Denied()
    if method == "POST" and re.fullmatch(r"db/[^/]+/devdbs", path):
        body = dict(body or {})
        body["owner"] = g.user               # Benutzer legen immer für sich selbst an
        return body
    for m, pattern in OWNER_ACTIONS:
        hit = re.fullmatch(pattern, path)
        if m == method and hit:
            status, dev = agent_call(host, "GET", f"devdbs/{hit.group(1)}", user=g.user)
            if status != 200:
                raise BadRequest(dev.get("error", "Entwickler-DB nicht gefunden."))
            if dev.get("owner") != g.user:
                raise Denied("Diese Entwickler-DB gehört einem anderen Benutzer.")
            return body
    raise Denied()


# --------------------------------------------------------------------------- Sitzung
@bp.before_app_request
def require_login():
    p = request.path
    if p.startswith("/agent/") or p in ("/login", "/api/login") or p.startswith("/static/"):
        return None
    user = session.get("user")
    row = get_user(user) if user else None
    if not row or row["disabled"]:
        session.clear()
        if p.startswith("/api/"):
            return jsonify(error="Bitte anmelden."), 401
        return redirect("/login")
    g.user, g.role = row["username"], row["role"]
    if request.method not in ("GET", "HEAD") and request.headers.get("X-Requested-With") != CSRF_HEADER:
        return jsonify(error="Anfrage abgelehnt."), 403
    return None


def need_admin():
    if g.role != "admin":
        raise Denied()


@bp.errorhandler(Denied)
def on_denied(e):
    audit((request.view_args or {}).get("hid"), 403)
    return jsonify(error=str(e) or "Dafür fehlen die Rechte (nur für Administratoren)."), 403


@bp.errorhandler(BadRequest)
def on_bad(e):
    return jsonify(error=str(e)), 400


@bp.get("/login")
def login_page():
    return send_file(STATIC / "login.html")


@bp.post("/api/login")
def api_login():
    ip = request.remote_addr or "?"
    recent = [t for t in _login_failures.get(ip, []) if t > time.time() - 300]
    if len(recent) >= 8:
        return jsonify(error="Zu viele Fehlversuche; bitte einige Minuten warten."), 429
    data = request.get_json(force=True)
    row = get_user((data.get("username") or "").strip().lower())
    if not row or row["disabled"] or not check_password_hash(row["pw_hash"], data.get("password") or ""):
        _login_failures[ip] = recent + [time.time()]
        time.sleep(0.5)
        return jsonify(error="Benutzername oder Passwort falsch."), 401
    _login_failures.pop(ip, None)
    session.clear()
    session.permanent = True
    session["user"] = row["username"]
    return jsonify(ok=True)


@bp.post("/api/logout")
def api_logout():
    session.clear()
    return jsonify(ok=True)


@bp.get("/")
def index():
    return send_file(STATIC / "index.html")


@bp.get("/api/me")
def api_me():
    return jsonify(username=g.user, role=g.role)


@bp.post("/api/me/password")
def api_me_password():
    data = request.get_json(force=True)
    row = get_user(g.user)
    if not check_password_hash(row["pw_hash"], data.get("old") or ""):
        raise BadRequest("Das bisherige Passwort stimmt nicht.")
    check_password_strength(data.get("new"))
    with _db_lock, db() as conn:
        conn.execute("update users set pw_hash=? where username=?", (generate_password_hash(data["new"]), g.user))
    audit(None, 200)
    return jsonify(ok=True)


# --------------------------------------------------------------------------- Benutzer
@bp.get("/api/users")
def api_users():
    need_admin()
    with db() as conn:
        rows = conn.execute("select username, role, disabled, created from users order by username").fetchall()
    return jsonify([dict(r) for r in rows])


@bp.post("/api/users")
def api_user_create():
    need_admin()
    data = request.get_json(force=True)
    create_user((data.get("username") or "").strip().lower(), data.get("password") or "", data.get("role") or "user")
    audit(None, 200)
    return jsonify(ok=True)


@bp.patch("/api/users/<username>")
def api_user_update(username):
    need_admin()
    data = request.get_json(force=True)
    with _db_lock, db() as conn:
        row = conn.execute("select * from users where username=?", (username,)).fetchone()
        if not row:
            raise BadRequest("Benutzer nicht gefunden.")
        role = data.get("role", row["role"])
        disabled = int(bool(data.get("disabled", row["disabled"])))
        if role not in ROLES:
            raise BadRequest("Unbekannte Rolle.")
        losing_admin = row["role"] == "admin" and not row["disabled"] and (role != "admin" or disabled)
        if losing_admin and admin_count(conn) <= 1:
            raise BadRequest("Es muss mindestens ein aktiver Administrator bleiben.")
        if username == g.user and (disabled or role != "admin"):
            raise BadRequest("Die eigene Rolle oder Sperre lässt sich nicht ändern.")
        conn.execute("update users set role=?, disabled=? where username=?", (role, disabled, username))
        if data.get("password"):
            check_password_strength(data["password"])
            conn.execute("update users set pw_hash=? where username=?",
                         (generate_password_hash(data["password"]), username))
    audit(None, 200)
    return jsonify(ok=True)


@bp.delete("/api/users/<username>")
def api_user_delete(username):
    need_admin()
    if username == g.user:
        raise BadRequest("Das eigene Konto lässt sich nicht löschen.")
    with _db_lock, db() as conn:
        row = conn.execute("select * from users where username=?", (username,)).fetchone()
        if not row:
            raise BadRequest("Benutzer nicht gefunden.")
        if row["role"] == "admin" and not row["disabled"] and admin_count(conn) <= 1:
            raise BadRequest("Es muss mindestens ein aktiver Administrator bleiben.")
        conn.execute("delete from users where username=?", (username,))
    audit(None, 200)
    return jsonify(ok=True)


# --------------------------------------------------------------------------- Hosts
def validate_host(data: dict, require_token: bool) -> dict:
    name = (data.get("name") or "").strip()
    url = (data.get("url") or "").strip().rstrip("/")
    token = (data.get("token") or "").strip()
    if not RE_HOSTNAME.match(name):
        raise BadRequest("Name: Buchstaben, Ziffern, . _ -")
    if not RE_URL.match(url):
        raise BadRequest("Adresse: http(s)://host:port des Agenten.")
    if require_token and len(token) < 24:
        raise BadRequest("Token: mindestens 24 Zeichen (ZK_AGENT_TOKEN des Agenten).")
    return dict(name=name, url=url, token=token)


@bp.get("/api/hosts")
def api_hosts():
    items = hosts()
    if g.role != "admin":
        items = [dict(id=h["id"], name=h["name"]) for h in items]
    return jsonify(items)


@bp.post("/api/hosts")
def api_host_create():
    need_admin()
    h = validate_host(request.get_json(force=True), True)
    status, res = agent_call(dict(h, name=h["name"]), "GET", "summary", user=g.user, timeout=8)
    if status != 200:
        raise BadRequest("Verbindungstest fehlgeschlagen: " + res.get("error", f"HTTP {status}"))
    with _db_lock, db() as conn:
        try:
            conn.execute("insert into hosts(name, url, token, created) values(?,?,?,?)",
                         (h["name"], h["url"], h["token"], now_iso()))
        except sqlite3.IntegrityError:
            raise BadRequest(f"Ein Host „{h['name']}“ ist schon eingetragen.") from None
    audit(None, 200)
    return jsonify(ok=True)


@bp.patch("/api/hosts/<int:hid>")
def api_host_update(hid):
    need_admin()
    old = get_host(hid)
    h = validate_host(request.get_json(force=True), False)
    h["token"] = h["token"] or old["token"]
    status, res = agent_call(h, "GET", "summary", user=g.user, timeout=8)
    if status != 200:
        raise BadRequest("Verbindungstest fehlgeschlagen: " + res.get("error", f"HTTP {status}"))
    with _db_lock, db() as conn:
        conn.execute("update hosts set name=?, url=?, token=? where id=?", (h["name"], h["url"], h["token"], hid))
    audit(hid, 200)
    return jsonify(ok=True)


@bp.delete("/api/hosts/<int:hid>")
def api_host_delete(hid):
    need_admin()
    get_host(hid)
    with _db_lock, db() as conn:
        conn.execute("delete from hosts where id=?", (hid,))
    audit(hid, 200)
    return jsonify(ok=True)


@bp.get("/api/overview")
def api_overview():
    items = hosts(with_token=True)
    me = g.user

    def fetch(h):
        status, res = agent_call(h, "GET", "summary", user=me, timeout=5)
        out = dict(id=h["id"], name=h["name"], ok=status == 200)
        if status == 200:
            out.update(dbs=res.get("dbs", []), disk=res.get("disk"), image=res.get("image"),
                       host=res.get("host"), running_jobs=res.get("running_jobs", 0))
        else:
            out.update(dbs=[], error=res.get("error", f"HTTP {status}"))
        return out

    with ThreadPoolExecutor(max_workers=8) as ex:
        result = list(ex.map(fetch, items))
    users = []
    if g.role == "admin":
        with db() as conn:
            users = [r["username"] for r in conn.execute("select username from users where disabled=0 order by username")]
    return jsonify(me=dict(username=g.user, role=g.role), hosts=result, users=users)


@bp.get("/api/audit")
def api_audit():
    need_admin()
    with db() as conn:
        rows = conn.execute("select a.*, h.name as host_name from audit a left join hosts h on h.id = a.host "
                            "order by a.id desc limit 200").fetchall()
    return jsonify([dict(r) for r in rows])


# --------------------------------------------------------------------------- Weiterleitung an Agenten
LONG_CALLS = (r"import/probe", r"import/local/probe", r"import/dp/probe", r"usage", r"newdb/check", r"schedules/[^/]+/run")


@bp.route("/api/hosts/<int:hid>/<path:path>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
def api_proxy(hid, path):
    host = get_host(hid)
    body = request.get_json(silent=True) if request.method in ("POST", "PUT", "PATCH") else None
    body = authorize(host, request.method, path, body)
    timeout = 960 if any(re.fullmatch(p, path) for p in LONG_CALLS) else 30
    status, res = agent_call(host, request.method, path, body, user=g.user,
                             query=request.query_string.decode(), timeout=timeout)
    audit(hid, status)
    return Response(json.dumps(res), status=status, mimetype="application/json")


# --------------------------------------------------------------------------- Einrichtung
def init_app(app, port: int, local_token: str | None) -> None:
    init_db()
    app.secret_key = setting("secret_key", lambda: secrets.token_hex(32))
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        SESSION_COOKIE_SECURE=env("COOKIE_SECURE", "0") == "1",
        SESSION_COOKIE_NAME="zeitkapsel",
        PERMANENT_SESSION_LIFETIME=timedelta(hours=int(env("SESSION_HOURS", "12"))),
    )
    if local_token:
        ensure_local_host(port, local_token)
    app.register_blueprint(bp)
