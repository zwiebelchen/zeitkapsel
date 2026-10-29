#!/usr/bin/env python3
"""
oradb-ui – schmale Weboberfläche für Oracle-Datenbanken in Docker auf btrfs.

Verwaltet die Ablage aus dem Runbook:
  <BASE>/master/<SID>            Master-DBs (Subvolumes, laufen über startdb.sh)
  <BASE>/golden/<SID>/<release>  Releases (schreibgeschützte Snapshots)
  <BASE>/dev/<SID>-<name>        Entwickler-DBs (beschreibbare Snapshots)
  <BASE>/devdumps/<SID>-<name>   Data-Pump-Verzeichnisse der Entwickler-DBs
  <BASE>/devsnaps/<SID>-<name>/<snapshot>  Snapshots von Entwickler-DBs (schreibgeschützt)
  <BASE>/meta/<SID>/             Release-Beschreibungen und Timeline (nur diese Oberfläche)

Läuft als root (btrfs und docker). Nur im internen Netz betreiben.
"""
from __future__ import annotations

import datetime as dt
import hmac
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_file

# --------------------------------------------------------------------------- Konfiguration
BASE = Path(os.environ.get("ORADB_BASE", "/srv/oradb"))
IMAGE = os.environ.get("ORADB_IMAGE", "oracle/database:19.30.0-ee")
DB_HOST = os.environ.get("ORADB_DB_HOST") or socket.getfqdn()
DEV_PORT_START = int(os.environ.get("ORADB_DEV_PORT_START", "15211"))
DEV_PORT_END = int(os.environ.get("ORADB_DEV_PORT_END", "15299"))
READY_TIMEOUT = int(os.environ.get("ORADB_READY_TIMEOUT", "900"))
RELEASE_SQL_FILE = os.environ.get("ORADB_RELEASE_SQL_FILE", "")
UI_USER = os.environ.get("ORADB_UI_USER", "admin")
UI_PASSWORD = os.environ.get("ORADB_UI_PASSWORD", "")
LISTEN = os.environ.get("ORADB_UI_LISTEN", "0.0.0.0")
PORT = int(os.environ.get("ORADB_UI_PORT", "8088"))

# Wird beim Erstellen eines Releases in der Zwischen-DB ausgeführt, vor "shutdown immediate".
# Bei AMM (memory_target > 0) oder für Anonymisierung eigene Datei über ORADB_RELEASE_SQL_FILE.
DEFAULT_RELEASE_SQL = """\
alter system set sga_target=1G scope=spfile;
alter system set sga_max_size=1G scope=spfile;
alter system set pga_aggregate_target=300M scope=spfile;
"""

ORACLE_UID = 54321
READY_MARK = "DATABASE IS READY TO USE"
FAIL_MARK = "DATABASE SETUP WAS NOT SUCCESSFUL"

RE_SID = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,15}$")
RE_RELEASE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$")
RE_DEVNAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,29}$")
RE_OWNER = re.compile(r"^[\w .@-]{0,40}$")
RE_DEVID = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,15}-[a-z0-9][a-z0-9-]{0,29}$")
TEXT_MAX = 500

MASTER = BASE / "master"
GOLDEN = BASE / "golden"
DEV = BASE / "dev"
BUILD = BASE / "build"
DEVDUMPS = BASE / "devdumps"
DEVSNAPS = BASE / "devsnaps"
META = BASE / "meta"

app = Flask(__name__)
INDEX_HTML = Path(__file__).with_name("index.html")


class UserError(Exception):
    """Fehler, die der Benutzer beheben kann (ungültige Eingabe, Konflikt)."""


# --------------------------------------------------------------------------- Hilfsfunktionen
def now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def sh(cmd: list[str], *, input: str | None = None, timeout: int | None = None,
       check: bool = True) -> subprocess.CompletedProcess:
    p = subprocess.run(cmd, input=input, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd[:4])} …: {(p.stderr or p.stdout).strip()[-500:]}")
    return p


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    tmp.replace(path)


def ts_key(value) -> float:
    """Sortierschlüssel für ISO-Zeitstempel mit unterschiedlichen Zeitzonen-Offsets."""
    try:
        d = dt.datetime.fromisoformat(value)
        if d.tzinfo is None:
            d = d.astimezone()
        return d.timestamp()
    except (TypeError, ValueError):
        return 0.0


def jdbc(port: int | None, sid: str) -> str | None:
    return f"jdbc:oracle:thin:@//{DB_HOST}:{port}/{sid}" if port else None


def text_field(value, label: str, required: bool = False) -> str:
    value = (value or "").strip()
    if required and not value:
        raise UserError(f"{label} fehlt.")
    if len(value) > TEXT_MAX:
        raise UserError(f"{label} ist zu lang (höchstens {TEXT_MAX} Zeichen).")
    return value


# --------------------------------------------------------------------------- btrfs
_sv_cache: dict[str, dict] = {}


def subvol_info(path: Path) -> dict | None:
    """Erstellzeit und Schreibschutz eines Subvolumes; None, wenn path kein Subvolume ist."""
    key = str(path)
    if key in _sv_cache:
        return _sv_cache[key]
    p = sh(["btrfs", "subvolume", "show", key], check=False)
    if p.returncode != 0:
        return None
    info = {"created": None, "readonly": False}
    for line in p.stdout.splitlines():
        k, _, v = line.strip().partition(":")
        v = v.strip()
        if k == "Creation time":
            try:
                info["created"] = dt.datetime.strptime(v, "%Y-%m-%d %H:%M:%S %z").isoformat()
            except ValueError:
                info["created"] = v
        elif k == "Flags":
            info["readonly"] = "readonly" in v
    _sv_cache[key] = info
    return info


def sv_forget(path: Path) -> None:
    _sv_cache.pop(str(path), None)


def check_volume(vol: Path, sid: str) -> None:
    """Dieselbe Prüfung wie startdb.sh: ohne Markierungsdateien würde das Oracle-Skript löschen."""
    missing = []
    for p in (vol / f".{sid}.created", vol / "dbconfig" / sid / f"spfile{sid}.ora",
              vol / "dbconfig" / sid / f"orapw{sid}"):
        if not p.exists():
            missing.append(str(p.relative_to(vol)))
    if not any((vol / sid).glob("*.dbf")):
        missing.append(f"{sid}/*.dbf")
    if not list((vol / "dbconfig" / sid).glob(".docker_*")):
        missing.append(f"dbconfig/{sid}/.docker_<edition>")
    if missing:
        raise RuntimeError("Fehlende Dateien, Start abgebrochen: " + ", ".join(missing))


# --------------------------------------------------------------------------- Docker
_ready_cache: dict[tuple, str] = {}


def containers() -> list[dict]:
    ids = sh(["docker", "ps", "-aq", "--no-trunc"]).stdout.split()
    if not ids:
        return []
    raw = sh(["docker", "inspect", *ids], check=False).stdout or "[]"
    out = []
    for c in json.loads(raw):
        binds = (c["HostConfig"].get("PortBindings") or {}).get("1521/tcp") or []
        port = int(binds[0]["HostPort"]) if binds and binds[0].get("HostPort") else None
        mount = next((m["Source"] for m in c.get("Mounts", [])
                      if m.get("Destination") == "/opt/oracle/oradata"), None)
        st = c["State"]
        out.append(dict(
            id=c["Id"], name=c["Name"].lstrip("/"), labels=c["Config"].get("Labels") or {},
            port=port, mount=os.path.realpath(mount) if mount else None,
            running=bool(st.get("Running")), restarting=bool(st.get("Restarting")),
            started=st.get("StartedAt"),
        ))
    return out


def db_state(c: dict | None) -> str:
    if c is None:
        return "kein Container"
    if c["restarting"]:
        return "startet neu"
    if not c["running"]:
        return "gestoppt"
    key = (c["id"], c["started"])
    if _ready_cache.get(key) in ("bereit", "Fehler"):
        return _ready_cache[key]
    p = sh(["docker", "logs", "--since", c["started"], c["id"]], check=False)
    text = p.stdout + p.stderr
    state = "Fehler" if FAIL_MARK in text else "bereit" if READY_MARK in text else "startet"
    _ready_cache[key] = state
    return state


def next_port(cs: list[dict]) -> int:
    used = {c["port"] for c in cs if c["port"]}
    for port in range(DEV_PORT_START, DEV_PORT_END + 1):
        if port in used:
            continue
        with socket.socket() as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("0.0.0.0", port))
            except OSError:
                continue
        return port
    raise RuntimeError(f"Kein freier Port zwischen {DEV_PORT_START} und {DEV_PORT_END}.")


# --------------------------------------------------------------------------- Jobs
class Job:
    def __init__(self, title: str, sid: str | None):
        self.id = uuid.uuid4().hex[:10]
        self.title, self.sid = title, sid
        self.status, self.error = "läuft", None
        self.started, self.ended = now_iso(), None
        self.lines: list[str] = []

    def log(self, msg: str) -> None:
        self.lines.append(f"{time.strftime('%H:%M:%S')}  {msg}")
        del self.lines[:-500]

    def run(self, cmd: list[str], **kw) -> subprocess.CompletedProcess:
        self.log("$ " + " ".join(cmd))
        p = sh(cmd, check=False, **kw)
        for line in (p.stdout + p.stderr).strip().splitlines()[-30:]:
            self.log("  " + line)
        if p.returncode != 0:
            raise RuntimeError(f"„{' '.join(cmd[:3])} …“ ist mit Exit-Code {p.returncode} fehlgeschlagen.")
        return p

    def as_dict(self, with_log: bool = False) -> dict:
        d = dict(id=self.id, title=self.title, sid=self.sid, status=self.status,
                 error=self.error, started=self.started, ended=self.ended)
        if with_log:
            d["log"] = self.lines
        return d


JOBS: dict[str, Job] = {}
_busy: set[str] = set()
_lock = threading.Lock()        # schützt JOBS und _busy
_port_lock = threading.Lock()   # Portvergabe bis zum docker run
_tl_lock = threading.Lock()


def start_job(title: str, sid: str | None, busy_key: str, fn, *args) -> Job:
    with _lock:
        if busy_key in _busy:
            raise UserError("Dafür läuft gerade schon ein Vorgang.")
        _busy.add(busy_key)
        job = Job(title, sid)
        JOBS[job.id] = job
        for old in sorted(JOBS.values(), key=lambda j: j.started)[:-50]:
            if old.status != "läuft":
                JOBS.pop(old.id, None)

    def worker():
        try:
            fn(job, *args)
            job.status = "fertig"
            job.log("Fertig.")
        except Exception as e:  # noqa: BLE001 – jeder Fehler landet im Job-Protokoll
            job.status, job.error = "fehlgeschlagen", str(e)
            job.log("FEHLER: " + str(e))
        finally:
            job.ended = now_iso()
            with _lock:
                _busy.discard(busy_key)

    threading.Thread(target=worker, daemon=True).start()
    return job


def wait_ready(job: Job, cname: str, since: float) -> None:
    job.log(f"Warte auf „{READY_MARK}“ (höchstens {READY_TIMEOUT // 60} Minuten) …")
    t0 = time.time()
    while time.time() - t0 < READY_TIMEOUT:
        p = sh(["docker", "logs", "--since", str(int(since) - 2), cname], check=False)
        text = p.stdout + p.stderr
        if FAIL_MARK in text:
            raise RuntimeError(f"Datenbankstart fehlgeschlagen, siehe „docker logs {cname}“.")
        if READY_MARK in text:
            job.log(f"Datenbank bereit nach {int(time.time() - t0)} s.")
            return
        running = sh(["docker", "inspect", "-f", "{{.State.Running}}", cname], check=False).stdout.strip()
        if running != "true":
            raise RuntimeError(f"Container {cname} läuft nicht mehr, siehe „docker logs {cname}“.")
        time.sleep(5)
    raise RuntimeError("Zeitüberschreitung beim Warten auf die Datenbank.")


# --------------------------------------------------------------------------- Timeline und Metadaten
def tl_file(sid: str) -> Path:
    return META / sid / "timeline.jsonl"


def tl_read(sid: str) -> list[dict]:
    f = tl_file(sid)
    if not f.exists():
        return []
    out = []
    for line in f.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


def tl_write(sid: str, entries: list[dict]) -> None:
    f = tl_file(sid)
    f.parent.mkdir(parents=True, exist_ok=True)
    tmp = f.with_suffix(".tmp")
    tmp.write_text("".join(json.dumps(e, ensure_ascii=False) + "\n" for e in entries))
    tmp.replace(f)


def tl_add(sid: str, kind: str, text: str, ref: str | None = None, ts: str | None = None) -> dict:
    entry = dict(id=uuid.uuid4().hex[:12], ts=ts or now_iso(), kind=kind, text=text, ref=ref)
    with _tl_lock:
        f = tl_file(sid)
        f.parent.mkdir(parents=True, exist_ok=True)
        with f.open("a") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return entry


def release_meta_file(sid: str, name: str) -> Path:
    return META / sid / "releases" / f"{name}.json"


def release_meta(sid: str, name: str) -> dict:
    f = release_meta_file(sid, name)
    return json.loads(f.read_text()) if f.exists() else {}


# --------------------------------------------------------------------------- Zustand lesen
def master_sids() -> list[str]:
    if not MASTER.is_dir():
        return []
    return sorted(p.name for p in MASTER.iterdir() if p.is_dir() and RE_SID.match(p.name))


def need_sid(sid: str) -> str:
    if not RE_SID.match(sid or "") or not (MASTER / sid).is_dir():
        raise UserError(f"Unbekannte Datenbank „{sid}“.")
    return sid


def releases(sid: str) -> list[dict]:
    d = GOLDEN / sid
    out = []
    if d.is_dir():
        for p in d.iterdir():
            info = subvol_info(p) if p.is_dir() else None
            if info is None:
                continue
            m = release_meta(sid, p.name)
            out.append(dict(name=p.name, created=m.get("created") or info["created"],
                            description=m.get("description", ""), readonly=info["readonly"],
                            parent=m.get("parent", ""), lineage=m.get("lineage", ""),
                            origin=m.get("origin", ""), schedule=m.get("schedule", "")))
    out.sort(key=lambda r: ts_key(r["created"]), reverse=True)
    return out


def devsnap_meta_file(sid: str, did: str, snap: str) -> Path:
    return META / sid / "devsnaps" / f"{did}__{snap}.json"


def devsnaps_of(sid: str, did: str) -> list[dict]:
    d = DEVSNAPS / did
    out = []
    if d.is_dir():
        for p in d.iterdir():
            info = subvol_info(p) if p.is_dir() else None
            if info is None:
                continue
            f = devsnap_meta_file(sid, did, p.name)
            m = json.loads(f.read_text()) if f.exists() else {}
            out.append(dict(name=p.name, dev=did, created=m.get("created") or info["created"],
                            description=m.get("description", ""), consistent=m.get("consistent"),
                            release=m.get("release", ""), source=f"snap:{did}/{p.name}",
                            schedule=m.get("schedule", "")))
    out.sort(key=lambda r: ts_key(r["created"]), reverse=True)
    return out


def resolve_source(sid: str, source: str) -> tuple[Path, str, str]:
    """Quelle einer neuen Entwickler-DB oder eines Releases.

    source = "release:<name>" oder "snap:<SID>-<dev>/<snapshot>".
    Liefert (Pfad, lesbare Bezeichnung, Release-Herkunft).
    """
    kind, _, rest = (source or "").partition(":")
    if kind == "release":
        if not RE_RELEASE.match(rest) or not (GOLDEN / sid / rest).is_dir():
            raise UserError("Release nicht gefunden.")
        return GOLDEN / sid / rest, f"Release {rest}", rest
    if kind == "snap":
        did, _, snap = rest.partition("/")
        if (not RE_DEVID.match(did) or not did.startswith(sid + "-") or not RE_RELEASE.match(snap)
                or not (DEVSNAPS / did / snap).is_dir()):
            raise UserError("Snapshot nicht gefunden.")
        f = devsnap_meta_file(sid, did, snap)
        release = (json.loads(f.read_text()) if f.exists() else {}).get("release", "")
        return DEVSNAPS / did / snap, f"Snapshot {snap} von {did.partition('-')[2]}", release
    raise UserError("Unbekannte Quelle.")


def base_label(dev: dict) -> str:
    if dev["base"].startswith("snap:"):
        did, _, snap = dev["base"][5:].partition("/")
        return f"Snapshot {snap} von {did.partition('-')[2]}"
    return f"Release {dev['base'][8:]}" if dev["base"] else ""


def devdbs(cs: list[dict]) -> list[dict]:
    items: dict[str, dict] = {}
    dev_root = os.path.realpath(DEV)
    for c in cs:
        L = c["labels"]
        in_dev = c["mount"] and os.path.dirname(c["mount"]) == dev_root
        if L.get("devdb") != "1" and not in_dev:
            continue
        did = os.path.basename(c["mount"]) if c["mount"] else c["name"]
        sid, _, name = did.partition("-")
        release = L.get("devrelease", "")
        items[did] = dict(
            id=did, sid=sid, name=name, release=release, owner=L.get("devowner", ""),
            base=L.get("devbase") or (f"release:{release}" if release else ""),
            created=L.get("devcreated", ""), port=c["port"], container=c["name"],
            state=db_state(c), jdbc=jdbc(c["port"], sid),
        )
    if DEV.is_dir():
        for p in DEV.iterdir():
            if p.name in items or not p.is_dir():
                continue
            sid, _, name = p.name.partition("-")
            info = subvol_info(p) or {}
            items[p.name] = dict(id=p.name, sid=sid, name=name, release="", owner="", base="",
                                 created=info.get("created") or "", port=None, container=None,
                                 state="ohne Container", jdbc=None)
    for d in items.values():
        d["base_label"] = base_label(d)
        d["snapshots"] = devsnaps_of(d["sid"], d["id"])
    return sorted(items.values(), key=lambda d: d["id"])


def master_info(sid: str, cs: list[dict]) -> dict:
    vol = os.path.realpath(MASTER / sid)
    c = next((c for c in cs if c["mount"] == vol), None)
    port = c["port"] if c else None
    return dict(container=c["name"] if c else None, state=db_state(c), port=port, jdbc=jdbc(port, sid))


def timeline(sid: str, rels: list[dict], devs: list[dict]) -> list[dict]:
    entries = [dict(e) for e in tl_read(sid)]
    rel_by_name = {r["name"]: r for r in rels}
    logged = {e.get("ref") for e in entries if e["kind"] == "release"}
    for r in rels:   # Releases, die nicht über die Oberfläche entstanden sind (z. B. golden.sh)
        if r["name"] not in logged:
            entries.append(dict(id="rel-" + r["name"], ts=r["created"] or "", kind="release",
                                text=f"Release {r['name']}", ref=r["name"], external=True))
    newest_release_entry: dict[str, str] = {}
    for e in sorted(entries, key=lambda x: ts_key(x["ts"])):
        if e["kind"] == "release":
            newest_release_entry[e["ref"]] = e["id"]
    for e in entries:
        if e["kind"] == "release":
            r = rel_by_name.get(e["ref"])
            current = r is not None and newest_release_entry.get(e["ref"]) == e["id"]
            e["exists"] = current
            e["description"] = r["description"] if current else ""
            e["branches"] = [dict(id=d["id"], state=d["state"]) for d in devs
                             if current and d["release"] == e["ref"]]
    entries.sort(key=lambda x: ts_key(x["ts"]), reverse=True)
    return entries


# --------------------------------------------------------------------------- Vorgänge (laufen als Job)
def job_release(job: Job, sid: str, name: str, description: str,
                src: Path | None = None, origin: str = "", parent: str = "", lineage: str = "",
                schedule: str = "") -> None:
    src = src or MASTER / sid
    dst = GOLDEN / sid / name
    stamp = time.strftime("%Y%m%d-%H%M%S")
    tmp = BUILD / f"{sid}-{name}-{stamp}"
    bname = f"release-build-{sid.lower()}-{stamp}"
    (GOLDEN / sid).mkdir(parents=True, exist_ok=True)
    BUILD.mkdir(parents=True, exist_ok=True)

    job.log(f"Snapshot von {origin or 'Master-DB ' + sid} (die Quelle läuft weiter)")
    job.run(["btrfs", "subvolume", "snapshot", str(src), str(tmp)])
    try:
        check_volume(tmp, sid)
        t0 = time.time()
        job.run(["docker", "run", "-d", "--name", bname, "--init", "-e", f"ORACLE_SID={sid}",
                 "-v", f"{tmp}:/opt/oracle/oradata", IMAGE])
        wait_ready(job, bname, t0)
        sql = Path(RELEASE_SQL_FILE).read_text() if RELEASE_SQL_FILE else DEFAULT_RELEASE_SQL
        job.log("Einstellungen für Entwickler-DBs setzen, sauber herunterfahren")
        job.run(["docker", "exec", "-i", bname, "sqlplus", "-s", "/ as sysdba"],
                input="whenever sqlerror exit failure\n" + sql + "\nshutdown immediate\nexit\n",
                timeout=1800)
        job.run(["docker", "rm", "-f", bname])
        job.log("Release schreibgeschützt einfrieren")
        job.run(["btrfs", "subvolume", "snapshot", "-r", str(tmp), str(dst)])
    finally:
        sh(["docker", "rm", "-f", bname], check=False)
        if tmp.exists():
            sh(["btrfs", "subvolume", "delete", str(tmp)], check=False)
            sv_forget(tmp)

    write_json(release_meta_file(sid, name),
               dict(name=name, description=description, created=now_iso(), origin=origin or "Master-DB",
                    parent=parent, lineage=lineage, schedule=schedule))
    tl_add(sid, "release", f"Release {name}" + (f" aus {origin}" if origin else ""), ref=name)


def job_release_derive(job: Job, sid: str, name: str, description: str,
                       src: Path, origin: str, parent: str, lineage: str) -> None:
    """Release von einem bestehenden Release ableiten: das Quell-Release ist bereits sauber
    geschlossen und für Entwickler eingerichtet, daher genügt ein schreibgeschützter Snapshot."""
    (GOLDEN / sid).mkdir(parents=True, exist_ok=True)
    job.run(["btrfs", "subvolume", "snapshot", "-r", str(src), str(GOLDEN / sid / name)])
    write_json(release_meta_file(sid, name),
               dict(name=name, description=description, created=now_iso(), origin=origin,
                    parent=parent, lineage=lineage))
    tl_add(sid, "release", f"Release {name} aus {origin}", ref=name)


def dev_run_cmd(did: str, sid: str, port: int, release: str, owner: str, created: str,
                base: str) -> list[str]:
    return ["docker", "run", "-d", "--name", did.lower(), "--init", "--stop-timeout", "600",
            "--label", "devdb=1", "--label", f"devport={port}", "--label", f"devsid={sid}",
            "--label", f"devrelease={release}", "--label", f"devowner={owner}",
            "--label", f"devbase={base}",
            "--label", f"devcreated={created}",
            "-p", f"{port}:1521", "-e", f"ORACLE_SID={sid}",
            "-v", f"{DEV / did}:/opt/oracle/oradata",
            "-v", f"{DEVDUMPS / did}:/oracle/dumps", IMAGE]


def prepare_dumps(did: str, sid: str) -> None:
    (DEVDUMPS / did / "19c" / "datapump" / sid).mkdir(parents=True, exist_ok=True)
    sh(["chown", "-R", f"{ORACLE_UID}:{ORACLE_UID}", str(DEVDUMPS / did)])


def job_dev_create(job: Job, sid: str, name: str, source: str, owner: str) -> None:
    src, label, release = resolve_source(sid, source)
    did = f"{sid}-{name}"
    vol = DEV / did
    DEV.mkdir(parents=True, exist_ok=True)
    job.run(["btrfs", "subvolume", "snapshot", str(src), str(vol)])
    try:
        check_volume(vol, sid)
        prepare_dumps(did, sid)
        with _port_lock:
            port = next_port(containers())
            job.log(f"Port {port}")
            t0 = time.time()
            job.run(dev_run_cmd(did, sid, port, release, owner, now_iso(), source))
    except Exception:
        sh(["docker", "rm", "-f", did.lower()], check=False)
        sh(["btrfs", "subvolume", "delete", str(vol)], check=False)
        shutil.rmtree(DEVDUMPS / did, ignore_errors=True)
        sv_forget(vol)
        raise
    tl_add(sid, "dev-created", f"Entwickler-DB {name} aus {label} angelegt"
           + (f" für {owner}" if owner else ""), ref=did)
    wait_ready(job, did.lower(), t0)


def job_dev_recreate(job: Job, dev: dict, source: str, new_base: bool = False) -> None:
    """Entwickler-DB durch einen frischen Snapshot von source ersetzen (Port bleibt).

    new_base=True macht source zum neuen Ausgangsstand (z. B. beim Wechsel auf das neueste Release)."""
    did, sid = dev["id"], dev["sid"]
    src, label, lineage = resolve_source(sid, source)
    base = source if new_base else dev["base"]
    release = lineage if new_base else dev["release"]
    vol = DEV / did
    job.run(["docker", "rm", "-f", dev["container"]])
    job.run(["btrfs", "subvolume", "delete", str(vol)])
    sv_forget(vol)
    job.run(["btrfs", "subvolume", "snapshot", str(src), str(vol)])
    check_volume(vol, sid)
    prepare_dumps(did, sid)
    with _port_lock:
        t0 = time.time()
        job.run(dev_run_cmd(did, sid, dev["port"], release, dev["owner"],
                            dev["created"] or now_iso(), base))
    tl_add(sid, "dev-reset", f"Entwickler-DB {dev['name']} auf {label} zurückgesetzt", ref=did)
    wait_ready(job, did.lower(), t0)


def job_dev_snapshot(job: Job, dev: dict, snap: str, description: str, consistent: bool,
                     schedule: str = "") -> None:
    did, sid = dev["id"], dev["sid"]
    running = dev["state"] in ("bereit", "startet", "startet neu")
    stop_first = consistent and running
    (DEVSNAPS / did).mkdir(parents=True, exist_ok=True)
    if stop_first:
        job.log("Entwickler-DB sauber herunterfahren")
        job.run(["docker", "stop", "-t", "600", dev["container"]], timeout=700)
    else:
        job.log("Snapshot im laufenden Betrieb (absturzkonsistent, Oracle führt beim Start eine Instance Recovery aus)"
                if running else "Entwickler-DB ist gestoppt, Snapshot ist konsistent")
    try:
        job.run(["btrfs", "subvolume", "snapshot", "-r", str(DEV / did), str(DEVSNAPS / did / snap)])
    finally:
        if stop_first:
            t0 = time.time()
            job.run(["docker", "start", dev["container"]])
            wait_ready(job, dev["container"], t0)
    write_json(devsnap_meta_file(sid, did, snap),
               dict(name=snap, description=description, created=now_iso(),
                    consistent=consistent or not running, release=dev["release"], base=dev["base"],
                    schedule=schedule))
    tl_add(sid, "devsnap", f"Snapshot {snap} von Entwickler-DB {dev['name']} erstellt", ref=f"{did}/{snap}")


def delete_devsnap(sid: str, did: str, snap: str) -> None:
    path = DEVSNAPS / did / snap
    sh(["btrfs", "subvolume", "delete", str(path)])
    sv_forget(path)
    devsnap_meta_file(sid, did, snap).unlink(missing_ok=True)


def job_dev_delete(job: Job, dev: dict) -> None:
    did = dev["id"]
    if dev["container"]:
        job.run(["docker", "rm", "-f", dev["container"]])
    if (DEV / did).exists():
        job.run(["btrfs", "subvolume", "delete", str(DEV / did)])
        sv_forget(DEV / did)
    shutil.rmtree(DEVDUMPS / did, ignore_errors=True)
    for snap in dev["snapshots"]:
        job.log(f"Snapshot {snap['name']} löschen")
        delete_devsnap(dev["sid"], did, snap["name"])
    if (DEVSNAPS / did).is_dir():
        try:
            (DEVSNAPS / did).rmdir()
        except OSError:
            pass
    tl_add(dev["sid"], "dev-deleted", f"Entwickler-DB {dev['name']} gelöscht"
           + (f" (mit {len(dev['snapshots'])} Snapshot(s))" if dev["snapshots"] else ""), ref=did)


def job_dev_start(job: Job, dev: dict) -> None:
    check_volume(DEV / dev["id"], dev["sid"])
    t0 = time.time()
    job.run(["docker", "start", dev["container"]])
    wait_ready(job, dev["container"], t0)


def job_dev_stop(job: Job, dev: dict) -> None:
    job.run(["docker", "stop", "-t", "600", dev["container"]], timeout=700)


# --------------------------------------------------------------------------- Zeitpläne
WEEKDAYS = ["mo", "di", "mi", "do", "fr", "sa", "so"]
ACTIONS = {
    "release": "Release aus Master-DB erstellen",
    "devsnap": "Snapshot einer Entwickler-DB erstellen",
    "stopdevs": "Entwickler-DBs stoppen",
    "resetdevs": "Entwickler-DBs zurücksetzen",
}
RESET_TARGETS = {"base": "auf ihren Ausgangsstand", "latest": "auf das neueste Release"}
RE_TIME = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")
_sched_lock = threading.Lock()


def sched_file() -> Path:
    return META / "schedules.json"


def sched_load() -> list[dict]:
    f = sched_file()
    return json.loads(f.read_text()) if f.exists() else []


def sched_save(items: list[dict]) -> None:
    write_json(sched_file(), items)


def sched_update(sid_: str, fn) -> None:
    with _sched_lock:
        items = sched_load()
        for sc in items:
            if sc["id"] == sid_:
                fn(sc)
        sched_save(items)


def expand_name(template: str, when: dt.datetime) -> str:
    return (template.replace("{datum}", when.strftime("%Y-%m-%d"))
            .replace("{zeit}", when.strftime("%H%M"))
            .replace("{wochentag}", WEEKDAYS[when.weekday()]))


def unique_name(base: str, exists) -> str:
    name, i = base, 2
    while exists(name):
        name = f"{base[:36]}-{i}"
        i += 1
    return name


def validate_schedule(data: dict) -> dict:
    action = data.get("action")
    if action not in ACTIONS:
        raise UserError("Unbekannte Aktion.")
    sid = need_sid(data.get("sid") or "")
    label = text_field(data.get("label"), "Bezeichnung", required=True)[:80]
    days = sorted({int(d) for d in data.get("days") or [] if str(d).isdigit() and 0 <= int(d) <= 6})
    if not days:
        raise UserError("Mindestens einen Wochentag wählen.")
    out = dict(action=action, sid=sid, label=label, days=days, enabled=bool(data.get("enabled", True)))
    out.update(validate_timing(data))
    if action in ("release", "devsnap"):
        tpl = (data.get("template") or "").strip()
        sample = expand_name(tpl, dt.datetime(2026, 12, 31, 23, 59))
        if not tpl or not RE_RELEASE.match(sample) or len(sample) > 36:
            raise UserError("Namensmuster ergibt keinen gültigen Namen (Buchstaben, Ziffern, . _ -; "
                            "Platzhalter {datum}, {zeit}, {wochentag}; höchstens 36 Zeichen).")
        keep = int(data.get("keep") or 0)
        if keep < 0 or keep > 999:
            raise UserError("Aufbewahren: 0 (alle behalten) bis 999.")
        out.update(template=tpl, keep=keep)
    if action == "resetdevs":
        devs = sorted({str(d) for d in data.get("devs") or []})
        if not devs or not all(RE_DEVID.match(d) and d.startswith(sid + "-") for d in devs):
            raise UserError("Mindestens eine Entwickler-DB wählen.")
        target = data.get("target") or "base"
        if target not in RESET_TARGETS:
            raise UserError("Unbekanntes Ziel.")
        out.update(devs=devs, target=target, keep_stopped=bool(data.get("keep_stopped", True)))
    if action == "devsnap":
        did = (data.get("dev") or "").strip()
        if not RE_DEVID.match(did) or not did.startswith(sid + "-"):
            raise UserError("Entwickler-DB wählen.")
        out.update(dev=did, consistent=bool(data.get("consistent")))
    return out


FREQS = {"daily": "täglich zu einer Uhrzeit", "hours": "stündlich bzw. alle n Stunden",
         "minutes": "alle n Minuten"}


def hm(value: str) -> int:
    hh, mm = map(int, value.split(":"))
    return hh * 60 + mm


def int_field(value, label: str, lo: int, hi: int) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise UserError(f"{label}: Zahl von {lo} bis {hi} angeben.") from None
    if not lo <= n <= hi:
        raise UserError(f"{label}: Zahl von {lo} bis {hi} angeben.")
    return n


def validate_timing(data: dict) -> dict:
    freq = data.get("freq") or "daily"
    if freq not in FREQS:
        raise UserError("Unbekannte Häufigkeit.")
    out = dict(freq=freq)
    if freq == "daily":
        t = (data.get("time") or "").strip()
        if not RE_TIME.match(t):
            raise UserError("Uhrzeit im Format HH:MM angeben.")
        out["time"] = t
        return out
    if freq == "hours":
        out["every"] = int_field(data.get("every"), "Alle … Stunden", 1, 23)
        out["minute"] = int_field(data.get("minute"), "Zur Minute", 0, 59)
    else:
        out["every"] = int_field(data.get("every"), "Alle … Minuten", 1, 720)
    w_from, w_to = (data.get("window_from") or "").strip(), (data.get("window_to") or "").strip()
    if w_from or w_to:
        if not (RE_TIME.match(w_from) and RE_TIME.match(w_to)):
            raise UserError("Zeitfenster: Beginn und Ende im Format HH:MM angeben.")
        out["window"] = dict(start=w_from, end=w_to)
    return out


def slots_on(sc: dict, day: dt.date) -> list[int]:
    """Geplante Zeitpunkte eines Tages in Minuten seit Mitternacht."""
    if day.weekday() not in sc["days"]:
        return []
    freq = sc.get("freq", "daily")
    if freq == "daily":
        return [hm(sc["time"])]
    if freq == "hours":
        slots = [h * 60 + sc["minute"] for h in range(0, 24, sc["every"])]
    else:
        slots = list(range(0, 1440, sc["every"]))
    w = sc.get("window")
    if w:
        a, b = hm(w["start"]), hm(w["end"])
        if a <= b:
            slots = [m for m in slots if a <= m <= b]
        else:                                           # Fenster über Mitternacht, z. B. 22:00–06:00
            slots = [m for m in slots if m >= a or m <= b]
    return slots


def slot_key(day: dt.date, minute: int) -> str:
    return f"{day.isoformat()}T{minute // 60:02d}:{minute % 60:02d}"


def due_slot(sc: dict, now: dt.datetime) -> str | None:
    """Jüngster heute schon erreichte Zeitpunkt (verpasste ältere werden nicht nachgeholt)."""
    cur = now.hour * 60 + now.minute
    past = [m for m in slots_on(sc, now.date()) if m <= cur]
    return slot_key(now.date(), past[-1]) if past else None


def last_slot(sc: dict) -> str | None:
    if sc.get("last_slot"):
        return sc["last_slot"]
    if sc.get("last_date") and sc.get("time"):          # Zeitpläne aus der Vorversion
        return f"{sc['last_date']}T{sc['time']}"
    return None


def mark_if_passed(sc: dict, now: dt.datetime | None = None) -> None:
    """Bereits verstrichene Zeitpunkte nicht nachträglich ausführen (beim Anlegen/Ändern)."""
    now = now or dt.datetime.now()
    sc.pop("last_date", None)
    sc["last_slot"] = due_slot(sc, now)


def retention_releases(job: Job, sid: str, sched_id: str, keep: int) -> None:
    if keep <= 0:
        return
    rels = releases(sid)
    mine = [r for r in rels if r["schedule"] == sched_id]
    devs = devdbs(containers())
    for r in mine[keep:]:
        key = "release:" + r["name"]
        deps = [d["name"] for d in devs if d["base"] == key] + [x["name"] for x in rels if x["parent"] == key]
        if deps:
            job.log(f"Release {r['name']} bleibt, daran hängen noch: {', '.join(deps)}")
            continue
        job.log(f"Aufbewahrung: Release {r['name']} löschen")
        job.run(["btrfs", "subvolume", "delete", str(GOLDEN / sid / r["name"])])
        sv_forget(GOLDEN / sid / r["name"])
        release_meta_file(sid, r["name"]).unlink(missing_ok=True)
        tl_add(sid, "release-deleted", f"Release {r['name']} gelöscht (Aufbewahrung)", ref=r["name"])


def retention_devsnaps(job: Job, sid: str, did: str, sched_id: str, keep: int) -> None:
    if keep <= 0:
        return
    mine = [n for n in devsnaps_of(sid, did) if n["schedule"] == sched_id]
    devs = devdbs(containers())
    rels = releases(sid)
    for n in mine[keep:]:
        deps = [d["name"] for d in devs if d["base"] == n["source"]] + [r["name"] for r in rels if r["parent"] == n["source"]]
        if deps:
            job.log(f"Snapshot {n['name']} bleibt, daran hängen noch: {', '.join(deps)}")
            continue
        job.log(f"Aufbewahrung: Snapshot {n['name']} löschen")
        delete_devsnap(sid, did, n["name"])
        tl_add(sid, "devsnap-deleted", f"Snapshot {n['name']} von {did.partition('-')[2]} gelöscht (Aufbewahrung)",
               ref=f"{did}/{n['name']}")


def job_scheduled(job: Job, sc: dict, when: dt.datetime) -> None:
    status = "fehlgeschlagen"
    try:
        sid = sc["sid"]
        desc = f"Automatisch durch Zeitplan „{sc['label']}“"
        if sc["action"] == "release":
            name = unique_name(expand_name(sc["template"], when), lambda n: (GOLDEN / sid / n).exists())
            job.log(f"Release-Name: {name}")
            job_release(job, sid, name, desc, schedule=sc["id"])
            retention_releases(job, sid, sc["id"], sc["keep"])
        elif sc["action"] == "devsnap":
            dev = find_dev(sc["dev"])
            if not dev["container"]:
                raise RuntimeError(f"Entwickler-DB {dev['name']} hat keinen Container.")
            name = unique_name(expand_name(sc["template"], when), lambda n: (DEVSNAPS / dev["id"] / n).exists())
            job.log(f"Snapshot-Name: {name}")
            job_dev_snapshot(job, dev, name, desc, sc.get("consistent", False), schedule=sc["id"])
            retention_devsnaps(job, sid, dev["id"], sc["id"], sc["keep"])
        elif sc["action"] == "stopdevs":
            stopped = 0
            for d in devdbs(containers()):
                if d["sid"] != sid or not d["container"] or d["state"] not in ("bereit", "startet", "startet neu"):
                    continue
                with _lock:
                    if f"dev:{d['id']}" in _busy:
                        job.log(f"{d['name']}: Vorgang läuft, übersprungen")
                        continue
                job.run(["docker", "stop", "-t", "600", d["container"]], timeout=700)
                stopped += 1
            job.log(f"{stopped} Entwickler-DB(s) gestoppt")
        elif sc["action"] == "resetdevs":
            failed = reset_devs(job, sid, sc["devs"], sc["target"], sc.get("keep_stopped", True))
            if failed:
                raise RuntimeError("Nicht zurückgesetzt: " + ", ".join(failed))
        status = "fertig"
    finally:
        final = status
        sched_update(sc["id"], lambda x: x.setdefault("last_run", {}).update(status=final, ended=now_iso()))


def reset_devs(job: Job, sid: str, ids: list[str], target: str, keep_stopped: bool) -> list[str]:
    """Setzt die gewählten Entwickler-DBs nacheinander zurück; liefert die Namen der fehlgeschlagenen."""
    failed = []
    all_devs = {d["id"]: d for d in devdbs(containers())}
    latest = next((r["name"] for r in releases(sid)), None)
    for did in ids:
        dev = all_devs.get(did)
        if not dev or not dev["container"] or not dev["port"]:
            job.log(f"{did}: nicht gefunden oder ohne Container, übersprungen")
            failed.append(did.partition("-")[2])
            continue
        if target == "latest":
            if not latest:
                job.log("Kein Release vorhanden.")
                failed.append(dev["name"])
                continue
            source, new_base = f"release:{latest}", True
        else:
            source, new_base = dev["base"], False
        key = f"dev:{did}"
        with _lock:
            if key in _busy:
                job.log(f"{dev['name']}: es läuft gerade ein anderer Vorgang, übersprungen")
                failed.append(dev["name"])
                continue
            _busy.add(key)
        was_stopped = dev["state"] == "gestoppt"
        try:
            job.log(f"— {dev['name']} zurücksetzen auf {source}")
            job_dev_recreate(job, dev, source, new_base)
            if was_stopped and keep_stopped:
                job.log(f"{dev['name']} war gestoppt und wird wieder gestoppt")
                job.run(["docker", "stop", "-t", "600", dev["container"]], timeout=700)
        except Exception as e:  # noqa: BLE001 – mit der nächsten DB weitermachen
            job.log(f"{dev['name']}: FEHLER {e}")
            failed.append(dev["name"])
        finally:
            with _lock:
                _busy.discard(key)
    return failed


def busy_key_for(sc: dict) -> str:
    return {"release": f"release:{sc['sid']}", "devsnap": f"dev:{sc.get('dev')}",
            "stopdevs": f"stopdevs:{sc['sid']}", "resetdevs": f"resetdevs:{sc['id']}"}[sc["action"]]


def fire_schedule(sc: dict, when: dt.datetime) -> None:
    """Startet einen Zeitplan als Vorgang; trägt das Ergebnis in sc["last_run"] ein (sc wird geändert)."""
    try:
        job = start_job(f"Zeitplan „{sc['label']}“", sc["sid"], busy_key_for(sc), job_scheduled, dict(sc), when)
        sc["last_run"] = dict(ts=now_iso(), job=job.id, status="läuft")
    except Exception as e:  # noqa: BLE001
        sc["last_run"] = dict(ts=now_iso(), status="übersprungen", error=str(e))


def scheduler_tick(now: dt.datetime | None = None) -> None:
    now = now or dt.datetime.now()
    with _sched_lock:
        items = sched_load()
        changed = False
        for sc in items:
            if not sc.get("enabled"):
                continue
            key = due_slot(sc, now)
            if key is None or key == last_slot(sc):
                continue
            sc.pop("last_date", None)
            sc["last_slot"] = key
            fire_schedule(sc, now)
            changed = True
        if changed:
            sched_save(items)


def scheduler_loop() -> None:
    while True:
        try:
            scheduler_tick()
        except Exception:  # noqa: BLE001
            app.logger.exception("Fehler im Zeitplaner")
        time.sleep(20)


def next_run(sc: dict, now: dt.datetime | None = None) -> str | None:
    if not sc.get("enabled"):
        return None
    now = now or dt.datetime.now()
    cur = now.hour * 60 + now.minute
    for add in range(0, 8):
        day = now.date() + dt.timedelta(days=add)
        for m in slots_on(sc, day):
            if add == 0 and m <= cur:
                continue
            return dt.datetime.combine(day, dt.time(m // 60, m % 60)).astimezone().isoformat(timespec="minutes")
    return None


# --------------------------------------------------------------------------- Zugriffsschutz
@app.before_request
def guard():
    auth = request.authorization
    ok = (auth is not None and auth.type == "basic"
          and hmac.compare_digest((auth.username or "").encode(), UI_USER.encode())
          and hmac.compare_digest((auth.password or "").encode(), UI_PASSWORD.encode()))
    if not ok:
        return Response("Anmeldung erforderlich.", 401, {"WWW-Authenticate": 'Basic realm="oradb-ui"'})
    # Schutz gegen Cross-Site-Requests: ändernde Aufrufe nur mit eigenem Header (nur per fetch setzbar)
    if request.method not in ("GET", "HEAD") and request.headers.get("X-Requested-With") != "oradb-ui":
        return jsonify(error="Anfrage abgelehnt."), 403
    return None


@app.errorhandler(UserError)
def on_user_error(e):
    return jsonify(error=str(e)), 400


@app.errorhandler(Exception)
def on_error(e):
    code = getattr(e, "code", 500)
    if isinstance(code, int) and code < 500:
        return jsonify(error=str(e)), code
    app.logger.exception("Fehler")
    return jsonify(error=str(e)), 500


# --------------------------------------------------------------------------- Routen
@app.get("/")
def index():
    return send_file(INDEX_HTML)


@app.get("/api/state")
def api_state():
    cs = containers()
    devs = devdbs(cs)
    dbs = []
    for sid in master_sids():
        rels = releases(sid)
        my_devs = [d for d in devs if d["sid"] == sid]
        for r in rels:
            r["devcount"] = sum(1 for d in my_devs if d["release"] == r["name"])
        with _lock:
            for d in my_devs:
                d["busy"] = f"dev:{d['id']}" in _busy
            release_busy = f"release:{sid}" in _busy
        dbs.append(dict(sid=sid, master=master_info(sid, cs), releases=rels, release_busy=release_busy,
                        devdbs=my_devs, timeline=timeline(sid, rels, my_devs)))
    du = shutil.disk_usage(BASE)
    with _lock:
        jobs = [j.as_dict() for j in sorted(JOBS.values(), key=lambda j: j.started, reverse=True)]
    return jsonify(host=DB_HOST, image=IMAGE, disk=dict(total=du.total, used=du.used, free=du.free),
                   dbs=dbs, jobs=jobs)


@app.get("/api/jobs/<jid>")
def api_job(jid):
    job = JOBS.get(jid)
    if not job:
        raise UserError("Vorgang nicht gefunden.")
    return jsonify(job.as_dict(with_log=True))


@app.post("/api/db/<sid>/releases")
def api_release_create(sid):
    need_sid(sid)
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    if not RE_RELEASE.match(name):
        raise UserError("Release-Name: Buchstaben, Ziffern, Punkt, Bindestrich, Unterstrich; "
                        "höchstens 40 Zeichen, beginnt mit Buchstabe oder Ziffer.")
    if (GOLDEN / sid / name).exists():
        raise UserError(f"Release „{name}“ gibt es für {sid} schon.")
    description = text_field(data.get("description"), "Beschreibung")
    source = (data.get("source") or "").strip()
    if source.startswith("release:"):
        src, origin, lineage = resolve_source(sid, source)
        job = start_job(f"Release {name} von {origin} ableiten", sid, f"release:{sid}",
                        job_release_derive, sid, name, description, src, origin, source, lineage)
        return jsonify(job=job.as_dict())
    src, origin, lineage = (resolve_source(sid, source) if source else (None, "", ""))
    job = start_job(f"Release {name} für {sid} erstellen" + (f" (aus {origin})" if origin else ""),
                    sid, f"release:{sid}", job_release, sid, name, description, src, origin, source, lineage)
    return jsonify(job=job.as_dict())


@app.patch("/api/db/<sid>/releases/<name>")
def api_release_update(sid, name):
    need_sid(sid)
    if not RE_RELEASE.match(name) or not (GOLDEN / sid / name).is_dir():
        raise UserError("Release nicht gefunden.")
    data = request.get_json(force=True)
    meta = release_meta(sid, name) or dict(name=name, created=(subvol_info(GOLDEN / sid / name) or {}).get("created"))
    meta["description"] = text_field(data.get("description"), "Beschreibung")
    write_json(release_meta_file(sid, name), meta)
    return jsonify(ok=True)


@app.delete("/api/db/<sid>/releases/<name>")
def api_release_delete(sid, name):
    need_sid(sid)
    path = GOLDEN / sid / name
    if not RE_RELEASE.match(name) or not path.is_dir():
        raise UserError("Release nicht gefunden.")
    with _lock:
        if f"release:{sid}" in _busy:
            raise UserError("Für diese Datenbank wird gerade ein Release erstellt.")
    sh(["btrfs", "subvolume", "delete", str(path)])
    sv_forget(path)
    release_meta_file(sid, name).unlink(missing_ok=True)
    tl_add(sid, "release-deleted", f"Release {name} gelöscht", ref=name)
    return jsonify(ok=True)


@app.post("/api/db/<sid>/devdbs")
def api_dev_create(sid):
    need_sid(sid)
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip().lower()
    source = (data.get("source") or "").strip()
    if not source and data.get("release"):
        source = "release:" + data["release"].strip()
    owner = (data.get("owner") or "").strip()
    if not RE_DEVNAME.match(name):
        raise UserError("Name: Kleinbuchstaben, Ziffern und Bindestrich, höchstens 30 Zeichen.")
    resolve_source(sid, source)
    if not RE_OWNER.match(owner):
        raise UserError("„Für wen“: höchstens 40 Zeichen, keine Sonderzeichen außer . @ -")
    did = f"{sid}-{name}"
    if (DEV / did).exists() or any(c["name"] == did.lower() for c in containers()):
        raise UserError(f"Eine Entwickler-DB „{name}“ gibt es für {sid} schon.")
    job = start_job(f"Entwickler-DB {name} ({sid}) anlegen", sid, f"dev:{did}",
                    job_dev_create, sid, name, source, owner)
    return jsonify(job=job.as_dict())


def find_dev(did: str) -> dict:
    if not RE_DEVID.match(did or ""):
        raise UserError("Entwickler-DB nicht gefunden.")
    dev = next((d for d in devdbs(containers()) if d["id"] == did), None)
    if not dev:
        raise UserError("Entwickler-DB nicht gefunden.")
    return dev


@app.post("/api/devdbs/<did>/<action>")
def api_dev_action(did, action):
    dev = find_dev(did)
    if action in ("start", "stop", "reset") and not dev["container"]:
        raise UserError("Zu dieser Entwickler-DB gibt es keinen Container; sie lässt sich nur löschen.")
    if action == "start":
        job = start_job(f"Entwickler-DB {dev['name']} starten", dev["sid"], f"dev:{did}", job_dev_start, dev)
    elif action == "stop":
        job = start_job(f"Entwickler-DB {dev['name']} stoppen", dev["sid"], f"dev:{did}", job_dev_stop, dev)
    elif action == "reset":
        if not dev["base"]:
            raise UserError("Der Ausgangsstand dieser Entwickler-DB ist unbekannt; bitte löschen und neu anlegen.")
        try:
            resolve_source(dev["sid"], dev["base"])
        except UserError:
            raise UserError(f"Der Ausgangsstand ({dev['base_label']}) wurde gelöscht; "
                            "auf einen eigenen Snapshot zurücksetzen oder neu anlegen.") from None
        if not dev["port"]:
            raise UserError("Port der Entwickler-DB unbekannt; bitte löschen und neu anlegen.")
        job = start_job(f"Entwickler-DB {dev['name']} zurücksetzen", dev["sid"], f"dev:{did}",
                        job_dev_recreate, dev, dev["base"])
    else:
        raise UserError("Unbekannte Aktion.")
    return jsonify(job=job.as_dict())


@app.post("/api/devdbs/<did>/snapshots")
def api_devsnap_create(did):
    dev = find_dev(did)
    if not dev["container"]:
        raise UserError("Zu dieser Entwickler-DB gibt es keinen Container.")
    data = request.get_json(force=True)
    snap = (data.get("name") or "").strip()
    if not RE_RELEASE.match(snap):
        raise UserError("Snapshot-Name: Buchstaben, Ziffern, Punkt, Bindestrich, Unterstrich; "
                        "höchstens 40 Zeichen, beginnt mit Buchstabe oder Ziffer.")
    if (DEVSNAPS / did / snap).exists():
        raise UserError(f"Snapshot „{snap}“ gibt es für {dev['name']} schon.")
    description = text_field(data.get("description"), "Beschreibung")
    consistent = bool(data.get("consistent"))
    job = start_job(f"Snapshot {snap} von Entwickler-DB {dev['name']} erstellen", dev["sid"], f"dev:{did}",
                    job_dev_snapshot, dev, snap, description, consistent)
    return jsonify(job=job.as_dict())


@app.post("/api/devdbs/<did>/snapshots/<snap>/restore")
def api_devsnap_restore(did, snap):
    dev = find_dev(did)
    if not dev["container"] or not dev["port"]:
        raise UserError("Zu dieser Entwickler-DB gibt es keinen Container; bitte neu anlegen.")
    source = f"snap:{did}/{snap}"
    resolve_source(dev["sid"], source)
    job = start_job(f"Entwickler-DB {dev['name']} auf Snapshot {snap} zurücksetzen", dev["sid"], f"dev:{did}",
                    job_dev_recreate, dev, source)
    return jsonify(job=job.as_dict())


@app.patch("/api/devdbs/<did>/snapshots/<snap>")
def api_devsnap_update(did, snap):
    dev = find_dev(did)
    resolve_source(dev["sid"], f"snap:{did}/{snap}")
    f = devsnap_meta_file(dev["sid"], did, snap)
    meta = json.loads(f.read_text()) if f.exists() else dict(name=snap, release=dev["release"])
    meta["description"] = text_field(request.get_json(force=True).get("description"), "Beschreibung")
    write_json(f, meta)
    return jsonify(ok=True)


@app.delete("/api/devdbs/<did>/snapshots/<snap>")
def api_devsnap_delete(did, snap):
    dev = find_dev(did)
    resolve_source(dev["sid"], f"snap:{did}/{snap}")
    with _lock:
        if f"dev:{did}" in _busy:
            raise UserError("Für diese Entwickler-DB läuft gerade ein Vorgang.")
    delete_devsnap(dev["sid"], did, snap)
    tl_add(dev["sid"], "devsnap-deleted", f"Snapshot {snap} von Entwickler-DB {dev['name']} gelöscht",
           ref=f"{did}/{snap}")
    return jsonify(ok=True)


@app.delete("/api/devdbs/<did>")
def api_dev_delete(did):
    dev = find_dev(did)
    job = start_job(f"Entwickler-DB {dev['name']} löschen", dev["sid"], f"dev:{did}", job_dev_delete, dev)
    return jsonify(job=job.as_dict())


@app.post("/api/db/<sid>/notes")
def api_note_add(sid):
    need_sid(sid)
    data = request.get_json(force=True)
    text = text_field(data.get("text"), "Text", required=True)
    ts = None
    if data.get("date"):
        try:
            day = dt.date.fromisoformat(data["date"])
        except ValueError:
            raise UserError("Datum ungültig.") from None
        ts = dt.datetime.combine(day, dt.datetime.now().time()).astimezone().isoformat(timespec="seconds")
    return jsonify(tl_add(sid, "note", text, ts=ts))


@app.patch("/api/db/<sid>/notes/<nid>")
def api_note_edit(sid, nid):
    need_sid(sid)
    text = text_field(request.get_json(force=True).get("text"), "Text", required=True)
    with _tl_lock:
        entries = tl_read(sid)
        note = next((e for e in entries if e["id"] == nid and e["kind"] == "note"), None)
        if not note:
            raise UserError("Notiz nicht gefunden.")
        note["text"] = text
        tl_write(sid, entries)
    return jsonify(ok=True)


@app.delete("/api/db/<sid>/notes/<nid>")
def api_note_delete(sid, nid):
    need_sid(sid)
    with _tl_lock:
        entries = tl_read(sid)
        rest = [e for e in entries if not (e["id"] == nid and e["kind"] == "note")]
        if len(rest) == len(entries):
            raise UserError("Notiz nicht gefunden.")
        tl_write(sid, rest)
    return jsonify(ok=True)


@app.get("/api/schedules")
def api_schedules():
    with _sched_lock:
        items = sched_load()
    for sc in items:
        sc["next_run"] = next_run(sc)
        sc["action_label"] = ACTIONS.get(sc["action"], sc["action"])
        lr = sc.get("last_run") or {}
        job = JOBS.get(lr.get("job", ""))
        if job and lr.get("status") == "läuft":
            lr["status"] = job.status
    return jsonify(schedules=items, server_time=now_iso(), actions=ACTIONS, reset_targets=RESET_TARGETS,
                   freqs=FREQS)


@app.post("/api/schedules")
def api_schedule_create():
    sc = validate_schedule(request.get_json(force=True))
    sc.update(id=uuid.uuid4().hex[:10], created=now_iso())
    mark_if_passed(sc)
    with _sched_lock:
        items = sched_load()
        items.append(sc)
        sched_save(items)
    return jsonify(sc)


@app.put("/api/schedules/<sched_id>")
def api_schedule_update(sched_id):
    new = validate_schedule(request.get_json(force=True))
    with _sched_lock:
        items = sched_load()
        sc = next((x for x in items if x["id"] == sched_id), None)
        if not sc:
            raise UserError("Zeitplan nicht gefunden.")
        timing_keys = ("freq", "time", "every", "minute", "window", "days")
        time_changed = (any(sc.get(k) != new.get(k) for k in timing_keys)
                        or (new["enabled"] and not sc.get("enabled")))
        for k in ("template", "keep", "dev", "consistent", "devs", "target", "keep_stopped",
                  "time", "every", "minute", "window"):
            sc.pop(k, None)
        sc.update(new)
        if time_changed:
            mark_if_passed(sc)
        sched_save(items)
    return jsonify(sc)


@app.delete("/api/schedules/<sched_id>")
def api_schedule_delete(sched_id):
    with _sched_lock:
        items = sched_load()
        rest = [x for x in items if x["id"] != sched_id]
        if len(rest) == len(items):
            raise UserError("Zeitplan nicht gefunden.")
        sched_save(rest)
    return jsonify(ok=True)


@app.post("/api/schedules/<sched_id>/run")
def api_schedule_run(sched_id):
    with _sched_lock:
        items = sched_load()
        sc = next((x for x in items if x["id"] == sched_id), None)
        if not sc:
            raise UserError("Zeitplan nicht gefunden.")
        fire_schedule(sc, dt.datetime.now())
        sched_save(items)
        lr = sc["last_run"]
    if lr["status"] == "übersprungen":
        raise UserError(lr.get("error") or "Konnte nicht gestartet werden.")
    return jsonify(job=JOBS[lr["job"]].as_dict())


def leftovers() -> dict:
    builds = [str(p) for p in sorted(BUILD.glob("*")) if subvol_info(p) is not None]
    cs = [c["name"] for c in containers() if c["name"].startswith("release-build-") and not c["running"]]
    with _lock:
        running = any(k.startswith("release:") for k in _busy)
    return dict(builds=builds, containers=cs, release_running=running)


@app.get("/api/admin")
def api_admin():
    du = shutil.disk_usage(BASE)
    return jsonify(host=DB_HOST, image=IMAGE, base=str(BASE), server_time=now_iso(),
                   disk=dict(total=du.total, used=du.used, free=du.free),
                   ports=f"{DEV_PORT_START}–{DEV_PORT_END}", ready_timeout=READY_TIMEOUT,
                   release_sql=RELEASE_SQL_FILE or "(eingebaut)", leftovers=leftovers())


@app.post("/api/admin/cleanup")
def api_admin_cleanup():
    lo = leftovers()
    if lo["release_running"]:
        raise UserError("Gerade läuft ein Release-Vorgang; Aufräumen erst danach.")
    for name in lo["containers"]:
        sh(["docker", "rm", "-f", name], check=False)
    for path in lo["builds"]:
        sh(["btrfs", "subvolume", "delete", path], check=False)
        sv_forget(Path(path))
    return jsonify(removed=len(lo["builds"]) + len(lo["containers"]))


@app.get("/api/snapshots")
def api_snapshots():
    by_mount = {c["mount"]: c["name"] for c in containers() if c["mount"]}
    rows = []

    def add(kind, sid, label, p):
        info = subvol_info(p)
        if info is None:
            return
        rows.append(dict(kind=kind, sid=sid, label=label, path=str(p), created=info["created"],
                         readonly=info["readonly"], container=by_mount.get(os.path.realpath(p))))

    for p in sorted(MASTER.glob("*")):
        add("Master-DB", p.name, p.name, p)
    for p in sorted(GOLDEN.glob("*/*")):
        add("Release", p.parent.name, p.name, p)
    for p in sorted(DEV.glob("*")):
        add("Entwickler-DB", p.name.partition("-")[0], p.name.partition("-")[2], p)
    for p in sorted(DEVSNAPS.glob("*/*")):
        add("Entwickler-Snapshot", p.parent.name.partition("-")[0],
            f"{p.parent.name.partition('-')[2]} / {p.name}", p)
    for p in sorted(BUILD.glob("*")):
        add("Zwischenstand", p.name.partition("-")[0], p.name, p)
    return jsonify(rows)


@app.get("/api/usage")
def api_usage():
    path = os.path.realpath(request.args.get("path", ""))
    roots = [os.path.realpath(r) for r in (MASTER, GOLDEN, DEV, DEVSNAPS, BUILD)]
    if not any(path.startswith(r + os.sep) for r in roots) or subvol_info(Path(path)) is None:
        raise UserError("Ungültiger Pfad.")
    p = sh(["btrfs", "filesystem", "du", "-s", "--raw", path], timeout=900)
    cols = p.stdout.strip().splitlines()[-1].split()
    nums = [int(x) if x.isdigit() else None for x in cols[:3]]
    return jsonify(total=nums[0], exclusive=nums[1], shared=nums[2])


# --------------------------------------------------------------------------- Start
def main() -> None:
    if not UI_PASSWORD:
        sys.exit("ORADB_UI_PASSWORD ist nicht gesetzt – Start abgebrochen.")
    if os.geteuid() != 0:
        print("Warnung: läuft nicht als root; btrfs- und docker-Befehle werden scheitern.", file=sys.stderr)
    for d in (MASTER, GOLDEN, DEV, DEVSNAPS, BUILD, DEVDUMPS, META):
        d.mkdir(parents=True, exist_ok=True)
    threading.Thread(target=scheduler_loop, daemon=True, name="zeitplaner").start()
    try:
        from waitress import serve
    except ImportError:
        print("waitress fehlt, starte Flask-Entwicklungsserver.", file=sys.stderr)
        app.run(host=LISTEN, port=PORT, threaded=True)
    else:
        print(f"oradb-ui auf http://{LISTEN}:{PORT}", file=sys.stderr)
        serve(app, host=LISTEN, port=PORT, threads=8)


if __name__ == "__main__":
    main()
