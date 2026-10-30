#!/usr/bin/env python3
"""
zeitkapsel – Agent: steuert auf einem DB-Host btrfs-Subvolumes und Oracle-Container.

Ablage auf dem Host:
  <BASE>/master/<SID>                      Master-DBs (Subvolumes)
  <BASE>/golden/<SID>/<release>            Releases (schreibgeschützte Snapshots)
  <BASE>/dev/<SID>-<name>                  Entwickler-DBs (beschreibbare Snapshots)
  <BASE>/devsnaps/<SID>-<name>/<snapshot>  Snapshots von Entwickler-DBs (schreibgeschützt)
  <BASE>/devdumps/<SID>-<name>             Data-Pump-Verzeichnisse der Entwickler-DBs
  <BASE>/meta/<SID>/                       Beschreibungen, Timeline, db.json (Port der Master-DB)
  <BASE>/meta/schedules.json               Zeitpläne

Im Wurzelverzeichnis jedes DB-Subvolumes kann eine Datei zeitkapsel.env liegen (siehe
read_volume_env); sie wandert mit jedem Snapshot mit und beschreibt zusätzliche Mounts,
Shared Memory, Edition und Service-Namen.

Der Agent spricht nur mit dem Controller (Token im Header X-Zeitkapsel-Token). Rechte prüft der
Controller; der Agent übernimmt den handelnden Benutzer aus X-Zeitkapsel-User für Timeline und
Besitz von Entwickler-DBs. Läuft als root (btrfs und docker).
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

import logging
import math
import shlex

from flask import Blueprint, current_app, has_request_context, jsonify, request

# --------------------------------------------------------------------------- Konfiguration
def env(name: str, default: str = "") -> str:
    return os.environ.get("ZK_" + name, os.environ.get("ORADB_" + name, default))


BASE = Path(env("BASE", "/srv/oradb"))
IMAGE = env("IMAGE", "oracle/database:19.30.0-ee")
DB_HOST = env("DB_HOST") or socket.getfqdn()
DEV_PORT_START = int(env("DEV_PORT_START", "15211"))
DEV_PORT_END = int(env("DEV_PORT_END", "15299"))
READY_TIMEOUT = int(env("READY_TIMEOUT", "900"))
CREATE_TIMEOUT = int(env("CREATE_TIMEOUT", "5400"))      # Neuanlage einer DB mit dbca
RELEASE_SQL_FILE = env("RELEASE_SQL_FILE", "")
AGENT_TOKEN = env("AGENT_TOKEN", "")
SSH_KEY = Path(env("SSH_KEY", "/root/.ssh/zeitkapsel_ed25519"))
# NOCOW (chattr +C) für neue DB-Subvolumes: schnellere Überschreibungen, aber keine Kompression und
# keine Prüfsummen. Standard aus, damit compress/compress-force des Dateisystems greift.
NOCOW = env("NOCOW", "0") == "1"
log = logging.getLogger("zeitkapsel.agent")

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
IMPORT = BASE / "import"      # Ablage für Subvolumes von anderswo (btrfs receive, Kopie einer anderen Instanz)
META = BASE / "meta"

bp = Blueprint("agent", __name__, url_prefix="/agent")

# Handelnder Benutzer: pro Anfrage bzw. pro Vorgangs-Thread
_ctx = threading.local()


def actor() -> str:
    return getattr(_ctx, "actor", "") or "system"


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


def jdbc(port: int | None, service: str) -> str | None:
    return f"jdbc:oracle:thin:@//{DB_HOST}:{port}/{service}" if port else None


# --------------------------------------------------------------------------- zeitkapsel.env im Subvolume
VOLUME_ENV = "zeitkapsel.env"


def read_volume_env(vol: Path) -> dict:
    """Schlüssel/Wert-Paare aus <vol>/zeitkapsel.env (gleiches Format wie für die Shell-Skripte).

    ZK_DB_TYPE   noncdb | cdb
    ZK_PDB       Name der PDB (bei cdb)
    ZK_SERVICE   Service-Name für JDBC (Standard: PDB bzw. SID)
    ZK_SHM_SIZE  --shm-size für AMM (memory_target), z. B. 6g
    ZK_EDITION   enterprise | standard
    ZK_MOUNTS    zusätzliche Mounts, Leerzeichen-getrennt: <Pfad im Subvolume>:<Pfad im Container>
    """
    out: dict[str, str] = {}
    f = vol / VOLUME_ENV
    try:
        text = f.read_text()
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        v = v.strip()
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
            v = v[1:-1]
        out[k.strip()] = v
    return out


def write_volume_env(vol: Path, data: dict) -> None:
    lines = ["# zeitkapsel – gelesen von Oberfläche und Skripten; wandert mit jedem Snapshot mit"]
    for k, v in data.items():
        if v not in (None, ""):
            lines.append(f'{k}="{v}"')
    (vol / VOLUME_ENV).write_text("\n".join(lines) + "\n")


def docker_extra(vol: Path) -> list[str]:
    """Zusätzliche docker-run-Argumente für ein DB-Subvolume (Mounts, Shared Memory, Edition)."""
    e = read_volume_env(vol)
    args: list[str] = []
    for m in e.get("ZK_MOUNTS", "").split():
        rel, _, target = m.partition(":")
        if not rel or not target.startswith("/") or ".." in rel.split("/") or rel.startswith("/"):
            continue
        args += ["-v", f"{vol / rel}:{target}"]
    if e.get("ZK_SHM_SIZE"):
        args += ["--shm-size", e["ZK_SHM_SIZE"]]
    if e.get("ZK_EDITION") and e["ZK_EDITION"] != "enterprise":
        args += ["-e", f"ORACLE_EDITION={e['ZK_EDITION']}"]
    return args


def service_name(vol: Path, sid: str) -> str:
    e = read_volume_env(vol)
    return e.get("ZK_SERVICE") or e.get("ZK_PDB") or sid


def db_meta_file(sid: str) -> Path:
    return META / sid / "db.json"


def db_meta(sid: str) -> dict:
    f = db_meta_file(sid)
    try:
        return json.loads(f.read_text())
    except (OSError, ValueError):
        return {}


def db_meta_update(sid: str, **kw) -> None:
    m = db_meta(sid)
    m.update(kw)
    write_json(db_meta_file(sid), m)


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
SECRET_HINT = re.compile(r"pass(word)?|pwd|secret|token", re.I)


def capture_retry() -> dict | None:
    """Merkt sich den API-Aufruf, der den Vorgang gestartet hat – ohne Passwörter (die Historie liegt auf der Platte)."""
    if not has_request_context() or not request.path.startswith("/agent/api/"):
        return None
    body = request.get_json(silent=True) if request.method in ("POST", "PUT", "PATCH") else None
    body = body if isinstance(body, dict) else {}
    secrets = sorted(k for k in body if SECRET_HINT.search(k) and body.get(k))
    clean = {k: v for k, v in body.items() if not SECRET_HINT.search(k)}
    path = request.full_path.rstrip("?")
    return dict(method=request.method, path=path[len("/agent/api/"):], body=clean, secrets=secrets)


class JobCancelled(Exception):
    """Vorgang wurde abgebrochen."""


JOBS_DIR = META / "jobs"        # Historie: <id>.json (Kopfdaten) und <id>.log (vollständiges Protokoll)
JOBS_KEEP = int(env("JOBS_KEEP", "500"))


class Job:
    def __init__(self, title: str, sid: str | None):
        self.id = time.strftime("%Y%m%d%H%M%S") + "-" + uuid.uuid4().hex[:6]
        self.title, self.sid = title, sid
        self.status, self.error = "läuft", None
        self.started, self.ended = now_iso(), None
        self.lines: list[str] = []
        self.actor = actor()
        self.cancel_requested = False
        self.cancelled_by = ""
        self.proc: subprocess.Popen | None = None
        self.cleanups: list = []              # bei Abbruch sofort auszuführen (z. B. Container entfernen)
        self.retry: dict | None = capture_retry()
        self.retry_of: str | None = getattr(_ctx, "retry_of", None)
        self._lock = threading.Lock()
        JOBS_DIR.mkdir(parents=True, exist_ok=True)
        self.save()

    # --- Protokoll und Historie
    def log(self, msg: str) -> None:
        line = f"{time.strftime('%H:%M:%S')}  {msg}"
        self.lines.append(line)
        del self.lines[:-500]
        try:
            with (JOBS_DIR / f"{self.id}.log").open("a") as fh:
                fh.write(line + "\n")
        except OSError:
            pass

    def save(self) -> None:
        try:
            write_json(JOBS_DIR / f"{self.id}.json", self.as_dict())
        except OSError:
            pass

    # --- Abbrechen
    def check(self) -> None:
        if self.cancel_requested:
            raise JobCancelled("abgebrochen" + (f" von {self.cancelled_by}" if self.cancelled_by else ""))

    def on_cancel(self, fn) -> None:
        """Aufräumaktion, die beim Abbrechen sofort läuft (der Vorgang selbst räumt danach weiter auf)."""
        self.cleanups.append(fn)

    def cancel(self, by: str) -> None:
        with self._lock:
            if self.status != "läuft" or self.cancel_requested:
                return
            self.cancel_requested, self.cancelled_by = True, by
        self.log(f"Abbruch angefordert von {by or 'unbekannt'}")
        proc = self.proc
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        for fn in reversed(self.cleanups):
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                self.log(f"Aufräumen beim Abbruch: {e}")

    # --- Befehle
    def _popen(self, cmd: list[str], input: str | None, merge: bool) -> subprocess.Popen:
        self.check()
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT if merge else subprocess.PIPE,
                             text=True, bufsize=1)
        self.proc = p
        return p

    def run(self, cmd: list[str], secret: str | None = None, quiet: bool = False, allow_fail: bool = False,
            input: str | None = None, timeout: int | None = None) -> subprocess.CompletedProcess:
        mask = (lambda t: t.replace(secret, "***")) if secret else (lambda t: t)
        self.log("$ " + mask(" ".join(cmd)))
        p = self._popen(cmd, input, merge=False)
        try:
            out, err = p.communicate(input=input, timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            out, err = p.communicate()
            raise RuntimeError(f"„{' '.join(cmd[:3])} …“: Zeitüberschreitung nach {timeout} s.") from None
        finally:
            self.proc = None
        self.check()
        lines = ((out or "") + (err or "")).strip().splitlines()
        for line in (lines[-30:] if not quiet or p.returncode else []):
            self.log("  " + mask(line))
        if p.returncode != 0 and not allow_fail:
            raise RuntimeError(f"„{' '.join(cmd[:3])} …“ ist mit Exit-Code {p.returncode} fehlgeschlagen.")
        return subprocess.CompletedProcess(cmd, p.returncode, out or "", err or "")

    def stream(self, cmd: list[str], input: str | None = None, secret: str | None = None) -> tuple[int, list[str]]:
        """Befehl ausführen und jede Ausgabezeile sofort protokollieren (für lange Vorgänge wie impdp)."""
        mask = (lambda t: t.replace(secret, "***")) if secret else (lambda t: t)
        self.log("$ " + mask(" ".join(cmd)))
        p = self._popen(cmd, input, merge=True)
        if input is not None:
            p.stdin.write(input)
            p.stdin.close()
        seen: list[str] = []
        try:
            for line in p.stdout:
                line = mask(line.rstrip())
                if line:
                    self.log("  " + line)
                    if "ORA-" in line:
                        seen.append(line)
            rc = p.wait()
        finally:
            self.proc = None
        self.check()
        return rc, seen

    def sleep(self, seconds: float) -> None:
        end = time.time() + seconds
        while time.time() < end:
            self.check()
            time.sleep(min(1.0, end - time.time()) if end > time.time() else 0)

    def as_dict(self, with_log: bool = False) -> dict:
        d = dict(id=self.id, title=self.title, sid=self.sid, status=self.status, error=self.error,
                 started=self.started, ended=self.ended, actor=self.actor,
                 cancel_requested=self.cancel_requested, cancelled_by=self.cancelled_by,
                 retry=self.retry, retry_of=self.retry_of)
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
        _ctx.actor = job.actor
        try:
            fn(job, *args)
            job.check()
            job.status = "fertig"
            job.log("Fertig.")
        except Exception as e:  # noqa: BLE001 – jeder Fehler landet im Job-Protokoll
            if job.cancel_requested:
                job.status, job.error = "abgebrochen", f"abgebrochen von {job.cancelled_by}" if job.cancelled_by else "abgebrochen"
                job.log("ABGEBROCHEN" + ("" if isinstance(e, JobCancelled) else f" ({e})"))
            else:
                job.status, job.error = "fehlgeschlagen", str(e)
                job.log("FEHLER: " + str(e))
        finally:
            job.ended = now_iso()
            job.save()
            with _lock:
                _busy.discard(busy_key)

    threading.Thread(target=worker, daemon=True).start()
    return job


def job_history(limit: int = 300) -> list[dict]:
    """Kopfdaten aller Vorgänge, neueste zuerst; laufende aus dem Speicher (aktueller Stand)."""
    out: dict[str, dict] = {}
    try:
        files = sorted(JOBS_DIR.glob("*.json"), reverse=True)[:limit]
    except OSError:
        files = []
    for f in files:
        try:
            d = json.loads(f.read_text())
            out[d["id"]] = d
        except (OSError, ValueError, KeyError):
            continue
    with _lock:
        for j in JOBS.values():
            out[j.id] = j.as_dict()
    return sorted(out.values(), key=lambda d: d.get("started") or "", reverse=True)[:limit]


def job_log(jid: str, max_lines: int = 5000) -> list[str]:
    f = JOBS_DIR / f"{jid}.log"
    try:
        lines = f.read_text(errors="replace").splitlines()
    except OSError:
        return []
    return lines[-max_lines:]


def jobs_housekeeping() -> None:
    """Beim Start: Vorgänge, die bei einem Neustart liefen, als unterbrochen markieren; alte entfernen."""
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(JOBS_DIR.glob("*.json"))
    for f in files:
        try:
            d = json.loads(f.read_text())
        except (OSError, ValueError):
            continue
        if d.get("status") == "läuft":
            d.update(status="unterbrochen", error="Dienst wurde neu gestartet, während der Vorgang lief.",
                     ended=d.get("ended") or now_iso())
            write_json(f, d)
    for f in files[:-JOBS_KEEP] if len(files) > JOBS_KEEP else []:
        f.unlink(missing_ok=True)
        f.with_suffix(".log").unlink(missing_ok=True)


def wait_ready(job: Job, cname: str, since: float, timeout: int | None = None) -> None:
    timeout = timeout or READY_TIMEOUT
    job.log(f"Warte auf „{READY_MARK}“ (höchstens {timeout // 60} Minuten) …")
    t0 = time.time()
    while time.time() - t0 < timeout:
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
        job.sleep(5)
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
    entry = dict(id=uuid.uuid4().hex[:12], ts=ts or now_iso(), kind=kind, text=text, ref=ref, by=actor())
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
        try:
            owner = json.loads(dev_meta_file(sid, did).read_text()).get("owner") or L.get("devowner", "")
        except (OSError, ValueError):
            owner = L.get("devowner", "")
        items[did] = dict(
            id=did, sid=sid, name=name, release=release, owner=owner,
            base=L.get("devbase") or (f"release:{release}" if release else ""),
            created=L.get("devcreated", ""), port=c["port"], container=c["name"],
            state=db_state(c), jdbc=jdbc(c["port"], service_name(DEV / did, sid)),
        )
    if DEV.is_dir():
        for p in DEV.iterdir():
            if p.name in items or not p.is_dir():
                continue
            sid, _, name = p.name.partition("-")
            info = subvol_info(p) or {}
            try:
                meta = json.loads(dev_meta_file(sid, p.name).read_text())
            except (OSError, ValueError):
                meta = {}
            items[p.name] = dict(id=p.name, sid=sid, name=name, release="", owner=meta.get("owner", ""),
                                 base=meta.get("base", ""),
                                 created=info.get("created") or "", port=None, container=None,
                                 state="ohne Container", jdbc=None)
    for d in items.values():
        d["base_label"] = base_label(d)
        d["snapshots"] = devsnaps_of(d["sid"], d["id"])
    return sorted(items.values(), key=lambda d: d["id"])


def master_container(sid: str, cs: list[dict]) -> dict | None:
    vol = os.path.realpath(MASTER / sid)
    return next((c for c in cs if c["mount"] == vol and not c["name"].startswith(("release-build-", "zk-create-"))), None)


def master_info(sid: str, cs: list[dict]) -> dict:
    c = master_container(sid, cs)
    e = read_volume_env(MASTER / sid)
    port = (c["port"] if c else None) or db_meta(sid).get("port")
    service = service_name(MASTER / sid, sid)
    return dict(container=c["name"] if c else None, state=db_state(c), port=port, service=service,
                type=e.get("ZK_DB_TYPE", "noncdb"), pdb=e.get("ZK_PDB", ""),
                jdbc=jdbc(port, service) if c else None, origin=db_meta(sid).get("origin", ""))


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
        job.on_cancel(lambda: sh(["docker", "rm", "-f", bname], check=False))
        job.run(["docker", "run", "-d", "--name", bname, "--init", "-e", f"ORACLE_SID={sid}",
                 "-v", f"{tmp}:/opt/oracle/oradata", *docker_extra(tmp), IMAGE])
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
            "-v", f"{DEVDUMPS / did}:/oracle/dumps", *docker_extra(DEV / did), IMAGE]


def prepare_dumps(did: str, sid: str) -> None:
    (DEVDUMPS / did / "19c" / "datapump" / sid).mkdir(parents=True, exist_ok=True)
    sh(["chown", "-R", f"{ORACLE_UID}:{ORACLE_UID}", str(DEVDUMPS / did)])


def dev_meta_file(sid: str, did: str) -> Path:
    return META / sid / "devdbs" / f"{did}.json"


def job_dev_create(job: Job, sid: str, name: str, source: str, owner: str) -> None:
    src, label, release = resolve_source(sid, source)
    did = f"{sid}-{name}"
    # Besitz auch ohne Container festhalten (z. B. wenn der Start scheitert)
    write_json(dev_meta_file(sid, did), dict(owner=owner, base=source, created=now_iso()))
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
    dev_meta_file(dev["sid"], did).unlink(missing_ok=True)
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
    _ctx.actor = "Zeitplan"
    while True:
        try:
            scheduler_tick()
        except Exception:  # noqa: BLE001
            log.exception("Fehler im Zeitplaner")
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


# --------------------------------------------------------------------------- Zugriffsschutz des Agenten
@bp.before_request
def agent_guard():
    token = request.headers.get("X-Zeitkapsel-Token", "")
    if not AGENT_TOKEN or not hmac.compare_digest(token.encode(), AGENT_TOKEN.encode()):
        return jsonify(error="Agent: Token fehlt oder ist falsch."), 401
    _ctx.actor = request.headers.get("X-Zeitkapsel-User", "")[:64]
    return None


@bp.errorhandler(UserError)
def on_user_error(e):
    return jsonify(error=str(e)), 400


@bp.errorhandler(Exception)
def on_error(e):
    code = getattr(e, "code", 500)
    if isinstance(code, int) and code < 500:
        return jsonify(error=str(e)), code
    log.exception("Fehler im Agenten")
    return jsonify(error=str(e)), 500


# --------------------------------------------------------------------------- Routen
@bp.get("/api/state")
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
                   dbs=dbs, jobs=jobs, busy=sorted(k for k in _busy if k.startswith(("newdb:", "import:"))))


RE_JOBID = re.compile(r"^[0-9a-f-]{6,40}$")


def job_info(jid: str) -> dict:
    if not RE_JOBID.match(jid or ""):
        raise UserError("Vorgang nicht gefunden.")
    job = JOBS.get(jid)
    if job:
        return job.as_dict()
    try:
        return json.loads((JOBS_DIR / f"{jid}.json").read_text())
    except (OSError, ValueError):
        raise UserError("Vorgang nicht gefunden.") from None


@bp.get("/api/jobs")
def api_jobs():
    limit = max(1, min(int(request.args.get("limit") or 300), 1000))
    return jsonify(jobs=job_history(limit))


@bp.get("/api/jobs/<jid>")
def api_job(jid):
    d = job_info(jid)
    full = request.args.get("full") == "1"
    job = JOBS.get(jid)
    d["log"] = job_log(jid) if (full or not job) else job.lines
    if not d["log"] and job:
        d["log"] = job.lines
    return jsonify(d)


RETRYABLE = ("fehlgeschlagen", "abgebrochen", "unterbrochen")


@bp.post("/api/jobs/<jid>/retry")
def api_job_retry(jid):
    info = job_info(jid)
    spec = info.get("retry")
    if not spec:
        raise UserError("Dieser Vorgang lässt sich nicht automatisch wiederholen (z. B. von einem Zeitplan gestartet).")
    if info.get("status") not in RETRYABLE:
        raise UserError("Nur fehlgeschlagene, abgebrochene oder unterbrochene Vorgänge lassen sich wiederholen.")
    given = (request.get_json(silent=True) or {}).get("secrets") or {}
    body = dict(spec.get("body") or {})
    for k in spec.get("secrets") or []:
        body[k] = given.get(k, "")
    headers = {"X-Zeitkapsel-Token": AGENT_TOKEN, "X-Zeitkapsel-User": actor()}
    _ctx.retry_of = jid
    try:
        with current_app.test_request_context("/agent/api/" + spec["path"], method=spec["method"],
                                              json=body if spec["method"] in ("POST", "PUT", "PATCH") else None,
                                              headers=headers):
            resp = current_app.full_dispatch_request()
    finally:
        _ctx.retry_of = None
    return resp


@bp.post("/api/jobs/<jid>/cancel")
def api_job_cancel(jid):
    job = JOBS.get(jid) if RE_JOBID.match(jid or "") else None
    if not job or job.status != "läuft":
        raise UserError("Dieser Vorgang läuft nicht (mehr).")
    threading.Thread(target=job.cancel, args=(actor(),), daemon=True).start()
    return jsonify(ok=True)


# --------------------------------------------------------------------------- Container
def container_kind(c: dict) -> str | None:
    name, mount = c["name"], c["mount"] or ""
    if name.startswith("release-build-"):
        return "Release-Erstellung"
    if name.startswith("zk-create-"):
        return "Neuanlage"
    if name.startswith("zk-verify-"):
        return "Prüfstart"
    if c["labels"].get("devdb") == "1" or os.path.dirname(mount) == os.path.realpath(DEV):
        return "Entwickler-DB"
    if os.path.dirname(mount) == os.path.realpath(MASTER):
        return "Master-DB"
    base = os.path.realpath(BASE)
    if mount == base or mount.startswith(base + os.sep):
        return "Sonstiger"
    return None   # Container, die nichts mit zeitkapsel zu tun haben, bleiben unsichtbar


def container_rows() -> list[dict]:
    cs = containers()
    devs = {d["container"]: d for d in devdbs(cs) if d["container"]}
    rows = []
    for c in cs:
        kind = container_kind(c)
        if not kind:
            continue
        d = devs.get(c["name"], {})
        sid = d.get("sid") or (os.path.basename(c["mount"]) if c["mount"] else "")
        rows.append(dict(name=c["name"], kind=kind, sid=sid, running=c["running"], restarting=c["restarting"],
                         started=c["started"], port=c["port"], state=db_state(c) if kind in ("Master-DB", "Entwickler-DB") else
                         ("läuft" if c["running"] else "gestoppt"), owner=d.get("owner", ""), dev_id=d.get("id", "")))
    order = ["Master-DB", "Entwickler-DB", "Release-Erstellung", "Neuanlage", "Prüfstart", "Sonstiger"]
    return sorted(rows, key=lambda r: (order.index(r["kind"]), r["name"]))


def find_container(name: str) -> dict:
    row = next((r for r in container_rows() if r["name"] == name), None)
    if not row:
        raise UserError("Container nicht gefunden (oder gehört nicht zu zeitkapsel).")
    return row


@bp.get("/api/containers")
def api_containers():
    return jsonify(containers=container_rows())


@bp.get("/api/containers/<name>")
def api_container(name):
    return jsonify(find_container(name))


def job_container_stop(job: Job, name: str) -> None:
    job.run(["docker", "stop", "-t", "600", name], timeout=700)


@bp.post("/api/containers/<name>/<action>")
def api_container_action(name, action):
    row = find_container(name)
    if action == "stop":
        if not row["running"]:
            raise UserError("Der Container läuft nicht.")
        job = start_job(f"Container {name} stoppen", row["sid"] or None, f"container:{name}", job_container_stop, name)
        return jsonify(job=job.as_dict())
    if action == "kill":
        if not row["running"] and not row["restarting"]:
            raise UserError("Der Container läuft nicht.")
        sh(["docker", "update", "--restart", "no", name], check=False)     # sonst startet Docker ihn gleich wieder
        sh(["docker", "kill", name])
        if row["sid"] and row["kind"] in ("Master-DB", "Entwickler-DB"):
            sid = row["sid"] if row["kind"] == "Master-DB" else row["dev_id"].partition("-")[0]
            tl_add(sid, "container-killed", f"Container {name} hart beendet ({row['kind']})")
        return jsonify(ok=True)
    if action == "remove":
        if row["kind"] in ("Master-DB", "Entwickler-DB"):
            raise UserError("Master- und Entwickler-DBs über ihre eigenen Aktionen löschen.")
        sh(["docker", "rm", "-f", name])
        return jsonify(ok=True)
    raise UserError("Unbekannte Aktion.")


@bp.post("/api/db/<sid>/releases")
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


@bp.patch("/api/db/<sid>/releases/<name>")
def api_release_update(sid, name):
    need_sid(sid)
    if not RE_RELEASE.match(name) or not (GOLDEN / sid / name).is_dir():
        raise UserError("Release nicht gefunden.")
    data = request.get_json(force=True)
    meta = release_meta(sid, name) or dict(name=name, created=(subvol_info(GOLDEN / sid / name) or {}).get("created"))
    meta["description"] = text_field(data.get("description"), "Beschreibung")
    write_json(release_meta_file(sid, name), meta)
    return jsonify(ok=True)


@bp.delete("/api/db/<sid>/releases/<name>")
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


@bp.post("/api/db/<sid>/devdbs")
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


@bp.post("/api/devdbs/<did>/<action>")
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


@bp.post("/api/devdbs/<did>/snapshots")
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


@bp.post("/api/devdbs/<did>/snapshots/<snap>/restore")
def api_devsnap_restore(did, snap):
    dev = find_dev(did)
    if not dev["container"] or not dev["port"]:
        raise UserError("Zu dieser Entwickler-DB gibt es keinen Container; bitte neu anlegen.")
    source = f"snap:{did}/{snap}"
    resolve_source(dev["sid"], source)
    job = start_job(f"Entwickler-DB {dev['name']} auf Snapshot {snap} zurücksetzen", dev["sid"], f"dev:{did}",
                    job_dev_recreate, dev, source)
    return jsonify(job=job.as_dict())


@bp.patch("/api/devdbs/<did>/snapshots/<snap>")
def api_devsnap_update(did, snap):
    dev = find_dev(did)
    resolve_source(dev["sid"], f"snap:{did}/{snap}")
    f = devsnap_meta_file(dev["sid"], did, snap)
    meta = json.loads(f.read_text()) if f.exists() else dict(name=snap, release=dev["release"])
    meta["description"] = text_field(request.get_json(force=True).get("description"), "Beschreibung")
    write_json(f, meta)
    return jsonify(ok=True)


@bp.delete("/api/devdbs/<did>/snapshots/<snap>")
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


@bp.delete("/api/devdbs/<did>")
def api_dev_delete(did):
    dev = find_dev(did)
    job = start_job(f"Entwickler-DB {dev['name']} löschen", dev["sid"], f"dev:{did}", job_dev_delete, dev)
    return jsonify(job=job.as_dict())


@bp.post("/api/db/<sid>/notes")
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


@bp.patch("/api/db/<sid>/notes/<nid>")
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


@bp.delete("/api/db/<sid>/notes/<nid>")
def api_note_delete(sid, nid):
    need_sid(sid)
    with _tl_lock:
        entries = tl_read(sid)
        rest = [e for e in entries if not (e["id"] == nid and e["kind"] == "note")]
        if len(rest) == len(entries):
            raise UserError("Notiz nicht gefunden.")
        tl_write(sid, rest)
    return jsonify(ok=True)


@bp.get("/api/schedules")
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


@bp.post("/api/schedules")
def api_schedule_create():
    sc = validate_schedule(request.get_json(force=True))
    sc.update(id=uuid.uuid4().hex[:10], created=now_iso())
    mark_if_passed(sc)
    with _sched_lock:
        items = sched_load()
        items.append(sc)
        sched_save(items)
    return jsonify(sc)


@bp.put("/api/schedules/<sched_id>")
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


@bp.delete("/api/schedules/<sched_id>")
def api_schedule_delete(sched_id):
    with _sched_lock:
        items = sched_load()
        rest = [x for x in items if x["id"] != sched_id]
        if len(rest) == len(items):
            raise UserError("Zeitplan nicht gefunden.")
        sched_save(rest)
    return jsonify(ok=True)


@bp.post("/api/schedules/<sched_id>/run")
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


@bp.get("/api/admin")
def api_admin():
    du = shutil.disk_usage(BASE)
    return jsonify(host=DB_HOST, image=IMAGE, base=str(BASE), server_time=now_iso(),
                   disk=dict(total=du.total, used=du.used, free=du.free),
                   ports=f"{DEV_PORT_START}–{DEV_PORT_END}", ready_timeout=READY_TIMEOUT,
                   release_sql=RELEASE_SQL_FILE or "(eingebaut)", leftovers=leftovers(),
                   storage=storage_info())


@bp.post("/api/admin/cleanup")
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


# --------------------------------------------------------------------------- Platzermittlung im Hintergrund
USAGE_MAX_AGE = int(env("USAGE_MAX_AGE", "900"))
_usage: dict[str, dict] = {}
_usage_pending: list[str] = []
_usage_lock = threading.Lock()
_usage_event = threading.Event()


def compute_usage(path: str) -> dict:
    out: dict = {"ts": time.time()}
    p = sh(["btrfs", "filesystem", "du", "-s", "--raw", path], check=False, timeout=3600)
    if p.returncode == 0 and p.stdout.strip():
        cols = p.stdout.strip().splitlines()[-1].split()
        nums = [int(x) if x.isdigit() else None for x in cols[:3]]
        out.update(total=nums[0], exclusive=nums[1], shared=nums[2])
    else:
        out["error"] = (p.stderr or p.stdout).strip()[-200:] or "btrfs filesystem du fehlgeschlagen"
    if shutil.which("compsize"):
        c = sh(["compsize", "-b", path], check=False, timeout=3600)
        for line in c.stdout.splitlines():
            parts = line.split()
            if parts[:1] == ["TOTAL"] and len(parts) >= 5 and parts[2].isdigit():
                out.update(disk=int(parts[2]), uncompressed=int(parts[3]), referenced=int(parts[4]))
    return out


def usage_worker() -> None:
    while True:
        _usage_event.wait(60)
        _usage_event.clear()
        while True:
            with _usage_lock:
                if not _usage_pending:
                    break
                path = _usage_pending.pop(0)
            if not os.path.isdir(path):
                continue
            try:
                result = compute_usage(path)
            except Exception as e:  # noqa: BLE001
                result = {"ts": time.time(), "error": str(e)}
            with _usage_lock:
                _usage[path] = result


def usage_get(path: str, force: bool = False) -> tuple[dict | None, bool]:
    """Liefert den zuletzt ermittelten Platz und ob gerade (neu) ermittelt wird."""
    with _usage_lock:
        u = _usage.get(path)
        stale = force or u is None or time.time() - u["ts"] > USAGE_MAX_AGE
        if stale and path not in _usage_pending:
            _usage_pending.append(path)
            _usage_event.set()
        return u, path in _usage_pending


def usage_forget(path: str) -> None:
    with _usage_lock:
        _usage.pop(path, None)


# --------------------------------------------------------------------------- Datenbanken in fremden Subvolumes erkennen
def detect_db(vol: Path) -> dict | None:
    """Sucht im Wurzelverzeichnis eines Subvolumes eine Datenbank in der Ablage von zeitkapsel."""
    try:
        sids = sorted({p.name[1:-len(".created")] for p in vol.glob(".*.created")})
        if not sids and (vol / "dbconfig").is_dir():
            sids = sorted(p.name for p in (vol / "dbconfig").iterdir() if p.is_dir())
    except OSError:
        return None
    if not sids:
        return None
    if len(sids) > 1:
        return dict(sid=None, sids=sids, ok=False, problems=["mehrere Datenbanken in einem Subvolume (" + ", ".join(sids) + ")"], fixes=[])
    sid = sids[0]
    problems, fixes = [], []
    if not RE_SID.match(sid):
        problems.append(f"ungültige SID „{sid}“")
    if not any((vol / sid).glob("*.dbf")):
        problems.append(f"keine Datendateien in {sid}/")
    for f in (f"spfile{sid}.ora", f"orapw{sid}"):
        if not (vol / "dbconfig" / sid / f).exists():
            problems.append(f"dbconfig/{sid}/{f} fehlt")
    if not (vol / f".{sid}.created").exists():
        fixes.append(f"Markierungsdatei .{sid}.created anlegen")
    if not list((vol / "dbconfig" / sid).glob(".docker_*")):
        fixes.append("Markierungsdatei .docker_enterprise anlegen")
    for name in ("listener.ora", "sqlnet.ora", "tnsnames.ora", "oratab"):
        if not (vol / "dbconfig" / sid / name).exists():
            fixes.append(f"dbconfig/{sid}/{name} anlegen")
    e = read_volume_env(vol)
    return dict(sid=sid, sids=sids, ok=not problems, problems=problems, fixes=fixes,
                type=e.get("ZK_DB_TYPE", ""), service=e.get("ZK_SERVICE") or e.get("ZK_PDB") or sid,
                exists=(MASTER / sid).exists())


def classify_subvolume(path: Path) -> str | None:
    parent = path.parent
    if parent == MASTER:
        return "master" if RE_SID.match(path.name) else "other"
    if parent == BUILD:
        return "build"
    if parent == IMPORT:
        return "import"
    return None


def job_adopt(job: Job, src: Path, port: int) -> None:
    det = detect_db(src)
    if not det or not det["ok"]:
        raise RuntimeError("Keine startbare Datenbank: " + "; ".join((det or {}).get("problems", ["nichts gefunden"])))
    sid = det["sid"]
    dst = MASTER / sid
    if dst.exists():
        raise RuntimeError(f"Eine Master-DB {sid} gibt es auf diesem Host schon; erst die vorhandene löschen oder umbenennen.")
    if port_in_use(port, containers()):
        raise RuntimeError(f"Port {port} ist belegt.")
    info = subvol_info(src) or {}
    if src.parent == MASTER and not info.get("readonly"):
        job.log(f"{src.name} → {sid} umbenennen")
        job.run(["mv", str(src), str(dst)])
        sv_forget(src)
        usage_forget(str(src))
    else:
        job.log(f"Beschreibbaren Snapshot von {src} als Master-DB {sid} anlegen (die Quelle bleibt erhalten)")
        job.run(["btrfs", "subvolume", "snapshot", str(src), str(dst)])
    cfg = dst / "dbconfig" / sid
    if not (dst / f".{sid}.created").exists():
        (dst / f".{sid}.created").touch()
    if not list(cfg.glob(".docker_*")):
        (cfg / ".docker_enterprise").touch()
    for name, text in NET_FILES.items():
        if not (cfg / name).exists():
            (cfg / name).write_text(text)
    if not (cfg / "tnsnames.ora").exists():
        (cfg / "tnsnames.ora").write_text(
            f"{sid} = (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1521))\n"
            f"          (CONNECT_DATA = (SERVICE_NAME = {det['service']})))\n")
    if not (cfg / "oratab").exists():
        (cfg / "oratab").write_text(f"{sid}:/opt/oracle/product/19c/dbhome_1:N\n")
    job.run(["chown", "-R", f"{ORACLE_UID}:{ORACLE_UID}", str(dst)])
    db_meta_update(sid, port=port, origin=f"übernommen aus {src}", created=now_iso())
    tl_add(sid, "db-adopted", f"Datenbank {sid} aus {src} übernommen")
    job_master_start(job, sid, port)


# --------------------------------------------------------------------------- Routen: Subvolumes
@bp.get("/api/snapshots")
def api_snapshots():
    cs = containers()
    by_mount = {c["mount"]: c for c in cs if c["mount"]}
    devs = {d["id"]: d for d in devdbs(cs)}
    force = request.args.get("refresh") == "1"
    rows = []

    def add(kind, sid, label, p, **extra):
        info = subvol_info(p)
        if info is None:
            return
        c = by_mount.get(os.path.realpath(p))
        u, pending = usage_get(str(p), force)
        rows.append(dict(kind=kind, sid=sid, label=label, path=str(p), created=info["created"],
                         readonly=info["readonly"], container=c["name"] if c else None,
                         container_running=bool(c and c["running"]), usage=u, usage_pending=pending, **extra))

    for p in sorted(MASTER.glob("*")):
        if RE_SID.match(p.name):
            add("Master-DB", p.name, p.name, p, delete="subvolume")
        else:   # z. B. DLHTEST.neu/.alt beim Umkopieren
            add("Sonstiges Subvolume", p.name.split(".")[0], p.name, p, delete="subvolume", detected=detect_db(p))
    for p in sorted(IMPORT.glob("*")):
        det = detect_db(p)
        add("Import", det["sid"] if det and det.get("sid") else "–", p.name, p, delete="subvolume", detected=det)
    for p in sorted(GOLDEN.glob("*/*")):
        add("Release", p.parent.name, p.name, p, delete="release", release=p.name)
    for p in sorted(DEV.glob("*")):
        d = devs.get(p.name, {})
        add("Entwickler-DB", p.name.partition("-")[0], p.name.partition("-")[2], p, delete="devdb",
            dev_id=p.name, owner=d.get("owner", ""))
    for p in sorted(DEVSNAPS.glob("*/*")):
        d = devs.get(p.parent.name, {})
        add("Entwickler-Snapshot", p.parent.name.partition("-")[0], f"{p.parent.name.partition('-')[2]} / {p.name}", p,
            delete="devsnap", dev_id=p.parent.name, snap=p.name, owner=d.get("owner", ""))
    for p in sorted(BUILD.glob("*")):
        add("Zwischenstand", p.name.partition("-")[0], p.name, p, delete="subvolume")
    return jsonify(rows=rows, compsize=bool(shutil.which("compsize")), import_dir=str(IMPORT))


@bp.get("/api/usage")
def api_usage():
    path = os.path.realpath(request.args.get("path", ""))
    roots = [os.path.realpath(r) for r in (MASTER, GOLDEN, DEV, DEVSNAPS, BUILD, IMPORT)]
    if not any(path.startswith(r + os.sep) for r in roots) or subvol_info(Path(path)) is None:
        raise UserError("Ungültiger Pfad.")
    u, pending = usage_get(path, force=True)
    return jsonify(usage=u, pending=pending)


def resolve_subvolume_arg() -> tuple[Path, str]:
    path = Path(os.path.realpath(request.args.get("path", "")))
    kind = classify_subvolume(path)
    if kind is None or subvol_info(path) is None:
        raise UserError("Dieses Subvolume lässt sich hier nicht verwalten (Releases und Entwickler-DBs über ihre eigenen Aktionen).")
    return path, kind


@bp.delete("/api/subvolume")
def api_subvolume_delete():
    path, kind = resolve_subvolume_arg()
    cs = containers()
    users = [c for c in cs if c["mount"] == str(path)]
    if any(c["running"] for c in users):
        raise UserError(f"Das Subvolume wird vom laufenden Container {users[0]['name']} benutzt; erst stoppen.")
    if users and kind != "master":
        raise UserError(f"Das Subvolume gehört zum Container {users[0]['name']}; erst den Container entfernen.")
    if kind == "master":
        sid = path.name
        if request.args.get("confirm") != sid:
            raise UserError(f"Zum Löschen einer Master-DB den Namen {sid} zur Bestätigung angeben.")
        with _lock:
            busy = [k for k in _busy if k.split(":", 1)[-1] == sid]
        if busy:
            raise UserError("Für diese Datenbank läuft gerade ein Vorgang.")
        for c in users:
            sh(["docker", "rm", c["name"]])
    if kind == "build":
        with _lock:
            if f"release:{path.name.partition('-')[0]}" in _busy:
                raise UserError("Gerade läuft ein Release-Vorgang für diese Datenbank.")
    sh(["btrfs", "subvolume", "delete", str(path)])
    sv_forget(path)
    usage_forget(str(path))
    if kind == "master":
        db_meta_file(path.name).unlink(missing_ok=True)
        tl_add(path.name, "master-deleted", f"Master-DB {path.name} gelöscht (Releases und Entwickler-DBs bleiben)")
    return jsonify(ok=True)


@bp.post("/api/subvolume/adopt")
def api_subvolume_adopt():
    path, kind = resolve_subvolume_arg()
    if kind not in ("other", "import", "build"):
        raise UserError("Nur sonstige oder importierte Subvolumes lassen sich übernehmen.")
    det = detect_db(path)
    if not det or not det["ok"]:
        raise UserError("Keine startbare Datenbank gefunden: " + "; ".join((det or {}).get("problems", ["keine Markierungen"])))
    if det["exists"]:
        raise UserError(f"Eine Master-DB {det['sid']} gibt es auf diesem Host schon; erst die vorhandene löschen.")
    port = int((request.get_json(silent=True) or {}).get("port") or 0)
    if not 1024 <= port <= 65535:
        raise UserError("Port: 1024 bis 65535.")
    job = start_job(f"Datenbank {det['sid']} aus {path.name} übernehmen", det["sid"], f"adopt:{det['sid']}",
                    job_adopt, path, port)
    return jsonify(job=job.as_dict())


# --------------------------------------------------------------------------- Speicher: Kompression und NOCOW
def is_nocow(path: Path) -> bool:
    p = sh(["lsattr", "-d", str(path)], check=False)
    flags = p.stdout.split()[0] if p.returncode == 0 and p.stdout.split() else ""
    return "C" in flags


def prepare_new_volume(job: Job, vol: Path) -> None:
    """Neues DB-Subvolume: NOCOW nur auf ausdrücklichen Wunsch, sonst sicherstellen, dass kein
    vom Elternverzeichnis geerbtes C-Attribut die Kompression verhindert."""
    if NOCOW:
        job.log("ZK_NOCOW=1: Copy-on-Write aus (keine Kompression, keine Prüfsummen)")
        job.run(["chattr", "+C", str(vol)])
    elif is_nocow(vol):
        job.log("Subvolume hat ein geerbtes NOCOW-Attribut; wird entfernt, damit komprimiert wird")
        job.run(["chattr", "-C", str(vol)])


def storage_info() -> dict:
    p = sh(["findmnt", "-no", "FSTYPE,OPTIONS", "-T", str(BASE)], check=False)
    fstype, _, opts = p.stdout.strip().partition(" ")
    compress = next((o for o in opts.strip().split(",") if o.startswith("compress")), "")
    nocow = []
    for sid in master_sids():
        vol = MASTER / sid
        if is_nocow(vol) or is_nocow(vol / sid):
            nocow.append(sid)
    return dict(fstype=fstype, compress=compress, nocow_setting=NOCOW, nocow_dbs=nocow)


# --------------------------------------------------------------------------- Master-DB starten und stoppen
MASTER_DUMPS = Path(env("MASTER_DUMPS", "/srv/oradumps"))


def port_in_use(port: int, cs: list[dict]) -> bool:
    if any(c["port"] == port for c in cs):
        return True
    with socket.socket() as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
        except OSError:
            return True
    return False


def master_run_cmd(sid: str, port: int) -> list[str]:
    vol = MASTER / sid
    cmd = ["docker", "run", "-d", "--name", sid.lower(), "--init", "--restart", "unless-stopped",
           "--stop-timeout", "600", "-p", f"{port}:1521", "-e", f"ORACLE_SID={sid}",
           "-v", f"{vol}:/opt/oracle/oradata"]
    if MASTER_DUMPS.is_dir():
        cmd += ["-v", f"{MASTER_DUMPS}:/oracle/dumps"]
    return cmd + docker_extra(vol) + [IMAGE]


def job_master_start(job: Job, sid: str, port: int | None = None) -> None:
    vol = MASTER / sid
    check_volume(vol, sid)
    cs = containers()
    c = master_container(sid, cs)
    t0 = time.time()
    if c:
        if c["running"]:
            job.log(f"Master-DB {sid} läuft bereits ({c['name']}).")
            return
        sh(["docker", "update", "--restart", "unless-stopped", c["name"]], check=False)  # nach hartem Beenden zurücksetzen
        job.run(["docker", "start", c["name"]])
    else:
        port = port or db_meta(sid).get("port")
        if not port:
            raise RuntimeError("Kein Port für die Master-DB angegeben.")
        if any(x["name"] == sid.lower() for x in cs):
            raise RuntimeError(f"Ein anderer Container heißt bereits „{sid.lower()}“.")
        if port_in_use(int(port), cs):
            raise RuntimeError(f"Port {port} ist belegt; bei der Datenbank über „Port ändern“ einen freien wählen. "
                               "Die Datenbank selbst ist unverändert.")
        job.run(master_run_cmd(sid, int(port)))
        db_meta_update(sid, port=int(port))
    wait_ready(job, c["name"] if c else sid.lower(), t0)
    tl_add(sid, "master", f"Master-DB {sid} gestartet")


def job_master_stop(job: Job, sid: str) -> None:
    c = master_container(sid, containers())
    if not c or not c["running"]:
        job.log("Master-DB läuft nicht.")
        return
    job.run(["docker", "stop", "-t", "600", c["name"]], timeout=700)
    tl_add(sid, "master", f"Master-DB {sid} gestoppt")


# --------------------------------------------------------------------------- Port ändern
def free_port(start: int, cs: list[dict] | None = None) -> int:
    cs = cs if cs is not None else containers()
    for port in range(max(1024, start), 65536):
        if not port_in_use(port, cs):
            return port
    raise RuntimeError("Kein freier Port gefunden.")


def check_new_port(port: int, own_container: str | None) -> None:
    if not 1024 <= port <= 65535:
        raise UserError("Port: 1024 bis 65535.")
    others = [c for c in containers() if c["name"] != own_container]
    if port_in_use(port, others):
        raise UserError(f"Port {port} ist belegt (von einem Container oder einem anderen Dienst auf dem Host).")


def job_master_port(job: Job, sid: str, port: int) -> None:
    """Docker kann die Portzuordnung eines Containers nicht ändern: Container mit neuem Port neu erzeugen."""
    c = master_container(sid, containers())
    old = (c["port"] if c else None) or db_meta(sid).get("port")
    if c:
        if c["running"]:
            job.log(f"Master-DB {sid} sauber herunterfahren")
            job.run(["docker", "stop", "-t", "600", c["name"]], timeout=700)
        job.run(["docker", "rm", c["name"]])
    db_meta_update(sid, port=port)
    tl_add(sid, "master-port", f"Port der Master-DB {sid}: {old or '–'} → {port}")
    job_master_start(job, sid, port)


def job_dev_port(job: Job, dev: dict, port: int) -> None:
    did, sid = dev["id"], dev["sid"]
    if dev["container"]:
        if dev["state"] in ("bereit", "startet", "startet neu"):
            job.run(["docker", "stop", "-t", "600", dev["container"]], timeout=700)
        job.run(["docker", "rm", dev["container"]])
    check_volume(DEV / did, sid)
    with _port_lock:
        t0 = time.time()
        job.run(dev_run_cmd(did, sid, port, dev["release"], dev["owner"], dev["created"] or now_iso(), dev["base"]))
    tl_add(sid, "dev-port", f"Port der Entwickler-DB {dev['name']}: {dev['port'] or '–'} → {port}", ref=did)
    wait_ready(job, did.lower(), t0)


@bp.get("/api/ports/free")
def api_free_port():
    return jsonify(port=free_port(int(request.args.get("start") or 1521)))


@bp.post("/api/db/<sid>/master/port")
def api_master_port(sid):
    need_sid(sid)
    port = int((request.get_json(force=True) or {}).get("port") or 0)
    c = master_container(sid, containers())
    if c and c["port"] == port:
        raise UserError(f"Die Master-DB läuft bereits auf Port {port}.")
    check_new_port(port, c["name"] if c else None)
    job = start_job(f"Port der Master-DB {sid} auf {port} ändern", sid, f"master:{sid}", job_master_port, sid, port)
    return jsonify(job=job.as_dict())


@bp.post("/api/devdbs/<did>/port")
def api_dev_port(did):
    dev = find_dev(did)
    port = int((request.get_json(force=True) or {}).get("port") or 0)
    if dev["port"] == port:
        raise UserError(f"Die Entwickler-DB läuft bereits auf Port {port}.")
    if not (DEV_PORT_START <= port <= DEV_PORT_END):
        raise UserError(f"Entwickler-DBs nutzen Ports von {DEV_PORT_START} bis {DEV_PORT_END}.")
    check_new_port(port, dev["container"])
    job = start_job(f"Port der Entwickler-DB {dev['name']} auf {port} ändern", dev["sid"], f"dev:{did}",
                    job_dev_port, dev, port)
    return jsonify(job=job.as_dict())


# --------------------------------------------------------------------------- neue Datenbank anlegen
RE_NEWSID = re.compile(r"^[A-Z][A-Z0-9]{0,7}$")
RE_PDBNAME = re.compile(r"^[A-Z][A-Z0-9_]{0,29}$")
RE_DBPWD = re.compile(r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)[A-Za-z0-9_#$]{8,30}$")
CHARSETS = ["AL32UTF8", "WE8MSWIN1252", "WE8ISO8859P15", "WE8ISO8859P1"]


def image_can_create() -> str | None:
    """Prüft, ob das Image dbca ausführen kann; liefert sonst den Grund."""
    p = sh(["docker", "run", "--rm", "--entrypoint", "bash", IMAGE, "-c",
            'test -s "$ORACLE_HOME/bin/platform_common" || exit 11; '
            'grep -q "/u01/app/oracle" "$ORACLE_HOME/bin/dbca" && exit 12; '
            'test -s "$ORACLE_HOME/bin/sqlplus" || exit 13; exit 0'], check=False, timeout=120)
    return {0: None, 11: "platform_common fehlt im Image", 12: "dbca verweist auf /u01/app/oracle",
            13: "sqlplus ist leer"}.get(p.returncode, (p.stderr or p.stdout).strip()[-300:] or "unbekannt")


TZ_UPGRADE_SH = """echo "Zeitzonendatei auf den neuesten Stand im Oracle Home bringen (startet die DB zweimal neu)"
sqlplus -s / as sysdba <<'EOF'
set feedback off
@?/rdbms/admin/utltz_upg_check.sql
@?/rdbms/admin/utltz_upg_apply.sql
EOF"""


def newdb_script(sid: str, pdb: str, password: str, charset: str, memory_mb: int,
                 ncharset: str = "AL16UTF16") -> str:
    """Bash-Skript, das im Image per dbca eine Datenbank in der Ablage von zeitkapsel anlegt.

    Wird über stdin übergeben, damit das Passwort weder in der Container-Konfiguration noch im
    Vorgangsprotokoll landet. Ergebnis: /opt/oracle/oradata/<SID>/ mit Datendateien, Controlfiles
    und Redo-Logs (wie bei einer migrierten DB), spfile und Passwortdatei unter dbconfig/<SID>/.
    """
    q = shlex.quote
    cdb_args = (f"-createAsContainerDatabase true -numberOfPDBs 1 -pdbName {q(pdb)} -pdbAdminPassword \"$PW\""
                if pdb else "-createAsContainerDatabase false")
    save_state = f"alter pluggable database {pdb} save state;" if pdb else ""
    # dbca übernimmt die Zeitzonendatei der Vorlage (19c: Version 32), das Oracle Home bringt mit den
    # RUs neuere mit. Ohne Anheben scheitert z. B. Data Pump aus neueren Quellen (ORA-39405).
    tz_block = "" if pdb else TZ_UPGRADE_SH
    return f"""set -uo pipefail
PW={q(password)}
SID={q(sid)}
export ORACLE_SID="$SID"
D=/opt/oracle/oradata
mkdir -p "$D/fast_recovery_area" "$ORACLE_BASE/admin/$SID/adump"
dbca -silent -createDatabase -templateName General_Purpose.dbc \\
  -gdbName "$SID" -sid "$SID" {cdb_args} \\
  -sysPassword "$PW" -systemPassword "$PW" \\
  -characterSet {q(charset)} -nationalCharacterSet {q(ncharset)} \\
  -storageType FS -datafileDestination "$D" -useOMF false \\
  -recoveryAreaDestination "$D/fast_recovery_area" -recoveryAreaSize 20480 \\
  -emConfiguration NONE -memoryMgmtType AUTO_SGA -totalMemory {int(memory_mb)} \\
  -redoLogFileSize 200 -databaseType MULTIPURPOSE -ignorePreReqs
rc=$?
if [ $rc -ne 0 ] || [ ! -s "$ORACLE_HOME/dbs/spfile$SID.ora" ]; then
  echo "ZKERR=dbca ist fehlgeschlagen (Exit $rc)"
  tail -n 40 "$ORACLE_BASE"/cfgtoollogs/dbca/"$SID"/*.log 2>/dev/null
  exit 1
fi
{tz_block}
sqlplus -s / as sysdba <<'EOF'
set heading off feedback off pagesize 0 linesize 300
select 'ZK_BANNER='||banner from v$version where rownum = 1;
select 'ZK_TZ='||version from v$timezone_file;
select 'ZK_CDB='||cdb from v$database;
{save_state}
shutdown immediate
exit
EOF
mkdir -p "$D/dbconfig/$SID"
cp "$ORACLE_HOME/dbs/spfile$SID.ora" "$ORACLE_HOME/dbs/orapw$SID" "$D/dbconfig/$SID/" || exit 1
echo "ZK_DONE=1"
"""


def job_newdb(job: Job, sid: str, pdb: str, password: str, charset: str, port: int,
              memory_mb: int = 2048, ncharset: str = "AL16UTF16", start: bool = True) -> None:
    vol = MASTER / sid
    reason = image_can_create()
    if reason:
        raise RuntimeError(f"Das Image {IMAGE} kann keine neuen Datenbanken anlegen ({reason}). "
                           "Bitte das Korrektur-Image aus docker/Dockerfile.fix bauen.")
    if vol.exists():
        raise RuntimeError(f"{vol} gibt es schon.")
    if port_in_use(port, containers()):
        raise RuntimeError(f"Port {port} ist belegt.")
    cname = f"zk-create-{sid.lower()}"
    job.run(["btrfs", "subvolume", "create", str(vol)])
    try:
        prepare_new_volume(job, vol)
        job.run(["chown", f"{ORACLE_UID}:{ORACLE_UID}", str(vol)])
        kind = f"Container-DB mit PDB {pdb}" if pdb else "Non-CDB"
        job.log(f"{kind} wird mit dbca angelegt; das dauert typischerweise 15 bis 45 Minuten.")
        job.on_cancel(lambda: sh(["docker", "rm", "-f", cname], check=False))
        p = job.run(["docker", "run", "--rm", "-i", "--name", cname, "--entrypoint", "bash",
                     "-e", f"ORACLE_SID={sid}", "-v", f"{vol}:/opt/oracle/oradata", IMAGE, "-s"],
                    input=newdb_script(sid, pdb, password, charset, memory_mb, ncharset), secret=password,
                    timeout=CREATE_TIMEOUT)
        out = p.stdout
        if "ZK_DONE=1" not in out:
            raise RuntimeError("Anlage unvollständig: " + (re.search(r"ZKERR=(.*)", out) or [None, "siehe Protokoll"])[1])
        banner = (re.search(r"ZK_BANNER=(.*)", out) or [None, ""])[1]
        edition = "standard" if "Standard" in banner else "enterprise"
        write_dbconfig(vol, sid, edition, "", pdb or sid)
        write_volume_env(vol, dict(ZK_DB_TYPE="cdb" if pdb else "noncdb", ZK_PDB=pdb,
                                   ZK_SERVICE=pdb or sid, ZK_EDITION=edition))
        job.run(["chown", "-R", f"{ORACLE_UID}:{ORACLE_UID}", str(vol)])
        check_volume(vol, sid)
    except Exception:
        sh(["docker", "rm", "-f", cname], check=False)
        sh(["btrfs", "subvolume", "delete", str(vol)], check=False)
        sv_forget(vol)
        raise
    db_meta_update(sid, port=port, origin="neu angelegt", created=now_iso())
    tl_add(sid, "db-created", f"Datenbank {sid} neu angelegt" + (f" (PDB {pdb})" if pdb else " (Non-CDB)"))
    if start:
        job_master_start(job, sid, port)


# --------------------------------------------------------------------------- Import per SSH
RE_HOST = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$")
RE_SSHUSER = re.compile(r"^[a-z_][a-z0-9_-]{0,31}$")
RE_ABSPATH = re.compile(r"^/[A-Za-z0-9._/-]{1,250}$")


def ensure_ssh_key() -> str:
    if not SSH_KEY.exists():
        SSH_KEY.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        sh(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", f"zeitkapsel@{DB_HOST}", "-f", str(SSH_KEY)])
    return Path(str(SSH_KEY) + ".pub").read_text().strip()


def ssh_opts(port: int) -> list[str]:
    return ["-i", str(SSH_KEY), "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ConnectTimeout=10", "-p", str(port)]


def ssh_run(src: dict, script: str, timeout: int = 120) -> subprocess.CompletedProcess:
    ensure_ssh_key()
    return sh(["ssh", *ssh_opts(src["ssh_port"]), f"{src['user']}@{src['host']}", "bash -l -s"],
              input=script, timeout=timeout, check=False)


def oracle_env_script(src: dict) -> str:
    q = shlex.quote
    lines = [f"export ORACLE_SID={q(src['sid'])}"]
    if src.get("oracle_home"):
        lines.append(f"export ORACLE_HOME={q(src['oracle_home'])}")
    lines += ['[ -n "$ORACLE_HOME" ] || { echo "ZKERR=ORACLE_HOME ist auf der Quelle nicht gesetzt"; exit 3; }',
              'export PATH="$ORACLE_HOME/bin:$PATH"']
    return "\n".join(lines) + "\n"


PROBE_SQL = r"""
sqlplus -s -L / as sysdba <<'EOF'
set pagesize 0 linesize 1000 feedback off heading off verify off trimspool on tab off
whenever sqlerror exit 5
select 'ZK_VERSION='||version_full from v$instance;
select 'ZK_STATUS='||status from v$instance;
select 'ZK_CDB='||cdb from v$database;
select 'ZK_LOGMODE='||log_mode from v$database;
select 'ZK_CHARSET='||value from nls_database_parameters where parameter='NLS_CHARACTERSET';
select 'ZK_BANNER='||banner from v$version where rownum = 1;
select 'ZK_SIZE='||sum(bytes) from v$datafile;
select 'ZK_DATAFILE='||name from v$datafile;
select 'ZK_FILE='||name from v$tempfile;
select 'ZK_FILE='||name from v$controlfile;
select 'ZK_FILE='||member from v$logfile;
select 'ZK_P_'||name||'='||value from v$parameter
 where name in ('spfile','memory_target','local_listener','audit_file_dest','db_domain','db_name');
select 'ZK_PDBNAME='||name from v$pdbs where name <> 'PDB$SEED';
exit
EOF
"""


def vtuple(v: str) -> tuple:
    try:
        return tuple(int(x) for x in v.split("."))
    except ValueError:
        return ()


_image_tz: dict[str, int] = {}


def image_tz_max() -> int:
    """Höchste Zeitzonendatei (timezlrg_<n>.dat) im Oracle Home des Images."""
    if IMAGE not in _image_tz:
        p = sh(["docker", "run", "--rm", "--entrypoint", "bash", IMAGE, "-c",
                "ls $ORACLE_HOME/oracore/zoneinfo/ 2>/dev/null"], check=False, timeout=120)
        nums = [int(n) for n in re.findall(r"timezlrg_(\d+)\.dat", p.stdout)]
        _image_tz[IMAGE] = max(nums) if nums else 0
    return _image_tz[IMAGE]


_image_version: dict[str, str] = {}


def image_version() -> str:
    if IMAGE not in _image_version:
        p = sh(["docker", "run", "--rm", "--entrypoint", "bash", IMAGE, "-c", "sqlplus -v"],
               check=False, timeout=120)
        m = re.search(r"Version\s+(\d+(?:\.\d+)+)", p.stdout)
        if not m:
            raise RuntimeError("Version des Images nicht ermittelbar (sqlplus -v). Ist das Image korrigiert?")
        _image_version[IMAGE] = m.group(1)
    return _image_version[IMAGE]


def validate_source(data: dict) -> dict:
    src = dict(host=(data.get("host") or "").strip(), user=(data.get("user") or "oracle").strip(),
               sid=(data.get("sid") or "").strip().upper(), oracle_home=(data.get("oracle_home") or "").strip())
    src["ssh_port"] = int(data.get("ssh_port") or 22)
    if not RE_HOST.match(src["host"]):
        raise UserError("Quell-Host ungültig.")
    if not RE_SSHUSER.match(src["user"]):
        raise UserError("SSH-Benutzer ungültig.")
    if not RE_SID.match(src["sid"]):
        raise UserError("SID ungültig.")
    if src["oracle_home"] and not RE_ABSPATH.match(src["oracle_home"]):
        raise UserError("ORACLE_HOME muss ein absoluter Pfad sein.")
    if not 1 <= src["ssh_port"] <= 65535:
        raise UserError("SSH-Port ungültig.")
    return src


def probe_source(src: dict) -> dict:
    """Liest Version, Dateien und Parameter der laufenden Quell-DB und plant den Import."""
    script = oracle_env_script(src) + 'echo "ZK_OH=$ORACLE_HOME"\n' + \
        'command -v rsync >/dev/null && echo "ZK_RSYNC=1"\n' + PROBE_SQL
    p = ssh_run(src, script, timeout=90)
    kv: dict[str, list[str]] = {}
    for line in p.stdout.splitlines():
        line = line.strip()
        if (line.startswith("ZK") and "=" in line):
            k, _, v = line.partition("=")
            kv.setdefault(k, []).append(v.strip())
    if "ZKERR" in kv:
        raise UserError(kv["ZKERR"][0])
    if "ZK_VERSION" not in kv:
        detail = (p.stderr or p.stdout).strip().splitlines()[-3:]
        raise UserError("Quelle nicht lesbar – läuft die DB, stimmen SSH-Zugang und SID? " + " / ".join(detail))
    one = lambda k, d="": (kv.get(k) or [d])[0]  # noqa: E731
    sid, oh = src["sid"], one("ZK_OH")
    datafiles, files = kv.get("ZK_DATAFILE", []), kv.get("ZK_DATAFILE", []) + kv.get("ZK_FILE", [])
    problems, warnings = [], []
    if not datafiles:
        problems.append("Keine Datendateien gefunden.")
    if not one("ZK_RSYNC"):
        problems.append("rsync fehlt auf der Quelle.")
    if any(f.startswith("+") for f in files):
        problems.append("Dateien liegen in ASM; der Import unterstützt nur Dateisysteme.")
    # Verzeichnisse: das mit der SYSTEM-Datei wird <SID>/, weitere werden _extra/<n>/
    sysdir = next((os.path.dirname(f) for f in datafiles if "system" in os.path.basename(f).lower()),
                  os.path.dirname(datafiles[0]) if datafiles else "")
    dirs = [sysdir] + sorted({os.path.dirname(f) for f in files} - {sysdir})
    if oh and any(d == oh or d.startswith(oh + "/") for d in dirs):
        problems.append(f"Dateien liegen unterhalb von ORACLE_HOME ({oh}); bitte vorher verlagern.")
    mapping = [dict(src=d, rel=sid if i == 0 else f"_extra/{i}",
                    files=sorted(os.path.basename(f) for f in files if os.path.dirname(f) == d))
               for i, d in enumerate(dirs) if d]
    mounts = [f"{m['rel']}:{m['src']}" for m in mapping if m["src"] != f"/opt/oracle/oradata/{sid}"]
    audit = one("ZK_P_audit_file_dest")
    if audit and audit != f"/opt/oracle/admin/{sid}/adump":
        mounts.append(f"_audit:{audit}")
    spfile = one("ZK_P_spfile").replace("?", oh, 1) if one("ZK_P_spfile").startswith("?") else one("ZK_P_spfile")
    if not spfile:
        problems.append("Die Quelle startet mit einer pfile; vorher „create spfile from pfile“ und neu starten.")
    elif spfile.startswith("+"):
        problems.append("Die spfile liegt in ASM.")
    mt = int(one("ZK_P_memory_target", "0") or 0)
    shm = f"{math.ceil(mt * 1.15 / 1024 ** 3)}g" if mt > 0 else ""
    if shm:
        warnings.append(f"AMM aktiv (memory_target {mt // 1024 ** 2} MB); Container bekommen --shm-size {shm}.")
    banner = one("ZK_BANNER")
    edition = "standard" if "Standard" in banner else "enterprise"
    if "Express" in banner:
        problems.append("Express Edition wird nicht unterstützt.")
    if edition == "standard":
        warnings.append("Standard Edition: Container laufen mit ORACLE_EDITION=standard.")
    cdb = one("ZK_CDB") == "YES"
    pdbs = kv.get("ZK_PDBNAME", [])
    domain = one("ZK_P_db_domain")
    base_service = (pdbs[0] if cdb and pdbs else one("ZK_P_db_name", sid))
    service = f"{base_service}.{domain}" if domain else base_service
    ll = one("ZK_P_local_listener")
    ll_kind = "" if not ll else ("address" if "(" in ll or ":" in ll else "alias")
    if ll_kind == "address":
        warnings.append("local_listener enthält eine Adresse; sie wird im Container auf localhost umgestellt.")
    if one("ZK_LOGMODE") == "ARCHIVELOG":
        warnings.append("ARCHIVELOG-Modus: das Archivziel muss im Container existieren (ggf. per Zusatz-Mount).")
    ver, img = one("ZK_VERSION"), image_version()
    if vtuple(img) < vtuple(ver):
        problems.append(f"Das Image ({img}) ist älter als die Quelle ({ver}); erst ein passendes Image bauen.")
    elif vtuple(img) > vtuple(ver):
        warnings.append(f"Das Image ({img}) ist neuer als die Quelle ({ver}); nach dem Import läuft datapatch.")
    if (MASTER / sid).exists():
        problems.append(f"Auf diesem Host gibt es bereits eine Datenbank {sid}.")
    return dict(source=src, version=ver, image_version=img, status=one("ZK_STATUS"), cdb=cdb, pdbs=pdbs,
                charset=one("ZK_CHARSET"), logmode=one("ZK_LOGMODE"), banner=banner, edition=edition,
                size=int(one("ZK_SIZE", "0") or 0), oracle_home=oh, spfile=spfile,
                orapw=f"{oh}/dbs/orapw{sid}", mapping=mapping, mounts=mounts, shm=shm, service=service,
                local_listener=ll, local_listener_kind=ll_kind, problems=problems, warnings=warnings)


NET_FILES = {
    "listener.ora": ("LISTENER =\n  (DESCRIPTION_LIST =\n    (DESCRIPTION =\n"
                     "      (ADDRESS = (PROTOCOL = IPC)(KEY = EXTPROC1))\n"
                     "      (ADDRESS = (PROTOCOL = TCP)(HOST = 0.0.0.0)(PORT = 1521))))\n"
                     "DEDICATED_THROUGH_BROKER_LISTENER = ON\nDIAG_ADR_ENABLED = off\n"),
    "sqlnet.ora": "NAMES.DIRECTORY_PATH = (EZCONNECT, TNSNAMES)\n",
}


def write_dbconfig(vol: Path, sid: str, edition: str, alias: str, service: str) -> None:
    cfg = vol / "dbconfig" / sid
    cfg.mkdir(parents=True, exist_ok=True)
    for name, text in NET_FILES.items():
        (cfg / name).write_text(text)
    tns = ""
    if alias:
        tns += f"{alias} = (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1521))\n\n"
    tns += (f"{sid} = (DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1521))\n"
            f"          (CONNECT_DATA = (SERVICE_NAME = {service})))\n")
    (cfg / "tnsnames.ora").write_text(tns)
    (cfg / "oratab").write_text(f"{sid}:/opt/oracle/product/19c/dbhome_1:N\n")
    (cfg / f".docker_{edition}").touch()
    (vol / f".{sid}.created").touch()


def job_import(job: Job, src: dict, port: int, restart_source: bool) -> None:
    sid = src["sid"]
    vol = MASTER / sid
    job.log(f"Quelle {src['user']}@{src['host']} prüfen")
    plan = probe_source(src)
    if plan["problems"]:
        raise RuntimeError("Import nicht möglich: " + " ".join(plan["problems"]))
    for w in plan["warnings"]:
        job.log("Hinweis: " + w)
    if port_in_use(port, containers()):
        raise RuntimeError(f"Port {port} ist belegt.")
    rsh = "ssh " + " ".join(shlex.quote(o) for o in ssh_opts(src["ssh_port"]))
    remote = f"{src['user']}@{src['host']}"
    shut_down = False
    created = False
    try:
        job.log("Quell-DB sauber herunterfahren (shutdown immediate)")
        p = ssh_run(src, oracle_env_script(src) + "sqlplus -s -L / as sysdba <<'EOF'\nshutdown immediate\nexit\nEOF\n",
                    timeout=900)
        job.log("  " + " ".join(p.stdout.split())[-200:])
        if p.returncode:
            raise RuntimeError("Herunterfahren der Quelle fehlgeschlagen.")
        shut_down = True
        job.run(["btrfs", "subvolume", "create", str(vol)])
        created = True
        prepare_new_volume(job, vol)
        job.log(f"Kopiere {plan['size'] / 1024 ** 3:.1f} GB Datendateien plus Redo-Logs und Controlfiles")
        for m in plan["mapping"]:
            dest = vol / m["rel"]
            dest.mkdir(parents=True, exist_ok=True)
            job.run(["rsync", "-a", "--files-from=-", "-e", rsh, f"{remote}:{m['src']}/", f"{dest}/"],
                    input="\n".join(m["files"]) + "\n", timeout=86400, quiet=True)
        cfg = vol / "dbconfig" / sid
        cfg.mkdir(parents=True, exist_ok=True)
        job.run(["rsync", "-a", "-e", rsh, f"{remote}:{plan['spfile']}", str(cfg / f"spfile{sid}.ora")])
        job.run(["rsync", "-a", "-e", rsh, f"{remote}:{plan['orapw']}", str(cfg / f"orapw{sid}")])
        alias = plan["local_listener"] if plan["local_listener_kind"] == "alias" else ""
        write_dbconfig(vol, sid, plan["edition"], alias, plan["service"])
        for m in plan["mounts"]:
            (vol / m.partition(":")[0]).mkdir(parents=True, exist_ok=True)
        write_volume_env(vol, dict(ZK_DB_TYPE="cdb" if plan["cdb"] else "noncdb",
                                   ZK_PDB=plan["pdbs"][0] if plan["cdb"] and plan["pdbs"] else "",
                                   ZK_SERVICE=plan["service"], ZK_SHM_SIZE=plan["shm"],
                                   ZK_EDITION=plan["edition"], ZK_MOUNTS=" ".join(plan["mounts"])))
        job.run(["chown", "-R", f"{ORACLE_UID}:{ORACLE_UID}", str(vol)])
        db_meta_update(sid, port=port, origin=f"importiert von {src['host']}", created=now_iso(),
                       source=dict(host=src["host"], user=src["user"], version=plan["version"]))
        tl_add(sid, "db-imported", f"Datenbank {sid} von {src['host']} importiert")
    except Exception:
        if created:
            sh(["btrfs", "subvolume", "delete", str(vol)], check=False)
            sv_forget(vol)
        if shut_down:
            job.log("Fehler – Quell-DB wieder starten")
            ssh_run(src, oracle_env_script(src) + "sqlplus -s -L / as sysdba <<'EOF'\nstartup\nexit\nEOF\n", timeout=900)
        raise
    if restart_source:
        job.log("Quell-DB wieder starten")
        ssh_run(src, oracle_env_script(src) + "sqlplus -s -L / as sysdba <<'EOF'\nstartup\nexit\nEOF\n", timeout=900)
    job_master_start(job, sid, port)
    cname = sid.lower()
    if plan["local_listener_kind"] == "address":
        job.log("local_listener auf den Listener im Container umstellen")
        job.run(["docker", "exec", "-i", cname, "sqlplus", "-s", "/ as sysdba"],
                input="alter system set local_listener='(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))' scope=both;\n"
                      "alter system register;\nexit\n")
    if vtuple(plan["image_version"]) > vtuple(plan["version"]):
        job.log("datapatch einspielen (Image neuer als Quelle)")
        job.run(["docker", "exec", cname, "bash", "-c", "$ORACLE_HOME/OPatch/datapatch -verbose"], timeout=7200)


# --------------------------------------------------------------------------- Routen für Master, Neuanlage, Import
@bp.get("/api/summary")
def api_summary():
    cs = containers()
    du = shutil.disk_usage(BASE)
    with _lock:
        running = sum(1 for j in JOBS.values() if j.status == "läuft")
    return jsonify(host=DB_HOST, image=IMAGE, running_jobs=running,
                   disk=dict(total=du.total, used=du.used, free=du.free),
                   dbs=[dict(sid=sid, **master_info(sid, cs)) for sid in master_sids()])


@bp.get("/api/devdbs/<did>")
def api_dev_get(did):
    return jsonify(find_dev(did))


@bp.patch("/api/devdbs/<did>")
def api_dev_owner(did):
    dev = find_dev(did)
    owner = (request.get_json(force=True).get("owner") or "").strip()
    if not RE_OWNER.match(owner) or not owner:
        raise UserError("Besitzer ungültig.")
    f = dev_meta_file(dev["sid"], did)
    try:
        meta = json.loads(f.read_text())
    except (OSError, ValueError):
        meta = dict(base=dev["base"], created=dev["created"])
    meta["owner"] = owner
    write_json(f, meta)
    tl_add(dev["sid"], "dev-owner", f"Entwickler-DB {dev['name']} gehört jetzt {owner}", ref=did)
    return jsonify(ok=True)


@bp.post("/api/db/<sid>/master/<action>")
def api_master(sid, action):
    need_sid(sid)
    if action == "start":
        data = request.get_json(silent=True) or {}
        port = int(data["port"]) if data.get("port") else None
        if port is not None and not 1024 <= port <= 65535:
            raise UserError("Port: 1024 bis 65535.")
        job = start_job(f"Master-DB {sid} starten", sid, f"master:{sid}", job_master_start, sid, port)
    elif action == "stop":
        job = start_job(f"Master-DB {sid} stoppen", sid, f"master:{sid}", job_master_stop, sid)
    else:
        raise UserError("Unbekannte Aktion.")
    return jsonify(job=job.as_dict())


@bp.get("/api/newdb/check")
def api_newdb_check():
    reason = image_can_create()
    return jsonify(ok=reason is None, reason=reason, image=IMAGE, charsets=CHARSETS)


@bp.post("/api/newdb")
def api_newdb():
    data = request.get_json(force=True)
    sid = (data.get("sid") or "").strip().upper()
    pdb = (data.get("pdb") or "").strip().upper()
    pwd = data.get("password") or ""
    charset = data.get("charset") or "AL32UTF8"
    port = int(data.get("port") or 0)
    if not RE_NEWSID.match(sid):
        raise UserError("SID: Großbuchstaben und Ziffern, beginnt mit einem Buchstaben, höchstens 8 Zeichen.")
    if pdb and (not RE_PDBNAME.match(pdb) or pdb == sid):
        raise UserError("PDB-Name: Großbuchstaben, Ziffern, Unterstrich; anders als die SID – oder leer für eine Non-CDB.")
    try:
        memory_mb = int(data.get("memory_mb") or 2048)
    except ValueError:
        raise UserError("Speicher: Zahl in MB.") from None
    if not 1024 <= memory_mb <= 262144:
        raise UserError("Speicher: 1024 bis 262144 MB.")
    if not RE_DBPWD.match(pwd):
        raise UserError("Passwort: 8–30 Zeichen mit Groß- und Kleinbuchstaben und Ziffer; erlaubt sind A–Z, a–z, 0–9, _ # $.")
    if charset not in CHARSETS:
        raise UserError("Zeichensatz nicht unterstützt.")
    if not 1024 <= port <= 65535:
        raise UserError("Port: 1024 bis 65535.")
    if (MASTER / sid).exists():
        raise UserError(f"Eine Datenbank {sid} gibt es auf diesem Host schon.")
    job = start_job(f"Neue Datenbank {sid} anlegen", sid, f"newdb:{sid}", job_newdb, sid, pdb, pwd, charset, port,
                    memory_mb)
    return jsonify(job=job.as_dict())


@bp.get("/api/sshkey")
def api_sshkey():
    return jsonify(public_key=ensure_ssh_key())


@bp.post("/api/import/probe")
def api_import_probe():
    return jsonify(probe_source(validate_source(request.get_json(force=True))))


@bp.post("/api/import")
def api_import():
    data = request.get_json(force=True)
    src = validate_source(data)
    port = int(data.get("port") or 0)
    if not 1024 <= port <= 65535:
        raise UserError("Port: 1024 bis 65535.")
    if (MASTER / src["sid"]).exists():
        raise UserError(f"Eine Datenbank {src['sid']} gibt es auf diesem Host schon.")
    job = start_job(f"Datenbank {src['sid']} von {src['host']} importieren", src["sid"], f"import:{src['sid']}",
                    job_import, src, port, bool(data.get("restart_source")))
    return jsonify(job=job.as_dict())


# --------------------------------------------------------------------------- Import aus einem Verzeichnis auf diesem Host
DB_FILE_RE = re.compile(rb"(/[A-Za-z0-9_.+/-]{2,400}\.(?:dbf|DBF|log|LOG|rdo|RDO|tmp|TMP|ctl|CTL))")


def read_param_file(path: Path) -> dict[str, str]:
    """Parameter aus einer spfile (binär mit Textzeilen) oder pfile lesen: *.name=wert bzw. name=wert."""
    out: dict[str, str] = {}
    try:
        text = path.read_bytes().decode("latin-1")
    except OSError:
        return out
    for raw in re.split(r"[\x00\n\r]", text):
        line = re.sub(r"[\x01-\x08\x0b-\x1f\x7f-\x9f]+", " ", raw)
        if line.lstrip().startswith("#"):
            continue
        # spfile: *.name=wert bzw. SID.name=wert, auch hinter Binärresten; pfile: name=wert
        m = (re.search(r"\*\.([A-Za-z_]\w*)\s*=\s*(.+?)\s*$", line)
             or re.match(r"^\s*[A-Za-z0-9_]+\.([A-Za-z_]\w*)\s*=\s*(.+?)\s*$", line)
             or re.match(r"^\s*([A-Za-z_]\w*)\s*=\s*(.+?)\s*$", line))
        if m:
            out.setdefault(m.group(1).lower(), m.group(2))
    return out


def quoted_list(value: str) -> list[str]:
    items = re.findall(r"'([^']+)'", value or "")
    return items or [v.strip() for v in (value or "").split(",") if v.strip()]


def controlfile_paths(ctl: Path) -> list[str]:
    """Pfade von Daten-, Temp- und Redo-Dateien aus einer Controlfile (Textstellen im Binärformat)."""
    try:
        data = ctl.read_bytes()
    except OSError:
        return []
    seen, out = set(), []
    for m in DB_FILE_RE.finditer(data):
        p = m.group(1).decode("latin-1")
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def instance_running(sid: str) -> bool:
    return sh(["pgrep", "-f", f"ora_pmon_{sid}$"], check=False).returncode == 0


def validate_local(data: dict) -> dict:
    src = dict(path=(data.get("path") or "").strip().rstrip("/"), sid=(data.get("sid") or "").strip().upper(),
               spfile=(data.get("spfile") or "").strip(), orapw=(data.get("orapw") or "").strip())
    if not RE_ABSPATH.match(src["path"] or "x"):
        raise UserError("Verzeichnis: absoluter Pfad.")
    for k in ("spfile", "orapw"):
        if src[k] and not RE_ABSPATH.match(src[k]):
            raise UserError(f"{k}: absoluter Pfad.")
    if not RE_SID.match(src["sid"]):
        raise UserError("SID ungültig.")
    real = os.path.realpath(src["path"])
    for managed in (MASTER, GOLDEN, DEV, DEVSNAPS, BUILD):
        m = os.path.realpath(managed)
        if real == m or real.startswith(m + os.sep):
            raise UserError("Das Verzeichnis gehört zur Ablage von zeitkapsel; dafür „Als Master-DB übernehmen“ in „Alle Snapshots“ nutzen.")
    return src


def probe_local(src: dict) -> dict:
    """Plant den Import einer DB aus einem Verzeichnis auf diesem Host (ohne die DB zu starten)."""
    sid, base = src["sid"], Path(src["path"])
    problems, warnings = [], []
    if not base.is_dir():
        raise UserError(f"{base} gibt es nicht oder ist kein Verzeichnis.")
    # Datenverzeichnis: entweder das angegebene oder <angegeben>/<SID> (Ablage eines Oracle-Containers)
    data_dir = base / sid if (base / sid).is_dir() else base
    cfg_dirs = [base / "dbconfig" / sid, data_dir.parent / "dbconfig" / sid, data_dir, base]

    def find(explicit: str, names: list[str]) -> Path | None:
        if explicit:
            return Path(explicit) if Path(explicit).is_file() else None
        for d in cfg_dirs:
            for n in names:
                if (d / n).is_file():
                    return d / n
        return None

    spfile = find(src["spfile"], [f"spfile{sid}.ora", "spfile.ora"])
    pfile = None if spfile else find("", [f"init{sid}.ora", "init.ora", "pfile.ora"])
    orapw = find(src["orapw"], [f"orapw{sid}", "orapw"])
    if src["spfile"] and not spfile:
        problems.append(f"Die angegebene spfile {src['spfile']} gibt es nicht.")
    if src["orapw"] and not orapw:
        problems.append(f"Die angegebene Passwortdatei {src['orapw']} gibt es nicht.")
    params = read_param_file(spfile or pfile) if (spfile or pfile) else {}

    # Controlfiles: aus den Parametern, sonst alle *.ctl im Datenverzeichnis
    db_ctl = quoted_list(params.get("control_files", ""))
    host_ctl_guess = sorted(str(p) for p in data_dir.rglob("*.ctl")) + sorted(str(p) for p in data_dir.rglob("*.CTL"))

    # DB-Pfad (wie in spfile/Controlfile) → Pfad auf dem Host: Wurzel suchen, unter der die Dateien liegen
    def find_root(db_paths: list[str]) -> str | None:
        for p in db_paths:
            d = os.path.dirname(p)
            for _ in range(4):
                if (data_dir / os.path.relpath(p, d)).exists():
                    return d
                d = os.path.dirname(d)
        return None

    first_ctl_host = None
    if db_ctl:
        db_root = find_root(db_ctl)
        for c in db_ctl:
            h = (data_dir / os.path.relpath(c, db_root)) if db_root and (c + "/").startswith(db_root + "/") else Path(c)
            if h.exists():
                first_ctl_host = first_ctl_host or h
    else:
        db_root = None
        first_ctl_host = Path(host_ctl_guess[0]) if host_ctl_guess else None
    db_files = controlfile_paths(first_ctl_host) if first_ctl_host else []
    if not db_root:
        db_root = find_root([p for p in db_files if p.lower().endswith(".dbf")] + db_ctl)
    if not first_ctl_host:
        problems.append("Keine Controlfile gefunden.")
    if not db_root:
        problems.append(f"Die Dateien der Datenbank liegen nicht unter {data_dir}.")
        db_root = f"/opt/oracle/oradata/{sid}"

    # Zuordnung: alles unter db_root kommt nach <SID>/, Dateien außerhalb (sofern auf dem Host vorhanden) nach _extra/<n>
    mapping = [dict(src=str(data_dir), rel=sid, db_path=db_root, files=None)]
    extra: dict[str, list[str]] = {}
    missing = []
    for p in db_ctl + db_files:
        if (p + "/").startswith(db_root + "/"):
            if not (data_dir / os.path.relpath(p, db_root)).exists():
                missing.append(p)
        elif os.path.exists(p):
            extra.setdefault(os.path.dirname(p), []).append(os.path.basename(p))
        else:
            missing.append(p)
    for i, (d, files) in enumerate(sorted(extra.items()), 1):
        mapping.append(dict(src=d, rel=f"_extra/{i}", db_path=d, files=sorted(set(files))))
    if missing:
        warnings.append(f"{len(missing)} in der Controlfile genannte Dateien fehlen (z. B. {missing[0]}); "
                        "das können alte Einträge sein – der Prüfstart zeigt, ob die DB trotzdem öffnet.")
    mounts = [f"{m['rel']}:{m['db_path']}" for m in mapping if m["db_path"] != f"/opt/oracle/oradata/{sid}"]
    audit = params.get("audit_file_dest", "").strip("'\"")
    if audit and audit != f"/opt/oracle/admin/{sid}/adump":
        mounts.append(f"_audit:{audit}")
    mt = params.get("memory_target", "0").strip("'\"")
    mt_bytes = parse_size(mt)
    shm = f"{math.ceil(mt_bytes * 1.15 / 1024 ** 3)}g" if mt_bytes > 0 else ""
    if shm:
        warnings.append(f"AMM aktiv (memory_target {mt}); Container bekommen --shm-size {shm}.")
    size = sum(f.stat().st_size for f in data_dir.rglob("*") if f.is_file()) + \
        sum(os.path.getsize(os.path.join(m["src"], f)) for m in mapping[1:] for f in m["files"])

    # Läuft die Quelle noch?
    if instance_running(sid):
        problems.append(f"Auf diesem Host läuft eine Instanz {sid} (nativ oder in einem Container); vorher sauber herunterfahren.")
    real = os.path.realpath(base)
    for c in containers():
        if c["running"] and c["mount"] and (c["mount"] == real or real.startswith(c["mount"] + os.sep)
                                             or c["mount"].startswith(real + os.sep)):
            problems.append(f"Der Container {c['name']} benutzt das Verzeichnis und läuft noch.")
    if (MASTER / sid).exists():
        problems.append(f"Auf diesem Host gibt es bereits eine Master-DB {sid}.")

    needs = {}
    if not spfile:
        needs["spfile"] = "pfile" if pfile else "minimal"
        warnings.append(f"Keine spfile gefunden; sie wird aus {pfile} erzeugt." if pfile else
                        "Weder spfile noch pfile gefunden; eine neue lässt sich nur aus Mindestangaben erzeugen "
                        "(DB-Name, Blockgröße, Undo-Tablespace, Speicher).")
    if not orapw:
        needs["orapw"] = True
        warnings.append("Keine Passwortdatei gefunden; eine neue braucht ein SYS-Passwort.")
    ll = params.get("local_listener", "").strip("'\"")
    db_control_files = db_ctl or [db_root.rstrip("/") + "/" + os.path.relpath(h, data_dir) for h in host_ctl_guess]
    return dict(mode="local", source=src, data_dir=str(data_dir), spfile=str(spfile or ""), pfile=str(pfile or ""),
                orapw=str(orapw or ""), control_files=db_control_files, db_root=db_root,
                mapping=[dict(m, count=None if m["files"] is None else len(m["files"])) for m in mapping],
                mounts=mounts, shm=shm, size=size, db_name=params.get("db_name", sid).strip("'\""),
                local_listener=ll, local_listener_kind="" if not ll else ("address" if "(" in ll or ":" in ll else "alias"),
                needs=needs, problems=problems, warnings=warnings, image_version=image_version())


def parse_size(value: str) -> int:
    m = re.match(r"^\s*(\d+)\s*([KMGT]?)\s*$", (value or "0").upper())
    if not m:
        return 0
    return int(m.group(1)) * {"": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}[m.group(2)]


def minimal_pfile(sid: str, db_name: str, control_files: list[str], block_size: int, undo: str, memory_mb: int) -> str:
    ctl = ",".join(f"'{c}'" for c in control_files)
    return (f"db_name='{db_name}'\ncontrol_files={ctl}\ndb_block_size={block_size}\ncompatible='19.0.0'\n"
            f"undo_tablespace='{undo}'\nundo_management='AUTO'\nsga_target={memory_mb * 7 // 10}M\n"
            f"pga_aggregate_target={memory_mb * 3 // 10}M\nprocesses=300\nremote_login_passwordfile='EXCLUSIVE'\n"
            f"audit_file_dest='/opt/oracle/admin/{sid}/adump'\naudit_trail='none'\ndiagnostic_dest='/opt/oracle'\n")


def verify_script(sid: str, make_spfile: bool, sys_pw: str | None) -> str:
    q = shlex.quote
    lines = [f"set -uo pipefail\nSID={q(sid)}\nexport ORACLE_SID=\"$SID\"\nC=/opt/oracle/oradata/dbconfig/$SID",
             'mkdir -p "$ORACLE_BASE/admin/$SID/adump"']
    if make_spfile:
        lines.append("""sqlplus -s / as sysdba <<EOF || { echo "ZKERR=spfile konnte nicht erzeugt werden"; exit 1; }
whenever sqlerror exit 3
create spfile='$C/spfile$SID.ora' from pfile='$C/init$SID.ora';
exit
EOF""")
    if sys_pw:
        lines.append(f'orapwd file="$C/orapw$SID" password={q(sys_pw)} format=12.2 force=y '
                     '|| { echo "ZKERR=Passwortdatei konnte nicht angelegt werden"; exit 1; }')
    lines.append('ln -sf "$C/spfile$SID.ora" "$ORACLE_HOME/dbs/spfile$SID.ora"; ln -sf "$C/orapw$SID" "$ORACLE_HOME/dbs/orapw$SID"')
    lines.append("""sqlplus -s / as sysdba <<'EOF'
set heading off feedback off pagesize 0 linesize 300
whenever sqlerror exit 7
startup
select 'ZK_BANNER='||banner from v$version where rownum = 1;
select 'ZK_CDB='||cdb from v$database;
select 'ZK_PDBNAME='||name from v$pdbs where name <> 'PDB$SEED';
select 'ZK_DOMAIN='||value from v$parameter where name = 'db_domain';
select 'ZK_DBNAME='||value from v$parameter where name = 'db_name';
shutdown immediate
exit
EOF
rc=$?
[ $rc -eq 0 ] || { echo "ZKERR=Prüfstart fehlgeschlagen (Exit $rc)"; exit 1; }
echo "ZK_DONE=1"
""")
    return "\n".join(lines) + "\n"


def job_local_import(job: Job, src: dict, port: int, opts: dict) -> None:
    sid = src["sid"]
    vol = MASTER / sid
    plan = probe_local(src)
    if plan["problems"]:
        raise RuntimeError("Import nicht möglich: " + " ".join(plan["problems"]))
    needs = plan["needs"]
    if needs.get("spfile") and not opts.get("create_spfile"):
        raise RuntimeError("Es gibt keine spfile; der Import geht nur mit „neue spfile anlegen“.")
    if needs.get("orapw") and not opts.get("sys_password"):
        raise RuntimeError("Ohne Passwortdatei geht es nur mit einem SYS-Passwort für eine neue.")
    if port_in_use(port, containers()):
        raise RuntimeError(f"Port {port} ist belegt.")
    for w in plan["warnings"]:
        job.log("Hinweis: " + w)
    job.run(["btrfs", "subvolume", "create", str(vol)])
    try:
        prepare_new_volume(job, vol)
        job.log(f"Kopiere {plan['size'] / 1024 ** 3:.1f} GB in das neue Subvolume (wird dabei komprimiert)")
        for m in plan["mapping"]:
            dest = vol / m["rel"]
            dest.mkdir(parents=True, exist_ok=True)
            if m["files"] is None:
                job.run(["rsync", "-a", f"{m['src']}/", f"{dest}/"], timeout=86400, quiet=True)
            else:
                job.run(["rsync", "-a", "--files-from=-", f"{m['src']}/", f"{dest}/"],
                        input="\n".join(m["files"]) + "\n", timeout=86400, quiet=True)
        cfg = vol / "dbconfig" / sid
        cfg.mkdir(parents=True, exist_ok=True)
        if plan["spfile"]:
            job.run(["cp", "-p", plan["spfile"], str(cfg / f"spfile{sid}.ora")])
        elif plan["pfile"]:
            job.run(["cp", "-p", plan["pfile"], str(cfg / f"init{sid}.ora")])
        else:
            o = opts
            (cfg / f"init{sid}.ora").write_text(minimal_pfile(
                sid, o.get("db_name") or plan["db_name"] or sid, plan["control_files"],
                int(o.get("block_size") or 8192), o.get("undo") or "UNDOTBS1", int(o.get("memory_mb") or 2048)))
            job.log(f"Minimale pfile geschrieben: {cfg / f'init{sid}.ora'}")
        if plan["orapw"]:
            job.run(["cp", "-p", plan["orapw"], str(cfg / f"orapw{sid}")])
        alias = plan["local_listener"] if plan["local_listener_kind"] == "alias" else ""
        write_dbconfig(vol, sid, "enterprise", alias, sid)
        for m in plan["mounts"]:
            (vol / m.partition(":")[0]).mkdir(parents=True, exist_ok=True)
        write_volume_env(vol, dict(ZK_DB_TYPE="noncdb", ZK_SHM_SIZE=plan["shm"], ZK_MOUNTS=" ".join(plan["mounts"])))
        job.run(["chown", "-R", f"{ORACLE_UID}:{ORACLE_UID}", str(vol)])

        job.log("Prüfstart in einem Wegwerf-Container (startup, Abfragen, shutdown immediate)")
        pw = opts.get("sys_password") if needs.get("orapw") else None
        job.on_cancel(lambda: sh(["docker", "rm", "-f", f"zk-verify-{sid.lower()}"], check=False))
        p = job.run(["docker", "run", "--rm", "-i", "--name", f"zk-verify-{sid.lower()}", "--entrypoint", "bash",
                     "-e", f"ORACLE_SID={sid}", "-v", f"{vol}:/opt/oracle/oradata", *docker_extra(vol), IMAGE, "-s"],
                    input=verify_script(sid, not plan["spfile"], pw), secret=pw, timeout=3600, allow_fail=True)
        out = p.stdout + p.stderr
        if "ZK_DONE=1" not in out:
            ora = sorted(set(re.findall(r"(ORA-\d{5}[^\n]*)", out)))[:3]
            raise RuntimeError((re.search(r"ZKERR=(.*)", out) or [None, "Prüfstart fehlgeschlagen"])[1]
                               + (": " + " | ".join(ora) if ora else "") + " – nichts übernommen, die Quelle ist unverändert.")
        kv = {}
        for line in out.splitlines():
            if line.startswith("ZK_") and "=" in line:
                k, _, v = line.partition("=")
                kv.setdefault(k, []).append(v.strip())
        cdb = (kv.get("ZK_CDB") or ["NO"])[0] == "YES"
        pdbs = kv.get("ZK_PDBNAME", [])
        domain = (kv.get("ZK_DOMAIN") or [""])[0]
        base_service = pdbs[0] if cdb and pdbs else (kv.get("ZK_DBNAME") or [sid])[0]
        service = f"{base_service}.{domain}" if domain else base_service
        edition = "standard" if "Standard" in (kv.get("ZK_BANNER") or [""])[0] else "enterprise"
        if edition != "enterprise":
            (cfg / ".docker_enterprise").unlink(missing_ok=True)
            (cfg / f".docker_{edition}").touch()
        write_dbconfig(vol, sid, edition, alias, service)
        write_volume_env(vol, dict(ZK_DB_TYPE="cdb" if cdb else "noncdb", ZK_PDB=pdbs[0] if cdb and pdbs else "",
                                   ZK_SERVICE=service, ZK_SHM_SIZE=plan["shm"], ZK_EDITION=edition,
                                   ZK_MOUNTS=" ".join(plan["mounts"])))
        job.run(["chown", "-R", f"{ORACLE_UID}:{ORACLE_UID}", str(vol / "dbconfig"), str(vol / VOLUME_ENV)])
        check_volume(vol, sid)
    except Exception:
        sh(["docker", "rm", "-f", f"zk-verify-{sid.lower()}"], check=False)
        sh(["btrfs", "subvolume", "delete", str(vol)], check=False)
        sv_forget(vol)
        raise
    db_meta_update(sid, port=port, origin=f"importiert aus {src['path']}", created=now_iso())
    tl_add(sid, "db-imported", f"Datenbank {sid} aus {src['path']} importiert")
    job_master_start(job, sid, port)
    if plan["local_listener_kind"] == "address":
        job.run(["docker", "exec", "-i", sid.lower(), "sqlplus", "-s", "/ as sysdba"],
                input="alter system set local_listener='(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))' scope=both;\n"
                      "alter system register;\nexit\n")
    job.log("datapatch einspielen (gleicht das Data Dictionary an den Patch-Stand des Images an)")
    job.run(["docker", "exec", sid.lower(), "bash", "-c", "$ORACLE_HOME/OPatch/datapatch -verbose"], timeout=7200)


@bp.post("/api/import/local/probe")
def api_local_probe():
    return jsonify(probe_local(validate_local(request.get_json(force=True))))


@bp.post("/api/import/local")
def api_local_import():
    data = request.get_json(force=True)
    src = validate_local(data)
    port = int(data.get("port") or 0)
    if not 1024 <= port <= 65535:
        raise UserError("Port: 1024 bis 65535.")
    opts = dict(create_spfile=bool(data.get("create_spfile")), db_name=(data.get("db_name") or "").strip().upper(),
                block_size=int(data.get("block_size") or 8192), undo=(data.get("undo") or "UNDOTBS1").strip().upper(),
                memory_mb=int(data.get("memory_mb") or 2048), sys_password=data.get("sys_password") or "")
    if opts["db_name"] and not re.match(r"^[A-Z][A-Z0-9_]{0,7}$", opts["db_name"]):
        raise UserError("DB-Name: höchstens 8 Zeichen, Großbuchstaben, Ziffern, Unterstrich.")
    if opts["block_size"] not in (2048, 4096, 8192, 16384, 32768):
        raise UserError("Blockgröße: 2048, 4096, 8192, 16384 oder 32768.")
    if not re.match(r"^[A-Z][A-Z0-9_$#]{0,29}$", opts["undo"]):
        raise UserError("Undo-Tablespace ungültig.")
    if not 1024 <= opts["memory_mb"] <= 262144:
        raise UserError("Speicher: 1024 bis 262144 MB.")
    if opts["sys_password"] and not RE_DBPWD.match(opts["sys_password"]):
        raise UserError("SYS-Passwort: 8–30 Zeichen mit Groß- und Kleinbuchstaben und Ziffer; erlaubt sind A–Z, a–z, 0–9, _ # $.")
    if (MASTER / src["sid"]).exists():
        raise UserError(f"Eine Datenbank {src['sid']} gibt es auf diesem Host schon.")
    job = start_job(f"Datenbank {src['sid']} aus {src['path']} importieren", src["sid"], f"import:{src['sid']}",
                    job_local_import, src, port, opts)
    return jsonify(job=job.as_dict())


# --------------------------------------------------------------------------- Übernahme per Data Pump über DB-Link (Quelle läuft weiter)
RE_SERVICE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
RE_DBUSER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]{0,127}$")
RE_SCHEMA = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}$")


def validate_dp_source(data: dict) -> dict:
    src = dict(host=(data.get("host") or "").strip(), port=int(data.get("port") or 1521),
               service=(data.get("service") or "").strip(), user=(data.get("user") or "SYSTEM").strip().upper(),
               password=data.get("password") or "")
    if not RE_HOST.match(src["host"]):
        raise UserError("Quell-Host ungültig.")
    if not 1 <= src["port"] <= 65535:
        raise UserError("Port der Quelle ungültig.")
    if not RE_SERVICE.match(src["service"]):
        raise UserError("Service-Name ungültig.")
    if not RE_DBUSER.match(src["user"]):
        raise UserError("Benutzer ungültig.")
    if not src["password"] or len(src["password"]) > 128 or any(c in src["password"] for c in '"\n\r\x00'):
        raise UserError("Passwort der Quelle fehlt oder enthält ein Anführungszeichen bzw. einen Zeilenumbruch.")
    return src


def dp_connect_sql(src: dict) -> str:
    return f'connect {src["user"]}/"{src["password"]}"@//{src["host"]}:{src["port"]}/{src["service"]}'


DP_PROBE_SQL = r"""
set pagesize 0 linesize 1000 feedback off heading off verify off trimspool on tab off
select 'ZK_VERSION='||version from v$instance;
select 'ZK_DBNAME='||name||' ZK_CDB='||cdb||' ZK_LOGMODE='||log_mode from v$database;
select 'ZK_CHARSET='||value from nls_database_parameters where parameter='NLS_CHARACTERSET';
select 'ZK_NCHARSET='||value from nls_database_parameters where parameter='NLS_NCHAR_CHARACTERSET';
select 'ZK_BANNER='||banner from v$version where rownum = 1;
select 'ZK_ROLE='||granted_role from user_role_privs where granted_role in ('DBA','DATAPUMP_EXP_FULL_DATABASE','EXP_FULL_DATABASE');
select 'ZK_SCHEMA='||u.username||'|'||nvl(sum(s.bytes),0) from dba_users u left join dba_segments s on s.owner = u.username
 where u.oracle_maintained = 'N' group by u.username order by u.username;
select 'ZK_TS='||t.tablespace_name||'|'||t.block_size||'|'||t.bigfile||'|'||
       nvl((select sum(bytes) from dba_data_files f where f.tablespace_name = t.tablespace_name),0)
  from dba_tablespaces t where t.contents = 'PERMANENT' and t.tablespace_name not in ('SYSTEM','SYSAUX');
select 'ZK_LONG='||count(*) from dba_tab_columns c join dba_users u on u.username = c.owner
 where u.oracle_maintained = 'N' and c.data_type in ('LONG','LONG RAW');
select 'ZK_UNDORET='||value from v$parameter where name = 'undo_retention';
select 'ZK_TZ='||version from v$timezone_file;
select 'ZK_TEMPTS='||tablespace_name||'|'||block_size||'|'||bigfile from dba_tablespaces where contents = 'TEMPORARY';
select 'ZK_DEFTEMP='||property_value from database_properties where property_name = 'DEFAULT_TEMP_TABLESPACE';
select 'ZK_DEFPERM='||property_value from database_properties where property_name = 'DEFAULT_PERMANENT_TABLESPACE';
select distinct 'ZK_VERIFYFN='||limit from dba_profiles where resource_name = 'PASSWORD_VERIFY_FUNCTION'
 and limit not in ('NULL','DEFAULT','UNLIMITED');
exit
"""


def run_oracle_client(script: str, secret: str, timeout: int = 180) -> subprocess.CompletedProcess:
    """sqlplus aus dem Image in einem Wegwerf-Container; das Skript (mit Passwort) kommt über stdin."""
    return sh(["docker", "run", "--rm", "-i", "--entrypoint", "bash", IMAGE, "-s"],
              input=script, timeout=timeout, check=False)


def probe_dp(src: dict, target_sid: str = "") -> dict:
    script = "sqlplus -s -L /nolog <<'EOF'\nwhenever sqlerror exit 5\n" + dp_connect_sql(src) + "\n" + DP_PROBE_SQL + "EOF\n"
    p = run_oracle_client(script, src["password"])
    out = (p.stdout + p.stderr).replace(src["password"], "***")
    kv: dict[str, list[str]] = {}
    for line in out.splitlines():
        for part in re.findall(r"(ZK_[A-Z]+)=(\S.*?)(?=\s+ZK_[A-Z]+=|$)", line.strip()):
            kv.setdefault(part[0], []).append(part[1].strip())
    if "ZK_VERSION" not in kv:
        ora = sorted(set(re.findall(r"(ORA-\d{5}[^\n]*)", out)))[:2]
        raise UserError("Verbindung zur Quelle fehlgeschlagen: " + (" | ".join(ora) or out.strip()[-200:] or "keine Antwort"))
    one = lambda k, d="": (kv.get(k) or [d])[0]  # noqa: E731
    problems, warnings = [], []
    roles = kv.get("ZK_ROLE", [])
    if not roles:
        problems.append(f"{src['user']} hat weder DBA noch DATAPUMP_EXP_FULL_DATABASE; damit lässt sich die Datenbank nicht übertragen.")
    schemas = [dict(name=n, bytes=int(b or 0)) for n, _, b in (x.partition("|") for x in kv.get("ZK_SCHEMA", []))]
    temp_ts = []
    for x in kv.get("ZK_TEMPTS", []):
        name, block, big = (x.split("|") + ["", "8192", "NO"])[:3]
        temp_ts.append(dict(name=name, block_size=int(block or 8192), bigfile=big == "YES"))
    verify_fns = sorted(set(kv.get("ZK_VERIFYFN", [])))
    if verify_fns:
        warnings.append("Profile mit Passwort-Prüffunktion (" + ", ".join(verify_fns) + "): die Funktionen liegen in SYS, "
                        "werden von Data Pump nicht übertragen und deshalb vorab aus der Quelle kopiert.")
    tablespaces = []
    for x in kv.get("ZK_TS", []):
        name, block, big, size = (x.split("|") + ["", "", "", "0"])[:4]
        tablespaces.append(dict(name=name, block_size=int(block or 8192), bigfile=big == "YES", bytes=int(size or 0)))
    odd = sorted({t["block_size"] for t in tablespaces if t["block_size"] != 8192})
    if odd:
        warnings.append(f"Tablespaces mit Blockgröße {', '.join(map(str, odd))}: dafür werden passende Caches gesetzt.")
    if int(one("ZK_LONG", "0") or 0):
        warnings.append(f"{one('ZK_LONG')} Spalten vom Typ LONG/LONG RAW: solche Tabellen lassen sich über einen DB-Link nicht "
                        "übertragen (ORA-31679) und fehlen danach; sie müssen per expdp-Datei nachgeholt werden.")
    ret = int(one("ZK_UNDORET", "900") or 900)
    size = sum(s["bytes"] for s in schemas)
    if size > 50 * 1024 ** 3 and ret < 3600:
        warnings.append(f"undo_retention der Quelle ist {ret} s; bei {size / 1024 ** 3:.0f} GB und konsistentem Stand "
                        "droht ORA-01555. Auf der Quelle erhöhen oder „konsistent“ abwählen.")
    src_tz, img_tz = int(one("ZK_TZ", "0") or 0), image_tz_max()
    if src_tz and img_tz and src_tz > img_tz:
        problems.append(f"Die Zeitzonendatei der Quelle (Version {src_tz}) ist neuer als die neueste im Image ({img_tz}); "
                        "dafür braucht das Image einen RU mit passendem DST-Stand.")
    elif src_tz and img_tz:
        warnings.append(f"Zeitzonendatei: Quelle Version {src_tz}, Image bis {img_tz}; die neue DB wird bei Bedarf angehoben.")
    major = vtuple(one("ZK_VERSION"))[:1]
    img = image_version()
    if major and major > vtuple(img)[:1]:
        problems.append(f"Die Quelle ({one('ZK_VERSION')}) ist neuer als das Image ({img}).")
    if one("ZK_CDB") == "YES" and "." not in src["service"] and src["service"].upper() == one("ZK_DBNAME").upper():
        warnings.append("Verbunden mit dem Root einer CDB; für die Anwendungsdaten meist den Service der PDB angeben.")
    if not schemas:
        problems.append("Keine Anwendungs-Schemas gefunden.")
    target = dict(sid=target_sid, exists=False, resume=False)
    if target_sid and RE_SID.match(target_sid) and (MASTER / target_sid).exists():
        target.update(exists=True, resume=bool(db_meta(target_sid).get("dp_pending")))
        if target["resume"]:
            warnings.append(f"Die Datenbank {target_sid} stammt aus einem abgebrochenen Versuch und wird weiterverwendet; "
                            "Port, Speicher und Passwort gelten dann nicht.")
        else:
            problems.append(f"Eine Datenbank {target_sid} gibt es auf diesem Host schon.")
    return dict(mode="dp", target=target, source={k: v for k, v in src.items() if k != "password"}, version=one("ZK_VERSION"),
                dbname=one("ZK_DBNAME"), cdb=one("ZK_CDB") == "YES", logmode=one("ZK_LOGMODE"),
                charset=one("ZK_CHARSET", "AL32UTF8"), ncharset=one("ZK_NCHARSET", "AL16UTF16"),
                banner=one("ZK_BANNER"), roles=roles, schemas=schemas, tablespaces=tablespaces, tz=src_tz, image_tz=img_tz,
                temp_tablespaces=temp_ts, default_temp=one("ZK_DEFTEMP"), default_perm=one("ZK_DEFPERM"),
                verify_functions=verify_fns,
                size=size, undo_retention=ret, image_version=img, problems=problems, warnings=warnings)


def sql_str(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def db_tz(cname: str) -> int:
    p = sh(["docker", "exec", "-i", cname, "sqlplus", "-s", "/ as sysdba"],
           input="set heading off feedback off pagesize 0\nselect 'ZK_TZ='||version from v$timezone_file;\nexit\n",
           check=False, timeout=120)
    m = re.search(r"ZK_TZ=(\d+)", p.stdout)
    return int(m.group(1)) if m else 0


def ensure_tz(job: Job, cname: str, needed: int) -> None:
    """Zeitzonendatei der DB anheben, wenn sie älter ist als needed (Data Pump verlangt Ziel >= Quelle)."""
    if not needed:
        return
    have = db_tz(cname)
    job.log(f"Zeitzonendatei: Quelle Version {needed}, neue DB Version {have or '?'}")
    if have and have >= needed:
        return
    job.log("Zeitzonendatei der neuen DB anheben (utltz_upg_check/apply; die DB startet dabei zweimal neu)")
    rc, _ = job.stream(["docker", "exec", "-i", cname, "sqlplus", "-s", "/ as sysdba"],
                       input="set feedback off\n@?/rdbms/admin/utltz_upg_check.sql\n@?/rdbms/admin/utltz_upg_apply.sql\nexit\n")
    have = db_tz(cname)
    if have < needed:
        raise RuntimeError(f"Zeitzonendatei der neuen DB ist nach dem Anheben Version {have or '?'}, "
                           f"die Quelle hat {needed}; Data Pump würde mit ORA-39405 abbrechen.")
    job.log(f"Zeitzonendatei jetzt Version {have}")


def job_dp_import(job: Job, src: dict, tgt: dict) -> None:
    sid = tgt["sid"]
    job.log(f"Quelle {src['user']}@{src['host']}:{src['port']}/{src['service']} prüfen")
    plan = probe_dp(src)
    if plan["problems"]:
        raise RuntimeError("Übernahme nicht möglich: " + " ".join(plan["problems"]))
    for w in plan["warnings"]:
        job.log("Hinweis: " + w)
    known = {s["name"] for s in plan["schemas"]}
    schemas = tgt["schemas"] or sorted(known)
    unknown = [s for s in schemas if s not in known]
    if unknown:
        raise RuntimeError("Unbekannte Schemas: " + ", ".join(unknown))
    charset = plan["charset"]
    if charset not in CHARSETS:
        job.log(f"Zeichensatz der Quelle {charset} wird übernommen")
    cname = sid.lower()
    if (MASTER / sid).exists():
        if not db_meta(sid).get("dp_pending"):
            raise RuntimeError(f"Eine Datenbank {sid} gibt es schon und stammt nicht aus einem abgebrochenen Data-Pump-Versuch.")
        job.log(f"Datenbank {sid} aus dem abgebrochenen Versuch wird weiterverwendet (keine Neuanlage)")
        c = master_container(sid, containers())
        if not c or not c["running"]:
            job_master_start(job, sid)
        job.log("Reste des vorigen Versuchs entfernen (Anwendungs-Benutzer mit ihren Objekten, eigene Rollen und Profile)")
        p = job.run(["docker", "exec", "-i", cname, "sqlplus", "-s", "/ as sysdba"], allow_fail=True, input=(
            "set serveroutput on feedback off\nbegin\n"
            "  for u in (select username from dba_users where oracle_maintained = 'N') loop\n"
            "    execute immediate 'drop user \"'||u.username||'\" cascade';\n  end loop;\n"
            "  for r in (select role from dba_roles where oracle_maintained = 'N') loop\n"
            "    execute immediate 'drop role \"'||r.role||'\"';\n  end loop;\n"
            "  for f in (select distinct profile from dba_profiles where profile <> 'DEFAULT' and common = 'NO'\n"
            "             and profile not in (select profile from dba_profiles where profile like 'ORA_%')) loop\n"
            "    begin execute immediate 'drop profile \"'||f.profile||'\" cascade'; exception when others then null; end;\n  end loop;\n"
            "  dbms_output.put_line('ZK_CLEAN=ok');\nend;\n/\nexit\n"))
        if "ZK_CLEAN=ok" not in p.stdout:
            raise RuntimeError("Reste des vorigen Versuchs ließen sich nicht entfernen: " +
                               " | ".join(dict.fromkeys(re.findall(r"(ORA-\d{5}[^\n]*)", p.stdout)))[:300])
    else:
        job.log(f"Neue Datenbank {sid} anlegen (Zeichensatz {charset}, national {plan['ncharset']})")
        job_newdb(job, sid, "", tgt["password"], charset, tgt["port"], tgt["memory_mb"], plan["ncharset"], start=False)
        # schon jetzt als fortsetzbar markieren: scheitert danach etwas (z. B. ein Port), entfällt beim nächsten Versuch dbca
        db_meta_update(sid, dp_pending=True, dp_source=f"{src['host']}:{src['port']}/{src['service']}")
        job_master_start(job, sid, tgt["port"])
    edition = read_volume_env(MASTER / sid).get("ZK_EDITION", "enterprise")
    parallel = tgt["parallel"] if edition == "enterprise" else 1
    try:
        job.run(["docker", "exec", cname, "mkdir", "-p", "/opt/oracle/oradata/zk_import"])
        ensure_tz(job, cname, plan.get("tz") or 0)
        # Tablespaces vorab als OMF-Dateien im Subvolume anlegen (die Pfade der Quelle gibt es hier nicht)
        caches = sorted({t["block_size"] for t in plan["tablespaces"] if t["block_size"] != 8192})
        ts_calls = "\n".join(
            f"  mk({sql_str(t['name'])}, {t['block_size']}, '{'YES' if t['bigfile'] else 'NO'}', {t['bytes']});"
            for t in plan["tablespaces"])
        temp_calls = "\n".join(f"  mkt({sql_str(t['name'])}, '{'YES' if t['bigfile'] else 'NO'}');"
                               for t in plan.get("temp_tablespaces", []))
        fn_calls = "\n".join(f"  copy_fn({sql_str(n)});" for n in plan.get("verify_functions", []))
        defaults = ""
        if plan.get("default_temp"):
            defaults += (f"begin execute immediate 'alter database default temporary tablespace \"'||{sql_str(plan['default_temp'])}||'\"';"
                         " exception when others then null; end;\n/\n")
        if plan.get("default_perm") and plan["default_perm"] not in ("SYSTEM", "SYSAUX"):
            defaults += (f"begin execute immediate 'alter database default tablespace \"'||{sql_str(plan['default_perm'])}||'\"';"
                         " exception when others then null; end;\n/\n")
        prep = (
            "whenever sqlerror exit 9\nset feedback off\n"
            "alter system set db_create_file_dest='/opt/oracle/oradata' scope=both;\n"
            + "".join(f"alter system set db_{b // 1024}k_cache_size=64M scope=both;\n" for b in caches)
            + "declare\n  procedure mk(n varchar2, bs number, big varchar2, bytes number) is c number; have number; files number;\n"
              "  begin\n    select count(*) into c from dba_tablespaces where tablespace_name = n;\n"
              "    if c = 0 then\n"
              "      execute immediate 'create '||case when big='YES' then 'bigfile ' end||'tablespace \"'||n||'\" datafile size 100M"
              " autoextend on next 256M maxsize unlimited'||case when bs <> 8192 then ' blocksize '||bs end;\n"
              "    end if;\n"
              # Smallfile: eine Datei wächst bis ca. 32 GB; auch bei vorhandenen Tablespaces auf die nötige Anzahl bringen
              "    if big = 'NO' then\n"
              "      select count(*) into have from dba_data_files where tablespace_name = n;\n"
              "      files := ceil(bytes * 1.1 / 32212254720);  -- 30 GB je Datei, 10 % Reserve\n"
              "      for i in have + 1 .. files loop\n          execute immediate 'alter tablespace \"'||n||'\" add datafile size 100M"
              " autoextend on next 256M maxsize unlimited';\n      end loop;\n    end if;\n  end;\nbegin\n"
            + (ts_calls or "  null;") + "\nend;\n/\n"
            "declare\n  procedure mkt(n varchar2, big varchar2) is c number;\n"
            "  begin\n    select count(*) into c from dba_tablespaces where tablespace_name = n;\n"
            "    if c = 0 then\n      execute immediate 'create '||case when big='YES' then 'bigfile ' end||'temporary tablespace \"'||n||'\""
            " tempfile size 100M autoextend on next 256M maxsize unlimited';\n    end if;\n  end;\nbegin\n"
            + (temp_calls or "  null;") + "\nend;\n/\n"
            + defaults +
            "select 'ZK_STEP=ts' from dual;\n"
            "create or replace directory ZK_IMPORT_DIR as '/opt/oracle/oradata/zk_import';\n"
            "begin execute immediate 'drop database link ZK_SRC'; exception when others then null; end;\n/\n"
            "select 'ZK_STEP=dir' from dual;\n"
            f"create database link ZK_SRC connect to {src['user']} identified by \"{src['password']}\" "
            f"using '//{src['host']}:{src['port']}/{src['service']}';\n"
            "select 'ZK_LINK='||dummy from dual@ZK_SRC;\n"
            # Passwort-Prüffunktionen der Profile aus SYS der Quelle kopieren (Data Pump überträgt SYS nicht)
            "set serveroutput on\n"
            "declare\n  procedure copy_fn(n varchar2) is c clob; cnt number;\n  begin\n"
            "    select count(*) into cnt from dba_objects where owner = 'SYS' and object_name = n and object_type = 'FUNCTION';\n"
            "    if cnt > 0 then return; end if;\n"
            "    for r in (select text from dba_source@ZK_SRC where owner = 'SYS' and name = n and type = 'FUNCTION' order by line) loop\n"
            "      c := c || r.text;\n    end loop;\n"
            "    if c is null then dbms_output.put_line('ZK_FNMISSING='||n); return; end if;\n"
            "    execute immediate 'create or replace '||c;\n    dbms_output.put_line('ZK_FNCOPIED='||n);\n"
            "  exception when others then dbms_output.put_line('ZK_FNFAILED='||n||' '||sqlerrm);\n  end;\nbegin\n"
            + (fn_calls or "  null;") + "\nend;\n/\nexit\n")
        job.log("Tablespaces (permanent und temporär), Verzeichnis, DB-Link und Passwort-Prüffunktionen anlegen")
        p = job.run(["docker", "exec", "-i", cname, "sqlplus", "-s", "/ as sysdba"], input=prep, secret=src["password"],
                    allow_fail=True)
        if "ZK_LINK=X" not in p.stdout:
            step = ("DB-Link zur Quelle" if "ZK_STEP=dir" in p.stdout else
                    "Verzeichnis ZK_IMPORT_DIR" if "ZK_STEP=ts" in p.stdout else "Anlegen der Tablespaces")
            ora = " | ".join(dict.fromkeys(re.findall(r"(ORA-\d{5}[^\n]*)", p.stdout)))[:300]
            raise RuntimeError(f"Vorbereitung in der neuen DB fehlgeschlagen ({step}): {ora or 'siehe Protokoll'}")
        for kind, label in (("ZK_FNCOPIED", "Prüffunktion kopiert"), ("ZK_FNMISSING", "Prüffunktion in der Quelle nicht lesbar"),
                            ("ZK_FNFAILED", "Prüffunktion ließ sich nicht anlegen")):
            for m in re.findall(kind + r"=(.*)", p.stdout):
                job.log(f"{label}: {m.strip()}")

        stamp = time.strftime("%Y%m%d-%H%M%S")
        par = ['userid="/ as sysdba"', "network_link=ZK_SRC", "directory=ZK_IMPORT_DIR",
               f"logfile=import_{stamp}.log", f"parallel={parallel}", "logtime=all", "metrics=yes",
               f"job_name=ZK_IMP_{stamp.replace('-', '_')}"]
        dp_job = f"ZK_IMP_{stamp.replace('-', '_')}"
        job.on_cancel(lambda: sh(["docker", "exec", "-i", cname, "sqlplus", "-s", "/ as sysdba"], check=False, timeout=120,
                                 input=f"declare h number; begin h := dbms_datapump.attach('{dp_job}', 'SYS'); "
                                       "dbms_datapump.stop_job(h, 1, 0); end;\n/\nexit\n"))
        if tgt["full"]:
            par += ["full=y", "exclude=tablespace"]
        else:
            par.append("schemas=" + ",".join(schemas))
        if tgt["consistent"]:
            par.append("flashback_time=systimestamp")
        script = ("cat > /tmp/zk_import.par <<'ZKPAR'\n" + "\n".join(par) + "\nZKPAR\n"
                  "impdp parfile=/tmp/zk_import.par\nrc=$?\nrm -f /tmp/zk_import.par\necho \"ZK_RC=$rc\"\n")
        job.log(f"Import über den DB-Link ({'vollständig' if tgt['full'] else ', '.join(schemas)}, "
                f"parallel {parallel}{', konsistent zum Startzeitpunkt' if tgt['consistent'] else ''}) – die Quelle läuft weiter")
        rc, oras = job.stream(["docker", "exec", "-i", cname, "bash", "-s"], input=script)
        lines = "\n".join(job.lines[-20:])
        m = re.search(r"ZK_RC=(\d+)", lines)
        irc = int(m.group(1)) if m else rc
        if irc not in (0, 5):
            raise RuntimeError(f"impdp ist mit Exit-Code {irc} abgebrochen; Protokoll in "
                               f"/opt/oracle/oradata/zk_import/import_{stamp}.log der neuen DB.")
        if irc == 5 or oras:
            job.log(f"impdp fertig mit {len(oras)} Fehlermeldungen (häufig harmlos, z. B. bereits vorhandene Objekte); "
                    f"Details im Protokoll oben und in zk_import/import_{stamp}.log.")

        owners = ",".join(sql_str(s) for s in schemas)
        post = (
            "set pagesize 0 linesize 400 feedback off heading off\n"
            "@?/rdbms/admin/utlrp.sql\n"
            "select 'ZK_INVALID='||count(*) from dba_objects where status <> 'VALID' and owner in (" + owners + ");\n"
            "select 'ZK_DIFF='||s.owner||'|'||s.object_type||'|'||s.cnt||'|'||nvl(t.cnt,0) from\n"
            " (select owner, object_type, count(*) cnt from dba_objects@ZK_SRC where owner in (" + owners + ") group by owner, object_type) s\n"
            " left join (select owner, object_type, count(*) cnt from dba_objects where owner in (" + owners + ") group by owner, object_type) t\n"
            " on t.owner = s.owner and t.object_type = s.object_type where nvl(t.cnt,0) <> s.cnt order by 1;\n"
            + ("" if tgt["keep_link"] else "drop database link ZK_SRC;\n") + "exit\n")
        job.log("Objekte neu kompilieren und mit der Quelle vergleichen")
        p = job.run(["docker", "exec", "-i", cname, "sqlplus", "-s", "/ as sysdba"], input=post, allow_fail=True, quiet=True)
        inv = (re.search(r"ZK_INVALID=(\d+)", p.stdout) or [None, "?"])[1]
        diffs = re.findall(r"ZK_DIFF=(\S+)", p.stdout)
        job.log(f"Ungültige Objekte nach dem Kompilieren: {inv}")
        if diffs:
            job.log(f"Abweichende Objektzahlen ({len(diffs)}), Format Schema|Typ|Quelle|neu:")
            for d in diffs[:40]:
                job.log("  " + d)
        else:
            job.log("Objektzahlen je Schema und Typ stimmen mit der Quelle überein.")
    except Exception:
        if not tgt["keep_link"]:
            sh(["docker", "exec", "-i", cname, "sqlplus", "-s", "/ as sysdba"],
               input="drop database link ZK_SRC;\nexit\n", check=False)
        job.log(f"Die neue Datenbank {sid} bleibt zur Analyse bestehen; löschen über „Alle Snapshots“.")
        raise
    db_meta_update(sid, dp_pending=False, origin=f"per Data Pump von {src['host']}/{src['service']}")
    tl_add(sid, "db-imported", f"Datenbank {sid} per Data Pump von {src['host']}/{src['service']} übernommen"
           f" ({'vollständig' if tgt['full'] else ', '.join(schemas)})")


@bp.post("/api/import/dp/probe")
def api_dp_probe():
    data = request.get_json(force=True)
    return jsonify(probe_dp(validate_dp_source(data), (data.get("target_sid") or "").strip().upper()))


@bp.post("/api/import/dp")
def api_dp_import():
    data = request.get_json(force=True)
    src = validate_dp_source(data)
    tgt = dict(sid=(data.get("target_sid") or "").strip().upper(), port=int(data.get("target_port") or 0),
               password=data.get("target_password") or "", memory_mb=int(data.get("memory_mb") or 2048),
               full=bool(data.get("full")), schemas=[s.strip().upper() for s in data.get("schemas") or [] if s.strip()],
               parallel=int(data.get("parallel") or 2), consistent=bool(data.get("consistent", True)),
               keep_link=bool(data.get("keep_link")))
    if not RE_NEWSID.match(tgt["sid"]):
        raise UserError("Neue SID: Großbuchstaben und Ziffern, beginnt mit einem Buchstaben, höchstens 8 Zeichen.")
    resume = (MASTER / tgt["sid"]).exists()
    if resume and not db_meta(tgt["sid"]).get("dp_pending"):
        raise UserError(f"Eine Datenbank {tgt['sid']} gibt es auf diesem Host schon.")
    if not resume and not 1024 <= tgt["port"] <= 65535:
        raise UserError("Port: 1024 bis 65535.")
    if not resume and not RE_DBPWD.match(tgt["password"]):
        raise UserError("Passwort der neuen DB: 8–30 Zeichen mit Groß- und Kleinbuchstaben und Ziffer; erlaubt sind A–Z, a–z, 0–9, _ # $.")
    if not 1024 <= tgt["memory_mb"] <= 262144:
        raise UserError("Speicher: 1024 bis 262144 MB.")
    if not 1 <= tgt["parallel"] <= 32:
        raise UserError("Parallelität: 1 bis 32.")
    if not tgt["full"] and not tgt["schemas"]:
        raise UserError("Mindestens ein Schema wählen oder „ganze Datenbank“.")
    if any(not RE_SCHEMA.match(s) for s in tgt["schemas"]):
        raise UserError("Ungültiger Schema-Name.")
    job = start_job(f"Datenbank {tgt['sid']} per Data Pump von {src['host']} übernehmen", tgt["sid"],
                    f"newdb:{tgt['sid']}", job_dp_import, src, tgt)
    return jsonify(job=job.as_dict())


# --------------------------------------------------------------------------- Start
def start_agent() -> None:
    """Verzeichnisse anlegen und den Zeitplaner starten (einmal pro Prozess)."""
    if not AGENT_TOKEN:
        raise SystemExit("ZK_AGENT_TOKEN ist nicht gesetzt – Agent kann nicht starten.")
    if os.geteuid() != 0:
        log.warning("Agent läuft nicht als root; btrfs- und docker-Befehle werden scheitern.")
    for d in (MASTER, GOLDEN, DEV, DEVSNAPS, BUILD, DEVDUMPS, IMPORT, META):
        d.mkdir(parents=True, exist_ok=True)
    jobs_housekeeping()
    threading.Thread(target=scheduler_loop, daemon=True, name="zeitplaner").start()
    threading.Thread(target=usage_worker, daemon=True, name="platzermittlung").start()
