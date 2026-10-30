"""zeitkapsel – Tags eines Image-Repositorys aus einer Docker-Registry lesen (Registry HTTP API v2).

Funktioniert mit Docker Hub (Bearer-Token), mit eigenen Registries mit Basic- oder Token-Anmeldung
und mit unverschlüsselten bzw. selbstsignierten Registries („insecure“).
"""
from __future__ import annotations

import base64
import json
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request

HUB = "docker.io"
HUB_API = "registry-1.docker.io"
RE_REF = re.compile(r"^[a-z0-9][a-z0-9.-]*(:\d+)?(/[a-z0-9][a-z0-9._-]*)+$|^[a-z0-9][a-z0-9._-]*$")
RE_TAG = re.compile(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?(?:-([a-z][a-z0-9.]*))?$")
MAX_PAGES = 60


class RegistryError(Exception):
    pass


def parse_repo(ref: str) -> tuple[str, str]:
    """„docker.io/library/postgres“, „postgres“, „registry.intern:5000/db/postgres“ → (Host, Repository-Pfad)."""
    ref = (ref or "").strip().lower()
    ref = re.sub(r"^https?://", "", ref).rstrip("/")
    if not RE_REF.match(ref):
        raise RegistryError("Repository bitte ohne Tag angeben, z. B. docker.io/library/postgres "
                            "oder registry.intern:5000/postgres.")
    parts = ref.split("/")
    if len(parts) == 1 or not ("." in parts[0] or ":" in parts[0] or parts[0] == "localhost"):
        host, path = HUB, ref
    else:
        host, path = parts[0], "/".join(parts[1:])
    if host in (HUB, "index.docker.io", HUB_API):
        host = HUB
        if "/" not in path:
            path = "library/" + path
    return host, path


def image_base(host: str, path: str) -> str:
    """Name, unter dem Docker das Image lokal führt (ohne Tag)."""
    if host == HUB:
        return path.removeprefix("library/")
    return f"{host}/{path}"


def tag_info(tag: str) -> dict | None:
    m = RE_TAG.match(tag)
    if not m:
        return None
    major, minor, patch, variant = m.groups()
    return dict(tag=tag, major=int(major), minor=int(minor) if minor else None,
                patch=int(patch) if patch else None, variant=variant or "")


def sort_key(t: dict) -> tuple:
    # neueste zuerst; ohne Nebenversion („17“) vor „17.6“, ohne Variante vor Varianten
    return (-t["major"], -(t["minor"] if t["minor"] is not None else 10 ** 6),
            -(t["patch"] if t["patch"] is not None else 10 ** 6), t["variant"] != "", t["variant"])


class Registry:
    def __init__(self, ref: str, user: str = "", password: str = "", insecure: bool = False):
        self.host, self.path = parse_repo(ref)
        self.api = HUB_API if self.host == HUB else self.host
        self.user, self.password, self.insecure = user or "", password or "", bool(insecure)
        self.token = ""
        self.scheme = "https"

    def _ctx(self):
        if not self.insecure:
            return None
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _basic(self) -> str:
        return "Basic " + base64.b64encode(f"{self.user}:{self.password}".encode()).decode()

    def _request(self, url: str, auth: str = "") -> tuple[int, dict, bytes]:
        req = urllib.request.Request(url, headers={"Accept": "application/json", **({"Authorization": auth} if auth else {})})
        try:
            with urllib.request.urlopen(req, timeout=20, context=self._ctx() if url.startswith("https") else None) as r:
                return r.status, dict(r.headers), r.read()
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers), e.read()

    def _auth_header(self) -> str:
        if self.token:
            return "Bearer " + self.token
        return self._basic() if self.user else ""

    def _login(self, challenge: str) -> None:
        scheme, _, params = challenge.partition(" ")
        if scheme.lower() == "basic":
            if not self.user:
                raise RegistryError("Die Registry verlangt eine Anmeldung; Benutzer und Passwort eintragen.")
            self.token = ""
            return
        if scheme.lower() != "bearer":
            raise RegistryError(f"Unbekanntes Anmeldeverfahren der Registry: {scheme}")
        p = dict(re.findall(r'(\w+)="([^"]*)"', params))
        if "realm" not in p:
            raise RegistryError("Die Registry nennt keinen Token-Dienst.")
        q = {k: p[k] for k in ("service", "scope") if k in p}
        q.setdefault("scope", f"repository:{self.path}:pull")
        url = p["realm"] + ("&" if "?" in p["realm"] else "?") + urllib.parse.urlencode(q)
        status, _, body = self._request(url, self._basic() if self.user else "")
        if status != 200:
            raise RegistryError(f"Anmeldung an der Registry fehlgeschlagen (HTTP {status}).")
        data = json.loads(body or b"{}")
        self.token = data.get("token") or data.get("access_token") or ""
        if not self.token:
            raise RegistryError("Die Registry hat kein Token geliefert.")

    def get(self, rel: str) -> tuple[dict, bytes]:
        for attempt in range(3):
            url = f"{self.scheme}://{self.api}{rel}"
            try:
                status, headers, body = self._request(url, self._auth_header())
            except (urllib.error.URLError, OSError) as e:
                reason = getattr(e, "reason", e)
                if self.insecure and self.scheme == "https":   # Registry ohne TLS
                    self.scheme = "http"
                    continue
                raise RegistryError(f"Registry {self.api} nicht erreichbar: {reason}") from None
            if status == 401 and attempt < 2:
                hdr = {k.lower(): v for k, v in headers.items()}.get("www-authenticate", "")
                self._login(hdr)
                continue
            if status == 404:
                raise RegistryError(f"Repository {self.path} gibt es in der Registry {self.api} nicht.")
            if status in (401, 403):
                raise RegistryError("Keine Berechtigung für das Repository; Benutzer und Passwort prüfen.")
            if status != 200:
                raise RegistryError(f"Registry antwortet mit HTTP {status}.")
            return headers, body
        raise RegistryError("Anmeldung an der Registry fehlgeschlagen.")

    def tags(self) -> list[str]:
        out: list[str] = []
        rel = f"/v2/{self.path}/tags/list?n=1000"
        for _ in range(MAX_PAGES):
            headers, body = self.get(rel)
            out += json.loads(body or b"{}").get("tags") or []
            link = {k.lower(): v for k, v in headers.items()}.get("link", "")
            m = re.search(r"<([^>]+)>\s*;\s*rel=\"?next\"?", link)
            if not m:
                break
            nxt = m.group(1)
            rel = urllib.parse.urlsplit(nxt)._replace(scheme="", netloc="").geturl() if nxt.startswith("http") else nxt
        return sorted(set(out))


def version_tags(tags: list[str]) -> list[dict]:
    """Nur Versions-Tags (17, 17.6, 17.6-alpine3.22, 16-bookworm …), ohne beta/rc/latest; neueste zuerst."""
    infos = [t for t in (tag_info(x) for x in tags) if t]
    return sorted(infos, key=sort_key)
