#!/usr/bin/env python3
"""
zeitkapsel – Oracle-Datenbanken in Docker auf btrfs: Releases, Entwickler-DBs, Snapshots.

Rollen (ZK_ROLE)
  all         Controller und Agent in einem Prozess (erster bzw. einziger Host)
  controller  nur Oberfläche, Benutzer und Hosts
  agent       nur Agent auf einem weiteren DB-Host

Aufruf
  app.py serve                         Dienst starten (Standard)
  app.py create-user <name> [--admin]  Benutzer anlegen (Passwort wird abgefragt)
  app.py set-password <name>           Passwort setzen
  app.py gen-token                     zufälliges Agent-Token ausgeben
"""
from __future__ import annotations

import getpass
import logging
import secrets
import sys

from flask import Flask, jsonify

import agent
from agent import env

ROLE = env("ROLE", "all")
LISTEN = env("LISTEN", env("UI_LISTEN", "0.0.0.0"))
PORT = int(env("PORT", env("UI_PORT", "8088")))


def create_app() -> Flask:
    app = Flask(__name__, static_folder=None)
    if ROLE not in ("all", "controller", "agent"):
        raise SystemExit(f"ZK_ROLE={ROLE!r} ist ungültig (all, controller, agent).")
    if ROLE in ("all", "agent"):
        agent.start_agent()
        app.register_blueprint(agent.bp)
    if ROLE in ("all", "controller"):
        import controller
        controller.init_app(app, PORT, agent.AGENT_TOKEN if ROLE == "all" else None)

    @app.errorhandler(Exception)
    def on_error(e):
        code = getattr(e, "code", 500)
        if isinstance(code, int) and code < 500:
            return jsonify(error=getattr(e, "description", str(e))), code
        app.logger.exception("Fehler")
        return jsonify(error=str(e)), 500

    return app


def ask_password() -> str:
    pw = getpass.getpass("Passwort: ")
    if pw != getpass.getpass("Passwort wiederholen: "):
        raise SystemExit("Die Passwörter stimmen nicht überein.")
    return pw


def main(argv: list[str]) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cmd = argv[1] if len(argv) > 1 else "serve"
    if cmd == "gen-token":
        print(secrets.token_urlsafe(32))
        return
    if cmd in ("create-user", "set-password"):
        import controller
        controller.init_db()
        if len(argv) < 3:
            raise SystemExit(f"Aufruf: app.py {cmd} <name>")
        name = argv[2].lower()
        try:
            if cmd == "create-user":
                controller.create_user(name, ask_password(), "admin" if "--admin" in argv else "user")
                print(f"Benutzer {name} angelegt.")
            else:
                if not controller.get_user(name):
                    raise SystemExit("Benutzer nicht gefunden.")
                pw = ask_password()
                controller.check_password_strength(pw)
                with controller.db() as conn:
                    conn.execute("update users set pw_hash=? where username=?",
                                 (controller.generate_password_hash(pw), name))
                print("Passwort gesetzt.")
        except controller.BadRequest as e:
            raise SystemExit(str(e)) from None
        return
    if cmd != "serve":
        raise SystemExit(__doc__)
    app = create_app()
    try:
        from waitress import serve
    except ImportError:
        print("waitress fehlt, starte Flask-Entwicklungsserver.", file=sys.stderr)
        app.run(host=LISTEN, port=PORT, threaded=True)
    else:
        print(f"zeitkapsel ({ROLE}) auf http://{LISTEN}:{PORT}", file=sys.stderr)
        serve(app, host=LISTEN, port=PORT, threads=16)


if __name__ == "__main__":
    main(sys.argv)
