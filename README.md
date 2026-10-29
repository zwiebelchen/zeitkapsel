# zeitkapsel

Datenbanken, deren Daten auf einem BTRFS-Snapshot liegen und wo die DB als Docker-Container läuft, in Sekunden snapshotten, branchen, Entwicklungsinstanzen hochziehen und mehr.

Die Weboberfläche (`oradb-ui`) ist eine kleine Flask-Anwendung für die Oracle-Container auf db05-alma10. Sie nutzt dieselbe Ablage
und dieselben Konventionen wie `startdb.sh`, `golden.sh` und `devdb.sh` aus dem Runbook;
Oberfläche und Skripte lassen sich parallel verwenden.

| Bereich | Was die Oberfläche tut |
| --- | --- |
| Datenbanken | listet jede Master-DB unter `/srv/oradb/master/<SID>` mit Status, Port und JDBC-URL |
| Release erstellen | wie `golden.sh`, aber mit frei wählbarem Namen und Beschreibung → `/srv/oradb/golden/<SID>/<name>` |
| Releases | Beschreibung bearbeiten, löschen, als Entwickler-DB starten, neues Release davon ableiten (wie ein Git-Branch) |
| Entwickler-DBs | aus einem Release oder einem Entwickler-Snapshot anlegen, starten, stoppen, auf den Ausgangsstand zurücksetzen, löschen |
| Entwickler-Snapshots | Stand einer laufenden Entwickler-DB festhalten (im Betrieb oder kurz heruntergefahren); darauf zurücksetzen, neue Entwickler-DB daraus anlegen, als Release übernehmen, löschen |
| Timeline | Baum je Datenbank: Releases der Master-DB auf dem Stamm, daran als Äste abgeleitete Releases, Entwickler-DBs und deren Snapshots; dazu eigene Notizen und auf Wunsch alle Ereignisse |
| Verwaltung | Zeitpläne (Release erstellen, Snapshot einer Entwickler-DB, Entwickler-DBs stoppen oder zurücksetzen) mit Wochentagen, Uhrzeit und Aufbewahrung; Systemangaben; Aufräumen abgebrochener Release-Vorgänge |
| Alle Snapshots | alle Subvolumes mit Art, Erstellzeit, Schreibschutz, Container und exklusiver Belegung |

Lange Vorgänge (Release erstellen, Entwickler-DB anlegen, starten, zurücksetzen, Snapshot) laufen im
Hintergrund; ihr Protokoll steht unten rechts unter „Vorgänge“.

## Installation (als root auf db05-alma10)

```bash
mkdir -p /opt/oradb-ui
cp app.py index.html requirements.txt /opt/oradb-ui/
python3 -m venv /opt/oradb-ui/venv
/opt/oradb-ui/venv/bin/pip install -r /opt/oradb-ui/requirements.txt

install -m 600 oradb-ui.env.example /etc/oradb-ui.env
vi /etc/oradb-ui.env                      # Passwort setzen, ggf. ORADB_DB_HOST

cp oradb-ui.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now oradb-ui
journalctl -u oradb-ui -f

firewall-cmd --permanent --add-port=8088/tcp && firewall-cmd --reload
```

Danach `http://db05-alma10:8088` öffnen und mit Benutzer und Passwort aus `/etc/oradb-ui.env` anmelden.

## Sicherheit

Die Anwendung läuft als root und steuert Docker und btrfs. Deshalb:

- nur im internen Netz erreichbar machen, nie ins Internet;
- ein eigenes, starkes Passwort in `/etc/oradb-ui.env` (Rechte 600);
- die Anmeldung läuft per HTTP Basic Auth und überträgt das Passwort ohne TLS lesbar. Für mehr als
  ein vertrauenswürdiges Admin-Netz gehört ein Reverse Proxy mit TLS davor;
- ändernde Aufrufe verlangen einen eigenen HTTP-Header, damit fremde Webseiten keine Aktionen
  im Browser eines angemeldeten Benutzers auslösen können;
- alle Namen werden gegen enge Muster geprüft; Befehle laufen ohne Shell.

## Ablage und Metadaten

```
/srv/oradb/master/<SID>             Master-DBs (werden weiter mit startdb.sh gestartet)
/srv/oradb/golden/<SID>/<release>   Releases (schreibgeschützt)
/srv/oradb/dev/<SID>-<name>         Entwickler-DBs
/srv/oradb/devdumps/<SID>-<name>    Data-Pump-Verzeichnisse der Entwickler-DBs
/srv/oradb/devsnaps/<SID>-<name>/<snapshot>   Snapshots von Entwickler-DBs (schreibgeschützt)
/srv/oradb/meta/<SID>/              Release- und Snapshot-Beschreibungen (releases/, devsnaps/) und Timeline (timeline.jsonl)
```

Entwickler-DB-Container tragen die Labels `devdb`, `devport`, `devsid`, `devrelease`,
`devbase`, `devowner` und `devcreated`. `devbase` ist der Ausgangsstand (`release:<name>` oder
`snap:<SID>-<dev>/<snapshot>`), `devrelease` das Release, auf das der Stand zurückgeht. Mit
`devdb.sh` angelegte DBs erscheinen ebenfalls, nur ohne bekannten Ausgangsstand; „Auf
Ausgangsstand zurücksetzen“ ist für sie gesperrt.

## Der Baum in der Timeline

Der Stamm zeigt die Releases, die aus der Master-DB entstanden sind, neueste oben. An jedem Release
hängen als Äste:

- abgeleitete Releases („Release ableiten“) – ein schreibgeschützter Snapshot des Releases, sofort
  fertig, weil das Quell-Release schon sauber geschlossen und für Entwickler eingerichtet ist;
- Entwickler-DBs, die aus dem Release angelegt wurden, mit ihren Snapshots;
- an jedem Snapshot wiederum die Entwickler-DBs und Releases, die daraus entstanden sind.

Releases, Snapshots und Entwickler-DBs lassen sich direkt im Baum starten („Als Entwickler-DB
starten“ legt eine neue Entwickler-DB an und schlägt einen Namen vor), ableiten oder als Release
übernehmen. Ist der Ursprung eines Astes gelöscht, hängt er am Release, auf das er zurückgeht, und
trägt den Vermerk „Herkunft gelöscht“ bzw. „Ausgangsstand gelöscht“.

Die Herkunft eines Releases steht in `meta/<SID>/releases/<name>.json` (`parent`, `lineage`).
Releases, die vor dieser Version aus einem Snapshot entstanden sind, kennen ihren Ursprung nicht
und stehen deshalb auf dem Stamm.

Ereignisse wie „Entwickler-DB angelegt“ sind standardmäßig ausgeblendet, weil der Baum diese
Information bereits zeigt; „Ereignisse anzeigen“ blendet sie ein (z. B. um Gelöschtes zu sehen).

## Zeitpläne

Unter „Verwaltung“ lassen sich wiederkehrende Aufgaben anlegen. Die Anwendung prüft alle 20
Sekunden, ob ein Zeitplan fällig ist, und startet ihn als normalen Vorgang (Protokoll unter
„Vorgänge“, Ergebnis am Zeitplan). Zeiten gelten in der Zeitzone des Servers.

| Häufigkeit | Einstellungen | Beispiel |
| --- | --- | --- |
| täglich zu einer Uhrzeit | Uhrzeit, Wochentage | 02:00 Uhr, täglich |
| stündlich bzw. alle n Stunden | alle n Stunden (1–23, ab Mitternacht gezählt), zur Minute, Wochentage, optional Zeitfenster | stündlich zur Minute 05; alle 2 Stunden zur Minute 30 zwischen 22:00 und 06:00 |
| alle n Minuten | n (1–720, ab Mitternacht gezählt), Wochentage, optional Zeitfenster | alle 15 Minuten, 08:00–18:00, Mo–Fr |

Ein Zeitfenster darf über Mitternacht gehen (22:00 bis 06:00). War der Dienst zu einem Termin nicht
aktiv, wird nur der jüngste verpasste Termin desselben Tages nachgeholt, nicht jeder einzelne.
Fällt ein Termin in einen noch laufenden Vorgang desselben Ziels (z. B. ein Release dauert länger
als das Intervall), wird er übersprungen und am Zeitplan als „übersprungen“ angezeigt.

Bei häufigen Läufen gehört `{zeit}` ins Namensmuster (Vorschlag der Oberfläche), sonst hängt die
Anwendung Zähler an (`-2`, `-3` …). „Aufbewahren“ ist bei kurzen Intervallen praktisch Pflicht,
damit sich Snapshots und Releases nicht unbegrenzt ansammeln.

| Aktion | Was passiert |
| --- | --- |
| Release aus Master-DB erstellen | wie „Release erstellen“; Name aus dem Muster, z. B. `nightly-{datum}` → `nightly-2026-11-02` |
| Snapshot einer Entwickler-DB erstellen | im laufenden Betrieb oder kurz heruntergefahren; Name aus dem Muster |
| Entwickler-DBs stoppen | alle laufenden Entwickler-DBs der Datenbank, z. B. abends, um Arbeitsspeicher freizugeben |
| Entwickler-DBs zurücksetzen | die ausgewählten Entwickler-DBs nacheinander auf ihren Ausgangsstand oder auf das neueste Release; gestoppte DBs werden auf Wunsch danach wieder gestoppt |

Beim Zurücksetzen auf das neueste Release wird dieses Release der neue Ausgangsstand der DB.
Zusammen mit einem nächtlichen Release ergibt das zum Beispiel: 02:00 Release `nightly-{datum}`,
06:00 ausgewählte Entwickler-DBs auf das neueste Release zurücksetzen – jeden Morgen frische Daten.
Eine DB, für die gerade ein anderer Vorgang läuft, wird übersprungen; der Lauf gilt dann als
fehlgeschlagen und nennt sie.

„Aufbewahren“ löscht nach jedem Lauf die älteren Releases bzw. Snapshots *dieses Zeitplans* und
behält die neuesten n. Stände, an denen noch Entwickler-DBs oder abgeleitete Releases hängen, bleiben
stehen. Von Hand erstellte Stände rührt die Aufbewahrung nie an.

Zeitpläne liegen in `/srv/oradb/meta/schedules.json`. Der Zeitplaner läuft im Dienst selbst; es
braucht keinen Cronjob.

## Snapshots von Entwickler-DBs

Ein Snapshot im laufenden Betrieb ist sofort fertig und absturzkonsistent: Alle Dateien der DB
liegen in einem Subvolume, das btrfs atomar einfriert; Oracle stellt den Stand beim nächsten Start
per Instance Recovery selbst wieder her. Mit „DB dafür kurz herunterfahren“ entsteht ein sauber
geschlossener Stand, dafür ist die DB ein bis zwei Minuten nicht erreichbar.

Beim Löschen einer Entwickler-DB werden ihre Snapshots mitgelöscht. Ein Stand, der bleiben soll,
wird vorher „Als Release übernehmen“; das durchläuft dieselben Schritte wie ein Release aus der
Master-DB (einmal starten, Entwickler-Einstellungen setzen, sauber herunterfahren, einfrieren).
Entwickler-DBs, die aus einem gelöschten Snapshot entstanden sind, behalten ihre Daten, lassen sich
aber nicht mehr auf ihren Ausgangsstand zurücksetzen – wohl aber auf eigene Snapshots. Releases aus `golden.sh`
erscheinen in der Timeline mit dem Vermerk „extern“.

## Konfiguration

Alle Einstellungen stehen kommentiert in `oradb-ui.env.example`. Wichtig für das Release-Erstellen:
Standardmäßig setzt die Oberfläche in der Zwischen-DB `sga_target` und `sga_max_size` auf 1 GB und
`pga_aggregate_target` auf 300 MB. Arbeitet die Master-DB mit AMM (`memory_target` > 0) oder
sollen weitere Schritte laufen (Passwörter, Anonymisierung), gehört eigenes SQL in eine Datei, auf
die `ORADB_RELEASE_SQL_FILE` zeigt. `shutdown immediate` hängt die Oberfläche selbst an.

## Grenzen

- Die Master-DBs startet und stoppt die Oberfläche bewusst nicht; dafür bleibt `startdb.sh`.
- Vorgangsprotokolle liegen nur im Arbeitsspeicher (die letzten 50) und sind nach einem Neustart
  des Dienstes weg. Die Timeline bleibt erhalten.
- Ein Neustart des Dienstes während eines laufenden Vorgangs bricht ihn ab. Ein abgebrochenes
  Release hinterlässt eventuell einen Zwischenstand unter `/srv/oradb/build` und einen Container
  `release-build-…`; beides ist unter „Alle Snapshots“ bzw. mit `docker ps -a` sichtbar und lässt
  sich von Hand entfernen.
