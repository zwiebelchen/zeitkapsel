# zeitkapsel

Datenbanken, deren Daten auf einem BTRFS-Snapshot liegen und wo die DB als Docker-Container läuft, in Sekunden snapshotten, branchen, Entwicklungsinstanzen hochziehen und mehr.

zeitkapsel verwaltet Oracle-Datenbanken, die als Container (Oracle-Image aus `oracle/docker-images`) auf einem oder mehreren Hosts mit btrfs laufen: Releases als schreibgeschützte Snapshots, Entwickler-DBs als beschreibbare Kopien davon, Snapshots dieser Entwickler-DBs, Zeitpläne – über eine Weboberfläche mit Benutzern und Rechten oder über Shell-Skripte.

| Bereich | Was zeitkapsel tut |
| --- | --- |
| Hosts | mehrere DB-Hosts in einer Oberfläche; auf jedem läuft ein Agent |
| Datenbanken | Master-DBs je Host mit Status, Port und JDBC-URL; starten und stoppen |
| Neue Datenbank | leere Container-DB (CDB mit PDB) mit eigenem Container anlegen |
| Import | bestehende DB von einem anderen Server per SSH übernehmen, inklusive spfile und Passwortdatei |
| Releases | aus der Master-DB, aus einem Entwickler-Snapshot oder von einem Release abgeleitet (wie ein Git-Branch) |
| Entwickler-DBs | aus einem Release oder Snapshot anlegen, starten, stoppen, zurücksetzen, snapshotten, löschen |
| Timeline | Baum je Datenbank mit Releases, Entwickler-DBs, Snapshots und Notizen |
| Zeitpläne | täglich, stündlich, minütlich: Releases, Snapshots, Entwickler-DBs stoppen oder zurücksetzen |
| Benutzer | Anmeldung, Rollen Administrator und Benutzer, Protokoll aller Änderungen |
| Skripte | `startdb.sh`, `golden.sh`, `devdb.sh` mit derselben Ablage, parallel zur Oberfläche nutzbar |

## Aufbau

```
Browser ──► Controller ──► Agent auf db05-alma10  (btrfs, docker)
            (Anmeldung,  ├► Agent auf db06-alma10
             Rechte,     └► …
             SQLite)
```

- **Agent** (`agent.py`): läuft auf jedem DB-Host als root, steuert btrfs-Subvolumes und Container, führt Zeitpläne aus. Er nimmt nur Aufrufe mit seinem Token an (`ZK_AGENT_TOKEN`).
- **Controller** (`controller.py`): Oberfläche, Anmeldung, Benutzer und Hosts in SQLite, Rechteprüfung, Weiterleitung an die Agenten, Protokoll.
- Beide stecken in derselben Anwendung; `ZK_ROLE` bestimmt, was ein Prozess tut: `all` auf dem ersten Host (Controller und Agent), `agent` auf weiteren Hosts. Im Betrieb `all` trägt sich der eigene Agent beim ersten Start als Host ein.

## Rollen und Rechte

| Aktion | Benutzer | Administrator |
| --- | --- | --- |
| Hosts, Datenbanken, Releases, Timeline, Snapshots ansehen | ja | ja |
| Entwickler-DB anlegen (aus jedem Release oder Snapshot) | ja, für sich | ja, für jeden |
| eigene Entwickler-DB starten, stoppen, zurücksetzen, snapshotten, löschen | ja | ja (auch fremde) |
| Snapshots der eigenen Entwickler-DB zurückspielen, beschreiben, löschen | ja | ja |
| Releases erstellen, ableiten, beschreiben, löschen; Snapshot als Release übernehmen | – | ja |
| Notizen, Zeitpläne, Aufräumen | – | ja |
| Master-DB starten und stoppen, neue Datenbank, Import | – | ja |
| Benutzer, Hosts, Protokoll, Besitzer einer Entwickler-DB ändern | – | ja |

Die Rechte prüft der Controller. Wem eine Entwickler-DB gehört, steht in `meta/<SID>/devdbs/<id>.json` und als Label `devowner` am Container.

## Installation

### Erster Host (Controller und Agent)

Voraussetzungen: AlmaLinux 10.1 oder neuer mit btrfs unter `/srv`, Docker, Python 3.11+, `rsync` und `ssh` für den Import, das Oracle-Image (siehe „Image“).

```bash
git clone https://github.com/zwiebelchen/zeitkapsel.git /opt/zeitkapsel
python3 -m venv /opt/zeitkapsel/venv
/opt/zeitkapsel/venv/bin/pip install -r /opt/zeitkapsel/requirements.txt

install -m 600 /opt/zeitkapsel/systemd/zeitkapsel.env.example /etc/zeitkapsel.env
/opt/zeitkapsel/venv/bin/python /opt/zeitkapsel/app.py gen-token     # → ZK_AGENT_TOKEN
vi /etc/zeitkapsel.env

# erstes Administrator-Konto
set -a; . /etc/zeitkapsel.env; set +a
/opt/zeitkapsel/venv/bin/python /opt/zeitkapsel/app.py create-user chef --admin

cp /opt/zeitkapsel/systemd/zeitkapsel.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now zeitkapsel
firewall-cmd --permanent --add-port=8088/tcp && firewall-cmd --reload
```

Danach `http://<host>:8088` öffnen und anmelden. Weitere Benutzer legst du unter „Benutzer, Hosts, Protokoll“ an.

### Weitere DB-Hosts (nur Agent)

Wie oben, aber in `/etc/zeitkapsel.env` `ZK_ROLE=agent` und ein eigenes Token; kein Benutzer nötig. Dann in der Oberfläche unter „Benutzer, Hosts, Protokoll“ → „Host hinzufügen“ Name, Adresse (`http://db06-alma10:8088`) und Token eintragen; die Verbindung wird beim Speichern geprüft. Den Port des Agenten nur für den Controller-Host öffnen.

### Umstieg von oradb-ui

1. `systemctl disable --now oradb-ui`, zeitkapsel wie oben installieren. Die Ablage unter `/srv/oradb` bleibt unverändert; Releases, Timeline und Zeitpläne werden übernommen. Variablen mit `ORADB_`-Präfix werden weiter gelesen, neue heißen `ZK_…`.
2. Bestehende Entwickler-DBs tragen im Feld „Besitzer“ den früheren Freitext („Für wen“). Bis ein Administrator über „Besitzer ändern“ einen Benutzer zuweist, kann sie nur ein Administrator bedienen.

## Image

Das Image wird nach dem Runbook gebaut (19.3 mit RU 19.30 über die Patching-Extension). Danach immer das Korrektur-Image bauen:

```bash
docker build -t oracle/database:19.30.0-ee -f docker/Dockerfile.fix .
```

Es linkt die leer gebliebenen Binaries (`sqlplus`, `adrci` …) neu und repariert die `/u01`-Pfade in `dbca` und `platform_common`. Ohne Letzteres kann das Image keine neuen Datenbanken anlegen; die Oberfläche prüft das vor jeder Neuanlage.

## Ablage

```
/srv/oradb/master/<SID>                       Master-DB (Subvolume, im Container /opt/oracle/oradata)
/srv/oradb/golden/<SID>/<release>             Releases (schreibgeschützt)
/srv/oradb/dev/<SID>-<name>                   Entwickler-DBs
/srv/oradb/devsnaps/<SID>-<name>/<snapshot>   Snapshots von Entwickler-DBs (schreibgeschützt)
/srv/oradb/devdumps/<SID>-<name>              Data-Pump-Verzeichnisse der Entwickler-DBs
/srv/oradb/meta/<SID>/                        db.json (Port), releases/, devdbs/, devsnaps/, timeline.jsonl
/srv/oradb/meta/schedules.json                Zeitpläne des Hosts
```

Jedes DB-Subvolume enthält die Markierungsdateien, ohne die das Startskript des Oracle-Images eine vorhandene Datenbank löschen würde (`.<SID>.created`, `dbconfig/<SID>/.docker_<edition>`), sowie optional `zeitkapsel.env`:

| Schlüssel | Bedeutung |
| --- | --- |
| `ZK_DB_TYPE` | `noncdb` oder `cdb` |
| `ZK_PDB` | Name der PDB (bei `cdb`) |
| `ZK_SERVICE` | Service-Name für die JDBC-URL (Standard: PDB bzw. SID) |
| `ZK_SHM_SIZE` | `--shm-size` für AMM (`memory_target`), z. B. `5g` |
| `ZK_EDITION` | `enterprise` oder `standard` |
| `ZK_MOUNTS` | zusätzliche Mounts `<Pfad im Subvolume>:<Pfad im Container>`, Leerzeichen-getrennt |

Weil die Datei im Subvolume liegt, wandert sie mit jedem Release und jeder Entwickler-DB mit; Oberfläche und Skripte setzen die Angaben bei jedem `docker run` um.

## Port ändern

Docker kann die Portzuordnung eines bestehenden Containers nicht ändern. „Port ändern“ (beim Kopf einer Datenbank, bei jeder Entwickler-DB und auf der Seite „Vorgänge und Container“) fährt den Container deshalb sauber herunter, entfernt ihn und erzeugt ihn mit dem neuen Port neu; die Daten im Subvolume bleiben unverändert. Das geht auch für eine Master-DB ohne Container, etwa wenn ihr gespeicherter Port inzwischen belegt ist. Der Dialog schlägt einen freien Port vor; Entwickler-DBs bleiben im Bereich `ZK_DEV_PORT_START` bis `ZK_DEV_PORT_END`. Master-Ports ändern Administratoren, Entwickler-DB-Ports auch ihre Besitzer.

## Auslastung und Logs

Der Agent misst alle 30 Sekunden (`ZK_METRICS_INTERVAL`) und hält 24 Stunden vor (`ZK_METRICS_HOURS`; gesichert in `meta/metrics.json`, damit ein Neustart die Kurven nicht löscht):

- je Container von zeitkapsel CPU, Arbeitsspeicher sowie gelesene und geschriebene Bytes pro Sekunde (aus `docker stats`; CPU 100 % entspricht einem Kern);
- den gesamten Host aus `/proc`: CPU-Auslastung und I/O-Wartezeit, Load, RAM gesamt/belegt/verfügbar, Swap, Lese- und Schreibrate der physischen Platten.

Die Seite „Auslastung des Hosts“ zeigt die aktuellen Werte als Kacheln, die Verläufe über 1, 6 oder 24 Stunden und alle laufenden Datenbanken nach Speicherbedarf. Die Kachel „Platz für weitere Entwickler-DBs“ schätzt aus dem verfügbaren Arbeitsspeicher abzüglich einer Reserve für das System (10 % des RAM, mindestens 2 GB), wie viele Entwickler-DBs à `ZK_DEV_DB_MB` (Standard 2048 MB) noch passen; lagert der Host schon aus, steht dort eine Warnung. Jede Datenbank-Ansicht zeigt außerdem die Verläufe ihrer Master-DB und Entwickler-DBs.

„Logs“ (in der Datenbank-Ansicht und auf „Vorgänge und Container“) zeigt `docker logs` eines Containers mit Zeitstempeln, wählbarer Zeilenzahl und automatischer Aktualisierung. Benutzer sehen die Logs ihrer eigenen Entwickler-DBs, Administratoren alle.

## Vorgänge und Container

Lange Aktionen laufen als Vorgänge auf dem Agenten des Hosts. Das Fenster „Vorgänge“ unten rechts zeigt die letzten; es lässt sich jederzeit ausblenden, auch während etwas läuft – dann bleibt ein kleiner Knopf mit der Zahl der laufenden Vorgänge, der es wieder öffnet.

Die Seite „Vorgänge und Container“ je Host zeigt:

- alle Container von zeitkapsel (Master- und Entwickler-DBs, Hilfscontainer für Release-Erstellung, Neuanlage und Prüfstart) mit Status, Laufzeit, Port und Besitzer. „Stoppen“ fährt sauber herunter, „Abschießen“ beendet sofort (`docker kill`; die DB führt beim nächsten Start eine Instance Recovery aus, die Neustart-Regel wird dabei ausgesetzt und beim nächsten „Master starten“ wiederhergestellt), „Entfernen“ räumt übrig gebliebene Hilfscontainer weg. Container, die nichts mit zeitkapsel zu tun haben, erscheinen nicht;
- die letzten 300 Vorgänge mit Status, Benutzer, Dauer und vollständigem Protokoll, filterbar nach laufend, fehlgeschlagen/abgebrochen und eigenen. Die Historie liegt in `/srv/oradb/meta/jobs/` (`<id>.json` und `<id>.log`, `ZK_JOBS_KEEP` Einträge, Standard 500) und bleibt über Neustarts erhalten; Vorgänge, die bei einem Neustart des Dienstes liefen, stehen danach als „unterbrochen“ darin.

„Abbrechen“ beendet den laufenden Befehl des Vorgangs und entfernt sofort Hilfscontainer (dbca-Anlage, Prüfstart, Release-Zwischen-DB) bzw. beendet den Data-Pump-Job über `DBMS_DATAPUMP`; danach räumt der Vorgang wie bei einem Fehler auf (halbfertige Subvolumes werden gelöscht, eine für den Import heruntergefahrene Quelle wieder gestartet). Benutzer können eigene Vorgänge abbrechen und die Container ihrer eigenen Entwickler-DBs stoppen oder abschießen, Administratoren alles.

„Erneut ausführen“ gibt es bei fehlgeschlagenen, abgebrochenen und unterbrochenen Vorgängen. Jeder Vorgang merkt sich den Aufruf, der ihn gestartet hat (Aktion und Angaben); die Wiederholung schickt genau diesen Aufruf noch einmal, ohne dass der Assistent erneut durchlaufen werden muss. Passwörter werden dabei nicht gespeichert – die Historie liegt auf der Platte –, sondern vor dem Wiederholen neu abgefragt. Die Rechte werden wie beim ersten Aufruf geprüft; Benutzer wiederholen nur eigene Vorgänge. Vorgänge, die ein Zeitplan gestartet hat, wiederholt man mit „Jetzt ausführen“ beim Zeitplan.

## Alle Snapshots: Platz, Löschen, Übernehmen

Die Seite „Alle Snapshots“ listet jedes Subvolume eines Hosts. Der Platz wird im Hintergrund ermittelt und eine Viertelstunde zwischengespeichert (`ZK_USAGE_MAX_AGE`, Sekunden): „Exklusiv“ (`btrfs filesystem du`, ohne Kompression gezählt) ist der Platz, der beim Löschen frei würde; „Auf Platte“ (`compsize`, wenn installiert) der tatsächlich belegte Platz nach Kompression.

Löschen geht für jede Art direkt in der Tabelle: Releases, Entwickler-DBs und ihre Snapshots über dieselben Aktionen wie in der Datenbank-Ansicht (Benutzer für die eigenen Entwickler-DBs), Master-DBs, Zwischenstände, importierte und sonstige Subvolumes nur für Administratoren. Eine Master-DB lässt sich nur löschen, wenn ihr Container nicht läuft, und nur nach Eingabe ihres Namens; ihr gestoppter Container wird mit entfernt, Releases und Entwickler-DBs bleiben.

Subvolumes von einer anderen zeitkapsel-Instanz (z. B. per `btrfs send | ssh … btrfs receive /srv/oradb/import`) gehören nach `/srv/oradb/import/`. zeitkapsel prüft dort – und bei sonstigen Subvolumes unter `master/` –, ob sie eine Datenbank in seiner Ablage enthalten (Markierungsdatei `.<SID>.created` oder `dbconfig/<SID>/`, Datendateien, spfile, Passwortdatei). Ist sie vollständig und gibt es noch keine Master-DB gleichen Namens, übernimmt „Als Master-DB übernehmen“ sie: als beschreibbarer Snapshot unter `master/<SID>` (ein beschreibbares sonstiges Subvolume unter `master/` wird einfach umbenannt), fehlende Markierungs- und Netzwerkdateien werden ergänzt, dann startet die DB mit dem gewählten Port. Die Quelle bleibt erhalten.

## Kompression

zeitkapsel lässt Copy-on-Write eingeschaltet, damit die Kompression des Dateisystems (`compress` bzw. `compress-force` in den Mount-Optionen) auch für die Datenbankdateien greift. btrfs komprimiert nur Dateien mit Copy-on-Write; Dateien mit dem NOCOW-Attribut (`chattr +C`) werden unkomprimiert und ohne Prüfsummen gespeichert.

| | Copy-on-Write (Standard) | NOCOW (`ZK_NOCOW=1`) |
| --- | --- | --- |
| Kompression | ja | nein |
| Prüfsummen | ja, btrfs erkennt beschädigte Blöcke | nein |
| Schreiben | Kopie bei jeder Änderung; Datendateien fragmentieren mit der Zeit | Überschreiben an Ort und Stelle (außer nach einem Snapshot) |
| Snapshots | ja | ja |

Für Entwicklungsdatenbanken auf SSDs überwiegt meist der Platzgewinn. Nicht defragmentieren (`btrfs filesystem defragment`, `autodefrag`): das hebt die gemeinsame Nutzung von Blöcken zwischen Master, Releases und Entwickler-DBs auf, und jeder Snapshot belegt danach seinen vollen Platz.

„Verwaltung“ zeigt Dateisystem und Kompression des Hosts und warnt, wenn eine Master-DB noch NOCOW-Dateien hat. Prüfen von Hand:

```bash
lsattr -d /srv/oradb/master/<SID> /srv/oradb/master/<SID>/<SID>   # "C" = NOCOW
compsize /srv/oradb/master/<SID>                                   # tatsächlich komprimierter Anteil
```

Eine Master-DB mit NOCOW-Dateien stellst du so um (das Attribut lässt sich an Dateien mit Inhalt nicht zuverlässig entfernen, sie müssen einmal neu geschrieben werden):

```bash
docker stop -t 600 <sid>
btrfs subvolume create /srv/oradb/master/<SID>.neu
lsattr -d /srv/oradb/master/<SID>.neu                     # darf kein C zeigen
cp -a --reflink=never /srv/oradb/master/<SID>/. /srv/oradb/master/<SID>.neu/
mv /srv/oradb/master/<SID> /srv/oradb/master/<SID>.alt
mv /srv/oradb/master/<SID>.neu /srv/oradb/master/<SID>
docker start <sid>
btrfs subvolume delete /srv/oradb/master/<SID>.alt          # wenn alles läuft
```

Releases und Entwickler-DBs aus der Zeit davor bleiben unkomprimiert, bis sie durch neue ersetzt werden.

## Neue Datenbank

„Verwaltung“ → „Neue Datenbank“ legt eine leere Datenbank mit eigenem Container an. Ohne PDB-Namen entsteht eine klassische Non-CDB in derselben Form wie eine migrierte (Datendateien, Controlfiles und Redo-Logs unter `<SID>/`, spfile und Passwortdatei unter `dbconfig/<SID>/`); mit PDB-Namen eine CDB mit dieser einen PDB.

zeitkapsel ruft dafür dbca selbst im Oracle-Image auf (General_Purpose-Vorlage, Dateisystem, automatische SGA mit der angegebenen Speichergröße, Recovery Area in `fast_recovery_area/` innerhalb des Subvolumes) und startet die DB danach als normale Master-DB. Das dauert 15 bis 45 Minuten. Das Passwort gilt für SYS und SYSTEM (bei einer CDB auch PDBADMIN), wird per stdin übergeben und erscheint weder in der Container-Konfiguration noch im Vorgangsprotokoll. Voraussetzung ist das Korrektur-Image (siehe „Image“); die Oberfläche prüft das vorher.

## Import von einem anderen Server

„Verwaltung“ → „Datenbank importieren“ übernimmt eine laufende Oracle-DB per SSH.

Voraussetzungen auf der Quelle:

- der öffentliche Schlüssel des Agenten (im Dialog angezeigt) in `~/.ssh/authorized_keys` des SSH-Benutzers (meist `oracle`), der die Datendateien lesen und `sqlplus / as sysdba` ausführen darf;
- `ORACLE_HOME` im Login-Profil oder im Dialog angegeben, `rsync` installiert;
- Dateisystem statt ASM, spfile statt pfile, dieselbe oder eine ältere Version als das Image.

Ablauf: Schritt 1 liest Version, Dateien, Parameter und plant den Import (Verzeichnisse, Zusatz-Mounts, Shared Memory, Service-Name, Hinweise). Schritt 2 fährt die Quelle sauber herunter, legt das Subvolume an, kopiert genau die Dateien der DB per `rsync`, holt spfile und Passwortdatei, schreibt Netzwerk-Konfiguration und Markierungsdateien und startet die DB als Master-DB. Liegen die Dateien auf der Quelle nicht unter `/opt/oracle/oradata/<SID>`, werden sie im Container zusätzlich unter ihrem alten Pfad eingehängt, sodass spfile und Controlfiles unverändert passen. Mit „Quell-DB nach dem Kopieren wieder starten“ ist es eine Kopie, ohne ist es ein Umzug. Geht etwas schief, wird die Quelle wieder gestartet und das halbfertige Subvolume entfernt. Ist das Image neuer als die Quelle, läuft danach `datapatch`.


## Import aus einem Verzeichnis auf dem Host

„Datenbank importieren“ → „Verzeichnis auf diesem Host“ übernimmt eine heruntergefahrene Datenbank, deren Dateien irgendwo im Dateisystem liegen – zum Beispiel noch in der Ablage eines früheren Oracle-Containers unter `/srv/oradata` (mit `<SID>/` und `dbconfig/<SID>/`) oder einfach als Verzeichnis mit Datendateien.

Die Prüfung startet die DB nicht. Sie sucht spfile und Passwortdatei (angegebener Pfad, sonst `dbconfig/<SID>/` und das Datenverzeichnis), liest aus der spfile `control_files` und weitere Parameter, aus der Controlfile die Pfade der Daten-, Temp- und Redo-Dateien, und ordnet sie dem Datenverzeichnis bzw. weiteren Verzeichnissen zu. Läuft auf dem Host noch eine Instanz dieser SID oder ein Container auf dem Verzeichnis, wird abgebrochen.

Fehlt die spfile, fragt der Dialog, ob eine neue angelegt werden soll – aus einer gefundenen pfile oder, wenn es keine gibt, aus Mindestangaben (DB-Name, Blockgröße, Undo-Tablespace, Speicher). Fehlt die Passwortdatei, fragt er nach einem neuen SYS-Passwort und legt sie mit `orapwd` an. Ohne Zustimmung läuft der Import nicht.

Der Import legt unter `master/<SID>` ein neues Subvolume an (mit Kompression, siehe „Kompression“), kopiert die Dateien hinein, schreibt Netzwerk-Konfiguration, Markierungsdateien und `zeitkapsel.env` (Zusatz-Mounts für Dateien, die in der DB unter anderen Pfaden stehen) und startet die Kopie einmal in einem Wegwerf-Container (`startup`, Abfragen, `shutdown immediate`). Nur wenn sie dabei sauber öffnet, wird sie zur Master-DB und danach per `datapatch` auf den Patch-Stand des Images gebracht; sonst wird das neue Subvolume wieder entfernt und die Oracle-Fehlermeldung angezeigt. Die Quelle wird in keinem Fall verändert.


## Übernahme per Data Pump (Quelle läuft weiter)

„Datenbank importieren“ → „Laufende DB per Data Pump“ übernimmt eine Datenbank, ohne sie anzuhalten – auch von älteren Versionen (ab 12c) und von anderen Plattformen.

Schritt 1 verbindet sich mit Host, Port, Service-Name, Benutzer und Passwort der Quelle (aus einem Wegwerf-Container mit dem `sqlplus` des Images) und liest Version, Zeichensätze, Schemas mit Größe, Tablespaces, Rechte und Stolpersteine. Der Benutzer braucht `DBA` oder `DATAPUMP_EXP_FULL_DATABASE`. Ist die Quelle eine CDB, gehört der Service-Name der PDB hinein.

Schritt 2: ganze Datenbank oder ausgewählte Schemas, SID, Port, Speicher und Passwort der neuen DB, Parallelität. Dann:

1. neue Non-CDB wie bei „Neue Datenbank“ anlegen – in einem komprimierten Subvolume, mit demselben Zeichensatz und nationalen Zeichensatz wie die Quelle;
2. permanente und temporäre Tablespaces der Quelle vorab als OMF-Dateien im Subvolume anlegen (Pfade der Quelle gibt es hier nicht; große Smallfile-Tablespaces bekommen mehrere Dateien, abweichende Blockgrößen einen passenden Cache), Standard-Tablespaces wie in der Quelle setzen, Directory `ZK_IMPORT_DIR` und DB-Link `ZK_SRC` einrichten, eigene Passwort-Prüffunktionen von Profilen aus SYS der Quelle kopieren (Data Pump überträgt SYS nicht – ohne sie scheitern Profile und damit alle Benutzer mit diesen Profilen);
3. `impdp` mit `NETWORK_LINK` – ohne Dump-Dateien, auf Wunsch mit `FLASHBACK_TIME=SYSTIMESTAMP` konsistent zum Startzeitpunkt; der Fortschritt erscheint live im Vorgangsprotokoll, das vollständige Protokoll liegt in `zk_import/` im Subvolume;
4. ungültige Objekte neu kompilieren (`utlrp`), Objektzahlen je Schema und Typ mit der Quelle vergleichen und Abweichungen auflisten, DB-Link wieder entfernen.

Exit-Code 5 von impdp („mit Fehlern abgeschlossen“) gilt als fertig, die Meldungen stehen im Protokoll. Scheitert der Import, bleibt die neue DB zur Analyse stehen und lässt sich unter „Alle Snapshots“ löschen. Ein erneuter Versuch mit derselben SID verwendet sie weiter (ohne dbca) und entfernt vorher die Anwendungs-Benutzer mit ihren Objekten sowie eigene Rollen und Profile, damit der neue Lauf vollständig ist – Data Pump würde vorhandene Tabellen sonst überspringen. Eine bereits erfolgreich übernommene DB ist dafür gesperrt; zum bewussten Wiederholen in `meta/<SID>/db.json` `"dp_pending": true` setzen. Die Passwörter von Quelle und neuer DB werden per stdin übergeben und erscheinen nicht im Protokoll; das Quell-Passwort liegt nur während des Imports im DB-Link der neuen DB.

Zeitzonendatei: Data Pump importiert nur in eine Datenbank mit gleicher oder neuerer Zeitzonendatei (sonst ORA-39405). dbca übernimmt die Version der Vorlage (in 19c: 32), obwohl das Oracle Home mit den RUs neuere mitbringt. zeitkapsel hebt deshalb jede neu angelegte Non-CDB mit `utltz_upg_check.sql`/`utltz_upg_apply.sql` auf den neuesten Stand im Home an und prüft vor dem Data-Pump-Import noch einmal; ist die Quelle neuer als alles im Image, meldet das schon die Prüfung.

Grenzen: Tabellen mit `LONG`/`LONG RAW` lassen sich über einen DB-Link nicht übertragen (ORA-31679) und müssen per expdp-Datei nachgeholt werden – die Prüfung zählt sie vorab. Für einen konsistenten Stand muss die Quelle genug Undo vorhalten (`undo_retention`), sonst droht ORA-01555. Die Übertragung läuft über das Netz und dauert entsprechend; Statistiken werden mit übernommen.

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

## Skripte

`scripts/` enthält die Kommandozeilen-Werkzeuge aus dem Runbook, abgestimmt auf dieselbe Ablage, dieselben Labels und `zeitkapsel.env`. Sie lesen `ZK_BASE` und `ZK_IMAGE` aus `/etc/zeitkapsel.env`.

| Skript | Zweck |
| --- | --- |
| `startdb.sh <SID> [Port]` | Master-DB starten (Port sonst aus `meta/<SID>/db.json`), prüft vorher die Markierungsdateien |
| `golden.sh <SID> [Name]` | Release vom laufenden Master erstellen |
| `devdb.sh create <SID> <name> [Release] [Besitzer]` | Entwickler-DB anlegen; Besitzer standardmäßig `$SUDO_USER` |
| `devdb.sh delete <SID> <name>` / `devdb.sh list` | löschen (mit Snapshots) bzw. auflisten |

## Sicherheit

- Agenten laufen als root und steuern Docker und btrfs: ihren Port nur für den Controller erreichbar machen, jedes Token geheim halten (`/etc/zeitkapsel.env` mit Rechten 600).
- Die Oberfläche nur intern betreiben; über fremde Netze einen Reverse Proxy mit TLS davorsetzen und `ZK_COOKIE_SECURE=1`. Auch Agent-Verbindungen über fremde Netze gehören hinter TLS.
- Passwörter liegen als Hash (werkzeug, scrypt) in SQLite; nach acht Fehlversuchen in fünf Minuten sperrt die Anmeldung für die Adresse kurz.
- Ändernde Aufrufe brauchen einen eigenen Header (Schutz gegen Cross-Site-Requests); alle Eingaben werden gegen enge Muster geprüft, Befehle laufen ohne Shell.
- Jede ändernde Aktion steht mit Benutzer, Host und Ergebnis im Protokoll.

## Grenzen

- Getestet ist zeitkapsel bisher gegen simulierte `btrfs`-, `docker`-, `ssh`- und `rsync`-Befehle. Vor allem Neuanlage (dbca im Korrektur-Image) und Import sollten zuerst mit einer Test-DB laufen.
- Der Import unterstützt keine ASM-Ablage und keine Archivlog-Ziele außerhalb der eingehängten Verzeichnisse; Directory-Objekte (Data Pump, UTL_FILE) zeigen danach eventuell auf Pfade, die im Container fehlen.
- Vorgangsprotokolle liegen im Arbeitsspeicher des jeweiligen Agenten (die letzten 50); ein Neustart des Dienstes bricht laufende Vorgänge ab.
