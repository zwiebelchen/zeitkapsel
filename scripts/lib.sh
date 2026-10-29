#!/bin/bash
# zeitkapsel – gemeinsame Funktionen der Shell-Skripte (wird von den anderen Skripten eingebunden)
# Liest ZK_BASE und ZK_IMAGE aus /etc/zeitkapsel.env, wenn vorhanden.

zk_read_env() {   # KEY=VALUE-Datei lesen, ohne sie auszuführen; Ergebnis in ZKV[KEY]
  declare -gA ZKV=()
  local f=$1 line k v
  [ -f "$f" ] || return 0
  while IFS= read -r line || [ -n "$line" ]; do
    [[ $line =~ ^[[:space:]]*# || $line != *=* ]] && continue
    k=${line%%=*}; v=${line#*=}
    k=${k//[[:space:]]/}; v=${v%\"}; v=${v#\"}; v=${v%\'}; v=${v#\'}
    ZKV[$k]=$v
  done < "$f"
}

zk_read_env /etc/zeitkapsel.env
BASE=${ZK_BASE:-${ZKV[ZK_BASE]:-/srv/oradb}}
IMAGE=${ZK_IMAGE:-${ZKV[ZK_IMAGE]:-oracle/database:19.30.0-ee}}
MASTER_DUMPS=${ZK_MASTER_DUMPS:-${ZKV[ZK_MASTER_DUMPS]:-/srv/oradumps}}

zk_extra_args() {   # zusätzliche docker-run-Argumente aus <vol>/zeitkapsel.env nach EXTRA
  local vol=$1 m
  EXTRA=()
  zk_read_env "$vol/zeitkapsel.env"
  for m in ${ZKV[ZK_MOUNTS]:-}; do EXTRA+=(-v "$vol/${m%%:*}:${m#*:}"); done
  [ -n "${ZKV[ZK_SHM_SIZE]:-}" ] && EXTRA+=(--shm-size "${ZKV[ZK_SHM_SIZE]:-}")
  if [ -n "${ZKV[ZK_EDITION]:-}" ] && [ "${ZKV[ZK_EDITION]:-}" != enterprise ]; then
    EXTRA+=(-e "ORACLE_EDITION=${ZKV[ZK_EDITION]:-}")
  fi
  return 0
}

zk_check_volume() {   # ohne Markierungsdateien würde das Startskript des Images Daten löschen
  local vol=$1 sid=$2 f
  for f in "$vol/.$sid.created" "$vol/dbconfig/$sid/spfile$sid.ora" "$vol/dbconfig/$sid/orapw$sid"; do
    [ -e "$f" ] || { echo "FEHLT: $f – abgebrochen" >&2; return 1; }
  done
  compgen -G "$vol/$sid/*.dbf" >/dev/null || { echo "FEHLT: $vol/$sid/*.dbf – abgebrochen" >&2; return 1; }
  compgen -G "$vol/dbconfig/$sid/.docker_*" >/dev/null || { echo "FEHLT: .docker_<edition> – abgebrochen" >&2; return 1; }
}

zk_wait_ready() {   # wartet auf "DATABASE IS READY TO USE" seit Zeitpunkt $2 (Unix-Zeit)
  local name=$1 since=$2 timeout=${3:-900} t0=$SECONDS
  while (( SECONDS - t0 < timeout )); do
    if docker logs --since "$since" "$name" 2>&1 | grep -q "DATABASE SETUP WAS NOT SUCCESSFUL"; then
      echo "Start fehlgeschlagen – docker logs $name" >&2; return 1
    fi
    docker logs --since "$since" "$name" 2>&1 | grep -q "DATABASE IS READY TO USE" && return 0
    sleep 5
  done
  echo "Zeitüberschreitung beim Warten auf $name" >&2; return 1
}

zk_json() {   # sehr einfache JSON-Ausgabe: zk_json key value [key value …]
  local out="{" sep=""
  while [ $# -ge 2 ]; do
    out+="$sep\"$1\": \"${2//\"/\\\"}\""; sep=", "; shift 2
  done
  printf '%s}\n' "$out"
}
