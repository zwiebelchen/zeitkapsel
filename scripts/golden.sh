#!/bin/bash
# Release (schreibgeschützter Snapshot) einer Master-DB erstellen, ohne den Master anzuhalten.
# Aufruf: golden.sh <SID> [Release-Name]      (Standardname: Datum und Uhrzeit)
# Speicherwerte für Entwickler-Instanzen: Datei in ZK_RELEASE_SQL_FILE oder die Werte unten.
set -euo pipefail
. "$(dirname "$(readlink -f "$0")")/lib.sh"
SID=${1:?Aufruf: golden.sh <SID> [Release-Name]}
NAME=${2:-$(date +%Y%m%d-%H%M)}
[[ $NAME =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$ ]] || { echo "Ungültiger Release-Name" >&2; exit 1; }
TS=$(date +%Y%m%d-%H%M%S)
TMP=$BASE/build/$SID-$NAME-$TS
BUILD=release-build-${SID,,}-$TS
DST=$BASE/golden/$SID/$NAME
[ -e "$DST" ] && { echo "Release $NAME gibt es schon" >&2; exit 1; }
mkdir -p "$BASE/golden/$SID" "$BASE/build" "$BASE/meta/$SID/releases"

cleanup() { docker rm -f "$BUILD" >/dev/null 2>&1 || true; [ -d "$TMP" ] && btrfs subvolume delete "$TMP" >/dev/null || true; }
trap cleanup EXIT

btrfs subvolume snapshot "$BASE/master/$SID" "$TMP"
zk_check_volume "$TMP" "$SID"
zk_extra_args "$TMP"
SINCE=$(date +%s)
docker run -d --name "$BUILD" --init -e ORACLE_SID="$SID" -v "$TMP:/opt/oracle/oradata" "${EXTRA[@]}" "$IMAGE"
zk_wait_ready "$BUILD" "$SINCE"

SQLFILE=${ZK_RELEASE_SQL_FILE:-${ZKV[ZK_RELEASE_SQL_FILE]:-}}
{
  echo "whenever sqlerror exit failure"
  if [ -n "$SQLFILE" ]; then cat "$SQLFILE"; else cat <<'SQL'
alter system set sga_target=1G scope=spfile;
alter system set sga_max_size=1G scope=spfile;
alter system set pga_aggregate_target=300M scope=spfile;
SQL
  fi
  echo "shutdown immediate"; echo "exit"
} | docker exec -i "$BUILD" sqlplus -s / as sysdba

docker rm -f "$BUILD" >/dev/null
btrfs subvolume snapshot -r "$TMP" "$DST"
zk_json name "$NAME" description "" created "$(date -Iseconds)" origin "Master-DB (golden.sh)" > "$BASE/meta/$SID/releases/$NAME.json"
echo "Release: $DST"
