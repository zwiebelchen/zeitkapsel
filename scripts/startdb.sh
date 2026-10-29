#!/bin/bash
# Master-DB aus ihrem Subvolume starten.
# Aufruf: startdb.sh <SID> [Port]     (ohne Port: Port aus meta/<SID>/db.json)
set -euo pipefail
. "$(dirname "$(readlink -f "$0")")/lib.sh"
SID=${1:?Aufruf: startdb.sh <SID> [Port]}
VOL=$BASE/master/$SID
PORT=${2:-$(python3 -c "import json,sys; print(json.load(open(sys.argv[1])).get('port',''))" "$BASE/meta/$SID/db.json" 2>/dev/null || true)}
[ -n "$PORT" ] || { echo "Kein Port angegeben und keiner in $BASE/meta/$SID/db.json" >&2; exit 1; }
zk_check_volume "$VOL" "$SID"
zk_extra_args "$VOL"
DUMPS=(); [ -d "$MASTER_DUMPS" ] && DUMPS=(-v "$MASTER_DUMPS:/oracle/dumps")
NAME=${SID,,}
SINCE=$(date +%s)
if docker inspect "$NAME" >/dev/null 2>&1; then
  docker start "$NAME"
else
  docker run -d --name "$NAME" --init --restart unless-stopped --stop-timeout 600 \
    -p "$PORT:1521" -e ORACLE_SID="$SID" -v "$VOL:/opt/oracle/oradata" "${DUMPS[@]}" "${EXTRA[@]}" "$IMAGE"
  mkdir -p "$BASE/meta/$SID"
  [ -f "$BASE/meta/$SID/db.json" ] || zk_json port "$PORT" > "$BASE/meta/$SID/db.json"
fi
zk_wait_ready "$NAME" "$SINCE" && echo "Master-DB $SID läuft (Port $PORT)."
