#!/bin/bash
# Entwickler-DBs auf der Kommandozeile verwalten – gleiche Ablage und Labels wie die Oberfläche.
# Aufruf:
#   devdb.sh create <SID> <name> [release] [besitzer]   (Standard: neuestes Release, Besitzer = $SUDO_USER)
#   devdb.sh delete <SID> <name>
#   devdb.sh list
set -euo pipefail
. "$(dirname "$(readlink -f "$0")")/lib.sh"

next_port() {
  local used p=${ZK_DEV_PORT_START:-${ZKV[ZK_DEV_PORT_START]:-15211}}
  used=$( { ss -ltn | awk 'NR>1{sub(/.*:/,"",$4); print $4}'
            docker ps -aq | xargs -r docker inspect -f '{{range $p, $c := .HostConfig.PortBindings}}{{(index $c 0).HostPort}} {{end}}'; } \
          | tr ' ' '\n' | sort -u )
  while grep -qx "$p" <<<"$used"; do p=$((p+1)); done
  echo "$p"
}

case "${1:-}" in
  create)
    SID=${2:?SID fehlt}; NAME=${3:?Name fehlt}
    [[ $NAME =~ ^[a-z0-9][a-z0-9-]{0,29}$ ]] || { echo "Name: Kleinbuchstaben, Ziffern, Bindestrich" >&2; exit 1; }
    REL=${4:-$(ls -1t "$BASE/golden/$SID" | head -1)}
    OWNER=${5:-${SUDO_USER:-root}}
    ID=$SID-$NAME; VOL=$BASE/dev/$ID; DUMPS=$BASE/devdumps/$ID
    [ -e "$VOL" ] && { echo "$VOL gibt es schon" >&2; exit 1; }
    mkdir -p "$BASE/dev" "$BASE/meta/$SID/devdbs" "$DUMPS/19c/datapump/$SID"
    btrfs subvolume snapshot "$BASE/golden/$SID/$REL" "$VOL"
    zk_check_volume "$VOL" "$SID" || { btrfs subvolume delete "$VOL"; exit 1; }
    chown -R 54321:54321 "$DUMPS"
    zk_extra_args "$VOL"
    PORT=$(next_port); NOW=$(date -Iseconds); SINCE=$(date +%s)
    zk_json owner "$OWNER" base "release:$REL" created "$NOW" > "$BASE/meta/$SID/devdbs/$ID.json"
    docker run -d --name "${ID,,}" --init --stop-timeout 600 \
      --label devdb=1 --label "devport=$PORT" --label "devsid=$SID" --label "devrelease=$REL" \
      --label "devowner=$OWNER" --label "devbase=release:$REL" --label "devcreated=$NOW" \
      -p "$PORT:1521" -e ORACLE_SID="$SID" \
      -v "$VOL:/opt/oracle/oradata" -v "$DUMPS:/oracle/dumps" "${EXTRA[@]}" "$IMAGE"
    zk_wait_ready "${ID,,}" "$SINCE"
    SERVICE=${ZKV[ZK_SERVICE]:-${ZKV[ZK_PDB]:-$SID}}
    echo "Release: $REL   Besitzer: $OWNER"
    echo "JDBC:    jdbc:oracle:thin:@//$(hostname -f):$PORT/$SERVICE"
    ;;
  delete)
    SID=${2:?SID fehlt}; NAME=${3:?Name fehlt}; ID=$SID-$NAME
    docker rm -f "${ID,,}" 2>/dev/null || true
    [ -d "$BASE/dev/$ID" ] && btrfs subvolume delete "$BASE/dev/$ID"
    for s in "$BASE/devsnaps/$ID"/*; do [ -d "$s" ] && btrfs subvolume delete "$s"; done
    rm -rf "$BASE/devdumps/$ID" "$BASE/devsnaps/$ID" "$BASE/meta/$SID/devdbs/$ID.json"
    rm -f "$BASE/meta/$SID/devsnaps/${ID}__"*.json
    ;;
  list)
    docker ps -a --filter label=devdb=1 --format 'table {{.Names}}\t{{.Status}}\t{{.Label "devport"}}\t{{.Label "devowner"}}\t{{.Label "devbase"}}'
    ;;
  *) sed -n '2,7p' "$0"; exit 1 ;;
esac
