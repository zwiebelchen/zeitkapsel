#!/bin/bash
# zeitkapsel – erzwungener Befehl für den SSH-Schlüssel eines anderen zeitkapsel-Hosts.
#
# Der Agent trägt den Schlüssel eines Quellhosts so in /root/.ssh/authorized_keys ein:
#   restrict,command="/opt/zeitkapsel/scripts/peer-ssh.sh" ssh-ed25519 AAAA… zeitkapsel-peer:<host>
# Erlaubt sind nur:
#   zeitkapsel-peer-check                      Verbindungstest
#   btrfs receive <ablage>/incoming/<id>/      btrfs-Stream empfangen
#   rsync --server … . <ablage>/incoming/<id>/<pfad>   rsync in ein Empfangsverzeichnis
# <ablage>/incoming/<id> muss schon existieren; der Zielagent legt es beim Vorbereiten an.
set -u
cmd=${SSH_ORIGINAL_COMMAND:-}
path_re='(/[A-Za-z0-9._/-]+/incoming/[A-Za-z0-9._-]+)(/[A-Za-z0-9._/-]*)?'

deny() { echo "zeitkapsel-peer: nicht erlaubt: $cmd" >&2; exit 126; }

check_path() {   # Empfangsverzeichnis muss existieren, kein ..
  local p=$1 base=$2
  [[ $p == *..* ]] && deny
  [ -d "$base" ] || { echo "zeitkapsel-peer: Empfangsverzeichnis $base fehlt" >&2; exit 1; }
}

if [[ $cmd == "zeitkapsel-peer-check" ]]; then
  echo "zeitkapsel-peer ok $(hostname -f 2>/dev/null || hostname)"
  exit 0
fi
if [[ $cmd =~ ^btrfs\ receive\ ${path_re}/?$ ]]; then
  base=${BASH_REMATCH[1]}
  check_path "$cmd" "$base"
  exec btrfs receive "$base/"
fi
long_ok='--(numeric-ids|delete|delete-during|delete-after|delete-delay|sparse|inplace|partial|one-file-system|compress-level=[0-9]+|compress-choice=[a-z0-9]+|checksum-choice=[a-z0-9]+)'
if [[ $cmd =~ ^rsync\ --server(\ -[A-Za-z0-9.]+|\ ${long_ok})*\ \.\ ${path_re}$ ]]; then
  base=${BASH_REMATCH[3]}
  check_path "$cmd" "$base"
  # shellcheck disable=SC2086  # rsync-Argumente sind oben auf harmlose Zeichen beschränkt
  exec $cmd
fi
deny
