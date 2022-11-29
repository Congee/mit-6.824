#!/usr/bin/env bash

set -eu
set -o pipefail

logfile="$1"

lnavformat=$(realpath .raft.lnav.json)
(cd ~/.config/lnav/formats/installed && ln -sf "$lnavformat")

go run ./cmd/ppl/main.go "$logfile"
exec lnav -f .filter-out.lnav "$logfile"
