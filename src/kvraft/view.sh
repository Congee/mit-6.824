#!/usr/bin/env bash

set -eu
set -o pipefail

logfile="$1"

lnavformat=$(realpath .raft.lnav.json)
(cd ~/.config/lnav/formats/installed && ln -sf "$lnavformat")

mkdir -p /tmp/raft
log=/tmp/raft/$(basename "$logfile")
cp -f "$logfile" "$log"

project_root=$(git rev-parse --show-toplevel)
go run $project_root/src/raft/cmd/ppl/main.go "$log"
exec lnav -f .filter-out.lnav "$log"
