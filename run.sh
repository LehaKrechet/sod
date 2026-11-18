#!/bin/sh
set -eu
cd "$(dirname "$0")"
[ -x ./sod ] || make
exec ./sod "$@"
