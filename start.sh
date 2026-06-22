#!/usr/bin/env sh
# POSIX launcher for UMANS Proxy. Mirrors start.cmd.
cd "$(dirname "$0")" || exit 1
exec node launcher.js "$@"
