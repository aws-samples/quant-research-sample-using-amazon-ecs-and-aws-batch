#!/bin/bash
# Only the backfill browser strategy needs a display. For it, start Xvfb
# explicitly and point DISPLAY at it — `xvfb-run -a` hangs indefinitely on
# Fargate (X server lock races / no clean startup signal). All other commands
# run directly with no X server.
set -e

if [ "$1" = "backfill-fetch" ]; then
    rm -f /tmp/.X99-lock
    Xvfb :99 -screen 0 1440x900x24 -nolisten tcp &
    export DISPLAY=:99
    # wait for the display socket; fail loudly rather than hang if it never comes
    ready=0
    for _ in $(seq 1 40); do
        if [ -e /tmp/.X11-unix/X99 ]; then ready=1; break; fi
        sleep 0.25
    done
    if [ "$ready" -ne 1 ]; then
        echo "FATAL: Xvfb failed to start display :99" >&2
        exit 1
    fi
    echo "Xvfb ready on :99" >&2
fi

exec python main.py "$@"
