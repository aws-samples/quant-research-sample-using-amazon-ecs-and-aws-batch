#!/bin/bash
# Dispatcher of the earnings research image (ENTRYPOINT):
#   run <package> <command> [args...]     -> python <package entry script> <command> [args...]
#   run <package> <script.py> [args...]   -> python <script.py> [args...]
# Always runs from /app/<package> so package-local imports resolve.
set -e

APP_ROOT="${APP_ROOT:-/app}"

usage() {
    echo "usage: run <basket_study|market_data|content_pipeline|sentiment_analysis> <command|script.py> [args...]" >&2
}

pkg="$1"
case "$pkg" in
    basket_study|sentiment_analysis) entry=evaluate.py ;;
    market_data|content_pipeline) entry=main.py ;;
    help|-h|--help) usage; exit 0 ;;
    *) echo "unknown package: '${pkg}'" >&2; usage; exit 2 ;;
esac
shift

cd "${APP_ROOT}/${pkg}"

if [[ "$1" == *.py ]]; then
    if [ ! -f "$1" ] || [[ "$1" == */* ]]; then
        echo "no script '$1' in ${pkg}" >&2
        exit 2
    fi
    entry="$1"
    shift
fi

# Only the content pipeline's browser backfill needs a display. Start Xvfb
# explicitly and point DISPLAY at it (`xvfb-run -a` can hang on X server lock races).
if [ "$pkg" = "content_pipeline" ] && [ "$1" = "backfill-fetch" ]; then
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

exec python "$entry" "$@"
