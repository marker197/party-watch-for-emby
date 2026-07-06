#!/bin/sh
# Nginx entrypoint: run nginx in the foreground, and reload every 6 hours
# so renewed SSL certificates are picked up without a container restart.

set -e

# Start the periodic reload loop in the background
(
  while true; do
    sleep 21600  # 6 hours
    echo "[nginx-entrypoint] Reloading nginx to pick up any renewed certificates…"
    nginx -s reload 2>/dev/null || true
  done
) &

# Start nginx in the foreground (keeps the container alive)
exec nginx -g 'daemon off;'
