#!/usr/bin/env bash
# Local end-to-end demo without Docker.
#
# Starts the relay, a playout-side sink and a lossy dual-path source, then
# prints the sink report and the relay's /skipped and /status output.
#
# Override ports if needed:
#   API_PORT=18000 UDP_PORT_A=19100 UDP_PORT_B=19101 FORWARD_PORT=19200 \
#     ./demo/run_local_demo.sh
set -euo pipefail

cd "$(dirname "$0")/.."

API_PORT="${API_PORT:-8000}"
PORT_A="${UDP_PORT_A:-9100}"
PORT_B="${UDP_PORT_B:-9101}"
FWD_PORT="${FORWARD_PORT:-9200}"
LOG_DIR="$(mktemp -d)"
echo "[demo] logs: $LOG_DIR"

check_port_free() {
  local port="$1"
  if ! python3 - "$port" <<'PY' 2>/dev/null
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
  then
    echo "[demo] ERROR: UDP port $port is already in use. Free it or rerun" \
         "with FORWARD_PORT/UDP_PORT_A/UDP_PORT_B overrides." >&2
    exit 1
  fi
}
check_port_free "$PORT_A"
check_port_free "$PORT_B"
check_port_free "$FWD_PORT"

# Run every child in our own process group so one signal tears the whole
# pipeline down (the relay is long-lived and would otherwise leak on exit).
cleanup() {
  trap - EXIT INT TERM
  kill -TERM 0 2>/dev/null || true
  sleep 0.3
  kill -KILL 0 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[demo] starting relay (ingress UDP $PORT_A/$PORT_B -> forward UDP $FWD_PORT, API $API_PORT)"
RELAY_RATE_BPS="${RELAY_RATE_BPS:-12000}" \
RELAY_BURST_BYTES="${RELAY_BURST_BYTES:-344}" \
RELAY_MAX_BACKLOG_BYTES="${RELAY_MAX_BACKLOG_BYTES:-3440}" \
API_PORT="$API_PORT" UDP_PORT_A="$PORT_A" UDP_PORT_B="$PORT_B" \
  FORWARD_HOST=127.0.0.1 FORWARD_PORT="$FWD_PORT" \
  python3 -u -m app.main >"$LOG_DIR/relay.log" 2>&1 &
RELAY_PID=$!

# Wait for the API to come up (fail fast if the relay dies).
for _ in $(seq 1 50); do
  if curl -sf "http://127.0.0.1:$API_PORT/health" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$RELAY_PID" 2>/dev/null; then
    echo "[demo] relay died early:"; cat "$LOG_DIR/relay.log"; exit 1
  fi
  sleep 0.1
done

echo "[demo] starting sink (playout machine stand-in)"
python3 -u -m demo.demo_sink --host 127.0.0.1 --port "$FWD_PORT" \
  --duration 4.5 --api-url "http://127.0.0.1:$API_PORT" \
  >"$LOG_DIR/sink.log" 2>&1 &
SINK_PID=$!

sleep 0.3
echo "[demo] starting lossy dual-path source (with a 10-packet burst at seq 60)"
python3 -u -m demo.demo_source --host 127.0.0.1 \
  --port-a "$PORT_A" --port-b "$PORT_B" \
  --duration 3.5 --packets-per-sec 50 \
  --loss-a 10,20,30 --loss-b 15,30 \
  --late-b 10:0.04,40:0.04 \
  --silence-after 50 --silence-ms 300 \
  --burst-at 60 --burst-packets 10 \
  >"$LOG_DIR/source.log" 2>&1

# Block until the sink finishes reporting; cleanup() then stops the relay.
wait "$SINK_PID"

echo
echo "================ sink report ================"
cat "$LOG_DIR/sink.log"
echo "================ relay /skipped =============="
curl -s "http://127.0.0.1:$API_PORT/skipped"; echo
echo "================ relay /status ==============="
curl -s "http://127.0.0.1:$API_PORT/status"; echo
echo "================ source log =================="
cat "$LOG_DIR/source.log"
echo "================ relay log tail =============="
tail -n 5 "$LOG_DIR/relay.log"
