#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Run integration tests through an HTTP CONNECT proxy inside a network namespace.
# This verifies HTTPS_PROXY and AWS_CA_BUNDLE are respected by all AWS SDK calls.
#
# Usage:
#   sudo -E env "PATH=$PATH" bash scripts/run_proxy_integ_tests.sh [pytest args...]
#
# Prerequisites:
#   - Linux with socat installed (sudo apt-get install -y socat)
#   - AWS credentials configured
#   - pip install -e . && pip install -r requirements-integ-testing.txt
#
# The script:
#   1. Starts a CONNECT proxy on a Unix socket (host namespace)
#   2. Bridges credential endpoints via socat (host → Unix socket)
#   3. Creates a network namespace with unshare --net
#   4. Inside the namespace: socat bridges, pre-flight checks, pytest
#   5. Prints proxy report (per-host connection counts, bytes relayed)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROXY_SOCK="/tmp/deadline_proxy.sock"
CRED_SOCK="/tmp/deadline_cred_bridge.sock"
PIDS_TO_KILL=()

cleanup() {
    for pid in "${PIDS_TO_KILL[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

# --- Host side setup ---

# Start CONNECT proxy
python3 "$SCRIPT_DIR/test/integ/proxy_ca_bundle/connect_proxy.py" "$PROXY_SOCK" &
PIDS_TO_KILL+=($!)
sleep 1

# Bridge credential endpoint if present
if [ -n "${AWS_CONTAINER_CREDENTIALS_FULL_URI:-}" ]; then
    CRED_PORT=$(echo "$AWS_CONTAINER_CREDENTIALS_FULL_URI" | sed -n 's|.*://[^:]*:\([0-9]*\).*|\1|p')
    if [ -n "$CRED_PORT" ]; then
        socat UNIX-LISTEN:"$CRED_SOCK",fork TCP:127.0.0.1:"${CRED_PORT}" &
        PIDS_TO_KILL+=($!)
    fi
fi

# --- Run tests in network namespace ---

PYTEST_ARGS="${*:-test/integ/cli/}"

unshare --net /bin/bash -c "
    set -euo pipefail
    cd '$SCRIPT_DIR'

    # Bring up loopback
    ip link set lo up

    # Bridge: proxy TCP 8888 -> Unix socket
    socat TCP-LISTEN:8888,fork,reuseaddr,bind=127.0.0.1 UNIX-CONNECT:$PROXY_SOCK &

    # Bridge: credential endpoint
    if [ -n \"\${AWS_CONTAINER_CREDENTIALS_FULL_URI:-}\" ]; then
        CRED_PORT=\$(echo \"\$AWS_CONTAINER_CREDENTIALS_FULL_URI\" | sed -n 's|.*://[^:]*:\([0-9]*\).*|\1|p')
        if [ -n \"\$CRED_PORT\" ]; then
            socat TCP-LISTEN:\${CRED_PORT},fork,reuseaddr,bind=127.0.0.1 UNIX-CONNECT:$CRED_SOCK &
        fi
    fi

    sleep 0.5

    # Pre-flight: verify namespace isolation
    python3 -c \"
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(3)
try:
    s.connect(('1.1.1.1', 443))
    print('ERROR: Direct internet access possible — namespace isolation broken!')
    sys.exit(1)
except OSError:
    print('OK: Direct internet access blocked')
finally:
    s.close()
\"

    python3 -c \"
import socket, sys
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(3)
try:
    s.connect(('127.0.0.1', 8888))
    print('OK: Proxy reachable on 127.0.0.1:8888')
except OSError as e:
    print(f'ERROR: Cannot reach proxy: {e}')
    sys.exit(1)
finally:
    s.close()
\"

    export HTTPS_PROXY=http://127.0.0.1:8888
    export HTTP_PROXY=http://127.0.0.1:8888
    export AWS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

    python3 -m pytest --no-cov -vvv -s $PYTEST_ARGS --tb=short
"
TEST_RC=$?

# --- Proxy report ---

python3 -c "
import socket, json, sys
try:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect('${PROXY_SOCK/.sock/_stats.sock}')
    data = s.recv(4096)
    s.close()
    stats = json.loads(data)
    print('=' * 60)
    print('PROXY REPORT')
    print('=' * 60)
    print(f'Connections proxied: {stats[\"connection_count\"]}')
    print(f'Bytes relayed:       {stats[\"bytes_relayed\"]}')
    print(f'Hosts connected to:  {len(stats[\"hosts\"])}')
    for host, count in sorted(stats['hosts'].items(), key=lambda x: -x[1]):
        print(f'  {host}: {count} connections')
    print('=' * 60)
    if stats['connection_count'] == 0:
        print('ERROR: No connections went through proxy!')
        sys.exit(1)
except Exception as e:
    print(f'Could not read proxy stats: {e}')
"

exit $TEST_RC
