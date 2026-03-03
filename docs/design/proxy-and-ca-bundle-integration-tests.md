# Design: Proxy and CA Bundle Integration Tests

## Overview

Run **existing integration tests** through an HTTP CONNECT proxy inside a Linux network
namespace. This verifies that the deadline-cloud package correctly respects `HTTPS_PROXY`
and `AWS_CA_BUNDLE` environment variables — any code that bypasses the proxy fails
immediately with "Network is unreachable" because no other route to the internet exists.

## Problem Statement

The deadline-cloud package relies on boto3 for AWS API calls, which should respect standard
AWS SDK environment variables like `AWS_CA_BUNDLE` and `HTTPS_PROXY`. However, there are
currently no automated tests to verify:

1. That these environment variables are properly honored across all AWS API calls
2. That new code additions continue to respect these settings
3. That the package works correctly in environments requiring custom CA certificates and proxies

Without these tests, regressions could be introduced where new functionality bypasses proxy
settings or fails to use custom CA bundles, breaking deployments in corporate or restricted
network environments.

## Goals

1. **Reuse existing integration tests** — no new test code, just infrastructure
2. **Linux-only** — Mac/Windows run tests normally without proxy
3. **Make HTTPS_PROXY bypass impossible** — network namespace has no internet routes
4. **Verify CA bundle usage** — `AWS_CA_BUNDLE` points to the system CA bundle
5. **Fail fast on regressions** — direct connections fail instantly
6. **CI/CD integration** — runs automatically in GitHub Actions

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Host Network Namespace (has internet)                  │
│                                                         │
│  ┌─────────────────────────┐                            │
│  │ Python CONNECT Proxy    │                            │
│  │ listening on Unix socket│──── connects to ──── AWS   │
│  │ /tmp/deadline_proxy.sock│      real internet         │
│  └────────────▲────────────┘                            │
│               │                                         │
│  ┌────────────┴────────────┐                            │
│  │ socat (cred bridge)     │                            │
│  │ Unix socket ──── TCP ───┼──── 127.0.0.1:1338        │
│  │ /tmp/deadline_cred.sock │     (credential endpoint)  │
│  └────────────▲────────────┘                            │
│               │ (unix sockets cross namespace           │
│               │  boundary via shared filesystem)        │
├───────────────┼─────────────────────────────────────────┤
│  Network Namespace (no internet, loopback only)         │
│               │                                         │
│  ┌────────────┴────────────┐  ┌────────────────────┐   │
│  │ socat (proxy bridge)    │  │ socat (cred bridge) │   │
│  │ TCP 127.0.0.1:8888  ───┘  │ TCP 127.0.0.1:1338 ─┘  │
│  └────────────▲────────────┘  └─────────▲──────────┘   │
│               │                         │               │
│  ┌────────────┴─────────────────────────┴──────────┐   │
│  │ pytest (integration tests + boto3 calls)        │   │
│  │  HTTPS_PROXY=http://127.0.0.1:8888             │   │
│  │  AWS creds refresh via 127.0.0.1:1338          │   │
│  └─────────────────────────────────────────────────┘   │
│                                                         │
│  ❌ Direct connections to internet → "Network is        │
│     unreachable" (no routes exist in this namespace)    │
└─────────────────────────────────────────────────────────┘
```

## How It Works

1. **Host side**: A lightweight Python HTTP CONNECT proxy starts, listening on a **Unix domain
   socket** (`/tmp/deadline_proxy.sock`). It runs in the host network namespace and has full
   internet access to forward requests to AWS.

2. **Namespace creation**: `sudo unshare --net` creates a new network namespace. Inside this
   namespace only the loopback interface exists — there are no routes to the internet.

3. **Bridge**: `socat` runs inside the namespace, listening on `TCP 127.0.0.1:8888` and
   forwarding connections to the Unix socket. The Unix socket crosses the namespace boundary
   because the filesystem is shared.

4. **Environment**: `HTTPS_PROXY=http://127.0.0.1:8888` and `AWS_CA_BUNDLE` are set.
   boto3/botocore will use the proxy for all HTTPS connections.

5. **Enforcement**: If any code bypasses `HTTPS_PROXY` and tries to connect directly to AWS
   on port 443, it gets `OSError: [Errno 101] Network is unreachable` — the test fails
   immediately.

6. **Pre-flight checks**: Before running tests, the script verifies that direct internet
   access is blocked (connect to `1.1.1.1:443` must fail) and the proxy is reachable
   (connect to `127.0.0.1:8888` must succeed).

### Key Insight: Unix Sockets Cross Namespace Boundaries

`unshare --net` only isolates the *network* namespace. The filesystem is shared. This means
a Unix domain socket created in the host namespace is accessible from inside the network
namespace — this is how the proxy bridge works without any complex networking setup.

### AWS Credentials

OIDC credentials from `configure-aws-credentials` are short-lived and may need refreshing
via a local credential endpoint on loopback. Since the network namespace has its own loopback,
credential endpoints on the host's `127.0.0.1` are NOT accessible from inside the namespace.

We bridge credential endpoints using additional socat instances. On the host side, socat
forwards from a Unix socket to the real TCP endpoint. Inside the namespace, socat listens
on the same TCP port and forwards to that Unix socket:

```bash
# Host side: TCP credential endpoint → Unix socket
socat UNIX-LISTEN:/tmp/deadline_cred_bridge.sock,fork TCP:127.0.0.1:${CRED_PORT} &

# Inside namespace: Unix socket → TCP on namespace loopback
socat TCP-LISTEN:${CRED_PORT},fork,reuseaddr,bind=127.0.0.1 \
      UNIX-CONNECT:/tmp/deadline_cred_bridge.sock &
```

### CA Bundle Verification

Since the CONNECT proxy is a tunneling proxy (not MITM), TLS verification happens directly
between boto3 and AWS through the tunnel. We set `AWS_CA_BUNDLE` to the system CA bundle
(`/etc/ssl/certs/ca-certificates.crt`). This proves the env var is respected — if boto3
ignored it and used no CA bundle, TLS would fail.

## File Structure

```
scripts/
└── run_proxy_integ_tests.sh           # Self-contained proxy test wrapper

test/integ/
├── conftest.py                        # Session fixtures (job templates, etc.)
└── proxy_ca_bundle/
    ├── __init__.py
    └── connect_proxy.py               # Python HTTP CONNECT proxy (unix socket)
```

## Implementation Details

### CONNECT Proxy (`connect_proxy.py`)

A minimal (~100 line) Python HTTP CONNECT proxy that:
- Listens on a Unix domain socket
- Accepts `CONNECT host:port` requests
- Opens a TCP connection to the target (using host network)
- Relays bytes bidirectionally using `select()`
- Uses blocking `sendall()` for reliable data transfer

No TLS termination, no certificate generation, no MITM.

### Namespace Runner (`scripts/run_proxy_integ_tests.sh`)

Self-contained wrapper script that any CI system can call:
1. Starts the CONNECT proxy on a Unix socket (host namespace)
2. Bridges credential endpoints via socat (host side)
3. Creates a network namespace with `unshare --net`
4. Inside the namespace: starts socat bridges, runs pre-flight checks, runs pytest
5. Prints proxy report after tests complete

Can be invoked directly or via hatch:
```bash
# Via hatch
hatch run integ:proxy-test

# Directly
sudo -E env "PATH=$PATH" bash scripts/run_proxy_integ_tests.sh [pytest args...]
```

### GitHub Actions / CodeBuild Integration

No special workflow changes needed. CI systems just call the wrapper script
instead of (or in addition to) the normal `hatch run integ:test`. The script
is self-contained — it handles proxy startup, namespace creation, bridging,
and cleanup.

## Known Behaviors

### socat "Connection reset by peer" Messages

During test runs you may see socat log lines like:
```
socat[3325] E read(6, 0x55eda2826000, 8192): Connection reset by peer
```

These are **harmless and expected**. When a TLS connection through the proxy completes, one
side closes the TCP connection. The socat bridge tries one more read on the now-closed socket
and gets `ECONNRESET`. The actual data transfer completed successfully before the reset —
socat just logs socket cleanup at error level by default.

## Dependencies

### System packages (installed in CI):
- `socat` — TCP-to-Unix-socket bridge

### Python packages:
- None beyond what's already in `requirements-integ-testing.txt`

## Platform Behavior

- **Linux (CI)**: Tests run inside network namespace, proxy enforced
- **Mac/Windows**: Proxy testing not applicable, tests run normally

## Running Tests

### In CI (CodeBuild / GitHub Actions)
CI calls the wrapper script which handles everything:
```bash
sudo -E env "PATH=$PATH" bash scripts/run_proxy_integ_tests.sh test/integ/cli/
```

### Locally on Linux
```bash
# Install socat
sudo apt-get install -y socat

# Run with proxy enforcement
hatch run integ:proxy-test

# Or run without proxy
hatch run integ:test
```

### Mac/Windows
```bash
# No proxy testing — run tests normally
hatch run integ:test
```

## Success Criteria

1. ✅ All existing integration tests pass through the proxy inside the namespace
2. ✅ Any code that bypasses HTTPS_PROXY fails immediately with "Network is unreachable"
3. ✅ Pre-flight checks verify isolation before tests run
4. ✅ Tests run normally on Mac/Windows without proxy
5. ✅ Tests run reliably in GitHub Actions without timeouts or resource exhaustion
6. ✅ Cleanup is automatic (namespace destroyed when unshare exits)

## Limitations

1. **Linux-only** — network namespaces are a Linux kernel feature
2. **Requires sudo** — `unshare --net` needs `CAP_SYS_ADMIN` on GitHub Actions runners
3. **Requires socat** — must be installed (trivial via apt)
4. **Loopback services must be bridged** — any host loopback endpoint the tests need
   (credential endpoints, metadata services) must have a socat bridge pair
