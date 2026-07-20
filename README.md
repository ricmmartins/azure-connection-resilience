# Azure Connection Resilience Monitor

> Reduce TCP connection recovery time from **~15 minutes to <20 seconds** during Azure live migration events.

## The Problem

When Azure performs live migrations on VMs behind a **Standard Load Balancer**, active TCP connections enter a "black hole" state. Unlike AWS NLB (which sends TCP RST by default), Azure's Standard LB silently drops in-flight packets during the migration window. Applications with long-lived persistent connections (database pools, message queues, gRPC streams) stall for up to 15 minutes waiting for TCP retransmission timeout to expire.

**Impact:** Application-level timeouts cascade, causing visible downtime even though the VM itself recovers in seconds.

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                    Azure Connection Resilience Monitor           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┐    ┌──────────────────┐                  │
│  │  Scheduled Events │    │   App Notifier   │                  │
│  │  Monitor (IMDS)  │───▶│  (webhook/signal) │──▶ App drains   │
│  └──────────────────┘    └──────────────────┘    connections    │
│                                                                 │
│  ┌──────────────────┐    ┌──────────────────┐                  │
│  │  TCP Tuning      │    │  Metrics/Logging │                  │
│  │  (on startup)    │    │  (Prometheus)    │                  │
│  └──────────────────┘    └──────────────────┘                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Component | What it does | Privilege needed |
|-----------|-------------|-----------------|
| **Scheduled Events Monitor** | Polls Azure IMDS every 5s for upcoming maintenance. Gives 5-15 min advance warning. | None (localhost HTTP) |
| **App Notifier** | Calls your app's drain endpoint or sends SIGUSR1 when maintenance detected. App reconnects gracefully. | None |
| **TCP Tuning** | On startup, applies optimal `sysctl` settings for faster dead-connection detection. | `CAP_NET_ADMIN` (one-time) |
| **Metrics** | Exposes Prometheus metrics: events detected, drains triggered, recovery time. | None |

### What Your App Must Do

The monitor detects the event. **Your app must handle the reconnection.** This is by design: only the application knows which connections matter and how to drain gracefully.

Your app exposes one of:
- **HTTP endpoint** (e.g., `POST /drain-connections`) — monitor calls it
- **Signal handler** (e.g., `SIGUSR1`) — monitor sends the signal
- **File watch** (e.g., `/tmp/azure-drain-trigger`) — monitor writes the file

When triggered, your app should:
1. Close idle connections in the pool
2. Mark active connections for reconnection after current operation completes
3. Open new connections (these will route to healthy backends)

## Quick Start

### 1. Run the monitor

```bash
# Docker sidecar (recommended)
docker run -d --name azure-resilience \
  --network host \
  -e NOTIFY_URL=http://localhost:8080/drain-connections \
  -e NOTIFY_METHOD=POST \
  ghcr.io/ricmmartins/azure-connection-resilience:latest

# Or directly
pip install -r requirements.txt
python monitor.py --notify-url http://localhost:8080/drain-connections
```

### 2. Add a drain endpoint to your app

```python
# Flask example
@app.route('/drain-connections', methods=['POST'])
def drain():
    # Close database connection pool
    db_pool.close_idle_connections()
    db_pool.mark_active_for_reconnect()
    app.logger.info("Connections drained due to Azure maintenance event")
    return {"status": "drained"}, 200
```

### 3. Apply TCP tuning (optional, one-time)

```bash
sudo ./tcp_tuning.sh
```

## Couchbase-Specific Integration

For Couchbase SDK clients connecting through Azure LB:

```python
from azure_resilience import CouchbaseAdapter

# Wraps your Couchbase cluster connection with resilience
adapter = CouchbaseAdapter(
    cluster=cluster,
    monitor_url="http://localhost:9090"  # monitor's API
)
adapter.start()  # begins watching for drain signals
```

The adapter:
- Registers with the monitor for drain notifications
- On notification: gracefully disconnects bucket connections
- Reconnects automatically with exponential backoff
- Logs recovery time for observability

### Couchbase SDK Settings (Recommended)

```json
{
  "io": {
    "tcpKeepAliveEnabled": true,
    "tcpKeepAliveTime": "15s",
    "configPollInterval": "5s"
  },
  "timeout": {
    "connectTimeout": "10s",
    "kvTimeout": "5s"
  }
}
```

## TCP Tuning Reference

These kernel parameters dramatically reduce black-hole detection time:

| Parameter | Default | Recommended | Effect |
|-----------|---------|-------------|--------|
| `tcp_keepalive_time` | 7200s | 15s | Start probing after 15s idle |
| `tcp_keepalive_intvl` | 75s | 5s | Probe every 5s once started |
| `tcp_keepalive_probes` | 9 | 3 | Give up after 3 failed probes |
| `TCP_USER_TIMEOUT` (per-socket) | 0 (disabled) | 20000ms | Hard deadline: if unACKed data sits for 20s, kill connection |

**Critical:** `TCP_USER_TIMEOUT` is the most important setting. It must be set **per-socket** by the application (not a sysctl). This is what turns a 15-minute stall into a 20-second detection.

```c
// C/C++ — set on the socket
int timeout = 20000; // 20 seconds in milliseconds
setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &timeout, sizeof(timeout));
```

```java
// Java — set via extended socket options (Java 11+)
socket.setOption(ExtendedSocketOptions.TCP_USER_TIMEOUT, 20000);
```

```python
# Python — set via socket option
import socket
TCP_USER_TIMEOUT = 18  # option number on Linux
sock.setsockopt(socket.IPPROTO_TCP, TCP_USER_TIMEOUT, 20000)
```

## Architecture Decision: Why Not Kill Connections Externally?

We considered using `ss -K` or iptables RST injection to kill stale connections from outside the app. We chose **not** to because:

1. **Blast radius** — wrong match criteria can kill healthy connections
2. **Privilege escalation** — requires root/CAP_NET_ADMIN on the app's network namespace
3. **Race conditions** — connection might recover naturally milliseconds before the kill
4. **App doesn't know** — externally killed connections look like network failures, not graceful drains

The app-notified approach is safer, more observable, and produces cleaner recovery behavior.

## Metrics

The monitor exposes Prometheus metrics on port 9090:

```
# Maintenance events detected
azure_resilience_events_total{type="Freeze|Reboot|Redeploy"} 

# Drain notifications sent
azure_resilience_drains_total{result="success|failed"}

# Time from event detection to app recovery
azure_resilience_recovery_seconds{quantile="0.5|0.9|0.99"}

# Current monitor status
azure_resilience_monitor_up 1
```

## Demo: Proving the Recovery Time

See [`demo/`](./demo/) for a failure injection script that:
1. Simulates a maintenance event via IMDS mock
2. Shows connection behavior WITHOUT the monitor (15-min timeout)
3. Shows connection behavior WITH the monitor (<20s recovery)

```bash
cd demo/
./run_demo.sh
```

## Deployment Options

| Mode | Best for | How |
|------|----------|-----|
| **Docker sidecar** | Kubernetes, AKS | Add container to pod spec |
| **systemd service** | VM-based workloads | `sudo ./install.sh` |
| **Azure VM Extension** | Fleet-wide rollout | ARM template included |

## FAQ

**Q: Does this work for unplanned host failures (crashes)?**  
A: No. Scheduled Events only covers planned migrations. For unplanned failures, `TCP_USER_TIMEOUT` is your safety net (detects the black hole in 20s instead of 15min).

**Q: Why not just use VNet Peering (remove the LB)?**  
A: VNet Peering is the ideal solution (removes LB entirely). This tool is for cases where you MUST go through a load balancer (e.g., Private Link, security requirements).

**Q: Does Azure plan to fix this?**  
A: This has been raised with the Azure Load Balancer Product Group as a competitive gap (AWS NLB sends RST by default). No ETA on a platform-level fix.

**Q: What about Application Gateway?**  
A: Application Gateway operates at L7 and has its own connection management. This tool targets L4 (Standard LB) scenarios.

## Requirements

- Python 3.9+
- Linux (uses IMDS and TCP socket options)
- Azure VM or VMSS (for Scheduled Events access)
- Network access to your app's drain endpoint

## License

MIT
