# Azure TCP Connection Resilience Pattern

> **⚠️ This is a proof-of-concept / reference implementation.**
> It demonstrates a node-agent integration pattern for proactive connection draining during Azure planned maintenance. It does NOT replace the platform-level fix being worked by the Azure Load Balancer product group. Validate thoroughly in non-production environments before any production consideration. Provided as-is, no warranties or support guarantees.

## The Problem

When Azure performs **live migrations** (Freeze events) on VM hosts behind a Standard Load Balancer, existing TCP connections can enter a "black hole" state. The backend VM moves to a new host, but the LB does **not** send a TCP RST for active connections. The client has no signal that the connection is dead.

With default Linux TCP settings (`tcp_retries2=15`), the kernel retransmits with exponential backoff for **up to 15 minutes** before declaring the connection failed.

**Competitive context:** AWS NLB sends TCP RST for active connections during live migration by default. Azure Standard LB does not. This is a known platform gap being addressed by the Azure LB product group.

## The Solution: Two Layers

This pattern provides **zero-second recovery** for planned maintenance and **~30s recovery** for unplanned failures:

| Scenario | Mechanism | Recovery Time | Coverage |
|----------|-----------|---------------|----------|
| Planned live migration (Freeze) | Proactive drain via Scheduled Events | **0 seconds** | Majority of cases |
| Predictive hardware failure | Scheduled migration (days notice) | **0 seconds** | Covered |
| Sudden hardware failure | TCP keepalive + TCP_USER_TIMEOUT | **~30 seconds** | Fallback |

### Why 0 seconds for planned maintenance?

Azure's [Scheduled Events API](https://learn.microsoft.com/en-us/azure/virtual-machines/linux/scheduled-events) provides **minimum 15 minutes advance notice** for Freeze events ([source](https://learn.microsoft.com/en-us/azure/virtual-machines/linux/scheduled-events#event-scheduling)). The event is **guaranteed not to start before the `NotBefore` time**.

This gives your node agent a full 15-minute window to:
1. Detect the upcoming Freeze
2. Drain active connections gracefully
3. Establish new connections (routed through healthy paths)
4. The Freeze happens with zero active connections impacted

## Architecture

This is designed as a **node-agent integration pattern**, not a standalone application. The reference implementation shows the mechanics; in production, this logic integrates into your existing VM orchestration agent.

```
┌─────────────────────────────────────────────────────────────┐
│  VM (each Couchbase node = 1 VMSS instance)                 │
│                                                             │
│  ┌─────────────────────┐      ┌──────────────────────────┐ │
│  │  Resilience Monitor │      │  Your Node Agent /       │ │
│  │  (this pattern)     │─────►│  Application Process     │ │
│  │                     │ drain│                          │ │
│  │  • Polls IMDS 1/sec │signal│  • Closes idle conns     │ │
│  │  • Detects Freeze   │      │  • Waits for in-flight   │ │
│  │  • Filters by VM    │      │  • Force-closes after Ns │ │
│  │  • Emits metrics    │      │  • Reconnects fresh      │ │
│  └─────────┬───────────┘      └──────────────────────────┘ │
│            │                                                │
│  ┌─────────▼───────────┐                                   │
│  │  Azure IMDS         │  (169.254.169.254, always local)  │
│  │  /scheduledevents   │                                   │
│  └─────────────────────┘                                   │
└─────────────────────────────────────────────────────────────┘
```

## Event Coverage Matrix

| Event Type | Advance Notice | This Pattern Helps? | Notes |
|------------|---------------|--------------------:|-------|
| `Freeze` (live migration) | 15 min minimum | ✅ **Yes — 0s impact** | Primary use case |
| `Reboot` (planned) | 15 min minimum | ✅ Yes | Drain before reboot |
| `Redeploy` (planned) | 10 min minimum | ✅ Yes | Drain before move |
| `Preempt` (Spot VM) | 30 seconds | ⚠️ Partial | Very tight window |
| Hardware failure (sudden) | None (`Started` immediately) | ❌ No proactive drain | TCP_USER_TIMEOUT fallback only |
| Service heal (post-failure) | None | ❌ No proactive drain | TCP_USER_TIMEOUT fallback only |

> **Key insight:** The 15-minute stalls Staples experiences are from planned live migrations. These ARE covered by Scheduled Events with 15 min notice. The proactive drain eliminates this entirely.

## Quick Start

```bash
# Option 1: Run as host daemon
python monitor.py --notify-url http://127.0.0.1:8099/drain --verbose

# Option 2: systemd service (production)
sudo ./install.sh

# Option 3: Docker (for containerized workloads)
docker build -t azure-resilience .
docker run -d --network=host azure-resilience
```

## Demo (Local Testing)

```bash
# Terminal 1: Mock application with drain endpoint
python demo/mock_app.py

# Terminal 2: Mock IMDS (injects Freeze event after 10s)
python demo/mock_imds.py --trigger-after 10

# Terminal 3: Monitor (polls mock IMDS, notifies mock app)
python monitor.py --imds-url http://localhost:8169 \
                  --notify-url http://127.0.0.1:8099/drain \
                  --poll-interval 1 --verbose
```

Watch Terminal 1: the app receives the drain signal and reconnects **before** the maintenance event fires.

## Deployment Model

### For VM-based stateful services (like Couchbase Capella)

Each VM node runs the monitor as a **systemd service** or as a module within your existing node orchestration agent. This is NOT a Kubernetes sidecar pattern.

```
Per-VM deployment:
  /opt/azure-resilience/monitor.py    ← polls IMDS
  /opt/azure-resilience/config.yaml   ← notification config
  systemd: azure-resilience-monitor.service
```

The monitor communicates with your application via **loopback-only** HTTP (127.0.0.1), Unix domain socket, or Unix signal. The drain endpoint is never exposed externally.

### Integration with existing node agents

If you already have a node agent (health checks, orchestration, metrics), the recommended path is to embed the Scheduled Events polling logic directly:

```python
# ~50 lines of integration code in your existing agent
import urllib.request, json

IMDS_URL = "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01"

def check_scheduled_events(seen_events: set) -> list:
    """Poll IMDS. Returns new Freeze/Reboot events not yet seen."""
    req = urllib.request.Request(IMDS_URL, headers={"Metadata": "true"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        data = json.loads(resp.read())
    
    new_events = []
    for event in data.get("Events", []):
        if event["EventId"] not in seen_events:
            if event["EventType"] in ("Freeze", "Reboot", "Redeploy"):
                # Filter: only react if THIS VM is in Resources list
                if MY_VM_NAME in event.get("Resources", []):
                    seen_events.add(event["EventId"])
                    new_events.append(event)
    return new_events
```

## TCP Tuning (Fallback Layer)

For unplanned failures where no Scheduled Event is emitted, aggressive TCP settings provide the safety net:

| Setting | Default | Recommended | Detection Time | Notes |
|---------|---------|-------------|----------------|-------|
| `tcp_keepalive_time` | 7200s | 30s | — | First probe after 30s idle. Requires `SO_KEEPALIVE` enabled by app. |
| `tcp_keepalive_intvl` | 75s | 10s | — | Interval between probes |
| `tcp_keepalive_probes` | 9 | 3 | **~60s total** | Probes before declaring dead |
| `TCP_USER_TIMEOUT` | none | 20000ms | **20s** | Per-socket. Must be set by application. Most important single setting. |

**Important:** Keepalive only detects dead connections on **idle** sockets (no data in flight). `TCP_USER_TIMEOUT` is the setting that catches active connections with unacknowledged data. It must be set per-socket by the application.

```bash
# Check current settings
./tcp_tuning.sh check

# Apply recommended values (runtime only)
sudo ./tcp_tuning.sh apply

# Apply and persist across reboots
sudo ./tcp_tuning.sh persist
```

## Security Model

The drain endpoint is a sensitive control plane surface. Unauthorized access could trigger connection resets (self-DoS).

**Requirements:**
- Drain listener binds to `127.0.0.1` only (never `0.0.0.0`)
- Optional auth token via `Authorization` header
- Drain is **idempotent per EventId** (duplicate signals are no-ops)
- Rate limiting: max 1 drain per 60 seconds (prevents drain storms)
- All communication is local to the VM (no network exposure)

## Drain State Machine

The drain flow follows a strict state machine to prevent deadlocks:

```
NORMAL → DRAINING → FORCE_CLOSE → RECONNECTING → NORMAL
         │                                         ▲
         │         (bounded timeout)               │
         └─────────────────────────────────────────┘
                   (on failure: backoff retry)
```

1. **NORMAL**: Accepting traffic, polling IMDS every 1s
2. **DRAINING**: Event detected. Stop new work. Wait up to `drain_timeout` (default: 5s) for in-flight ops.
3. **FORCE_CLOSE**: If ops still pending after timeout, force-close all sockets. Do not wait indefinitely.
4. **RECONNECTING**: Open new connections with exponential backoff (1s, 2s, 4s... cap 30s).
5. **NORMAL**: Health check passes on new connections. Resume traffic.

> **Critical:** Never wait unbounded for in-flight operations. A hung operation on a black-holed socket will deadlock the entire drain. The force-close after `drain_timeout` is mandatory.

## Failure Modes

| Failure | Impact | Mitigation |
|---------|--------|------------|
| IMDS unreachable | No proactive detection | Alert on consecutive failures. TCP tuning is the fallback. IMDS is local (169.254.169.254), almost never fails. |
| Event missed (poll gap) | Maintenance hits before drain | Poll at 1/sec per Microsoft guidance. Even 5s gap still leaves 14:55 of the 15-min window. |
| App unresponsive to drain | Connections stay up during freeze | Force-close after timeout. Alert operator. |
| Reconnect storm (all nodes drain simultaneously) | Thundering herd on remaining backends | Jitter on reconnect delay. Stagger by node ID. |
| False positive (event cancelled) | Unnecessary reconnection | Monitor event removal from array. Log as no-op. Low cost (one reconnect cycle). |
| Approval race (multi-VM) | One VM approves, shortens window for peers | Do NOT auto-approve events. Filter by `Resources`. Each VM handles its own drain independently. |

## Couchbase-Specific Integration

For Couchbase's architecture (Private Link → ILB → VMSS, 1 node per VM, memcached binary protocol, smart client SDK):

**The monitor runs on the Couchbase SERVER VM.** Full lifecycle:

1. IMDS announces Freeze (15 min advance notice)
2. Monitor sends drain signal to local adapter
3. Adapter calls `startGracefulFailover` on the local Couchbase node
4. SDK smart clients discover topology change, route operations to other nodes
5. In-flight operations complete (bounded 5s timeout)
6. Node is quiesced with zero active connections
7. Azure Freeze happens with zero customer impact (typically ≤5s per Microsoft docs)
8. VM resumes, Couchbase process is still running
9. Adapter calls `setRecoveryType(delta)` to preserve on-disk data
10. Adapter triggers rebalance to bring node back into the cluster
11. Node rejoins, SDK clients resume routing operations here

**What Couchbase's node agent would implement** (pseudocode, adapt to your agent framework):

```python
import requests
import time
import threading

COUCHBASE_ADMIN = "http://127.0.0.1:8091"
AUTH = ("admin", "password")
OTP_NODE = "ns_1@127.0.0.1"

def handle_drain(event_data, drain_timeout=5.0):
    """PRE-FREEZE: Called when monitor detects a Freeze event on this VM.
    NOTE: startGracefulFailover applies to Data Service nodes.
    """

    # Step 1: Tell Couchbase Server this node is going away.
    # SDK clients will discover topology change and route elsewhere.
    requests.post(
        f"{COUCHBASE_ADMIN}/controller/startGracefulFailover",
        auth=AUTH,
        data={"otpNode": OTP_NODE}
    )

    # Step 2: Wait (bounded) for failover to actually complete.
    # Gate: rebalanceStatus == "none" AND node is "inactiveFailed"
    deadline = time.monotonic() + drain_timeout
    while time.monotonic() < deadline:
        resp = requests.get(f"{COUCHBASE_ADMIN}/pools/default", auth=AUTH).json()
        rebalance_done = resp.get("rebalanceStatus") == "none"
        node_failed_over = any(
            n.get("clusterMembership") == "inactiveFailed"
            for n in resp.get("nodes", [])
            if n.get("otpNode") == OTP_NODE
        )
        if rebalance_done and node_failed_over:
            break
        time.sleep(0.5)

    # Step 3: Node is quiesced. Freeze can proceed safely.
    # Step 4: Schedule post-freeze recovery.
    # Pass the EventId so recovery knows what to wait for.
    event_id = event_data.get("EventId")
    threading.Thread(target=handle_recovery, args=(event_id,), daemon=True).start()

def handle_recovery(event_id):
    """POST-FREEZE: Rejoin the cluster after the freeze event completes.

    CRITICAL: The freeze hasn't happened yet when this thread starts!
    The 15-min window is advance NOTICE. We must wait for the event to
    actually complete (disappear from IMDS) before rejoining.
    """

    # Wait for the freeze event to COMPLETE.
    # When an event finishes, it's removed from the IMDS events array.
    # Poll until our EventId is no longer present.
    IMDS_URL = "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01"
    while True:
        try:
            resp = requests.get(
                IMDS_URL, headers={"Metadata": "true"}, timeout=5
            ).json()
            event_ids = [e.get("EventId") for e in resp.get("Events", [])]
            if event_id not in event_ids:
                break  # Event completed and was removed
        except Exception:
            pass  # IMDS unreachable during freeze itself; keep trying
        time.sleep(2)

    # Verify Couchbase service is healthy after freeze
    for _ in range(30):
        try:
            if requests.get(f"{COUCHBASE_ADMIN}/pools/default",
                           auth=AUTH, timeout=2).status_code == 200:
                break
        except Exception:
            pass
        time.sleep(1)

    # Verify no conflicting rebalance is running
    resp = requests.get(f"{COUCHBASE_ADMIN}/pools/default", auth=AUTH).json()
    if resp.get("rebalanceStatus") != "none":
        return  # Another operation in progress

    # Step 5: Delta recovery preserves data already on disk (fast rejoin)
    requests.post(
        f"{COUCHBASE_ADMIN}/controller/setRecoveryType",
        auth=AUTH,
        data={"otpNode": OTP_NODE, "recoveryType": "delta"}
    )

    # Step 6: Trigger rebalance to bring node back
    resp = requests.get(f"{COUCHBASE_ADMIN}/pools/default", auth=AUTH).json()
    known_nodes = [n["otpNode"] for n in resp.get("nodes", [])]
    requests.post(
        f"{COUCHBASE_ADMIN}/controller/rebalance",
        auth=AUTH,
        data={"knownNodes": ",".join(known_nodes)}
    )
```

**Key points for Couchbase engineering:**
- This is SERVER-SIDE, not client-side. No customer code changes needed.
- `startGracefulFailover` applies to **Data Service nodes**. For Index/Query/FTS-only nodes, the drain mechanism may differ (though those services are typically stateless from the connection perspective).
- Memcached binary protocol is irrelevant here: drain happens via Couchbase's REST admin API (port 8091), not at the memcached layer.
- Smart client SDK handles topology discovery automatically. `startGracefulFailover` updates the cluster map, SDK reroutes.
- Delta recovery preserves data on disk. Node rejoins in seconds, not minutes.
- Each VMSS instance runs its own monitor. No coordination needed between nodes.
- The 15-minute window from Scheduled Events gives plenty of time for graceful failover + recovery scheduling.
- TCP_USER_TIMEOUT (20s, already in your SDK) remains the fallback for unplanned failures where no Scheduled Event fires.
- Works with your existing custom node agents (metrics, health, orchestration). The adapter is ~50 LOC of integration logic.

## Files

| File | Purpose |
|------|---------|
| `monitor.py` | Core daemon: IMDS polling (1/sec), event filtering, drain notification, Prometheus metrics |
| `couchbase_adapter.py` | Reference integration: drain listener, bounded state machine, reconnect with backoff |
| `tcp_tuning.sh` | Check/apply/persist/revert TCP kernel settings (fallback layer) |
| `config.yaml` | Configuration reference with security and tuning options |
| `Dockerfile` | Container packaging (for containerized workloads) |
| `install.sh` | systemd service installation (for VM workloads) |
| `demo/mock_imds.py` | Fake IMDS for local testing |
| `demo/mock_app.py` | Fake app with drain endpoint |
| `demo/run_demo.sh` | Narrated simulation showing the event detection flow |

## Non-Goals

This pattern does **not**:
- Replace Azure Load Balancer's responsibility to send TCP RST (that's the platform fix)
- Cover sudden hardware failures with 0-second recovery (TCP_USER_TIMEOUT is the fallback there)
- Require application code changes on the end-customer side (this is operator infrastructure)
- Auto-approve Scheduled Events (approval is out of scope; drain only)
- Work as a general-purpose service mesh or load balancer replacement

## Limitations and Drawbacks

| Limitation | Impact | Mitigation |
|-----------|--------|------------|
| **Only covers planned maintenance** | Unplanned hardware failures give zero IMDS notice. | TCP_USER_TIMEOUT (20s) is the fallback for those. Combined: proactive drain for planned + faster detection for unplanned. |
| **Cluster runs at N-1 during drain/rejoin** | Reduced capacity while node is out. If multiple VMs freeze simultaneously, capacity drops further. | Freeze events are typically staggered by Azure. Monitor for cluster-level capacity alerts. |
| **Post-freeze rejoin is not instant** | Delta recovery + rebalance takes seconds to minutes depending on mutation volume during the freeze window. | Freeze is typically ≤5s (per Microsoft docs). Delta recovery is fast when mutation load is low. |
| **False positives from cancelled events** | Azure can schedule then cancel an event. Node would be failed over unnecessarily. | Cost is one drain/rejoin cycle. Low frequency in practice. Monitor event cancellation rate. |
| **Freeze duration is non-deterministic** | Typically ≤5s but not guaranteed. Recovery that fires before freeze ends could conflict. | Detect VM resume (IMDS availability returning) rather than hard-coded sleep. |
| **Requires engineering integration** | ~50 LOC in the node agent, plus testing and rollout. Not zero-effort. | Integration is straightforward for teams with existing node agents. |
| **No cross-node coordination** | Each node independently fails over. Rack-level maintenance could cascade multiple failovers. | Azure typically staggers maintenance across fault domains. Add cluster-level gate if needed. |
| **Workaround, not a root-cause fix** | The Azure Load Balancer TCP RST gap still exists. No platform-level roadmap ETA for a fix. | This buys time until the platform fix ships. When it does, this becomes optional hardening. |

## FAQ

**Q: Does this eliminate the 15-minute stalls completely?**
A: For planned live migrations (which cause the majority of the stalls): yes, 0 seconds. For sudden hardware failures: reduces to ~30s via TCP tuning. Together, this provides near-complete coverage.

**Q: Why not just set TCP_USER_TIMEOUT to 1 second?**
A: Because transient network blips or brief Azure fabric operations would falsely kill healthy connections. 20s is the sweet spot between fast detection and false positive avoidance. And for planned maintenance, you don't need TCP_USER_TIMEOUT at all because you drain proactively.

**Q: Can this be deployed without modifying the application?**
A: The TCP tuning (sysctl) requires no code changes. The proactive drain requires the application to expose a local drain handler (HTTP endpoint, signal handler, or file watcher). For platforms with existing node agents, this is ~50 lines of integration.

**Q: What about Azure Load Balancer health probes?**
A: Health probes detect backend failures, but existing connections are NOT reset when a backend is removed from the pool. The black hole affects connections that were already established. This is the core platform gap.

**Q: How does this differ from the Couchbase SDK's built-in failover?**
A: The SDK's smart client handles topology changes (node added/removed from cluster map). But a black-holed TCP socket doesn't trigger a topology change. The socket just silently stops working. The SDK can't fail over what it doesn't know is dead. This pattern tells the SDK "drain NOW" 15 minutes before the failure would occur.

**Q: What's the overhead?**
A: One HTTP GET to IMDS every second. IMDS is a VM-local metadata service (169.254.169.254), so there's no network hop. CPU: negligible. Memory: minimal (single-threaded Python daemon).

## Official References

- [Azure IMDS Scheduled Events (Linux)](https://learn.microsoft.com/en-us/azure/virtual-machines/linux/scheduled-events) — The API this pattern uses
- [Event scheduling minimum notice times](https://learn.microsoft.com/en-us/azure/virtual-machines/linux/scheduled-events#event-scheduling) — Freeze: 15 min guaranteed
- [Azure VM maintenance and updates](https://learn.microsoft.com/en-us/azure/virtual-machines/maintenance-and-updates) — Live migration behavior
- [Monitor Scheduled Events](https://learn.microsoft.com/en-us/azure/virtual-machines/windows/scheduled-event-service) — Microsoft's reference implementation for event monitoring
- [Azure LB TCP idle timeout](https://learn.microsoft.com/en-us/azure/load-balancer/load-balancer-tcp-idle-timeout) — Load Balancer TCP behavior

## Status

🟡 **Proof of Concept** — Reference implementation demonstrating the integration pattern. Tested in lab with mock IMDS. Not yet validated under real Azure live migration with packet captures. The Azure LB product group is working on a platform-level fix for the TCP RST gap. This pattern is a bridge for operators who need 0-second impact today.

## Contributing

Issues and PRs welcome. Particularly interested in:
- Adaptations for other database SDKs (Redis, PostgreSQL, MongoDB, gRPC)
- Real Azure live migration test results and packet captures
- Integration patterns for other node agent frameworks

## License

MIT
