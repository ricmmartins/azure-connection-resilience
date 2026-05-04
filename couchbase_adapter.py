"""
Couchbase Resilience Adapter — Server-Side Drain Pattern

Receives drain signals from the Azure Connection Resilience Monitor
and executes a bounded graceful drain of the LOCAL Couchbase node.

This runs ON THE SERVER VM (same VMSS instance as the Couchbase process).
When a Freeze event is detected, it tells the local Couchbase node to
enter maintenance/drain mode so SDK smart clients discover the topology
change and route operations to other healthy nodes.

    ┌─ Server VM (one per Couchbase node) ─────────────────────────┐
    │                                                               │
    │  monitor.py ──(drain webhook)──► THIS ADAPTER                │
    │                                       │                       │
    │                                       ▼                       │
    │                               Couchbase Server Process        │
    │                               (enters maintenance mode)       │
    │                                       │                       │
    │                                       ▼                       │
    │                               SDK clients discover via        │
    │                               topology refresh and            │
    │                               route elsewhere                 │
    └───────────────────────────────────────────────────────────────┘

WHAT COUCHBASE WOULD ACTUALLY IMPLEMENT (pseudocode — adapt to your node agent):

    import requests, time, threading

    ADMIN = "http://127.0.0.1:8091"
    AUTH = ("admin", "password")
    OTP_NODE = "ns_1@127.0.0.1"

    def handle_drain(event_data, drain_timeout=5.0):
        # PRE-FREEZE: Remove node from cluster before Azure freezes the VM.
        # NOTE: startGracefulFailover applies to DATA SERVICE nodes.
        # For Index/Query/FTS nodes, the pattern may differ.

        # 1. Mark node as "draining" via Couchbase admin API.
        #    SDK smart clients discover topology change and route elsewhere.
        requests.post(
            f"{ADMIN}/controller/startGracefulFailover",
            auth=AUTH,
            data={"otpNode": OTP_NODE}
        )

        # 2. Wait bounded time for failover to complete.
        #    Gate: rebalanceStatus transitions from "running" → "none"
        #    AND the node's clusterMembership becomes "inactiveFailed".
        deadline = time.monotonic() + drain_timeout
        while time.monotonic() < deadline:
            resp = requests.get(f"{ADMIN}/pools/default", auth=AUTH).json()
            rebalance_done = resp.get("rebalanceStatus") == "none"
            node_failed_over = any(
                n.get("clusterMembership") == "inactiveFailed"
                for n in resp.get("nodes", [])
                if n.get("otpNode") == OTP_NODE
            )
            if rebalance_done and node_failed_over:
                break
            time.sleep(0.5)

        # 3. Node is quiesced. Freeze can proceed safely.
        # 4. Schedule post-freeze recovery (runs after VM resumes)
        threading.Thread(target=handle_recovery, daemon=True).start()

    def handle_recovery():
        # POST-FREEZE: Rejoin the cluster after VM resumes.
        # Gate on actual readiness rather than fixed timer.

        # Wait until IMDS is reachable (proves VM has resumed from freeze)
        while True:
            try:
                requests.get(
                    "http://169.254.169.254/metadata/instance",
                    headers={"Metadata": "true"}, timeout=2
                )
                break
            except Exception:
                time.sleep(1)

        # Verify Couchbase service is healthy before attempting rejoin
        for _ in range(30):
            try:
                resp = requests.get(f"{ADMIN}/pools/default", auth=AUTH, timeout=2)
                if resp.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(1)

        # Verify no conflicting rebalance is already running
        resp = requests.get(f"{ADMIN}/pools/default", auth=AUTH).json()
        if resp.get("rebalanceStatus") != "none":
            return  # Another operation in progress; let it finish

        # 5. Delta recovery preserves data already on disk (fast rejoin)
        requests.post(
            f"{ADMIN}/controller/setRecoveryType",
            auth=AUTH,
            data={"otpNode": OTP_NODE, "recoveryType": "delta"}
        )

        # 6. Trigger rebalance to bring node back into the cluster
        resp = requests.get(f"{ADMIN}/pools/default", auth=AUTH).json()
        known_nodes = [n["otpNode"] for n in resp.get("nodes", [])]
        requests.post(
            f"{ADMIN}/controller/rebalance",
            auth=AUTH,
            data={"knownNodes": ",".join(known_nodes)}
        )

    # Full lifecycle:
    #   IMDS event → handle_drain() → freeze → VM resumes → handle_recovery()
    #   Total: node exits cluster, freeze passes, node rejoins with delta recovery

This file is a GENERIC REFERENCE showing the state machine pattern.
For Couchbase specifically, replace the _disconnect/_connect methods
with calls to Couchbase Server's admin REST API (port 8091).

Usage (generic demo mode):
    python couchbase_adapter.py --drain-port 8099 --auth-token secret

The state machine (NORMAL → DRAINING → FORCE_CLOSE → RECONNECT → NORMAL)
is universal. The specific drain mechanism is application-dependent.
"""

import argparse
import json
import logging
import os
import random
import threading
import time
from enum import Enum
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
from typing import Optional

logger = logging.getLogger("couchbase-resilience")

try:
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions
    from couchbase.auth import PasswordAuthenticator
    HAS_COUCHBASE = True
except ImportError:
    HAS_COUCHBASE = False


class AdapterState(Enum):
    """Strict state machine for drain lifecycle."""
    NORMAL = "normal"           # Accepting traffic, healthy
    DRAINING = "draining"       # Drain initiated, waiting for in-flight ops
    FORCE_CLOSING = "force_closing"  # Timeout hit, force-closing sockets
    RECONNECTING = "reconnecting"    # Opening new connections
    FAILED = "failed"           # Reconnect failed after all retries


class CouchbaseResilienceAdapter:
    """
    Handles drain/reconnect lifecycle for Couchbase connections.

    State machine (strict, bounded transitions):
      NORMAL -> DRAINING -> FORCE_CLOSING -> RECONNECTING -> NORMAL
                                                          -> FAILED (if exhausted retries)

    Critical design decisions:
      - Drain timeout is BOUNDED (default 5s). Never wait indefinitely.
      - Force-close after timeout prevents deadlock on black-holed sockets.
      - Drain is idempotent per event_id (duplicate signals are no-ops).
      - Auth token required to prevent unauthorized drain triggers.
    """

    def __init__(
        self,
        connection_string: str,
        bucket: str,
        username: str = "",
        password: str = "",
        drain_port: int = 8099,
        auth_token: Optional[str] = None,
        drain_timeout: float = 5.0,
        reconnect_max_retries: int = 10,
        reconnect_base_delay: float = 1.0,
        reconnect_jitter: float = 0.5,
    ):
        self.connection_string = connection_string
        self.bucket_name = bucket
        self.username = username
        self.password = password
        self.drain_port = drain_port
        self.auth_token = auth_token
        self.drain_timeout = drain_timeout
        self.reconnect_max_retries = reconnect_max_retries
        self.reconnect_base_delay = reconnect_base_delay
        self.reconnect_jitter = reconnect_jitter

        self.cluster: Optional[object] = None
        self.bucket: Optional[object] = None
        self.state = AdapterState.NORMAL
        self.drain_count = 0
        self.last_drain_at: Optional[str] = None
        self.last_reconnect_at: Optional[str] = None
        self.last_event_id: Optional[str] = None
        self._lock = threading.Lock()

    @property
    def is_connected(self) -> bool:
        return self.state == AdapterState.NORMAL

    def start(self):
        """Start the drain listener HTTP server."""
        if self._connect():
            self.state = AdapterState.NORMAL
        else:
            self.state = AdapterState.FAILED
        self._start_drain_listener()

    def _connect(self) -> bool:
        """
        Establish Couchbase connection (transport only).
        Does NOT mutate self.state — the caller (FSM controller) owns state transitions.
        Returns True on success, False on failure.
        """
        if not HAS_COUCHBASE:
            logger.info("Couchbase SDK not installed. Running in simulation mode.")
            return True

        try:
            auth = PasswordAuthenticator(self.username, self.password)
            self.cluster = Cluster(
                self.connection_string, ClusterOptions(auth)
            )
            self.bucket = self.cluster.bucket(self.bucket_name)
            self.bucket.on_connect()
            logger.info(f"Connected to {self.connection_string}/{self.bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def drain(self, event_data: dict = None):
        """
        Execute bounded drain sequence.

        State machine: NORMAL -> DRAINING -> FORCE_CLOSING -> RECONNECTING -> NORMAL
        Total time is bounded to: drain_timeout + reconnect_time
        """
        event_id = event_data.get("event_id", "unknown") if event_data else "manual"

        with self._lock:
            # Idempotent: ignore duplicate events
            if event_id != "manual" and event_id == self.last_event_id:
                logger.info(f"Duplicate drain for event {event_id}, ignoring.")
                return

            if self.state != AdapterState.NORMAL:
                logger.warning(
                    f"Drain requested in state {self.state.value}, ignoring. "
                    f"Wait for current cycle to complete."
                )
                return

            self.state = AdapterState.DRAINING
            self.drain_count += 1
            self.last_drain_at = datetime.now().isoformat()
            self.last_event_id = event_id

        logger.warning(
            f"DRAIN initiated (#{self.drain_count}, event={event_id}). "
            f"Timeout: {self.drain_timeout}s"
        )

        # Phase 1: DRAINING — bounded wait for in-flight operations
        logger.info(f"Waiting up to {self.drain_timeout}s for in-flight ops...")
        deadline = time.monotonic() + self.drain_timeout

        # In a real implementation, poll pending operation count and break early when zero.
        # For this reference demo, simulate a fast drain (~1s) then proceed.
        while time.monotonic() < deadline:
            time.sleep(0.1)
            # Simulated: in production, check SDK pending_ops == 0 and break
            elapsed = time.monotonic() - (deadline - self.drain_timeout)
            if elapsed >= 1.0:
                logger.info("Simulated drain complete (in-flight ops flushed).")
                break

        # Phase 2: FORCE_CLOSING — close all sockets regardless of in-flight state
        with self._lock:
            self.state = AdapterState.FORCE_CLOSING

        logger.info("Force-closing all Couchbase connections...")
        self._disconnect()

        # Phase 3: RECONNECTING — exponential backoff with jitter
        with self._lock:
            self.state = AdapterState.RECONNECTING

        logger.info("Reconnecting to Couchbase...")
        success = self._reconnect_with_backoff()

        with self._lock:
            if success:
                self.state = AdapterState.NORMAL
                self.last_reconnect_at = datetime.now().isoformat()
                logger.info("Drain complete. Fresh connections established.")
            else:
                self.state = AdapterState.FAILED
                logger.error("DRAIN FAILED: could not reconnect. Manual intervention needed.")

    def _disconnect(self):
        """Force-close existing Couchbase connections."""
        if not HAS_COUCHBASE or not self.cluster:
            return

        try:
            self.cluster.close()
            logger.info("Connections force-closed.")
        except Exception as e:
            logger.error(f"Error force-closing connections: {e}")

    def _reconnect_with_backoff(self) -> bool:
        """Reconnect with exponential backoff + jitter. Returns True on success."""
        for attempt in range(1, self.reconnect_max_retries + 1):
            delay = self.reconnect_base_delay * (2 ** (attempt - 1))
            delay = min(delay, 30.0)
            # Add jitter to prevent thundering herd across nodes
            jitter = random.uniform(0, self.reconnect_jitter * delay)
            total_delay = delay + jitter

            logger.info(
                f"Reconnect attempt {attempt}/{self.reconnect_max_retries} "
                f"(delay: {total_delay:.1f}s)"
            )
            time.sleep(total_delay)

            try:
                if self._connect():
                    logger.info(
                        f"Reconnected on attempt {attempt}. "
                        f"New connections route through healthy backend."
                    )
                    return True
            except Exception as e:
                logger.warning(f"Attempt {attempt} failed: {e}")

        return False

    def _start_drain_listener(self):
        """Start HTTP server on loopback only for drain signals."""
        adapter = self

        class DrainHandler(BaseHTTPRequestHandler):
            def do_POST(self):
                # Auth check
                if adapter.auth_token:
                    auth_header = self.headers.get("Authorization", "")
                    expected = f"Bearer {adapter.auth_token}"
                    if auth_header != expected:
                        self.send_response(401)
                        self.end_headers()
                        self.wfile.write(b'{"error": "unauthorized"}')
                        logger.warning(f"Unauthorized drain attempt from {self.client_address}")
                        return

                if "/drain" in self.path:
                    content_len = int(self.headers.get("Content-Length", 0))
                    body = self.rfile.read(content_len).decode() if content_len else "{}"

                    try:
                        event_data = json.loads(body)
                    except json.JSONDecodeError:
                        event_data = {"raw": body}

                    # Execute drain in background thread (non-blocking response)
                    threading.Thread(
                        target=adapter.drain,
                        args=(event_data,),
                        daemon=True,
                    ).start()

                    self.send_response(202)  # Accepted (async processing)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "status": "accepted",
                        "state": adapter.state.value,
                        "drain_count": adapter.drain_count,
                    }).encode())
                else:
                    self.send_response(404)
                    self.end_headers()

            def do_GET(self):
                if "/health" in self.path:
                    status = 200 if adapter.state == AdapterState.NORMAL else 503
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "status": "healthy" if status == 200 else "unhealthy",
                        "state": adapter.state.value,
                    }).encode())

                elif "/status" in self.path:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "state": adapter.state.value,
                        "drain_count": adapter.drain_count,
                        "last_drain_at": adapter.last_drain_at,
                        "last_reconnect_at": adapter.last_reconnect_at,
                        "last_event_id": adapter.last_event_id,
                        "connection_string": adapter.connection_string,
                        "bucket": adapter.bucket_name,
                        "drain_timeout": adapter.drain_timeout,
                    }, indent=2).encode())
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format, *args):
                logger.debug(f"HTTP: {args[0]}")

        # SECURITY: bind to loopback only
        server = HTTPServer(("127.0.0.1", self.drain_port), DrainHandler)
        logger.info(f"Drain listener on 127.0.0.1:{self.drain_port} (loopback only)")
        logger.info(f"  POST /drain  — trigger drain (auth required)")
        logger.info(f"  GET  /health — connection health")
        logger.info(f"  GET  /status — full state")

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        return server


def main():
    parser = argparse.ArgumentParser(
        description="Couchbase Resilience Adapter — "
        "reference implementation of drain/reconnect state machine"
    )
    parser.add_argument(
        "--connection-string", default="couchbase://localhost",
        help="Couchbase connection string",
    )
    parser.add_argument("--bucket", default="default", help="Bucket name")
    parser.add_argument("--username", default="", help="Couchbase username")
    parser.add_argument("--password", default="", help="Couchbase password")
    parser.add_argument(
        "--drain-port", type=int, default=8099,
        help="Drain listener port (binds to 127.0.0.1 only)"
    )
    parser.add_argument(
        "--auth-token", help="Required bearer token for drain requests"
    )
    parser.add_argument(
        "--drain-timeout", type=float, default=5.0,
        help="Max seconds to wait for in-flight ops before force-close"
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    token = args.auth_token or os.environ.get("DRAIN_AUTH_TOKEN")

    adapter = CouchbaseResilienceAdapter(
        connection_string=args.connection_string,
        bucket=args.bucket,
        username=args.username,
        password=args.password,
        drain_port=args.drain_port,
        auth_token=token,
        drain_timeout=args.drain_timeout,
    )
    adapter.start()

    logger.info("Adapter running. Waiting for drain signals...")
    if not token:
        logger.warning("No auth token set. Drain endpoint is unauthenticated!")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down.")


if __name__ == "__main__":
    main()
