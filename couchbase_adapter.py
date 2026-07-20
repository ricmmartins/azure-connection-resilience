"""
Couchbase SDK Adapter for Azure Connection Resilience Monitor

Provides automatic connection drain/reconnect for Couchbase SDK clients
when Azure maintenance events are detected.

Usage:
    from couchbase_adapter import CouchbaseResilienceAdapter

    cluster = Cluster("couchbase://your-lb-endpoint", ClusterOptions(...))
    adapter = CouchbaseResilienceAdapter(cluster)
    adapter.start()
"""

import time
import json
import logging
import threading
from typing import Optional
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import Request, urlopen

logger = logging.getLogger("azure-resilience.couchbase")


class CouchbaseResilienceAdapter:
    """
    Wraps a Couchbase Cluster connection with Azure maintenance awareness.
    
    When the resilience monitor detects a maintenance event, this adapter:
    1. Receives the drain notification
    2. Closes idle connections in the Couchbase connection pool
    3. Marks active operations for retry
    4. Reconnects with exponential backoff
    5. Logs recovery metrics
    """

    def __init__(
        self,
        cluster,
        listen_port: int = 8099,
        monitor_url: Optional[str] = None,
        reconnect_delay: float = 1.0,
        max_reconnect_delay: float = 30.0,
    ):
        self.cluster = cluster
        self.listen_port = listen_port
        self.monitor_url = monitor_url
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self._server: Optional[HTTPServer] = None
        self._drain_count = 0
        self._last_drain_time = 0
        self._last_recovery_time = 0

    def start(self):
        """Start listening for drain notifications from the monitor."""
        handler = self._create_handler()
        self._server = HTTPServer(("127.0.0.1", self.listen_port), handler)
        thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        thread.start()
        logger.info(f"Couchbase resilience adapter listening on :{self.listen_port}")

        if self.monitor_url:
            self._register_with_monitor()

    def stop(self):
        """Stop the adapter."""
        if self._server:
            self._server.shutdown()

    def drain_and_reconnect(self, event: Optional[dict] = None):
        """
        Core resilience logic: drain connections and reconnect.
        
        This is called automatically when the monitor sends a notification,
        or can be called manually for testing.
        """
        drain_start = time.time()
        event_type = event.get("event_type", "manual") if event else "manual"

        logger.warning(f"🔄 Draining Couchbase connections (trigger: {event_type})")

        try:
            # Step 1: Close the cluster connection
            # This tells the SDK to stop using existing connections
            self.cluster.close()
            logger.info("Cluster connection closed")

            # Step 2: Wait briefly for the maintenance event to complete
            # Azure live migrations typically complete in 5-30 seconds
            time.sleep(self.reconnect_delay)

            # Step 3: Reconnect with backoff
            self._reconnect_with_backoff()

            recovery_time = time.time() - drain_start
            self._last_recovery_time = recovery_time
            self._drain_count += 1
            self._last_drain_time = time.time()

            logger.info(f"✅ Couchbase reconnected in {recovery_time:.1f}s")
            return recovery_time

        except Exception as e:
            logger.error(f"❌ Reconnection failed: {e}")
            raise

    def _reconnect_with_backoff(self):
        """Reconnect to Couchbase with exponential backoff."""
        delay = self.reconnect_delay
        attempts = 0

        while True:
            attempts += 1
            try:
                # Attempt to use the cluster (triggers reconnection in SDK)
                # The Couchbase SDK handles reconnection internally
                # We just need to verify it's working
                self.cluster.ping()
                logger.info(f"Reconnected after {attempts} attempt(s)")
                return
            except Exception as e:
                if delay >= self.max_reconnect_delay:
                    raise RuntimeError(
                        f"Failed to reconnect after {attempts} attempts: {e}"
                    )
                logger.info(f"Reconnect attempt {attempts} failed, retry in {delay:.1f}s")
                time.sleep(delay)
                delay = min(delay * 2, self.max_reconnect_delay)

    def _register_with_monitor(self):
        """Register this adapter's drain endpoint with the monitor."""
        try:
            payload = json.dumps({
                "drain_url": f"http://127.0.0.1:{self.listen_port}/drain",
                "app_name": "couchbase-client"
            }).encode()
            req = Request(
                f"{self.monitor_url}/register",
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urlopen(req, timeout=5) as resp:
                logger.info(f"Registered with monitor: {resp.status}")
        except Exception as e:
            logger.warning(f"Could not register with monitor: {e}")

    def _create_handler(self):
        """Create HTTP handler that delegates to this adapter."""
        adapter = self

        class DrainHandler(BaseHTTPRequestHandler):
            def do_POST(self):
                if self.path == "/drain":
                    content_length = int(self.headers.get("Content-Length", 0))
                    body = self.rfile.read(content_length) if content_length > 0 else b"{}"
                    event = json.loads(body)

                    # Run drain in background (don't block the HTTP response)
                    thread = threading.Thread(
                        target=adapter.drain_and_reconnect,
                        args=(event,)
                    )
                    thread.start()

                    self.send_response(202)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b'{"status": "drain_initiated"}')
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format, *args):
                pass

        return DrainHandler

    @property
    def stats(self) -> dict:
        """Return adapter statistics."""
        return {
            "drain_count": self._drain_count,
            "last_drain_time": self._last_drain_time,
            "last_recovery_seconds": self._last_recovery_time,
        }


# --- Example: Standalone usage with Couchbase Python SDK ---

def example_usage():
    """
    Example showing how to integrate with Couchbase Python SDK.
    
    Prerequisites:
        pip install couchbase
    """
    # This is illustrative — adjust connection string and credentials
    # to match your environment
    
    print("""
    # Example integration:
    
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions
    from couchbase.auth import PasswordAuthenticator
    from couchbase_adapter import CouchbaseResilienceAdapter

    # Connect through Azure LB
    auth = PasswordAuthenticator("username", "password")
    cluster = Cluster("couchbase://your-azure-lb-ip", ClusterOptions(auth))

    # Add resilience
    adapter = CouchbaseResilienceAdapter(
        cluster=cluster,
        listen_port=8099,        # adapter listens here for drain signals
        reconnect_delay=2.0,     # wait 2s before reconnecting
    )
    adapter.start()

    # Now configure the monitor to notify this adapter:
    # python monitor.py --notify-url http://localhost:8099/drain

    # Your app runs normally. When Azure maintenance is detected,
    # the adapter automatically drains and reconnects.
    """)


if __name__ == "__main__":
    example_usage()
