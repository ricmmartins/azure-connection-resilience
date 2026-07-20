"""
Azure Connection Resilience Monitor

Polls Azure IMDS Scheduled Events API and notifies applications
to drain connections before live migration events cause TCP black holes.
"""

import time
import json
import signal
import logging
import argparse
import threading
from dataclasses import dataclass, field
from typing import Optional, Callable
from urllib.request import Request, urlopen
from urllib.error import URLError
from http.server import HTTPServer, BaseHTTPRequestHandler

__version__ = "0.1.0"

IMDS_SCHEDULED_EVENTS_URL = "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01"
IMDS_HEADERS = {"Metadata": "true"}

logger = logging.getLogger("azure-resilience")


@dataclass
class MonitorConfig:
    poll_interval: float = 5.0
    notify_url: Optional[str] = None
    notify_method: str = "POST"
    notify_signal: Optional[int] = None
    notify_pid: Optional[int] = None
    notify_file: Optional[str] = None
    metrics_port: int = 9090
    event_types: list = field(default_factory=lambda: ["Freeze", "Reboot", "Redeploy"])
    acknowledge_events: bool = True
    dry_run: bool = False


@dataclass
class Metrics:
    events_detected: int = 0
    drains_triggered: int = 0
    drain_failures: int = 0
    last_event_time: float = 0
    last_drain_time: float = 0
    recovery_times: list = field(default_factory=list)

    def record_event(self):
        self.events_detected += 1
        self.last_event_time = time.time()

    def record_drain(self, success: bool, recovery_time: float = 0):
        if success:
            self.drains_triggered += 1
            self.last_drain_time = time.time()
            if recovery_time > 0:
                self.recovery_times.append(recovery_time)
        else:
            self.drain_failures += 1


class ScheduledEventsMonitor:
    """Polls Azure IMDS Scheduled Events and triggers drain on maintenance."""

    def __init__(self, config: MonitorConfig):
        self.config = config
        self.metrics = Metrics()
        self._running = False
        self._seen_events: set = set()

    def poll_scheduled_events(self) -> list:
        """Query IMDS for upcoming scheduled events."""
        try:
            req = Request(IMDS_SCHEDULED_EVENTS_URL, headers=IMDS_HEADERS)
            with urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                return data.get("Events", [])
        except URLError as e:
            logger.debug(f"IMDS poll failed (expected outside Azure): {e}")
            return []
        except Exception as e:
            logger.warning(f"Unexpected error polling IMDS: {e}")
            return []

    def acknowledge_event(self, event_id: str):
        """Acknowledge a scheduled event (tells Azure we're ready)."""
        if not self.config.acknowledge_events:
            return

        payload = json.dumps({
            "StartRequests": [{"EventId": event_id}]
        }).encode()

        try:
            req = Request(
                IMDS_SCHEDULED_EVENTS_URL,
                data=payload,
                headers={**IMDS_HEADERS, "Content-Type": "application/json"},
                method="POST"
            )
            with urlopen(req, timeout=5) as resp:
                logger.info(f"Acknowledged event {event_id}: {resp.status}")
        except Exception as e:
            logger.warning(f"Failed to acknowledge event {event_id}: {e}")

    def notify_app(self, event: dict) -> bool:
        """Notify the application to drain connections."""
        success = False

        # HTTP webhook notification
        if self.config.notify_url:
            success = self._notify_http(event)

        # Signal-based notification
        if self.config.notify_signal and self.config.notify_pid:
            success = self._notify_signal(event) or success

        # File-based notification
        if self.config.notify_file:
            success = self._notify_file(event) or success

        return success

    def _notify_http(self, event: dict) -> bool:
        """Send HTTP notification to app's drain endpoint."""
        payload = json.dumps({
            "event_type": event.get("EventType"),
            "event_id": event.get("EventId"),
            "resources": event.get("Resources", []),
            "not_before": event.get("NotBefore"),
            "action": "drain_connections"
        }).encode()

        try:
            req = Request(
                self.config.notify_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method=self.config.notify_method
            )
            with urlopen(req, timeout=10) as resp:
                logger.info(f"Drain notification sent: {resp.status}")
                return resp.status < 400
        except Exception as e:
            logger.error(f"Failed to notify app at {self.config.notify_url}: {e}")
            return False

    def _notify_signal(self, event: dict) -> bool:
        """Send signal to target process."""
        import os
        try:
            os.kill(self.config.notify_pid, self.config.notify_signal)
            logger.info(f"Sent signal {self.config.notify_signal} to PID {self.config.notify_pid}")
            return True
        except ProcessLookupError:
            logger.error(f"PID {self.config.notify_pid} not found")
            return False

    def _notify_file(self, event: dict) -> bool:
        """Write trigger file for file-watching apps."""
        try:
            with open(self.config.notify_file, "w") as f:
                json.dump({
                    "timestamp": time.time(),
                    "event": event
                }, f)
            logger.info(f"Drain trigger written to {self.config.notify_file}")
            return True
        except Exception as e:
            logger.error(f"Failed to write trigger file: {e}")
            return False

    def process_events(self, events: list):
        """Process new scheduled events."""
        for event in events:
            event_id = event.get("EventId")
            event_type = event.get("EventType")

            if event_id in self._seen_events:
                continue

            if event_type not in self.config.event_types:
                logger.debug(f"Ignoring event type: {event_type}")
                continue

            self._seen_events.add(event_id)
            self.metrics.record_event()

            logger.warning(
                f"🚨 Maintenance event detected: {event_type} "
                f"(ID: {event_id}, NotBefore: {event.get('NotBefore')})"
            )

            if self.config.dry_run:
                logger.info("[DRY RUN] Would notify app to drain connections")
                continue

            drain_start = time.time()
            success = self.notify_app(event)
            drain_duration = time.time() - drain_start

            self.metrics.record_drain(success, drain_duration)

            if success:
                logger.info(f"✅ App notified in {drain_duration:.2f}s")
                self.acknowledge_event(event_id)
            else:
                logger.error("❌ Failed to notify app — connections may stall")

    def run(self):
        """Main polling loop."""
        self._running = True
        logger.info(
            f"Azure Connection Resilience Monitor v{__version__} started\n"
            f"  Poll interval: {self.config.poll_interval}s\n"
            f"  Notify URL: {self.config.notify_url}\n"
            f"  Event types: {self.config.event_types}\n"
            f"  Metrics port: {self.config.metrics_port}"
        )

        while self._running:
            events = self.poll_scheduled_events()
            if events:
                self.process_events(events)
            time.sleep(self.config.poll_interval)

    def stop(self):
        """Stop the polling loop."""
        self._running = False
        logger.info("Monitor stopping...")


class MetricsHandler(BaseHTTPRequestHandler):
    """Prometheus-compatible metrics endpoint."""

    monitor: Optional[ScheduledEventsMonitor] = None

    def do_GET(self):
        if self.path == "/metrics":
            self._serve_metrics()
        elif self.path == "/health":
            self._serve_health()
        else:
            self.send_response(404)
            self.end_headers()

    def _serve_metrics(self):
        m = self.monitor.metrics if self.monitor else Metrics()
        body = (
            f"# HELP azure_resilience_events_total Maintenance events detected\n"
            f"# TYPE azure_resilience_events_total counter\n"
            f"azure_resilience_events_total {m.events_detected}\n\n"
            f"# HELP azure_resilience_drains_total Drain notifications sent\n"
            f"# TYPE azure_resilience_drains_total counter\n"
            f'azure_resilience_drains_total{{result="success"}} {m.drains_triggered}\n'
            f'azure_resilience_drains_total{{result="failed"}} {m.drain_failures}\n\n'
            f"# HELP azure_resilience_monitor_up Monitor is running\n"
            f"# TYPE azure_resilience_monitor_up gauge\n"
            f"azure_resilience_monitor_up 1\n"
        ).encode()

        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.end_headers()
        self.wfile.write(body)

    def _serve_health(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status": "healthy"}')

    def log_message(self, format, *args):
        pass  # suppress request logging


def start_metrics_server(monitor: ScheduledEventsMonitor, port: int):
    """Start the metrics HTTP server in a background thread."""
    MetricsHandler.monitor = monitor
    server = HTTPServer(("0.0.0.0", port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Metrics server listening on :{port}")
    return server


def main():
    parser = argparse.ArgumentParser(
        description="Azure Connection Resilience Monitor — detect maintenance, drain connections"
    )
    parser.add_argument("--notify-url", help="HTTP endpoint to call when maintenance detected")
    parser.add_argument("--notify-method", default="POST", help="HTTP method (default: POST)")
    parser.add_argument("--notify-signal", type=int, help="Signal number to send (e.g., 10 for SIGUSR1)")
    parser.add_argument("--notify-pid", type=int, help="PID to signal")
    parser.add_argument("--notify-file", help="File path to write drain trigger")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="IMDS poll interval in seconds")
    parser.add_argument("--metrics-port", type=int, default=9090, help="Prometheus metrics port")
    parser.add_argument("--dry-run", action="store_true", help="Detect events but don't notify")
    parser.add_argument("--no-acknowledge", action="store_true", help="Don't acknowledge events to Azure")
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    config = MonitorConfig(
        poll_interval=args.poll_interval,
        notify_url=args.notify_url,
        notify_method=args.notify_method,
        notify_signal=args.notify_signal,
        notify_pid=args.notify_pid,
        notify_file=args.notify_file,
        metrics_port=args.metrics_port,
        acknowledge_events=not args.no_acknowledge,
        dry_run=args.dry_run,
    )

    monitor = ScheduledEventsMonitor(config)

    # Graceful shutdown
    def shutdown(signum, frame):
        monitor.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Start metrics server
    start_metrics_server(monitor, config.metrics_port)

    # Run the monitor
    monitor.run()


if __name__ == "__main__":
    main()
