"""
Azure Connection Resilience Monitor

Polls IMDS Scheduled Events and notifies your application before
Azure maintenance causes TCP black holes. Designed as a node-agent
integration pattern for VM-based stateful services.

Usage:
    python monitor.py --notify-url http://127.0.0.1:8099/drain --verbose
    python monitor.py --notify-file /tmp/azure-drain-trigger

Microsoft docs reference:
    https://learn.microsoft.com/en-us/azure/virtual-machines/linux/scheduled-events
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

logger = logging.getLogger("azure-resilience")

IMDS_SCHEDULED_EVENTS_URL = (
    "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01"
)
IMDS_HEADERS = {"Metadata": "true"}


class MonitorState(Enum):
    """Monitor lifecycle states."""
    POLLING = "polling"
    EVENT_DETECTED = "event_detected"
    NOTIFYING = "notifying"
    COOLDOWN = "cooldown"


@dataclass
class MonitorConfig:
    # Polling — Microsoft recommends 1/sec for fastest reaction
    poll_interval: float = 1.0
    imds_url: str = IMDS_SCHEDULED_EVENTS_URL

    # Notification targets (loopback only for security)
    notify_url: Optional[str] = None
    notify_file: Optional[str] = None
    notify_signal_pid_file: Optional[str] = None
    notify_signal: int = signal.SIGUSR1 if hasattr(signal, "SIGUSR1") else 0

    # Security
    auth_token: Optional[str] = None  # Token to include in drain requests

    # Metrics
    metrics_port: int = 9090

    # Event filtering
    event_types: list = field(default_factory=lambda: ["Freeze", "Reboot", "Redeploy"])
    vm_name: Optional[str] = None  # Filter events to this VM only

    # Behavior
    dry_run: bool = False
    cooldown_seconds: float = 60.0  # Min time between drain notifications
    notify_timeout: float = 5.0


class Metrics:
    """Prometheus metrics (gracefully no-ops if prometheus_client missing)."""

    def __init__(self, enabled: bool = True):
        self.enabled = enabled and HAS_PROMETHEUS
        if self.enabled:
            self.polls = Counter(
                "resilience_imds_polls_total", "Total IMDS polls"
            )
            self.poll_errors = Counter(
                "resilience_imds_poll_errors_total", "IMDS poll failures"
            )
            self.events_detected = Counter(
                "resilience_events_detected_total",
                "Maintenance events detected",
                ["event_type"],
            )
            self.notifications_sent = Counter(
                "resilience_notifications_sent_total",
                "Drain notifications sent",
                ["method"],
            )
            self.notification_errors = Counter(
                "resilience_notification_errors_total",
                "Failed drain notifications",
                ["method"],
            )
            self.last_poll_time = Gauge(
                "resilience_last_poll_timestamp",
                "Timestamp of last successful IMDS poll",
            )
            self.poll_duration = Histogram(
                "resilience_poll_duration_seconds",
                "IMDS poll latency",
                buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
            )
            self.state = Gauge(
                "resilience_monitor_state",
                "Current monitor state (0=polling, 1=event_detected, 2=notifying, 3=cooldown)",
            )
            self.consecutive_poll_failures = Gauge(
                "resilience_consecutive_poll_failures",
                "Consecutive IMDS poll failures (alert if >10)",
            )

    def inc_polls(self):
        if self.enabled:
            self.polls.inc()

    def inc_poll_errors(self):
        if self.enabled:
            self.poll_errors.inc()

    def inc_events(self, event_type: str):
        if self.enabled:
            self.events_detected.labels(event_type=event_type).inc()

    def inc_notifications(self, method: str):
        if self.enabled:
            self.notifications_sent.labels(method=method).inc()

    def inc_notification_errors(self, method: str):
        if self.enabled:
            self.notification_errors.labels(method=method).inc()

    def set_last_poll(self):
        if self.enabled:
            self.last_poll_time.set_to_current_time()

    def observe_poll(self, duration: float):
        if self.enabled:
            self.poll_duration.observe(duration)

    def set_state(self, state: MonitorState):
        if self.enabled:
            state_map = {
                MonitorState.POLLING: 0,
                MonitorState.EVENT_DETECTED: 1,
                MonitorState.NOTIFYING: 2,
                MonitorState.COOLDOWN: 3,
            }
            self.state.set(state_map.get(state, 0))

    def set_consecutive_failures(self, count: int):
        if self.enabled:
            self.consecutive_poll_failures.set(count)


class ScheduledEventsMonitor:
    """Core monitor: polls IMDS and notifies app on maintenance events."""

    # Max events to track before pruning oldest entries
    MAX_SEEN_EVENTS = 500

    def __init__(self, config: MonitorConfig):
        self.config = config
        self.metrics = Metrics(enabled=config.metrics_port > 0)
        # Tracks events that were successfully delivered (notification confirmed)
        self.delivered_events: dict = {}  # event_id -> monotonic timestamp
        # Tracks events we intentionally skip (wrong type, wrong VM)
        self.ignored_events: set = set()
        self.running = True
        self.state = MonitorState.POLLING
        self.last_notification_time: float = 0
        self.consecutive_poll_failures: int = 0

    def start(self):
        """Start the monitoring loop."""
        if self.metrics.enabled and not self.config.dry_run:
            start_http_server(self.config.metrics_port, addr="127.0.0.1")
            logger.info(f"Metrics server on 127.0.0.1:{self.config.metrics_port}")

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        logger.info(f"Monitor started. Polling every {self.config.poll_interval}s")
        logger.info(f"IMDS URL: {self.config.imds_url}")
        logger.info(f"Event types: {self.config.event_types}")
        logger.info(f"Cooldown: {self.config.cooldown_seconds}s between notifications")

        if self.config.vm_name:
            logger.info(f"VM filter: only reacting to events targeting '{self.config.vm_name}'")

        if self.config.notify_url:
            logger.info(f"Notification: HTTP POST -> {self.config.notify_url}")
        elif self.config.notify_file:
            logger.info(f"Notification: file -> {self.config.notify_file}")
        elif self.config.notify_signal_pid_file:
            logger.info(f"Notification: signal -> PID from {self.config.notify_signal_pid_file}")

        self.metrics.set_state(MonitorState.POLLING)

        while self.running:
            try:
                self._poll_once()
            except Exception as e:
                logger.error(f"Unexpected poll error: {e}", exc_info=True)
            time.sleep(self.config.poll_interval)

    def _poll_once(self):
        """Single poll cycle."""
        start = time.monotonic()

        try:
            req = urllib.request.Request(
                self.config.imds_url, headers=IMDS_HEADERS
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
        except urllib.error.URLError as e:
            self.consecutive_poll_failures += 1
            self.metrics.inc_poll_errors()
            self.metrics.set_consecutive_failures(self.consecutive_poll_failures)
            if self.consecutive_poll_failures % 30 == 1:  # Log every 30s at 1/sec
                logger.warning(
                    f"IMDS unreachable ({self.consecutive_poll_failures} consecutive failures): {e}"
                )
            return
        except Exception as e:
            self.consecutive_poll_failures += 1
            self.metrics.inc_poll_errors()
            self.metrics.set_consecutive_failures(self.consecutive_poll_failures)
            logger.error(f"IMDS request failed: {e}")
            return

        # Successful poll
        self.consecutive_poll_failures = 0
        self.metrics.set_consecutive_failures(0)
        duration = time.monotonic() - start
        self.metrics.inc_polls()
        self.metrics.set_last_poll()
        self.metrics.observe_poll(duration)

        events = data.get("Events", [])
        for event in events:
            event_id = event.get("EventId", "")
            event_type = event.get("EventType", "")

            # Skip events we already delivered successfully
            if event_id in self.delivered_events:
                continue

            # Skip events we intentionally ignore (wrong type/VM)
            if event_id in self.ignored_events:
                continue

            # Filter by event type
            if event_type not in self.config.event_types:
                logger.debug(f"Ignoring event type: {event_type}")
                self._add_ignored(event_id)
                continue

            # Filter by VM name (Resources list)
            if self.config.vm_name:
                resources = event.get("Resources", [])
                if self.config.vm_name not in resources:
                    logger.debug(
                        f"Ignoring event {event_id}: targets {resources}, not '{self.config.vm_name}'"
                    )
                    self._add_ignored(event_id)
                    continue

            # Cooldown check — prevent drain storms
            # NOTE: do NOT mark as delivered. Event stays eligible for retry after cooldown.
            now = time.monotonic()
            if (now - self.last_notification_time) < self.config.cooldown_seconds:
                logger.debug(
                    f"Event {event_id} detected but in cooldown. Will retry next poll."
                )
                continue

            self.metrics.inc_events(event_type)

            logger.warning(
                f"MAINTENANCE EVENT DETECTED: {event_type} "
                f"(EventId={event_id}, "
                f"NotBefore={event.get('NotBefore', 'unknown')}, "
                f"Resources={event.get('Resources', [])})"
            )

            self._set_state(MonitorState.EVENT_DETECTED)
            delivered = self._notify_app(event)

            if delivered:
                # Only mark as handled if at least one notification succeeded
                self.delivered_events[event_id] = time.monotonic()
                self.last_notification_time = time.monotonic()
                self._set_state(MonitorState.COOLDOWN)
                self._prune_delivered_events()
            else:
                # Notification failed — event stays eligible for retry on next poll
                logger.error(
                    f"Event {event_id}: ALL notification channels failed. "
                    f"Will retry on next poll cycle."
                )

            self._set_state(MonitorState.POLLING)

    def _notify_app(self, event: dict) -> bool:
        """
        Send drain notification to the application.
        Returns True if at least one notification channel succeeded.
        Retries HTTP notifications up to 3 times with short backoff.
        """
        self._set_state(MonitorState.NOTIFYING)

        payload = json.dumps({
            "action": "drain",
            "event_type": event.get("EventType"),
            "event_id": event.get("EventId"),
            "not_before": event.get("NotBefore"),
            "resources": event.get("Resources", []),
            "duration_seconds": event.get("DurationInSeconds", -1),
            "description": event.get("Description", ""),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }).encode()

        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would notify: {payload.decode()}")
            return True

        any_success = False

        # HTTP notification (primary method) — with retry
        if self.config.notify_url:
            for attempt in range(1, 4):  # 3 attempts
                try:
                    headers = {"Content-Type": "application/json"}
                    if self.config.auth_token:
                        headers["Authorization"] = f"Bearer {self.config.auth_token}"

                    req = urllib.request.Request(
                        self.config.notify_url,
                        data=payload,
                        headers=headers,
                        method="POST",
                    )
                    with urllib.request.urlopen(req, timeout=self.config.notify_timeout) as resp:
                        logger.info(f"Drain notification sent -> HTTP {resp.status}")
                        self.metrics.inc_notifications("http")
                        any_success = True
                        break
                except Exception as e:
                    logger.warning(f"HTTP notify attempt {attempt}/3 failed: {e}")
                    if attempt < 3:
                        time.sleep(0.5 * attempt)
                    else:
                        logger.error(f"HTTP notification FAILED after 3 attempts: {e}")
                        self.metrics.inc_notification_errors("http")

        # File notification (simplest, for sidecar/shared-volume patterns)
        if self.config.notify_file:
            try:
                with open(self.config.notify_file, "w") as f:
                    f.write(payload.decode())
                logger.info(f"Drain trigger written -> {self.config.notify_file}")
                self.metrics.inc_notifications("file")
                any_success = True
            except Exception as e:
                logger.error(f"Failed to write trigger file: {e}")
                self.metrics.inc_notification_errors("file")

        # Signal notification (for apps that handle SIGUSR1/SIGUSR2)
        if self.config.notify_signal_pid_file:
            if self.config.notify_signal == 0:
                logger.error(
                    "Signal notification configured but SIGUSR1 not available on this platform"
                )
                self.metrics.inc_notification_errors("signal")
            else:
                try:
                    with open(self.config.notify_signal_pid_file) as f:
                        pid = int(f.read().strip())
                    os.kill(pid, self.config.notify_signal)
                    logger.info(f"Signal {self.config.notify_signal} sent -> PID {pid}")
                    self.metrics.inc_notifications("signal")
                    any_success = True
                except Exception as e:
                    logger.error(f"Failed to send signal: {e}")
                    self.metrics.inc_notification_errors("signal")

        return any_success

    def _set_state(self, state: MonitorState):
        self.state = state
        self.metrics.set_state(state)

    def _add_ignored(self, event_id: str):
        """Add to ignored set with independent bounding."""
        self.ignored_events.add(event_id)
        if len(self.ignored_events) > self.MAX_SEEN_EVENTS:
            # Discard oldest half (sets are unordered, but clearing half is fine
            # since ignored events are permanent-skip; worst case we re-evaluate)
            to_remove = list(self.ignored_events)[:len(self.ignored_events) // 2]
            for eid in to_remove:
                self.ignored_events.discard(eid)

    def _prune_delivered_events(self):
        """Remove old entries from delivered_events to prevent memory leak."""
        if len(self.delivered_events) <= self.MAX_SEEN_EVENTS:
            return
        # Keep only the newest half
        sorted_items = sorted(self.delivered_events.items(), key=lambda x: x[1])
        cutoff = len(sorted_items) // 2
        for event_id, _ in sorted_items[:cutoff]:
            del self.delivered_events[event_id]

    def _handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received. Exiting.")
        self.running = False


def main():
    parser = argparse.ArgumentParser(
        description="Azure Connection Resilience Monitor — "
        "polls IMDS Scheduled Events and notifies your app before maintenance."
    )
    parser.add_argument(
        "--poll-interval", type=float, default=1.0,
        help="IMDS poll interval in seconds (default: 1, per Microsoft guidance)"
    )
    parser.add_argument(
        "--imds-url", default=IMDS_SCHEDULED_EVENTS_URL,
        help="IMDS endpoint URL (override for testing with mock_imds.py)"
    )
    parser.add_argument(
        "--notify-url",
        help="HTTP endpoint for drain notification (POST). Must be loopback (127.0.0.1)."
    )
    parser.add_argument(
        "--notify-file", help="File path to write drain trigger"
    )
    parser.add_argument(
        "--notify-signal-pid", help="PID file for signal-based notification"
    )
    parser.add_argument(
        "--auth-token", help="Bearer token for drain endpoint authentication"
    )
    parser.add_argument(
        "--vm-name", help="Only react to events targeting this VM name (from Resources list)"
    )
    parser.add_argument(
        "--cooldown", type=float, default=60.0,
        help="Minimum seconds between drain notifications (prevents storms)"
    )
    parser.add_argument(
        "--metrics-port", type=int, default=9090,
        help="Prometheus metrics port (0=disabled). Binds to 127.0.0.1 only."
    )
    parser.add_argument("--dry-run", action="store_true", help="Log events but don't notify")
    parser.add_argument("--verbose", action="store_true", help="Debug logging")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = MonitorConfig(
        poll_interval=args.poll_interval,
        imds_url=args.imds_url,
        notify_url=args.notify_url,
        notify_file=args.notify_file,
        notify_signal_pid_file=args.notify_signal_pid,
        auth_token=args.auth_token or os.environ.get("DRAIN_AUTH_TOKEN"),
        vm_name=args.vm_name,
        cooldown_seconds=args.cooldown,
        metrics_port=args.metrics_port,
        dry_run=args.dry_run,
    )

    if not any([config.notify_url, config.notify_file, config.notify_signal_pid_file]):
        if not config.dry_run:
            logger.warning(
                "No notification method configured. Use --notify-url, "
                "--notify-file, or --notify-signal-pid. Running in observe-only mode."
            )

    monitor = ScheduledEventsMonitor(config)
    monitor.start()


if __name__ == "__main__":
    main()
