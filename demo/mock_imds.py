"""
Mock IMDS Server — Simulates Azure Instance Metadata Service

Serves fake Scheduled Events responses for local testing.
Listens on localhost:8169 (not 169.254.169.254, which requires root).

Usage:
    python mock_imds.py                     # Start with no events
    python mock_imds.py --trigger-after 10  # Inject event after 10s

Then point the monitor at it:
    python monitor.py --imds-url http://localhost:8169 --poll-interval 2
"""

import argparse
import json
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timedelta, timezone

events = {"Events": []}
lock = threading.Lock()


class IMDSHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if "scheduledevents" in self.path:
            with lock:
                payload = json.dumps(events)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload.encode())
        elif "instance" in self.path:
            # Basic instance metadata
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "compute": {
                    "vmId": "mock-vm-001",
                    "name": "demo-vm",
                    "location": "eastus"
                }
            }).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        # Event acknowledgment
        content_len = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_len) if content_len else b""
        print(f"[IMDS] Event acknowledged: {body.decode()}")
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        print(f"[IMDS] {args[0]}")


def inject_event(delay: float, event_type: str = "Freeze"):
    """Inject a maintenance event after a delay."""
    time.sleep(delay)
    not_before = (datetime.now(timezone.utc) + timedelta(minutes=5)).strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
    event = {
        "EventId": f"mock-{int(time.time())}",
        "EventType": event_type,
        "ResourceType": "VirtualMachine",
        "Resources": ["demo-vm"],
        "EventStatus": "Scheduled",
        "NotBefore": not_before,
    }
    with lock:
        events["Events"].append(event)
    print(f"\n{'='*60}")
    print(f"[IMDS] 🚨 MAINTENANCE EVENT INJECTED: {event_type}")
    print(f"[IMDS]    EventId: {event['EventId']}")
    print(f"[IMDS]    NotBefore: {not_before}")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description="Mock IMDS for testing")
    parser.add_argument("--port", type=int, default=8169, help="Listen port")
    parser.add_argument(
        "--trigger-after",
        type=float,
        default=0,
        help="Inject Freeze event after N seconds (0 = manual only)",
    )
    parser.add_argument(
        "--event-type",
        default="Freeze",
        choices=["Freeze", "Reboot", "Redeploy", "Preempt", "Terminate"],
        help="Type of event to inject",
    )
    args = parser.parse_args()

    if args.trigger_after > 0:
        t = threading.Thread(
            target=inject_event,
            args=(args.trigger_after, args.event_type),
            daemon=True,
        )
        t.start()
        print(f"[IMDS] Will inject '{args.event_type}' event in {args.trigger_after}s")

    server = HTTPServer(("0.0.0.0", args.port), IMDSHandler)
    print(f"[IMDS] Mock IMDS running on http://localhost:{args.port}")
    print(f"[IMDS] Endpoints:")
    print(f"[IMDS]   GET /metadata/scheduledevents?api-version=2020-07-01")
    print(f"[IMDS]   GET /metadata/instance?api-version=2021-02-01")
    print(f"[IMDS]   POST /metadata/scheduledevents (acknowledge)")
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[IMDS] Shutting down.")


if __name__ == "__main__":
    main()
