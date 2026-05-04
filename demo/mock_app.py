"""
Mock Application — Simulates a service with a /drain endpoint

Represents a typical application (like a Couchbase SDK client) that
receives drain notifications from the resilience monitor.

Exposes:
  POST /drain  — called by the monitor when maintenance is detected
  GET  /health — liveness check
  GET  /status — shows current connection state

Usage:
    python mock_app.py --port 8099
"""

import argparse
import json
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

state = {
    "connections": 8,
    "status": "healthy",
    "drain_received_at": None,
    "reconnected_at": None,
}


class AppHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if "/drain" in self.path:
            content_len = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_len).decode() if content_len else "{}"

            now = datetime.now().isoformat()
            state["drain_received_at"] = now
            state["status"] = "draining"

            print(f"\n{'='*60}")
            print(f"[APP] ⚡ DRAIN SIGNAL RECEIVED at {now}")
            print(f"[APP]    Payload: {body}")
            print(f"[APP]    Closing {state['connections']} connections...")
            print(f"{'='*60}")

            # Simulate graceful connection drain
            threading.Thread(target=self._simulate_drain, daemon=True).start()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "draining",
                "connections_closing": state["connections"]
            }).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if "/health" in self.path:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        elif "/status" in self.path:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(state, indent=2).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass

    def _simulate_drain(self):
        """Simulate closing connections one by one, then reconnecting."""
        for i in range(state["connections"], 0, -1):
            time.sleep(0.3)
            state["connections"] = i - 1
            print(f"[APP]    Connections remaining: {i - 1}")

        print(f"[APP] ✅ All connections closed.")
        time.sleep(1)

        # Reconnect
        state["connections"] = 8
        state["status"] = "healthy"
        state["reconnected_at"] = datetime.now().isoformat()
        print(f"[APP] ✅ Reconnected with {state['connections']} fresh connections")
        print(f"[APP]    New connections route through healthy backend")
        print()


def main():
    parser = argparse.ArgumentParser(description="Mock application with drain endpoint")
    parser.add_argument("--port", type=int, default=8099, help="Listen port")
    args = parser.parse_args()

    server = HTTPServer(("0.0.0.0", args.port), AppHandler)
    print(f"[APP] Mock application running on http://localhost:{args.port}")
    print(f"[APP] Endpoints:")
    print(f"[APP]   POST /drain  — receive drain notifications")
    print(f"[APP]   GET  /health — liveness check")
    print(f"[APP]   GET  /status — connection state")
    print(f"[APP]")
    print(f"[APP] Simulating {state['connections']} active TCP connections")
    print()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[APP] Shutting down.")


if __name__ == "__main__":
    main()
