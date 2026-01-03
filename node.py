import json
import time
import argparse
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import urllib.request
import urllib.parse

class Node:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers
        self.clock = 0
        self.store = {}  # key -> {"value":..., "ts":..., "origin":...}
        self.lock = threading.Lock()

    # Lamport: local event
    def tick(self):
        with self.lock:
            self.clock += 1
            return self.clock

    # Lamport: receive message with timestamp t
    def update_clock(self, t):
        with self.lock:
            self.clock = max(self.clock, int(t)) + 1
            return self.clock

    def get_clock(self):
        with self.lock:
            return self.clock

    def apply_update(self, key, value, ts, origin):
        ts = int(ts)
        with self.lock:
            cur = self.store.get(key)
            if (cur is None or
                ts > cur["ts"] or
                (ts == cur["ts"] and origin > cur["origin"])):
                self.store[key] = {"value": value, "ts": ts, "origin": origin}
                print(f"[{self.node_id}] APPLY {key}={value} ts={ts} from {origin} clock={self.clock}")

    # Lamport: send message (tick + attach)
    def replicate_to_peers(self, payload):
        for peer in self.peers:
            peer = peer.strip().rstrip("/")
            if not peer:
                continue

            # Scenario A: delay only A -> C (C uses port 8002)
            if self.node_id == "A" and peer.endswith(":8002"):
                print("[A] Artificial delay A -> C (2s)")
                time.sleep(2)

            for attempt in range(1, 4):
                try:
                    data = json.dumps(payload).encode("utf-8")
                    req = urllib.request.Request(
                        peer + "/replicate",
                        data=data,
                        headers={"Content-Type": "application/json"},
                        method="POST"
                    )
                    urllib.request.urlopen(req, timeout=5).read()
                    print(f"[{self.node_id}] Replicated to {peer} attempt={attempt}")
                    break
                except Exception as e:
                    print(f"[{self.node_id}] Replication to {peer} failed attempt={attempt}: {repr(e)}")
                    time.sleep(0.5 * attempt)

    # Manual pull sync (for outage recovery demo)
    def sync_from_peers(self):
        for peer in self.peers:
            peer = peer.strip().rstrip("/")
            if not peer:
                continue
            try:
                raw = urllib.request.urlopen(peer + "/dump", timeout=5).read().decode("utf-8")
                data = json.loads(raw) if raw else {}
                peer_clock = int(data.get("clock", 0))
                peer_store = data.get("store", {})

                # receive event
                self.update_clock(peer_clock)

                # merge store using LWW rules
                for k, v in peer_store.items():
                    if isinstance(v, dict) and "value" in v and "ts" in v and "origin" in v:
                        self.apply_update(k, v["value"], v["ts"], v["origin"])

                print(f"[{self.node_id}] SYNC from {peer} OK")
            except Exception as e:
                print(f"[{self.node_id}] SYNC from {peer} failed: {repr(e)}")

node = None

class Handler(BaseHTTPRequestHandler):
    def _send_json(self, code, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        global node
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        data = json.loads(body) if body else {}

        if self.path == "/put":
            key = data.get("key")
            value = data.get("value")
            if key is None:
                return self._send_json(400, {"error": "key is required"})

            # local event
            ts = node.tick()

            # apply locally (origin is this node)
            node.apply_update(key, value, ts, node.node_id)

            # send event: tick before sending and attach timestamp
            send_ts = node.tick()
            payload = {"key": key, "value": value, "ts": send_ts, "origin": node.node_id}

            threading.Thread(target=node.replicate_to_peers, args=(payload,), daemon=True).start()
            return self._send_json(200, {"status": "OK", "key": key, "value": value, "ts": send_ts, "origin": node.node_id})

        if self.path == "/replicate":
            key = data.get("key")
            value = data.get("value")
            ts = data.get("ts")
            origin = data.get("origin")

            if key is None or ts is None or origin is None:
                return self._send_json(400, {"error": "key, ts, origin required"})

            # receive event
            node.update_clock(ts)
            node.apply_update(key, value, ts, origin)
            return self._send_json(200, {"status": "OK"})

        if self.path == "/sync":
            # manual sync for Scenario C
            threading.Thread(target=node.sync_from_peers, daemon=True).start()
            return self._send_json(200, {"status": "SYNC_STARTED"})

        self._send_json(404, {"error": "not found"})

    def do_GET(self):
        global node

        if self.path.startswith("/get"):
            qs = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(qs)
            key = params.get("key", [None])[0]
            if key is None:
                return self._send_json(400, {"error": "key query param required"})
            return self._send_json(200, {"key": key, "record": node.store.get(key)})

        if self.path == "/status":
            return self._send_json(200, {
                "node": node.node_id,
                "lamport": node.get_clock(),
                "store": node.store
            })

        if self.path == "/dump":
            # internal endpoint for sync
            return self._send_json(200, {
                "node": node.node_id,
                "clock": node.get_clock(),
                "store": node.store
            })

        self._send_json(404, {"error": "not found"})

def main():
    global node
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peers", required=True)
    args = parser.parse_args()

    peers = [p.strip() for p in args.peers.split(",") if p.strip()]
    node = Node(args.id, peers)

    server = HTTPServer(("0.0.0.0", args.port), Handler)
    print(f"Node {args.id} running on port {args.port}")
    print(f"Peers: {peers}")
    server.serve_forever()

if __name__ == "__main__":
    main()