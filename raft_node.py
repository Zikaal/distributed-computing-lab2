#!/usr/bin/env python3
"""
Lab 3 Starter: Raft-Lite Leader Election
Distributed Computing • 3-5 nodes
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
import argparse
import json
import threading
import time
import random
from enum import Enum
from typing import List, Optional

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────

ELECTION_TIMEOUT_MIN = 150  # ms
ELECTION_TIMEOUT_MAX = 300  # ms
HEARTBEAT_INTERVAL = 50     # ms

# ─────────────────────────────────────────────────────────────
# Node State
# ─────────────────────────────────────────────────────────────

class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

lock = threading.Lock()

NODE_ID: str = ""
PEERS: List[str] = []

# Persistent state
current_term: int = 0
voted_for: Optional[str] = None

# Volatile state
role: Role = Role.FOLLOWER
current_leader: Optional[str] = None
last_heartbeat: float = 0.0

# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def random_election_timeout() -> float:
    return random.randint(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) / 1000.0

def log(msg: str) -> None:
    print(f"[{NODE_ID}] term={current_term} role={role.value} | {msg}")

def become_follower(new_term: int, leader: Optional[str] = None) -> None:
    global role, current_term, voted_for, current_leader, last_heartbeat
    with lock:
        if new_term > current_term:
            current_term = new_term
            voted_for = None
        role = Role.FOLLOWER
        current_leader = leader
        last_heartbeat = time.time()
    log(f"became FOLLOWER (leader={leader})")

def become_candidate() -> None:
    global role, current_term, voted_for, current_leader
    with lock:
        role = Role.CANDIDATE
        current_term += 1
        voted_for = NODE_ID
        current_leader = None
    log("became CANDIDATE, starting election")

def become_leader() -> None:
    global role, current_leader
    with lock:
        role = Role.LEADER
        current_leader = NODE_ID
    log("became LEADER")

# ─────────────────────────────────────────────────────────────
# Vote RPC
# ─────────────────────────────────────────────────────────────

def handle_vote_request(term: int, candidate_id: str) -> dict:
    global current_term, voted_for, role, last_heartbeat
    with lock:
        if term < current_term:
            vote_granted = False
        else:
            if term > current_term:
                current_term = term
                voted_for = None
                role = Role.FOLLOWER
            if voted_for is None or voted_for == candidate_id:
                vote_granted = True
                voted_for = candidate_id
                last_heartbeat = time.time()
            else:
                vote_granted = False
    log(f"vote request from {candidate_id} term={term} -> granted={vote_granted}")
    return {"term": current_term, "vote_granted": vote_granted}

def request_votes() -> int:
    global current_term, voted_for
    votes = 1  # self-vote
    my_term = current_term
    for peer in PEERS:
        url = peer.rstrip("/") + "/vote"
        payload = json.dumps({"term": my_term, "candidate_id": NODE_ID}).encode()
        try:
            req = request.Request(url, data=payload,
                                  headers={"Content-Type": "application/json"},
                                  method="POST")
            with request.urlopen(req, timeout=0.1) as resp:
                data = json.loads(resp.read().decode())
                if data.get("term", 0) > my_term:
                    become_follower(data["term"])
                    return votes
                if data.get("vote_granted"):
                    votes += 1
        except Exception as e:
            log(f"vote request to {peer} failed: {e}")
    return votes

# ─────────────────────────────────────────────────────────────
# Heartbeat RPC
# ─────────────────────────────────────────────────────────────

def handle_heartbeat(term: int, leader_id: str) -> dict:
    global current_term, role, current_leader, last_heartbeat, voted_for
    with lock:
        if term < current_term:
            success = False
        else:
            if term > current_term:
                current_term = term
                voted_for = None
            current_leader = leader_id
            role = Role.FOLLOWER
            last_heartbeat = time.time()
            success = True
    log(f"heartbeat from {leader_id} term={term} -> success={success}")
    return {"term": current_term, "success": success}

def send_heartbeats() -> None:
    global current_term
    my_term = current_term
    for peer in PEERS:
        url = peer.rstrip("/") + "/heartbeat"
        payload = json.dumps({"term": my_term, "leader_id": NODE_ID}).encode()
        try:
            req = request.Request(url, data=payload,
                                  headers={"Content-Type": "application/json"},
                                  method="POST")
            with request.urlopen(req, timeout=0.1) as resp:
                data = json.loads(resp.read().decode())
                if data.get("term", 0) > my_term:
                    become_follower(data["term"])
                    return
        except Exception:
            pass

# ─────────────────────────────────────────────────────────────
# Background loops
# ─────────────────────────────────────────────────────────────

def election_loop() -> None:
    global last_heartbeat, current_term, voted_for
    last_heartbeat = time.time()
    timeout = random_election_timeout()
    while True:
        time.sleep(0.01)
        with lock:
            current_role = role
            elapsed = time.time() - last_heartbeat
        if current_role == Role.LEADER:
            continue
        if current_role == Role.FOLLOWER and elapsed > timeout:
            become_candidate()
            timeout = random_election_timeout()
        if current_role == Role.CANDIDATE:
            votes = request_votes()
            majority = (len(PEERS) + 1) // 2 + 1
            log(f"got {votes}/{len(PEERS)+1} votes (need {majority})")
            if votes >= majority:
                become_leader()
            else:
                time.sleep(random_election_timeout())
                with lock:
                    if role == Role.CANDIDATE:
                        current_term += 1
                        voted_for = NODE_ID

def leader_loop() -> None:
    while True:
        time.sleep(HEARTBEAT_INTERVAL / 1000.0)
        with lock:
            if role != Role.LEADER:
                continue
        send_heartbeats()

# ─────────────────────────────────────────────────────────────
# HTTP Handler
# ─────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, obj: dict) -> None:
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/status"):
            with lock:
                self._send(200, {
                    "ok": True,
                    "node": NODE_ID,
                    "term": current_term,
                    "role": role.value,
                    "leader": current_leader,
                    "voted_for": voted_for,
                    "peers": PEERS
                })
            return
        self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            body = json.loads(raw.decode())
        except:
            self._send(400, {"ok": False, "error": "invalid json"})
            return
        if self.path == "/vote":
            term = int(body.get("term", 0))
            candidate_id = str(body.get("candidate_id", ""))
            if not candidate_id:
                self._send(400, {"ok": False, "error": "candidate_id required"})
                return
            result = handle_vote_request(term, candidate_id)
            self._send(200, result)
            return
        if self.path == "/heartbeat":
            term = int(body.get("term", 0))
            leader_id = str(body.get("leader_id", ""))
            if not leader_id:
                self._send(400, {"ok": False, "error": "leader_id required"})
                return
            result = handle_heartbeat(term, leader_id)
            self._send(200, result)
            return
        self._send(404, {"ok": False, "error": "not found"})

    def log_message(self, fmt, *args):
        pass

# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    global NODE_ID, PEERS, last_heartbeat
    parser = argparse.ArgumentParser(description="Raft-Lite Node")
    parser.add_argument("--id", required=True, help="Node ID (A, B, C, ...)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--peers", default="", help="Comma-separated peer URLs")
    args = parser.parse_args()

    NODE_ID = args.id
    PEERS = [p.strip() for p in args.peers.split(",") if p.strip()]
    last_heartbeat = time.time()

    threading.Thread(target=election_loop, daemon=True).start()
    threading.Thread(target=leader_loop, daemon=True).start()

    log(f"starting on {args.host}:{args.port} peers={PEERS}")
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    server.serve_forever()

if __name__ == "__main__":
    main()
