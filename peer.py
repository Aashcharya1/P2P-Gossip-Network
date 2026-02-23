import socket
import threading
import time
import sys
import random
from common import *

HOST = sys.argv[1]
PORT = int(sys.argv[2])


# Tracks suspected nodes and who confirmed
suspicions = {}
# Format:
# {
#   "IP:PORT": {
#       "votes": set(),
#       "start_time": timestamp
#   }
# }

neighbors = set()      # Other peers we are connected to
message_list = set()   # History of message hashes (to prevent infinite loops)
lock = threading.Lock()

def load_seeds():
    seeds = []
    with open("config.txt") as f:
        for line in f:
            ip, port = line.strip().split(":")
            seeds.append((ip, int(port)))
    return seeds

SEEDS = load_seeds()

def register():
    """Pick a majority of seeds and tell them 'I am online'."""
    # We choose a random majority (n/2 + 1) of seeds to register with
    chosen = random.sample(SEEDS, len(SEEDS)//2 + 1)
    for ip, port in chosen:
        try:
            s = socket.socket()
            s.settimeout(3) # prevents hanging if seed is down

            s.connect((ip, port))
            send_json(s, {
                "type": "REGISTER",
                "peer": f"{HOST}:{PORT}"
            })
            s.close()
        except:
            pass

def fetch_peers():
    """Ask seeds for the current list of verified peers in the network."""
    all_peers = set()
    for ip, port in SEEDS:
        try:
            s = socket.socket()
            s.settimeout(3)

            s.connect((ip, port))
            send_json(s, {"type": "GET_PEERS"})
            data = recv_json(s)
            if data and data["type"] == "PEER_LIST":
                for p in data["peers"]:
                    if p != f"{HOST}:{PORT}": # Don't add yourself as a neighbor
                        all_peers.add(p)
            s.close()
        except:
            pass
    return all_peers

def connect_neighbors(peers):
    for p in peers:
        neighbors.add(p)

def broadcast(msg):
    """The core of the Gossip Protocol."""
    h = hash_message(msg) # Generate a unique fingerprint for the message

    with lock:
        # CRITICAL: If we have already seen this hash, stop! 
        # This prevents the message from bouncing back and forth forever.
        if h in message_list:
            return
        message_list.add(h)

    with lock:
        current_neighbors = list(neighbors)
    
    # Forward the message to every known neighbor
    for p in current_neighbors:
        ip, port = p.split(":")
        try:
            s = socket.socket()
            s.settimeout(3)

            s.connect((ip, int(port)))
            send_json(s, {"type": "GOSSIP", "message": msg})
            s.close()
        except:
            pass

def gossip_sender():
    """Background thread that generates 10 unique messages over time."""
    count = 0
    while count < 10:
        time.sleep(5)
        msg = f"{time.time()}:{HOST}:{count}" # Create a unique timestamped message
        broadcast(msg)
        count += 1

def report_dead_to_seeds(target):
    """
    After peer-level majority agreement,
    notify connected seeds.
    """
    for ip, port in SEEDS:
        try:
            s = socket.socket()
            s.settimeout(3)

            s.connect((ip, port))
            send_json(s, {
                "type": "DEAD_REPORT",
                "target": target
            })
            s.close()
        except:
            pass

def handle_client(conn, addr):
    """
    Handles incoming peer-to-peer messages.
    Defensive parsing is used to prevent crashes due to malformed
    or partial network data.
    """
    data = recv_json(conn)

    # If connection closed abruptly or invalid JSON received
    if not data:
        conn.close()
        return

    # Use .get() to avoid KeyError if "type" is missing
    msg_type = data.get("type")

    # GOSSIP 
    if msg_type == "GOSSIP":
        msg = data.get("message")
        if not msg:
            conn.close()
            return

        print(f"[GOSSIP RECEIVED] {msg}")
        broadcast(msg)

    # SUSPECT REQUEST - A peer is asking "Can you reach this target node?"
    elif msg_type == "SUSPECT_REQUEST":
        target = data.get("target")
        requester = data.get("requester")

        # Validate required fields
        if not target or not requester:
            conn.close()
            return

        alive = check_node_alive(target) # Attempt direct ping to target to see if it's alive

        response = {
            "type": "SUSPECT_RESPONSE",
            "target": target,
            "alive": alive,
            "responder": f"{HOST}:{PORT}"
        }

        ip, port = requester.split(":")
        try:
            s = socket.socket()
            s.settimeout(3)
            s.connect((ip, int(port)))
            send_json(s, response)
            s.close()
        except:
            pass  # If requester is gone, ignore

    # SUSPECT RESPONSE 
    elif msg_type == "SUSPECT_RESPONSE":
        target = data.get("target")
        alive = data.get("alive")
        responder = data.get("responder")

        # Validate fields
        if target is None or alive is None or responder is None:
            conn.close()
            return

        if not alive:
            with lock:
                if target in suspicions:
                    suspicions[target]["votes"].add(responder)

                    total_voters = len(neighbors) - 1
                    required = total_voters // 2 + 1

                    if len(suspicions[target]["votes"]) >= required:
                        print(f"[PEER CONSENSUS] {target} confirmed dead")

                        neighbors.discard(target)
                        del suspicions[target]

                        report_dead_to_seeds(target)

    # Unknown message types are ignored safely
    conn.close()


def start_server():
    server = socket.socket()
    server.bind((HOST, PORT))
    server.listen()

    print(f"[PEER] Running at {HOST}:{PORT}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()


PING_TIMEOUT = 3
SUSPICION_TIMEOUT = 5


def ping_neighbors():
    """
    Periodically checks neighbor liveness.
    If ping fails, initiate suspicion protocol.
    """
    while True:
        time.sleep(5)

        for neighbor in list(neighbors):
            ip, port = neighbor.split(":")
            try:
                s = socket.socket()
                s.settimeout(3)

                s.settimeout(PING_TIMEOUT)
                s.connect((ip, int(port)))
                s.close()
            except:
                print(f"[PING FAILED] Suspecting {neighbor}")
                initiate_suspicion(neighbor)

def initiate_suspicion(target):
    """
    Starts peer-level consensus for suspected node.
    Sends SUSPECT_REQUEST to other neighbors.
    """
    with lock:
        if target in suspicions:
            return  # Already under suspicion

        suspicions[target] = {
            "votes": set(),
            "start_time": time.time()
        }

    for peer in neighbors:
        if peer == target:
            continue
        send_suspect_request(peer, target)

def send_suspect_request(peer, target):
    """
    Ask another peer if it can reach target.
    """
    ip, port = peer.split(":")
    try:
        s = socket.socket()
        s.settimeout(3)

        s.connect((ip, int(port)))
        send_json(s, {
            "type": "SUSPECT_REQUEST",
            "target": target,
            "requester": f"{HOST}:{PORT}"
        })
        s.close()
    except:
        pass

def check_node_alive(node):
    """
    Attempt direct ping.
    Returns True if reachable, False otherwise.
    """
    ip, port = node.split(":")
    try:
        s = socket.socket()
        s.settimeout(3)

        s.settimeout(PING_TIMEOUT)
        s.connect((ip, int(port)))
        s.close()
        return True
    except:
        return False
    

def suspicion_cleanup():
    while True:
        time.sleep(2)
        now = time.time()

        with lock:
            to_remove = []
            for target, data in suspicions.items():
                if now - data["start_time"] > SUSPICION_TIMEOUT:
                    print(f"[SUSPICION CLEARED] {target}")
                    to_remove.append(target)

            for t in to_remove:
                del suspicions[t]

# MAIN EXECUTION FLOW:
if __name__ == "__main__":
    # 1. Tell seeds we exist
    register()
    time.sleep(2) # Wait for seeds to finish voting
    # 2. Get the list of other active peers
    peers = fetch_peers()
    # 3. Save them as neighbors
    neighbors.update(peers)

    # 4. Start a thread to send messages every few seconds
    threading.Thread(target=gossip_sender).start()

    # 5. Start a thread to ping neighbors and check for failures
    threading.Thread(target=ping_neighbors).start()

    threading.Thread(target=suspicion_cleanup).start()

    # Start the server to receive messages from others
    start_server()