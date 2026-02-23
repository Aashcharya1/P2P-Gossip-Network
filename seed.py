import socket
import threading
import sys
import json
from common import *
import peer

# Global storage for the network state
peer_list = set()  # Final list of verified peers
votes = {}  # Dictionary to track how many seeds have "voted" for a peer
# Format:
# {
#   "peerIP:PORT": set(seed_ports_that_voted)
# }
lock = threading.Lock() # Prevents data corruption when multiple threads edit votes/peer_list

dead_reports = {}
# Format:
# {
#   "IP:PORT": set(seed_ports_that_reported)
# }

def load_seeds():
    """Reads the IP:Port of all authoritative seeds from a config file."""
    seeds = []
    with open("config.txt") as f:
        for line in f:
            ip, port = line.strip().split(":")
            seeds.append((ip, int(port)))
    return seeds

SEEDS = load_seeds()

# Command-line arguments: e.g., 'python seed.py 127.0.0.1 5000'
HOST = sys.argv[1]
PORT = int(sys.argv[2])

def broadcast_to_seeds(message):
    """
    When this seed gets a registration, it tells ALL OTHER seeds to vote.
    This creates a distributed consensus.
    """
    for ip, port in SEEDS:
        if port == PORT: # Don't send the message to yourself
            continue
        try:
            s = socket.socket() # socket creation
            s.settimeout(3) # prevents hanging if seed is down
            s.connect((ip, port))
            send_json(s, message)
            s.close()
        except:
            pass # If a seed is down, just skip it

def handle_client(conn, addr):
    """The main logic for processing incoming network requests.
    Handles incoming peer and seed messages.
    Defensive parsing ensures resilience against malformed input.
    """
    data = recv_json(conn)

    if not data:
        conn.close()
        return

    msg_type = data.get("type")

    # A new peer wants to join the network
    if msg_type == "REGISTER":
        peer = data.get("peer")
        if not peer:
            conn.close()
            return
        
        with lock: # Thread-safe: ensure only one thread modifies 'votes' at a time
            if peer not in votes: # Initialize the vote set for this peer if it doesn't exist
                votes[peer] = set()

            votes[peer].add(PORT)  # Add yourself 
        
        # Tell other seeds: "Hey, I saw this peer, you should vote for them too!"
        # This is how we achieve distributed consensus: if a majority of seeds report seeing this peer, we consider it verified and add it to the peer list.
        broadcast_to_seeds({
            "type": "VOTE",
            "peer": peer,
            "seed": PORT
        })

    # Another seed is telling us they saw a peer
    elif msg_type == "VOTE":
        peer = data.get("peer")
        reporting_seed = data.get("seed") #data.get - returns None if "seed" key is not present, preventing KeyError

        if not peer or reporting_seed is None:
            conn.close()
            return
        
        with lock:
            if peer not in votes:
                votes[peer] = set() #creates an empty set to track votes for this peer

            votes[peer].add(reporting_seed) # Add the reporting seed's port to the vote set for this peer

            if len(votes[peer]) >= (len(SEEDS)//2 + 1): #if majority of seeds have voted for this peer
                if peer not in peer_list:
                    peer_list.add(peer)
                    print(f"[CONSENSUS] Peer added: {peer}")

    # A peer is asking "Who else is online?"
    elif msg_type == "GET_PEERS":
        with lock:
            current_peers = list(peer_list) # convert set to list for JSON serialization

        send_json(conn, {
            "type": "PEER_LIST",
            "peers": current_peers
        })
        # this version of GET_PEERS prevents race condition during removal 
        # race condition is possible if a seed is in the process of removing a peer
        # while another seed is sending the peer list (could lead to inconsistent views of the network)
        
    elif msg_type == "DEAD_REPORT":
        target = data.get("target")
        if not target:
            conn.close()
            return
        print(f"[DEAD REPORT RECEIVED] {target}")

        with lock:
            if target not in dead_reports:
                dead_reports[target] = set()

            if PORT not in dead_reports[target]:
                dead_reports[target].add(PORT)
                #if this seed hasn't already voted that this peer is dead
                #broadcast to other seeds that this peer is dead, so they can vote too => faster consensus


        broadcast_to_seeds({
            "type": "SEED_DEAD_VOTE",
            "target": target,
            "seed": PORT
        })

    elif msg_type == "SEED_DEAD_VOTE":
        target = data.get("target")
        reporting_seed = data.get("seed")

        if not target or reporting_seed is None:
            conn.close()
            return

        with lock:
            if target not in dead_reports:
                dead_reports[target] = set()

            dead_reports[target].add(reporting_seed)

            if len(dead_reports[target]) >= (len(SEEDS)//2 + 1):
                if target in peer_list:
                    peer_list.remove(target)
                    print(f"[SEED CONSENSUS] Removed dead peer: {target}")

                # Cleanup to prevent reprocessing
                del dead_reports[target]

    conn.close()

def start_server():
    """Starts the TCP server to listen for peer and seed connections."""
    server = socket.socket()
    server.bind((HOST, PORT))
    server.listen()
    print(f"[SEED] Running at {HOST}:{PORT}")

    while True:
        # Every time a new connection arrives, spin up a new thread to handle it
        conn, addr = server.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()

if __name__ == "__main__":
    start_server()