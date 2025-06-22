# node.py
import sys, os, shutil, time, socketserver, threading, json
from partition import Partition
from network import send_request
from config import CLUSTER_TOPOLOGY

class NodeTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            data = self.request.recv(1024).strip().decode('utf-8')
            if not data: return
            parts = data.split(' ', 3); command = parts[0].upper()
            response = "ERROR: Invalid command"
            if command == 'PUT' and len(parts) == 4:
                p_id, key, val_str = int(parts[1]), parts[2], parts[3]
                response = self.server.node.handle_put(p_id, key, json.loads(val_str))
            elif command == 'GET' and len(parts) == 3:
                p_id, key = int(parts[1]), parts[2]
                response = self.server.node.handle_get(p_id, key)
            elif command == 'REPLICATE' and len(parts) == 4:
                p_id, key, val_str = int(parts[1]), parts[2], parts[3]
                response = self.server.node.handle_replicate(p_id, key, json.loads(val_str))
            elif command == 'STATUS' and len(parts) == 3:
                p_id, key = int(parts[1]), parts[2]
                response = self.server.node.handle_status(p_id, key)
            elif command == 'INSPECT':
                response = self.server.node.handle_inspect()
            elif command == 'HEX' and len(parts) == 3:
                p_id, key = int(parts[1]), parts[2]
                response = self.server.node.handle_hex(p_id, key)
            elif command == 'SHUTDOWN':
                self.server.node.close()
                self.request.sendall(b"SUCCESS: Shutting down.")
                self.server.shutdown()
                return
            self.request.sendall(response.encode('utf-8'))
        except Exception as e:
            self.request.sendall(f"SERVER_ERROR: {e}".encode('utf-8'))

class Node:
    def __init__(self, node_id, host, port, cluster_topology):
        self.node_id=node_id; self.host=host; self.port=port
        self.cluster_topology=cluster_topology; self.replicas = {}
        data_dir = f"data/node_{node_id}"
        for p_id, roles in cluster_topology['partitions'].items():
            if roles['leader'] == node_id: self.replicas[p_id] = Partition(p_id, data_dir, self, 'leader')
            elif node_id in roles['followers']: self.replicas[p_id] = Partition(p_id, data_dir, self, 'follower')
    def start_server(self):
        server = socketserver.ThreadingTCPServer((self.host, self.port), NodeTCPHandler)
        server.daemon_threads = True
        server.node = self; self.server = server
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True; server_thread.start()
        print(f"Node-{self.node_id} server running at {self.host}:{self.port}")
    def close(self):
        for partition in self.replicas.values(): partition.close()
    def handle_put(self, p_id, key, value):
        partition = self.replicas.get(p_id)
        if partition and partition.role == 'leader':
            partition.put(key, value); return "SUCCESS: Put data to leader."
        return "ERROR: Not a leader for this partition."
    def handle_get(self, p_id, key):
        partition = self.replicas.get(p_id)
        return json.dumps(partition.get(key)) if partition else "ERROR: Partition not found."
    def handle_replicate(self, p_id, key, value):
        partition = self.replicas.get(p_id)
        if partition and partition.role == 'follower':
            partition.put(key, value); return "SUCCESS: Replicated data."
        return "ERROR: Not a follower."
    def replicate_to_followers(self, p_id, key, value):
        roles = self.cluster_topology['partitions'].get(p_id)
        if not roles: return
        msg = f"REPLICATE {p_id} {key} {json.dumps(value)}"
        for f_id in roles['followers']:
            info = self.cluster_topology['nodes'][f_id]
            t = threading.Thread(target=send_request, args=(info['host'], info['port'], msg))
            t.daemon = True; t.start()
    def handle_status(self, p_id, key):
        """Menangani permintaan status dan mendelegasikannya ke partisi."""
        partition = self.replicas.get(p_id)
        if partition:
            return partition.get_key_location(key)
        return "ERROR: Partition not found on this node."
    def handle_inspect(self):
        """Mengumpulkan dan mengembalikan isi dari semua hot storage di node ini."""
        hot_storage_summary = {}
        for p_id, partition in self.replicas.items():
            # Mengambil salinan hot_storage dengan aman menggunakan lock
            with partition.lock:
                hot_storage_summary[f"partition_{p_id}"] = list(partition.hot_storage.keys())
        return json.dumps(hot_storage_summary, indent=2)
    
    def handle_hex(self, p_id, key):
        """Menangani permintaan hex dan mendelegasikannya ke partisi."""
        partition = self.replicas.get(p_id)
        if partition:
            raw_bytes = partition.get_raw_value_bytes(key)
            return raw_bytes.hex() if raw_bytes else "NOT_FOUND"
        return "ERROR: Partition not found on this node."

def start_node_process(node_id, host, port, topology):
    node = Node(node_id, host, port, topology); node.start_server()
    try: node.server.serve_forever()
    finally: print(f"\nNode-{node_id} process finished.")

if __name__ == "__main__":
    if len(sys.argv) != 2: sys.exit(f"Usage: python {sys.argv[0]} <node_id>")
    node_id = int(sys.argv[1])
    info = CLUSTER_TOPOLOGY['nodes'].get(node_id)
    if not info: sys.exit(f"Error: Node ID {node_id} not found.")
    start_node_process(node_id, info['host'], info['port'], CLUSTER_TOPOLOGY)