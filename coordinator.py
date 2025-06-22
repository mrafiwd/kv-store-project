# coordinator.py

import hashlib
import json
from network import send_request

class Coordinator:
    """
    Bertindak sebagai koordinator partisi.
    Mengatur partisi-partisi dan memindahkan permintaan ke partisi yang sesuai.
    """
    def __init__(self, cluster_topology):
        self.cluster_topology = cluster_topology
        self.num_partitions = len(cluster_topology['partitions'])

    def _get_leader_for_key(self, key: str):
        hash_val = int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16)
        partition_id = hash_val % self.num_partitions
        
        leader_id = self.cluster_topology['partitions'][partition_id]['leader']
        leader_info = self.cluster_topology['nodes'][leader_id]
        return partition_id, leader_info['host'], leader_info['port']

    def put(self, key: str, value: any):
        partition_id, host, port = self._get_leader_for_key(key)
        print(f"Coordinator: Routing PUT key '{key}' to leader of Partition-{partition_id} at {host}:{port}")
        
        value_str = json.dumps(value)
        message = f"PUT {partition_id} {key} {value_str}"
        return send_request(host, port, message)

    def get(self, key: str) -> any:
        partition_id, host, port = self._get_leader_for_key(key)
        print(f"Coordinator: Routing GET key '{key}' to leader of Partition-{partition_id} at {host}:{port}")
        
        message = f"GET {partition_id} {key}"
        response = send_request(host, port, message)
        
        # Cek jika respons adalah error dari network.py sebelum di-parse
        if response is None or response.startswith("Error:"):
            return response # Kembalikan pesan error apa adanya

        if response != "NOT_FOUND":
            return json.loads(response)
            
        return None
    
    def status(self, key: str):
        """Me-routing permintaan STATUS ke leader yang sesuai."""
        p_id, host, port = self._get_leader_for_key(key)
        message = f"STATUS {p_id} {key}"
        return send_request(host, port, message)
    
    def hex(self, key: str):
        """Me-routing permintaan HEX ke leader yang sesuai."""
        p_id, host, port = self._get_leader_for_key(key)
        message = f"HEX {p_id} {key}"
        return send_request(host, port, message)