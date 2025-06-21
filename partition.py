# partition.py
import os
import struct
import json
import threading
from serializer import Serializer

class Partition:
    HOT_STORAGE_LIMIT = 5

    def __init__(self, partition_id: int, data_dir: str, node, role: str):
        self.partition_id = partition_id
        self.data_dir = os.path.join(data_dir, f"partition_{partition_id}")
        self.log_file_path = os.path.join(self.data_dir, "segment.log")
        self.node = node
        self.role = role
        self.serializer = Serializer()
        self.hot_storage = {}
        self.cold_storage_index = {}
        self.lock = threading.Lock()
        os.makedirs(self.data_dir, exist_ok=True)
        self._load_index_from_log()

    def _load_index_from_log(self):
        with self.lock:
            if not os.path.exists(self.log_file_path): return
            with open(self.log_file_path, 'rb') as f:
                offset = 0
                while True:
                    len_bytes = f.read(4)
                    if not len_bytes: break
                    record_len, = struct.unpack('!I', len_bytes)
                    record_bytes = f.read(record_len)
                    if len(record_bytes) < record_len: break
                    key_len, = struct.unpack('!I', record_bytes[:4])
                    key = record_bytes[4:4+key_len].decode('utf-8')
                    self.cold_storage_index[key] = offset
                    offset += (4 + record_len)

    def put(self, key: str, value: any):
        with self.lock:
            self.hot_storage[key] = value
            should_flush = len(self.hot_storage) >= self.HOT_STORAGE_LIMIT
        
        if should_flush:
            self._flush_hot_to_cold()

        if self.role == 'leader':
            self.node.replicate_to_followers(self.partition_id, key, value)
            
    def _flush_hot_to_cold(self):
        with self.lock:
            if not self.hot_storage:
                return
            items_to_flush = dict(self.hot_storage)
            self.hot_storage.clear()

        if not items_to_flush:
            return

        with open(self.log_file_path, 'ab') as f:
            for key, value in items_to_flush.items():
                offset = f.tell()
                with self.lock:
                    self.cold_storage_index[key] = offset
                
                key_bytes = key.encode('utf-8')
                value_bytes = self.serializer.encode_value(value)
                record_bytes = struct.pack(f'!I{len(key_bytes)}s', len(key_bytes), key_bytes) + value_bytes
                f.write(struct.pack('!I', len(record_bytes)))
                f.write(record_bytes)

    def get(self, key: str) -> any:
        with self.lock:
            if key in self.hot_storage: return self.hot_storage[key]
            if key in self.cold_storage_index:
                offset = self.cold_storage_index[key]
                with open(self.log_file_path, 'rb') as f:
                    f.seek(offset)
                    len_bytes = f.read(4)
                    if not len_bytes: return None
                    record_len, = struct.unpack('!I', len_bytes)
                    record_bytes = f.read(record_len)
                    key_len, = struct.unpack('!I', record_bytes[:4])
                    value_bytes = record_bytes[4 + key_len:]
                    decoded = self.serializer.decode_value(value_bytes)
                    if 'data' in decoded: return decoded
                    else: return decoded.get('value')
        return None
    
    def get_key_location(self, key: str) -> str:
        """Mengecek lokasi sebuah kunci."""
        with self.lock:
            if key in self.hot_storage:
                return "HOT_STORAGE"
            elif key in self.cold_storage_index:
                return "COLD_STORAGE"
            else:
                return "NOT_FOUND"
            
    def close(self):
        print(f"Partition-{self.partition_id} on Node-{self.node.node_id}: Flushing remaining data before shutdown...")
        self._flush_hot_to_cold()