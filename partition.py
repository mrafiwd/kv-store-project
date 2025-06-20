# partition.py

import os
import struct
from serializer import Serializer

class Partition:
    """
    Mengatur sebuah partisi dari data, termasuk hot (in-memory) dan cold (on-disk) storage.
    """
    # Batas sederhana untuk memicu flush dari hot ke cold storage
    HOT_STORAGE_LIMIT = 5

    def __init__(self, partition_id: int, data_dir: str = "data"):
        self.partition_id = partition_id
        self.data_dir = os.path.join(data_dir, f"partition_{partition_id}")
        self.log_file_path = os.path.join(self.data_dir, "segment.log")
        
        self.serializer = Serializer()
        
        # Hot Storage: cache di memori untuk data yang baru ditulis
        self.hot_storage = {}
        
        # Indeks untuk cold storage: mapping key -> byte offset di file log
        self.cold_storage_index = {}

        # Pastikan direktori data ada
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Muat indeks dari file log yang ada saat startup
        self._load_index_from_log()

    def _load_index_from_log(self):
        """
        Membaca indeks cold storage dari file log.
        Ini memungkinkan partisi untuk memulai dari kondisi sebelumnya.
        """
        if not os.path.exists(self.log_file_path):
            return

        with open(self.log_file_path, 'rb') as f:
            current_offset = 0
            while True:
                # Baca 4 byte pertama untuk panjang record
                len_bytes = f.read(4)
                if not len_bytes:
                    break  # End of file
                
                record_len, = struct.unpack('!I', len_bytes)
                
                # Baca sisa record (tanpa panjang record itu sendiri)
                record_bytes = f.read(record_len)
                if len(record_bytes) < record_len:
                    break # Incomplete record at the end
                
                # Ekstrak panjang key
                key_len, = struct.unpack('!I', record_bytes[:4])
                # Ekstrak key
                key = record_bytes[4:4+key_len].decode('utf-8')
                
                # Simpan key dan offsetnya di indeks
                self.cold_storage_index[key] = current_offset
                
                # Pindah ke record berikutnya
                current_offset += (4 + record_len)

        print(f"Partition {self.partition_id}: Index loaded. {len(self.cold_storage_index)} keys in cold storage.")

    def put(self, key: str, value: any):
        """
        Menambahkan key-value pair ke partisi.
        Key-value pair akan disimpan di hot storage (in-memory) dan di flush ke cold storage jika hot storage penuh.
        """
        self.hot_storage[key] = value
        print(f"Partition {self.partition_id}: Put key '{key}' to hot storage.")

        # Jika hot storage penuh, flush ke disk
        if len(self.hot_storage) >= self.HOT_STORAGE_LIMIT:
            self._flush_hot_to_cold()

    def _flush_hot_to_cold(self):
        """
        Menulis data dari hot storage ke cold storage.
        """
        print(f"Partition {self.partition_id}: Hot storage limit reached, flushing to disk...")
        if not self.hot_storage:
            return

        with open(self.log_file_path, 'ab') as f:
            for key, value in self.hot_storage.items():
                current_offset = f.tell()

                key_bytes = key.encode('utf-8')
                value_bytes = self.serializer.encode_value(value)

                # Format record: [key_len (4b)] [key] [value_bytes]
                record_bytes = struct.pack(f'!I{len(key_bytes)}s', len(key_bytes), key_bytes) + value_bytes
                
                # Format file: [total_record_len (4b)] [record_bytes]
                f.write(struct.pack('!I', len(record_bytes)))
                f.write(record_bytes)
                
                # Update indeks cold storage dengan offset yang baru
                self.cold_storage_index[key] = current_offset
                print(f"  - Flushed key '{key}' to offset {current_offset}")

        # Kosongkan hot storage setelah flush berhasil
        self.hot_storage.clear()
        print(f"Partition {self.partition_id}: Flush complete. Hot storage is now empty.")

    def get(self, key: str) -> any:
        """
        Gets a value by key. Returns the original value consistently,
        whether from hot or cold storage.
        """
        # 1. Cek di hot storage (cache)
        if key in self.hot_storage:
            print(f"Partition {self.partition_id}: Get key '{key}' from hot storage (HIT).")
            return self.hot_storage[key]
        
        # 2. Cek di indeks cold storage
        if key in self.cold_storage_index:
            print(f"Partition {self.partition_id}: Get key '{key}' from cold storage (MISS).")
            offset = self.cold_storage_index[key]

            with open(self.log_file_path, 'rb') as f:
                f.seek(offset)
                len_bytes = f.read(4)
                if not len_bytes: return None
                record_len, = struct.unpack('!I', len_bytes)
                record_bytes = f.read(record_len)
                
                key_len, = struct.unpack('!I', record_bytes[:4])
                value_bytes = record_bytes[4 + key_len:]
                
                # Decode value dari biner
                decoded_wrapper = self.serializer.decode_value(value_bytes)
                
                # --- Perubahan Logika ada di sini ---
                # Kembalikan nilai asli, bukan wrapper-nya
                if decoded_wrapper['schema_version'] == 1:
                    return decoded_wrapper['value']
                elif decoded_wrapper['schema_version'] == 2:
                    return {
                        'data': decoded_wrapper['data'],
                        'timestamp': decoded_wrapper['timestamp']
                    }
                # ------------------------------------

        print(f"Partition {self.partition_id}: Key '{key}' not found.")
        return None

    def close(self):
        """Mem-flush data yang tersisa dari hot storage."""
        self._flush_hot_to_cold()