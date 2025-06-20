# coordinator.py

import hashlib
from partition import Partition
from typing import List

class Coordinator:
    """
    Bertindak sebagai koordinator partisi.
    Mengatur partisi-partisi dan memindahkan permintaan ke partisi yang sesuai.
    """
    def __init__(self, num_partitions: int, data_dir: str = "data"):
        if num_partitions <= 0:
            raise ValueError("Number of partitions must be positive.")

        self.num_partitions = num_partitions
        self.data_dir = data_dir
        
        # Inisialisasi semua partisi yang akan dikelola oleh koordinator ini
        print(f"Initializing coordinator with {num_partitions} partitions...")
        self.partitions: List[Partition] = []
        for i in range(num_partitions):
            partition = Partition(partition_id=i, data_dir=self.data_dir)
            self.partitions.append(partition)
        print("All partitions initialized.")

    def _get_partition_for_key(self, key: str) -> Partition:
        """
        Menentukan partisi yang sesuai untuk suatu kunci.
        """
        # Gunakan SHA1 untuk hash yang baik, lalu konversi ke integer
        hash_val = int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16)
        
        # Terapkan formula: hash(key) % N
        partition_id = hash_val % self.num_partitions
        
        # Kembalikan instance partisi yang sesuai
        return self.partitions[partition_id]

    def put(self, key: str, value: any):
        """
        Me-route permintaan PUT ke partisi yang sesuai.
        """
        target_partition = self._get_partition_for_key(key)
        print(f"Coordinator: Routing key '{key}' to Partition {target_partition.partition_id}")
        target_partition.put(key, value)

    def get(self, key: str) -> any:
        """
        Me-routing permintaan GET ke partisi yang sesuai.
        """
        target_partition = self._get_partition_for_key(key)
        print(f"Coordinator: Routing key '{key}' to Partition {target_partition.partition_id}")
        return target_partition.get(key)

    def close(self):
        """
        Menutup semua partisi.
        """
        print("\nCoordinator: Closing all partitions...")
        for partition in self.partitions:
            partition.close()
        print("Coordinator: All partitions closed.")