# test.py

import os
import shutil
import time
import multiprocessing
import hashlib
from coordinator import Coordinator
from network import send_request
from config import CLUSTER_TOPOLOGY
from node import start_node_process

def find_keys_for_partition(target_partition_id, num_keys, num_partitions):
    """Fungsi helper untuk mencari kunci yang cocok untuk partisi target."""
    keys = []
    i = 0
    while len(keys) < num_keys:
        key_candidate = f"testkey:{i}"
        hash_val = int(hashlib.sha1(key_candidate.encode('utf-8')).hexdigest(), 16)
        partition_id = hash_val % num_partitions
        if partition_id == target_partition_id:
            keys.append(key_candidate)
        i += 1
    return keys

def run_replication_test():
    print("--- MULAI PENGUJIAN AKHIR (VERSI DINAMIS) ---\n")
    
    data_dir = "data"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    # Hapus direktori data lama untuk semua node yang ada di konfigurasi
    for node_id in CLUSTER_TOPOLOGY['nodes']:
        dir_path = f"data/node_{node_id}"
        if os.path.exists(dir_path): shutil.rmtree(dir_path)
    

    # Jalankan semua node di proses terpisah
    processes = []
    for node_id, info in CLUSTER_TOPOLOGY['nodes'].items():
        process = multiprocessing.Process(target=start_node_process, args=(node_id, info['host'], info['port'], CLUSTER_TOPOLOGY))
        processes.append(process)
        process.start()
        print(f"Starting Node-{node_id} process...")

    print("\nWaiting for all nodes to start...")
    time.sleep(2)

    coordinator = Coordinator(CLUSTER_TOPOLOGY)
    
    num_partitions = len(CLUSTER_TOPOLOGY['partitions'])
    all_keys = {} # Untuk menyimpan kunci yang kita PUT agar bisa di-GET nanti

    # Loop untuk setiap partisi yang didefinisikan di config.py
    for i in range(num_partitions):
        print(f"\n--- Mencari dan Melakukan PUT untuk Memicu Flush di Partisi {i} ---")
        keys_for_p = find_keys_for_partition(i, 5, num_partitions)
        all_keys[i] = keys_for_p # Simpan daftar kunci untuk partisi ini
        print(f"Kunci untuk P{i}: {keys_for_p}")
        for key in keys_for_p:
            coordinator.put(key, {"data": f"ini adalah nilai untuk {key}"})
    
    print("\nWaiting for replication & flush to complete...")
    time.sleep(2)
    
    print("\n--- Melakukan Operasi GET untuk Verifikasi ---")
    # Loop lagi untuk memverifikasi GET di setiap partisi
    for i in range(num_partitions):
        key_to_get = all_keys[i][2] # Ambil kunci ke-3 dari daftar sebagai sampel
        value = coordinator.get(key_to_get)
        print(f"GET {key_to_get} (from P{i}) -> {value}")
        assert value['data'] == f"ini adalah nilai untuk {key_to_get}"
    print("✅  GET requests successful for all partitions.")
    
    print("\n--- Sending SHUTDOWN command to all nodes ---")
    for node_id, info in CLUSTER_TOPOLOGY['nodes'].items():
        send_request(info['host'], info['port'], "SHUTDOWN")
    time.sleep(1)

    print("\n--- Verifikasi File Fisik pada Leader dan Follower ---")
    # Loop untuk memverifikasi file di setiap partisi
    for i in range(num_partitions):
        roles = CLUSTER_TOPOLOGY['partitions'][i]
        leader_node = roles['leader']
        
        # Cek file di leader
        leader_file = f"data/node_{leader_node}/partition_{i}/segment.log"
        assert os.path.exists(leader_file)
        
        # Cek file di semua follower
        for follower_node in roles['followers']:
            follower_file = f"data/node_{follower_node}/partition_{i}/segment.log"
            assert os.path.exists(follower_file)
        print(f"✅  Data untuk Partisi {i} tereplikasi dengan benar.")

    # =================================================================

    print("\n--- Menutup proses utama ---")
    for p in processes:
        if p.is_alive(): p.terminate()
        
    print("\n\n**********************************************")
    print("      SELURUH SISTEM BERHASIL DIUJI!")
    print("**********************************************")

if __name__ == "__main__":
    run_replication_test()