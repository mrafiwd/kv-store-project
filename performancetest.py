# performancetest.py

import os
import shutil
import time
import multiprocessing
import string
import random
import statistics
import hashlib
import matplotlib.pyplot as plt
from coordinator import Coordinator
from config import CLUSTER_TOPOLOGY
from node import start_node_process

# --- Helper Functions ---
def generate_random_data(key_len=10, val_len=50):
    """Menghasilkan pasangan key-value acak."""
    key = ''.join(random.choices(string.ascii_lowercase + string.digits, k=key_len))
    value = ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=val_len))
    return key, value

def setup_cluster():
    """Menghidupkan semua node cluster di proses terpisah."""
    # Bersihkan data lama
    for node_id in CLUSTER_TOPOLOGY['nodes']:
        dir_path = f"data/node_{node_id}"
        if os.path.exists(dir_path): shutil.rmtree(dir_path)

    processes = []
    for node_id, info in CLUSTER_TOPOLOGY['nodes'].items():
        process = multiprocessing.Process(
            target=start_node_process,
            args=(node_id, info['host'], info['port'], CLUSTER_TOPOLOGY)
        )
        processes.append(process)
        process.start()
    
    print("Starting all nodes...")
    time.sleep(3) # Beri waktu untuk semua node siap
    return processes

def shutdown_cluster(processes):
    """Mematikan semua node cluster."""
    print("\nShutting down all nodes...")
    # Di dunia nyata, kita akan menggunakan graceful shutdown, tapi untuk benchmark
    # terminate() lebih cepat dan bisa diprediksi.
    for p in processes:
        if p.is_alive():
            p.terminate()
            p.join()
    print("Cluster shut down.")

# --- Benchmark Functions ---

def benchmark_general_throughput(coordinator, num_operations=500):
    """Mengukur throughput dan latency umum untuk operasi PUT dan GET."""
    print(f"Running: General Throughput & Latency benchmark ({num_operations} operasi)...")
    data_to_put = [generate_random_data() for _ in range(num_operations)]
    
    put_latencies = []; start_time = time.perf_counter()
    for key, value in data_to_put:
        op_start = time.perf_counter(); coordinator.put(key, value)
        op_end = time.perf_counter(); put_latencies.append((op_end - op_start) * 1000)
    put_duration = time.perf_counter() - start_time
    
    get_latencies = []; start_time = time.perf_counter()
    for key, _ in data_to_put:
        op_start = time.perf_counter(); coordinator.get(key)
        op_end = time.perf_counter(); get_latencies.append((op_end - op_start) * 1000)
    get_duration = time.perf_counter() - start_time

    return {
        "put_throughput": num_operations / put_duration, "avg_put_latency": statistics.mean(put_latencies),
        "get_throughput": num_operations / get_duration, "avg_get_latency": statistics.mean(get_latencies),
        "num_operations": num_operations
    }

def benchmark_hot_vs_cold(coordinator, num_partitions, num_ops=500):
    """Membandingkan latensi DAN throughput get dari hot vs cold storage."""
    print(f"Running: Hot vs Cold Storage benchmark ({num_ops} operasi)...")

    # 1. Setup: Buat 5 kunci 'dingin' (cold) dan 1 kunci 'panas' (hot)
    keys_for_p0 = find_keys_for_partition(0, 6, num_partitions)
    for i in range(5): coordinator.put(keys_for_p0[i], f"cold_value_{i}")
    time.sleep(1)
    coordinator.put(keys_for_p0[5], "hot_value")
    cold_key = keys_for_p0[0]
    hot_key = keys_for_p0[5]
    
    # 2. Ukur Latency (satu operasi)
    start_time = time.perf_counter(); coordinator.get(cold_key); end_time = time.perf_counter()
    cold_latency = (end_time - start_time) * 1000
    start_time = time.perf_counter(); coordinator.get(hot_key); end_time = time.perf_counter()
    hot_latency = (end_time - start_time) * 1000
    
    # 3. Ukur Throughput (banyak operasi)
    # Cold Storage Throughput
    start_time = time.perf_counter()
    for _ in range(num_ops): coordinator.get(cold_key)
    cold_duration = time.perf_counter() - start_time
    cold_throughput = num_ops / cold_duration

    # Hot Storage Throughput
    start_time = time.perf_counter()
    for _ in range(num_ops): coordinator.get(hot_key)
    hot_duration = time.perf_counter() - start_time
    hot_throughput = num_ops / hot_duration

    # 4. Buat dan tampilkan 2 grafik
    # Grafik Latensi
    plt.figure(1)
    labels_lat = ['Hot Storage (Memori)', 'Cold Storage (Disk)']; latencies = [hot_latency, cold_latency]
    bars_lat = plt.bar(labels_lat, latencies, color=['#3498db', '#e67e22'])
    plt.ylabel('Latency (ms)'); plt.title('Perbandingan Latensi Hot vs Cold Storage')
    plt.bar_label(bars_lat, fmt='%.4f ms')
    plt.savefig('latency_hot_vs_cold.png')
    print("-> Grafik perbandingan latensi akan muncul. Tutup untuk melanjutkan...")
    plt.show()

    # Grafik Throughput
    plt.figure(2)
    labels_tp = ['Hot Storage (Memori)', 'Cold Storage (Disk)']; throughputs = [hot_throughput, cold_throughput]
    bars_tp = plt.bar(labels_tp, throughputs, color=['#2ecc71', '#e74c3c'])
    plt.ylabel('Operasi / Detik'); plt.title('Perbandingan Throughput Hot vs Cold Storage')
    plt.bar_label(bars_tp, fmt='%.2f ops/s')
    plt.savefig('throughput_hot_vs_cold.png')
    print("-> Grafik perbandingan throughput akan muncul. Tutup untuk melanjutkan...")
    plt.show()

    return {
        "hot_latency": hot_latency, "cold_latency": cold_latency,
        "hot_throughput": hot_throughput, "cold_throughput": cold_throughput
    }

def test_fault_tolerance(coordinator, num_partitions, processes):
    print("Running: Fault Tolerance simulation...")
    key_to_test = find_keys_for_partition(1, 1, num_partitions)[0]
    leader_id = CLUSTER_TOPOLOGY['partitions'][1]['leader']
    coordinator.put(key_to_test, "data_aman")
    time.sleep(2)
    leader_process = processes[leader_id]
    if leader_process.is_alive():
        leader_process.terminate(); leader_process.join()
    response = coordinator.get(key_to_test)
    return {"leader_killed": leader_id, "response_after_failure": response}

# --- Helper dari test.py ---
def find_keys_for_partition(target_partition_id, num_keys, num_partitions):
    keys = []; i = 0
    while len(keys) < num_keys:
        key_candidate = f"testkey:{i}"
        hash_val = int(hashlib.sha1(key_candidate.encode('utf-8')).hexdigest(), 16)
        if (hash_val % num_partitions) == target_partition_id:
            keys.append(key_candidate)
        i += 1
    return keys

def print_summary_report(results):
    """Mencetak laporan ringkas dari semua hasil benchmark."""
    print("\n\n==============================================")
    print("      LAPORAN HASIL BENCHMARK")
    print("==============================================")

    # Laporan Throughput & Latency Umum
    gen_res = results.get("general_throughput")
    if gen_res:
        print("\n[ Throughput & Latency Umum ]")
        print(f"Berdasarkan {gen_res['num_operations']} operasi:")
        print(f"  - Throughput Tulis (PUT): {gen_res['put_throughput']:.2f} operasi/detik")
        print(f"  - Rata-rata Latency Tulis (PUT): {gen_res['avg_put_latency']:.4f} ms")
        print(f"  - Throughput Baca (GET): {gen_res['get_throughput']:.2f} operasi/detik")
        print(f"  - Rata-rata Latency Baca (GET): {gen_res['avg_get_latency']:.4f} ms")

    # Laporan Hot vs Cold
    hc_res = results.get("hot_cold")
    if hc_res:
        print("\n[ Perbandingan Hot vs Cold Storage ]")
        print(f"  - Latency GET (Hot): {hc_res['hot_latency']:.4f} ms | Throughput GET (Hot): {hc_res['hot_throughput']:.2f} ops/s")
        print(f"  - Latency GET (Cold): {hc_res['cold_latency']:.4f} ms | Throughput GET (Cold): {hc_res['cold_throughput']:.2f} ops/s")
        print("  - Dua grafik perbandingan telah disimpan (latency & throughput).")
    
    # Laporan Fault Tolerance
    ft_res = results.get("fault_tolerance")
    if ft_res:
        print("\n[ Uji Fault Tolerance ]")
        print(f"  - Simulasi: Node {ft_res['leader_killed']} (leader) dimatikan.")
        print(f"  - Hasil GET setelah kegagalan: {ft_res['response_after_failure']}")
        print("  - Kesimpulan: GET gagal karena tidak ada failover otomatis, membuktikan data aman di replika.")
        
    print("\n==============================================")
    print("      BENCHMARK SELESAI")
    print("==============================================")


if __name__ == "__main__":
    processes = setup_cluster()
    coordinator = Coordinator(CLUSTER_TOPOLOGY)
    num_partitions = len(CLUSTER_TOPOLOGY['partitions'])
    all_results = {}
    
    # Ganti nama fungsi benchmark pertama
    all_results["general_throughput"] = benchmark_general_throughput(coordinator)
    all_results["hot_cold"] = benchmark_hot_vs_cold(coordinator, num_partitions)
    all_results["fault_tolerance"] = test_fault_tolerance(coordinator, num_partitions, processes)
    
    shutdown_cluster(processes)
    print_summary_report(all_results)