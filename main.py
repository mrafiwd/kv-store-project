# main.py (MENGUJI KOORDINATOR)

import os
import shutil
from coordinator import Coordinator

def run_sharding_test():
    print("--- MULAI PENGUJIAN PARTISI (SHARDING) ---\n")
    
    # Konfigurasi jumlah partisi
    NUM_PARTITIONS = 4
    
    # Bersihkan direktori data lama
    data_dir = "data"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    # 1. Inisialisasi Koordinator dengan N partisi
    coordinator = Coordinator(num_partitions=NUM_PARTITIONS, data_dir=data_dir)

    # 2. Siapkan beberapa data untuk diuji
    test_data = {
        "user:101": "Andi",
        "user:102": "Budi",
        "product:A1": "Laptop",
        "product:B2": "Mouse",
        "session:xyz": "active_token",
        "session:abc": "another_token",
        "user:103": "Caca",
        "product:C3": "Keyboard"
    }
    
    # 3. Lakukan operasi PUT melalui koordinator
    print("\n--- Melakukan Operasi PUT ---")
    for key, value in test_data.items():
        coordinator.put(key, value)
        print("-" * 10)

    # 4. Lakukan operasi GET melalui koordinator untuk verifikasi
    print("\n--- Melakukan Operasi GET untuk Verifikasi ---")
    for key, expected_value in test_data.items():
        retrieved_value = coordinator.get(key)
        print(f"GET {key} -> {retrieved_value}")
        assert retrieved_value == expected_value
    print("✅  Semua data berhasil diambil kembali dengan benar.\n")
    
    # Verifikasi bahwa data benar-benar tersebar
    # (Opsional, untuk melihat file fisiknya)
    print("\n--- Verifikasi Penyebaran File Fisik ---")
    for i in range(NUM_PARTITIONS):
        log_path = os.path.join(data_dir, f"partition_{i}", "segment.log")
        if os.path.exists(log_path):
            print(f"File log untuk Partisi {i} telah dibuat di: {log_path}")
        else:
            print(f"Tidak ada data yang masuk ke Partisi {i}")
            
    # 5. Tutup koordinator
    coordinator.close()
    print("\n--- PENGUJIAN PARTISI SELESAI ---")


if __name__ == "__main__":
    run_sharding_test()