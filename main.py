# main.py (REVISI PADA ASSERTIONS)

import os
import shutil
import time
from partition import Partition

def run_test():
    print("--- MULAI PENGUJIAN STORAGE ENGINE ---\n")
    
    if os.path.exists("data"):
        shutil.rmtree("data")

    p = Partition(partition_id=0, data_dir="data")

    p.put("nama", "Andi")
    p.put("kota", "Surabaya")
    p.put("pekerjaan", "Insinyur")
    
    print("\n--- Mengambil data dari Hot Storage ---")
    nama_hot = p.get("nama")
    print("Nama:", nama_hot)
    assert nama_hot == "Andi" 
    
    kota_hot = p.get("kota")
    print("Kota:", kota_hot)
    assert kota_hot == "Surabaya" 
    print("-" * 20)

    print("\n--- Memicu Flush ke Cold Storage ---")
    p.put("email", "andi@example.com")
    p.put("status", "aktif")
    
    print(f"Status Hot Storage setelah flush: {p.hot_storage}")
    assert len(p.hot_storage) == 0

    print("\n--- Mengambil data dari Cold Storage ---")
    nama_cold = p.get("nama")
    print("Nama:", nama_cold)
    assert nama_cold == "Andi" 

    status_cold = p.get("status")
    print("Status:", status_cold)
    assert status_cold == "aktif" 
    print("-" * 20)

    print("\n--- Menambahkan data dengan Skema V2 ---")
    value_v2 = {
        "data": "Login terakhir dari perangkat mobile",
        "timestamp": int(time.time())
    }
    p.put("last_event", value_v2)
    
    event = p.get("last_event")
    print("Event terakhir:", event)

    assert isinstance(event, dict)
    assert event['data'] == value_v2['data']
    assert event['timestamp'] == value_v2['timestamp']

    print("\n--- Menutup Partisi ---")
    p.close()
    
    print("\n--- PENGUJIAN RECOVERY STATE ---")
    print("Membuat instance partisi baru dari file log yang ada...")
    p_reloaded = Partition(partition_id=0, data_dir="data")
    
    print("\nMengambil data dari partisi yang baru dimuat...")
    nama_reloaded = p_reloaded.get("nama")
    print("Nama (reloaded):", nama_reloaded)
    assert nama_reloaded == "Andi" 
    
    kota_reloaded = p_reloaded.get("kota")
    print("Kota (reloaded):", kota_reloaded)
    assert kota_reloaded == "Surabaya" 
    
    event_reloaded = p_reloaded.get("last_event")
    print("Event (reloaded):", event_reloaded)
    assert event_reloaded['data'] == value_v2['data'] 
    
    print("\n--- SEMUA TES BERHASIL! ---")

if __name__ == "__main__":
    run_test()