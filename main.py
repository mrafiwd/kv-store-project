# main.py
import os, shutil, time, multiprocessing, json
from datetime import datetime
from coordinator import Coordinator
from config import CLUSTER_TOPOLOGY
from node import start_node_process
from network import send_request

def run_interactive_mode():
    print("--- MEMULAI SISTEM KEY-VALUE STORE ---")
    
    # Opsi untuk membersihkan data sebelum memulai.
    # Hilangkan tanda komentar pada blok 'for' di bawah jika Anda ingin memulai dari keadaan bersih.
    # print("Membersihkan data dari sesi sebelumnya...")
    # for node_id in CLUSTER_TOPOLOGY['nodes']:
    #     dir_path = f"data/node_{node_id}"
    #     if os.path.exists(dir_path):
    #         shutil.rmtree(dir_path)

    processes = []
    for node_id, info in CLUSTER_TOPOLOGY['nodes'].items():
        process = multiprocessing.Process(target=start_node_process, args=(node_id, info['host'], info['port'], CLUSTER_TOPOLOGY))
        processes.append(process); process.start()
        print(f"Starting background process for Node-{node_id}...")
    print("\nWaiting for all nodes to start...")
    time.sleep(3)

    coordinator = Coordinator(CLUSTER_TOPOLOGY)
    print("\n==============================================================")
    print("  Selamat Datang di Key-Value Store CLI!")
    print("  (Timestamp dalam format 'YYYY-MM-DD HH:MM:SS')")
    print("==============================================================")
    print("Perintah: put <key> <value>             -> Khusus untuk string")
    print("Perintah: put <key> '{\"json\":\"value\"}'  -> Khusus untuk JSON")
    print("Perintah: get <key>")
    print("Perintah: status <key>                  -> Cek lokasi data (hot/cold)")
    print("Perintah: inspect <node_id>             -> Lihat isi memori (hot) sebuah node")
    print("Perintah: hex <key>                     -> Lihat hasil encoding (hexdump)")
    print("Perintah: exit atau quit")
    print("--------------------------------------------------------------")

    while True:
        try:
            command_line = input("> ")
            if not command_line: continue
            parts = command_line.strip().split(' ', 2)
            command = parts[0].lower()
            if command in ["exit", "quit"]: break
            elif command == "put":
                if len(parts) != 3: print("Error: Format -> put <key> <value>"); continue
                key, value_str = parts[1], parts[2]
                if (value_str.startswith("'") and value_str.endswith("'")) or \
                   (value_str.startswith('"') and value_str.endswith('"')):
                    value_str = value_str[1:-1]
                try:
                    value = json.loads(value_str)
                    if isinstance(value, dict) and 'timestamp' in value and isinstance(value['timestamp'], str):
                        try:
                            dt_obj = datetime.strptime(value['timestamp'], "%Y-%m-%d %H:%M:%S")
                            value['timestamp'] = int(dt_obj.timestamp())
                        except ValueError: print("Error: Format timestamp salah."); continue
                except json.JSONDecodeError: value = value_str
                response = coordinator.put(key, value); print(f"Server Response: {response}")
            elif command == "get":
                if len(parts) != 2: print("Error: Format -> get <key>"); continue
                key = parts[1]
                response = coordinator.get(key)
                if isinstance(response, dict) and 'timestamp' in response:
                    try:
                        response['timestamp'] = datetime.fromtimestamp(response['timestamp']).strftime("%Y-%m-%d %H:%M:%S")
                    except: pass
                print(f"Value: {response}")
            elif command == "status":
                if len(parts) != 2:
                    print("Error: Format -> status <key>")
                    continue
                key = parts[1]
                response = coordinator.status(key)
                print(f"Lokasi: {response}")
            elif command == "hex":
                if len(parts) != 2:
                    print("Error: Format -> hex <key>")
                    continue
                key = parts[1]
                response = coordinator.hex(key)
                print(f"Hexdump: {response}")
            elif command == "inspect":
                if len(parts) != 2:
                    print("Error: Format -> inspect <node_id>")
                    continue
                try:
                    node_id = int(parts[1])
                    if node_id not in CLUSTER_TOPOLOGY['nodes']:
                        print(f"Error: Node ID {node_id} tidak ada di konfigurasi.")
                        continue
                    
                    # Inspect langsung ke node, tidak melalui koordinator
                    node_info = CLUSTER_TOPOLOGY['nodes'][node_id]
                    response = send_request(node_info['host'], node_info['port'], "INSPECT")
                    # Tampilkan sebagai JSON yang rapi
                    print(json.dumps(json.loads(response), indent=2))
                except ValueError:
                    print("Error: Node ID harus berupa angka.")
                except Exception as e:
                    print(f"Gagal menginspeksi node: {e}")
            else: print(f"Error: Perintah '{command}' tidak dikenal.")
        except (KeyboardInterrupt, EOFError): break
        except Exception as e: print(f"Terjadi error: {e}")

    print("\n--- Menutup semua node server ---")
    for node_id, info in CLUSTER_TOPOLOGY['nodes'].items():
        send_request(info['host'], info['port'], "SHUTDOWN")
    
    print("Waiting for node processes to terminate...")
    for p in processes:
        p.join(timeout=5)
        if p.is_alive(): p.terminate()
    
    print("Sistem telah dimatikan. Sampai jumpa!")

if __name__ == "__main__":
    run_interactive_mode()