# Sistem Key-Value Store Terdistribusi

![Python](https://img.shields.io/badge/python-3.13.3-blue.svg)

## Deskripsi Proyek

Proyek ini adalah implementasi sebuah sistem penyimpanan Key-Value terdistribusi yang dibangun dari awal menggunakan Python. Tujuan utama proyek ini adalah untuk menerapkan materi dari mata kuliah Sistem Data-Intensif, seperti partisi (sharding), replikasi, penyimpanan log-structured, dan evolusi skema, tanpa bergantung pada database eksternal.

Sistem ini berjalan sebagai sebuah cluster dari beberapa node yang saling berkomunikasi, di mana data didistribusikan dan direplikasi.

## Konsep Inti yang Diimplementasikan

Proyek ini secara praktis menerapkan berbagai teori dari sistem data-intensif:

* **Partisi (Sharding):** Data didistribusikan ke beberapa partisi berdasarkan nilai hash dari kunci (`hash(key) % N`) untuk menyeimbangkan beban.
* **Replikasi Asinkron Leader-Follower:** Setiap partisi memiliki replika (leader dan follower) untuk mencapai *high availability* dan toleransi kesalahan (*fault tolerance*). Replikasi bersifat asinkron untuk menjaga latensi penulisan tetap rendah.
* **Log-Structured Storage:** Mekanisme penyimpanan di disk menggunakan file log *append-only* (`segment.log`), sebuah pendekatan yang sangat efisien untuk operasi tulis.
* **Caching (Hot/Cold Storage):** Sistem menggunakan memori sebagai *hot storage* (cache) untuk data yang baru ditulis dan disk sebagai *cold storage* untuk persistensi jangka panjang.
* **Custom Binary Serialization & Schema Evolution:** Data diserialisasi ke dalam format biner kustom yang ringkas. Sistem terdapat evolusi skema melalui *versioning*, memungkinkan penambahan format data baru tanpa merusak data yang sudah ada.
* **Concurrency & Thread-Safety:** Sistem menangani permintaan konkuren menggunakan *multi-threading* dan mekanisme *locking* untuk menjaga integritas data di memori.

## Fitur

* **Penyimpanan Key-Value:** Menyimpan dan mengambil data berdasarkan kunci unik.
* **Tipe Data Fleksibel:** Mendukung penyimpanan nilai berupa string dan objek JSON.
* **Partisi & Replikasi:** Distribusi dan replikasi data yang dapat dikonfigurasi secara dinamis.
* **Introspeksi Sistem:**
    * `status <key>`: Memeriksa lokasi data (di memori atau di disk).
    * `inspect <node_id>`: Melihat isi data yang ada di memori sebuah node.

## Struktur Direktori

```
kv-store-project/
├── pycache/                      # Direktori cache bytecode yang dibuat otomatis oleh Python untuk mempercepat import.
│   ├── config.cpython-313.pyc
│   └── ...
├── data/                         # Direktori utama untuk penyimpanan data persisten (cold storage).
│   ├── node_0/                   # Data spesifik untuk Node 0.
│   │   ├── partition_0/          # Data untuk replika Partisi 0 yang dipegang Node 0.
│   │   │   └── segment.log
│   │   ├── partition_2/
│   │   │   └── segment.log
│   │   └── partition_3/
│   │       └── segment.log
│   ├── node_1/                   # Data spesifik untuk Node 1.
│   └── node_2/                   # Data spesifik untuk Node 2.
├── config.py                     # Konfigurasi utama untuk mendefinisikan topologi cluster.
├── coordinator.py                # Logika routing yang mengarahkan permintaan klien ke node yang tepat.
├── main.py                       # Aplikasi utama (klien interaktif CLI) yang dijalankan pengguna.
├── network.py                    # Fungsi helper untuk komunikasi jaringan antar node.
├── node.py                       # Logika untuk sebuah server node, termasuk TCP server.
├── partition.py                  # Logika inti untuk satu partisi (mengelola Hot & Cold Storage).
├── serializer.py                 # Menangani encoding/decoding data dan evolusi skema.
├── test.py                       # Skrip untuk pengujian otomatis seluruh sistem.
├── .gitignore                    # Menginstruksikan Git untuk mengabaikan file/folder tertentu.
└── README.md                     # Dokumentasi proyek (file ini).
```
## Cara Menjalankan

Sistem ini dapat dijalankan dalam dua mode: mode interaktif (CLI) secara manual, dan mode tes.

### Mode Interaktif (Aplikasi Utama)

1.  **Konfigurasi Cluster (Opsional):**
    Buka `config.py` untuk menyesuaikan jumlah node dan partisi jika diperlukan.
2.  **Jalankan Aplikasi Utama:**
    ```bash
    python main.py
    ```
3.  **Menghentikan Sistem:**
    Ketik `exit` atau `quit`.

### Mode Tes Otomatis

Melakukan serangkaian tes otomatis untuk memverifikasi fungsionalitas sistem.

1.  **Jalankan Skrip Tes:**
    ```bash
    python test.py
    ```

## Daftar Perintah CLI

| Perintah            | Contoh Penggunaan                                    | Deskripsi                                                        |
| ------------------- | ---------------------------------------------------- | ---------------------------------------------------------------- |
| `put <key> <value>` | `put user:101 "Andi Pratama"`                        | Menyimpan nilai string.                                          |
| `put <key> '{...}'` | `put user:101:profile '{"kota": "Jakarta"}'`         | Menyimpan nilai berupa objek JSON (gunakan kutip tunggal).       |
| `get <key>`         | `get user:101`                                       | Mengambil dan menampilkan nilai dari sebuah kunci.               |
| `status <key>`      | `status user:101`                                    | Memeriksa lokasi data (di `HOT_STORAGE` atau `COLD_STORAGE`).     |
| `inspect <node_id>` | `inspect 0`                                          | Menampilkan kunci-kunci yang ada di memori (hot storage) Node 0. |
| `exit` atau `quit`  | `exit`                                               | Keluar dari aplikasi dan mematikan semua node.     |

## Contoh Penggunaan

#### Menyimpan String (Skema V1)
```bash
> put nama "Rafi Widya"
```
```bash
> get nama
```
#### Menyimpan Event dengan Timestamp (Skema V2)
```bash
> put event:login '{"data": "user:101 login", "timestamp": "2025-06-22 10:30:00"}'
```
```bash
> get event:login
```
#### Menyimpan Kamus/JSON Generik (Skema V3)
```bash
> put user:101:profile '{"jurusan": "Sistem Informasi", "angkatan": 2022}'
```
```bash
> get user:101:profile
```
#### Menggunakan Fitur Introspeksi
```bash
> status nama
```
```bash
> inspect 1 # node_id bisa disesuaikan (0, 1, atau 2)
```
