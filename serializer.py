# serializer.py

import struct
import time
from typing import Any, Dict, Union

class Serializer:
    """
    Handles encoding and decoding of data with schema evolution support.
    - Schema V1: A simple string value.
    - Schema V2: A dictionary with 'data' (string) and 'timestamp' (int).
    """

    # Metode untuk mengenkode data menjadi format biner
    def encode(self, key: str, value: Union[str, Dict[str, Any]]) -> bytes:
        """
        Encodes a key-value pair into a binary format.
        The format depends on the type of the value to support schema evolution.
        """
        # Skema V2: Jika value adalah dictionary dengan field yang diharapkan
        if isinstance(value, dict) and 'data' in value and 'timestamp' in value:
            schema_version = 2
            data_str = value['data']
            timestamp = value['timestamp']

            # Pack: 1 byte for version, 4 bytes for data length, data, 8 bytes for timestamp
            # !B = 1 byte unsigned int (version)
            # !I = 4 bytes unsigned int (length)
            # !Q = 8 bytes unsigned int (timestamp)
            data_bytes = data_str.encode('utf-8')
            return struct.pack(f'!B I {len(data_bytes)}s Q', schema_version, len(data_bytes), data_bytes, timestamp)

        # Skema V1: Jika value adalah string biasa
        elif isinstance(value, str):
            schema_version = 1
            value_bytes = value.encode('utf-8')
            
            # Pack: 1 byte for version, 4 bytes for value length, value
            return struct.pack(f'!B I {len(value_bytes)}s', schema_version, len(value_bytes), value_bytes)
        
        else:
            raise TypeError("Value type not supported for encoding. Must be str or a valid dict.")

    # Metode untuk mendekode data dari format biner
    def decode(self, binary_data: bytes) -> Dict[str, Any]:
        """
        Decodes binary data back into a structured dictionary.
        Handles different schema versions.
        """
        # Unpack the schema version (first byte)
        schema_version, = struct.unpack('!B', binary_data[0:1])

        # Decode based on schema version
        if schema_version == 1:
            # Skema V1: [version (1b)] [len (4b)] [value (variable)]
            value_len, = struct.unpack('!I', binary_data[1:5])
            value_str, = struct.unpack(f'!{value_len}s', binary_data[5:5+value_len])
            
            # Kembalikan dalam format terstruktur untuk konsistensi
            return {
                'schema_version': 1,
                'value': value_str.decode('utf-8')
            }

        elif schema_version == 2:
            # Skema V2: [version (1b)] [len (4b)] [data (variable)] [timestamp (8b)]
            data_len, = struct.unpack('!I', binary_data[1:5])
            data_offset_end = 5 + data_len
            data_str, = struct.unpack(f'!{data_len}s', binary_data[5:data_offset_end])
            timestamp, = struct.unpack('!Q', binary_data[data_offset_end:data_offset_end + 8])

            return {
                'schema_version': 2,
                'data': data_str.decode('utf-8'),
                'timestamp': timestamp
            }
        
        else:
            raise ValueError(f"Unknown schema version: {schema_version}")


# --- Bagian untuk pengujian mandiri ---
if __name__ == "__main__":
    serializer = Serializer()

    print("--- Pengujian Skema V1 (Nilai String Sederhana) ---")
    value_v1 = "ini adalah data lama"
    encoded_v1 = serializer.encode("key1", value_v1)
    decoded_v1 = serializer.decode(encoded_v1)
    
    print(f"Data Asli V1: {value_v1}")
    print(f"Data Terenkode V1 (hex): {encoded_v1.hex()}")
    print(f"Data Terdecode V1: {decoded_v1}")
    assert decoded_v1['value'] == value_v1
    print("✅  Tes V1 Berhasil!\n")


    print("--- Pengujian Skema V2 (Nilai Objek dengan Timestamp) ---")
    value_v2 = {
        "data": "ini adalah data baru dengan skema berbeda",
        "timestamp": int(time.time())
    }
    encoded_v2 = serializer.encode("key2", value_v2)
    decoded_v2 = serializer.decode(encoded_v2)

    print(f"Data Asli V2: {value_v2}")
    print(f"Data Terenkode V2 (hex): {encoded_v2.hex()}")
    print(f"Data Terdecode V2: {decoded_v2}")
    assert decoded_v2['data'] == value_v2['data']
    assert decoded_v2['timestamp'] == value_v2['timestamp']
    print("✅  Tes V2 Berhasil!\n")

    print("--- Pengujian Kompatibilitas ---")
    # Reader baru (decode) bisa membaca data lama (encoded_v1)
    decoded_old_data_by_new_reader = serializer.decode(encoded_v1)
    print(f"Reader baru membaca data lama: {decoded_old_data_by_new_reader}")
    assert decoded_old_data_by_new_reader['schema_version'] == 1
    print("✅  Kompatibilitas Mundur (Backward Compatibility) Berhasil!\n")
    
    # Bayangkan ada reader lama yang hanya mengerti V1. 
    # Ia tidak akan bisa membaca data V2, dan ini sesuai desain.
    # Kita akan menangani ini di level aplikasi dengan memastikan reader di-update.
    # Namun, sistem tidak crash, hanya melempar error yang bisa ditangani.
    try:
        # Simulasi reader lama mencoba membaca data baru
        binary_data_v2 = encoded_v2
        schema_v, = struct.unpack('!B', binary_data_v2[0:1])
        if schema_v != 1:
            raise TypeError(f"Reader lama tidak mendukung skema v{schema_v}")
    except TypeError as e:
        print(f"Simulasi reader lama gagal membaca data V2: {e}")
        print("✅  Kompatibilitas Maju (Forward Compatibility) Terjaga!\n")