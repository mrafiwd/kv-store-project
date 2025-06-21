# serializer.py

import struct
import json
from typing import Any, Dict, Union

class Serializer:
    """
    Mengatasi encoding dan decoding key-value pairs.
    Evolusi skema dengan memanfaatkan byte versi.
    """
    def encode_value(self, value: Union[str, Dict[str, Any]]) -> bytes:
        if isinstance(value, dict) and 'data' in value and 'timestamp' in value:
            schema_version = 2
            data_bytes = value['data'].encode('utf-8')
            timestamp = value['timestamp']
            # Format: [version (1b)] [data_len (4b)] [data] [timestamp (8b)]
            return struct.pack(f'!B I {len(data_bytes)}s Q', schema_version, len(data_bytes), data_bytes, timestamp)
        elif isinstance(value, str):
            schema_version = 1
            value_bytes = value.encode('utf-8')
            # Format: [version (1b)] [value_len (4b)] [value]
            return struct.pack(f'!B I {len(value_bytes)}s', schema_version, len(value_bytes), value_bytes)
        elif isinstance(value, dict):
            schema_version = 3
            # Ubah dict menjadi string JSON, lalu encode ke bytes
            value_json_str = json.dumps(value)
            value_bytes = value_json_str.encode('utf-8')
            # Format: [versi (1b)] [panjang_json (4b)] [json_string_bytes]
            return struct.pack(f'!B I {len(value_bytes)}s', schema_version, len(value_bytes), value_bytes)
        else:
            raise TypeError("Value type not supported for encoding.")

    def decode_value(self, value_bytes: bytes) -> Dict[str, Any]:
        schema_version, = struct.unpack('!B', value_bytes[0:1])
        if schema_version == 1:
            value_len, = struct.unpack('!I', value_bytes[1:5])
            value_str, = struct.unpack(f'!{value_len}s', value_bytes[5:5 + value_len])
            return {'schema_version': 1, 'value': value_str.decode('utf-8')}
        elif schema_version == 2:
            data_len, = struct.unpack('!I', value_bytes[1:5])
            data_end_offset = 5 + data_len
            data_str, = struct.unpack(f'!{data_len}s', value_bytes[5:data_end_offset])
            timestamp, = struct.unpack('!Q', value_bytes[data_end_offset:data_end_offset + 8])
            return {'schema_version': 2, 'data': data_str.decode('utf-8'), 'timestamp': timestamp}
        elif schema_version == 3:
            value_len, = struct.unpack('!I', value_bytes[1:5])
            value_json_str_bytes, = struct.unpack(f'!{value_len}s', value_bytes[5:5 + value_len])
            # Decode bytes ke string JSON, lalu parse JSON ke dictionary
            original_dict = json.loads(value_json_str_bytes.decode('utf-8'))
            return {'schema_version': 3, 'value': original_dict}
        else:
            raise ValueError(f"Unknown schema version: {schema_version}")