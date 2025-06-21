# network.py
import socket

def send_request(host, port, message):
    """Fungsi klien untuk mengirim permintaan ke server."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(message.encode('utf-8'))
            response = s.recv(1024)
            return response.decode('utf-8')
    except ConnectionRefusedError:
        return f"Error: Connection refused from {host}:{port}. Node might be down."
    except Exception as e:
        return f"Error: {e}"