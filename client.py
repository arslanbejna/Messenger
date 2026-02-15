from __future__ import annotations

import json
import os
import socket
import struct
import sys
import threading
import time
from typing import Any, Dict, Optional

# -------------------- Protocol framing (TCP JSON messages) --------------------
ENCODING = "utf-8"
LEN_PREFIX_FMT = "!I"
LEN_PREFIX_BYTES = struct.calcsize(LEN_PREFIX_FMT)
MAX_MSG_BYTES = 10 * 1024 * 1024  # 10MB for control messages

def tcp_send(sock: socket.socket, msg: Dict[str, Any]) -> None:
    data = json.dumps(msg).encode(ENCODING)
    if len(data) > MAX_MSG_BYTES:
        raise ValueError("control message too large")
    sock.sendall(struct.pack(LEN_PREFIX_FMT, len(data)) + data)

def tcp_recv(sock: socket.socket) -> Optional[Dict[str, Any]]:
    hdr = _recv_exact(sock, LEN_PREFIX_BYTES)
    if hdr is None:
        return None
    (n,) = struct.unpack(LEN_PREFIX_FMT, hdr)
    if n <= 0 or n > MAX_MSG_BYTES:
        raise ValueError(f"invalid message size: {n}")
    body = _recv_exact(sock, n)
    if body is None:
        return None
    return json.loads(body.decode(ENCODING))

def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)

# -------------------- Message types --------------------
TYPE_JOIN = "JOIN"
TYPE_LEAVE = "LEAVE"
TYPE_INFO = "INFO"
TYPE_ERROR = "ERROR"

TYPE_CHAT = "CHAT"
TYPE_GROUP_CREATE = "GROUP_CREATE"
TYPE_GROUP_JOIN = "GROUP_JOIN"
TYPE_GROUP_LEAVE = "GROUP_LEAVE"

TYPE_FILES_ACCESS = "FILES_ACCESS"
TYPE_FILES_LIST = "FILES_LIST"
TYPE_FILE_GET = "FILE_GET"
TYPE_FILE_DATA = "FILE_DATA"

TYPE_FILE_GET_UDP = "FILE_GET_UDP"
TYPE_FILE_UDP_META = "FILE_UDP_META"

# -------------------- UDP transfer --------------------
UDP_HDR_FMT = "!III"  # transfer_id, seq, total_chunks
UDP_HDR_BYTES = struct.calcsize(UDP_HDR_FMT)
UDP_MAX_DGRAM = 65507  # theoretical max payload size for UDP
UDP_PAYLOAD = 1400  

MAX_FILE_SIZE = 100 * 1024 * 1024

# -------------------- Utilities --------------------
def safe_output_path(base_dir: str, filename: str) -> str:
    name = os.path.basename(filename)
    return os.path.join(base_dir, name)

# -------------------- UDP download worker --------------------
def udp_download_worker(udp_sock: socket.socket, download_dir: str, meta: Dict[str, Any]) -> None:
    transfer_id = meta.get("transfer_id")
    filename = meta.get("filename")
    size = meta.get("size")
    total_chunks = meta.get("total_chunks")

    if not isinstance(transfer_id, int):
        print("\n[!] UDP download refused: invalid transfer_id")
        return
    if not isinstance(filename, str) or not filename:
        print("\n[!] UDP download refused: invalid filename")
        return
    if not isinstance(size, int) or size < 0 or size > MAX_FILE_SIZE:
        print(f"\n[!] UDP download refused: invalid size {size}")
        return
    if not isinstance(total_chunks, int) or total_chunks <= 0:
        print("\n[!] UDP download refused: invalid total_chunks")
        return

    os.makedirs(download_dir, exist_ok=True)
    out_path = safe_output_path(download_dir, filename)

    received: Dict[int, bytes] = {}
    start = time.time()
    deadline = start + max(3.0, min(20.0, 0.5 + total_chunks * 0.05))  # bounded wait
    udp_sock.settimeout(0.5)

    while time.time() < deadline and len(received) < total_chunks:
        try:
            data, _addr = udp_sock.recvfrom(UDP_HDR_BYTES + UDP_PAYLOAD)
        except socket.timeout:
            continue
        except Exception:
            break

        if len(data) < UDP_HDR_BYTES:
            continue
        tid, seq, total = struct.unpack(UDP_HDR_FMT, data[:UDP_HDR_BYTES])
        if tid != transfer_id:
            continue
        if total != total_chunks:
            continue
        if not (0 <= seq < total_chunks):
            continue
        if seq in received:
            continue
        received[seq] = data[UDP_HDR_BYTES:]

    if len(received) != total_chunks:
        print(f"\n[!] UDP download incomplete: got {len(received)}/{total_chunks} chunks (file not written)")
        return

    blob = b"".join(received[i] for i in range(total_chunks))
    blob = blob[:size]

    try:
        with open(out_path, "wb") as f:
            f.write(blob)
        print(f"\n[+] UDP Downloaded {os.path.basename(filename)} ({size} bytes) -> {out_path}")
    except Exception as e:
        print(f"\n[!] UDP download failed: {e}")

# -------------------- Receiver loop --------------------
def receiver_loop(tcp_sock: socket.socket, udp_sock: socket.socket, download_dir: str) -> None:
    while True:
        try:
            msg = tcp_recv(tcp_sock)
            if msg is None:
                print("\n[!] Disconnected from server.")
                return
        except Exception as e:
            print(f"\n[!] Receiver error: {e}")
            return

        mtype = msg.get("type")
        if mtype in (TYPE_INFO, TYPE_ERROR):
            text = msg.get("message", "")
            print(f"\n{text}")
            continue

        if mtype == TYPE_CHAT:
            frm = msg.get("from", "?")
            text = msg.get("message", "")
            print(f"\n{frm}: {text}")
            continue

        if mtype == TYPE_FILES_LIST:
            files = msg.get("files", [])
            if not files:
                print("\n[SharedFiles] (empty)")
            else:
                print("\n[SharedFiles]")
                for f in files:
                    print(f" - {f}")
            continue

        if mtype == TYPE_FILE_DATA:
            # TCP file data: header already received, then raw bytes follow on TCP stream.
            filename = msg.get("filename")
            size = msg.get("size")
            if not isinstance(filename, str) or not isinstance(size, int) or size < 0:
                print("\n[!] Invalid FILE_DATA header.")
                continue

            os.makedirs(download_dir, exist_ok=True)
            out_path = safe_output_path(download_dir, filename)

            remaining = size
            try:
                with open(out_path, "wb") as f:
                    while remaining > 0:
                        chunk = tcp_sock.recv(min(64 * 1024, remaining))
                        if not chunk:
                            raise ConnectionError("socket closed during file transfer")
                        f.write(chunk)
                        remaining -= len(chunk)
                print(f"\n[+] Downloaded {os.path.basename(filename)} ({size} bytes) -> {out_path}")
            except Exception as e:
                print(f"\n[!] TCP download failed: {e}")
            continue

        if mtype == TYPE_FILE_UDP_META:
            t = threading.Thread(target=udp_download_worker, args=(udp_sock, download_dir, msg), daemon=True)
            t.start()
            continue

# -------------------- Main --------------------
def main() -> None:
    if len(sys.argv) != 4:
        print("Usage: python client.py [username] [hostname] [port]")
        sys.exit(2)

    username = sys.argv[1].strip()
    host = sys.argv[2].strip()
    try:
        port = int(sys.argv[3])
    except Exception:
        print("Usage: python client.py [username] [hostname] [port]")
        sys.exit(2)

    if not username:
        print("Username cannot be empty.")
        sys.exit(2)
    if not host:
        print("Hostname cannot be empty.")
        sys.exit(2)
    if not (1 <= port <= 65535):
        print("Port must be 1..65535.")
        sys.exit(2)

    # UDP socket (for UDP downloads).
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.bind(("0.0.0.0", 0))
    udp_port = udp_sock.getsockname()[1]

    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.connect((host, port))

    # Join
    tcp_send(tcp_sock, {"type": TYPE_JOIN, "username": username})

    download_dir = os.path.join(os.getcwd(), username)
    selected_proto = "tcp"

    # Start receiver thread
    threading.Thread(target=receiver_loop, args=(tcp_sock, udp_sock, download_dir), daemon=True).start()

    print("Commands: /quit, /access, /list, /proto tcp|udp, /get <filename>, @user <msg>, #group <msg>, /join <group>, /leave <group>")

    try:
        while True:
            try:
                line = input("> ").strip()
            except EOFError:
                line = "/quit"

            if not line:
                continue

            if line in ("/quit", "/exit"):
                tcp_send(tcp_sock, {"type": TYPE_LEAVE})
                break

            if line.startswith("/proto "):
                val = line.split(" ", 1)[1].strip().lower()
                if val in ("tcp", "udp"):
                    selected_proto = val
                    print(f"Protocol set to {selected_proto.upper()}")
                else:
                    print("Usage: /proto tcp|udp")
                continue

            if line == "/access":
                tcp_send(tcp_sock, {"type": TYPE_FILES_ACCESS})
                continue

            if line == "/list":
                tcp_send(tcp_sock, {"type": TYPE_FILES_LIST})
                continue

            if line.startswith("/get "):
                filename = line.split(" ", 1)[1].strip()
                if not filename:
                    print("Usage: /get <filename>")
                    continue
                if selected_proto == "tcp":
                    tcp_send(tcp_sock, {"type": TYPE_FILE_GET, "filename": filename})
                else:
                    tcp_send(tcp_sock, {"type": TYPE_FILE_GET_UDP, "filename": filename, "udp_port": udp_port})
                continue

            if line.startswith("/join "):
                group = line.split(" ", 1)[1].strip()
                if group:
                    tcp_send(tcp_sock, {"type": TYPE_GROUP_JOIN, "group": group})
                else:
                    print("Usage: /join <group>")
                continue

            if line.startswith("/leave "):
                group = line.split(" ", 1)[1].strip()
                if group:
                    tcp_send(tcp_sock, {"type": TYPE_GROUP_LEAVE, "group": group})
                else:
                    print("Usage: /leave <group>")
                continue

            if line.startswith("@") and " " in line:
                target, text = line[1:].split(" ", 1)
                tcp_send(tcp_sock, {"type": TYPE_CHAT, "mode": "UNICAST", "to": target, "message": text})
                continue

            if line.startswith("#") and " " in line:
                group, text = line[1:].split(" ", 1)
                tcp_send(tcp_sock, {"type": TYPE_CHAT, "mode": "GROUP", "group": group, "message": text})
                continue

            # default broadcast
            tcp_send(tcp_sock, {"type": TYPE_CHAT, "mode": "BROADCAST", "message": line})

    finally:
        try:
            tcp_sock.close()
        except Exception:
            pass
        try:
            udp_sock.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
