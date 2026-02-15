from __future__ import annotations

import json
import os
import socket
import struct
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

ENCODING = "utf-8"
LEN_PREFIX_FMT = "!I"
LEN_PREFIX_BYTES = struct.calcsize(LEN_PREFIX_FMT)
MAX_MSG_BYTES = 10 * 1024 * 1024  # 10MB for control messages

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

def list_shared_files(shared_dir: str) -> List[str]:
    try:
        names = []
        for entry in os.listdir(shared_dir):
            p = os.path.join(shared_dir, entry)
            if os.path.isfile(p):
                names.append(entry)
        names.sort()
        return names
    except Exception:
        return []

def safe_shared_path(shared_dir: str, filename: str) -> Optional[str]:
    # prevent path traversal
    name = os.path.basename(filename)
    path = os.path.join(shared_dir, name)
    if os.path.isfile(path):
        return path
    return None

def broadcast(clients: Dict[socket.socket, Dict[str, Any]], sender: Optional[socket.socket], msg: Dict[str, Any]) -> None:
    dead: List[socket.socket] = []
    for s in list(clients.keys()):
        if sender is not None and s is sender:
            continue
        try:
            tcp_send(s, msg)
        except Exception:
            dead.append(s)
    for s in dead:
        cleanup_client(clients, groups, s, notify=False)

def groupcast(clients: Dict[socket.socket, Dict[str, Any]], groups: Dict[str, Set[socket.socket]], group: str, sender: socket.socket, msg: Dict[str, Any]) -> None:
    members = groups.get(group, set())
    dead: List[socket.socket] = []
    for s in list(members):
        if s is sender:
            continue
        try:
            tcp_send(s, msg)
        except Exception:
            dead.append(s)
    for s in dead:
        cleanup_client(clients, groups, s, notify=False)

def cleanup_client(clients: Dict[socket.socket, Dict[str, Any]], groups: Dict[str, Set[socket.socket]], s: socket.socket, notify: bool=True) -> None:
    info = clients.pop(s, None)
    if info is None:
        try:
            s.close()
        except Exception:
            pass
        return
    username = info.get("username", "unknown")

    # Remove from all groups
    for g in list(groups.keys()):
        if s in groups[g]:
            groups[g].discard(s)
            if not groups[g]:
                del groups[g]

    try:
        s.close()
    except Exception:
        pass

    if notify:
        broadcast(clients, None, {"type": TYPE_INFO, "message": f"{username} has left"})

def send_access_ok_and_list(clients: Dict[socket.socket, Dict[str, Any]], s: socket.socket, shared_dir: str) -> None:
    files = list_shared_files(shared_dir)
    tcp_send(s, {"type": TYPE_INFO, "message": f"Access OK. {len(files)} file(s) available in SharedFiles."})
    tcp_send(s, {"type": TYPE_FILES_LIST, "files": files})

def handle_tcp_file_get(clients: Dict[socket.socket, Dict[str, Any]], s: socket.socket, shared_dir: str, filename: str) -> None:
    if not clients[s].get("access", False):
        tcp_send(s, {"type": TYPE_ERROR, "message": "Please run /access before downloading files."})
        return
    path = safe_shared_path(shared_dir, filename)
    if path is None:
        tcp_send(s, {"type": TYPE_ERROR, "message": f"File not found: {os.path.basename(filename)}"})
        return
    size = os.path.getsize(path)
    if size > MAX_FILE_SIZE:
        tcp_send(s, {"type": TYPE_ERROR, "message": "File too large."})
        return

    # Send header then raw bytes
    tcp_send(s, {"type": TYPE_FILE_DATA, "filename": os.path.basename(path), "size": size})
    with open(path, "rb") as f:
        while True:
            chunk = f.read(64 * 1024)
            if not chunk:
                break
            s.sendall(chunk)

def handle_udp_file_get(clients: Dict[socket.socket, Dict[str, Any]], s: socket.socket, shared_dir: str, filename: str, udp_port: int, transfer_id: int, server_ip: str) -> int:
    if not clients[s].get("access", False):
        tcp_send(s, {"type": TYPE_ERROR, "message": "Please run /access before downloading files."})
        return transfer_id
    path = safe_shared_path(shared_dir, filename)
    if path is None:
        tcp_send(s, {"type": TYPE_ERROR, "message": f"File not found: {os.path.basename(filename)}"})
        return transfer_id
    size = os.path.getsize(path)
    if size > MAX_FILE_SIZE:
        tcp_send(s, {"type": TYPE_ERROR, "message": "File too large."})
        return transfer_id

    # Determine UDP destination = client's TCP remote IP + given udp_port
    client_ip = s.getpeername()[0]
    dest = (client_ip, udp_port)

    # Compute chunks
    chunk_size = UDP_PAYLOAD
    total_chunks = (size + chunk_size - 1) // chunk_size

    transfer_id += 1
    meta = {
        "type": TYPE_FILE_UDP_META,
        "transfer_id": transfer_id,
        "filename": os.path.basename(path),
        "size": size,
        "total_chunks": total_chunks,
    }
    tcp_send(s, meta)
    time.sleep(0.05)  # give client a moment to start UDP receiver

    udp_sock = udp_socket
    with open(path, "rb") as f:
        for seq in range(total_chunks):
            payload = f.read(chunk_size)
            pkt = struct.pack(UDP_HDR_FMT, transfer_id, seq, total_chunks) + payload
            try:
                udp_sock.sendto(pkt, dest)
            except Exception:
                break

    return transfer_id

# Globals used by helpers
clients: Dict[socket.socket, Dict[str, Any]] = {}
groups: Dict[str, Set[socket.socket]] = {}
udp_socket: socket.socket

def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python server.py [port]")
        sys.exit(2)

    try:
        port = int(sys.argv[1])
    except Exception:
        print("Usage: python server.py [port]")
        sys.exit(2)

    if not (1 <= port <= 65535):
        print("Port must be 1..65535.")
        sys.exit(2)

    shared_dir = os.environ.get("SERVER_SHARED_FILES", os.path.join(os.getcwd(), "SharedFiles"))

    # TCP server
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen()
    srv.setblocking(False)

    # UDP socket for file sending
    global udp_socket
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.bind(("0.0.0.0", 0))  # ephemeral

    print(f"Server listening on 0.0.0.0:{port}")
    print(f"SharedFiles dir: {shared_dir}")

    import select
    transfer_id = 0

    while True:
        try:
            rlist = [srv] + list(clients.keys())
            readable, _, _ = select.select(rlist, [], [], 1.0)
        except KeyboardInterrupt:
            print("\nServer shutting down.")
            break

        for sock in readable:
            if sock is srv:
                c, addr = srv.accept()
                c.setblocking(True)
                print(f"New connection from {addr[0]}:{addr[1]}")
                clients[c] = {"username": None, "access": False}
                tcp_send(c, {"type": TYPE_INFO, "message": "Welcome to the server!"})
                continue

            s = sock
            try:
                msg = tcp_recv(s)
                if msg is None:
                    cleanup_client(clients, groups, s, notify=True)
                    continue
            except Exception:
                cleanup_client(clients, groups, s, notify=True)
                continue

            mtype = msg.get("type")

            if mtype == TYPE_JOIN:
                username = str(msg.get("username", "")).strip()
                if not username:
                    tcp_send(s, {"type": TYPE_ERROR, "message": "Invalid username."})
                    cleanup_client(clients, groups, s, notify=False)
                    continue
                clients[s]["username"] = username
                broadcast(clients, s, {"type": TYPE_INFO, "message": f"{username} has joined"})
                continue

            if mtype == TYPE_LEAVE:
                cleanup_client(clients, groups, s, notify=True)
                continue

            if mtype == TYPE_FILES_ACCESS:
                clients[s]["access"] = True
                send_access_ok_and_list(clients, s, shared_dir)
                continue

            if mtype == TYPE_FILES_LIST:
                if not clients[s].get("access", False):
                    tcp_send(s, {"type": TYPE_ERROR, "message": "Please run /access first."})
                    continue
                files = list_shared_files(shared_dir)
                tcp_send(s, {"type": TYPE_FILES_LIST, "files": files})
                continue

            if mtype == TYPE_FILE_GET:
                filename = str(msg.get("filename", "")).strip()
                if not filename:
                    tcp_send(s, {"type": TYPE_ERROR, "message": "Usage: /get <filename>"})
                    continue
                handle_tcp_file_get(clients, s, shared_dir, filename)
                continue

            if mtype == TYPE_FILE_GET_UDP:
                filename = str(msg.get("filename", "")).strip()
                udp_port = msg.get("udp_port")
                if not filename or not isinstance(udp_port, int) or not (1 <= udp_port <= 65535):
                    tcp_send(s, {"type": TYPE_ERROR, "message": "Usage: /get <filename> (udp_port invalid)"})
                    continue
                transfer_id = handle_udp_file_get(clients, s, shared_dir, filename, udp_port, transfer_id, "0.0.0.0")
                continue

            if mtype == TYPE_GROUP_JOIN:
                group = str(msg.get("group", "")).strip()
                if not group:
                    tcp_send(s, {"type": TYPE_ERROR, "message": "Usage: /join <group>"})
                    continue
                groups.setdefault(group, set()).add(s)
                tcp_send(s, {"type": TYPE_INFO, "message": f"Joined group {group}"})
                continue

            if mtype == TYPE_GROUP_LEAVE:
                group = str(msg.get("group", "")).strip()
                if not group:
                    tcp_send(s, {"type": TYPE_ERROR, "message": "Usage: /leave <group>"})
                    continue
                if group in groups and s in groups[group]:
                    groups[group].discard(s)
                    if not groups[group]:
                        del groups[group]
                    tcp_send(s, {"type": TYPE_INFO, "message": f"Left group {group}"})
                else:
                    tcp_send(s, {"type": TYPE_ERROR, "message": f"You are not in group {group}"})
                continue

            if mtype == TYPE_CHAT:
                mode = str(msg.get("mode", "BROADCAST")).upper()
                sender = clients.get(s, {}).get("username") or "unknown"
                text = str(msg.get("message", ""))

                if mode == "BROADCAST":
                    broadcast(clients, s, {"type": TYPE_CHAT, "from": sender, "message": text})
                    continue

                if mode == "UNICAST":
                    target = str(msg.get("to", "")).strip()
                    if not target:
                        tcp_send(s, {"type": TYPE_ERROR, "message": "Unicast usage: @user <message>"})
                        continue
                    target_sock = None
                    for cs, info in clients.items():
                        if info.get("username") == target:
                            target_sock = cs
                            break
                    if target_sock is None:
                        tcp_send(s, {"type": TYPE_ERROR, "message": f"User not found: {target}"})
                        continue
                    tcp_send(target_sock, {"type": TYPE_CHAT, "from": sender, "message": text})
                    continue

                if mode == "GROUP":
                    group = str(msg.get("group", "")).strip()
                    if not group:
                        tcp_send(s, {"type": TYPE_ERROR, "message": "Group usage: #group <message>"})
                        continue
                    if group not in groups or s not in groups[group]:
                        tcp_send(s, {"type": TYPE_ERROR, "message": f"You are not a member of group {group}"})
                        continue
                    groupcast(clients, groups, group, s, {"type": TYPE_CHAT, "from": f"{sender}@{group}", "message": text})
                    continue

                tcp_send(s, {"type": TYPE_ERROR, "message": "Unknown chat mode."})
                continue

            tcp_send(s, {"type": TYPE_ERROR, "message": "Unknown command/type."})

    # cleanup
    for s in list(clients.keys()):
        cleanup_client(clients, groups, s, notify=False)
    try:
        srv.close()
    except Exception:
        pass
    try:
        udp_socket.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
    