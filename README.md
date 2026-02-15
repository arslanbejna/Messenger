# Messenger
Multi-client messaging system built in Python using TCP/UDP sockets. Supports broadcast, unicast, group messaging, and file transfer with selectable transport protocol.

# Python Socket Messenger

A multi-client messaging system built in Python using TCP/UDP sockets.


---

## Requirements

- Python 3.13

---

## How to Run

### 1) Start the Server (choose a port)

```
python server.py <port>
```

### 2) Start one or more Clients (each in a new terminal)

```
python client.py <username> <host> <port>
```

---

## Basic Chat Commands

### Broadcast (default)
Type a message and press Enter.

### Unicast
```
@username message
```

### Quit Cleanly
```
/quit
```
or
```
/exit
```

---

## Groups

### Join or Create a Group
```
/join groupname
```

If the group does not exist yet, the server creates it when you join.

### Leave a Group
```
/leave groupname
```

### Send a Group Message
```
#groupname message
```

Only members of that group will receive the message.

---

## Shared Files / Downloads

The server shares files from the folder:

```
SharedFiles
```

You can optionally set an environment variable on the server machine.

### Windows (CMD)
```
set SERVER_SHARED_FILES=C:\path\to\SharedFiles
```

### Windows (PowerShell)
```
$env:SERVER_SHARED_FILES="C:\path\to\SharedFiles"
```

---

## Client Commands for Files

### 1) Access the Shared Folder and Get the File List
```
/access
```

### 2) Choose Download Protocol
```
/proto tcp
```
or
```
/proto udp
```

### 3) Download a File
```
/get filename.ext
```

---

## Download Location

Downloads are saved into a folder named after your username (created automatically):

```
./username/filename.ext
```

---

## Notes

- If a client disconnects unexpectedly, the server removes it and keeps running.
- UDP downloads are best-effort (no retransmissions).
- For large files, TCP is more reliable.
