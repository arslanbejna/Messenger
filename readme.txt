Files included
- server.py
- client.py

Requirements
- Python 3.13


How to run
1) Start the server (choose a port):
   python server.py port

2) Start one or more clients (each in a new terminal):
   python client.py username host port

Basic chat commands
- Broadcast (default): type a message and press Enter
- Unicast:            @username message
- Quit cleanly:       /quit   (or /exit)

Groups
- Join/create group:  /join groupname
  (If the group does not exist yet, the server creates it when you join.)
- Leave group:        /leave groupname
- Group message:      #groupname message
  (Only members of that group receive it.)

Shared files / downloads
The server shares files from the folder "SharedFiles" by default.
Optionally, you can set an environment variable on the server machine:

Windows (CMD):
  set SERVER_SHARED_FILES=C:\path\to\SharedFiles

Windows (PowerShell):
  $env:SERVER_SHARED_FILES="C:\path\to\SharedFiles"

Client commands for files
1) Access the shared folder + get the file list:
   /access

2) Choose download protocol:
   /proto tcp
   /proto udp

3) Download a file:
   /get filename.ext

Download location
- Downloads are saved into a folder named after your username (created automatically),
  e.g. ./username/filename.ext

Notes
- If a client disconnects unexpectedly, the server removes it and keeps running.
- UDP downloads are best-effort (no retransmissions). For large files, TCP is more reliable.
