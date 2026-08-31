# Data Synchronisation Tool

A prototype for reliably synchronising a watched client directory to a server
over a low-bandwidth connection. Files are split into fixed-size chunks, so a
retry sends only the chunks that the server still needs.

The current prototype performs one client scan per run. It is designed to be
run again after a connection or server error: the server keeps a pending upload
session, and the client keeps the file unsynchronised until the server confirms
the completed file.

## How it works

For every changed file, the client:

1. Scans `test_data/watched` and compares each file with its local SQLite
   manifest.
2. Calculates the whole-file hash and SHA-256 hash for each 1 MiB chunk.
3. Calls `POST /files/init` so the server can report the missing chunk indexes.
4. Uploads only those missing chunks with `POST /files/chunks`.
5. Calls `POST /files/complete` to have the server assemble and validate the
   final file.
6. Marks the file as synchronised in the client manifest only after the server
   acknowledges the matching path and file hash.

The server validates each chunk and stores upload progress in its own manifest.
This means an interrupted transfer can continue on the next client run without
resending chunks that were already accepted.

## Requirements

- Python 3.10 or later
- PowerShell (the commands below target Windows)

Create and populate a virtual environment from the project directory:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Run the prototype

Open two PowerShell terminals in the project directory.

In the first terminal, start the server:

```powershell
.\.venv\Scripts\python.exe -m uvicorn server.main:app --reload
```

The server listens at `http://127.0.0.1:8000` and stores committed files in
`server_storage` by default.

In the second terminal, run one client scan and upload pass:

```powershell
.\.venv\Scripts\python.exe -m client.main
```

Put files in `test_data/watched` before running the client. The client prints
one `synced` or `failed` line for every changed file it processes. Run the same
command again after a failure to resume the pending upload.

## Local state

- `test_data/watched/` — default directory scanned by the client.
- `state/manifest.db` — client SQLite manifest. It records the current and
  last successfully synchronised hash for each watched file.
- `server_storage/` — server destination root.
- `server_storage/server_state/server_manifest.db` — server SQLite manifest
  for committed files and pending upload sessions.
- `server_storage/tmp/uploads/` — server-side temporary chunk staging area.

The client and server state directories are runtime data. Delete them only if
you deliberately want to discard local sync history or pending uploads.

## Project layout

| Path | Responsibility |
| --- | --- |
| `client/main.py` | One-shot command-line entry point. |
| `client/uploader.py` | Client chunk protocol: initialise, upload missing chunks, complete, and acknowledge. |
| `client/scanner.py` | Recursively finds watched files and decides which files still need synchronising. |
| `client/manifest.py` | Client SQLite manifest access. |
| `client/hashing.py` | Streaming whole-file SHA-256 helper used by the scanner. |
| `server/main.py` | FastAPI application and HTTP routes. |
| `server/chunking.py` | Upload validation, staging, resumable chunk handling, final assembly, and safe publication. |
| `server/storage.py` | Server SQLite manifest schema and transactional updates. |
| `shared/models.py` | Request models shared by the client/server protocol. |
| `tests/` | Automated tests for client upload behaviour, server API, chunking, and manifests. |

## Current scope

This is a first prototype. It performs synchronous requests, runs one scan at a
time, and does not yet include directory watching, scheduling, authentication,
or configurable command-line options. The default chunk size is 1 MiB on both
the client and server.
