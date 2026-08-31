# Server Chunked Upload Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a resumable server API that receives only changed fixed-size chunks, validates them, atomically publishes complete files, and maintains committed and pending SQLite manifests.

**Architecture:** Keep HTTP handling thin in `server/main.py`, put filesystem/protocol behavior in a new `server/chunking.py`, and make `server/storage.py` responsible only for versioned SQLite state. Each upload is identified by normalized relative path plus target file hash; staging files and pending database rows survive retry, while completion assembles and verifies once before atomic publication.

**Tech Stack:** Python, FastAPI, Pydantic, SQLite, pytest, FastAPI TestClient/httpx

**Spec:** `docs/superpowers/specs/2026-08-30-server-chunked-upload-design.md`

## Global Constraints

- Client implementation is out of scope.
- Chunks are fixed-size, zero-indexed, ordered, and hashed with SHA-256; all supplied hashes are lowercase 64-character hexadecimal strings.
- Only one target version may upload to a normalized relative path at a time; the same hash resumes and a different hash replaces the incomplete session.
- Paths use `/` after normalization, remain beneath the configured storage directory, and reject absolute, drive-qualified, empty, and `..` paths.
- Existing `GET /health` and whole-file `POST /upload` clients remain supported.
- The legacy whole-file route records chunks at exactly 1,048,576 bytes.
- Do not add authentication, deletion synchronization, cross-file deduplication, or age-based abandoned-session cleanup.
- Use temporary files on the destination filesystem and `os.replace` for chunk publication and completed-file publication.
- Never hold an entire uploaded or assembled file in memory.

---

### Task 1: Versioned manifest storage and request models

**Files:**
- Modify: `shared/models.py`
- Replace: `server/storage.py`
- Modify: `requirements.txt`
- Create: `tests/test_storage.py`

**Interfaces:**
- Consumes: only Python standard-library `sqlite3`, `dataclasses`, `pathlib`, and `collections.abc`.
- Produces: `FileInitRequest`, `FileCompleteRequest`, `ChunkRecord`, and `ServerManifestDB` with the exact methods listed below.

- [ ] **Step 1: Write failing storage and model tests**

Create `tests/test_storage.py` with literal metadata proving that a file can own multiple chunks, pending progress persists across connections, replacement cascades old chunks, and an incompatible prototype table is preserved:

```python
import sqlite3

from server.storage import ChunkRecord, ServerManifestDB
from shared.models import FileCompleteRequest, FileInitRequest


def test_manifest_persists_committed_and_pending_chunks(tmp_path):
    db_path = tmp_path / "manifest.db"
    chunks = [
        ChunkRecord(chunk_num=0, size=4, hash="a" * 64),
        ChunkRecord(chunk_num=1, size=2, hash="b" * 64),
    ]

    with ServerManifestDB(db_path) as db:
        db.replace_file("folder/data.bin", 6, 123, 4, "c" * 64, chunks)
        resumed = db.start_upload(
            "folder/data.bin", "d" * 64, 6, 4, chunks
        )
        db.mark_chunk_received("folder/data.bin", 1, 2, "b" * 64)

    assert resumed is False
    with ServerManifestDB(db_path) as db:
        assert db.get_file("folder/data.bin")["current_hash"] == "c" * 64
        assert [row["chunk_num"] for row in db.get_file_chunks("folder/data.bin")] == [0, 1]
        pending = db.get_pending_chunks("folder/data.bin")
        assert pending[0]["received_hash"] is None
        assert pending[1]["received_hash"] == "b" * 64


def test_replacing_file_removes_obsolete_chunk_rows(tmp_path):
    db_path = tmp_path / "manifest.db"
    with ServerManifestDB(db_path) as db:
        db.replace_file(
            "data.bin",
            6,
            123,
            4,
            "c" * 64,
            [ChunkRecord(0, 4, "a" * 64), ChunkRecord(1, 2, "b" * 64)],
        )
        db.replace_file(
            "data.bin", 3, 456, 4, "e" * 64, [ChunkRecord(0, 3, "f" * 64)]
        )
        assert [row["chunk_num"] for row in db.get_file_chunks("data.bin")] == [0]


def test_incompatible_prototype_table_is_preserved(tmp_path):
    db_path = tmp_path / "manifest.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE server_file_chunks (path TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO server_file_chunks VALUES ('legacy.bin')")
    conn.commit()
    conn.close()

    with ServerManifestDB(db_path) as db:
        names = {
            row[0]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "server_file_chunks" in names
        assert "legacy_server_file_chunks" in names
        assert db.conn.execute("PRAGMA user_version").fetchone()[0] == 1


def test_request_models_keep_client_contract():
    request = FileInitRequest(
        rel_file_path="folder/data.bin",
        file_hash="a" * 64,
        file_size=4,
        chunk_size=4,
        chunk_hashes=["b" * 64],
    )
    complete = FileCompleteRequest(
        rel_file_path=request.rel_file_path,
        file_hash=request.file_hash,
    )
    assert complete.rel_file_path == "folder/data.bin"
```

- [ ] **Step 2: Run the tests and verify the missing interfaces fail**

Run:

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_storage.py -v
```

Expected: collection fails because `ChunkRecord`, `FileInitRequest`, and `FileCompleteRequest` do not exist (and the current `haashlib` import may fail first).

- [ ] **Step 3: Add the Pydantic request models and runtime/test dependencies**

Implement `shared/models.py`:

```python
from pydantic import BaseModel, Field


class FileInitRequest(BaseModel):
    rel_file_path: str
    file_hash: str
    file_size: int = Field(ge=0)
    chunk_size: int = Field(gt=0)
    chunk_hashes: list[str]


class FileCompleteRequest(BaseModel):
    rel_file_path: str
    file_hash: str
```

Populate `requirements.txt` with the project's imported runtime packages plus its test runner:

```text
fastapi
httpx
pandas
python-multipart
pytest
requests
uvicorn
```

- [ ] **Step 4: Replace the broken storage module with the versioned schema**

Define this value object in `server/storage.py`:

```python
@dataclass(frozen=True)
class ChunkRecord:
    chunk_num: int
    size: int
    hash: str
```

Implement `ServerManifestDB` as a context manager with these exact signatures:

- `__init__(db_path: str | Path)`
- `__enter__() -> ServerManifestDB`
- `__exit__(exc_type, exc_value, traceback) -> None`
- `get_file(path: str) -> sqlite3.Row | None`
- `get_file_chunks(path: str) -> list[sqlite3.Row]`
- `delete_file(path: str) -> None`
- `replace_file(path: str, size: int, mtime_ns: int, chunk_size: int, current_hash: str, chunks: Sequence[ChunkRecord], *, clear_pending: bool = False) -> None`
- `get_pending_upload(path: str) -> sqlite3.Row | None`
- `get_pending_chunk(path: str, chunk_num: int) -> sqlite3.Row | None`
- `get_pending_chunks(path: str) -> list[sqlite3.Row]`
- `start_upload(path: str, target_hash: str, size: int, chunk_size: int, chunks: Sequence[ChunkRecord]) -> bool`
- `mark_chunk_received(path: str, chunk_num: int, size: int, chunk_hash: str) -> None`
- `clear_chunk_received(path: str, chunk_num: int) -> None`
- `delete_pending_upload(path: str) -> None`

Use `sqlite3.Row`, `PRAGMA foreign_keys = ON`, and `PRAGMA user_version = 1`. Before creating tables, inspect `PRAGMA table_info(server_files)` and `PRAGMA table_info(server_file_chunks)`. Rename an incompatible table to `legacy_<name>`, adding `_2`, `_3`, and so on only when that legacy name already exists. Preserve the unrelated prototype `file_chunks` table.

Create exactly these active constraints:

```sql
CREATE TABLE server_files (
    path TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    current_hash TEXT NOT NULL,
    last_synched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE server_file_chunks (
    path TEXT NOT NULL,
    chunk_num INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    current_chunk_hash TEXT NOT NULL,
    PRIMARY KEY (path, chunk_num),
    FOREIGN KEY (path) REFERENCES server_files(path) ON DELETE CASCADE
);

CREATE TABLE pending_uploads (
    path TEXT PRIMARY KEY,
    target_hash TEXT NOT NULL,
    size INTEGER NOT NULL,
    chunk_size INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE pending_upload_chunks (
    path TEXT NOT NULL,
    chunk_num INTEGER NOT NULL,
    expected_size INTEGER NOT NULL,
    expected_hash TEXT NOT NULL,
    received_size INTEGER,
    received_hash TEXT,
    received_at TEXT,
    PRIMARY KEY (path, chunk_num),
    FOREIGN KEY (path) REFERENCES pending_uploads(path) ON DELETE CASCADE
);
```

`start_upload` returns `True` only when every target and chunk field already matches and the existing session is resumed. Otherwise it atomically replaces the path's pending rows and returns `False`. `replace_file` atomically upserts the file, deletes prior committed chunks, inserts the supplied chunks, and optionally deletes pending rows.

- [ ] **Step 5: Run the focused tests and full existing suite**

Run:

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_storage.py -v
..\.venv\Scripts\python.exe -m pytest -q
```

Expected: all tests pass with no warnings or collection errors.

- [ ] **Step 6: Commit the storage foundation**

```powershell
git add shared/models.py server/storage.py requirements.txt tests/test_storage.py
git commit -m "feat: add server upload manifest storage"
```

---

### Task 2: Filesystem service, reconciliation, and legacy upload

**Files:**
- Create: `server/chunking.py`
- Replace: `server/main.py`
- Create: `tests/test_chunking.py`
- Create: `tests/test_api.py`

**Interfaces:**
- Consumes: `ChunkRecord`, `ServerManifestDB`, `FileInitRequest`, and `FileCompleteRequest` from Task 1.
- Produces: `ChunkUploadService`, protocol exception types, `normalize_relative_path`, `create_app`, `app`, and a manifest-aware streaming implementation of `POST /upload`.

- [ ] **Step 1: Write failing path, reconciliation, and legacy-upload tests**

Create `tests/test_chunking.py` with real temporary files:

```python
import hashlib
import os

import pytest

from server.chunking import ChunkUploadService, InvalidUploadError, normalize_relative_path
from server.storage import ServerManifestDB


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize("path", ["", "../escape.bin", "/absolute.bin", "C:/drive.bin"])
def test_normalize_relative_path_rejects_unsafe_paths(path):
    with pytest.raises(InvalidUploadError):
        normalize_relative_path(path)


def test_reconcile_indexes_an_existing_file_once(tmp_path, monkeypatch):
    stored = tmp_path / "existing.bin"
    stored.write_bytes(b"abcdefgh")
    service = ChunkUploadService(tmp_path)

    first = service.reconcile_manifest("existing.bin", 4)
    assert first["current_hash"] == sha256(b"abcdefgh")

    def unexpected_scan(*args, **kwargs):
        raise AssertionError("unchanged manifest must avoid hashing")

    monkeypatch.setattr(service, "_scan_file", unexpected_scan)
    second = service.reconcile_manifest("existing.bin", 4)
    assert second["current_hash"] == first["current_hash"]


def test_reconcile_refreshes_manifest_after_destination_changes(tmp_path):
    stored = tmp_path / "existing.bin"
    stored.write_bytes(b"abcdefgh")
    service = ChunkUploadService(tmp_path)
    original = service.reconcile_manifest("existing.bin", 4)

    stored.write_bytes(b"abcdWXYZ")
    os.utime(stored, ns=(stored.stat().st_atime_ns, original["mtime_ns"] + 1))
    refreshed = service.reconcile_manifest("existing.bin", 4)

    assert refreshed["current_hash"] == sha256(b"abcdWXYZ")
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        assert [row["current_chunk_hash"] for row in db.get_file_chunks("existing.bin")] == [
            sha256(b"abcd"), sha256(b"WXYZ")
        ]
```

Create `tests/test_api.py` with an application factory fixture and legacy upload behavior:

```python
import hashlib

from fastapi.testclient import TestClient

from server.main import create_app
from server.storage import ServerManifestDB


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def test_legacy_upload_streams_atomically_and_records_manifest(tmp_path):
    app = create_app(tmp_path)
    client = TestClient(app)
    content = b"legacy upload"
    response = client.post(
        "/upload",
        files={"file": ("nested.bin", content, "application/octet-stream")},
        data={"file_hash": sha256(content)},
    )

    assert response.status_code == 200
    assert (tmp_path / "nested.bin").read_bytes() == content
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        assert db.get_file("nested.bin")["current_hash"] == sha256(content)


def test_legacy_upload_rejects_content_hash_mismatch(tmp_path):
    response = TestClient(create_app(tmp_path)).post(
        "/upload",
        files={"file": ("bad.bin", b"actual", "application/octet-stream")},
        data={"file_hash": sha256(b"different")},
    )
    assert response.status_code == 400
    assert not (tmp_path / "bad.bin").exists()
```

- [ ] **Step 2: Run the tests and verify the missing service/app factory fails**

Run:

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_chunking.py tests/test_api.py -v
```

Expected: collection fails because `server.chunking` and `create_app` do not exist.

- [ ] **Step 3: Implement path validation, hashing snapshots, and manifest reconciliation**

In `server/chunking.py`, define:

```python
DEFAULT_CHUNK_SIZE = 1024 * 1024
HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class UploadProtocolError(Exception):
    status_code = 400

    def __init__(self, detail: str | dict):
        super().__init__(str(detail))
        self.detail = detail


class InvalidUploadError(UploadProtocolError):
    status_code = 400


class UploadNotFoundError(UploadProtocolError):
    status_code = 404


class UploadConflictError(UploadProtocolError):
    status_code = 409


class MissingChunksError(UploadConflictError):
    def __init__(self, missing_chunks: list[int]):
        super().__init__({"message": "upload is missing chunks", "missing_chunks": missing_chunks})


@dataclass(frozen=True)
class FileSnapshot:
    size: int
    file_hash: str
    chunks: tuple[ChunkRecord, ...]
```

Implement `normalize_relative_path(raw_path: str) -> str` and `ChunkUploadService` methods `__init__(storage_dir: str | Path)`, `reconcile_manifest(rel_file_path: str, chunk_size: int) -> sqlite3.Row | None`, `store_whole_file(stream: BinaryIO, rel_file_path: str, file_hash: str) -> dict`, and `_scan_file(path: Path, chunk_size: int) -> FileSnapshot`.

`normalize_relative_path` converts `\` to `/`, rejects the unsafe cases in the test, rejects any `..` component, and returns `PurePosixPath(normalized_text).as_posix()`. `ChunkUploadService.__init__` resolves the storage root, creates `tmp/uploads` and `server_state`, and sets `db_path` to `server_state/server_manifest.db`.

`_scan_file` reads exactly one chunk at a time, updating a whole-file SHA-256 and a fresh per-chunk SHA-256 together. `reconcile_manifest` trusts a row only when destination existence, size, mtime, and chunk size match; otherwise it scans once and calls `replace_file`. A missing destination deletes a stale committed row.

- [ ] **Step 4: Implement the FastAPI factory and streaming legacy route**

Replace `server/main.py` with an app factory that keeps the conventional module-level app:

```python
def create_app(storage_dir: str | Path = Path("server_storage")) -> FastAPI:
    app = FastAPI()
    service = ChunkUploadService(storage_dir)
    app.state.upload_service = service

    @app.exception_handler(UploadProtocolError)
    def upload_error_handler(request: Request, exc: UploadProtocolError):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/upload")
    def upload_file(file: UploadFile = File(...), file_hash: str = Form(...)):
        return service.store_whole_file(file.file, file.filename or "", file_hash)

    return app


app = create_app()
```

`store_whole_file` validates the path/hash, streams to a UUID-suffixed temporary file while computing whole and 1,048,576-byte chunk hashes, rejects mismatches before publication, flushes and calls `os.fsync`, creates destination parents, calls `os.replace`, reads the destination stat, and records the committed manifest. Always remove an unpublished temporary file in `finally`.

- [ ] **Step 5: Run focused and full tests**

Run:

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_chunking.py tests/test_api.py -v
..\.venv\Scripts\python.exe -m pytest -q
```

Expected: all tests pass and the current legacy upload contract remains operational.

- [ ] **Step 6: Commit the service foundation**

```powershell
git add server/chunking.py server/main.py tests/test_chunking.py tests/test_api.py
git commit -m "feat: add atomic server upload service"
```

---

### Task 3: Initialize and resume chunk sessions

**Files:**
- Modify: `server/chunking.py`
- Modify: `server/main.py`
- Modify: `tests/test_api.py`

**Interfaces:**
- Consumes: `FileInitRequest`, `ChunkRecord`, `ServerManifestDB.start_upload`, committed manifest reconciliation, and protocol exception handling.
- Produces: `ChunkUploadService.initialize(request: FileInitRequest) -> dict` and `POST /files/init`.

- [ ] **Step 1: Write failing init/resume/reuse tests**

Append tests that derive literal chunk hashes independently:

```python
def init_payload(path: str, content: bytes, chunk_size: int = 4) -> dict:
    chunks = [content[i : i + chunk_size] for i in range(0, len(content), chunk_size)]
    return {
        "rel_file_path": path,
        "file_hash": sha256(content),
        "file_size": len(content),
        "chunk_size": chunk_size,
        "chunk_hashes": [sha256(chunk) for chunk in chunks],
    }


def test_init_new_file_requests_every_chunk(tmp_path):
    response = TestClient(create_app(tmp_path)).post(
        "/files/init", json=init_payload("data.bin", b"abcdefghij")
    )
    assert response.status_code == 200
    assert response.json() == {
        "status": "success",
        "up_to_date": False,
        "file_hash": sha256(b"abcdefghij"),
        "missing_chunks": [0, 1, 2],
    }


def test_init_empty_file_requires_no_chunks(tmp_path):
    response = TestClient(create_app(tmp_path)).post(
        "/files/init", json=init_payload("empty.bin", b"")
    )
    assert response.status_code == 200
    assert response.json()["missing_chunks"] == []
    assert response.json()["up_to_date"] is False


def test_init_current_file_short_circuits(tmp_path):
    client = TestClient(create_app(tmp_path))
    content = b"abcdefgh"
    client.post(
        "/upload",
        files={"file": ("data.bin", content, "application/octet-stream")},
        data={"file_hash": sha256(content)},
    )
    response = client.post("/files/init", json=init_payload("data.bin", content))
    assert response.json()["up_to_date"] is True
    assert response.json()["missing_chunks"] == []


def test_init_reuses_unchanged_committed_chunk(tmp_path):
    client = TestClient(create_app(tmp_path))
    original = b"aaaabbbb"
    client.post(
        "/upload",
        files={"file": ("data.bin", original, "application/octet-stream")},
        data={"file_hash": sha256(original)},
    )
    changed = b"aaaacccc"
    first = client.post("/files/init", json=init_payload("data.bin", changed))
    assert first.json()["missing_chunks"] == [1]


def test_init_resumes_a_chunk_recorded_before_restart(tmp_path):
    target = b"abcdefgh"
    payload = init_payload("data.bin", target)
    client = TestClient(create_app(tmp_path))
    client.post("/files/init", json=payload)

    path_digest = sha256(b"data.bin")
    chunk_path = (
        tmp_path
        / "tmp"
        / "uploads"
        / path_digest
        / sha256(target)
        / "chunks"
        / "0"
    )
    chunk_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_path.write_bytes(b"abcd")
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        db.mark_chunk_received("data.bin", 0, 4, sha256(b"abcd"))

    resumed = TestClient(create_app(tmp_path)).post("/files/init", json=payload)
    assert resumed.json()["missing_chunks"] == [1]


def test_init_new_target_replaces_old_pending_session(tmp_path):
    client = TestClient(create_app(tmp_path))
    old = b"abcdefgh"
    new = b"abcdWXYZ"
    client.post("/files/init", json=init_payload("data.bin", old))
    old_session = (
        tmp_path / "tmp" / "uploads" / sha256(b"data.bin") / sha256(old)
    )
    old_session.mkdir(parents=True, exist_ok=True)
    (old_session / "orphan").write_bytes(b"partial")

    response = client.post("/files/init", json=init_payload("data.bin", new))

    assert response.status_code == 200
    assert not old_session.exists()
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        assert db.get_pending_upload("data.bin")["target_hash"] == sha256(new)


def test_init_rejects_inconsistent_chunk_count_and_unsafe_path(tmp_path):
    client = TestClient(create_app(tmp_path))
    payload = init_payload("data.bin", b"abcdefgh")
    payload["chunk_hashes"].pop()
    assert client.post("/files/init", json=payload).status_code == 400
    unsafe = init_payload("../escape.bin", b"abcd")
    assert client.post("/files/init", json=unsafe).status_code == 400
```

- [ ] **Step 2: Run init tests and verify `/files/init` is absent**

Run:

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_api.py -k "init" -v
```

Expected: requests fail with HTTP 404 because `/files/init` is not registered.

- [ ] **Step 3: Implement request validation and initialization**

Add `ChunkUploadService.initialize(request: FileInitRequest) -> dict`, `_validate_hash(value: str, label: str) -> None`, `_expected_chunks(request: FileInitRequest) -> tuple[ChunkRecord, ...]`, `_session_dir(path: str, target_hash: str) -> Path`, and `_chunk_path(path: str, target_hash: str, chunk_num: int) -> Path`.

`_expected_chunks` calculates `ceil(file_size / chunk_size)`, validates the exact hash count and every hash, and assigns full chunk sizes except for the final remainder. `_session_dir` uses `sha256(normalized_path.encode()).hexdigest()` plus the already-validated target hash.

During initialization:

1. Normalize and validate all request fields.
2. Reconcile any committed destination with the requested chunk size.
3. If its whole hash matches, delete pending rows/staging and return `up_to_date: True`.
4. Read the prior pending target; create/resume the supplied session transactionally and remove a superseded target's staging directory.
5. For a received row, trust it only when the recorded size/hash match expectations and its staged file exists; otherwise clear the received fields.
6. If a staged file exists without received fields after a crash, scan that one chunk, retain and mark it only when size/hash match, and otherwise delete it.
7. Treat a committed chunk as reusable only when index, actual size, and hash match.
8. Return all other indices in ascending order as `missing_chunks`.

- [ ] **Step 4: Register the JSON init route**

Add to `create_app`:

```python
@app.post("/files/init")
def init_file(request: FileInitRequest):
    return service.initialize(request)
```

- [ ] **Step 5: Run focused and full tests**

Run:

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_api.py -k "init" -v
..\.venv\Scripts\python.exe -m pytest -q
```

Expected: all tests pass; response booleans are JSON booleans and missing indices are ordered lists.

- [ ] **Step 6: Commit initialization**

```powershell
git add server/chunking.py server/main.py tests/test_api.py
git commit -m "feat: initialize resumable chunk uploads"
```

---

### Task 4: Validate and persist uploaded chunks

**Files:**
- Modify: `server/chunking.py`
- Modify: `server/main.py`
- Modify: `tests/test_api.py`

**Interfaces:**
- Consumes: initialized pending upload rows and staging-path helpers from Task 3.
- Produces: `ChunkUploadService.receive_chunk(rel_file_path: str, file_hash: str, chunk_num: int, chunk_hash: str, stream: BinaryIO) -> dict` and multipart `POST /files/chunks`.

- [ ] **Step 1: Write failing chunk upload, retry, and rejection tests**

Append:

```python
def post_chunk(client, path: str, target: bytes, index: int, chunk: bytes):
    return client.post(
        "/files/chunks",
        files={"chunk": (f"{index}.chunk", chunk, "application/octet-stream")},
        data={
            "rel_file_path": path,
            "file_hash": sha256(target),
            "chunk_num": str(index),
            "chunk_hash": sha256(chunk),
        },
    )


def test_chunk_upload_persists_and_retry_is_idempotent(tmp_path):
    client = TestClient(create_app(tmp_path))
    target = b"abcdefgh"
    client.post("/files/init", json=init_payload("data.bin", target))

    first = post_chunk(client, "data.bin", target, 0, b"abcd")
    second = post_chunk(client, "data.bin", target, 0, b"abcd")

    assert first.json() == {"status": "success", "chunk_num": 0, "already_received": False}
    assert second.json() == {"status": "success", "chunk_num": 0, "already_received": True}
    resumed = client.post("/files/init", json=init_payload("data.bin", target))
    assert resumed.json()["missing_chunks"] == [1]


def test_chunk_upload_rejects_wrong_content_size_hash_index_and_version(tmp_path):
    client = TestClient(create_app(tmp_path))
    target = b"abcdefgh"
    client.post("/files/init", json=init_payload("data.bin", target))

    wrong_content = post_chunk(client, "data.bin", target, 0, b"zzzz")
    wrong_size = post_chunk(client, "data.bin", target, 0, b"abc")
    wrong_index = post_chunk(client, "data.bin", target, 3, b"abcd")
    wrong_version = client.post(
        "/files/chunks",
        files={"chunk": ("0.chunk", b"abcd", "application/octet-stream")},
        data={
            "rel_file_path": "data.bin",
            "file_hash": "f" * 64,
            "chunk_num": "0",
            "chunk_hash": sha256(b"abcd"),
        },
    )

    assert wrong_content.status_code == 400
    assert wrong_size.status_code == 400
    assert wrong_index.status_code == 400
    assert wrong_version.status_code == 409
```

- [ ] **Step 2: Run tests and verify the chunk route is absent**

Run:

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_api.py -k "chunk_upload" -v
```

Expected: requests fail with HTTP 404 because `/files/chunks` is not registered.

- [ ] **Step 3: Implement streaming validation and idempotent persistence**

Implement `ChunkUploadService.receive_chunk(rel_file_path: str, file_hash: str, chunk_num: int, chunk_hash: str, stream: BinaryIO) -> dict`.

Normalize and validate form metadata; require an active pending row; return 404 when absent and 409 when `file_hash` differs. Require a pending chunk row and exact `chunk_hash`; an out-of-range index or mismatched posted chunk hash is HTTP 400.

If received metadata and the staged file already match expectations, return `already_received: True`. Otherwise stream to `<chunk-path>.part.<uuid>`, hashing and counting bytes during the write. Reject and delete the temporary file unless actual size and SHA-256 equal the expected row and posted hash. Flush, `os.fsync`, `os.replace` into the stable chunk path, then call `mark_chunk_received`. Always delete an unpublished `.part` file in `finally`.

- [ ] **Step 4: Register the multipart chunk endpoint**

Add:

```python
@app.post("/files/chunks")
def upload_chunk(
    chunk: UploadFile = File(...),
    rel_file_path: str = Form(...),
    file_hash: str = Form(...),
    chunk_num: int = Form(...),
    chunk_hash: str = Form(...),
):
    return service.receive_chunk(
        rel_file_path, file_hash, chunk_num, chunk_hash, chunk.file
    )
```

- [ ] **Step 5: Run focused and full tests**

Run:

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_api.py -k "chunk_upload" -v
..\.venv\Scripts\python.exe -m pytest -q
```

Expected: all tests pass, bad chunks leave no stable staging file, and retries do not alter stored progress.

- [ ] **Step 6: Commit chunk receipt**

```powershell
git add server/chunking.py server/main.py tests/test_api.py
git commit -m "feat: receive validated upload chunks"
```

---

### Task 5: Assemble, verify, publish, and recover completed files

**Files:**
- Modify: `server/chunking.py`
- Modify: `server/main.py`
- Modify: `tests/test_api.py`

**Interfaces:**
- Consumes: pending expected/received rows, committed chunk metadata, and stable chunk files from Tasks 1–4.
- Produces: `ChunkUploadService.complete(request: FileCompleteRequest) -> dict` and JSON `POST /files/complete`.

- [ ] **Step 1: Write failing missing-chunk, assembly, reuse, idempotency, and stale-source tests**

Append:

```python
def test_complete_lists_missing_chunks(tmp_path):
    client = TestClient(create_app(tmp_path))
    target = b"abcdefgh"
    client.post("/files/init", json=init_payload("data.bin", target))
    post_chunk(client, "data.bin", target, 0, b"abcd")

    response = client.post(
        "/files/complete",
        json={"rel_file_path": "data.bin", "file_hash": sha256(target)},
    )
    assert response.status_code == 409
    assert response.json()["detail"]["missing_chunks"] == [1]


def test_complete_assembles_chunks_updates_manifest_and_is_idempotent(tmp_path):
    client = TestClient(create_app(tmp_path))
    target = b"abcdefghij"
    client.post("/files/init", json=init_payload("nested/data.bin", target))
    post_chunk(client, "nested/data.bin", target, 0, b"abcd")
    post_chunk(client, "nested/data.bin", target, 1, b"efgh")
    post_chunk(client, "nested/data.bin", target, 2, b"ij")

    body = {"rel_file_path": "nested/data.bin", "file_hash": sha256(target)}
    first = client.post("/files/complete", json=body)
    second = client.post("/files/complete", json=body)

    assert first.json() == {
        "status": "success",
        "rel_file_path": "nested/data.bin",
        "file_hash": sha256(target),
        "bytes_written": len(target),
    }
    assert second.status_code == 200
    assert (tmp_path / "nested" / "data.bin").read_bytes() == target
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        assert db.get_pending_upload("nested/data.bin") is None
        assert [row["current_chunk_hash"] for row in db.get_file_chunks("nested/data.bin")] == [
            sha256(b"abcd"), sha256(b"efgh"), sha256(b"ij")
        ]


def test_complete_reuses_unchanged_committed_chunk(tmp_path):
    client = TestClient(create_app(tmp_path))
    original = b"aaaabbbb"
    client.post(
        "/upload",
        files={"file": ("data.bin", original, "application/octet-stream")},
        data={"file_hash": sha256(original)},
    )
    target = b"aaaacccc"
    client.post("/files/init", json=init_payload("data.bin", target))
    post_chunk(client, "data.bin", target, 1, b"cccc")

    response = client.post(
        "/files/complete",
        json={"rel_file_path": "data.bin", "file_hash": sha256(target)},
    )
    assert response.status_code == 200
    assert (tmp_path / "data.bin").read_bytes() == target


def test_complete_rechecks_changed_reusable_source(tmp_path):
    client = TestClient(create_app(tmp_path))
    original = b"aaaabbbb"
    client.post(
        "/upload",
        files={"file": ("data.bin", original, "application/octet-stream")},
        data={"file_hash": sha256(original)},
    )
    target = b"aaaacccc"
    client.post("/files/init", json=init_payload("data.bin", target))
    (tmp_path / "data.bin").write_bytes(b"zzzzbbbb")
    post_chunk(client, "data.bin", target, 1, b"cccc")

    response = client.post(
        "/files/complete",
        json={"rel_file_path": "data.bin", "file_hash": sha256(target)},
    )
    assert response.status_code == 409
    assert response.json()["detail"]["missing_chunks"] == [0]


def test_complete_publishes_empty_file(tmp_path):
    client = TestClient(create_app(tmp_path))
    client.post("/files/init", json=init_payload("empty.bin", b""))
    response = client.post(
        "/files/complete",
        json={"rel_file_path": "empty.bin", "file_hash": sha256(b"")},
    )
    assert response.status_code == 200
    assert (tmp_path / "empty.bin").read_bytes() == b""


def test_complete_rejects_inconsistent_file_and_chunk_hashes(tmp_path):
    client = TestClient(create_app(tmp_path))
    actual = b"abcd"
    payload = init_payload("data.bin", actual)
    payload["file_hash"] = sha256(b"different target")
    client.post("/files/init", json=payload)
    upload = client.post(
        "/files/chunks",
        files={"chunk": ("0.chunk", actual, "application/octet-stream")},
        data={
            "rel_file_path": "data.bin",
            "file_hash": payload["file_hash"],
            "chunk_num": "0",
            "chunk_hash": sha256(actual),
        },
    )
    assert upload.status_code == 200

    response = client.post(
        "/files/complete",
        json={"rel_file_path": "data.bin", "file_hash": payload["file_hash"]},
    )
    assert response.status_code == 400
    assert not (tmp_path / "data.bin").exists()
```

- [ ] **Step 2: Run completion tests and verify the route is absent**

Run:

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_api.py -k "complete" -v
```

Expected: requests fail with HTTP 404 because `/files/complete` is not registered.

- [ ] **Step 3: Implement source revalidation, assembly, and atomic commit**

Implement `ChunkUploadService.complete(request: FileCompleteRequest) -> dict` and `_missing_chunks(path: str, target_hash: str) -> list[int]`.

Completion must follow this exact order:

1. Normalize the path and validate the target hash.
2. If no pending row exists, reconcile an existing committed row using its stored chunk size; return success only when its current hash matches, otherwise return 404.
3. Return 409 when the pending target differs.
4. Reconcile the committed destination again using the pending chunk size so changed size/mtime invalidates prior reuse decisions.
5. For every expected row in order, choose the staged file only when received metadata and existence match; otherwise choose the committed range only when index, size, and hash match the reconciled committed row.
6. Raise `MissingChunksError` with all unavailable indices before creating the assembled file.
7. Stream sources in index order into `<session-dir>/assembled.part.<uuid>`. Keep the committed source open once, seek to `chunk_num * chunk_size`, read exactly `expected_size`, and close it before publication. Update whole-file SHA-256 and byte count during writes.
8. Reject final size/hash mismatches, delete only the assembled `.part`, and preserve valid staged chunks.
9. Flush and `os.fsync`; create destination parents; close all source handles; `os.replace` the assembled file over the destination.
10. Read destination stat and call `replace_file(path, size, stat.st_mtime_ns, chunk_size, target_hash, expected_chunks, clear_pending=True)` using the already-validated expected chunk records.
11. Remove the completed session directory after the database commit. Cleanup failure must not change a successful response.

- [ ] **Step 4: Register the JSON completion endpoint**

Add:

```python
@app.post("/files/complete")
def complete_file(request: FileCompleteRequest):
    return service.complete(request)
```

- [ ] **Step 5: Run focused tests, full tests, compilation, and diff checks**

Run:

```powershell
..\.venv\Scripts\python.exe -m pytest tests/test_api.py -k "complete" -v
..\.venv\Scripts\python.exe -m pytest -q
..\.venv\Scripts\python.exe -m compileall -q server shared tests
git diff --check
```

Expected: every command exits 0, all tests pass, no warnings/errors are emitted, and compilation creates no diagnostics.

- [ ] **Step 6: Manually inspect the generated OpenAPI contract**

Run:

```powershell
..\.venv\Scripts\python.exe -c "from server.main import app; print(sorted((path, sorted(methods)) for path, methods in app.openapi()['paths'].items()))"
```

Expected paths: `/files/chunks`, `/files/complete`, `/files/init`, `/health`, and `/upload`, with the new metadata endpoints represented as JSON bodies and chunk receipt represented as multipart form data.

- [ ] **Step 7: Commit completed chunking support**

```powershell
git add server/chunking.py server/main.py tests/test_api.py
git commit -m "feat: finalize resumable chunk uploads"
```

- [ ] **Step 8: Request final code review and address all critical or important findings**

Compare the implementation commits with `6efb60e`, verify each requirement in the linked specification, run the complete verification commands again after any review fix, and only then report completion.
