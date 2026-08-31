import hashlib
from pathlib import Path

import requests

from client.uploader import ChunkedUploader
from client.manifest import ManifestDB
from client.scanner import Scanner


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.responses)


class FakeScanner:
    def __init__(self, files):
        self.files = files

    def get_file_status(self):
        return self.files


class FakeManifest:
    def __init__(self):
        self.synched = []

    def mark_as_synched(self, path, file_hash):
        self.synched.append((path, file_hash))


def changed_file(path: Path, relative_path: str):
    content = path.read_bytes()
    return {
        "full_path": str(path),
        "relative_path": relative_path,
        "changed": True,
        "current_hash": sha256(content),
    }


def test_sync_once_posts_only_missing_chunks_completes_and_marks_manifest(tmp_path):
    file_path = tmp_path / "data.bin"
    file_path.write_bytes(b"abcdef")
    file_hash = sha256(b"abcdef")
    session = FakeSession(
        [
            FakeResponse(
                {
                    "status": "success",
                    "up_to_date": False,
                    "file_hash": file_hash,
                    "missing_chunks": [1],
                }
            ),
            FakeResponse({"status": "success", "chunk_num": 1}),
            FakeResponse(
                {
                    "status": "success",
                    "rel_file_path": "nested/data.bin",
                    "file_hash": file_hash,
                    "bytes_written": 6,
                }
            ),
        ]
    )
    manifest = FakeManifest()
    uploader = ChunkedUploader(
        scanner=FakeScanner([changed_file(file_path, "nested\\data.bin")]),
        manifest=manifest,
        session=session,
        chunk_size=3,
    )

    results = uploader.sync_once()

    assert results == [{"path": "nested\\data.bin", "status": "synced"}]
    assert manifest.synched == [(str(file_path), file_hash)]
    assert session.calls[0] == (
        "http://127.0.0.1:8000/files/init",
        {
            "json": {
                "rel_file_path": "nested/data.bin",
                "file_hash": file_hash,
                "file_size": 6,
                "chunk_size": 3,
                "chunk_hashes": [sha256(b"abc"), sha256(b"def")],
            }
        },
    )
    assert session.calls[1] == (
        "http://127.0.0.1:8000/files/chunks",
        {
            "data": {
                "rel_file_path": "nested/data.bin",
                "file_hash": file_hash,
                "chunk_num": "1",
                "chunk_hash": sha256(b"def"),
            },
            "files": {"chunk": ("1.chunk", b"def", "application/octet-stream")},
        },
    )
    assert session.calls[2] == (
        "http://127.0.0.1:8000/files/complete",
        {"json": {"rel_file_path": "nested/data.bin", "file_hash": file_hash}},
    )


def test_sync_once_marks_manifest_from_server_up_to_date_acknowledgement(tmp_path):
    file_path = tmp_path / "data.bin"
    file_path.write_bytes(b"already there")
    file_hash = sha256(b"already there")
    session = FakeSession(
        [
            FakeResponse(
                {
                    "status": "success",
                    "up_to_date": True,
                    "file_hash": file_hash,
                    "missing_chunks": [],
                }
            )
        ]
    )
    manifest = FakeManifest()
    uploader = ChunkedUploader(
        scanner=FakeScanner([changed_file(file_path, "data.bin")]),
        manifest=manifest,
        session=session,
        chunk_size=3,
    )

    results = uploader.sync_once()

    assert results == [{"path": "data.bin", "status": "synced"}]
    assert manifest.synched == [(str(file_path), file_hash)]
    assert len(session.calls) == 1


def test_sync_once_leaves_manifest_unsynched_when_completion_is_rejected(tmp_path):
    file_path = tmp_path / "data.bin"
    file_path.write_bytes(b"abc")
    file_hash = sha256(b"abc")
    session = FakeSession(
        [
            FakeResponse(
                {
                    "status": "success",
                    "up_to_date": False,
                    "file_hash": file_hash,
                    "missing_chunks": [0],
                }
            ),
            FakeResponse({"status": "success", "chunk_num": 0}),
            FakeResponse({"detail": "not complete"}, status_code=409),
        ]
    )
    manifest = FakeManifest()
    uploader = ChunkedUploader(
        scanner=FakeScanner([changed_file(file_path, "data.bin")]),
        manifest=manifest,
        session=session,
        chunk_size=3,
    )

    results = uploader.sync_once()

    assert results == [{"path": "data.bin", "status": "failed"}]
    assert manifest.synched == []


def test_scanner_retries_an_unchanged_file_until_it_has_been_synched(
    tmp_path, monkeypatch
):
    watched_dir = tmp_path / "watched"
    watched_dir.mkdir()
    file_path = watched_dir / "data.bin"
    file_path.write_bytes(b"pending upload")
    manifest = ManifestDB(tmp_path / "manifest.db")
    monkeypatch.setattr("client.scanner.ManifestDB", lambda: manifest)
    scanner = Scanner(watched_dir)

    first_scan = scanner.get_file_status()
    second_scan = scanner.get_file_status()
    manifest.mark_as_synched(str(file_path), sha256(b"pending upload"))
    third_scan = scanner.get_file_status()

    assert first_scan[0]["changed"] is True
    assert second_scan[0]["changed"] is True
    assert third_scan[0]["changed"] is False
    manifest.close()


def test_sync_once_rejects_completion_acknowledging_another_path(tmp_path):
    file_path = tmp_path / "data.bin"
    file_path.write_bytes(b"abc")
    file_hash = sha256(b"abc")
    session = FakeSession(
        [
            FakeResponse(
                {
                    "status": "success",
                    "up_to_date": False,
                    "file_hash": file_hash,
                    "missing_chunks": [0],
                }
            ),
            FakeResponse({"status": "success", "chunk_num": 0}),
            FakeResponse(
                {
                    "status": "success",
                    "rel_file_path": "other.bin",
                    "file_hash": file_hash,
                    "bytes_written": 3,
                }
            ),
        ]
    )
    manifest = FakeManifest()
    uploader = ChunkedUploader(
        scanner=FakeScanner([changed_file(file_path, "data.bin")]),
        manifest=manifest,
        session=session,
        chunk_size=3,
    )

    results = uploader.sync_once()

    assert results == [{"path": "data.bin", "status": "failed"}]
    assert manifest.synched == []
