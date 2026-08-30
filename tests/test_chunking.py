import hashlib
import os

import pytest

from server.chunking import ChunkUploadService, InvalidUploadError, normalize_relative_path
from server.storage import ServerManifestDB


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.mark.parametrize(
    "path", ["", "../escape.bin", "/absolute.bin", "C:/drive.bin", "C:relative.bin"]
)
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
    os.utime(
        stored,
        ns=(stored.stat().st_atime_ns, original["mtime_ns"] + 1_000_000_000),
    )
    refreshed = service.reconcile_manifest("existing.bin", 4)

    assert refreshed["current_hash"] == sha256(b"abcdWXYZ")
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        assert [row["current_chunk_hash"] for row in db.get_file_chunks("existing.bin")] == [
            sha256(b"abcd"), sha256(b"WXYZ")
        ]
