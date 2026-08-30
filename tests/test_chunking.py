import hashlib
import os

import pytest

from server.chunking import ChunkUploadService, InvalidUploadError, normalize_relative_path
from server.storage import ChunkRecord, ServerManifestDB
from shared.models import FileInitRequest


HASH_BUFFER_SIZE = 64 * 1024


class ReadSizeGuard:
    def __init__(self, source, maximum_read_size: int, read_sizes: list[int]):
        self.source = source
        self.maximum_read_size = maximum_read_size
        self.read_sizes = read_sizes

    def __enter__(self):
        self.source.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.source.__exit__(exc_type, exc_value, traceback)

    def read(self, size: int = -1):
        assert 0 < size <= self.maximum_read_size
        self.read_sizes.append(size)
        return self.source.read(size)


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


def test_scan_file_uses_fixed_reads_and_preserves_requested_chunk_boundaries(
    tmp_path, monkeypatch
):
    logical_chunk_size = HASH_BUFFER_SIZE * 2 + 3
    content = b"a" * logical_chunk_size + b"b" * 17
    stored = tmp_path / "existing.bin"
    stored.write_bytes(content)
    service = ChunkUploadService(tmp_path)
    original_open = type(stored).open
    read_sizes = []

    def guarded_open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        source = original_open(
            self,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
        if self == stored and mode == "rb":
            return ReadSizeGuard(source, HASH_BUFFER_SIZE, read_sizes)
        return source

    monkeypatch.setattr(type(stored), "open", guarded_open)

    snapshot = service._scan_file(stored, logical_chunk_size)

    assert read_sizes
    assert snapshot.size == len(content)
    assert snapshot.file_hash == sha256(content)
    assert snapshot.chunks == (
        ChunkRecord(0, logical_chunk_size, sha256(content[:logical_chunk_size])),
        ChunkRecord(1, 17, sha256(content[logical_chunk_size:])),
    )


def test_init_crash_recovery_hashes_staged_chunk_in_fixed_reads(tmp_path, monkeypatch):
    content = b"x" * (HASH_BUFFER_SIZE * 2 + 1)
    request = FileInitRequest(
        rel_file_path="data.bin",
        file_hash=sha256(content),
        file_size=len(content),
        chunk_size=len(content),
        chunk_hashes=[sha256(content)],
    )
    service = ChunkUploadService(tmp_path)
    service.initialize(request)
    staged = service._chunk_path("data.bin", sha256(content), 0)
    staged.parent.mkdir(parents=True)
    staged.write_bytes(content)
    original_open = type(staged).open
    read_sizes = []

    def guarded_open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        source = original_open(
            self,
            mode=mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )
        if self == staged and mode == "rb":
            return ReadSizeGuard(source, HASH_BUFFER_SIZE, read_sizes)
        return source

    monkeypatch.setattr(type(staged), "open", guarded_open)

    result = service.initialize(request)

    assert result["missing_chunks"] == []
    assert read_sizes
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        pending = db.get_pending_chunk("data.bin", 0)
        assert (pending["received_size"], pending["received_hash"]) == (
            len(content),
            sha256(content),
        )
