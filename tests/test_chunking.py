import hashlib
import io
import multiprocessing
import os
import subprocess
import threading

import pytest

from server.chunking import (
    ChunkUploadService,
    InvalidUploadError,
    UploadConflictError,
    normalize_relative_path,
)
from server.storage import ChunkRecord, ServerManifestDB
from shared.models import FileCompleteRequest, FileInitRequest


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


def create_directory_link(link, target) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        if os.name != "nt":
            raise
        subprocess.run(
            ["cmd.exe", "/c", "mklink", "/J", str(link), str(target)],
            check=True,
            capture_output=True,
        )


def hold_path_lock(storage_dir, path, acquired, release) -> None:
    service = ChunkUploadService(storage_dir)
    with service._path_lock(path):
        acquired.set()
        release.wait(timeout=10)


def acquire_path_lock(storage_dir, path, acquired) -> None:
    service = ChunkUploadService(storage_dir)
    with service._path_lock(path):
        acquired.set()


@pytest.mark.parametrize(
    "path", ["", "../escape.bin", "/absolute.bin", "C:/drive.bin", "C:relative.bin"]
)
def test_normalize_relative_path_rejects_unsafe_paths(path):
    with pytest.raises(InvalidUploadError):
        normalize_relative_path(path)


@pytest.mark.parametrize(
    "path",
    [
        "tmp/client.bin",
        "TMP/client.bin",
        "tmp./client.bin",
        "server_state/manifest.db",
    ],
)
def test_normalize_relative_path_rejects_server_internal_namespaces(path):
    with pytest.raises(InvalidUploadError):
        normalize_relative_path(path)


def test_whole_file_upload_rejects_resolved_directory_link_escape(tmp_path):
    storage_dir = tmp_path / "storage"
    outside_dir = tmp_path / "outside"
    storage_dir.mkdir()
    outside_dir.mkdir()
    create_directory_link(storage_dir / "linked", outside_dir)
    service = ChunkUploadService(storage_dir)
    content = b"must stay inside storage"

    with pytest.raises(InvalidUploadError):
        service.store_whole_file(
            io.BytesIO(content), "linked/escape.bin", sha256(content)
        )

    assert not (outside_dir / "escape.bin").exists()


def test_whole_file_upload_rejects_link_to_server_internal_directory(tmp_path):
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    service = ChunkUploadService(storage_dir)
    manifest = service.state_dir / "server_manifest.db"
    manifest.write_bytes(b"server manifest")
    create_directory_link(storage_dir / "state_alias", service.state_dir)

    with pytest.raises(InvalidUploadError):
        service.store_whole_file(
            io.BytesIO(b"replacement"),
            "state_alias/server_manifest.db",
            sha256(b"replacement"),
        )

    assert manifest.read_bytes() == b"server manifest"


def test_path_lock_serializes_across_processes(tmp_path):
    context = multiprocessing.get_context("spawn")
    first_acquired = context.Event()
    second_acquired = context.Event()
    release_first = context.Event()
    first = context.Process(
        target=hold_path_lock,
        args=(tmp_path, "data.bin", first_acquired, release_first),
    )
    second = context.Process(
        target=acquire_path_lock,
        args=(tmp_path, "data.bin", second_acquired),
    )

    first.start()
    try:
        assert first_acquired.wait(timeout=5)
        second.start()
        assert not second_acquired.wait(timeout=0.3)
        release_first.set()
        assert second_acquired.wait(timeout=5)
    finally:
        release_first.set()
        first.join(timeout=5)
        second.join(timeout=5)
        if first.is_alive():
            first.terminate()
        if second.is_alive():
            second.terminate()

    assert first.exitcode == 0
    assert second.exitcode == 0


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


def test_receive_chunk_does_not_mark_a_replaced_pending_upload(tmp_path, monkeypatch):
    path = "data.bin"
    version_a = b"abcdefgh"
    version_b = b"abcdWXYZ"
    request_a = FileInitRequest(
        rel_file_path=path,
        file_hash=sha256(version_a),
        file_size=len(version_a),
        chunk_size=4,
        chunk_hashes=[sha256(b"abcd"), sha256(b"efgh")],
    )
    request_b = FileInitRequest(
        rel_file_path=path,
        file_hash=sha256(version_b),
        file_size=len(version_b),
        chunk_size=4,
        chunk_hashes=[sha256(b"abcd"), sha256(b"WXYZ")],
    )
    service = ChunkUploadService(tmp_path)
    service.initialize(request_a)
    original_replace = os.replace

    def replace_then_initialize_b(source, destination):
        original_replace(source, destination)
        service.initialize(request_b)

    monkeypatch.setattr("server.chunking.os.replace", replace_then_initialize_b)

    with pytest.raises(UploadConflictError):
        service.receive_chunk(
            path,
            sha256(version_a),
            0,
            sha256(b"abcd"),
            io.BytesIO(b"abcd"),
        )

    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        pending_upload = db.get_pending_upload(path)
        pending_chunk = db.get_pending_chunk(path, 0)
        assert pending_upload["target_hash"] == sha256(version_b)
        assert pending_chunk["received_size"] is None
        assert pending_chunk["received_hash"] is None


def test_receive_chunk_removes_artifact_when_manifest_update_is_rejected(
    tmp_path, monkeypatch
):
    path = "data.bin"
    content = b"abcdefgh"
    target_hash = sha256(content)
    service = ChunkUploadService(tmp_path)
    service.initialize(
        FileInitRequest(
            rel_file_path=path,
            file_hash=target_hash,
            file_size=len(content),
            chunk_size=4,
            chunk_hashes=[sha256(b"abcd"), sha256(b"efgh")],
        )
    )
    monkeypatch.setattr(
        ServerManifestDB,
        "mark_chunk_received",
        lambda *args, **kwargs: False,
    )

    with pytest.raises(UploadConflictError):
        service.receive_chunk(
            path,
            target_hash,
            0,
            sha256(b"abcd"),
            io.BytesIO(b"abcd"),
        )

    assert not service._chunk_path(path, target_hash, 0).exists()


def test_receive_chunk_retry_uses_received_metadata_without_rehashing_staged_file(
    tmp_path, monkeypatch
):
    content = b"abcdefgh"
    request = FileInitRequest(
        rel_file_path="data.bin",
        file_hash=sha256(content),
        file_size=len(content),
        chunk_size=4,
        chunk_hashes=[sha256(b"abcd"), sha256(b"efgh")],
    )
    service = ChunkUploadService(tmp_path)
    service.initialize(request)
    service.receive_chunk(
        "data.bin",
        sha256(content),
        0,
        sha256(b"abcd"),
        io.BytesIO(b"abcd"),
    )

    def unexpected_hash(*args, **kwargs):
        raise AssertionError("ordinary retries must not scan staged chunks")

    monkeypatch.setattr(service, "_hash_file", unexpected_hash)

    assert service.receive_chunk(
        "data.bin",
        sha256(content),
        0,
        sha256(b"abcd"),
        io.BytesIO(b"abcd"),
    ) == {"status": "success", "chunk_num": 0, "already_received": True}


def test_newer_legacy_upload_cannot_be_overtaken_by_older_completion(
    tmp_path, monkeypatch
):
    path = "data.bin"
    older = b"older-v1"
    newer = b"newer-v2"
    service = ChunkUploadService(tmp_path)
    older_request = FileInitRequest(
        **{
            "rel_file_path": path,
            "file_hash": sha256(older),
            "file_size": len(older),
            "chunk_size": len(older),
            "chunk_hashes": [sha256(older)],
        }
    )
    service.initialize(older_request)
    service.receive_chunk(
        path,
        sha256(older),
        0,
        sha256(older),
        io.BytesIO(older),
    )

    older_copy_paused = threading.Event()
    release_older_copy = threading.Event()
    newer_finished = threading.Event()
    errors = {}
    original_copy_source = service._copy_source

    def pausing_copy_source(*args, **kwargs):
        older_copy_paused.set()
        assert release_older_copy.wait(timeout=5)
        return original_copy_source(*args, **kwargs)

    def complete_older():
        try:
            service.complete(
                FileCompleteRequest(rel_file_path=path, file_hash=sha256(older))
            )
        except Exception as exc:  # pragma: no cover - asserted below
            errors["older"] = exc

    def upload_newer():
        try:
            service.store_whole_file(io.BytesIO(newer), path, sha256(newer))
        except Exception as exc:  # pragma: no cover - asserted below
            errors["newer"] = exc
        finally:
            newer_finished.set()

    monkeypatch.setattr(service, "_copy_source", pausing_copy_source)
    older_thread = threading.Thread(target=complete_older)
    newer_thread = threading.Thread(target=upload_newer)
    older_thread.start()
    assert older_copy_paused.wait(timeout=5)
    newer_thread.start()
    newer_overtook_older = newer_finished.wait(timeout=1)
    release_older_copy.set()
    older_thread.join(timeout=5)
    newer_thread.join(timeout=5)

    assert not older_thread.is_alive()
    assert not newer_thread.is_alive()
    assert newer_overtook_older is False
    assert errors == {}
    assert (tmp_path / path).read_bytes() == newer
    with ServerManifestDB(service.db_path) as db:
        assert db.get_file(path)["current_hash"] == sha256(newer)


def test_reconciliation_serializes_with_same_path_legacy_publication(
    tmp_path, monkeypatch
):
    path = "data.bin"
    original = b"original"
    newer = b"new-data"
    destination = tmp_path / path
    destination.write_bytes(original)
    service = ChunkUploadService(tmp_path)
    snapshot_ready = threading.Event()
    release_reconcile = threading.Event()
    upload_finished = threading.Event()
    errors = {}
    original_scan_file = service._scan_file

    def pausing_scan_file(*args, **kwargs):
        snapshot = original_scan_file(*args, **kwargs)
        snapshot_ready.set()
        assert release_reconcile.wait(timeout=5)
        return snapshot

    def reconcile_original():
        try:
            service.reconcile_manifest(path, len(original))
        except Exception as exc:  # pragma: no cover - asserted below
            errors["reconcile"] = exc

    def upload_newer():
        try:
            service.store_whole_file(io.BytesIO(newer), path, sha256(newer))
        except Exception as exc:  # pragma: no cover - asserted below
            errors["upload"] = exc
        finally:
            upload_finished.set()

    monkeypatch.setattr(service, "_scan_file", pausing_scan_file)
    reconcile_thread = threading.Thread(target=reconcile_original)
    upload_thread = threading.Thread(target=upload_newer)
    reconcile_thread.start()
    assert snapshot_ready.wait(timeout=5)
    upload_thread.start()
    upload_overtook_reconcile = upload_finished.wait(timeout=1)
    release_reconcile.set()
    reconcile_thread.join(timeout=5)
    upload_thread.join(timeout=5)

    assert not reconcile_thread.is_alive()
    assert not upload_thread.is_alive()
    assert upload_overtook_reconcile is False
    assert errors == {}
    assert destination.read_bytes() == newer
    with ServerManifestDB(service.db_path) as db:
        assert db.get_file(path)["current_hash"] == sha256(newer)
