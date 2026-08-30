import hashlib
import os
import re
import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from server.storage import ChunkRecord, ServerManifestDB
from shared.models import FileInitRequest


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
        super().__init__(
            {"message": "upload is missing chunks", "missing_chunks": missing_chunks}
        )


@dataclass(frozen=True)
class FileSnapshot:
    size: int
    file_hash: str
    chunks: tuple[ChunkRecord, ...]


def normalize_relative_path(raw_path: str) -> str:
    normalized_text = raw_path.replace("\\", "/")
    path = PurePosixPath(normalized_text)
    if (
        not normalized_text
        or normalized_text.startswith("/")
        or re.match(r"^[A-Za-z]:", normalized_text)
        or ".." in path.parts
        or path.as_posix() == "."
    ):
        raise InvalidUploadError("file path must be a safe relative path")
    return path.as_posix()


class ChunkUploadService:
    def __init__(self, storage_dir: str | Path):
        self.storage_dir = Path(storage_dir).resolve()
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.uploads_dir = self.storage_dir / "tmp" / "uploads"
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir = self.storage_dir / "server_state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.state_dir / "server_manifest.db"

    def reconcile_manifest(
        self, rel_file_path: str, chunk_size: int
    ) -> sqlite3.Row | None:
        path = normalize_relative_path(rel_file_path)
        if chunk_size <= 0:
            raise InvalidUploadError("chunk size must be positive")
        destination = self.storage_dir / path

        with ServerManifestDB(self.db_path) as db:
            existing = db.get_file(path)
            if not destination.is_file():
                if existing is not None:
                    db.delete_file(path)
                return None

            stat = destination.stat()
            if existing is not None and (
                existing["size"],
                existing["mtime_ns"],
                existing["chunk_size"],
            ) == (stat.st_size, stat.st_mtime_ns, chunk_size):
                return existing

            snapshot = self._scan_file(destination, chunk_size)
            db.replace_file(
                path,
                stat.st_size,
                stat.st_mtime_ns,
                chunk_size,
                snapshot.file_hash,
                snapshot.chunks,
            )
            return db.get_file(path)

    def store_whole_file(
        self, stream: BinaryIO, rel_file_path: str, file_hash: str
    ) -> dict:
        path = normalize_relative_path(rel_file_path)
        if not HASH_PATTERN.fullmatch(file_hash):
            raise InvalidUploadError("file hash must be a lowercase SHA-256 hash")

        temporary_path = self.uploads_dir / f"upload-{uuid.uuid4().hex}.tmp"
        whole_hash = hashlib.sha256()
        chunks: list[ChunkRecord] = []
        size = 0

        try:
            with temporary_path.open("xb") as temporary_file:
                chunk_num = 0
                while data := stream.read(DEFAULT_CHUNK_SIZE):
                    temporary_file.write(data)
                    whole_hash.update(data)
                    chunks.append(
                        ChunkRecord(
                            chunk_num=chunk_num,
                            size=len(data),
                            hash=hashlib.sha256(data).hexdigest(),
                        )
                    )
                    chunk_num += 1
                    size += len(data)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())

            calculated_hash = whole_hash.hexdigest()
            if calculated_hash != file_hash:
                raise InvalidUploadError("file hash does not match uploaded content")

            destination = self.storage_dir / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(temporary_path, destination)
            temporary_path = None

            stat = destination.stat()
            with ServerManifestDB(self.db_path) as db:
                db.replace_file(
                    path,
                    size,
                    stat.st_mtime_ns,
                    DEFAULT_CHUNK_SIZE,
                    calculated_hash,
                    chunks,
                    clear_pending=True,
                )

            return {
                "status": "success",
                "filename": path,
                "bytes_received": size,
                "calculated_hash": calculated_hash,
                "posted_hash": file_hash,
                "hash_matches": True,
            }
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def initialize(self, request: FileInitRequest) -> dict:
        path = normalize_relative_path(request.rel_file_path)
        self._validate_hash(request.file_hash, "file hash")
        expected_chunks = self._expected_chunks(request)
        session_dir = self._session_dir(path, request.file_hash)

        manifest = self.reconcile_manifest(path, request.chunk_size)
        if manifest is not None and manifest["current_hash"] == request.file_hash:
            with ServerManifestDB(self.db_path) as db:
                db.delete_pending_upload(path)
            shutil.rmtree(session_dir.parent, ignore_errors=True)
            return {
                "status": "success",
                "up_to_date": True,
                "file_hash": request.file_hash,
                "missing_chunks": [],
            }

        with ServerManifestDB(self.db_path) as db:
            prior = db.get_pending_upload(path)
            db.start_upload(
                path,
                request.file_hash,
                request.file_size,
                request.chunk_size,
                expected_chunks,
            )
            if prior is not None and prior["target_hash"] != request.file_hash:
                shutil.rmtree(self._session_dir(path, prior["target_hash"]), ignore_errors=True)

            pending_chunks = {
                row["chunk_num"]: row for row in db.get_pending_chunks(path)
            }
            reusable_chunks = {
                row["chunk_num"]
                for row in db.get_file_chunks(path)
                if row["chunk_num"] < len(expected_chunks)
                and (row["chunk_size"], row["current_chunk_hash"])
                == (
                    expected_chunks[row["chunk_num"]].size,
                    expected_chunks[row["chunk_num"]].hash,
                )
            }
            received_chunks = set()
            for expected in expected_chunks:
                pending = pending_chunks[expected.chunk_num]
                chunk_path = self._chunk_path(path, request.file_hash, expected.chunk_num)
                if pending["received_size"] is not None or pending["received_hash"] is not None:
                    if (
                        pending["received_size"],
                        pending["received_hash"],
                    ) == (expected.size, expected.hash) and chunk_path.is_file():
                        received_chunks.add(expected.chunk_num)
                    else:
                        db.clear_chunk_received(path, expected.chunk_num)
                elif chunk_path.is_file():
                    content = chunk_path.read_bytes()
                    if (len(content), hashlib.sha256(content).hexdigest()) == (
                        expected.size,
                        expected.hash,
                    ):
                        db.mark_chunk_received(
                            path, expected.chunk_num, expected.size, expected.hash
                        )
                        received_chunks.add(expected.chunk_num)
                    else:
                        chunk_path.unlink()

        missing_chunks = [
            chunk.chunk_num
            for chunk in expected_chunks
            if chunk.chunk_num not in received_chunks
            and chunk.chunk_num not in reusable_chunks
        ]
        return {
            "status": "success",
            "up_to_date": False,
            "file_hash": request.file_hash,
            "missing_chunks": missing_chunks,
        }

    def _validate_hash(self, value: str, label: str) -> None:
        if not HASH_PATTERN.fullmatch(value):
            raise InvalidUploadError(f"{label} must be a lowercase SHA-256 hash")

    def _expected_chunks(self, request: FileInitRequest) -> tuple[ChunkRecord, ...]:
        if request.file_size < 0:
            raise InvalidUploadError("file size must not be negative")
        if request.chunk_size <= 0:
            raise InvalidUploadError("chunk size must be positive")
        expected_count = (request.file_size + request.chunk_size - 1) // request.chunk_size
        if len(request.chunk_hashes) != expected_count:
            raise InvalidUploadError("chunk hash count does not match file size")

        chunks = []
        for chunk_num, chunk_hash in enumerate(request.chunk_hashes):
            self._validate_hash(chunk_hash, f"chunk {chunk_num} hash")
            size = min(
                request.chunk_size,
                request.file_size - chunk_num * request.chunk_size,
            )
            chunks.append(ChunkRecord(chunk_num, size, chunk_hash))
        return tuple(chunks)

    def _session_dir(self, path: str, target_hash: str) -> Path:
        return self.uploads_dir / hashlib.sha256(path.encode()).hexdigest() / target_hash

    def _chunk_path(self, path: str, target_hash: str, chunk_num: int) -> Path:
        return self._session_dir(path, target_hash) / "chunks" / str(chunk_num)

    def _scan_file(self, path: Path, chunk_size: int) -> FileSnapshot:
        whole_hash = hashlib.sha256()
        chunks: list[ChunkRecord] = []
        size = 0

        with path.open("rb") as source:
            chunk_num = 0
            while data := source.read(chunk_size):
                whole_hash.update(data)
                chunks.append(
                    ChunkRecord(
                        chunk_num=chunk_num,
                        size=len(data),
                        hash=hashlib.sha256(data).hexdigest(),
                    )
                )
                chunk_num += 1
                size += len(data)

        return FileSnapshot(size, whole_hash.hexdigest(), tuple(chunks))
