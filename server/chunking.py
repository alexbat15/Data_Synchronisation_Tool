import hashlib
import os
import re
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from server.storage import ChunkRecord, ServerManifestDB


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
