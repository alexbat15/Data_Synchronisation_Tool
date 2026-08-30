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
from shared.models import FileCompleteRequest, FileInitRequest


DEFAULT_CHUNK_SIZE = 1024 * 1024
HASH_BUFFER_SIZE = 64 * 1024
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
                    staged_size, staged_hash = self._hash_file(chunk_path)
                    if (staged_size, staged_hash) == (
                        expected.size,
                        expected.hash,
                    ):
                        if db.mark_chunk_received(
                            path,
                            request.file_hash,
                            expected.chunk_num,
                            expected.size,
                            expected.hash,
                            expected.size,
                            expected.hash,
                        ):
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

    def receive_chunk(
        self,
        rel_file_path: str,
        file_hash: str,
        chunk_num: int,
        chunk_hash: str,
        stream: BinaryIO,
    ) -> dict:
        path = normalize_relative_path(rel_file_path)
        self._validate_hash(file_hash, "file hash")
        self._validate_hash(chunk_hash, "chunk hash")
        if (
            not isinstance(chunk_num, int)
            or isinstance(chunk_num, bool)
            or chunk_num < 0
        ):
            raise InvalidUploadError("chunk number must be a non-negative integer")

        with ServerManifestDB(self.db_path) as db:
            pending_upload = db.get_pending_upload(path)
            if pending_upload is None:
                raise UploadNotFoundError("no pending upload exists for file")
            if pending_upload["target_hash"] != file_hash:
                raise UploadConflictError("file hash does not match pending upload")

            pending_chunk = db.get_pending_chunk(path, chunk_num)
            if pending_chunk is None:
                raise InvalidUploadError(
                    "chunk number is not expected for pending upload"
                )
            if pending_chunk["expected_hash"] != chunk_hash:
                raise InvalidUploadError("chunk hash does not match pending upload")

            chunk_path = self._chunk_path(path, file_hash, chunk_num)
            if (
                (pending_chunk["received_size"], pending_chunk["received_hash"])
                == (pending_chunk["expected_size"], pending_chunk["expected_hash"])
                and chunk_path.is_file()
            ):
                return {
                    "status": "success",
                    "chunk_num": chunk_num,
                    "already_received": True,
                }

            temporary_path = chunk_path.with_name(
                f"{chunk_path.name}.part.{uuid.uuid4().hex}"
            )
            received_hash = hashlib.sha256()
            received_size = 0

            try:
                temporary_path.parent.mkdir(parents=True, exist_ok=True)
                with temporary_path.open("xb") as temporary_file:
                    while data := stream.read(HASH_BUFFER_SIZE):
                        temporary_file.write(data)
                        received_hash.update(data)
                        received_size += len(data)
                    temporary_file.flush()
                    os.fsync(temporary_file.fileno())

                calculated_hash = received_hash.hexdigest()
                if (received_size, calculated_hash) != (
                    pending_chunk["expected_size"],
                    pending_chunk["expected_hash"],
                ):
                    raise InvalidUploadError("chunk content does not match pending upload")

                os.replace(temporary_path, chunk_path)
                temporary_path = None
                if not db.mark_chunk_received(
                    path,
                    file_hash,
                    chunk_num,
                    pending_chunk["expected_size"],
                    pending_chunk["expected_hash"],
                    received_size,
                    calculated_hash,
                ):
                    raise UploadConflictError("pending upload changed while receiving chunk")
            finally:
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)

        return {
            "status": "success",
            "chunk_num": chunk_num,
            "already_received": False,
        }

    def complete(self, request: FileCompleteRequest) -> dict:
        path = normalize_relative_path(request.rel_file_path)
        self._validate_hash(request.file_hash, "file hash")

        with ServerManifestDB(self.db_path) as db:
            pending_upload = db.get_pending_upload(path)
            committed_file = db.get_file(path)

        if pending_upload is None:
            if committed_file is None:
                raise UploadNotFoundError("no pending upload exists for file")
            reconciled = self.reconcile_manifest(path, committed_file["chunk_size"])
            if reconciled is None or reconciled["current_hash"] != request.file_hash:
                raise UploadNotFoundError("no matching completed file exists")
            return self._completion_response(path, request.file_hash, reconciled["size"])

        if pending_upload["target_hash"] != request.file_hash:
            raise UploadConflictError("file hash does not match pending upload")

        self.reconcile_manifest(path, pending_upload["chunk_size"])
        missing_chunks = self._missing_chunks(path, request.file_hash)
        if missing_chunks:
            raise MissingChunksError(missing_chunks)

        pending_upload, expected_chunks, sources = self._completion_sources(
            path, request.file_hash
        )
        newly_missing = [
            chunk.chunk_num
            for chunk, source in zip(expected_chunks, sources)
            if source is None
        ]
        if newly_missing:
            raise MissingChunksError(newly_missing)

        session_dir = self._session_dir(path, request.file_hash)
        assembled_path = session_dir / f"assembled.part.{uuid.uuid4().hex}"
        destination = self.storage_dir / path
        assembled_size = 0
        assembled_hash = hashlib.sha256()

        try:
            session_dir.mkdir(parents=True, exist_ok=True)
            with assembled_path.open("xb") as assembled_file:
                committed_source = None
                try:
                    if any(source and source[0] == "committed" for source in sources):
                        committed_source = destination.open("rb")

                    for expected, source in zip(expected_chunks, sources):
                        if source[0] == "committed":
                            committed_source.seek(
                                expected.chunk_num * pending_upload["chunk_size"]
                            )
                            source_file = committed_source
                            assembled_size += self._copy_source(
                                source_file,
                                assembled_file,
                                expected.size,
                                assembled_hash,
                            )
                        else:
                            with source[1].open("rb") as source_file:
                                assembled_size += self._copy_source(
                                    source_file,
                                    assembled_file,
                                    expected.size,
                                    assembled_hash,
                                )
                finally:
                    if committed_source is not None:
                        committed_source.close()

                if (
                    assembled_size != pending_upload["size"]
                    or assembled_hash.hexdigest() != request.file_hash
                ):
                    raise InvalidUploadError(
                        "assembled file does not match pending upload"
                    )
                assembled_file.flush()
                os.fsync(assembled_file.fileno())

            destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(assembled_path, destination)
            assembled_path = None

            stat = destination.stat()
            with ServerManifestDB(self.db_path) as db:
                committed = db.replace_file(
                    path,
                    assembled_size,
                    stat.st_mtime_ns,
                    pending_upload["chunk_size"],
                    request.file_hash,
                    expected_chunks,
                    clear_pending=True,
                    expected_pending_target_hash=request.file_hash,
                )
            if not committed:
                raise UploadConflictError(
                    "pending upload changed while completing file"
                )

            shutil.rmtree(session_dir, ignore_errors=True)
            return self._completion_response(path, request.file_hash, assembled_size)
        finally:
            if assembled_path is not None:
                assembled_path.unlink(missing_ok=True)

    def _missing_chunks(self, path: str, target_hash: str) -> list[int]:
        _, expected_chunks, sources = self._completion_sources(path, target_hash)
        return [
            chunk.chunk_num
            for chunk, source in zip(expected_chunks, sources)
            if source is None
        ]

    def _completion_sources(
        self, path: str, target_hash: str
    ) -> tuple[sqlite3.Row, tuple[ChunkRecord, ...], list[tuple[str, Path | None] | None]]:
        with ServerManifestDB(self.db_path) as db:
            pending_upload = db.get_pending_upload(path)
            if pending_upload is None:
                raise UploadNotFoundError("no pending upload exists for file")
            if pending_upload["target_hash"] != target_hash:
                raise UploadConflictError("file hash does not match pending upload")

            pending_chunks = db.get_pending_chunks(path)
            committed_file = db.get_file(path)
            committed_chunks = {
                row["chunk_num"]: row for row in db.get_file_chunks(path)
            }

        expected_chunks = tuple(
            ChunkRecord(row["chunk_num"], row["expected_size"], row["expected_hash"])
            for row in pending_chunks
        )
        sources = []
        for expected, pending_chunk in zip(expected_chunks, pending_chunks):
            staged_path = self._chunk_path(path, target_hash, expected.chunk_num)
            if (
                pending_chunk["received_size"],
                pending_chunk["received_hash"],
            ) == (expected.size, expected.hash) and staged_path.is_file():
                sources.append(("staged", staged_path))
                continue

            committed_chunk = committed_chunks.get(expected.chunk_num)
            if (
                committed_file is not None
                and committed_chunk is not None
                and (
                    committed_chunk["chunk_num"],
                    committed_chunk["chunk_size"],
                    committed_chunk["current_chunk_hash"],
                )
                == (expected.chunk_num, expected.size, expected.hash)
            ):
                sources.append(("committed", None))
            else:
                sources.append(None)

        return pending_upload, expected_chunks, sources

    def _copy_source(
        self,
        source: BinaryIO,
        destination: BinaryIO,
        expected_size: int,
        whole_hash,
    ) -> int:
        remaining = expected_size
        copied = 0
        while remaining:
            data = source.read(min(HASH_BUFFER_SIZE, remaining))
            if not data:
                break
            destination.write(data)
            whole_hash.update(data)
            copied += len(data)
            remaining -= len(data)
        return copied

    def _completion_response(self, path: str, file_hash: str, size: int) -> dict:
        return {
            "status": "success",
            "rel_file_path": path,
            "file_hash": file_hash,
            "bytes_written": size,
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

    def _hash_file(self, path: Path) -> tuple[int, str]:
        whole_hash = hashlib.sha256()
        size = 0

        with path.open("rb") as source:
            while data := source.read(HASH_BUFFER_SIZE):
                whole_hash.update(data)
                size += len(data)
        return size, whole_hash.hexdigest()

    def _scan_file(self, path: Path, chunk_size: int) -> FileSnapshot:
        whole_hash = hashlib.sha256()
        chunks: list[ChunkRecord] = []
        size = 0

        with path.open("rb") as source:
            chunk_num = 0
            current_chunk_hash = hashlib.sha256()
            current_chunk_size = 0
            while data := source.read(HASH_BUFFER_SIZE):
                whole_hash.update(data)
                size += len(data)
                view = memoryview(data)
                offset = 0
                while offset < len(view):
                    take = min(chunk_size - current_chunk_size, len(view) - offset)
                    current_chunk_hash.update(view[offset : offset + take])
                    current_chunk_size += take
                    offset += take
                    if current_chunk_size == chunk_size:
                        chunks.append(
                            ChunkRecord(
                                chunk_num=chunk_num,
                                size=current_chunk_size,
                                hash=current_chunk_hash.hexdigest(),
                            )
                        )
                        chunk_num += 1
                        current_chunk_hash = hashlib.sha256()
                        current_chunk_size = 0
            if current_chunk_size:
                chunks.append(
                    ChunkRecord(
                        chunk_num=chunk_num,
                        size=current_chunk_size,
                        hash=current_chunk_hash.hexdigest(),
                    )
                )

        return FileSnapshot(size, whole_hash.hexdigest(), tuple(chunks))
