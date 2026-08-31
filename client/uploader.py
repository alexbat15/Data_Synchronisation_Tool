import hashlib
from pathlib import Path, PurePosixPath

import requests

from client.manifest import ManifestDB
from client.scanner import Scanner


DEFAULT_CHUNK_SIZE = 1024 * 1024
DEFAULT_SERVER_URL = "http://127.0.0.1:8000"


class UploadProtocolError(Exception):
    """Raised when a successful HTTP response does not acknowledge an upload."""


class ChunkedUploader:
    def __init__(
        self,
        scanner=None,
        manifest=None,
        session=None,
        server_url: str = DEFAULT_SERVER_URL,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")

        self.scanner = scanner or Scanner()
        self.manifest = manifest or self.scanner.comparer.manifest
        self.session = session or requests.Session()
        self.server_url = server_url.rstrip("/")
        self.chunk_size = chunk_size

    def sync_once(self) -> list[dict[str, str]]:
        results = []
        for file_info in self.scanner.get_file_status():
            if not file_info["changed"]:
                continue

            try:
                self._sync_file(file_info)
            except (OSError, UploadProtocolError, requests.RequestException):
                results.append({"path": file_info["relative_path"], "status": "failed"})
                continue

            self.manifest.mark_as_synched(
                file_info["full_path"], file_info["current_hash"]
            )
            results.append({"path": file_info["relative_path"], "status": "synced"})

        return results

    def _sync_file(self, file_info: dict) -> None:
        file_path = Path(file_info["full_path"])
        relative_path = PurePosixPath(
            file_info["relative_path"].replace("\\", "/")
        ).as_posix()
        file_size, file_hash, chunk_hashes = self._hash_chunks(file_path)
        if file_hash != file_info["current_hash"]:
            raise UploadProtocolError("file changed since the scan")

        init_response = self._post_json(
            "/files/init",
            {
                "rel_file_path": relative_path,
                "file_hash": file_hash,
                "file_size": file_size,
                "chunk_size": self.chunk_size,
                "chunk_hashes": chunk_hashes,
            },
        )
        self._require_matching_file_hash(init_response, file_hash)
        if init_response.get("up_to_date"):
            return

        for chunk_num in init_response.get("missing_chunks", []):
            self._upload_chunk(
                file_path,
                relative_path,
                file_hash,
                chunk_hashes,
                chunk_num,
            )

        complete_response = self._post_json(
            "/files/complete",
            {
                "rel_file_path": relative_path,
                "file_hash": file_hash,
            },
        )
        self._require_matching_file_hash(complete_response, file_hash)
        if complete_response.get("rel_file_path") != relative_path:
            raise UploadProtocolError("server acknowledged a different file path")

    def _upload_chunk(
        self,
        file_path: Path,
        relative_path: str,
        file_hash: str,
        chunk_hashes: list[str],
        chunk_num: int,
    ) -> None:
        if not isinstance(chunk_num, int) or not 0 <= chunk_num < len(chunk_hashes):
            raise UploadProtocolError("server requested an invalid chunk number")

        with file_path.open("rb") as source:
            source.seek(chunk_num * self.chunk_size)
            chunk = source.read(self.chunk_size)

        if hashlib.sha256(chunk).hexdigest() != chunk_hashes[chunk_num]:
            raise UploadProtocolError("file changed while uploading")

        response = self.session.post(
            f"{self.server_url}/files/chunks",
            data={
                "rel_file_path": relative_path,
                "file_hash": file_hash,
                "chunk_num": str(chunk_num),
                "chunk_hash": chunk_hashes[chunk_num],
            },
            files={
                "chunk": (
                    f"{chunk_num}.chunk",
                    chunk,
                    "application/octet-stream",
                )
            },
        )
        self._success_payload(response)

    def _post_json(self, path: str, payload: dict) -> dict:
        response = self.session.post(f"{self.server_url}{path}", json=payload)
        return self._success_payload(response)

    def _success_payload(self, response) -> dict:
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success":
            raise UploadProtocolError("server did not acknowledge the upload")
        return payload

    def _require_matching_file_hash(self, payload: dict, file_hash: str) -> None:
        if payload.get("file_hash") != file_hash:
            raise UploadProtocolError("server acknowledged a different file hash")

    def _hash_chunks(self, file_path: Path) -> tuple[int, str, list[str]]:
        whole_hash = hashlib.sha256()
        chunk_hashes = []
        file_size = 0

        with file_path.open("rb") as source:
            while chunk := source.read(self.chunk_size):
                whole_hash.update(chunk)
                chunk_hashes.append(hashlib.sha256(chunk).hexdigest())
                file_size += len(chunk)

        return file_size, whole_hash.hexdigest(), chunk_hashes
