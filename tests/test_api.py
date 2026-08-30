import hashlib

from fastapi.testclient import TestClient

from server.main import create_app
from server.storage import ChunkRecord, ServerManifestDB


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


def test_legacy_upload_clears_pending_chunk_upload(tmp_path):
    pending_content = b"older upload"
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        db.start_upload(
            "nested.bin",
            sha256(pending_content),
            len(pending_content),
            len(pending_content),
            [ChunkRecord(0, len(pending_content), sha256(pending_content))],
        )

    content = b"legacy upload"
    response = TestClient(create_app(tmp_path)).post(
        "/upload",
        files={"file": ("nested.bin", content, "application/octet-stream")},
        data={"file_hash": sha256(content)},
    )

    assert response.status_code == 200
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        assert db.get_pending_upload("nested.bin") is None
        assert db.get_pending_chunks("nested.bin") == []
