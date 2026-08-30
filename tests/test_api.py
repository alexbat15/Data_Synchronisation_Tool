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
