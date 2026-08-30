import hashlib

from fastapi.testclient import TestClient

from server.main import create_app
from server.storage import ChunkRecord, ServerManifestDB


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def init_payload(path: str, content: bytes, chunk_size: int = 4) -> dict:
    chunks = [content[i : i + chunk_size] for i in range(0, len(content), chunk_size)]
    return {
        "rel_file_path": path,
        "file_hash": sha256(content),
        "file_size": len(content),
        "chunk_size": chunk_size,
        "chunk_hashes": [sha256(chunk) for chunk in chunks],
    }


def test_init_new_file_requests_every_chunk(tmp_path):
    response = TestClient(create_app(tmp_path)).post(
        "/files/init", json=init_payload("data.bin", b"abcdefghij")
    )
    assert response.status_code == 200
    assert response.json() == {
        "status": "success",
        "up_to_date": False,
        "file_hash": sha256(b"abcdefghij"),
        "missing_chunks": [0, 1, 2],
    }


def test_init_empty_file_requires_no_chunks(tmp_path):
    response = TestClient(create_app(tmp_path)).post(
        "/files/init", json=init_payload("empty.bin", b"")
    )
    assert response.status_code == 200
    assert response.json()["missing_chunks"] == []
    assert response.json()["up_to_date"] is False


def test_init_current_file_short_circuits(tmp_path):
    client = TestClient(create_app(tmp_path))
    content = b"abcdefgh"
    client.post(
        "/upload",
        files={"file": ("data.bin", content, "application/octet-stream")},
        data={"file_hash": sha256(content)},
    )
    response = client.post("/files/init", json=init_payload("data.bin", content))
    assert response.json()["up_to_date"] is True
    assert response.json()["missing_chunks"] == []


def test_init_reuses_unchanged_committed_chunk(tmp_path):
    client = TestClient(create_app(tmp_path))
    original = b"aaaabbbb"
    client.post(
        "/upload",
        files={"file": ("data.bin", original, "application/octet-stream")},
        data={"file_hash": sha256(original)},
    )
    changed = b"aaaacccc"
    first = client.post("/files/init", json=init_payload("data.bin", changed))
    assert first.json()["missing_chunks"] == [1]


def test_init_resumes_a_chunk_recorded_before_restart(tmp_path):
    target = b"abcdefgh"
    payload = init_payload("data.bin", target)
    client = TestClient(create_app(tmp_path))
    client.post("/files/init", json=payload)

    path_digest = sha256(b"data.bin")
    chunk_path = (
        tmp_path / "tmp" / "uploads" / path_digest / sha256(target) / "chunks" / "0"
    )
    chunk_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_path.write_bytes(b"abcd")
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        db.mark_chunk_received("data.bin", 0, 4, sha256(b"abcd"))

    resumed = TestClient(create_app(tmp_path)).post("/files/init", json=payload)
    assert resumed.json()["missing_chunks"] == [1]


def test_init_recovers_valid_staged_chunk_without_received_record(tmp_path):
    target = b"abcdefgh"
    payload = init_payload("data.bin", target)
    client = TestClient(create_app(tmp_path))
    client.post("/files/init", json=payload)

    chunk_path = (
        tmp_path
        / "tmp"
        / "uploads"
        / sha256(b"data.bin")
        / sha256(target)
        / "chunks"
        / "0"
    )
    chunk_path.parent.mkdir(parents=True, exist_ok=True)
    chunk_path.write_bytes(b"abcd")

    resumed = TestClient(create_app(tmp_path)).post("/files/init", json=payload)
    assert resumed.json()["missing_chunks"] == [1]
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        chunk = db.get_pending_chunk("data.bin", 0)
        assert (chunk["received_size"], chunk["received_hash"]) == (4, sha256(b"abcd"))


def test_init_new_target_replaces_old_pending_session(tmp_path):
    client = TestClient(create_app(tmp_path))
    old = b"abcdefgh"
    new = b"abcdWXYZ"
    client.post("/files/init", json=init_payload("data.bin", old))
    old_session = tmp_path / "tmp" / "uploads" / sha256(b"data.bin") / sha256(old)
    old_session.mkdir(parents=True, exist_ok=True)
    (old_session / "orphan").write_bytes(b"partial")

    response = client.post("/files/init", json=init_payload("data.bin", new))

    assert response.status_code == 200
    assert not old_session.exists()
    with ServerManifestDB(tmp_path / "server_state" / "server_manifest.db") as db:
        assert db.get_pending_upload("data.bin")["target_hash"] == sha256(new)


def test_init_rejects_inconsistent_chunk_count_and_unsafe_path(tmp_path):
    client = TestClient(create_app(tmp_path))
    payload = init_payload("data.bin", b"abcdefgh")
    payload["chunk_hashes"].pop()
    assert client.post("/files/init", json=payload).status_code == 400
    unsafe = init_payload("../escape.bin", b"abcd")
    assert client.post("/files/init", json=unsafe).status_code == 400


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
