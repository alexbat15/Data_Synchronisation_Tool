import sqlite3

from server.storage import ChunkRecord, ServerManifestDB
from shared.models import FileCompleteRequest, FileInitRequest


def test_manifest_persists_committed_and_pending_chunks(tmp_path):
    db_path = tmp_path / "manifest.db"
    chunks = [
        ChunkRecord(chunk_num=0, size=4, hash="a" * 64),
        ChunkRecord(chunk_num=1, size=2, hash="b" * 64),
    ]

    with ServerManifestDB(db_path) as db:
        db.replace_file("folder/data.bin", 6, 123, 4, "c" * 64, chunks)
        resumed = db.start_upload(
            "folder/data.bin", "d" * 64, 6, 4, chunks
        )
        db.mark_chunk_received("folder/data.bin", 1, 2, "b" * 64)

    assert resumed is False
    with ServerManifestDB(db_path) as db:
        assert db.get_file("folder/data.bin")["current_hash"] == "c" * 64
        assert [row["chunk_num"] for row in db.get_file_chunks("folder/data.bin")] == [0, 1]
        pending = db.get_pending_chunks("folder/data.bin")
        assert pending[0]["received_hash"] is None
        assert pending[1]["received_hash"] == "b" * 64


def test_replacing_file_removes_obsolete_chunk_rows(tmp_path):
    db_path = tmp_path / "manifest.db"
    with ServerManifestDB(db_path) as db:
        db.replace_file(
            "data.bin",
            6,
            123,
            4,
            "c" * 64,
            [ChunkRecord(0, 4, "a" * 64), ChunkRecord(1, 2, "b" * 64)],
        )
        db.replace_file(
            "data.bin", 3, 456, 4, "e" * 64, [ChunkRecord(0, 3, "f" * 64)]
        )
        assert [row["chunk_num"] for row in db.get_file_chunks("data.bin")] == [0]


def test_incompatible_prototype_table_is_preserved(tmp_path):
    db_path = tmp_path / "manifest.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE server_file_chunks (path TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO server_file_chunks VALUES ('legacy.bin')")
    conn.commit()
    conn.close()

    with ServerManifestDB(db_path) as db:
        names = {
            row[0]
            for row in db.conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "server_file_chunks" in names
        assert "legacy_server_file_chunks" in names
        assert db.conn.execute("PRAGMA user_version").fetchone()[0] == 1


def test_request_models_keep_client_contract():
    request = FileInitRequest(
        rel_file_path="folder/data.bin",
        file_hash="a" * 64,
        file_size=4,
        chunk_size=4,
        chunk_hashes=["b" * 64],
    )
    complete = FileCompleteRequest(
        rel_file_path=request.rel_file_path,
        file_hash=request.file_hash,
    )
    assert complete.rel_file_path == "folder/data.bin"
