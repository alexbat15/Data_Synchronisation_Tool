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


def test_matching_pending_upload_resumes_received_chunks(tmp_path):
    db_path = tmp_path / "manifest.db"
    chunks = [ChunkRecord(0, 4, "a" * 64)]
    with ServerManifestDB(db_path) as db:
        assert db.start_upload("data.bin", "b" * 64, 4, 4, chunks) is False
        db.mark_chunk_received("data.bin", 0, 4, "a" * 64)

        assert db.start_upload("data.bin", "b" * 64, 4, 4, chunks) is True
        assert db.get_pending_chunk("data.bin", 0)["received_hash"] == "a" * 64


def test_mismatched_pending_upload_replaces_received_chunks(tmp_path):
    db_path = tmp_path / "manifest.db"
    chunks = [ChunkRecord(0, 4, "a" * 64)]
    with ServerManifestDB(db_path) as db:
        db.start_upload("data.bin", "b" * 64, 4, 4, chunks)
        db.mark_chunk_received("data.bin", 0, 4, "a" * 64)

        assert db.start_upload("data.bin", "c" * 64, 4, 4, chunks) is False
        pending = db.get_pending_chunk("data.bin", 0)
        assert pending["received_size"] is None
        assert pending["received_hash"] is None


def test_clear_received_chunk_removes_received_metadata(tmp_path):
    db_path = tmp_path / "manifest.db"
    chunks = [ChunkRecord(0, 4, "a" * 64)]
    with ServerManifestDB(db_path) as db:
        db.start_upload("data.bin", "b" * 64, 4, 4, chunks)
        db.mark_chunk_received("data.bin", 0, 4, "a" * 64)
        db.clear_chunk_received("data.bin", 0)

        pending = db.get_pending_chunk("data.bin", 0)
        assert pending["received_size"] is None
        assert pending["received_hash"] is None
        assert pending["received_at"] is None


def test_deleting_pending_upload_and_file_cascades_chunks(tmp_path):
    db_path = tmp_path / "manifest.db"
    chunks = [ChunkRecord(0, 4, "a" * 64)]
    with ServerManifestDB(db_path) as db:
        db.start_upload("data.bin", "b" * 64, 4, 4, chunks)
        db.delete_pending_upload("data.bin")
        assert db.get_pending_upload("data.bin") is None
        assert db.get_pending_chunks("data.bin") == []

        db.replace_file("data.bin", 4, 123, 4, "c" * 64, chunks)
        db.delete_file("data.bin")
        assert db.get_file("data.bin") is None
        assert db.get_file_chunks("data.bin") == []


def test_replacing_file_can_clear_pending_upload(tmp_path):
    db_path = tmp_path / "manifest.db"
    chunks = [ChunkRecord(0, 4, "a" * 64)]
    with ServerManifestDB(db_path) as db:
        db.start_upload("data.bin", "b" * 64, 4, 4, chunks)
        db.replace_file("data.bin", 4, 123, 4, "c" * 64, chunks, clear_pending=True)

        assert db.get_pending_upload("data.bin") is None
        assert db.get_pending_chunks("data.bin") == []


def test_incompatible_prototype_table_is_preserved(tmp_path):
    db_path = tmp_path / "manifest.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE server_file_chunks (path TEXT PRIMARY KEY)")
    conn.execute("INSERT INTO server_file_chunks VALUES ('legacy.bin')")
    conn.execute("CREATE TABLE legacy_server_file_chunks (marker TEXT)")
    conn.execute("INSERT INTO legacy_server_file_chunks VALUES ('first')")
    conn.execute("CREATE TABLE legacy_server_file_chunks_2 (marker TEXT)")
    conn.execute("INSERT INTO legacy_server_file_chunks_2 VALUES ('second')")
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
        assert "legacy_server_file_chunks_2" in names
        assert "legacy_server_file_chunks_3" in names
        assert db.conn.execute(
            "SELECT path FROM legacy_server_file_chunks_3"
        ).fetchone()[0] == "legacy.bin"
        assert db.conn.execute("PRAGMA user_version").fetchone()[0] == 1


def test_name_compatible_table_missing_foreign_key_is_preserved(tmp_path):
    db_path = tmp_path / "manifest.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE server_file_chunks (
            path TEXT NOT NULL,
            chunk_num INTEGER NOT NULL,
            chunk_size INTEGER NOT NULL,
            current_chunk_hash TEXT NOT NULL,
            PRIMARY KEY (path, chunk_num)
        )
        """
    )
    conn.execute(
        "INSERT INTO server_file_chunks VALUES ('legacy.bin', 0, 4, 'a')"
    )
    conn.commit()
    conn.close()

    with ServerManifestDB(db_path) as db:
        assert db.conn.execute(
            "SELECT path FROM legacy_server_file_chunks"
        ).fetchone()[0] == "legacy.bin"
        foreign_keys = db.conn.execute(
            "PRAGMA foreign_key_list(server_file_chunks)"
        ).fetchall()
        assert [
            (row["table"], row["from"], row["to"], row["on_delete"])
            for row in foreign_keys
        ] == [
            ("server_files", "path", "path", "CASCADE")
        ]


def test_name_compatible_table_with_wrong_default_is_preserved(tmp_path):
    db_path = tmp_path / "manifest.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE server_files (
            path TEXT PRIMARY KEY,
            size INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            chunk_size INTEGER NOT NULL,
            current_hash TEXT NOT NULL,
            last_synched_at TEXT NOT NULL DEFAULT CURRENT_DATE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO server_files (path, size, mtime_ns, chunk_size, current_hash)
        VALUES ('legacy.bin', 4, 123, 4, 'a')
        """
    )
    conn.commit()
    conn.close()

    with ServerManifestDB(db_path) as db:
        assert db.conn.execute(
            "SELECT current_hash FROM legacy_server_files"
        ).fetchone()[0] == "a"
        table_info = db.conn.execute("PRAGMA table_info(server_files)").fetchall()
        assert table_info[-1]["dflt_value"] == "CURRENT_TIMESTAMP"


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
