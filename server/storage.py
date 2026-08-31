import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


_SERVER_FILES_COLUMNS = (
    ("path", "TEXT", 0, None, 1),
    ("size", "INTEGER", 1, None, 0),
    ("mtime_ns", "INTEGER", 1, None, 0),
    ("chunk_size", "INTEGER", 1, None, 0),
    ("current_hash", "TEXT", 1, None, 0),
    ("last_synched_at", "TEXT", 1, "CURRENT_TIMESTAMP", 0),
)
_SERVER_FILE_CHUNKS_COLUMNS = (
    ("path", "TEXT", 1, None, 1),
    ("chunk_num", "INTEGER", 1, None, 2),
    ("chunk_size", "INTEGER", 1, None, 0),
    ("current_chunk_hash", "TEXT", 1, None, 0),
)
_SERVER_FILE_CHUNKS_FOREIGN_KEYS = (
    ("server_files", "path", "path", "NO ACTION", "CASCADE", "NONE"),
)


@dataclass(frozen=True)
class ChunkRecord:
    chunk_num: int
    size: int
    hash: str


class ServerManifestDB:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._create_tables()

    def __enter__(self) -> "ServerManifestDB":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.conn.close()

    def _create_tables(self) -> None:
        with self.conn:
            self._replace_incompatible_table(
                "server_files",
                _SERVER_FILES_COLUMNS,
            )
            self._replace_incompatible_table(
                "server_file_chunks",
                _SERVER_FILE_CHUNKS_COLUMNS,
                _SERVER_FILE_CHUNKS_FOREIGN_KEYS,
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS server_files (
                    path TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    chunk_size INTEGER NOT NULL,
                    current_hash TEXT NOT NULL,
                    last_synched_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS server_file_chunks (
                    path TEXT NOT NULL,
                    chunk_num INTEGER NOT NULL,
                    chunk_size INTEGER NOT NULL,
                    current_chunk_hash TEXT NOT NULL,
                    PRIMARY KEY (path, chunk_num),
                    FOREIGN KEY (path) REFERENCES server_files(path) ON DELETE CASCADE
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_uploads (
                    path TEXT PRIMARY KEY,
                    target_hash TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    chunk_size INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_upload_chunks (
                    path TEXT NOT NULL,
                    chunk_num INTEGER NOT NULL,
                    expected_size INTEGER NOT NULL,
                    expected_hash TEXT NOT NULL,
                    received_size INTEGER,
                    received_hash TEXT,
                    received_at TEXT,
                    PRIMARY KEY (path, chunk_num),
                    FOREIGN KEY (path) REFERENCES pending_uploads(path) ON DELETE CASCADE
                )
                """
            )
            self.conn.execute("PRAGMA user_version = 1")

    def _replace_incompatible_table(
        self,
        table_name: str,
        expected_columns: tuple[tuple[str, str, int, str | None, int], ...],
        expected_foreign_keys: tuple[
            tuple[str, str, str, str, str, str], ...
        ] = (),
    ) -> None:
        columns = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        column_signature = tuple(
            (
                column["name"],
                column["type"],
                column["notnull"],
                column["dflt_value"],
                column["pk"],
            )
            for column in columns
        )
        foreign_key_signature = tuple(
            (
                foreign_key["table"],
                foreign_key["from"],
                foreign_key["to"],
                foreign_key["on_update"],
                foreign_key["on_delete"],
                foreign_key["match"],
            )
            for foreign_key in self.conn.execute(
                f"PRAGMA foreign_key_list({table_name})"
            ).fetchall()
        )
        if not columns or (
            column_signature == expected_columns
            and foreign_key_signature == expected_foreign_keys
        ):
            return

        suffix = ""
        index = 2
        while self._table_exists(f"legacy_{table_name}{suffix}"):
            suffix = f"_{index}"
            index += 1
        self.conn.execute(
            f"ALTER TABLE {table_name} RENAME TO legacy_{table_name}{suffix}"
        )

    def _table_exists(self, table_name: str) -> bool:
        return (
            self.conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (table_name,),
            ).fetchone()
            is not None
        )

    def get_file(self, path: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM server_files WHERE path = ?", (path,)
        ).fetchone()

    def get_file_chunks(self, path: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM server_file_chunks WHERE path = ? ORDER BY chunk_num", (path,)
        ).fetchall()

    def delete_file(self, path: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM server_files WHERE path = ?", (path,))

    def replace_file(
        self,
        path: str,
        size: int,
        mtime_ns: int,
        chunk_size: int,
        current_hash: str,
        chunks: Sequence[ChunkRecord],
        *,
        clear_pending: bool = False,
        expected_pending_target_hash: str | None = None,
    ) -> bool:
        with self.conn:
            if expected_pending_target_hash is not None:
                self.conn.execute("BEGIN IMMEDIATE")
                pending_upload = self.get_pending_upload(path)
                pending_chunks = [
                    (row["chunk_num"], row["expected_size"], row["expected_hash"])
                    for row in self.get_pending_chunks(path)
                ]
                expected_chunks = [
                    (chunk.chunk_num, chunk.size, chunk.hash) for chunk in chunks
                ]
                if (
                    pending_upload is None
                    or (
                        pending_upload["target_hash"],
                        pending_upload["size"],
                        pending_upload["chunk_size"],
                    )
                    != (expected_pending_target_hash, size, chunk_size)
                    or pending_chunks != expected_chunks
                ):
                    return False

            self.conn.execute(
                """
                INSERT INTO server_files (path, size, mtime_ns, chunk_size, current_hash)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    size = excluded.size,
                    mtime_ns = excluded.mtime_ns,
                    chunk_size = excluded.chunk_size,
                    current_hash = excluded.current_hash,
                    last_synched_at = CURRENT_TIMESTAMP
                """,
                (path, size, mtime_ns, chunk_size, current_hash),
            )
            self.conn.execute("DELETE FROM server_file_chunks WHERE path = ?", (path,))
            self.conn.executemany(
                """
                INSERT INTO server_file_chunks
                    (path, chunk_num, chunk_size, current_chunk_hash)
                VALUES (?, ?, ?, ?)
                """,
                ((path, chunk.chunk_num, chunk.size, chunk.hash) for chunk in chunks),
            )
            if clear_pending:
                if expected_pending_target_hash is None:
                    self.conn.execute(
                        "DELETE FROM pending_uploads WHERE path = ?", (path,)
                    )
                else:
                    self.conn.execute(
                        "DELETE FROM pending_uploads WHERE path = ? AND target_hash = ?",
                        (path, expected_pending_target_hash),
                    )
        return True

    def get_pending_upload(self, path: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM pending_uploads WHERE path = ?", (path,)
        ).fetchone()

    def get_pending_chunk(self, path: str, chunk_num: int) -> sqlite3.Row | None:
        return self.conn.execute(
            """
            SELECT * FROM pending_upload_chunks
            WHERE path = ? AND chunk_num = ?
            """,
            (path, chunk_num),
        ).fetchone()

    def get_pending_chunks(self, path: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            """
            SELECT * FROM pending_upload_chunks
            WHERE path = ? ORDER BY chunk_num
            """,
            (path,),
        ).fetchall()

    def start_upload(
        self,
        path: str,
        target_hash: str,
        size: int,
        chunk_size: int,
        chunks: Sequence[ChunkRecord],
    ) -> bool:
        pending_upload = self.get_pending_upload(path)
        expected_chunks = [
            (chunk.chunk_num, chunk.size, chunk.hash) for chunk in chunks
        ]
        pending_chunks = [
            (row["chunk_num"], row["expected_size"], row["expected_hash"])
            for row in self.get_pending_chunks(path)
        ]
        if (
            pending_upload is not None
            and (pending_upload["target_hash"], pending_upload["size"], pending_upload["chunk_size"])
            == (target_hash, size, chunk_size)
            and pending_chunks == expected_chunks
        ):
            return True

        with self.conn:
            self.conn.execute("DELETE FROM pending_uploads WHERE path = ?", (path,))
            self.conn.execute(
                """
                INSERT INTO pending_uploads (path, target_hash, size, chunk_size)
                VALUES (?, ?, ?, ?)
                """,
                (path, target_hash, size, chunk_size),
            )
            self.conn.executemany(
                """
                INSERT INTO pending_upload_chunks
                    (path, chunk_num, expected_size, expected_hash)
                VALUES (?, ?, ?, ?)
                """,
                ((path, chunk.chunk_num, chunk.size, chunk.hash) for chunk in chunks),
            )
        return False

    def mark_chunk_received(
        self,
        path: str,
        target_hash: str,
        chunk_num: int,
        expected_size: int,
        expected_hash: str,
        received_size: int,
        received_hash: str,
    ) -> bool:
        with self.conn:
            updated_chunk = self.conn.execute(
                """
                UPDATE pending_upload_chunks
                SET received_size = ?, received_hash = ?, received_at = CURRENT_TIMESTAMP
                WHERE path = ? AND chunk_num = ?
                    AND expected_size = ? AND expected_hash = ?
                    AND EXISTS (
                        SELECT 1 FROM pending_uploads
                        WHERE path = ? AND target_hash = ?
                    )
                """,
                (
                    received_size,
                    received_hash,
                    path,
                    chunk_num,
                    expected_size,
                    expected_hash,
                    path,
                    target_hash,
                ),
            )
            if updated_chunk.rowcount != 1:
                return False
            self.conn.execute(
                """
                UPDATE pending_uploads
                SET updated_at = CURRENT_TIMESTAMP
                WHERE path = ? AND target_hash = ?
                """,
                (path, target_hash),
            )
        return True

    def clear_chunk_received(self, path: str, chunk_num: int) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE pending_upload_chunks
                SET received_size = NULL, received_hash = NULL, received_at = NULL
                WHERE path = ? AND chunk_num = ?
                """,
                (path, chunk_num),
            )
            self.conn.execute(
                "UPDATE pending_uploads SET updated_at = CURRENT_TIMESTAMP WHERE path = ?",
                (path,),
            )

    def delete_pending_upload(self, path: str) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM pending_uploads WHERE path = ?", (path,))
