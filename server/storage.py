import sqlite3
import pandas as pd
from pathlib import Path


class ServerManifestDB:
    def __init__(self, db_path: str = "server_storage/server_state/server_manifest.db"):
        self.db_path = Path(db_path)

        # Create the state directory if it doesn't exist
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Connect to SQLite.
        # If the database file doesn't exist, SQLite creates it.
        self.conn = sqlite3.connect(self.db_path)

        # Makes query results easier to work with
        self.conn.row_factory = sqlite3.Row

        # Useful SQLite setting, particularly once we add related tables
        self.conn.execute("PRAGMA foreign_keys = ON")

        self._create_tables()

    #Create two tables, one for cchunks and one for files
    def _create_tables(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS server_file_chunks (
                path TEXT PRIMARY KEY,
                chunk_num INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                chunk_mtime_ns INTEGER NOT NULL,
                current_chunk_hash TEXT
            )
            """
        )
        self.conn.commit()
        self.conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS server_files (
                        path TEXT NOT NULL,
                        size INTEGER NOT NULL,
                        mtime_ns INTEGER NOT NULL, 
                        current_hash TEXT,
                        last_synched_at INTEGER NOT NULL
                        
                        PRIMARY KEY (path, chunk_num),

                        FOREIGN KEY (path)
                            REFERENCES files(path)
                            ON DELETE CASCADE
                    )
                    """
                )
        self.conn.commit()

    #chunk = {"path":str, "chunk_num": int, "size": int, "mtime_ns": int, "chunk_mtime_ns": int, "current_hash": str, "current_chunk_hash": str, "last_synched_at": str}
    def lookup_chunk(self, chunk: dict):
        cursor = self.conn.execute(
            """
                SELECT * FROM file_chunks WHERE path = ? AND chunk_num = ?
            """, (chunk["path"], chunk["chunk_num"],)
        )
        return cursor

    def lookup_file(self, path):
        cursor = self.conn.execute(
            """
                SELECT
            """
        )
    
    def upload_chunk(self, chunk: dict):
        self.conn.execute(
        """
            INSERT INTO file_chunks (
                path,
                chunk_num,
                size,
                chunk_size,
                mtime_ns,
                chunk_mtime_ns,

        """
        )

    def inspect_db(self, print_rows=True):
        cursor = self.conn.execute("SELECT * FROM file_chunks")
        rows = cursor.fetchall()

        column_names = [description[0] for description in cursor.description]

        rows = pd.DataFrame(rows, columns=column_names)

        if print_rows:
            print(rows.head())

        return rows

    def clear_table(self):
        self.conn.execute("DELETE FROM file_chunks")
        self.conn.commit()

    def rename_col(self, old_name, new_name):
        self.conn.execute(
            f"""
                ALTER TABLE file_chunks 
                RENAME COLUMN {old_name} TO {new_name}
            """
        )
        self.conn.commit()

    def close(self):
        self.conn.close()

if __name__ == "__main__":
    db = ServerManifestDB()
    # db._create_tables()

    db.inspect_db()

    db.close()