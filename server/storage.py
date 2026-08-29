import sqlite3
import pandas as pd
import haashlib
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
        #build the chunk metadata table
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

        #build the file metadata table
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
    def lookup_chunk(self, path:str, chunk_num:int):
        cursor = self.conn.execute(
            """
                SELECT * FROM server_file_chunks WHERE path = ? AND chunk_num = ?
            """, (path, chunk_num,)
        )
        return cursor

    def lookup_file(self, path:str):
        cursor = self.conn.execute(
            """
                SELECT * FROM server_files WHERE path = ?
            """, (path,)
        )

    #hash file
    def hash_file(path):
        sha256 = hashlib.sha256()

        with open(path, "rb") as f:
            while chunk := f.read(1024 * 1024):
                sha256.update(chunk)

        return sha256.hexdigest()

    #get metadata from stored file
    def get_file_metadata(self, path, calculate_hash = True):
        file_path = Path(path)
        stat = file_path.stat()
        size = stat.st_size #get the file size
        mtime_ns = stat.st_mtime_ns #get the last modified time of the file
        if calculate_hash == True:
            file_hash = hashing.hash_file(path)
            return size, mtime_ns, file_hash
        return size, mtime_ns

    def update_file(self, path:str, size:int, mtime_ns:int, current_hash:str):
        cursor = self.conn.execute(
            """
                UPDATE server_files
                SET
                    size = ?,
                    mtime_ns = ?,
                    current_hash = ?,
                    last_synched_at = CURRENT_TIMESTAMP
                WHERE
                    path = ?
            """
        ), (size,mtime_ns,current_hash,path,)
        self.conn.commit()
    
    def upload_chunk(self, chunk: dict):
        self.conn.execute(
        """
            INSERT INTO server_file_chunks (
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