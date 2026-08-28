import sqlite3
import pandas as pd
from pathlib import Path


class ManifestDB:
    def __init__(self, db_path: str = "state/manifest.db"):
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

    #Create the manifet database
    def _create_tables(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                mtime_ns INTEGER NOT NULL,
                current_hash TEXT,
                synced_hash TEXT,
                last_seen_at INTEGER,
                last_synced_at TEXT
            )
            """
        )

        self.conn.commit()

    #retrive a file's metadata from the data base
    def get_file(self, path: str):
        cursor = self.conn.execute(
            """
            SELECT * FROM files WHERE path = ?
            """, (path,)
        )
        return cursor.fetchone()

    #Add a file's metadata to the database
    def add_file(self,
                 path: str,
                 size: int,
                 mtime_ns: int,
                 current_hash: str,
                 ):
        self.conn.execute(
            """INSERT INTO files (
                path,
                size,
                mtime_ns,
                current_hash
            )
            VALUES (?,?,?,?)
            """,
            (
                path,
                size,
                mtime_ns,
                current_hash,
            )
        )
        self.conn.commit()

    #Mark the most recent synch of a file in the database
    def mark_as_synched(self,
                        path: str,
                        file_hash: str
                        ):
        self.conn.executr(
            """
            UPDATE files
            SET
                synched_hash = ?
                last_synched_at = CURRENT_TIMESTAMP
            WHERE path = ?
            """,
            (
                file_hash,
                path,
            )
        )
        self.conn.commit()

    def inspect_db(self, print_rows=True):
        cursor = self.conn.execute("SELECT * FROM files")
        rows = cursor.fetchall()

        column_names = [description[0] for description in cursor.description]

        rows = pd.DataFrame(rows, columns=column_names)

        if print_rows:
            print(rows.head())

        return rows

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    db = ManifestDB()
    # db.add_file(
    #     path='test1.txt',
    #     size=500,
    #     mtime_ns=123456,
    #     current_hash='abcd1234'
    #     )
    
    record = db.get_file("test.txt")

    if record is None:
        print("We have never seen this file before.")
    else:
        print(record["current_hash"])

    db.inspect_db()
    db.close()