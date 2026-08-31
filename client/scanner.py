from client.manifest import ManifestDB
import client.hashing as hashing
import pandas as pd
from pathlib import Path


class FileCompare:
    def __init__(self):
        self.manifest = ManifestDB()

    def get_file_metadata(self, path, calculate_hash = True):
        file_path = Path(path)
        stat = file_path.stat()
        size = stat.st_size #get the file size
        mtime_ns = stat.st_mtime_ns #get the last modified time of the file
        if calculate_hash == True:
            file_hash = hashing.hash_file(path)
            return size, mtime_ns, file_hash
        return size, mtime_ns

    def lookup_file(self, path):
        file_metadata = self.manifest.get_file(path)

        if file_metadata == None:
            size, mtime_ns, file_hash = self.get_file_metadata(path)
            self.manifest.add_file(
                path=path,
                size=size,
                mtime_ns=mtime_ns,
                current_hash=file_hash
            )
            file_metadata = self.manifest.get_file(path)

        return file_metadata

    def compare_current_synched(self, path):
        current_size, current_mtime_ns = self.get_file_metadata(
            path,
            calculate_hash=False
        )

        stored_file = self.manifest.get_file(path)

        # Brand-new file
        if stored_file is None:
            current_hash = hashing.hash_file(path)

            self.manifest.add_file(
                path=path,
                size=current_size,
                mtime_ns=current_mtime_ns,
                current_hash=current_hash
            )

            return True, current_hash

        # Previously seen file
        metadata_changed = (
            current_size != stored_file["size"]
            or current_mtime_ns != stored_file["mtime_ns"]
        )

        if metadata_changed:
            current_hash = hashing.hash_file(path)

            file_changed = current_hash != stored_file["synched_hash"]

            return file_changed, current_hash

        return (
            stored_file["current_hash"] != stored_file["synched_hash"],
            stored_file["current_hash"],
        )

class Scanner:
    def __init__(self, data_path: str = "test_data/watched"):
        self.data_path = Path(data_path)
        self.comparer = FileCompare()

    def get_files(self):
        return [
            {"full_path":str(path), "relative_path":str(path.relative_to(self.data_path))}
            for path in self.data_path.rglob("*")
            if path.is_file()
        ]

    def get_file_status(self):
        files = self.get_files()

        results = []

        for file in files:
            changed, current_hash = self.comparer.compare_current_synched(
                file["full_path"]
            )

            results.append({
                "full_path": file["full_path"],
                "relative_path": file["relative_path"],
                "changed": changed,
                "current_hash": current_hash
            })

        return results


if __name__ == "__main__":
    comparer = FileCompare()
    scanner = Scanner()
    print(dict(comparer.lookup_file('test_data/watched/new_file.txt')))
    print(scanner.get_files())
    print(scanner.get_file_status())
