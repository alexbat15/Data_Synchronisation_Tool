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
        size = stat.st_size
        mtime_ns = stat.st_mtime_ns
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
        file_changed = False
        current_size, current_mtime_ns= self.get_file_metadata(path, calculate_hash=False)
        synched_file = self.lookup_file(path)

        #compare filemtime and size with previously seen data. 
        if current_size != synched_file["size"]:
            file_changed = True
        if current_mtime_ns != synched_file["mtime_ns"]:
            file_changed = True

        #if we have seen a change in mtime and file size then we recalculate the file hash
        if file_changed == True:
            current_size, current_mtime_ns, current_file_hash = self.get_file_metadata(path)
            if current_file_hash != synched_file["synched_hash"]:
                file_changed = True
            else:
                file_changed == False
            return file_changed, current_file_hash
        return file_changed, synched_file["current_file_hash"]

class Scanner:
    def __init__(self, data_path: str="test_data/watched"):
        self.data=Path(data_path).rglob("*")
        self.manifest = ManifestDB()
        self.comparer = FileCompare()


if __name__ == "__main__":
    comparer = FileCompare()
    print(dict(comparer.lookup_file('test_data/watched/new_file.txt')))
