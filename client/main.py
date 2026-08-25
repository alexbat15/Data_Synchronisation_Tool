import requests
import hashlib
path = "test_data/watched/test.txt"

def hash_file(path):
    sha256 = hashlib.sha256()

    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha256.update(chunk)

    return sha256.hexdigest

with open(path, "rb") as f:
    response = requests.post(
        "http://127.0.0.1:8000/upload",
        files={"file": f},
        data={"file_hash": hash_file(path)},

    )

print(response.status_code)
print(response.json())