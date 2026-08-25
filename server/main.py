from pathlib import Path
import hashlib
import os
from fastapi import FastAPI, UploadFile, File, Form, HTTPException

app = FastAPI()

STORAGE_DIR = Path("server_storage")
STORAGE_DIR.mkdir(exist_ok=True)

class file:
    name: str
    hash: str

# ------ helper functions ------
#calculate has of one file from is contents using sha256
def get_file_hash(file_path):
    sha256 = hashlib.sha256()

    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                sha256.update(chunk)

        return sha256.hexdigest()
    except Exception as e:
        print(f"failed to calculate hash: {e}")
        return

#compare 2 file hashes
def compare_file_hash(file_hash_1, file_hash_2):
    try:
        return file_hash_1 == file_hash_2
    except Exception as e:
        print(f"failed to compare hash: {e}")

#Main API functions
@app.get("/health")
def heath():
    return {"status": "ok"}

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...), 
    file_hash: str = Form(...)
    ):
    destination = STORAGE_DIR / file.filename
    contents = await file.read()

    server_file_hash = get_file_hash(destination)

    hash_matches = compare_file_hash(server_file_hash, file_hash)

    if hash_matches:
        with open(destination, "wb") as f:
            f.write(contents)

        return {
            "status": "success",
            "filename": file.filename,
            "bytes_received": len(contents),
            "calculated_hash": server_file_hash,
            "posted_hash": file_hash,
            "hash_matches": file_hash == server_file_hash,
        }

    if not hash_matches:
        return {
            "status": "failed",
            "failure_message": "file hashes do not match",
            "calculated_hash": server_file_hash,
            "posted_hash": file_hash,
        }