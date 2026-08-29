from pathlib import Path
import hashlib
import os
import shutil
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
import server.storage as storage

app = FastAPI()

STORAGE_DIR = Path("server_storage")
STORAGE_DIR.mkdir(exist_ok=True)

TMP_DIR = Path("server_storage/tmp")
TMP_DIR.mkdir(exist_ok=True)

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

#empty a directory
def empty_directory(directory):
    directory = Path(directory)

    for item in directory.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            shutil.rmtree(item)

# ------ Main API functions ------
@app.get("/health")
def heath():
    return {"status": "ok"}

@app.post("/files/init")
async def init_file(
        rel_file_path: str = Form(...),
        file_hash: str = Form(...),
        chunk_hashes: dict = Form(...)
    ):

    db = storage.ServerManifestDB()

    destination = Path(f"{STORAGE_DIR}/{rel_file_path}")
    file_data = db.lookup_file(rel_file_path)

    chunks = {}
    chunk_num = 0
    while True:
        try:
            chunks["chunk_num"] = db.lookup_chunk(rel_file_path, chunk_num)
            chunk_num += 1
        except Exception:
            break
    all_keys = chunk_hashes.keys() | chunks.keys()

    diff_dict = {
        k: (chunk_hashes.get(k), chunks.get(k)) 
        for k in all_keys 
        if chunk_hashes.get(k) != chunks.get(k)
    }
    if file_hash == file_data["current_hash"]:
        return {
            "status":"success",
            "up_to_date":"True",
            "file_current_hash": file_data["current_hash"],
            "changed_chunks": "",
        }
    elif  len(diff_dict) == 0:
        return {
            "status":"success",
            "up_to_date":"True",
            "file_current_hash": file_data["current_hash"],
            "changed_chunks": "",
        }
    else:
        return {
            "status":"success",
            "up_to_date":"False",
            "file_current_hash": file_data["current_hash"],
            "changed_chunks":diff_dict,
        }
    

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...), 
    file_hash: str = Form(...)
    ):
    destination = STORAGE_DIR / file.filename

    #put files to a temp destination so that they can be hashed
    tmp_destination = TMP_DIR / file.filename
    contents = await file.read()
    with open(tmp_destination, "wb") as f:
                f.write(contents)
    server_file_hash = get_file_hash(tmp_destination)

    empty_directory(TMP_DIR)

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