from pathlib import Path
import hashlib
from fastapi import FastAPI, UploadFile, File, Form, HTTPException

app = FastAPI()

STORAGE_DIR = Path("server_storage")
STORAGE_DIR.mkdir(exist_ok=True)

class file:
    name: str
    hash: str

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
    with open(destination, "wb") as f:
        f.write(contents)

    calculated_hash = hashlib.sha256(contents).hexdigest()

    return {
        "status": "success",
        "filename": file.filename,
        "bytes_received": len(contents),
        "calculated_hash": calculated_hash,
        "posted_hash": file_hash,
    }