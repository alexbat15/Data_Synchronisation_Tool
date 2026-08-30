from pathlib import Path

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse

from server.chunking import ChunkUploadService, UploadProtocolError
from shared.models import FileInitRequest


def create_app(storage_dir: str | Path = Path("server_storage")) -> FastAPI:
    app = FastAPI()
    service = ChunkUploadService(storage_dir)
    app.state.upload_service = service

    @app.exception_handler(UploadProtocolError)
    def upload_error_handler(request: Request, exc: UploadProtocolError):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/upload")
    def upload_file(file: UploadFile = File(...), file_hash: str = Form(...)):
        return service.store_whole_file(file.file, file.filename or "", file_hash)

    @app.post("/files/init")
    def init_file(request: FileInitRequest):
        return service.initialize(request)

    return app


app = create_app()
