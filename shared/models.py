from pydantic import BaseModel, Field


class FileInitRequest(BaseModel):
    rel_file_path: str
    file_hash: str
    file_size: int = Field(ge=0)
    chunk_size: int = Field(gt=0)
    chunk_hashes: list[str]


class FileCompleteRequest(BaseModel):
    rel_file_path: str
    file_hash: str
