
from pathlib import Path

def resolve_parquet_file(path: Path) -> Path:
    if path.is_file():
        return path
    first_entry = next(path.iterdir())
    return first_entry