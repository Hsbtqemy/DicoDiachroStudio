from __future__ import annotations

import hashlib
import mimetypes
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

import requests


def _filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name
    return name or "download.bin"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def import_from_share_link(
    url: str,
    imports_dir: Path,
    extract_dir: Path,
    timeout: int = 120,
) -> tuple[list[Path], dict[str, str]]:
    imports_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    filename = _filename_from_url(url)
    target = imports_dir / filename

    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with target.open("wb") as out:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    out.write(chunk)

    file_hash = _sha256_file(target)
    imported_files: list[Path] = []

    is_zip = zipfile.is_zipfile(target)
    if is_zip:
        dest_dir = extract_dir / f"{target.stem}-{file_hash[:10]}"
        dest_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(target, "r") as archive:
            archive.extractall(dest_dir)
        imported_files.extend(sorted(p for p in dest_dir.rglob("*") if p.is_file()))
    else:
        guessed_ext = (
            Path(filename).suffix or mimetypes.guess_extension("application/octet-stream") or ".bin"
        )
        renamed = imports_dir / f"{target.stem}-{file_hash[:10]}{guessed_ext}"
        if target != renamed:
            if renamed.exists():
                renamed.unlink()
            target.rename(renamed)
            target = renamed
        imported_files.append(target)

    metadata = {
        "source_url": url,
        "downloaded_at": datetime.now(tz=UTC).isoformat(),
        "sha256": file_hash,
        "artifact": str(target),
        "is_zip": str(is_zip),
    }
    return imported_files, metadata
