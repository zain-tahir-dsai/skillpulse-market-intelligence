from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class RawFileAlreadyExistsError(FileExistsError):
    """Raised when a raw ingestion file already exists."""


def write_json_once(
    output_path: Path,
    payload: dict[str, Any] | list[dict[str, Any]],
) -> Path:
    """Write JSON only when the target raw file does not already exist."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        raise RawFileAlreadyExistsError(
            f"Raw file already exists: {output_path}"
        )

    with output_path.open("x", encoding="utf-8") as raw_file:
        json.dump(payload, raw_file, ensure_ascii=False, indent=2)

    return output_path
