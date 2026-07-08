from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def payload_fingerprint(payload: Any) -> str:
    """Return a stable SHA-256 fingerprint for a JSON-compatible payload."""
    canonical_payload = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()


def write_json_once(output_path: Path, payload: Any) -> Path:
    """Write JSON only when the exact output path does not already exist."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        return output_path

    with output_path.open("w", encoding="utf-8") as raw_file:
        json.dump(payload, raw_file, ensure_ascii=False, indent=2)

    return output_path


def find_existing_payload(
    directory: Path,
    fingerprint: str,
) -> Path | None:
    """Return an existing raw JSON file with the same payload fingerprint."""
    if not directory.exists():
        return None

    for candidate_path in directory.glob("*.json"):
        try:
            with candidate_path.open("r", encoding="utf-8") as raw_file:
                existing_payload = json.load(raw_file)
        except (OSError, json.JSONDecodeError):
            continue

        if payload_fingerprint(existing_payload) == fingerprint:
            return candidate_path

    return None


def write_json_with_fingerprint(
    output_path: Path,
    payload: Any,
) -> tuple[Path, str, bool]:
    """
    Save a payload only if its content is new within the source directory.

    Returns:
        output path, SHA-256 fingerprint, and whether a new file was created.
    """
    fingerprint = payload_fingerprint(payload)
    existing_path = find_existing_payload(output_path.parent, fingerprint)

    if existing_path is not None:
        return existing_path, fingerprint, False

    saved_path = write_json_once(output_path, payload)
    return saved_path, fingerprint, True


def write_run_manifest(
    source: str,
    run_id: str,
    records: list[dict[str, Any]],
    manifest_directory: str | Path = "logs/manifests",
) -> Path:
    """Write a durable JSON manifest describing files handled in one run."""
    directory = Path(manifest_directory)
    directory.mkdir(parents=True, exist_ok=True)

    manifest_path = directory / f"{source}_{run_id}.json"

    manifest = {
        "source": source,
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "records": records,
    }

    with manifest_path.open("w", encoding="utf-8") as manifest_file:
        json.dump(manifest, manifest_file, ensure_ascii=False, indent=2)

    return manifest_path
