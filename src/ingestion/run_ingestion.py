from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.ingestion.sources import AdzunaIngestor, RemoteOkIngestor


RUN_LOG_DIRECTORY = Path("logs/ingestion")


def utc_now() -> datetime:
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc)


def write_run_log(run_metadata: dict[str, Any]) -> Path:
    """Write one ingestion-run audit record as JSON."""
    RUN_LOG_DIRECTORY.mkdir(parents=True, exist_ok=True)

    timestamp_text = utc_now().strftime("%Y%m%dT%H%M%SZ")
    source_name = run_metadata["source"]

    output_path = RUN_LOG_DIRECTORY / (
        f"{source_name}_{timestamp_text}.json"
    )

    with output_path.open("x", encoding="utf-8") as log_file:
        json.dump(run_metadata, log_file, ensure_ascii=False, indent=2)

    return output_path


def run_remoteok() -> dict[str, Any]:
    """Run RemoteOK ingestion and return audit metadata."""
    started_at = utc_now()

    try:
        output_path, record_count = RemoteOkIngestor().run()

        return {
            "source": "remoteok",
            "status": "success",
            "started_at": started_at.isoformat(),
            "finished_at": utc_now().isoformat(),
            "record_count": record_count,
            "raw_files": [output_path.as_posix()],
        }
    except Exception as error:
        return {
            "source": "remoteok",
            "status": "failed",
            "started_at": started_at.isoformat(),
            "finished_at": utc_now().isoformat(),
            "record_count": 0,
            "raw_files": [],
            "error": str(error),
        }


def run_remoteok_for_airflow() -> dict[str, Any]:
    """Run RemoteOK ingestion and fail the Airflow task on errors."""
    result = run_remoteok()

    if result["status"] == "failed":
        raise RuntimeError(
            f"RemoteOK ingestion failed: {result['error']}"
        )

    return result


def run_adzuna_for_airflow(
    query: str,
    country: str,
    page_size: int,
    max_pages: int,
) -> dict[str, Any]:
    """Run Adzuna ingestion and fail the Airflow task on errors."""
    result = run_adzuna(
        query=query,
        country=country,
        page_size=page_size,
        max_pages=max_pages,
    )

    if result["status"] == "failed":
        raise RuntimeError(
            f"Adzuna ingestion failed: {result['error']}"
        )

    return result


def run_source(
    source: str,
    *,
    query: str = "data engineer",
    country: str = "gb",
    page_size: int = 10,
    max_pages: int = 1,
) -> dict[str, Any]:
    """Run one configured ingestion source and return its audit metadata."""
    if source == "remoteok":
        return run_remoteok()

    if source == "adzuna":
        return run_adzuna(
            query=query,
            country=country,
            page_size=page_size,
            max_pages=max_pages,
        )

    raise ValueError(
        f"Unsupported source '{source}'. Expected 'remoteok' or 'adzuna'."
    )


def run_adzuna(
    query: str,
    country: str,
    page_size: int,
    max_pages: int,
) -> dict[str, Any]:
    """Run Adzuna ingestion and return audit metadata."""
    started_at = utc_now()

    try:
        output_paths, record_count = AdzunaIngestor().run(
            query=query,
            country=country,
            page_size=page_size,
            max_pages=max_pages,
        )

        return {
            "source": "adzuna",
            "status": "success",
            "started_at": started_at.isoformat(),
            "finished_at": utc_now().isoformat(),
            "record_count": record_count,
            "raw_files": [path.as_posix() for path in output_paths],
        }
    except Exception as error:
        return {
            "source": "adzuna",
            "status": "failed",
            "started_at": started_at.isoformat(),
            "finished_at": utc_now().isoformat(),
            "record_count": 0,
            "raw_files": [],
            "error": str(error),
        }


def parse_arguments() -> argparse.Namespace:
    """Parse command-line options for an ingestion run."""
    parser = argparse.ArgumentParser(
        description="Run SkillPulse raw job-data ingestion."
    )

    parser.add_argument(
        "--source",
        choices=["remoteok", "adzuna", "all"],
        default="all",
        help="Choose which source to ingest.",
    )
    parser.add_argument(
        "--query",
        default="data engineer",
        help="Adzuna job search query.",
    )
    parser.add_argument(
        "--country",
        default="gb",
        help="Adzuna country code.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=10,
        help="Number of Adzuna jobs per page.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="Maximum Adzuna pages to fetch.",
    )

    return parser.parse_args()


def main() -> None:
    """Run selected ingestion sources and write audit logs."""
    args = parse_arguments()
    run_results: list[dict[str, Any]] = []

    if args.source in ("remoteok", "all"):
        run_results.append(run_source("remoteok"))

    if args.source in ("adzuna", "all"):
        run_results.append(
            run_source(
                "adzuna",
                query=args.query,
                country=args.country,
                page_size=args.page_size,
                max_pages=args.max_pages,
            )
        )

    for result in run_results:
        log_path = write_run_log(result)

        print(
            f"{result['source']}: {result['status']} | "
            f"records={result['record_count']} | "
            f"log={log_path}"
        )

        if result["status"] == "failed":
            print(f"error={result['error']}")


if __name__ == "__main__":
    main()
