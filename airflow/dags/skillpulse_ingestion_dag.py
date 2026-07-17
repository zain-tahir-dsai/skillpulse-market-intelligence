from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context

from src.ingestion.run_ingestion import run_source


DAG_ID = "skillpulse_ingestion"

DEFAULT_ARGS = {
    "owner": "skillpulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SUPPORTED_SOURCES = {"remoteok", "adzuna"}


def log_task_failure(context: dict[str, Any]) -> None:
    """Write useful failure details to the Airflow task log."""
    task_instance = context["task_instance"]
    exception = context.get("exception")

    logging.error(
        "skillpulse_task_failed | "
        "dag_id=%s | "
        "task_id=%s | "
        "run_id=%s | "
        "logical_date=%s | "
        "try_number=%s | "
        "exception=%s | "
        "log_url=%s",
        task_instance.dag_id,
        task_instance.task_id,
        task_instance.run_id,
        context.get("logical_date"),
        task_instance.try_number,
        exception,
        task_instance.log_url,
    )


with DAG(
    dag_id=DAG_ID,
    description="Run SkillPulse job-posting ingestion from RemoteOK or Adzuna.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 9),
    schedule="@daily",
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    max_active_runs=1,
    tags=["skillpulse", "ingestion", "remoteok", "adzuna"],
    on_failure_callback=log_task_failure,
) as dag:

    @task(task_id="validate_config")
    def validate_config() -> dict[str, Any]:
        """Check required folders and runtime parameters."""

        required_directories = [
            Path("/opt/airflow/data/raw"),
            Path("/opt/airflow/project_logs"),
        ]

        missing_directories = [
            directory
            for directory in required_directories
            if not directory.exists()
        ]

        if missing_directories:
            missing_text = ", ".join(
                directory.as_posix()
                for directory in missing_directories
            )
            raise AirflowFailException(
                f"Required directories are missing: {missing_text}"
            )

        context = get_current_context()
        dag_run = context["dag_run"]
        run_config = dag_run.conf or {}

        source = run_config.get("source", "remoteok")
        if source not in SUPPORTED_SOURCES:
            raise AirflowFailException(
                f"Unsupported source '{source}'. "
                f"Expected one of: {sorted(SUPPORTED_SOURCES)}"
            )

        print("Configuration validation passed.")

        return {
            "source": source,
            "query": run_config.get("query", "data engineer"),
            "country": run_config.get("country", "gb"),
            "page_size": int(run_config.get("page_size", 10)),
            "max_pages": int(run_config.get("max_pages", 1)),
            "simulate_retry": bool(run_config.get("simulate_retry", False)),
            "simulate_failure": bool(run_config.get("simulate_failure", False)),
        }

    @task(task_id="ingest_source")
    def ingest_source(runtime_config: dict[str, Any]) -> dict[str, Any]:
        """Run the selected source using runtime DAG configuration."""

        context = get_current_context()
        dag_run = context["dag_run"]
        task_instance = context["task_instance"]

        source = runtime_config["source"]
        simulate_retry = runtime_config["simulate_retry"]
        simulate_failure = runtime_config["simulate_failure"]

        # Fail only on first attempt to verify retry behaviour.
        if simulate_retry and task_instance.try_number == 1:
            raise RuntimeError(
                "Controlled transient failure for retry verification."
            )

        # Fail on every attempt until retries are exhausted.
        if simulate_failure:
            raise RuntimeError(
                "Controlled persistent failure for retry "
                "exhaustion verification."
            )

        result = run_source(
            source,
            query=runtime_config["query"],
            country=runtime_config["country"],
            page_size=runtime_config["page_size"],
            max_pages=runtime_config["max_pages"],
        )

        if result["status"] != "success":
            raise AirflowFailException(
                f"{source} ingestion failed: "
                f"{result.get('error', 'Unknown error')}"
            )

        result["airflow_run_id"] = dag_run.run_id
        result["airflow_logical_date"] = str(context["logical_date"])

        print(
            f"{source} ingestion completed. "
            f"records={result['record_count']}"
        )

        return result

    @task(task_id="validate_raw_output")
    def validate_raw_output(
        ingestion_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Confirm ingestion produced usable raw output."""

        raw_files = ingestion_result.get("raw_files", [])
        record_count = ingestion_result.get("record_count", 0)

        if not raw_files:
            raise AirflowFailException(
                "Ingestion finished but did not report any raw output files."
            )

        if record_count <= 0:
            raise AirflowFailException(
                "Ingestion finished but returned zero job records."
            )

        for raw_file in raw_files:
            raw_path = Path(raw_file)
            if not raw_path.exists():
                raise AirflowFailException(
                    f"Expected raw output file does not exist: {raw_file}"
                )

        print(
            "Raw output validation passed. "
            f"files={len(raw_files)}, records={record_count}"
        )

        return ingestion_result

    @task(task_id="report_run_status")
    def report_run_status(validated_result: dict[str, Any]) -> None:
        """Write final Airflow run summary."""

        print(
            "SkillPulse ingestion DAG completed successfully. "
            f"source={validated_result['source']}, "
            f"records={validated_result['record_count']}, "
            f"raw_files={validated_result['raw_files']}, "
            f"airflow_run_id={validated_result['airflow_run_id']}, "
            f"airflow_logical_date={validated_result['airflow_logical_date']}"
        )

    config_check = validate_config()
    ingestion_result = ingest_source(config_check)
    validated_result = validate_raw_output(ingestion_result)
    run_report = report_run_status(validated_result)

    config_check >> ingestion_result
    validated_result >> run_report
