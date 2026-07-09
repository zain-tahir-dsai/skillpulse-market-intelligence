from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context

from src.ingestion.run_ingestion import run_remoteok_for_airflow


DAG_ID = "skillpulse_ingestion"

DEFAULT_ARGS = {
    "owner": "skillpulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


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
    description="Run SkillPulse job-posting ingestion from RemoteOK.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 9),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["skillpulse", "ingestion", "remoteok"],
    on_failure_callback=log_task_failure,
) as dag:

    @task(task_id="validate_config")
    def validate_config() -> None:
        """Check that the required project folders exist."""
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

        print("Configuration validation passed.")

    @task(task_id="ingest_remoteok")
    def ingest_remoteok() -> dict[str, Any]:
        """Run RemoteOK ingestion using the runtime DAG configuration."""
        context = get_current_context()
        dag_run = context["dag_run"]
        run_config = dag_run.conf or {}

        source = run_config.get("source", "remoteok")

        if source != "remoteok":
            raise AirflowFailException(
                "This DAG currently supports only source='remoteok'. "
                f"Received source='{source}'."
            )

        result = run_remoteok_for_airflow()

        if result["status"] != "success":
            raise AirflowFailException(
                "RemoteOK ingestion failed: "
                f"{result.get('error', 'Unknown error')}"
            )

        result["airflow_run_id"] = dag_run.run_id
        result["airflow_logical_date"] = str(context["logical_date"])

        print(
            "RemoteOK ingestion completed. "
            f"records={result['record_count']}"
        )

        return result

    @task(task_id="validate_raw_output")
    def validate_raw_output(
        ingestion_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Confirm that ingestion produced usable raw output."""
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
        """Write the final Airflow-level run summary."""
        print(
            "SkillPulse ingestion DAG completed successfully. "
            f"source={validated_result['source']}, "
            f"records={validated_result['record_count']}, "
            f"raw_files={validated_result['raw_files']}, "
            f"airflow_run_id={validated_result['airflow_run_id']}, "
            f"airflow_logical_date="
            f"{validated_result['airflow_logical_date']}"
        )

    config_check = validate_config()
    ingestion_result = ingest_remoteok()
    validated_result = validate_raw_output(ingestion_result)
    run_report = report_run_status(validated_result)

    config_check >> ingestion_result
    validated_result >> run_report
