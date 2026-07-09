from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException

from src.ingestion.run_ingestion import run_remoteok_for_airflow


DAG_ID = "skillpulse_ingestion"
TASK_EXECUTION_TIMEOUT = timedelta(minutes=10)


def log_task_failure(context: dict[str, Any]) -> None:
    """Write useful task-failure details into the Airflow task log."""
    task_instance = context["task_instance"]
    exception = context.get("exception", "Unknown error")

    print(
        "SkillPulse task failed. "
        f"dag_id={context['dag'].dag_id}, "
        f"task_id={task_instance.task_id}, "
        f"run_id={context['run_id']}, "
        f"try_number={task_instance.try_number}, "
        f"exception={exception}"
    )


DEFAULT_ARGS = {
    "owner": "skillpulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": TASK_EXECUTION_TIMEOUT,
    "on_failure_callback": log_task_failure,
}


with DAG(
    dag_id=DAG_ID,
    description="Run SkillPulse job-posting ingestion from RemoteOK.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 9),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["skillpulse", "ingestion", "remoteok"],
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
    def ingest_remoteok() -> dict:
        """Run the existing RemoteOK ingestion function."""
        result = run_remoteok_for_airflow()

        if result["status"] != "success":
            raise AirflowFailException(
                f"RemoteOK ingestion failed: "
                f"{result.get('error', 'Unknown error')}"
            )

        print(
            f"RemoteOK ingestion completed. "
            f"records={result['record_count']}"
        )

        return result

    @task(task_id="validate_raw_output")
    def validate_raw_output(ingestion_result: dict) -> dict:
        """Confirm that the ingestion task produced at least one raw file."""
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
            f"Raw output validation passed. "
            f"files={len(raw_files)}, records={record_count}"
        )

        return ingestion_result

    @task(task_id="report_run_status")
    def report_run_status(validated_result: dict) -> None:
        """Write a final task-level run summary into Airflow logs."""
        print(
            "SkillPulse ingestion DAG completed successfully. "
            f"source={validated_result['source']}, "
            f"records={validated_result['record_count']}, "
            f"raw_files={validated_result['raw_files']}"
        )

    config_check = validate_config()
    ingestion_result = ingest_remoteok()
    validated_result = validate_raw_output(ingestion_result)
    report_run_status(validated_result)

    config_check >> ingestion_result
