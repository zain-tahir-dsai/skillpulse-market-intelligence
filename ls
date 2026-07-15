[1mdiff --git a/airflow/dags/skillpulse_ingestion_dag.py b/airflow/dags/skillpulse_ingestion_dag.py[m
[1mindex dbd1908..a90cced 100644[m
[1m--- a/airflow/dags/skillpulse_ingestion_dag.py[m
[1m+++ b/airflow/dags/skillpulse_ingestion_dag.py[m
[36m@@ -6,12 +6,14 @@[m [mfrom typing import Any[m
 [m
 from airflow import DAG[m
 from airflow.decorators import task[m
[32m+[m[32mfrom airflow.operators.python import get_current_context[m
 from airflow.exceptions import AirflowFailException[m
 [m
 from src.ingestion.run_ingestion import run_remoteok_for_airflow[m
 [m
 [m
 DAG_ID = "skillpulse_ingestion"[m
[32m+[m[32mDEFAULT_SOURCE = "remoteok"[m
 TASK_EXECUTION_TIMEOUT = timedelta(minutes=10)[m
 [m
 [m
[36m@@ -48,12 +50,26 @@[m [mwith DAG([m
     schedule="@daily",[m
     catchup=False,[m
     max_active_runs=1,[m
[32m+[m[32m    params={"source": DEFAULT_SOURCE},[m
     tags=["skillpulse", "ingestion", "remoteok"],[m
 ) as dag:[m
 [m
     @task(task_id="validate_config")[m
[31m-    def validate_config() -> None:[m
[31m-        """Check that the required project folders exist."""[m
[32m+[m[32m    def validate_config() -> dict[str, str]:[m
[32m+[m[32m        """Validate run parameters and required project folders."""[m
[32m+[m[32m        context = get_current_context()[m
[32m+[m[32m        dag_run = context["dag_run"][m
[32m+[m
[32m+[m[32m        source = DEFAULT_SOURCE[m
[32m+[m[32m        if dag_run and dag_run.conf:[m
[32m+[m[32m            source = dag_run.conf.get("source", DEFAULT_SOURCE)[m
[32m+[m
[32m+[m[32m        if source != "remoteok":[m
[32m+[m[32m            raise AirflowFailException([m
[32m+[m[32m                f"Unsupported source for this DAG: {source}. "[m
[32m+[m[32m                "Only remoteok is currently enabled."[m
[32m+[m[32m            )[m
[32m+[m
         required_directories = [[m
             Path("/opt/airflow/data/raw"),[m
             Path("/opt/airflow/project_logs"),[m
[36m@@ -74,12 +90,32 @@[m [mwith DAG([m
                 f"Required directories are missing: {missing_text}"[m
             )[m
 [m
[31m-        print("Configuration validation passed.")[m
[32m+[m[32m        logical_date = context["logical_date"].isoformat()[m
[32m+[m
[32m+[m[32m        print([m
[32m+[m[32m            "Configuration validation passed. "[m
[32m+[m[32m            f"source={source}, logical_date={logical_date}"[m
[32m+[m[32m        )[m
[32m+[m
[32m+[m[32m        return {[m
[32m+[m[32m            "source": source,[m
[32m+[m[32m            "logical_date": logical_date,[m
[32m+[m[32m        }[m
 [m
     @task(task_id="ingest_remoteok")[m
[31m-    def ingest_remoteok() -> dict:[m
[31m-        """Run the existing RemoteOK ingestion function."""[m
[32m+[m[32m    def ingest_remoteok(run_parameters: dict[str, str]) -> dict:[m
[32m+[m[32m        """Run RemoteOK ingestion and attach Airflow run metadata."""[m
[32m+[m[32m        source = run_parameters["source"][m
[32m+[m[32m        logical_date = run_parameters["logical_date"][m
[32m+[m
[32m+[m[32m        if source != "remoteok":[m
[32m+[m[32m            raise AirflowFailException([m
[32m+[m[32m                f"Task ingest_remoteok cannot run source: {source}"[m
[32m+[m[32m            )[m
[32m+[m
         result = run_remoteok_for_airflow()[m
[32m+[m[32m        result["airflow_logical_date"] = logical_date[m
[32m+[m[32m        result["airflow_source_parameter"] = source[m
 [m
         if result["status"] != "success":[m
             raise AirflowFailException([m
[36m@@ -88,7 +124,8 @@[m [mwith DAG([m
             )[m
 [m
         print([m
[31m-            f"RemoteOK ingestion completed. "[m
[32m+[m[32m            "RemoteOK ingestion completed. "[m
[32m+[m[32m            f"source={source}, logical_date={logical_date}, "[m
             f"records={result['record_count']}"[m
         )[m
 [m
[36m@@ -96,7 +133,7 @@[m [mwith DAG([m
 [m
     @task(task_id="validate_raw_output")[m
     def validate_raw_output(ingestion_result: dict) -> dict:[m
[31m-        """Confirm that the ingestion task produced at least one raw file."""[m
[32m+[m[32m        """Confirm that ingestion produced at least one raw file."""[m
         raw_files = ingestion_result.get("raw_files", [])[m
         record_count = ingestion_result.get("record_count", 0)[m
 [m
[36m@@ -119,7 +156,7 @@[m [mwith DAG([m
                 )[m
 [m
         print([m
[31m-            f"Raw output validation passed. "[m
[32m+[m[32m            "Raw output validation passed. "[m
             f"files={len(raw_files)}, records={record_count}"[m
         )[m
 [m
[36m@@ -127,17 +164,16 @@[m [mwith DAG([m
 [m
     @task(task_id="report_run_status")[m
     def report_run_status(validated_result: dict) -> None:[m
[31m-        """Write a final task-level run summary into Airflow logs."""[m
[32m+[m[32m        """Write a final run summary into Airflow task logs."""[m
         print([m
             "SkillPulse ingestion DAG completed successfully. "[m
             f"source={validated_result['source']}, "[m
[32m+[m[32m            f"logical_date={validated_result['airflow_logical_date']}, "[m
             f"records={validated_result['record_count']}, "[m
             f"raw_files={validated_result['raw_files']}"[m
         )[m
 [m
[31m-    config_check = validate_config()[m
[31m-    ingestion_result = ingest_remoteok()[m
[32m+[m[32m    run_parameters = validate_config()[m
[32m+[m[32m    ingestion_result = ingest_remoteok(run_parameters)[m
     validated_result = validate_raw_output(ingestion_result)[m
     report_run_status(validated_result)[m
[31m-[m
[31m-    config_check >> ingestion_result[m
