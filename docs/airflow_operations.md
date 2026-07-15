# SkillPulse Airflow Operations

## DAG

Name:
`skillpulse_ingestion`

Purpose:
Ingest RemoteOK job postings into the SkillPulse raw data layer.

---

### Schedule

- Daily
- Cron: `@daily`

---

### Retry Policy

- Retries: **2**
- Retry Delay: **5 minutes**

---

### Timeout

- DAG Run Timeout: **30 minutes**

---

### Catchup

Disabled

```python
catchup=False
```

Reason:

Historical executions are handled explicitly through Airflow backfill commands rather than automatic scheduler catchup.

---

### Max Active Runs

```python
max_active_runs=1
```

Reason:

Prevents overlapping ingestion runs.

---

### Supported Source Parameters

Default:

```json
{
  "source": "remoteok"
}
```

Optional runtime parameters:

```json
{
  "source": "remoteok",
  "simulate_retry": true
}
```

Purpose:

Used to validate Airflow retry behavior during testing.

---

### Historical Backfill

Example:

```bash
airflow dags backfill \
skillpulse_ingestion \
-s 2026-07-09 \
-e 2026-07-10
```

---

### Validation Checks

- Configuration validation
- RemoteOK ingestion
- Raw data validation
- Final run status reporting

---

### Failure Handling

- Automatic retries
- Airflow task logs
- DAG failure callback
- Structured run metadata