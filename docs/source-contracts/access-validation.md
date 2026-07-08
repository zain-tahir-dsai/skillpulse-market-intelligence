# API Access Validation

This document records the API-access validation performed for the SkillPulse ingestion sources.

## Validation Environment

* Project: SkillPulse Market Intelligence
* Environment: Local development
* Python environment: Python 3.11 virtual environment
* Validation date: 2026-07-08
* Credentials policy: API credentials are stored only in `.env` and are excluded from Git.

## RemoteOK

### Access Method

* Base URL: `https://remoteok.com/api`
* Authentication: No API key required
* Request method: `GET`
* Required request header:

  * `User-Agent: SkillPulse/0.1 (local development)`

### Validation Result

RemoteOK access was validated successfully.

A request to the RemoteOK API returned:

* HTTP status: `200`
* Response format: JSON list
* Observed payload size: 101 JSON items
* Observed job-like records: 100 records containing the `position` field

The first list item may contain feed metadata rather than a job record. The ingestion module counts job records by checking for the `position` field.

### Ingestion Evidence

The RemoteOK ingestion module successfully:

1. Requested the API feed.
2. Validated that the response was a JSON list.
3. Saved the unchanged payload to `data/raw/remoteok/`.
4. Returned the raw file path and job record count.

## Adzuna

### Access Method

* Base URL: `https://api.adzuna.com/v1/api/jobs`
* Authentication: `app_id` and `app_key`
* Request method: `GET`
* Credentials source: `.env`
* Required environment variables:

  * `ADZUNA_APP_ID`
  * `ADZUNA_APP_KEY`

### Validation Result

Adzuna access was validated successfully using a controlled search request.

Validated request parameters:

* Query: `data engineer`
* Country: `gb`
* Page size: `5`
* Maximum pages: `1`

Observed result:

* HTTP status: `200`
* Response format: JSON object
* Required response field: `results`
* Observed job records: `5`
* Raw files saved: `1`

Credentials were successfully masked in structured request logs. The `app_id` and `app_key` values appear as `***` rather than actual credential values.

### Ingestion Evidence

The Adzuna ingestion module successfully:

1. Loaded credentials from `.env`.
2. Built the country and page-specific API URL.
3. Sent query and pagination parameters.
4. Validated that the response was a JSON object containing a list in `results`.
5. Saved the unchanged response to `data/raw/adzuna/`.
6. Returned saved file paths and total record count.

## Validation Conclusion

Both configured sources are accessible from the local development environment.

* RemoteOK is a public snapshot feed with no authentication.
* Adzuna is an authenticated, paginated search API.
* Both sources return JSON payloads compatible with the current raw landing-zone design.
* Credentials are excluded from version control and masked in structured logs.
