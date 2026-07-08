# RemoteOK Source Contract

## Purpose

RemoteOK provides remote job-posting data through a public JSON API. SkillPulse will use it to capture remote-first job demand and skill signals.

## Base Endpoint

`https://remoteok.com/api`

## Authentication

No API key is required for the public endpoint.

The ingestion service must send a clear `User-Agent` header and follow the source’s terms and access expectations.

## Default Request Parameters

The API endpoint generally returns a single JSON payload containing available jobs. It does not use the same country, query, or page parameters as Adzuna.

Example request shape:

```text
GET /api
```

## Expected Response Structure

The response is JSON and is expected to be a list.

The first list item may contain metadata rather than a job posting. The ingestion pipeline must detect and exclude metadata records before counting jobs.

Job objects may contain:

* `id`: source-specific job identifier
* `slug`: URL-friendly job identifier
* `position`: job title
* `company`: company name
* `location`: location text
* `description`: HTML job description
* `tags`: skill or category tags
* `date`: posting timestamp
* `url`: job-posting URL
* `salary_min`: minimum salary, when available
* `salary_max`: maximum salary, when available

## Pagination

The public endpoint is treated as a snapshot feed. It does not provide conventional page-number pagination.

The pipeline must:

1. Fetch the endpoint once per configured run.
2. Save the full response unchanged.
3. Record the ingestion timestamp.
4. Use stable source IDs later to identify repeated postings across runs.

## Rate Limits and Reliability

RemoteOK does not provide a guaranteed public quota in the API response. The ingestion service will therefore use conservative behavior:

* Request timeout: `30` seconds
* Maximum retries: `3`
* Exponential backoff between retries
* One request per scheduled run initially
* Structured logs for request status, failures, and job count

## Raw Landing Requirement

Each successful API response must be saved unchanged as JSON in:

```text
data/raw/remoteok/
```

The file name will include the source and ingestion timestamp.

## Data Limitations

* The response may include a metadata record before job records.
* Descriptions are HTML and require later cleaning.
* Salary values may be missing or inconsistent.
* Location can be global, regional, or absent.
* The public feed can change without notice.
* The feed may include inactive or duplicate postings.

## Failure Handling

* `200`: save the response unchanged.
* `403` or `429`: log the access issue, apply conservative retry behavior, and stop after the configured retry limit.
* `500` to `599`: retry up to the configured limit.
* Invalid JSON or a response that is not a list: log the failure and do not write it as a valid raw file.
