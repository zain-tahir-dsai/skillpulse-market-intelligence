# Adzuna Source Contract

## Purpose

Adzuna provides job-posting data through its Jobs API. SkillPulse will use it as a structured source for job title, company, location, salary, description, posting date, and job URL.

## Base Endpoint

`https://api.adzuna.com/v1/api/jobs/{country}/search/{page}`

## Authentication

Adzuna requires two query parameters:

* `app_id`: stored as `ADZUNA_APP_ID` in `.env`
* `app_key`: stored as `ADZUNA_APP_KEY` in `.env`

Credentials must never be written directly in source code or committed to Git.

## Default Request Parameters

* Country: `gb`
* Page: configurable, starting at `1`
* Results per page: `50`
* Search query: configurable
* Category: configurable when needed

Example request shape:

```text
GET /v1/api/jobs/gb/search/1?app_id=...&app_key=...&results_per_page=50&what=data%20engineer
```

## Expected Response Structure

The response is JSON and is expected to contain:

* `count`: total matching jobs
* `results`: list of job-posting objects

Each job object may contain:

* `id`: source-specific job identifier
* `title`: job title
* `description`: job description
* `redirect_url`: job-posting URL
* `created`: source posting timestamp
* `company.display_name`: company name
* `location.display_name`: location text
* `category.label`: job category
* `salary_min`: minimum salary, when available
* `salary_max`: maximum salary, when available
* `contract_type`: permanent, contract, or unknown
* `contract_time`: full-time, part-time, or unknown

## Pagination

Pagination uses the page number in the URL path. The pipeline must:

1. Start at page `1`.
2. Continue until the configured maximum page count, the API returns an empty `results` list, or the expected result count has been reached.
3. Record the page number in raw-data metadata.
4. Stop safely if the API reports an error.

## Rate Limits and Reliability

The exact request quota depends on the Adzuna account plan. The ingestion service will use:

* Request timeout: `30` seconds
* Maximum retries: `3`
* Exponential backoff between retries
* Structured logs for every request and failure

## Raw Landing Requirement

Each successful API response must be saved unchanged as JSON in:

```text
data/raw/adzuna/
```

The file name will include the source, country, search query, page number, and ingestion timestamp.

## Data Limitations

* Salary fields are frequently missing or incomplete.
* Location may be broad or inconsistent.
* A posting can appear more than once across pages or runs.
* The source does not guarantee that all postings are still active.
* The API may return HTML or malformed content during temporary failures.

## Failure Handling

* `200`: save and process the raw response.
* `401` or `403`: stop the source run and report an authentication or authorization error.
* `429`: retry using exponential backoff.
* `500` to `599`: retry up to the configured limit.
* Invalid JSON or missing `results`: log the failure and do not write the response as a valid raw file.
