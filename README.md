# CIAN Listings Parser

This project collects apartment listings from public CIAN search pages, stores parsed listing data as JSON, optionally saves listing photos locally or to a local MinIO bucket, and validates the resulting dataset with Pandera.

The repository is meant to be runnable by a new developer without private keys. The default setup does not require any external API keys.

## What Is Included

- `src/main.py` - a small single-search parser entry point.
- `src/dataset_runner.py` - a resumable dataset collection runner that reads search jobs from JSON.
- `src/parser/cian_parser.py` - Playwright-based CIAN parser.
- `src/validation/filter_dataset.py` - dataset validation and filtering.
- `configs/search_jobs.json` - a small search-job set for normal runs and tests.
- `configs/search_jobs_10000.json` - an extended search-job set for larger dataset collection.
- `docker-compose.yml` - optional local MinIO service for S3-compatible photo storage.
- `data.dvc` - DVC metadata for datasets, if you configure your own DVC remote.

## Requirements

- Python 3.14, as declared in `pyproject.toml`.
- `uv` for dependency management.
- Chromium installed through Playwright.
- Docker, only if you want to store photos in MinIO.

Install `uv` with pip if you do not already have it:

```bash
python -m pip install uv
```

## Setup

Clone the repository and install dependencies:

```bash
git clone <repository-url>
cd parser-repo
uv sync --group dev
uv run playwright install chromium
```

Create a local environment file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

The `.env` file is local-only and is ignored by git.

## Configuration

The example `.env.example` is configured for a small safe run:

```env
DOWNLOAD_IMAGES=true
IMAGE_STORAGE=minio
MAX_OFFERS=3
TARGET_OFFERS=3
MAX_PAGES_PER_JOB=1
MAX_OFFERS_PER_JOB=3
```

Useful options:

- `DOWNLOAD_IMAGES` - save image files when `true`.
- `IMAGE_STORAGE` - `local` or `minio`.
- `MAX_OFFERS` - maximum listings for `src/main.py`.
- `TARGET_OFFERS` - target unique listings for `src/dataset_runner.py`.
- `SEARCH_JOBS_PATH` - path to the search job config.
- `DATASET_OUTPUT_PATH` - raw dataset JSON output path.
- `DATASET_PROGRESS_PATH` - resumable runner progress path.
- `HEADLESS` - run Chromium without a visible browser window when `true`.
- `OFFER_CONCURRENCY` - number of listing pages parsed in parallel.
- `IMAGE_CONCURRENCY` - number of image downloads/uploads in parallel.
- `MAX_IMAGES_PER_OFFER` - optional cap for photos per listing.

If CIAN starts returning errors or captcha pages, lower `OFFER_CONCURRENCY`, increase pauses, and keep the run small.

## Optional MinIO Photo Storage

If `.env` uses `IMAGE_STORAGE=minio`, start the local MinIO service:

```bash
docker compose up -d
```

MinIO console:

```text
http://localhost:9001
```

The default credentials in `.env.example` are local development credentials for the Docker service. Change them in your local `.env` for any non-local use.

To avoid Docker entirely, set:

```env
IMAGE_STORAGE=local
```

Local images are written under `data/images/`.

## Run A Small Parser Smoke Test

Make sure `.env` contains `MAX_OFFERS=3`, then run:

```bash
uv run python src/main.py
```

The output is written to:

```text
data/raw/listings.json
```

## Run The Dataset Collector

The dataset runner uses search jobs and keeps progress, so interrupted runs can continue later.

```bash
uv run python src/dataset_runner.py
```

Default output paths from `.env.example`:

```text
data/raw/listings.json
data/raw/progress.json
```

For a larger collection, edit `.env`:

```env
TARGET_OFFERS=10000
SEARCH_JOBS_PATH=configs/search_jobs_10000.json
DATASET_OUTPUT_PATH=data/raw/listings_10000.json
DATASET_PROGRESS_PATH=data/raw/progress_10000.json
MAX_PAGES_PER_JOB=100
MAX_OFFERS_PER_JOB=700
RESUME_COMPLETED_JOBS=true
```

Then run:

```bash
uv run python src/dataset_runner.py
```

Each saved listing includes parsed listing fields plus:

```text
source_job
parsed_at
```

## Validate A Dataset

Validate the default large-dataset path:

```bash
uv run python src/validation/filter_dataset.py
```

Or validate a small local run explicitly:

```bash
uv run python src/validation/filter_dataset.py \
  --input data/raw/listings.json \
  --output data/processed/listings_valid.json \
  --errors data/processed/listings_validation_errors.csv
```

Require at least one MinIO photo path for every listing:

```bash
uv run python src/validation/filter_dataset.py --require-photos
```

The validator writes valid rows to JSON and validation failures to CSV.

## Data And DVC

The `data/` directory is ignored by git. Generate data locally with the parser, or configure your own DVC remote and run:

```bash
uv run dvc pull
```

Machine-specific DVC remotes should be stored in `.dvc/config.local`, not in the committed `.dvc/config`.

Example:

```bash
uv run dvc remote add -f -l localstore /path/to/dvc-storage
```

## Notes

- The parser depends on CIAN page markup, so selectors may need updates if the website changes.
- Automated scraping can be rate-limited. Keep concurrency modest and respect the target site's rules.
- Do not commit `.env`, downloaded data, browser caches, or MinIO storage files.
