import json
import os
import shutil
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from parser.cian_parser import CianPlaywrightParser, ParserConfig


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_JOBS_PATH = PROJECT_ROOT / "configs/search_jobs.json"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "data/raw/listings.json"
DEFAULT_PROGRESS_PATH = PROJECT_ROOT / "data/raw/progress.json"


def _resolve_project_path(path: str | Path) -> Path:
    path = Path(path)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _get_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


def _get_optional_int_env(name: str) -> Optional[int]:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return int(value)


def _load_env_file(path: str | Path = DEFAULT_ENV_PATH) -> bool:
    path = _resolve_project_path(path)
    if not os.path.exists(path):
        return False

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            name, value = line.split("=", 1)
            os.environ.setdefault(name.strip(), value.strip().strip("'\""))
    return True


def _load_json(path: str | Path, default: Any) -> Any:
    path = _resolve_project_path(path)
    if not os.path.exists(path):
        return default
    if os.path.getsize(path) == 0:
        print(f"[WARN] JSON file is empty, using default value: {path}")
        return default

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as error:
        broken_path = path.with_suffix(f"{path.suffix}.broken")
        shutil.copy2(path, broken_path)
        print(f"[WARN] JSON file is corrupted and will be ignored: {path}")
        print(f"[WARN] Backup copy saved to: {broken_path}")
        print(f"[WARN] JSON error: {error}")
        return default


def _save_json(path: str | Path, payload: Any) -> None:
    path = _resolve_project_path(path)
    os.makedirs(path.parent, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, path)


def _load_jobs(path: str | Path) -> List[Dict[str, Any]]:
    jobs = _load_json(path, [])
    if not isinstance(jobs, list):
        raise ValueError(f"Search jobs file must contain a list: {path}")
    return jobs


def _extract_seen_offer_ids(offers: List[Dict[str, Any]]) -> Set[str]:
    return {str(offer["offer_id"]) for offer in offers if offer.get("offer_id")}


def _build_parser_config(job: Dict[str, Any], remaining_target: int) -> ParserConfig:
    max_pages = int(job.get("max_pages", 10))
    max_pages_override = os.getenv("MAX_PAGES_PER_JOB")
    if max_pages_override:
        max_pages = min(max_pages, int(max_pages_override))

    job_max_offers: Optional[int] = job.get("max_offers")
    if job_max_offers is None:
        job_max_offers = remaining_target
    else:
        job_max_offers = min(int(job_max_offers), remaining_target)

    max_offers_override = os.getenv("MAX_OFFERS_PER_JOB")
    if max_offers_override:
        job_max_offers = min(job_max_offers, int(max_offers_override), remaining_target)

    return ParserConfig(
        search_url=job["search_url"],
        max_pages=max_pages,
        max_offers=job_max_offers,
        download_images=_get_bool_env("DOWNLOAD_IMAGES", False),
        image_storage=os.getenv("IMAGE_STORAGE", "local"),
        headless=_get_bool_env("HEADLESS", True),
        timeout_ms=_get_int_env("TIMEOUT_MS", 30000),
        pause_between_pages=float(os.getenv("PAUSE_BETWEEN_PAGES", "2.0")),
        pause_between_offers=float(os.getenv("PAUSE_BETWEEN_OFFERS", "1.5")),
        offer_concurrency=_get_int_env("OFFER_CONCURRENCY", 3),
        image_concurrency=_get_int_env("IMAGE_CONCURRENCY", 2),
        max_images_per_offer=_get_optional_int_env("MAX_IMAGES_PER_OFFER"),
        block_assets=_get_bool_env("BLOCK_BROWSER_ASSETS", True),
        search_wait_ms=_get_int_env("SEARCH_WAIT_MS", 1200),
        scroll_wait_ms=_get_int_env("SCROLL_WAIT_MS", 800),
        offer_wait_ms=_get_int_env("OFFER_WAIT_MS", 2000),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "parser"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "parser-secret"),
        minio_bucket=os.getenv("MINIO_BUCKET", "cian-photos"),
        minio_secure=_get_bool_env("MINIO_SECURE", False),
    )


def _save_progress(
    path: str | Path,
    target_offers: int,
    total_saved: int,
    completed_jobs: List[str],
    current_job: Optional[str] = None,
) -> None:
    _save_json(
        path,
        {
            "target_offers": target_offers,
            "total_saved": total_saved,
            "completed_jobs": completed_jobs,
            "current_job": current_job,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def main() -> None:
    env_loaded = _load_env_file()
    if not env_loaded:
        raise SystemExit(
            f"[ERROR] .env file not found: {DEFAULT_ENV_PATH}. "
            "Create it from the project root with: Copy-Item .env.example .env"
        )

    jobs_path = _resolve_project_path(os.getenv("SEARCH_JOBS_PATH", DEFAULT_JOBS_PATH))
    output_path = _resolve_project_path(os.getenv("DATASET_OUTPUT_PATH", DEFAULT_OUTPUT_PATH))
    progress_path = _resolve_project_path(os.getenv("DATASET_PROGRESS_PATH", DEFAULT_PROGRESS_PATH))
    target_offers = _get_int_env("TARGET_OFFERS", 10_000)
    resume_completed_jobs = _get_bool_env("RESUME_COMPLETED_JOBS", True)

    jobs = _load_jobs(jobs_path)
    output_exists = output_path.exists()
    progress_exists = progress_path.exists()
    all_offers: List[Dict[str, Any]] = _load_json(output_path, [])
    progress = _load_json(progress_path, {})
    completed_jobs = list(progress.get("completed_jobs", [])) if resume_completed_jobs else []
    seen_offer_ids = _extract_seen_offer_ids(all_offers)

    if not output_exists:
        _save_json(output_path, all_offers)
        print(f"[INFO] Created dataset output file: {output_path}")

    if not progress_exists:
        _save_progress(progress_path, target_offers, len(seen_offer_ids), completed_jobs)
        print(f"[INFO] Created dataset progress file: {progress_path}")

    print(f"[INFO] Search jobs: {jobs_path}")
    print(f"[INFO] Dataset output: {output_path}")
    print(f"[INFO] Dataset progress: {progress_path}")
    print(f"[INFO] Target unique listings: {target_offers}")

    for job in jobs:
        job_name = job["name"]
        if len(seen_offer_ids) >= target_offers:
            break

        if resume_completed_jobs and job_name in completed_jobs:
            continue

        remaining_target = target_offers - len(seen_offer_ids)
        _save_progress(progress_path, target_offers, len(seen_offer_ids), completed_jobs, current_job=job_name)

        config = _build_parser_config(job, remaining_target)
        parser = CianPlaywrightParser(config)
        offers = parser.run(
            skip_offer_ids=seen_offer_ids,
            source_job=job_name,
            target_count=remaining_target,
        )

        new_count = 0
        for offer in offers:
            if offer.offer_id in seen_offer_ids:
                continue

            seen_offer_ids.add(offer.offer_id)
            all_offers.append(asdict(offer))
            new_count += 1

            if len(seen_offer_ids) >= target_offers:
                break

        if job_name not in completed_jobs:
            completed_jobs.append(job_name)

        _save_json(output_path, all_offers)
        _save_progress(progress_path, target_offers, len(seen_offer_ids), completed_jobs)
        print(
            f"[INFO] Job '{job_name}' finished: added {new_count} new listings, "
            f"total unique listings {len(seen_offer_ids)}."
        )

    _save_json(output_path, all_offers)
    _save_progress(progress_path, target_offers, len(seen_offer_ids), completed_jobs)
    print(f"[INFO] Dataset collection finished. Saved {len(seen_offer_ids)} unique listings.")


if __name__ == "__main__":
    main()
