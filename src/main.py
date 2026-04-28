import os

from parser.cian_parser import CianPlaywrightParser, ParserConfig


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


def _get_optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return None
    return int(value)


def _load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            name, value = line.split("=", 1)
            os.environ.setdefault(name.strip(), value.strip().strip("'\""))


def main() -> None:
    _load_env_file()

    config = ParserConfig(
        search_url="https://www.cian.ru/kupit-kvartiru/",
        max_offers=int(os.getenv("MAX_OFFERS")) if os.getenv("MAX_OFFERS") else None,
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

    parser = CianPlaywrightParser(config)
    offers = parser.run()
    parser.save_json(offers, "data/raw/listings.json")

    print(f"Saved listings: {len(offers)}")


if __name__ == "__main__":
    main()
