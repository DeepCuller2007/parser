import os

from parser.cian_parser import CianPlaywrightParser, ParserConfig


def _get_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


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
