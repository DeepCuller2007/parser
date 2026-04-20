# CIAN parser

Парсер объявлений ЦИАН на Playwright. Собирает ссылки из выдачи, открывает объявления, сохраняет характеристики квартир в JSON и умеет сохранять фотографии локально или в MinIO.

## MinIO

Запустите локальное S3-совместимое хранилище:

```powershell
docker compose up -d
```

Консоль MinIO будет доступна по адресу:

```text
http://localhost:9001
```

Логин и пароль по умолчанию:

```text
parser
parser-secret
```

## Настройка парсера

Создайте локальный `.env` из примера:

```powershell
Copy-Item .env.example .env
```

Ключевые параметры:

```text
DOWNLOAD_IMAGES=true
IMAGE_STORAGE=minio
TARGET_OFFERS=10000
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=parser
MINIO_SECRET_KEY=parser-secret
MINIO_BUCKET=cian-photos
MINIO_SECURE=false
```

Для короткой проверки можно добавить в `.env`:

```text
MAX_OFFERS=1
```

## Запуск

```powershell
uv sync
uv run playwright install chromium
uv run python src/main.py
```

Результат сохраняется в:

```text
data/raw/listings.json
```

Если включен `IMAGE_STORAGE=minio`, в `image_paths` будут ссылки вида:

```text
minio://cian-photos/offers/{offer_id}/1.jpg
```

## Сбор датасета

Для автоматического сбора большого датасета используется:

```powershell
uv run python src/dataset_runner.py
```

Runner читает поисковые задачи из:

```text
configs/search_jobs.json
```

Каждая задача описывает отдельный срез выдачи ЦИАН: комнаты, цену, тип жилья и лимит страниц. Это помогает собрать более разнообразный датасет, а не первые объявления из одной выдачи.

По умолчанию runner стремится собрать:

```text
TARGET_OFFERS=10000
```

Для короткой проверки перед большим запуском временно поставьте в `.env`:

```text
TARGET_OFFERS=3
MAX_PAGES_PER_JOB=1
MAX_OFFERS_PER_JOB=3
DOWNLOAD_IMAGES=true
IMAGE_STORAGE=minio
```

Runner сохраняет:

```text
data/raw/listings.json
data/raw/progress.json
```

`listings.json` содержит объявления, а `progress.json` хранит завершенные поисковые задачи. Если запуск прервался, повторный запуск продолжит сбор с уже сохраненного состояния и пропустит дубли по `offer_id`.

Для полного сбора удалите или увеличьте тестовые ограничения:

```text
TARGET_OFFERS=10000
MAX_PAGES_PER_JOB=30
MAX_OFFERS_PER_JOB=10000
```

В каждом объявлении дополнительно сохраняются:

```text
source_job
parsed_at
```
