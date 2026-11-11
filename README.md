# Fragrance Catalog Pipeline

**A production-minded data pipeline that scrapes a full retail fragrance catalog, validates every record, and loads it into MongoDB, orchestrated with Apache Airflow and fully containerized.**

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.2-017CEE?logo=apacheairflow&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-6-47A248?logo=mongodb&logoColor=white)
![Pydantic](https://img.shields.io/badge/Pydantic-v2-E92063?logo=pydantic&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

The pipeline crawls the [Ulta fragrance catalog](https://www.ulta.com/shop/fragrance/all). For every product, it captures each size variant with its own SKU, volume and price. Records are validated against a strict schema, and anything malformed is quarantined with the reason attached. Clean data is upserted idempotently, and every document can be traced back to the run that produced it.

> **In a sample run:** about 1,900 products were scraped, validated and written in under 9 minutes, with 0 quarantined. The work was spread across up to 10 parallel Airflow tasks.

---

## Highlights

- **Parallel scraping with dynamic task mapping.** Product URLs are split into batches and fanned out with Airflow's `.expand()`. Concurrency is capped so the source site isn't overwhelmed.
- **Strict validation with quarantine.** Records are checked with Pydantic v2 in strict mode. Failures fall into two groups:
  - **Hard failures** (missing ID or name, no variants, invalid ISO-4217 currency) send the record to a quarantine collection with the reasons attached. These records never reach the main collection.
  - **Soft issues** keep the record but add a `data_quality_flags` entry. For example, when the brand is missing, the pipeline infers it from the product name and flags it.
- **Idempotent loads.** Products are upserted on `product_id`, so re-running the pipeline never creates duplicates. `first_seen_at` is set only on insert (`$setOnInsert`), and `last_updated_at` is refreshed on every run.
- **Traceability and recovery.** Every document carries a `pipeline_run_id`. Raw HTML is saved before any parsing, so data can be reprocessed if a parser breaks. Run metadata (counts, timing, quarantine reasons) goes to a `pipeline_runs` collection.
- **Resilient to failure.**
  - Network-bound tasks retry automatically.
  - A missing tag or a layout change on one page doesn't crash the run.
  - Failures show up in the Airflow UI with clear log messages.
- **Thorough variant extraction.** The pipeline reads the page's embedded `__APOLLO_STATE__` JSON to list every size variant. When a variant's price is missing, it fetches that variant's page and reads the price from the JSON-LD.

---

## Tech stack

| Layer | Technology |
|---|---|
| Orchestration | **Apache Airflow 3.2** (CeleryExecutor, TaskFlow API, dynamic task mapping) |
| Task queue / metadata | **Redis 7.2**, **PostgreSQL 16**, optional **Flower** for worker monitoring |
| Storage | **MongoDB 6** |
| Scraping | **requests**, **BeautifulSoup4**, embedded JSON (Apollo state, JSON-LD) |
| Validation | **Pydantic v2** (strict mode, `extra="forbid"`) |
| Packaging | **uv** with a lock file for reproducible installs |
| Infrastructure | **Docker** and **Docker Compose** (one command brings up the whole stack) |
| Testing | **pytest** (schema unit tests, live scraper integration tests, DAG import checks) |

---

## Architecture

```
              ┌────────────────────────────┐
              │  collect_listing_products  │  paginate listing pages, dedupe URLs
              └─────────────┬──────────────┘
                            ▼
              ┌────────────────────────────┐
              │    build_detail_batches    │  chunk URLs (100 per batch)
              └─────────────┬──────────────┘
                            ▼
   ┌────────────┬───────────┼────────────┬────────────┐
   ▼            ▼           ▼            ▼            ▼
 fetch_details_batch ×N  (dynamic task mapping, retries, ≤10 concurrent)
   └────────────┴───────────┼────────────┴────────────┘
                            ▼
              ┌────────────────────────────┐
              │ consolidate_fetched_batches│
              └─────────────┬──────────────┘
                            ▼
              ┌────────────────────────────┐
              │     validate_products      │  Pydantic strict → valid | quarantine
              └─────────────┬──────────────┘
                            ▼
              ┌────────────────────────────┐
              │       write_to_mongo       │  upsert + raw HTML + run metadata
              └────────────────────────────┘
```

Tasks pass file paths to each other instead of the data itself. The data goes into per-run JSONL files, which keeps Airflow's XCom small. It also means every intermediate step can be inspected on disk.

---

## Data model

```python
class Variant(BaseModel):          # one per size
    sku_id: str
    volume_ml: int | None          # normalized from oz
    price: float | None            # ≥ 0, USD
    image: str | None

class Perfume(BaseModel):
    product_id: str                # stable key (not the URL)
    product_name: str
    brand: str | None
    main_image: str | None
    variants: list[Variant]
    currency: str                  # ISO 4217
    description: str | None
    ingredients_text: str | None
    source_url: str
    fetched_at: datetime
    data_quality_flags: list[str]
```

### MongoDB collections

| Collection | Contents |
|---|---|
| `products` | Validated products, upserted on `product_id`, with `first_seen_at`, `last_updated_at` and `pipeline_run_id` |
| `quarantine_products` | Rejected records with `hard_fail_reasons` and the full Pydantic error |
| `raw_products` | Raw HTML per URL per run (the recovery path) |
| `pipeline_runs` | `run_id`, start/end time, fetched, written and quarantined counts |

---

## Quick start

**Requirements:** Docker and Docker Compose. Nothing else is needed on the host.

```bash
cp .env_example .env
docker compose up --build -d
```

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 (credentials are `_AIRFLOW_WWW_USER_*` in `.env`) |
| MongoDB | `localhost:27017` |
| Flower (optional) | http://localhost:5555 (start it with `docker compose --profile flower up -d`) |

In the Airflow UI, open the **`ulta_products_sync`** DAG and click **Trigger**. The DAG runs on demand only.

### Inspect the results

```bash
./export_mongo_results.sh     # writes mongo_results.json
```

The script exports collection counts, the latest run's metadata, sample products and a breakdown of quarantine reasons. For example, it answers *"how many records were quarantined in the last run, and why?"*

---

## Configuration

All settings come from environment variables in `.env`.

| Variable | Default | Description |
|---|---|---|
| `ULTA_BASE_URL` | `https://www.ulta.com/shop/fragrance/all` | Listing page to crawl |
| `ULTA_MAX_PAGES` | `500` | Maximum number of listing pages to paginate |
| `ULTA_DETAIL_BATCH_SIZE` | `100` | Products per parallel fetch task |
| `ULTA_ARTIFACT_DIR` | `/opt/airflow/include/ulta_pipeline` | Per-run JSONL artifacts |
| `ULTA_MONGO_URI` / `MONGO_URI` | — | Full MongoDB URI (overrides the values below) |
| `MONGO_ROOT_USER` / `MONGO_ROOT_PASSWORD` | — | MongoDB credentials |
| `MONGO_HOST` / `MONGO_PORT` / `MONGO_DB` | `mongodb` / `27017` / `parfum` | Connection target |

---

## Local development

```bash
uv sync

uv run pytest tests/unit/test_models.py -v   # schema tests, no network
uv run pytest tests/ -v                      # includes live scraper tests
```

### Project layout

```
dags/pipeline.py      Airflow DAG: tasks, retries, fan-out, Mongo writes
src/ulta.py           Scraping and parsing (listing cards, Apollo state, JSON-LD)
src/models.py         Pydantic models (Variant, Perfume)
tests/unit/           Model validation (offline fixtures) and live scraper tests
tests/dags/           DAG import and integrity check
docker-compose.yaml   Airflow (Celery), Postgres, Redis, Flower, MongoDB
```

---

## What I'd improve next

- **Bundle filtering.** Detect gift sets, kits and multi-piece bundles, and exclude them before validation.
- **Change detection.** Compare each run against the previous one and log what was added, removed or changed in price.
- **Better retries and politeness.** Use exponential backoff with jitter, rotate user agents, and respect `robots.txt` crawl delays.
- **Bulk writes.** Replace per-document `update_one` calls with `bulk_write` to speed up loading.
- **Enrichment.** Classify fragrance concentration (EDP, EDT, Parfum) with an LLM using structured output. Low-confidence results would go to a human review queue.
- **Scheduling and alerts.** Run the DAG on a schedule and send failure notifications to Slack or email.
