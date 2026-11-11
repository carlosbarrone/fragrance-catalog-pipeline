from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4
from urllib.parse import urljoin

from airflow.sdk import dag, task
from pymongo import MongoClient

from src import parse_product_details_html, process_page, read_page
from src.models import Perfume

DEFAULT_BASE_URL = "https://www.ulta.com/shop/fragrance/all"
DEFAULT_MAX_PAGES = 500
DEFAULT_ARTIFACT_DIR = "/opt/airflow/include/ulta_pipeline"
ISO_4217_RE = re.compile(r"^[A-Z]{3}$")

logger = logging.getLogger(__name__)


def _mongo_uri() -> str:
    if os.getenv("ULTA_MONGO_URI"):
        return os.getenv("ULTA_MONGO_URI", "")
    if os.getenv("MONGO_URI"):
        return os.getenv("MONGO_URI", "")
    if os.getenv("AIRFLOW_CONN_MONGO_DEFAULT"):
        return os.getenv("AIRFLOW_CONN_MONGO_DEFAULT", "")

    user = os.getenv("MONGO_ROOT_USER", "mongoadmin")
    password = os.getenv("MONGO_ROOT_PASSWORD", "change_me_mongo_password")
    host = os.getenv("MONGO_HOST", "mongodb")
    port = os.getenv("MONGO_PORT", "27017")
    db_name = os.getenv("MONGO_DB", "parfum")
    return f"mongodb://{user}:{password}@{host}:{port}/{db_name}?authSource=admin"


def _mongo_db_name() -> str:
    return os.getenv("MONGO_DB", "parfum")


def _run_dir(run_id: str) -> Path:
    artifact_root = Path(os.getenv("ULTA_ARTIFACT_DIR", DEFAULT_ARTIFACT_DIR))
    run_path = artifact_root / run_id
    run_path.mkdir(parents=True, exist_ok=True)
    return run_path


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, default=str))
            f.write("\n")


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _normalize_currency(value: str | None) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().upper()


def _infer_brand(product_name: str | None) -> str | None:
    if not isinstance(product_name, str):
        return None
    cleaned = product_name.strip()
    if not cleaned:
        return None
    token = cleaned.split(" ")[0].strip()
    return token if token else None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


@dag(
    dag_id="ulta_products_sync",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ulta", "scraper"],
)
def ulta_products_sync():
    @task(retries=2, retry_delay=timedelta(seconds=30))
    def resolve_base_url() -> str:
        return os.getenv("ULTA_BASE_URL", DEFAULT_BASE_URL)

    @task(retries=2, retry_delay=timedelta(seconds=30))
    def collect_listing_products(base_url: str) -> dict:
        max_pages = int(os.getenv("ULTA_MAX_PAGES", str(DEFAULT_MAX_PAGES)))
        run_id = str(uuid4())
        started_at = datetime.now(timezone.utc)
        listing_rows: list[dict] = []

        logger.info("Collecting listing pages run_id=%s base_url=%s", run_id, base_url)
        for page in range(1, max_pages + 1):
            products = process_page(base_url, page)
            if not products:
                logger.info("Stopping listing collection at page %s: no items", page)
                break

            logger.info("Fetched page %s with %s items", page, len(products))
            for product in products:
                source_url = product.get("source_url")
                if source_url:
                    listing_rows.append(
                        {
                            "source_url": urljoin(base_url, source_url),
                            "product_id": product.get("product_id"),
                            "product_name": product.get("product_name"),
                            "brand": product.get("brand"),
                            "page": page,
                        }
                    )

        deduped_by_url: dict[str, dict] = {}
        for row in listing_rows:
            source_url = row.get("source_url")
            if source_url:
                deduped_by_url[source_url] = row

        deduped_rows = list(deduped_by_url.values())
        run_path = _run_dir(run_id)
        listing_path = run_path / "listing_products.jsonl"
        _write_jsonl(listing_path, deduped_rows)

        logger.info("Collected %s unique product URLs", len(deduped_rows))

        return {
            "run_id": run_id,
            "started_at": started_at.isoformat(),
            "listing_path": str(listing_path),
            "total_listed": len(deduped_rows),
        }

    @task()
    def build_detail_batches(listing_result: dict) -> list[dict]:
        run_id = listing_result["run_id"]
        started_at = listing_result["started_at"]
        run_path = _run_dir(run_id)

        batch_size = max(1, int(os.getenv("ULTA_DETAIL_BATCH_SIZE", "100")))
        listing_rows = _read_jsonl(Path(listing_result["listing_path"]))

        batch_specs: list[dict] = []
        for index in range(0, len(listing_rows), batch_size):
            chunk = listing_rows[index:index + batch_size]
            batch_index = len(batch_specs)
            batch_path = run_path / f"listing_batch_{batch_index}.jsonl"
            _write_jsonl(batch_path, chunk)
            batch_specs.append(
                {
                    "run_id": run_id,
                    "batch_index": batch_index,
                    "batch_path": str(batch_path),
                }
            )

        logger.info(
            "Prepared %s detail batches (batch_size=%s) from %s URLs",
            len(batch_specs),
            batch_size,
            len(listing_rows),
        )

        return batch_specs

    @task(retries=2, retry_delay=timedelta(seconds=30), max_active_tis_per_dag=10)
    def fetch_details_batch(batch_spec: dict) -> dict:
        run_id = batch_spec["run_id"]
        batch_index = batch_spec["batch_index"]
        batch_rows = _read_jsonl(Path(batch_spec["batch_path"]))

        raw_rows: list[dict] = []
        fetched_rows: list[dict] = []
        success = 0
        failures = 0

        for listing in batch_rows:
            product_url = listing.get("source_url")
            if not product_url:
                continue

            try:
                html = read_page(product_url)
                fetched_at = datetime.now(timezone.utc)

                raw_rows.append(
                    {
                        "pipeline_run_id": run_id,
                        "source_url": product_url,
                        "fetched_at": fetched_at.isoformat(),
                        "raw_html": html,
                    }
                )

                details = parse_product_details_html(product_url, html, expand_variant_pages=True)
                fetched_rows.append(
                    {
                        "product_id": listing.get("product_id"),
                        "product_name": listing.get("product_name"),
                        "brand": listing.get("brand"),
                        "main_image": details.get("main_image"),
                        "variants": details.get("variants", []),
                        "currency": _normalize_currency(details.get("currency")),
                        "description": details.get("description"),
                        "ingredients_text": details.get("ingredients_text"),
                        "source_url": product_url,
                        "fetched_at": fetched_at.isoformat(),
                        "pipeline_run_id": run_id,
                        "data_quality_flags": [],
                    }
                )
                success += 1
            except Exception as exc:
                failures += 1
                logger.error(
                    "Batch %s failed url=%s error=%s",
                    batch_index,
                    product_url,
                    exc,
                )

        run_path = _run_dir(run_id)
        fetched_path = run_path / f"fetched_batch_{batch_index}.jsonl"
        raw_path = run_path / f"raw_batch_{batch_index}.jsonl"
        _write_jsonl(fetched_path, fetched_rows)
        _write_jsonl(raw_path, raw_rows)

        logger.info(
            "Batch %s done total=%s success=%s failures=%s",
            batch_index,
            len(batch_rows),
            success,
            failures,
        )

        return {
            "batch_index": batch_index,
            "fetched_path": str(fetched_path),
            "raw_path": str(raw_path),
            "success": success,
            "failures": failures,
        }

    @task()
    def consolidate_fetched_batches(listing_result: dict, batch_summaries: list[dict]) -> dict:
        run_id = listing_result["run_id"]
        started_at = listing_result["started_at"]

        fetched_rows: list[dict] = []
        raw_rows: list[dict] = []
        total_success = 0
        total_failures = 0

        for summary in batch_summaries:
            if not summary:
                continue
            total_success += int(summary.get("success", 0))
            total_failures += int(summary.get("failures", 0))
            fetched_rows.extend(_read_jsonl(Path(summary["fetched_path"])))
            raw_rows.extend(_read_jsonl(Path(summary["raw_path"])))

        run_path = _run_dir(run_id)
        fetched_path = run_path / "fetched_products.jsonl"
        raw_path = run_path / "raw_products.jsonl"
        _write_jsonl(fetched_path, fetched_rows)
        _write_jsonl(raw_path, raw_rows)

        logger.info(
            "Consolidated detail batches success=%s failures=%s fetched_rows=%s",
            total_success,
            total_failures,
            len(fetched_rows),
        )

        return {
            "run_id": run_id,
            "started_at": started_at,
            "total_fetched": len(fetched_rows),
            "fetched_path": str(fetched_path),
            "raw_path": str(raw_path),
        }

    @task()
    def validate_products(fetch_result: dict) -> dict:
        run_id = fetch_result["run_id"]
        fetched_path = Path(fetch_result["fetched_path"])
        run_path = _run_dir(run_id)
        validated_path = run_path / "validated_products.jsonl"
        quarantine_path = run_path / "quarantine_products.jsonl"

        rows = _read_jsonl(fetched_path)
        validated_rows: list[dict] = []
        quarantine_rows: list[dict] = []
        reason_counts: dict[str, int] = {}

        for doc in rows:
            flags: list[str] = list(doc.get("data_quality_flags", []))
            hard_fail_reasons: list[str] = []

            # Extract metadata fields that shouldn't be in the Perfume model
            pipeline_run_id_meta = doc.pop("pipeline_run_id", None)
            fetched_at_meta = doc.pop("fetched_at", None)

            product_id = doc.get("product_id")
            if not isinstance(product_id, str) or not product_id.strip():
                hard_fail_reasons.append("missing_product_id")

            product_name = doc.get("product_name")
            if not isinstance(product_name, str) or not product_name.strip():
                hard_fail_reasons.append("missing_product_name")

            if not doc.get("brand"):
                inferred_brand = _infer_brand(product_name)
                if inferred_brand:
                    doc["brand"] = inferred_brand
                    flags.append("missing_brand_inferred_from_product_name")
                else:
                    flags.append("missing_brand")

            variants = doc.get("variants")
            if not isinstance(variants, list) or len(variants) == 0:
                hard_fail_reasons.append("empty_variants")

            currency = _normalize_currency(doc.get("currency"))
            doc["currency"] = currency
            if not ISO_4217_RE.match(currency):
                hard_fail_reasons.append("invalid_currency")
            try:
                if isinstance(fetched_at_meta, str):
                    fetched_at_parsed = datetime.fromisoformat(fetched_at_meta.replace("Z", "+00:00"))
                else:
                    fetched_at_parsed = fetched_at_meta
            except (ValueError, AttributeError):
                fetched_at_parsed = datetime.now(timezone.utc)

            doc["fetched_at"] = fetched_at_parsed
            doc["data_quality_flags"] = flags

            validation_errors: list[str] = []
            try:
                validated = Perfume.model_validate(doc, strict=True)
            except Exception as exc:
                validation_errors.append(str(exc))
                hard_fail_reasons.append("pydantic_validation_error")
                validated = None

            if hard_fail_reasons:
                for reason in hard_fail_reasons:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
                quarantine_rows.append(
                    {
                        "pipeline_run_id": run_id,
                        "quarantined_at": datetime.now(timezone.utc).isoformat(),
                        "hard_fail_reasons": hard_fail_reasons,
                        "validation_errors": validation_errors,
                        "document": doc,
                    }
                )
                logger.error(
                    "Quarantined product product_id=%s reasons=%s",
                    doc.get("product_id"),
                    hard_fail_reasons,
                )
                continue

            if flags:
                logger.warning(
                    "Soft validation warnings product_id=%s flags=%s",
                    doc.get("product_id"),
                    flags,
                )

            # Add metadata back to validated product
            validated_dict = validated.model_dump(mode="json")
            validated_dict["pipeline_run_id"] = pipeline_run_id_meta
            validated_rows.append(validated_dict)

        _write_jsonl(validated_path, validated_rows)
        _write_jsonl(quarantine_path, quarantine_rows)

        return {
            "run_id": run_id,
            "started_at": fetch_result["started_at"],
            "total_fetched": fetch_result["total_fetched"],
            "validated_path": str(validated_path),
            "quarantine_path": str(quarantine_path),
            "raw_path": fetch_result["raw_path"],
            "total_written_candidates": len(validated_rows),
            "total_quarantined": len(quarantine_rows),
            "quarantine_reason_counts": reason_counts,
        }

    @task()
    def write_to_mongo(validation_result: dict) -> dict:
        run_id = validation_result["run_id"]
        started_at = datetime.fromisoformat(validation_result["started_at"])
        completed_at = datetime.now(timezone.utc)

        validated_rows = _read_jsonl(Path(validation_result["validated_path"]))
        quarantine_rows = _read_jsonl(Path(validation_result["quarantine_path"]))
        raw_rows = _read_jsonl(Path(validation_result["raw_path"]))

        db_name = _mongo_db_name()
        client = MongoClient(_mongo_uri())
        db = client[db_name]

        products_collection = db["products"]
        quarantine_collection = db["quarantine_products"]
        raw_collection = db["raw_products"]
        runs_collection = db["pipeline_runs"]

        total_written = 0

        if raw_rows:
            for raw_doc in raw_rows:
                parsed_fetched_at = _parse_iso_datetime(raw_doc.get("fetched_at"))
                if parsed_fetched_at:
                    raw_doc["fetched_at"] = parsed_fetched_at
            raw_collection.insert_many(raw_rows)

        if quarantine_rows:
            for quarantine_doc in quarantine_rows:
                parsed_quarantined_at = _parse_iso_datetime(quarantine_doc.get("quarantined_at"))
                if parsed_quarantined_at:
                    quarantine_doc["quarantined_at"] = parsed_quarantined_at
            quarantine_collection.insert_many(quarantine_rows)

        now_utc = datetime.now(timezone.utc)
        for doc in validated_rows:
            product_id = doc["product_id"]
            parsed_fetched_at = _parse_iso_datetime(doc.get("fetched_at"))
            if parsed_fetched_at:
                doc["fetched_at"] = parsed_fetched_at
            update_doc = {
                "$set": {
                    **doc,
                    "pipeline_run_id": run_id,
                    "last_updated_at": now_utc,
                },
                "$setOnInsert": {
                    "first_seen_at": now_utc,
                },
            }
            products_collection.update_one({"product_id": product_id}, update_doc, upsert=True)
            total_written += 1

        run_doc = {
            "run_id": run_id,
            "started_at": started_at,
            "completed_at": completed_at,
            "total_fetched": validation_result["total_fetched"],
            "total_written": total_written,
            "total_quarantined": len(quarantine_rows),
            "quarantine_reason_counts": validation_result.get("quarantine_reason_counts", {}),
        }
        runs_collection.update_one({"run_id": run_id}, {"$set": run_doc}, upsert=True)
        client.close()

        logger.info("Run completed metadata=%s", run_doc)

        return run_doc

    base_url = resolve_base_url()
    listing_result = collect_listing_products(base_url)
    batch_specs = build_detail_batches(listing_result)
    fetched_batches = fetch_details_batch.expand(batch_spec=batch_specs)
    fetched_batches_output = getattr(fetched_batches, "output", fetched_batches)
    fetched = consolidate_fetched_batches(listing_result, fetched_batches_output)
    validated = validate_products(fetched)
    write_to_mongo(validated)


ulta_products_sync()
