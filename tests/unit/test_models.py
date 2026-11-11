import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.models import Perfume, Variant


FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _load_json(name: str):
    with open(FIXTURE_DIR / name, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_perfume_payload() -> dict:
    first_page = _load_json("listing_page.json")
    details = _load_json("product_details.json")

    listing = first_page[0]
    return {
        "product_id": listing["product_id"],
        "product_name": listing["product_name"],
        "brand": listing["brand"],
        "main_image": details["main_image"],
        "variants": details["variants"],
        "currency": details["currency"],
        "description": details["description"],
        "ingredients_text": details["ingredients_text"],
        "source_url": details["source_url"],
        "fetched_at": datetime.now(timezone.utc),
        "data_quality_flags": [],
    }


def test_variant_required_format():
    details = _load_json("product_details.json")
    variant = Variant.model_validate(details["variants"][0], strict=True)

    dumped = variant.model_dump(mode="json")
    assert list(dumped.keys()) == ["sku_id", "volume_ml", "price", "image"]


def test_perfume_required_format():
    payload = _build_perfume_payload()
    perfume = Perfume.model_validate(payload, strict=True)

    dumped = perfume.model_dump(mode="json")
    assert list(dumped.keys()) == [
        "product_id",
        "product_name",
        "brand",
        "main_image",
        "variants",
        "currency",
        "description",
        "ingredients_text",
        "source_url",
        "fetched_at",
        "data_quality_flags",
    ]


def test_currency_normalizes_to_iso_uppercase():
    payload = _build_perfume_payload()
    payload["currency"] = " usd "

    perfume = Perfume.model_validate(payload, strict=True)
    assert perfume.currency == "USD"


def test_strict_validation_rejects_wrong_types():
    payload = _build_perfume_payload()
    payload["variants"] = [{"sku_id": "123", "volume_ml": 30, "price": "99.0", "image": "x"}]

    with pytest.raises(ValidationError):
        Perfume.model_validate(payload, strict=True)


def test_forbid_extra_fields():
    payload = _build_perfume_payload()
    payload["unexpected"] = "nope"

    with pytest.raises(ValidationError):
        Perfume.model_validate(payload, strict=True)
