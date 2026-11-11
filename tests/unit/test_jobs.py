import os
from pathlib import Path
import json

from src import process_page, get_product_details

config = json.load(open("config.json"))
WRITE_TO_FILE = True
OUTPUT_DIR = Path(__file__).parent / "output"
URL_BASE = config["ulta_base_url"]

class TestUltaActions:
    def test_get_first_page(self):
        products = process_page(URL_BASE, 1)
        assert products is not None
        if WRITE_TO_FILE:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            with open(OUTPUT_DIR / "test_first_page.json", "w+", encoding="utf-8") as f:
                json.dump(products, f, indent=2)

    def test_get_third_page(self):
        products = process_page(URL_BASE, 3)
        assert products is not None
        if WRITE_TO_FILE:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            with open(OUTPUT_DIR / "test_third_page.json", "w+", encoding="utf-8") as f:
                json.dump(products, f, indent=2)
    
    def test_get_product_details(self):
        products = process_page(URL_BASE, 1)
        assert products is not None
        target = None
        for product in products:
            if len(product["price"].split("$")) > 2:
                target = product
                break
        assert target is not None
        details = get_product_details(target["source_url"])
        assert details is not None
        variants = details.get("variants", [])
        assert len(variants) > 1, "expected multiple size variants"
        assert all(
            isinstance(v.get("sku_id"), str) for v in variants
        ), "every variant must have a sku_id"
        assert any(
            v.get("price") is not None for v in variants
        ), "at least one variant must have a price"
        if WRITE_TO_FILE:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            with open(OUTPUT_DIR / "test_product_details.json", "w+", encoding="utf-8") as f:
                json.dump(details, f, indent=2)