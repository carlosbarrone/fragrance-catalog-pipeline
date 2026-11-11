import json
import re
import requests
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from bs4 import BeautifulSoup

def read_page(url: str) -> str:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text

def get_product_cards(soup: BeautifulSoup) -> list[BeautifulSoup]:
    items = soup.find_all("div", {"class": "ProductCard"})
    return items

def _clean(text: str | None) -> str | None:
    return " ".join(text.split()) if text else None

def _oz_to_ml(size_str: str | None) -> int | None:
    if not isinstance(size_str, str):
        return None
    m = re.search(r"([\d.]+)\s*oz", size_str, re.IGNORECASE)
    return round(float(m.group(1)) * 29.5735) if m else None


def _parse_price_value(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace("$", "").replace(",", "")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _extract_variant_image(variant: dict) -> str | None:
    for key in ("imageUrl", "image", "thumbnail", "swatchImage"):
        image_val = variant.get(key)
        if isinstance(image_val, str) and image_val.strip():
            return image_val
        if isinstance(image_val, dict):
            for nested_key in ("url", "src", "href"):
                nested_val = image_val.get(nested_key)
                if isinstance(nested_val, str) and nested_val.strip():
                    return nested_val
    return None

def parse_product_card(card: BeautifulSoup, page: int) -> dict:
    item_id = card.get("data-item-id")
    link = card.find("a", class_="pal-c-Link--absolute")
    url = link["href"] if link else None
    brand_div = card.find("div", class_="pal-c-ProductCardBody--brandName")
    brand = _clean(brand_div.find("p").get_text()) if brand_div else None
    name_tag = card.find("p", class_=lambda c: c and "pal-c-Text__style--label" in c and "pal-c-Text__style--label-sm" not in c)
    name = _clean(name_tag.get_text()) if name_tag else None
    rating, review_count = None, None
    sr_only = card.find("span", class_="sr-only")
    if sr_only:
        text = sr_only.get_text()
        m = re.search(r"([\d.]+)\s+out of\s+[\d.]+\s+stars\s*[;,]\s*([\d,]+)\s+reviews?", text)
        if m:
            rating = float(m.group(1))
            review_count = int(m.group(2).replace(",", ""))

    price_container = card.find("div", class_="pal-c-Price__priceContainer")
    price = _clean(price_container.get_text()) if price_container else None

    variant_span = card.find("div", class_="pal-c-ProductCardHeader__variant")
    variants = _clean(variant_span.get_text()) if variant_span else None

    return {
        "product_id": item_id,
        "source_url": url,
        "brand": brand,
        "product_name": name,
        "rating": rating,
        "review_count": review_count,
        "price": price,
        "variants": variants,
        "page": page
    }


def _with_page(url: str, page: int) -> str:
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    query["page"] = [str(page)]
    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))


def process_page(url: str, page: int) -> list[dict]:
    html = read_page(_with_page(url, page))
    cards = get_product_cards(BeautifulSoup(html, "html.parser"))
    return [parse_product_card(card, page) for card in cards]


def parse_product_details_html(url: str, html: str, *, expand_variant_pages: bool = False) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    image_link = soup.find("link", {"rel": "preload", "as": "image"})
    main_image = image_link.get("href") if image_link else None

    square = soup.find("square-placement")
    currency = square.get("data-currency") if square else None

    summary_div = soup.find("div", class_="ProductSummary__summary")
    description = None
    if summary_div:
        summary_p = summary_div.find("p")
        description = _clean(summary_p.get_text()) if summary_p else _clean(summary_div.get_text())

    ingredients_text = None
    ingredients_btn = soup.find("button", {"aria-label": "Ingredients"})
    if ingredients_btn:
        accordion = ingredients_btn.find_parent("div", class_="pal-c-Accordion__header")
        if accordion:
            body = accordion.find_next_sibling("section", class_="pal-c-Accordion__body")
            if body:
                ingredients_text = _clean(body.get_text())

    current_ld = _extract_variant_from_json_ld(soup)
    apollo_variants = _get_variants_from_apollo(soup)
    image_by_sku: dict[str, str | None] = {}
    for av in apollo_variants:
        sku = str(av.get("skuId") or "")
        img = _extract_variant_image(av)
        if sku and img:
            image_by_sku[sku] = img
    if current_ld and current_ld.get("sku_id") and current_ld.get("image"):
        image_by_sku[str(current_ld["sku_id"])] = current_ld["image"]

    size_prices = parse_size_prices(html, fetch_missing=expand_variant_pages)
    variants: list[dict] = [
        {
            "sku_id": sp["sku_id"],
            "volume_ml": sp["volume_ml"],
            "price": sp["price"],
            "image": image_by_sku.get(sp["sku_id"]),
        }
        for sp in size_prices
    ]

    if not variants and current_ld:
        variants = [current_ld]

    return {
        "main_image": main_image,
        "currency": currency,
        "description": description,
        "ingredients_text": ingredients_text,
        "variants": [v for v in variants if isinstance(v, dict) and v.get("sku_id")],
        "source_url": url,
    }


def get_product_details(url: str) -> dict:
    html = read_page(url)
    return parse_product_details_html(url, html, expand_variant_pages=True)


def _extract_variant_from_json_ld(soup: BeautifulSoup) -> dict | None:
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            ld = json.loads(tag.string)
        except (json.JSONDecodeError, AttributeError):
            continue
        if not isinstance(ld, dict):
            continue
        if ld.get("@type") != "Product":
            continue
        sku_id = ld.get("sku")
        if not sku_id:
            continue
        offer = ld.get("offers", {})
        if isinstance(offer, list):
            offer = next((item for item in offer if isinstance(item, dict)), {})
        if not isinstance(offer, dict):
            offer = {}
        price_str = offer.get("price")
        parsed_price = None
        if price_str is not None:
            try:
                parsed_price = float(price_str)
            except (TypeError, ValueError):
                parsed_price = None
        return {
            "sku_id": sku_id,
            "volume_ml": _oz_to_ml(ld.get("size")),
            "price": parsed_price,
            "image": ld.get("image"),
        }
    return None


def _get_variants_from_apollo(soup: BeautifulSoup) -> list[dict]:
    script = soup.find("script", string=re.compile(r"__APOLLO_STATE__"))
    if not script or not script.string:
        return []

    text = script.string
    idx = text.find("__APOLLO_STATE__ = ")
    if idx < 0:
        return []
    
    start = text.index("{", idx)
    depth, end = 0, start
    for i, ch in enumerate(text[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    try:
        data = json.loads(text[start:end])
    except json.JSONDecodeError:
        return []

    rq = data.get("ROOT_QUERY", {})
    page_key = next((k for k in rq if k.startswith("Page(")), None)
    if not page_key:
        return []

    content = rq[page_key].get("content", {})

    def _find_variant_module(obj):
        if isinstance(obj, dict):
            if obj.get("type") == "ProductVariant" and isinstance(obj.get("variants"), list):
                return obj
            for v in obj.values():
                result = _find_variant_module(v)
                if result:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = _find_variant_module(item)
                if result:
                    return result
        return None

    vm = _find_variant_module(content)
    if not vm:
        return []

    result = []
    for v in vm["variants"]:
        sku_id = v.get("skuId")
        if not sku_id:
            continue
        result.append({
            "skuId": str(sku_id),
            "name": v.get("name", ""),
            "url": (v.get("linkSelectAction") or {}).get("url"),
            "selected": v.get("selected", False),
            "price": v.get("price"),
            "salePrice": v.get("salePrice"),
            "finalSalePrice": v.get("finalSalePrice"),
            "amount": v.get("amount"),
            "image": v.get("image"),
            "imageUrl": v.get("imageUrl"),
        })
    return result


def parse_size_prices(html: str, *, fetch_missing: bool = False) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", string=re.compile(r"__APOLLO_STATE__"))
    if not script or not script.string:
        return []

    text = script.string
    idx = text.find("__APOLLO_STATE__ = ")
    if idx < 0:
        return []

    start = text.index("{", idx)
    depth, end = 0, start
    for i, ch in enumerate(text[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    try:
        data = json.loads(text[start:end])
    except json.JSONDecodeError:
        return []

    rq = data.get("ROOT_QUERY", {})
    page_key = next((k for k in rq if k.startswith("Page(")), None)
    if not page_key:
        return []

    content = rq[page_key].get("content", {})

    def _find_variant_module(obj):
        if isinstance(obj, dict):
            if obj.get("type") == "ProductVariant" and isinstance(obj.get("variants"), list):
                return obj
            for v in obj.values():
                result = _find_variant_module(v)
                if result:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = _find_variant_module(item)
                if result:
                    return result
        return None

    vm = _find_variant_module(content)
    if not vm:
        return []

    results: list[dict] = []
    for v in vm["variants"]:
        sku_id = str(v.get("skuId") or "")
        if not sku_id:
            continue

        size_label: str = v.get("name") or ""
        url: str | None = (v.get("linkSelectAction") or {}).get("url")
        price: float | None = _parse_price_value(v.get("listPrice")) or _parse_price_value(v.get("salePrice"))

        if price is None and fetch_missing and url:
            try:
                vld = _extract_variant_from_json_ld(
                    BeautifulSoup(read_page(url), "html.parser")
                )
                if vld:
                    price = vld.get("price")
            except Exception:
                pass

        results.append(
            {
                "sku_id": sku_id,
                "size_label": size_label,
                "volume_ml": _oz_to_ml(size_label),
                "price": price,
                "url": url,
            }
        )

    return results