import pytest

from src.ulta import _extract_variant_image, _normalize_image, _oz_to_ml


@pytest.mark.parametrize(
    "value, expected",
    [
        ("https://img/a.jpg", "https://img/a.jpg"),
        ("   ", None),
        (None, None),
        (["https://img/a.jpg", "https://img/b.jpg"], "https://img/a.jpg"),  # JSON-LD lists
        (["", {"url": "https://img/c.jpg"}], "https://img/c.jpg"),
        ({"src": "https://img/d.jpg"}, "https://img/d.jpg"),
        ([], None),
        (42, None),
    ],
)
def test_normalize_image_always_returns_single_url_or_none(value, expected):
    assert _normalize_image(value) == expected


def test_extract_variant_image_prefers_first_available_key():
    variant = {"imageUrl": None, "image": ["https://img/list.jpg"], "thumbnail": "https://img/t.jpg"}
    assert _extract_variant_image(variant) == "https://img/list.jpg"


def test_oz_to_ml_handles_missing_size():
    assert _oz_to_ml(None) is None
