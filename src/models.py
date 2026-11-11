from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

class Variant(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    sku_id: str
    volume_ml: int | None = None
    price: float | None = Field(default=None, ge=0)
    image: str | None = None


class Perfume(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    product_id: str = Field(min_length=1)
    product_name: str = Field(min_length=1)
    brand: str | None = None
    main_image: str | None = None
    variants: list[Variant]
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    description: str | None = None
    ingredients_text: str | None = None
    source_url: str
    fetched_at: datetime
    data_quality_flags: list[str] = Field(default_factory=list)

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, value: str | None) -> str:
        if not isinstance(value, str):
            return ""
        return value.strip().upper()