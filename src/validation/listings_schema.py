from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
import pandera.pandas as pa

Check = pa.Check
Column = pa.Column
DataFrameSchema = pa.DataFrameSchema


CURRENT_YEAR = datetime.now().year
UI_GARBAGE_VALUES = {
    "Подробнее",
    "Пожаловаться",
    "Сравнить",
    "На карте",
    "Показать телефон",
    "Подписаться на дом",
}


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _is_empty_or_str(value: Any) -> bool:
    return _is_empty(value) or isinstance(value, str)


def _has_no_ui_garbage(value: Any) -> bool:
    if _is_empty(value):
        return True
    if not isinstance(value, str):
        return False
    return not any(garbage.lower() in value.lower() for garbage in UI_GARBAGE_VALUES)


def _is_list_or_empty(value: Any) -> bool:
    return _is_empty(value) or isinstance(value, list)


def _is_optional_string_list(value: Any) -> bool:
    if not _is_list_or_empty(value):
        return False
    if _is_empty(value):
        return True
    return all(isinstance(item, str) for item in value)


def _is_optional_minio_path_list(value: Any) -> bool:
    if not _is_optional_string_list(value):
        return False
    if _is_empty(value):
        return True
    return all(item.startswith("minio://cian-photos/offers/") for item in value)


def _optional_str_column(max_length: int, min_length: int = 0) -> Column:
    checks = [
        Check(lambda s: s.map(_is_empty_or_str), element_wise=False, error="must be a string or empty"),
        Check(lambda s: s.map(_has_no_ui_garbage), element_wise=False, error="contains UI garbage"),
        Check(
            lambda s: s.map(lambda value: _is_empty(value) or min_length <= len(str(value)) <= max_length),
            element_wise=False,
            error=f"length must be between {min_length} and {max_length}",
        ),
    ]
    return Column(None, checks=checks, nullable=True, required=False)


def _nullable_number_column(dtype: pa.DataType, min_value: float, max_value: float) -> Column:
    column_dtype = dtype
    if dtype is int:
        column_dtype = float
        checks = [
            Check.in_range(min_value=min_value, max_value=max_value),
            Check(lambda s: s.dropna().map(float.is_integer), element_wise=False, error="must be integer-like"),
        ]
    else:
        checks = [Check.in_range(min_value=min_value, max_value=max_value)]

    return Column(
        column_dtype,
        checks=checks,
        nullable=True,
        required=False,
        coerce=True,
    )


raw_listings_schema = DataFrameSchema(
    {
        "offer_id": Column(str, nullable=False, required=True, coerce=True),
        "url": Column(str, nullable=False, required=True, coerce=True),
        "source_job": _optional_str_column(max_length=120),
        "parsed_at": Column(str, nullable=True, required=False, coerce=True),
        "price_rub": Column(
            int,
            checks=Check.in_range(min_value=2_000_000, max_value=500_000_000),
            nullable=False,
            required=True,
            coerce=True,
        ),
        "area_m2": Column(
            float,
            checks=Check.in_range(min_value=12, max_value=500),
            nullable=False,
            required=True,
            coerce=True,
        ),
        "floor": _nullable_number_column(int, 1, 100),
        "floors_total": _nullable_number_column(int, 1, 100),
        "address": _optional_str_column(max_length=300),
        "build_year": _nullable_number_column(int, 1900, CURRENT_YEAR),
        "rooms": Column(
            float,
            checks=[
                Check.in_range(min_value=0, max_value=8),
                Check(lambda s: s.dropna().map(float.is_integer), element_wise=False, error="must be integer-like"),
            ],
            nullable=True,
            required=True,
            coerce=True,
        ),
        "housing_type": _optional_str_column(max_length=80),
        "residential_complex": _optional_str_column(max_length=150, min_length=2),
        "year_completion": _nullable_number_column(int, 2020, CURRENT_YEAR + 5),
        "building_status": _optional_str_column(max_length=80),
        "district": _optional_str_column(max_length=80),
        "settlement": _optional_str_column(max_length=120),
        "highway": _optional_str_column(max_length=120),
        "distance_to_mkad_km": _nullable_number_column(int, 0, 80),
        "metro_station": Column(
            None,
            checks=[
                Check(lambda s: s.map(_is_empty_or_str), element_wise=False, error="must be a string or empty"),
                Check(lambda s: s.map(_has_no_ui_garbage), element_wise=False, error="contains UI garbage"),
                Check(
                    lambda s: s.map(lambda value: _is_empty(value) or len(str(value)) <= 80),
                    element_wise=False,
                    error="length must be <= 80",
                ),
                Check(
                    lambda s: s.map(lambda value: _is_empty(value) or not any(char.isdigit() for char in str(value))),
                    element_wise=False,
                    error="must not contain digits",
                ),
            ],
            nullable=True,
            required=False,
        ),
        "metro_time_min": _nullable_number_column(int, 1, 90),
        "lifts_info": _optional_str_column(max_length=100),
        "house_type": _optional_str_column(max_length=100),
        "parking": _optional_str_column(max_length=100),
        "complex_type": _optional_str_column(max_length=100),
        "image_urls": Column(
            None,
            checks=Check(lambda s: s.map(_is_optional_string_list), element_wise=False),
            nullable=True,
            required=False,
        ),
        "image_paths": Column(
            None,
            checks=Check(lambda s: s.map(_is_optional_string_list), element_wise=False),
            nullable=True,
            required=False,
        ),
    },
    checks=[
        Check(
            lambda df: df["floor"].isna()
            | df["floors_total"].isna()
            | (df["floor"] <= df["floors_total"]),
            error="floor must be less than or equal to floors_total",
        ),
        Check(
            lambda df: df["floor"].isna() == df["floors_total"].isna(),
            error="floor and floors_total must be filled together",
        ),
        Check(
            lambda df: (df["price_rub"] / df["area_m2"]) <= 1_500_000,
            error="price_rub / area_m2 must be <= 1_500_000",
        ),
    ],
    strict=False,
)


photo_columns = raw_listings_schema.columns.copy()
photo_columns["image_paths"] = Column(
    None,
    checks=[
        Check(lambda s: s.map(_is_optional_minio_path_list), element_wise=False),
        Check(lambda s: s.map(lambda value: isinstance(value, list) and len(value) >= 1), element_wise=False),
    ],
    nullable=False,
    required=True,
)

photo_listings_schema = DataFrameSchema(
    photo_columns,
    checks=raw_listings_schema.checks,
    strict=raw_listings_schema.strict,
)
