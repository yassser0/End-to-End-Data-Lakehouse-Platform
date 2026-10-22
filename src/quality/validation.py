"""
Reusable data validation functions.
Pure Python functions that can be called both from Spark pipelines
and from unit tests without requiring a SparkSession.
"""
from __future__ import annotations

from typing import Any

# ─── Schema checks ────────────────────────────────────────────────────────────


def check_required_columns(
    columns: list[str], required: list[str]
) -> list[str]:
    """
    Return a list of missing column names.
    Empty list means all required columns are present.
    """
    return [c for c in required if c not in columns]


# ─── Null checks ──────────────────────────────────────────────────────────────


def is_valid_identifier(value: Any) -> bool:
    """Return True if value is a non-null, non-empty string."""
    if value is None:
        return False
    return str(value).strip() != ""


# ─── Date checks ──────────────────────────────────────────────────────────────


def is_valid_date_string(date_str: Any, fmt: str = "%Y-%m-%d") -> bool:
    """
    Return True if date_str parses correctly with the given format
    and represents a date between 2000-01-01 and today.
    """
    from datetime import date, datetime

    if date_str is None:
        return False
    try:
        d = datetime.strptime(str(date_str), fmt).date()
    except ValueError:
        return False
    return date(2000, 1, 1) <= d <= date.today()


# ─── Numeric checks ───────────────────────────────────────────────────────────


def is_positive_number(value: Any) -> bool:
    """Return True if value is a number greater than zero."""
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


# ─── Status checks ────────────────────────────────────────────────────────────


VALID_ORDER_STATUSES = frozenset({"pending", "paid", "cancelled", "refunded"})
VALID_PAYMENT_STATUSES = frozenset({"unpaid", "paid", "failed", "refunded"})


def is_valid_order_status(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in VALID_ORDER_STATUSES


def is_valid_payment_status(value: Any) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in VALID_PAYMENT_STATUSES


# ─── Referential integrity ────────────────────────────────────────────────────


def check_referential_integrity(
    fk_values: list[Any], valid_ids: set[Any]
) -> list[Any]:
    """
    Return a list of FK values not present in valid_ids.
    Empty list means all references are valid.
    """
    return [v for v in fk_values if v not in valid_ids]


# ─── Row count checks ─────────────────────────────────────────────────────────


def check_row_count_delta(
    source_count: int,
    target_count: int,
    tolerance_pct: float = 10.0,
) -> bool:
    """
    Return True if target_count is within tolerance_pct of source_count.
    Used to detect unexpectedly large drops between layers.
    """
    if source_count == 0:
        return target_count == 0
    delta_pct = abs(target_count - source_count) / source_count * 100
    return delta_pct <= tolerance_pct


# ─── Validate a dict record (for unit tests) ──────────────────────────────────


def validate_order_record(record: dict) -> list[str]:
    """
    Validate a single order record dict.
    Returns a list of validation failure messages (empty = valid).
    Used in unit tests; mirrors the Silver layer business rules.
    """
    errors: list[str] = []

    for field in ("order_id", "customer_id", "product_id"):
        if not is_valid_identifier(record.get(field)):
            errors.append(f"NULL_{field.upper()}")

    if not is_valid_date_string(record.get("order_date")):
        errors.append("INVALID_ORDER_DATE")

    if not is_positive_number(record.get("quantity")):
        errors.append("INVALID_QUANTITY")

    if not is_positive_number(record.get("unit_price")):
        errors.append("INVALID_UNIT_PRICE")

    if not is_valid_order_status(record.get("order_status")):
        errors.append("INVALID_ORDER_STATUS")

    if not is_valid_payment_status(record.get("payment_status")):
        errors.append("INVALID_PAYMENT_STATUS")

    return errors
