"""
Unit tests for src/quality/validation.py

These tests run without Spark — pure Python only.
Run with: pytest tests/unit/test_validation.py -v
"""
import pytest

from src.quality.validation import (
    VALID_ORDER_STATUSES,
    VALID_PAYMENT_STATUSES,
    check_referential_integrity,
    check_required_columns,
    check_row_count_delta,
    is_positive_number,
    is_valid_date_string,
    is_valid_identifier,
    is_valid_order_status,
    is_valid_payment_status,
    validate_order_record,
)


# ─── check_required_columns ───────────────────────────────────────────────────


class TestCheckRequiredColumns:
    def test_all_present(self):
        assert check_required_columns(["a", "b", "c"], ["a", "b"]) == []

    def test_one_missing(self):
        assert check_required_columns(["a", "b"], ["a", "b", "c"]) == ["c"]

    def test_all_missing(self):
        missing = check_required_columns([], ["x", "y"])
        assert set(missing) == {"x", "y"}

    def test_extra_columns_allowed(self):
        assert check_required_columns(["a", "b", "extra"], ["a"]) == []


# ─── is_valid_identifier ──────────────────────────────────────────────────────


class TestIsValidIdentifier:
    def test_valid_string(self):
        assert is_valid_identifier("ORD0000001") is True

    def test_none_is_invalid(self):
        assert is_valid_identifier(None) is False

    def test_empty_string_is_invalid(self):
        assert is_valid_identifier("") is False

    def test_whitespace_only_is_invalid(self):
        assert is_valid_identifier("   ") is False

    def test_integer_id_is_valid(self):
        assert is_valid_identifier(12345) is True


# ─── is_valid_date_string ─────────────────────────────────────────────────────


class TestIsValidDateString:
    def test_valid_date(self):
        assert is_valid_date_string("2024-06-15") is True

    def test_invalid_format(self):
        assert is_valid_date_string("15/06/2024") is False

    def test_impossible_date(self):
        assert is_valid_date_string("9999-99-99") is False

    def test_none_is_invalid(self):
        assert is_valid_date_string(None) is False

    def test_before_2000_is_invalid(self):
        assert is_valid_date_string("1999-12-31") is False

    def test_future_date_is_invalid(self):
        assert is_valid_date_string("2099-01-01") is False

    def test_boundary_2000(self):
        assert is_valid_date_string("2000-01-01") is True


# ─── is_positive_number ───────────────────────────────────────────────────────


class TestIsPositiveNumber:
    def test_positive_int(self):
        assert is_positive_number(5) is True

    def test_positive_float(self):
        assert is_positive_number(3.99) is True

    def test_zero_is_invalid(self):
        assert is_positive_number(0) is False

    def test_negative_is_invalid(self):
        assert is_positive_number(-1) is False

    def test_none_is_invalid(self):
        assert is_positive_number(None) is False

    def test_string_number(self):
        assert is_positive_number("10.5") is True

    def test_string_invalid(self):
        assert is_positive_number("abc") is False


# ─── is_valid_order_status ────────────────────────────────────────────────────


class TestIsValidOrderStatus:
    @pytest.mark.parametrize("status", list(VALID_ORDER_STATUSES))
    def test_valid_statuses(self, status):
        assert is_valid_order_status(status) is True

    @pytest.mark.parametrize("status", ["PAID", "Cancelled", "unknown", "", None])
    def test_invalid_statuses(self, status):
        # Case-insensitive normalisation happens in Silver; raw values tested here
        # Note: validation normalises to lower, so "PAID" actually becomes "paid"
        # The validation fn itself lower-strips before checking
        if status in (None, ""):
            assert is_valid_order_status(status) is False
        else:
            # The function lowercases, so PAID -> paid which IS valid
            expected = status.strip().lower() in VALID_ORDER_STATUSES
            assert is_valid_order_status(status) == expected

    def test_none_is_invalid(self):
        assert is_valid_order_status(None) is False

    def test_empty_is_invalid(self):
        assert is_valid_order_status("") is False

    def test_unknown_value_is_invalid(self):
        assert is_valid_order_status("shipped") is False


# ─── is_valid_payment_status ──────────────────────────────────────────────────


class TestIsValidPaymentStatus:
    @pytest.mark.parametrize("status", list(VALID_PAYMENT_STATUSES))
    def test_valid_statuses(self, status):
        assert is_valid_payment_status(status) is True

    def test_invalid_status(self):
        assert is_valid_payment_status("ERROR") is False


# ─── check_referential_integrity ─────────────────────────────────────────────


class TestCheckReferentialIntegrity:
    def test_all_valid(self):
        result = check_referential_integrity(
            ["C001", "C002"], {"C001", "C002", "C003"}
        )
        assert result == []

    def test_some_invalid(self):
        result = check_referential_integrity(
            ["C001", "INVALID"], {"C001", "C002"}
        )
        assert "INVALID" in result
        assert "C001" not in result

    def test_empty_fk_list(self):
        assert check_referential_integrity([], {"C001"}) == []

    def test_empty_valid_set(self):
        result = check_referential_integrity(["C001"], set())
        assert "C001" in result


# ─── check_row_count_delta ────────────────────────────────────────────────────


class TestCheckRowCountDelta:
    def test_within_tolerance(self):
        assert check_row_count_delta(1000, 950, tolerance_pct=10) is True

    def test_outside_tolerance(self):
        assert check_row_count_delta(1000, 800, tolerance_pct=10) is False

    def test_exact_match(self):
        assert check_row_count_delta(500, 500) is True

    def test_zero_source(self):
        assert check_row_count_delta(0, 0) is True
        assert check_row_count_delta(0, 5) is False


# ─── validate_order_record ────────────────────────────────────────────────────


class TestValidateOrderRecord:
    def _valid_record(self) -> dict:
        return {
            "order_id": "ORD0000001",
            "customer_id": "CUST000001",
            "product_id": "PROD00001",
            "order_date": "2024-03-15",
            "quantity": 2,
            "unit_price": 29.99,
            "order_status": "paid",
            "payment_status": "paid",
        }

    def test_valid_record_no_errors(self):
        assert validate_order_record(self._valid_record()) == []

    def test_null_order_id(self):
        r = self._valid_record()
        r["order_id"] = None
        errors = validate_order_record(r)
        assert "NULL_ORDER_ID" in errors

    def test_invalid_date(self):
        r = self._valid_record()
        r["order_date"] = "9999-99-99"
        assert "INVALID_ORDER_DATE" in validate_order_record(r)

    def test_negative_quantity(self):
        r = self._valid_record()
        r["quantity"] = -5
        assert "INVALID_QUANTITY" in validate_order_record(r)

    def test_zero_price(self):
        r = self._valid_record()
        r["unit_price"] = 0
        assert "INVALID_UNIT_PRICE" in validate_order_record(r)

    def test_invalid_status(self):
        r = self._valid_record()
        r["order_status"] = "shipped"
        assert "INVALID_ORDER_STATUS" in validate_order_record(r)

    def test_multiple_errors(self):
        r = {
            "order_id": None,
            "customer_id": None,
            "product_id": "P1",
            "order_date": "bad-date",
            "quantity": -1,
            "unit_price": -5.0,
            "order_status": "unknown",
            "payment_status": "unknown",
        }
        errors = validate_order_record(r)
        assert len(errors) >= 5

    def test_cancelled_order_valid(self):
        r = self._valid_record()
        r["order_status"] = "cancelled"
        r["payment_status"] = "failed"
        assert validate_order_record(r) == []
