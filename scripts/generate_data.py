"""
Synthetic e-commerce dataset generator.

Generates reproducible CSV files for:
  - customers.csv
  - products.csv
  - orders.csv

Data quality issues are deliberately injected at a controlled rate:
  - Duplicate records (~3 %)
  - Missing required values (~5 %)
  - Invalid dates
  - Negative quantities / prices
  - Invalid FK references
  - Inconsistent status values

Usage:
    python scripts/generate_data.py --seed 42 --orders 5000
"""

import argparse
import csv
import os
import random
import string
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
STATUSES = ["pending", "paid", "cancelled", "refunded"]
# Deliberate data quality issues — invalid values injected at low rate
BAD_STATUSES = ["PENDING", "Paid", "canceled", "unknown", ""]

PAYMENT_STATUSES = ["unpaid", "paid", "failed", "refunded"]
BAD_PAYMENT_STATUSES = ["UNPAID", "Paid", "ERROR", ""]

CATEGORIES = [
    "Electronics", "Clothing", "Books", "Home & Garden",
    "Sports", "Beauty", "Toys", "Automotive",
]


def _rand_str(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def _rand_date(start: date, end: date) -> str:
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()


def _maybe_null(value, null_rate: float = 0.05):
    """Return None at null_rate probability, else return value."""
    return None if random.random() < null_rate else value


def _maybe_bad_status(good: str, bad_pool: list, bad_rate: float = 0.04):
    return random.choice(bad_pool) if random.random() < bad_rate else good


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------

def generate_customers(n: int) -> list[dict]:
    customers = []
    for i in range(1, n + 1):
        cid = f"CUST{i:06d}"
        customers.append(
            {
                "customer_id": cid,
                "first_name": _maybe_null(f"FirstName{i}", 0.03),
                "last_name": _maybe_null(f"LastName{i}", 0.02),
                "email": _maybe_null(f"user{i}@example.com", 0.04),
                "country": random.choice(
                    ["US", "UK", "DE", "FR", "CA", "AU", "JP", "BR"]
                ),
                "signup_date": _rand_date(date(2020, 1, 1), date(2024, 12, 31)),
                "segment": random.choice(["gold", "silver", "bronze", "standard"]),
            }
        )
    return customers


def generate_products(n: int) -> list[dict]:
    products = []
    for i in range(1, n + 1):
        price = round(random.uniform(1.0, 500.0), 2)
        # Inject negative price quality issue
        if random.random() < 0.02:
            price = -abs(price)
        products.append(
            {
                "product_id": f"PROD{i:05d}",
                "product_name": _maybe_null(f"Product {_rand_str(4)}", 0.02),
                "category": _maybe_null(random.choice(CATEGORIES), 0.03),
                "unit_price": price,
                "brand": f"Brand{random.randint(1, 50)}",
                "in_stock": random.choice([True, False]),
            }
        )
    return products


def generate_orders(
    n: int,
    customer_ids: list[str],
    product_ids: list[str],
    start: date = date(2023, 1, 1),
    end: date = date(2024, 12, 31),
) -> list[dict]:
    orders = []
    for i in range(1, n + 1):
        order_id = f"ORD{i:07d}"
        # Inject invalid FK reference
        if random.random() < 0.02:
            cid = "CUST_INVALID"
        else:
            cid = random.choice(customer_ids)
        if random.random() < 0.02:
            pid = "PROD_INVALID"
        else:
            pid = random.choice(product_ids)

        qty = random.randint(1, 20)
        # Inject negative quantity
        if random.random() < 0.02:
            qty = -qty

        unit_price = round(random.uniform(1.0, 500.0), 2)
        # Inject negative price
        if random.random() < 0.02:
            unit_price = -unit_price

        order_date = _rand_date(start, end)
        # Inject invalid date
        if random.random() < 0.02:
            order_date = "9999-99-99"

        status = random.choice(STATUSES)
        status = _maybe_bad_status(status, BAD_STATUSES)

        payment_status = "paid" if status == "paid" else "unpaid"
        if status in ("cancelled", "refunded"):
            payment_status = status if status == "refunded" else "failed"
        payment_status = _maybe_bad_status(payment_status, BAD_PAYMENT_STATUSES)

        row = {
            "order_id": _maybe_null(order_id, 0.01),
            "customer_id": _maybe_null(cid, 0.02),
            "product_id": _maybe_null(pid, 0.02),
            "order_date": order_date,
            "quantity": qty,
            "unit_price": unit_price,
            "order_status": status,
            "payment_status": payment_status,
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        orders.append(row)

    # Inject duplicates (~3 %)
    n_dupes = max(1, int(n * 0.03))
    duplicates = random.sample(orders, min(n_dupes, len(orders)))
    orders.extend(duplicates)
    random.shuffle(orders)

    return orders


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_csv(records: list[dict], path: Path) -> None:
    if not records:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)
    print(f"  Wrote {len(records):,} rows -> {path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--customers", type=int, default=500, help="Customer count")
    parser.add_argument("--products", type=int, default=200, help="Product count")
    parser.add_argument("--orders", type=int, default=5000, help="Order count")
    parser.add_argument(
        "--out",
        type=str,
        default=str(Path(__file__).parent.parent / "data" / "sample"),
        help="Output directory",
    )
    args = parser.parse_args()

    random.seed(args.seed)
    out = Path(args.out)

    print(f"[generate_data] seed={args.seed}  customers={args.customers}  "
          f"products={args.products}  orders={args.orders}")

    customers = generate_customers(args.customers)
    products = generate_products(args.products)

    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]
    orders = generate_orders(args.orders, customer_ids, product_ids)

    write_csv(customers, out / "customers.csv")
    write_csv(products, out / "products.csv")
    write_csv(orders, out / "orders.csv")

    print("[generate_data] Done.")


if __name__ == "__main__":
    main()
