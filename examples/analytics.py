"""
Analytics example.

Shows MooFile's group/agg pipeline for aggregating over a dataset of
sales events. Demonstrates count, sum, mean, min, max, and chaining
group results with sort and limit.
"""

import os
import random
import tempfile
from moofile import Collection, count, sum, mean, min, max, collect

DB_PATH = os.path.join(tempfile.mkdtemp(), "sales.bson")

REGIONS = ["North", "South", "East", "West"]
CATEGORIES = ["Electronics", "Clothing", "Food", "Books"]
REPS = ["Alice", "Bob", "Carol", "Dave", "Eve"]


def seed(db: Collection, n: int = 500) -> None:
    """Generate n synthetic sales records."""
    random.seed(42)
    records = []
    for i in range(n):
        records.append({
            "_id": str(i),
            "rep": random.choice(REPS),
            "region": random.choice(REGIONS),
            "category": random.choice(CATEGORIES),
            "amount": round(random.uniform(10, 2000), 2),
            "units": random.randint(1, 50),
            "quarter": random.choice(["Q1", "Q2", "Q3", "Q4"]),
        })
    db.insert_many(records)


def sales_by_region(db: Collection) -> None:
    print("\n--- Sales by Region ---")
    results = (
        db.find()
        .group("region")
        .agg(count(), sum("amount"), mean("amount"))
        .sort("sum_amount", descending=True)
        .to_list()
    )
    print(f"  {'Region':<10} {'Orders':>7} {'Total $':>12} {'Avg $':>10}")
    print("  " + "-" * 43)
    for r in results:
        print(f"  {r['region']:<10} {r['count']:>7} {r['sum_amount']:>12,.2f} {r['mean_amount']:>10,.2f}")


def top_reps_by_revenue(db: Collection, top_n: int = 3) -> None:
    print(f"\n--- Top {top_n} Sales Reps by Revenue ---")
    results = (
        db.find()
        .group("rep")
        .agg(count(), sum("amount"), sum("units"))
        .sort("sum_amount", descending=True)
        .limit(top_n)
        .to_list()
    )
    for i, r in enumerate(results, 1):
        print(f"  {i}. {r['rep']:<8} ${r['sum_amount']:>10,.2f}  ({r['count']} orders, {r['sum_units']} units)")


def category_performance(db: Collection) -> None:
    print("\n--- Category Performance ---")
    results = (
        db.find()
        .group("category")
        .agg(count(), mean("amount"), min("amount"), max("amount"))
        .sort("mean_amount", descending=True)
        .to_list()
    )
    print(f"  {'Category':<15} {'Orders':>7} {'Avg $':>10} {'Min $':>10} {'Max $':>10}")
    print("  " + "-" * 57)
    for r in results:
        print(
            f"  {r['category']:<15} {r['count']:>7} "
            f"{r['mean_amount']:>10,.2f} {r['min_amount']:>10,.2f} {r['max_amount']:>10,.2f}"
        )


def quarterly_summary(db: Collection) -> None:
    print("\n--- Quarterly Summary (Electronics only) ---")
    results = (
        db.find({"category": "Electronics"})
        .group("quarter")
        .agg(count(), sum("amount"))
        .sort("quarter")
        .to_list()
    )
    for r in results:
        print(f"  {r['quarter']}: {r['count']} orders, ${r['sum_amount']:,.2f}")


def high_value_orders(db: Collection, threshold: float = 1500.0) -> None:
    print(f"\n--- Orders over ${threshold:,.0f} ---")
    results = (
        db.find({"amount": {"$gt": threshold}})
        .sort("amount", descending=True)
        .limit(5)
        .to_list()
    )
    for r in results:
        print(f"  ${r['amount']:>8,.2f}  {r['rep']:<8} {r['region']:<8} {r['category']}")


def reps_per_region(db: Collection) -> None:
    print("\n--- Unique Reps per Region ---")
    results = (
        db.find()
        .group("region")
        .agg(collect("rep"))
        .to_list()
    )
    for r in results:
        unique_reps = sorted(set(r["collect_rep"]))
        print(f"  {r['region']:<8}: {unique_reps}")


def main() -> None:
    print("=== MooFile — Sales Analytics ===")

    with Collection(DB_PATH, indexes=["rep", "region", "category", "quarter"]) as db:
        seed(db, n=500)
        print(f"Loaded {db.count()} sales records")

        sales_by_region(db)
        top_reps_by_revenue(db, top_n=3)
        category_performance(db)
        quarterly_summary(db)
        high_value_orders(db, threshold=1500)
        reps_per_region(db)

        s = db.stats()
        print(f"\nDB stats: {s['documents']} docs, {s['file_size_bytes'] / 1024:.1f} KB")


if __name__ == "__main__":
    main()
