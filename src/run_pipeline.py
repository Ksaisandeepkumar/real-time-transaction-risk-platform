import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
INPUT = BASE / "data" / "input"
OUTPUT = BASE / "data" / "output"
INPUT.mkdir(parents=True, exist_ok=True)
OUTPUT.mkdir(parents=True, exist_ok=True)


def generate_transactions(n=1000):
    rows = []
    for i in range(1, n + 1):
        amount = round(random.uniform(5, 5000), 2)
        rows.append({
            "transaction_id": f"TXN{i:07d}",
            "customer_id": f"CUST{random.randint(1000,9999)}",
            "event_time": (datetime.now() - timedelta(minutes=random.randint(0, 60))).isoformat(timespec="seconds"),
            "amount": amount,
            "merchant": random.choice(["Amazon", "Walmart", "Target", "Costco", "BestBuy"]),
            "location": random.choice(["TX", "CA", "NY", "FL", "IL"]),
            "payment_method": random.choice(["card", "wallet", "bank_transfer"]),
        })
    rows.append(rows[10].copy())
    rows.append(rows[20].copy())
    return pd.DataFrame(rows)


def main():
    raw = generate_transactions()
    raw_path = INPUT / "transactions_raw.csv"
    raw.to_csv(raw_path, index=False)

    silver = raw.drop_duplicates(subset=["transaction_id"])
    silver = silver[silver["amount"] > 0].copy()
    silver["risk_score"] = silver["amount"].apply(lambda x: 90 if x >= 3000 else 60 if x >= 1000 else 25)
    silver["risk_band"] = silver["risk_score"].apply(lambda x: "HIGH" if x >= 80 else "MEDIUM" if x >= 50 else "LOW")
    silver_path = OUTPUT / "silver_transactions_scored.csv"
    silver.to_csv(silver_path, index=False)

    gold = silver.groupby("risk_band", as_index=False).agg(
        transaction_count=("transaction_id", "count"),
        total_amount=("amount", "sum"),
        avg_amount=("amount", "mean"),
    )
    gold_path = OUTPUT / "gold_risk_summary.csv"
    gold.to_csv(gold_path, index=False)

    print("Real-time transaction risk platform completed")
    print(f"Raw: {raw_path}")
    print(f"Silver: {silver_path}")
    print(f"Gold: {gold_path}")


if __name__ == "__main__":
    main()
