from pathlib import Path
from datetime import date, timedelta
import csv, random

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "raw" / "transactions.csv"
RNG = random.Random(42)
SNAPSHOT = date(2025, 10, 1)
OBS_START = SNAPSHOT - timedelta(days=180)

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for n in range(1, 1201):
        customer = f"C{n:05d}"
        churn_prone = RNG.random() < 0.34
        count = RNG.randint(1, 9) if churn_prone else RNG.randint(3, 15)
        for j in range(count):
            dt = OBS_START + timedelta(days=RNG.randint(0, 179))
            amount = round(RNG.uniform(12, 450) * (0.9 if churn_prone else 1.25), 2)
            rows.append((customer, f"{customer}-H{j+1:02d}", dt.isoformat(), amount))
        future_count = 0 if churn_prone and RNG.random() < 0.82 else RNG.randint(1, 4)
        for j in range(future_count):
            dt = SNAPSHOT + timedelta(days=RNG.randint(0, 89))
            rows.append((customer, f"{customer}-F{j+1:02d}", dt.isoformat(), round(RNG.uniform(12, 450), 2)))
    rows.sort(key=lambda x: (x[2], x[0]))
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["customer_id","order_id","order_date","amount"]); w.writerows(rows)
    print(f"Created {len(rows):,} transactions for 1,200 customers at {OUT}")

if __name__ == "__main__":
    main()
