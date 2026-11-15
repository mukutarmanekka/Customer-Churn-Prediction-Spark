from pathlib import Path
import pandas as pd

root = Path(__file__).resolve().parents[1]
df = pd.read_csv(root / "data/raw/transactions.csv", parse_dates=["order_date"])
for snapshot in ["2018-03-01", "2018-06-01"]:
    s = pd.Timestamp(snapshot)
    obs = df[(df.order_date >= s - pd.Timedelta(days=365)) & (df.order_date < s)]
    fut = df[(df.order_date >= s) & (df.order_date < s + pd.Timedelta(days=90))]
    freq = obs.groupby("customer_id").order_id.nunique()
    future = set(fut.customer_id.unique())
    print("\nSNAPSHOT", snapshot)
    print("all obs customers", len(freq), "repeat >=2", int((freq >= 2).sum()), "repeat >=3", int((freq >= 3).sum()))
    for minimum in [1, 2, 3]:
        ids = set(freq[freq >= minimum].index)
        retained = len(ids & future)
        print(f"min_prior_orders={minimum}: customers={len(ids)}, churn={len(ids)-retained}, repeaters={retained}, churn_rate={(len(ids)-retained)/len(ids):.4%}")
