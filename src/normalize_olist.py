import pandas as pd
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
SRC=ROOT/"data/raw/olist"
OUT=ROOT/"data/raw/transactions.csv"

def main():
    orders=pd.read_csv(SRC/"olist_orders_dataset.csv")
    customers=pd.read_csv(SRC/"olist_customers_dataset.csv",usecols=["customer_id","customer_unique_id","customer_state"])
    items=pd.read_csv(SRC/"olist_order_items_dataset.csv")
    payments=pd.read_csv(SRC/"olist_order_payments_dataset.csv")
    reviews=pd.read_csv(SRC/"olist_order_reviews_dataset.csv",usecols=["order_id","review_score"])
    orders=orders[orders.order_status.eq("delivered")].copy()
    orders=orders.merge(customers,on="customer_id",how="inner",validate="one_to_one")
    orders["order_purchase_timestamp"]=pd.to_datetime(orders.order_purchase_timestamp,errors="coerce")
    orders["order_delivered_customer_date"]=pd.to_datetime(orders.order_delivered_customer_date,errors="coerce")
    orders["order_estimated_delivery_date"]=pd.to_datetime(orders.order_estimated_delivery_date,errors="coerce")
    item_features=items.groupby("order_id").agg(item_count=("order_item_id","count"),freight_value=("freight_value","sum")).reset_index()
    payment_features=payments.groupby("order_id").agg(amount=("payment_value","sum"),payment_installments=("payment_installments","max"),payment_type_count=("payment_type","nunique")).reset_index()
    review_features=reviews.groupby("order_id").agg(review_score=("review_score","mean")).reset_index()
    df=orders.merge(item_features,on="order_id",how="left").merge(payment_features,on="order_id",how="left").merge(review_features,on="order_id",how="left")
    df["delivery_days"]=(df.order_delivered_customer_date-df.order_purchase_timestamp).dt.total_seconds()/86400
    df["delivery_delay_days"]=(df.order_delivered_customer_date-df.order_estimated_delivery_date).dt.total_seconds()/86400
    df["order_date"]=df.order_purchase_timestamp.dt.strftime("%Y-%m-%d")
    df["delivery_date"]=df.order_delivered_customer_date.dt.strftime("%Y-%m-%d")
    df=df.drop(columns=["customer_id"]).rename(columns={"customer_unique_id":"customer_id"})
    cols=["customer_id","order_id","order_date","delivery_date","amount","item_count","freight_value","payment_installments","payment_type_count","review_score","delivery_days","delivery_delay_days","customer_state"]
    df=df[cols].dropna(subset=["customer_id","order_id","order_date","amount"])
    df=df[df.amount.ge(0)].drop_duplicates("order_id")
    OUT.parent.mkdir(parents=True,exist_ok=True)
    df.to_csv(OUT,index=False)
    print(f"Delivered orders: {len(df):,}; unique customers: {df.customer_id.nunique():,}")
    print(f"Date range: {df.order_date.min()} to {df.order_date.max()}")
    print(f"Saved {OUT}")

if __name__=="__main__": main()
