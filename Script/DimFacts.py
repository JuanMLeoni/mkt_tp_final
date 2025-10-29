from pathlib import Path
import argparse
import sys
import pandas as pd
from typing import Dict, Any


def read_staging(name: str, staging_dir: Path) -> pd.DataFrame:
    path = staging_dir / f"{name}.csv"
    if not path.exists():
        print(f"⚠️  Staging file not found: {path}. Returning empty DataFrame.")
        return pd.DataFrame()
    return pd.read_csv(path)


def build_dims_and_facts(staging_dir: Path, dw_dir: Path):
    dw_dir.mkdir(parents=True, exist_ok=True)

    
    cust = read_staging("stg_customer", staging_dir)
    prod = read_staging("stg_product", staging_dir)
    chan = read_staging("stg_channel", staging_dir)
    store = read_staging("stg_store", staging_dir)
    addr = read_staging("stg_address", staging_dir)
    sales = read_staging("stg_sales_order", staging_dir)
    sales_item = read_staging("stg_sales_order_item", staging_dir)
    payments = read_staging("stg_payment", staging_dir)
    shipments = read_staging("stg_shipment", staging_dir)
    web_sessions = read_staging("stg_web_session", staging_dir)
    nps = read_staging("stg_nps_response", staging_dir)

    
    def make_mapping(series: pd.Series) -> Dict[Any, int]:
        vals = pd.Series(series.dropna().unique())
        return {v: int(i + 1) for i, v in enumerate(vals)}

    mappings: Dict[str, Dict[Any, int]] = {}

    if not cust.empty:
        dim_customer = cust.copy()
        if "customer_id" in dim_customer.columns:
            cust_map = make_mapping(dim_customer["customer_id"])
            mappings["customer_id"] = cust_map
            dim_customer = dim_customer.drop_duplicates(subset=["customer_id"]).copy()
            dim_customer["customer_key"] = dim_customer["customer_id"].map(cust_map).astype(int)
            dim_customer = dim_customer.rename(columns={"customer_id": "customer_natural_key"})
            cols = [c for c in ["customer_key"] if c in dim_customer.columns] + [c for c in dim_customer.columns if c != "customer_key"]
            dim_customer = dim_customer[cols]
        dim_customer.to_csv(dw_dir / "dim_customer.csv", index=False)
        print(f"Wrote dim_customer ({len(dim_customer)} rows)")
    else:
        print("dim_customer skipped (no data)")

    
    if not prod.empty:
        dim_product = prod.copy()
        if "product_id" in dim_product.columns:
            prod_map = make_mapping(dim_product["product_id"])
            mappings["product_id"] = prod_map
            dim_product = dim_product.drop_duplicates(subset=["product_id"]).copy()
            dim_product["product_key"] = dim_product["product_id"].map(prod_map).astype(int)
            dim_product = dim_product.rename(columns={"product_id": "product_natural_key", "name": "product_name"})
            keep = [c for c in ["product_key", "product_natural_key", "product_name", "category_name", "parent_category_name"] if c in dim_product.columns]
            dim_product = dim_product[keep]
        else:
            keep = [c for c in ["name", "category_name", "parent_category_name"] if c in dim_product.columns]
            dim_product = dim_product[keep]
        dim_product.to_csv(dw_dir / "dim_product.csv", index=False)
        print(f"Wrote dim_product ({len(dim_product)} rows)")
    else:
        print("dim_product skipped (no data)")
    
    if not chan.empty:
        dim_channel = chan.copy()
        if "channel_id" in dim_channel.columns:
            chan_map = make_mapping(dim_channel["channel_id"])
            mappings["channel_id"] = chan_map
            dim_channel = dim_channel.drop_duplicates(subset=["channel_id"]).copy()
            dim_channel["channel_key"] = dim_channel["channel_id"].map(chan_map).astype(int)
            dim_channel = dim_channel.rename(columns={"channel_id": "channel_natural_key"})
            cols = [c for c in ["channel_key"] if c in dim_channel.columns] + [c for c in dim_channel.columns if c != "channel_key"]
            dim_channel = dim_channel[cols]
        dim_channel.to_csv(dw_dir / "dim_channel.csv", index=False)
        print(f"Wrote dim_channel ({len(dim_channel)} rows)")
    else:
        print("dim_channel skipped (no data)")

    
    if not store.empty:
        dim_store = store.copy()
        if "store_id" in dim_store.columns:
            store_map = make_mapping(dim_store["store_id"])
            mappings["store_id"] = store_map
            dim_store = dim_store.drop_duplicates(subset=["store_id"]).copy()
            dim_store["store_key"] = dim_store["store_id"].map(store_map).astype(int)
            dim_store = dim_store.rename(columns={"store_id": "store_natural_key"})
            cols = [c for c in ["store_key"] if c in dim_store.columns] + [c for c in dim_store.columns if c != "store_key"]
            dim_store = dim_store[cols]
        dim_store.to_csv(dw_dir / "dim_store.csv", index=False)
        print(f"Wrote dim_store ({len(dim_store)} rows)")
    else:
        print("dim_store skipped (no data)")

    
    if not addr.empty:
        dim_address = addr.copy()
        if "address_id" in dim_address.columns:
            addr_map = make_mapping(dim_address["address_id"])
            mappings["address_id"] = addr_map
            dim_address = dim_address.drop_duplicates(subset=["address_id"]).copy()
            dim_address["address_key"] = dim_address["address_id"].map(addr_map).astype(int)
            dim_address = dim_address.rename(columns={"address_id": "address_natural_key"})
            keep = [c for c in ["address_key", "address_natural_key", "line1", "line2", "city", "province_id", "province_name", "postal_code", "country_code"] if c in dim_address.columns]
            dim_address = dim_address[keep]
        else:
            keep = [c for c in ["line1", "line2", "city", "province_id", "province_name", "postal_code", "country_code"] if c in dim_address.columns]
            dim_address = dim_address[keep]
        dim_address.to_csv(dw_dir / "dim_address.csv", index=False)
        print(f"Wrote dim_address ({len(dim_address)} rows)")
    else:
        print("dim_address skipped (no data)")

    
    if not sales.empty:
        date_col = None
        for c in ["created_at", "order_date", "ord_created_at", "created"]:
            if c in sales.columns:
                date_col = c
                break
        if date_col is not None:
            dates = pd.to_datetime(sales[date_col], errors="coerce").dt.date.dropna().unique()
            dim_date = pd.DataFrame({"date": sorted(dates)})
            dim_date = dim_date.reset_index(drop=True)
            dim_date["date_key"] = (dim_date.index + 1).astype(int)
            dim_date["date"] = pd.to_datetime(dim_date["date"]).dt.date.astype(str)
            dim_date = dim_date[["date_key", "date"]]
            mappings["date"] = {row["date"]: int(row["date_key"]) for _, row in dim_date.iterrows()}
            dim_date.to_csv(dw_dir / "dim_date.csv", index=False)
            print(f"Wrote dim_date ({len(dim_date)} rows) using column {date_col}")
        else:
            print("dim_date skipped (no date column found in sales)")
    else:
        print("dim_date skipped (no sales orders)")

    
    if not sales.empty:
        fact_sales = sales.copy()
        keep = [c for c in ["order_id", "customer_id", "channel_id", "store_id", "total_amount", "ord_created_at", "created_at", "order_date"] if c in fact_sales.columns]
        fact_sales = fact_sales[keep]
        if "order_id" in fact_sales.columns:
            order_map = make_mapping(fact_sales["order_id"])
            mappings["order_id"] = order_map
            fact_sales["order_key"] = fact_sales["order_id"].map(order_map).astype(pd.Int64Dtype())
        if "customer_id" in fact_sales.columns and "customer_id" in mappings:
            fact_sales["customer_key"] = fact_sales["customer_id"].map(mappings["customer_id"]).astype(pd.Int64Dtype())
        if "channel_id" in fact_sales.columns and "channel_id" in mappings:
            fact_sales["channel_key"] = fact_sales["channel_id"].map(mappings["channel_id"]).astype(pd.Int64Dtype())
        if "store_id" in fact_sales.columns and "store_id" in mappings:
            fact_sales["store_key"] = fact_sales["store_id"].map(mappings["store_id"]).astype(pd.Int64Dtype())
        drop_cols = [c for c in ["order_id", "customer_id", "channel_id", "store_id"] if c in fact_sales.columns]
        fact_sales = fact_sales.drop(columns=drop_cols)
        cols = [c for c in ["order_key"] if c in fact_sales.columns]
        cols += [c for c in ["customer_key", "channel_key", "store_key"] if c in fact_sales.columns]
        cols += [c for c in fact_sales.columns if c not in cols]
        fact_sales = fact_sales[cols]
        fact_sales.to_csv(dw_dir / "fact_sales_order.csv", index=False)
        print(f"Wrote fact_sales_order ({len(fact_sales)} rows)")
    else:
        print("fact_sales_order skipped (no sales orders)")

    order_to_customer_key = {}
    order_to_channel_key = {}
    order_to_store_key = {}
    if not sales.empty and "order_id" in sales.columns:
        if "customer_id" in sales.columns:
            order_to_customer_natural = dict(zip(sales["order_id"], sales["customer_id"]))
            if "customer_id" in mappings:
                order_to_customer_key = {o: mappings["customer_id"].get(c) for o, c in order_to_customer_natural.items()}
        if "channel_id" in sales.columns:
            order_to_channel_natural = dict(zip(sales["order_id"], sales["channel_id"]))
            if "channel_id" in mappings:
                order_to_channel_key = {o: mappings["channel_id"].get(ch) for o, ch in order_to_channel_natural.items()}
        if "store_id" in sales.columns:
            order_to_store_natural = dict(zip(sales["order_id"], sales["store_id"]))
            if "store_id" in mappings:
                order_to_store_key = {o: mappings["store_id"].get(s) for o, s in order_to_store_natural.items()}

    
    if not payments.empty:
        fact_payments = payments.copy()
        keep = [c for c in ["payment_id", "order_id", "amount", "created_at", "method", "status", "paid_at", "transaction_ref"] if c in fact_payments.columns]
        fact_payments = fact_payments[keep]
        if "payment_id" in fact_payments.columns:
            pay_map = make_mapping(fact_payments["payment_id"])
            mappings["payment_id"] = pay_map
            fact_payments["payment_key"] = fact_payments["payment_id"].map(pay_map).astype(pd.Int64Dtype())
        if "order_id" in fact_payments.columns:
            if order_to_customer_key:
                fact_payments["customer_key"] = fact_payments["order_id"].map(order_to_customer_key).astype(pd.Int64Dtype())
            if order_to_channel_key:
                fact_payments["channel_key"] = fact_payments["order_id"].map(order_to_channel_key).astype(pd.Int64Dtype())
            if order_to_store_key:
                fact_payments["store_key"] = fact_payments["order_id"].map(order_to_store_key).astype(pd.Int64Dtype())
        drop_cols = [c for c in ["payment_id", "order_id"] if c in fact_payments.columns]
        fact_payments = fact_payments.drop(columns=drop_cols)
        cols = [c for c in ["payment_key"] if c in fact_payments.columns]
        cols += [c for c in ["customer_key", "channel_key", "store_key"] if c in fact_payments.columns]
        cols += [c for c in fact_payments.columns if c not in cols]
        fact_payments = fact_payments[cols]
        fact_payments.to_csv(dw_dir / "fact_payments.csv", index=False)
        print(f"Wrote fact_payments ({len(fact_payments)} rows)")
    else:
        print("fact_payments skipped (no payments)")

    
    if not sales_item.empty:
        fact_items = sales_item.copy()
        keep = [c for c in ["order_item_id", "order_id", "product_id", "quantity", "unit_price"] if c in fact_items.columns]
        fact_items = fact_items[keep]
        if "order_item_id" in fact_items.columns:
            oi_map = make_mapping(fact_items["order_item_id"])
            mappings["order_item_id"] = oi_map
            fact_items["order_item_key"] = fact_items["order_item_id"].map(oi_map).astype(pd.Int64Dtype())
        if "order_id" in fact_items.columns:
            if order_to_customer_key:
                fact_items["customer_key"] = fact_items["order_id"].map(order_to_customer_key).astype(pd.Int64Dtype())
            if order_to_channel_key:
                fact_items["channel_key"] = fact_items["order_id"].map(order_to_channel_key).astype(pd.Int64Dtype())
            if order_to_store_key:
                fact_items["store_key"] = fact_items["order_id"].map(order_to_store_key).astype(pd.Int64Dtype())
        if "product_id" in fact_items.columns and "product_id" in mappings:
            fact_items["product_key"] = fact_items["product_id"].map(mappings["product_id"]).astype(pd.Int64Dtype())
        drop_cols = [c for c in ["order_item_id", "order_id", "product_id"] if c in fact_items.columns]
        fact_items = fact_items.drop(columns=drop_cols)
        cols = [c for c in ["order_item_key"] if c in fact_items.columns]
        cols += [c for c in ["customer_key", "channel_key", "store_key"] if c in fact_items.columns]
        cols += [c for c in ["product_key"] if c in fact_items.columns]
        cols += [c for c in fact_items.columns if c not in cols]
        fact_items = fact_items[cols]
        fact_items.to_csv(dw_dir / "fact_sales_order_item.csv", index=False)
        print(f"Wrote fact_sales_order_item ({len(fact_items)} rows)")
    else:
        print("fact_sales_order_item skipped (no sales order items)")



    
    if not shipments.empty:
        fact_shipments = shipments.copy()
        keep = [c for c in ["shipment_id", "order_id", "shipped_at", "delivered_at"] if c in fact_shipments.columns]
        fact_shipments = fact_shipments[keep]
        if "shipment_id" in fact_shipments.columns:
            sh_map = make_mapping(fact_shipments["shipment_id"])
            mappings["shipment_id"] = sh_map
            fact_shipments["shipment_key"] = fact_shipments["shipment_id"].map(sh_map).astype(pd.Int64Dtype())
        if "order_id" in fact_shipments.columns:
            if order_to_customer_key:
                fact_shipments["customer_key"] = fact_shipments["order_id"].map(order_to_customer_key).astype(pd.Int64Dtype())
            if order_to_channel_key:
                fact_shipments["channel_key"] = fact_shipments["order_id"].map(order_to_channel_key).astype(pd.Int64Dtype())
            if order_to_store_key:
                fact_shipments["store_key"] = fact_shipments["order_id"].map(order_to_store_key).astype(pd.Int64Dtype())
        drop_cols = [c for c in ["shipment_id", "order_id"] if c in fact_shipments.columns]
        fact_shipments = fact_shipments.drop(columns=drop_cols)
        cols = [c for c in ["shipment_key"] if c in fact_shipments.columns]
        cols += [c for c in ["customer_key", "channel_key", "store_key"] if c in fact_shipments.columns]
        cols += [c for c in fact_shipments.columns if c not in cols]
        fact_shipments = fact_shipments[cols]
        fact_shipments.to_csv(dw_dir / "fact_shipments.csv", index=False)
        print(f"Wrote fact_shipments ({len(fact_shipments)} rows)")
    else:
        print("fact_shipments skipped (no shipments)")

    
    if not web_sessions.empty:
        fact_web_sessions = web_sessions.copy()
        keep = [c for c in ["session_id", "customer_id", "started_at", "ended_at", "source", "device"] if c in fact_web_sessions.columns]
        fact_web_sessions = fact_web_sessions[keep]
        if "session_id" in fact_web_sessions.columns:
            sess_map = make_mapping(fact_web_sessions["session_id"])
            mappings["session_id"] = sess_map
            fact_web_sessions["session_key"] = fact_web_sessions["session_id"].map(sess_map).astype(pd.Int64Dtype())
        if "customer_id" in fact_web_sessions.columns and "customer_id" in mappings:
            fact_web_sessions["customer_key"] = fact_web_sessions["customer_id"].map(mappings["customer_id"]).astype(pd.Int64Dtype())
        drop_cols = [c for c in ["session_id", "customer_id"] if c in fact_web_sessions.columns]
        fact_web_sessions = fact_web_sessions.drop(columns=drop_cols)
        cols = [c for c in ["session_key"] if c in fact_web_sessions.columns]
        cols += [c for c in ["customer_key"] if c in fact_web_sessions.columns]
        cols += [c for c in fact_web_sessions.columns if c not in cols]
        fact_web_sessions = fact_web_sessions[cols]
        fact_web_sessions.to_csv(dw_dir / "fact_web_sessions.csv", index=False)
        print(f"Wrote fact_web_sessions ({len(fact_web_sessions)} rows)")
    else:
        print("fact_web_sessions skipped (no web sessions)")

    
    if not nps.empty:
        fact_nps = nps.copy()
        keep = [c for c in ["nps_id", "customer_id", "channel_id", "score", "responded_at"] if c in fact_nps.columns]
        fact_nps = fact_nps[keep]
        if "nps_id" in fact_nps.columns:
            nps_map = make_mapping(fact_nps["nps_id"])
            mappings["nps_id"] = nps_map
            fact_nps["nps_key"] = fact_nps["nps_id"].map(nps_map).astype(pd.Int64Dtype())
        if "customer_id" in fact_nps.columns and "customer_id" in mappings:
            fact_nps["customer_key"] = fact_nps["customer_id"].map(mappings["customer_id"]).astype(pd.Int64Dtype())
        if "channel_id" in fact_nps.columns and "channel_id" in mappings:
            fact_nps["channel_key"] = fact_nps["channel_id"].map(mappings["channel_id"]).astype(pd.Int64Dtype())
        drop_cols = [c for c in ["nps_id", "customer_id", "channel_id"] if c in fact_nps.columns]
        fact_nps = fact_nps.drop(columns=drop_cols)
        cols = [c for c in ["nps_key"] if c in fact_nps.columns]
        cols += [c for c in ["customer_key", "channel_key"] if c in fact_nps.columns]
        cols += [c for c in fact_nps.columns if c not in cols]
        fact_nps = fact_nps[cols]
        fact_nps.to_csv(dw_dir / "fact_nps.csv", index=False)
        print(f"Wrote fact_nps ({len(fact_nps)} rows)")
    else:
        print("fact_nps skipped (no nps responses)")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build simple Kimball-style DIM and FACT CSVs from STAGING/")
    parser.add_argument("--staging-dir", default=str(Path(__file__).resolve().parents[1] / "STAGING"), help="Path to STAGING folder")
    parser.add_argument("--dw-dir", default=str(Path(__file__).resolve().parents[1] / "DW"), help="Path to write DW files (dimensions and facts)")
    args = parser.parse_args(argv)

    staging_dir = Path(args.staging_dir)
    dw_dir = Path(args.dw_dir)
    if not staging_dir.exists():
        print(f"❌ Staging directory does not exist: {staging_dir}. Please run the desnormalizador first to create STAGING/")
        sys.exit(2)

    print(f"Reading staging from: {staging_dir} (only STAGING will be used)")
    print(f"Writing DW outputs to: {dw_dir}")

    build_dims_and_facts(staging_dir, dw_dir)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
