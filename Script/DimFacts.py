from pathlib import Path
import argparse
import sys
import pandas as pd


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

    
    if not cust.empty:
        dim_customer = cust.copy()
        if "customer_id" in dim_customer.columns:
            dim_customer = dim_customer.rename(columns={"customer_id": "customer_key"})
        dim_customer.to_csv(dw_dir / "dim_customer.csv", index=False)
        print(f"Wrote dim_customer ({len(dim_customer)} rows)")
    else:
        print("dim_customer skipped (no data)")

    
    if not prod.empty:
        dim_product = prod.copy()
        keep = [c for c in ["product_id", "name", "category_name", "parent_category_name"] if c in dim_product.columns]
        dim_product = dim_product[keep]
        dim_product = dim_product.rename(columns={"product_id": "product_key", "name": "product_name"})
        dim_product.to_csv(dw_dir / "dim_product.csv", index=False)
        print(f"Wrote dim_product ({len(dim_product)} rows)")
    else:
        print("dim_product skipped (no data)")
    
    if not chan.empty:
        dim_channel = chan.copy()
        if "channel_id" in dim_channel.columns:
            dim_channel = dim_channel.rename(columns={"channel_id": "channel_key"})
        dim_channel.to_csv(dw_dir / "dim_channel.csv", index=False)
        print(f"Wrote dim_channel ({len(dim_channel)} rows)")
    else:
        print("dim_channel skipped (no data)")

    
    if not store.empty:
        dim_store = store.copy()
        if "store_id" in dim_store.columns:
            dim_store = dim_store.rename(columns={"store_id": "store_key"})
        dim_store.to_csv(dw_dir / "dim_store.csv", index=False)
        print(f"Wrote dim_store ({len(dim_store)} rows)")
    else:
        print("dim_store skipped (no data)")

    
    if not addr.empty:
        dim_address = addr.copy()
        if "address_id" in dim_address.columns:
            dim_address = dim_address.rename(columns={"address_id": "address_key"})
        keep = [c for c in ["address_key", "line1", "line2", "city", "province_id", "province_name", "postal_code", "country_code"] if c in dim_address.columns]
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
            dim_date["date_key"] = dim_date["date"].astype(str)
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
        rename_map = {}
        if "order_id" in fact_sales.columns:
            rename_map["order_id"] = "order_key"
        if "customer_id" in fact_sales.columns:
            rename_map["customer_id"] = "customer_key"
        if "channel_id" in fact_sales.columns:
            rename_map["channel_id"] = "channel_key"
        if "store_id" in fact_sales.columns:
            rename_map["store_id"] = "store_key"
        fact_sales = fact_sales.rename(columns=rename_map)
        fact_sales.to_csv(dw_dir / "fact_sales_order.csv", index=False)
        print(f"Wrote fact_sales_order ({len(fact_sales)} rows)")
    else:
        print("fact_sales_order skipped (no sales orders)")

    
    if not payments.empty:
        fact_payments = payments.copy()
        keep = [c for c in ["payment_id", "order_id", "amount", "created_at"] if c in fact_payments.columns]
        fact_payments = fact_payments[keep]
        rename_map = {}
        if "payment_id" in fact_payments.columns:
            rename_map["payment_id"] = "payment_key"
        if "order_id" in fact_payments.columns:
            rename_map["order_id"] = "order_key"
        fact_payments = fact_payments.rename(columns=rename_map)
        fact_payments.to_csv(dw_dir / "fact_payments.csv", index=False)
        print(f"Wrote fact_payments ({len(fact_payments)} rows)")
    else:
        print("fact_payments skipped (no payments)")

    
    if not sales_item.empty:
        fact_items = sales_item.copy()
        keep = [c for c in ["order_item_id", "order_id", "product_id", "quantity", "unit_price"] if c in fact_items.columns]
        fact_items = fact_items[keep]
        rename_map = {}
        if "order_item_id" in fact_items.columns:
            rename_map["order_item_id"] = "order_item_key"
        if "order_id" in fact_items.columns:
            rename_map["order_id"] = "order_key"
        if "product_id" in fact_items.columns:
            rename_map["product_id"] = "product_key"
        fact_items = fact_items.rename(columns=rename_map)
        fact_items.to_csv(dw_dir / "fact_sales_order_item.csv", index=False)
        print(f"Wrote fact_sales_order_item ({len(fact_items)} rows)")
    else:
        print("fact_sales_order_item skipped (no sales order items)")

    
    if not sales_item.empty:
        fact_order_items = fact_items.copy()
        fact_order_items.to_csv(dw_dir / "fact_order_items.csv", index=False)
        print(f"Wrote fact_order_items ({len(fact_order_items)} rows)")
    else:
        print("fact_order_items skipped (no sales order items)")

    
    if not shipments.empty:
        fact_shipments = shipments.copy()
        keep = [c for c in ["shipment_id", "order_id", "shipped_at", "delivered_at"] if c in fact_shipments.columns]
        fact_shipments = fact_shipments[keep]
        if "shipment_id" in fact_shipments.columns:
            fact_shipments = fact_shipments.rename(columns={"shipment_id": "shipment_key", "order_id": "order_key"})
        fact_shipments.to_csv(dw_dir / "fact_shipments.csv", index=False)
        print(f"Wrote fact_shipments ({len(fact_shipments)} rows)")
    else:
        print("fact_shipments skipped (no shipments)")

    
    if not web_sessions.empty:
        fact_web_sessions = web_sessions.copy()
        keep = [c for c in ["session_id", "customer_id", "started_at", "ended_at", "source", "device"] if c in fact_web_sessions.columns]
        fact_web_sessions = fact_web_sessions[keep]
        if "session_id" in fact_web_sessions.columns:
            fact_web_sessions = fact_web_sessions.rename(columns={"session_id": "session_key", "customer_id": "customer_key"})
        fact_web_sessions.to_csv(dw_dir / "fact_web_sessions.csv", index=False)
        print(f"Wrote fact_web_sessions ({len(fact_web_sessions)} rows)")
    else:
        print("fact_web_sessions skipped (no web sessions)")

    
    if not nps.empty:
        fact_nps = nps.copy()
        keep = [c for c in ["nps_id", "customer_id", "channel_id", "score", "responded_at"] if c in fact_nps.columns]
        fact_nps = fact_nps[keep]
        rename_map = {}
        if "nps_id" in fact_nps.columns:
            rename_map["nps_id"] = "nps_key"
        if "customer_id" in fact_nps.columns:
            rename_map["customer_id"] = "customer_key"
        if "channel_id" in fact_nps.columns:
            rename_map["channel_id"] = "channel_key"
        fact_nps = fact_nps.rename(columns=rename_map)
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
