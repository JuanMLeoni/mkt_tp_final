from pathlib import Path
import argparse
import zipfile
import sys
import pandas as pd


RAW_DIR = Path(__file__).resolve().parents[1] / "raw"
STAGING_DIR = Path(__file__).resolve().parents[1] / "STAGING"
STAGING_DIR.mkdir(parents=True, exist_ok=True)


from pathlib import Path
import argparse
import zipfile
import sys
import pandas as pd

RAW_DIR = Path(__file__).resolve().parents[1] / "raw"
STAGING_DIR = Path(__file__).resolve().parents[1] / "STAGING"
STAGING_DIR.mkdir(parents=True, exist_ok=True)


def load(name, raw_dir: Path = RAW_DIR):
    path = raw_dir / f"{name}.csv"
    if not path.exists():
        print(f"⚠️  Warning: {path} not found. Returning empty DataFrame.")
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as e:
        print(f"❌ Error reading {path}: {e}")
        raise


def save(df: pd.DataFrame, name: str, staging_dir: Path = STAGING_DIR):
    out = staging_dir / f"{name}.csv"
    df.to_csv(out, index=False)
    print(f"Saved: {out}")


def archive_csvs(src_dir: Path, output_path: Path):
    files = list(src_dir.glob("*.csv"))
    if not files:
        print(f"No CSV files found in {src_dir} to archive.")
        return None
    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for f in files:
            z.write(f, arcname=f.name)
    print(f"Created archive: {output_path}")
    return output_path


channel = load("channel")
province = load("province")
product_category = load("product_category")
customer = load("customer")
address = load("address")
store = load("store")
product = load("product")
sales_order = load("sales_order")
sales_item = load("sales_order_item")
payment = load("payment")
shipment = load("shipment")
web_session = load("web_session")
nps = load("nps_response")


stg_address = (
    address
    .merge(province.add_prefix("province_"), left_on="province_id", right_on="province_province_id", how="left")
    .drop(columns=["province_province_id"]) if not province.empty else address
)
if not stg_address.empty:
    save(stg_address, "stg_address")


stg_store = (store.merge(stg_address.add_prefix("addr_"), left_on="address_id", right_on="addr_address_id", how="left")) if not store.empty else pd.DataFrame()
if not stg_store.empty:
    save(stg_store, "stg_store")


stg_product_category = (product_category.merge(product_category.add_prefix("parent_"), left_on="parent_id", right_on="parent_category_id", how="left")) if not product_category.empty else pd.DataFrame()
if not stg_product_category.empty:
    save(stg_product_category, "stg_product_category")


stg_product = (product.merge(stg_product_category[["category_id", "name", "parent_name"]].rename(columns={"name": "category_name", "parent_name": "parent_category_name"}), on="category_id", how="left")) if not product.empty else pd.DataFrame()
if not stg_product.empty:
    save(stg_product, "stg_product")


if not customer.empty:
    save(customer, "stg_customer")


if not channel.empty:
    save(channel, "stg_channel")


if not province.empty:
    save(province, "stg_province")


addr_cols = ["address_id", "line1", "line2", "city", "province_id", "postal_code", "country_code", "created_at"]
billing = stg_address[["address_id", "city", "province_id", "province_name"]].rename(columns={"address_id": "billing_address_id", "city": "billing_city", "province_id": "billing_province_id", "province_name": "billing_province_name"})
shipping = stg_address[["address_id", "city", "province_id", "province_name"]].rename(columns={"address_id": "shipping_address_id", "city": "shipping_city", "province_id": "shipping_province_id", "province_name": "shipping_province_name"})

stg_sales_order = (sales_order.merge(channel.add_prefix("channel_"), left_on="channel_id", right_on="channel_channel_id", how="left")
                  .merge(customer.add_prefix("cust_"), left_on="customer_id", right_on="cust_customer_id", how="left")
                  .merge(stg_store.add_prefix("store_"), left_on="store_id", right_on="store_store_id", how="left")
                  .merge(billing, on="billing_address_id", how="left")
                  .merge(shipping, on="shipping_address_id", how="left")) if not sales_order.empty else pd.DataFrame()
if not stg_sales_order.empty:
    save(stg_sales_order, "stg_sales_order")


stg_sales_order_item = (sales_item.merge(stg_product.add_prefix("prod_"), left_on="product_id", right_on="prod_product_id", how="left")
                       .merge(stg_sales_order.add_prefix("ord_"), left_on="order_id", right_on="ord_order_id", how="left")) if not sales_item.empty else pd.DataFrame()
if not stg_sales_order_item.empty:
    save(stg_sales_order_item, "stg_sales_order_item")


stg_payment = (payment.merge(stg_sales_order.add_prefix("ord_"), left_on="order_id", right_on="ord_order_id", how="left")) if not payment.empty else pd.DataFrame()
if not stg_payment.empty:
    save(stg_payment, "stg_payment")


stg_shipment = (shipment.merge(stg_sales_order.add_prefix("ord_"), left_on="order_id", right_on="ord_order_id", how="left")) if not shipment.empty else pd.DataFrame()
if not stg_shipment.empty:
    save(stg_shipment, "stg_shipment")


stg_web_session = web_session.merge(customer.add_prefix("cust_"), left_on="customer_id", right_on="cust_customer_id", how="left") if not web_session.empty else pd.DataFrame()
if not stg_web_session.empty:
    save(stg_web_session, "stg_web_session")


stg_nps = (nps.merge(channel.add_prefix("channel_"), left_on="channel_id", right_on="channel_channel_id", how="left")
           .merge(customer.add_prefix("cust_"), left_on="customer_id", right_on="cust_customer_id", how="left")) if not nps.empty else pd.DataFrame()
if not stg_nps.empty:
    save(stg_nps, "stg_nps_response")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Desnormalizador: crea archivos STAGING a partir de RAW CSVs")
    parser.add_argument("--raw-dir", default=str(RAW_DIR), help="Path to raw CSV folder")
    parser.add_argument("--staging-dir", default=str(STAGING_DIR), help="Path to write staging CSVs")
    parser.add_argument("--zip-raw", default=None, help="If set, create a ZIP archive of all raw CSVs at this path")
    args = parser.parse_args(argv)

    raw_dir = Path(args.raw_dir)
    staging_dir = Path(args.staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    print(f"RAW_DIR = {raw_dir}")
    print(f"STAGING_DIR = {staging_dir}")

    if args.zip_raw:
        archive_csvs(raw_dir, Path(args.zip_raw))

    print("✅ Staging listo: archivos desnormalizados en carpeta", staging_dir)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
 
