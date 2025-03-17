import os
import pandas as pd
from dbfread import DBF

LOCAL_UNZIPPED_BASE = "/tmp/extracted/<storeid>"
REPORT_PATH = "./reports/monthly_sales_report.csv"


def find_store_folders(base_dir):
    """
    Returns a list of (store_id, store_path) for each subdirectory in base_dir.
    """
    store_folders = []
    if not os.path.isdir(base_dir):
        print(f"Warning: {base_dir} does not exist.")
        return store_folders
    for entry in os.scandir(base_dir):
        if entry.is_dir():
            store_folders.append((entry.name, os.path.join(base_dir, entry.name)))
    return store_folders


def read_dbf_to_df(dbf_path):
    """
    Reads a DBF file into a DataFrame. Returns empty if missing.
    """
    if not os.path.isfile(dbf_path):
        print(f"Warning: Missing DBF: {dbf_path}")
        return pd.DataFrame()
    table = DBF(dbf_path, load=True)
    return pd.DataFrame(iter(table))


def normalize_column(df, target_name):
    """
    If df has a column whose .lower() matches target_name.lower(),
    rename it to exactly target_name and return True. Else return False.
    """
    for col in df.columns:
        if col.lower() == target_name.lower():
            df.rename(columns={col: target_name}, inplace=True)
            return True
    return False


def process_store_data(store_id, folder_path):
    # 1) Read str.dbf (optional store name) and jnl.dbf
    str_dbf = os.path.join(folder_path, "str.dbf")
    jnl_dbf = os.path.join(folder_path, "jnl.dbf")

    df_str = read_dbf_to_df(str_dbf)
    df_jnl = read_dbf_to_df(jnl_dbf)

    # 2) Determine store name
    if not df_str.empty and "NAME" in df_str.columns:
        store_name = str(df_str.iloc[0]["NAME"])
    else:
        store_name = store_id

    if df_jnl.empty:
        print(f"Warning: No jnl data for store {store_id}.")
        return pd.DataFrame(
            columns=[
                "Astoreid",
                "Storename",
                "date",
                "Type",
                "sale_amount",
                "sale_count",
                "currency",
            ]
        )

    # 3) Normalize columns we care about: "Line", "Price", "Descript", "Date"
    found_line = normalize_column(df_jnl, "Line")
    found_price = normalize_column(df_jnl, "Price")
    found_descript = normalize_column(df_jnl, "Descript")
    found_date = normalize_column(
        df_jnl, "Date"
    )  # In case it's spelled "DATE" or "date"

    # If missing, create fallback columns
    if not found_line:
        print(
            f"Warning: 'Line' column missing for store {store_id}. {df_jnl.columns.tolist()}"
        )
        df_jnl["Line"] = None
    if not found_price:
        print(
            f"Warning: 'Price' column missing for store {store_id}. {df_jnl.columns.tolist()}"
        )
        df_jnl["Price"] = 0
    if not found_descript:
        print(
            f"Warning: 'Descript' column missing for store {store_id}. {df_jnl.columns.tolist()}"
        )
        df_jnl["Descript"] = None
    if not found_date:
        print(
            f"Warning: 'Date' column missing for store {store_id}. {df_jnl.columns.tolist()}"
        )
        df_jnl["Date"] = None

    # Convert "Line" to string, "Price" to numeric, parse "Date"
    df_jnl["Line"] = df_jnl["Line"].astype(str)
    df_jnl["Price"] = pd.to_numeric(df_jnl["Price"], errors="coerce").fillna(0)

    # Attempt to parse the date into YYYY-MM-DD
    if df_jnl["Date"].notnull().any():
        try:
            df_jnl["Date"] = pd.to_datetime(df_jnl["Date"]).dt.strftime("%Y-%m-%d")
        except Exception as e:
            print(f"Warning: Error converting date for store {store_id}: {e}")
            df_jnl["Date"] = df_jnl["Date"].astype(str)
    else:
        df_jnl["Date"] = None

    # 4) Keep the original row order so we can pair consecutive rows
    df_jnl.reset_index(drop=False, inplace=True)

    # 5) Build a list of (date, type, sale_amount) from consecutive (950->980) pairs
    pairs = []

    for i in range(len(df_jnl) - 1):
        line_val = df_jnl.loc[i, "Line"]
        price_val = df_jnl.loc[i, "Price"]
        date_val = df_jnl.loc[i, "Date"]  # we assume the date is in line 950 row
        # Next row
        line_next = df_jnl.loc[i + 1, "Line"]
        desc_next = df_jnl.loc[i + 1, "Descript"]

        # If row i is line==950 and row i+1 is line==980
        if line_val == "950" and line_next == "980":
            pairs.append(
                {
                    "date": date_val,
                    "Type": desc_next,
                    "sale_amount": price_val,
                    "sale_count": 1,
                }
            )

    # Convert pairs to DataFrame
    df_pairs = pd.DataFrame(pairs)
    if df_pairs.empty:
        print(f"Warning: No (950->980) pairs found for store {store_id}.")
        return pd.DataFrame(
            columns=[
                "Astoreid",
                "Storename",
                "date",
                "Type",
                "sale_amount",
                "sale_count",
                "currency",
            ]
        )

    # 6) Group by (date, Type) => sum sale_amount, count
    grouped = (
        df_pairs.groupby(["date", "Type"], dropna=False)
        .agg(sale_amount=("sale_amount", "sum"), sale_count=("sale_count", "sum"))
        .reset_index()
    )

    # 7) Insert store metadata, currency
    grouped.insert(0, "Astoreid", store_id)
    grouped.insert(1, "Storename", store_name)
    grouped["currency"] = "USD"

    # Final columns
    expected = [
        "Astoreid",
        "Storename",
        "date",
        "Type",
        "sale_amount",
        "sale_count",
        "currency",
    ]
    for col in expected:
        if col not in grouped.columns:
            grouped[col] = None
    grouped = grouped[expected]
    return grouped


def main():
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    store_folders = find_store_folders(LOCAL_UNZIPPED_BASE)

    all_dfs = []
    for store_id, folder_path in store_folders:
        df_store = process_store_data(store_id, folder_path)
        if not df_store.empty:
            all_dfs.append(df_store)

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame(
            columns=[
                "Astoreid",
                "Storename",
                "date",
                "Type",
                "sale_amount",
                "sale_count",
                "currency",
            ]
        )

    # If you want to filter by the current month, do it now
    current_year = os.environ.get("YEAR")
    current_month = os.environ.get("MONTH")
    if current_year and current_month and "date" in final_df.columns:
        final_df["date"] = final_df["date"].astype(str)
        prefix = f"{current_year}-{current_month}"
        final_df = final_df[final_df["date"].str.startswith(prefix)]

    final_df.to_csv(REPORT_PATH, index=False)
    print(f"Monthly Sales Report generated at: {REPORT_PATH}")


if __name__ == "__main__":
    main()
