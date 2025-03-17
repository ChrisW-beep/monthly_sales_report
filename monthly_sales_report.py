import os
import re
import pandas as pd
from dbfread import DBF

# Adjust these to your environment
LOCAL_UNZIPPED_BASE = "/tmp/extracted/"            # Where Jenkins has already unzipped everything
REPORT_PATH = "./reports/monthly_sales_report.csv"  # Final aggregated CSV output

def find_store_folders(base_dir):
    """
    Looks inside 'base_dir' for subdirectories,
    each representing a store’s unzipped DBFs.
    Returns a list of tuples: (store_id, store_folder).
    """
    store_folders = []
    if not os.path.isdir(base_dir):
        print(f"Warning: {base_dir} does not exist or is not a directory.")
        return store_folders

    for entry in os.scandir(base_dir):
        if entry.is_dir():
            # Use folder name as the store_id. 
            # If your folders are named '1234', '5678', etc., this works.
            # Or parse differently if your naming is more complex.
            store_id = entry.name
            store_folder = os.path.join(base_dir, store_id)
            store_folders.append((store_id, store_folder))

    return store_folders

def read_dbf_to_df(dbf_path):
    """
    Reads a DBF file into a pandas DataFrame using dbfread.
    Returns an empty DF if the file does not exist.
    """
    if not os.path.isfile(dbf_path):
        print(f"Warning: DBF not found: {dbf_path}")
        return pd.DataFrame()

    table = DBF(dbf_path, load=True)
    return pd.DataFrame(iter(table))

def process_store_data(store_id, folder_path):
    """
    Reads the DBFs in 'folder_path': str.dbf, cat.dbf, jnh.dbf, jnl.dbf.
    Produces a DataFrame with columns:
        Astoreid, Storename, date, Type, sale_amount, sale_count, currency
    filtered so that cat.code == 'N'.
    Merges line-level amounts from jnl with jnh (for date) 
    and cat (for category name).
    """
    # Paths (adjust names if your DBFs differ)
    str_dbf = os.path.join(folder_path, "str.dbf")
    cat_dbf = os.path.join(folder_path, "cat.dbf")
    jnh_dbf = os.path.join(folder_path, "jnh.dbf")
    jnl_dbf = os.path.join(folder_path, "jnl.dbf")

    # Read DBFs if they exist
    df_str = read_dbf_to_df(str_dbf)
    df_cat = read_dbf_to_df(cat_dbf)
    df_jnh = read_dbf_to_df(jnh_dbf)
    df_jnl = read_dbf_to_df(jnl_dbf)

    # Extract store name from str.dbf
    if not df_str.empty and "NAME" in df_str.columns:
        store_name = df_str.iloc[0]["NAME"]
    else:
        store_name = store_id  # fallback if missing

    # Merge jnl + jnh on "SALE" to get the date
    if not df_jnh.empty and not df_jnl.empty:
        # jnh must have columns "SALE" and "DATE"
        # jnl must have "SALE" as well
        df_merged = pd.merge(
            df_jnl,
            df_jnh[["SALE", "DATE"]], 
            on="SALE",
            how="left"
        )
    else:
        df_merged = df_jnl.copy()

    # Merge cat on jnl["CAT_CODE"] == cat["CODE"] 
    # to bring in the category name
    if not df_cat.empty and "CODE" in df_cat.columns:
        df_merged = pd.merge(
            df_merged,
            df_cat[["CODE", "NAME"]],
            left_on="CAT_CODE",
            right_on="CODE",
            how="left"
        )
    else:
        df_merged["CODE"] = None
        df_merged["NAME"] = None

    # Filter for cat.code == 'N'
    df_merged = df_merged[df_merged["CODE"] == "N"].copy()

    # Determine line-level amount from jnl
    # If you have "PRICE" plus "QTY", do PRICE * QTY
    # If you have "AMT" directly, just rename it here.
    if "PRICE" in df_merged.columns:
        if "QTY" in df_merged.columns:
            df_merged["line_amount"] = df_merged["PRICE"] * df_merged["QTY"]
        else:
            df_merged["line_amount"] = df_merged["PRICE"]
    elif "AMT" in df_merged.columns:
        df_merged["line_amount"] = df_merged["AMT"]
    else:
        df_merged["line_amount"] = 0

    # Group by date + category name, sum line_amount, count lines
    grouped = (
        df_merged
        .groupby(["DATE", "NAME"], dropna=False)
        .agg(
            sale_amount=("line_amount", "sum"),
            sale_count=("line_amount", "count")
        )
        .reset_index()
    )

    # Insert store info
    grouped.insert(0, "Astoreid", store_id)
    grouped.insert(1, "Storename", store_name)
    grouped["currency"] = "USD"

    # Rename columns to final output
    grouped.rename(
        columns={
            "DATE": "date",
            "NAME": "Type"
        },
        inplace=True
    )

    return grouped

def main():
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    # 1) Find all store subdirectories
    store_folders = find_store_folders(LOCAL_UNZIPPED_BASE)

    # 2) For each store, process DBFs
    all_dfs = []
    for store_id, folder_path in store_folders:
        df_store = process_store_data(store_id, folder_path)
        if not df_store.empty:
            all_dfs.append(df_store)

    # 3) Combine results
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
    else:
        final_df = pd.DataFrame(columns=["Astoreid","Storename","date","Type","sale_amount","sale_count","currency"])

    # 4) Save final CSV
    final_df.to_csv(REPORT_PATH, index=False)
    print(f"Monthly Sales Report generated at: {REPORT_PATH}")


if __name__ == "__main__":
    main()
