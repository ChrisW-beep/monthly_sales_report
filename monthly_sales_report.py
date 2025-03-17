import os
import pandas as pd
from dbfread import DBF

# Adjust to your environment
LOCAL_UNZIPPED_BASE = "/tmp/extracted/"
REPORT_PATH = "./reports/monthly_sales_report.csv"


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
            # Use folder name as the store_id
            store_id = entry.name
            store_folder = os.path.join(base_dir, store_id)
            store_folders.append((store_id, store_folder))
    return store_folders


def read_dbf_to_df(dbf_path):
    """
    Safely reads a DBF file into a pandas DataFrame using dbfread.
    Returns an empty DataFrame if the file doesn't exist.
    """
    if not os.path.isfile(dbf_path):
        print(f"Warning: Missing DBF: {dbf_path}")
        return pd.DataFrame()
    table = DBF(dbf_path, load=True)
    return pd.DataFrame(iter(table))


def process_store_data(store_id, folder_path):
    """
    Reads str.dbf, cat.dbf, jnh.dbf, jnl.dbf from `folder_path`.
    Tries to do merges (on CAT, SALE, etc.) if columns exist,
    and gracefully falls back if they're missing.

    Final output is a DataFrame with columns:
       Astoreid, Storename, date, Type, sale_amount, sale_count, currency
    filtered so that cat.CODE == 'N' where possible.
    """
    # 1) Identify DBF file paths
    str_dbf = os.path.join(folder_path, "str.dbf")
    cat_dbf = os.path.join(folder_path, "cat.dbf")
    jnh_dbf = os.path.join(folder_path, "jnh.dbf")
    jnl_dbf = os.path.join(folder_path, "jnl.dbf")

    # 2) Load DBFs into DataFrames (empty if missing)
    df_str = read_dbf_to_df(str_dbf)
    df_cat = read_dbf_to_df(cat_dbf)
    df_jnh = read_dbf_to_df(jnh_dbf)
    df_jnl = read_dbf_to_df(jnl_dbf)

    # 3) Determine store name, fallback to store_id if missing
    if not df_str.empty and "NAME" in df_str.columns:
        store_name = str(df_str.iloc[0]["NAME"])
    else:
        store_name = str(store_id)

    # 4) If jnl is empty, nothing to process for this store
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

    # 5) Merge jnl + jnh on SALE if possible
    df_merged = df_jnl.copy()
    if not df_jnh.empty and "SALE" in df_jnh.columns and "SALE" in df_merged.columns:
        if "DATE" in df_jnh.columns:
            df_merged = pd.merge(
                df_merged, df_jnh[["SALE", "DATE"]], on="SALE", how="left"
            )
        else:
            # If jnh is missing "DATE", we just skip
            print(
                f"Warning: jnh missing DATE column for store {store_id}. Skipping date merge."
            )
    else:
        if df_jnh.empty:
            print(f"Warning: No jnh data for store {store_id}. Skipping jnh merge.")
        else:
            missing_cols = [c for c in ["SALE", "DATE"] if c not in df_jnh.columns]
            print(
                f"Warning: jnh missing columns {missing_cols} for store {store_id}. Skipping date merge."
            )

    # 6) Merge with cat on "CAT" if columns are present
    if not df_cat.empty and "CAT" in df_cat.columns and "CAT" in df_merged.columns:
        # Merge directly on "CAT"
        df_merged = pd.merge(
            df_merged,
            df_cat,  # includes 'CAT', 'CODE', 'NAME' presumably
            on="CAT",
            how="left",
        )
    else:
        # If we can't merge, fill in placeholders
        df_merged["CODE"] = None
        df_merged["NAME"] = None
        if df_cat.empty:
            print(f"Warning: cat.dbf missing or empty for store {store_id}.")
        else:
            missing_cols = []
            if "CAT" not in df_cat.columns:
                missing_cols.append("'CAT' not in cat")
            if "CAT" not in df_merged.columns:
                missing_cols.append("'CAT' not in jnl")
            print(
                f"Warning: Could not merge cat for store {store_id} because {missing_cols}."
            )

    # 7) Filter for cat.CODE == 'N' if 'CODE' is present
    if "CODE" in df_merged.columns:
        df_merged = df_merged[df_merged["CODE"] == "N"].copy()
    else:
        print(
            f"Warning: Merged data missing 'CODE' column, skipping cat.CODE == 'N' filter."
        )

    # 8) Determine line-level amount
    #    - If PRICE & QTY -> line_amount = PRICE * QTY
    #    - Else if AMT -> line_amount = AMT
    #    - Else fallback 0
    if "PRICE" in df_merged.columns:
        if "QTY" in df_merged.columns:
            df_merged["line_amount"] = df_merged["PRICE"] * df_merged["QTY"]
        else:
            df_merged["line_amount"] = df_merged["PRICE"]
    elif "AMT" in df_merged.columns:
        df_merged["line_amount"] = df_merged["AMT"]
    else:
        df_merged["line_amount"] = 0
        print(f"Warning: No PRICE or AMT columns found in jnl for store {store_id}.")

    # 9) Group by date + cat.NAME if possible
    #    Some stores might not have 'DATE' or 'NAME'.
    #    We'll group on whichever columns exist.
    group_cols = []
    if "DATE" in df_merged.columns:
        group_cols.append("DATE")
    if "NAME" in df_merged.columns:
        group_cols.append("NAME")

    if group_cols:
        grouped = (
            df_merged.groupby(group_cols, dropna=False)
            .agg(
                sale_amount=("line_amount", "sum"), sale_count=("line_amount", "count")
            )
            .reset_index()
        )
    else:
        # If there's no date or name column, we just sum everything
        total_line_amount = df_merged["line_amount"].sum()
        line_count = df_merged["line_amount"].count()
        # Create a single-row DataFrame
        grouped = pd.DataFrame(
            [{"sale_amount": total_line_amount, "sale_count": line_count}]
        )

    # 10) Insert store metadata, currency
    grouped.insert(0, "Astoreid", store_id)
    grouped.insert(1, "Storename", store_name)
    grouped["currency"] = "USD"

    # 11) Rename columns if we do have them
    if "DATE" in grouped.columns:
        grouped.rename(columns={"DATE": "date"}, inplace=True)
    if "NAME" in grouped.columns:
        grouped.rename(columns={"NAME": "Type"}, inplace=True)

    # Ensure final DataFrame has these columns, at least empty:
    for col in [
        "Astoreid",
        "Storename",
        "date",
        "Type",
        "sale_amount",
        "sale_count",
        "currency",
    ]:
        if col not in grouped.columns:
            grouped[col] = None

    return grouped[
        [
            "Astoreid",
            "Storename",
            "date",
            "Type",
            "sale_amount",
            "sale_count",
            "currency",
        ]
    ]


def main():
    # Ensure the directory for the CSV output exists
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    # 1) Identify all store subfolders
    store_folders = find_store_folders(LOCAL_UNZIPPED_BASE)

    # 2) Process each store, accumulate results
    all_dfs = []
    for store_id, folder_path in store_folders:
        df_store = process_store_data(store_id, folder_path)
        if not df_store.empty:
            all_dfs.append(df_store)

    # 3) Combine all stores
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

    # 4) Write final CSV
    final_df.to_csv(REPORT_PATH, index=False)
    print(f"Monthly Sales Report generated at: {REPORT_PATH}")


if __name__ == "__main__":
    main()
