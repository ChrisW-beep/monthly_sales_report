import os
import re
import pandas as pd
import zipfile
from dbfread import DBF

# 1) Folders / output
LOCAL_DOWNLOAD_PATH = "/tmp/data/"        # where the .zip files land
LOCAL_EXTRACT_PATH = "/tmp/extracted/"    # unzip destination
REPORT_PATH = "./reports/monthly_sales_report.csv"

def extract_zip_files(zip_dir, extract_to):
    """
    Unzips all *.zip files in 'zip_dir' into 'extract_to'.
    Returns a list of (store_id, extracted_folder_path) tuples for each file.
    """
    os.makedirs(extract_to, exist_ok=True)
    store_folders = []

    for fname in os.listdir(zip_dir):
        if fname.lower().endswith(".zip"):
            # Parse store_id from zip file name: e.g. "1234.zip" -> "1234"
            store_id = re.sub(r"\.zip$", "", fname, flags=re.IGNORECASE)

            # Create subfolder for each store’s extracted data
            subfolder = os.path.join(extract_to, store_id)
            os.makedirs(subfolder, exist_ok=True)

            zip_path = os.path.join(zip_dir, fname)
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(subfolder)

            store_folders.append((store_id, subfolder))

    return store_folders


def read_dbf_to_df(dbf_path):
    """
    Reads a DBF file into a pandas DataFrame using dbfread.
    """
    table = DBF(dbf_path, load=True)
    return pd.DataFrame(iter(table))


def process_store_data(store_id, folder_path):
    """
    Reads the DBF files in `folder_path`, merges them on SALE, filters by cat.CODE == 'N',
    and sums jnl.price for those lines. Returns a DataFrame of:
        Astoreid, Storename, date, Type, sale_amount, sale_count, currency
    """

    # 2) Look for the DBF files in the unzipped folder
    str_dbf = os.path.join(folder_path, "str.dbf")
    cat_dbf = os.path.join(folder_path, "cat.dbf")
    jnh_dbf = os.path.join(folder_path, "jnh.dbf")
    jnl_dbf = os.path.join(folder_path, "jnl.dbf")

    # 3) Read each DBF if it exists
    df_str = read_dbf_to_df(str_dbf) if os.path.isfile(str_dbf) else pd.DataFrame()
    df_cat = read_dbf_to_df(cat_dbf) if os.path.isfile(cat_dbf) else pd.DataFrame()
    df_jnh = read_dbf_to_df(jnh_dbf) if os.path.isfile(jnh_dbf) else pd.DataFrame()
    df_jnl = read_dbf_to_df(jnl_dbf) if os.path.isfile(jnl_dbf) else pd.DataFrame()

    # 4) Determine store name (fallback is store_id)
    if not df_str.empty and "NAME" in df_str.columns:
        store_name = df_str.iloc[0]["NAME"]
    else:
        store_name = store_id

    # 5) Merge jnl + jnh on 'SALE' to get the date from jnh
    #    We’re ignoring jnh’s "total sale amount" column if it exists,
    #    because we want to sum jnl’s line-level prices.
    if not df_jnh.empty and not df_jnl.empty:
        df_merged = pd.merge(
            df_jnl,                 # line-level
            df_jnh[["SALE", "DATE"]],  # only columns we need from jnh
            on="SALE",
            how="left"
        )
    else:
        df_merged = df_jnl.copy()

    # 6) Merge cat to bring in category name. jnl["CAT_CODE"] matches cat["CODE"].
    #    We’ll later filter to cat.CODE == 'N'.
    if not df_cat.empty and "CODE" in df_cat.columns:
        df_merged = pd.merge(
            df_merged,
            df_cat[["CODE", "NAME"]],
            left_on="CAT_CODE",
            right_on="CODE",
            how="left"
        )
    else:
        # If cat is missing, fill in nulls
        df_merged["CODE"] = None
        df_merged["NAME"] = None

    # 7) Filter so we only keep lines where cat.CODE == 'N'
    df_merged = df_merged[df_merged["CODE"] == "N"].copy()

    # 8) Sum jnl.price for each date+cat.name
    #    - If your line price column is named "PRICE", replace "AMT" below with "PRICE"
    #    - We'll also count how many lines contributed
    #    - The "dropna=False" ensures we keep NaN groups if any exist
    grouped = (
        df_merged
        .groupby(["DATE", "NAME"], dropna=False)
        .agg(
            sale_amount=("PRICE", "sum"),  # or ("PRICE", "sum") if your column is PRICE
            sale_count=("PRICE", "count")  # or ("PRICE", "count")
        )
        .reset_index()
    )

    # 9) Add store info and rename columns
    grouped.insert(0, "Astoreid", store_id)
    grouped.insert(1, "Storename", store_name)
    grouped["currency"] = "USD"
    grouped.rename(columns={"DATE": "date", "NAME": "Type"}, inplace=True)

    return grouped


def main():
    # Ensure directories exist
    os.makedirs(LOCAL_EXTRACT_PATH, exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    # 1) Unzip each store file
    store_folders = extract_zip_files(LOCAL_DOWNLOAD_PATH, LOCAL_EXTRACT_PATH)

    # 2) Process each store’s DBFs
    all_dfs = []
    for store_id, folder_path in store_folders:
        df_store = process_store_data(store_id, folder_path)
        if not df_store.empty:
            all_dfs.append(df_store)

    # 3) Combine all
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
    else:
        # Empty fallback with correct columns
        final_df = pd.DataFrame(columns=["Astoreid","Storename","date","Type","sale_amount","sale_count","currency"])

    # 4) Output monthly_sales_report.csv
    final_df.to_csv(REPORT_PATH, index=False)
    print(f"Monthly Sales Report generated at: {REPORT_PATH}")


if __name__ == "__main__":
    main()
