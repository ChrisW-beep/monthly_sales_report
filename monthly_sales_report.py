import os
import re
import pandas as pd
import zipfile
from dbfread import DBF

LOCAL_DOWNLOAD_PATH = "/tmp/data/"
LOCAL_EXTRACT_PATH = "/tmp/extracted/"
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
            # Parse Astoreid from the ZIP file name (e.g. "1234.zip" -> "1234")
            # Adjust the parsing logic as needed if your filenames have different formats
            store_id = re.sub(r"\.zip$", "", fname, flags=re.IGNORECASE)

            # Create a subfolder for each store’s extracted data
            subfolder = os.path.join(extract_to, store_id)
            os.makedirs(subfolder, exist_ok=True)

            zip_path = os.path.join(zip_dir, fname)
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(subfolder)

            store_folders.append((store_id, subfolder))

    return store_folders


def read_dbf_to_df(dbf_file):
    """Reads a DBF file into a pandas DataFrame."""
    table = DBF(dbf_file, load=True)
    df = pd.DataFrame(iter(table))
    return df


def process_store_data(store_id, folder_path):
    """
    Reads the necessary DBFs in `folder_path` and produces
    a DataFrame containing:
      Astoreid, Storename, date, Type, sale_amount, sale_count, currency
    filtered so that only cat.code == 'N' is used.
    """

    # These are guesses at the DBF filenames; adjust them as appropriate
    str_dbf_path = os.path.join(folder_path, "str.dbf")
    cat_dbf_path = os.path.join(folder_path, "cat.dbf")
    jnh_dbf_path = os.path.join(folder_path, "jnh.dbf")
    jnl_dbf_path = os.path.join(folder_path, "jnl.dbf")

    # Read each DBF if it exists. Some might not be present in every store’s folder
    df_str = read_dbf_to_df(str_dbf_path) if os.path.isfile(str_dbf_path) else pd.DataFrame()
    df_cat = read_dbf_to_df(cat_dbf_path) if os.path.isfile(cat_dbf_path) else pd.DataFrame()
    df_jnh = read_dbf_to_df(jnh_dbf_path) if os.path.isfile(jnh_dbf_path) else pd.DataFrame()
    df_jnl = read_dbf_to_df(jnl_dbf_path) if os.path.isfile(jnl_dbf_path) else pd.DataFrame()

    # Extract store name from STR (assuming it has a field called 'NAME').
    # If there might be multiple records, adjust accordingly (e.g. pick the first).
    store_name = None
    if not df_str.empty and "NAME" in df_str.columns:
        store_name = df_str.iloc[0]["NAME"]
    else:
        # Fallback: store_name could just be the store_id if not found in STR
        store_name = store_id

 
    if not df_jnh.empty and not df_jnl.empty:
        # rename columns as needed
        # Example fields:
        #   JNH["DOC_NUM"], JNH["DATE"] -> the date
        #   JNL["DOC_NUM"], JNL["CAT_CODE"], JNL["AMT"] -> line-level data
        df_merged = pd.merge(
            df_jnl,
            df_jnh[["SALE", "DATE"]], 
            on="SALE",
            how="left"
        )
    else:
        df_merged = df_jnl.copy()

    # 2) Merge in CAT so we can filter cat.code == "N" and get cat.name as 'Type'
    #    Suppose CAT has columns "CODE" and "NAME". We match JNL["CAT_CODE"] == CAT["CODE"].
    if not df_cat.empty and "CODE" in df_cat.columns:
        df_merged = pd.merge(
            df_merged,
            df_cat[["CODE", "NAME"]],  # e.g. cat.df columns
            left_on="CAT_CODE", 
            right_on="CODE",
            how="left"
        )
    else:
        df_merged["CODE"] = None
        df_merged["NAME"] = None

    # Now filter so we only keep cat.code == 'N'
    # (Note that JNL might store the category code in some column, we used "CAT_CODE" -> df_merged["CODE"]
    df_merged = df_merged[df_merged["CODE"] == "N"].copy()

    # Summaries:
    #   - Astoreid   -> store_id
    #   - Storename  -> store_name
    #   - date       -> use the merged date from JNH or JNL (df_merged["DATE"])
    #   - Type       -> df_merged["NAME"] (the cat name)
    #   - sale_amount -> sum of line amounts (say, df_merged["AMT"])
    #   - sale_count  -> number of lines
    #   - currency    -> "USD"

    # If you need per-date grouping, we can group by date + cat.name
    # If you want a single total, skip grouping by date.
    # Below example groups by [date, cat.name].
    grouped = (
        df_merged
        .groupby(["DATE", "NAME"], dropna=False)
        .agg(
            sale_amount=("AMT", "sum"),
            sale_count=("AMT", "count")  # or count any non-null field
        )
        .reset_index()
    )

    # Insert the columns that do not come from the groupby
    grouped.insert(0, "Astoreid", store_id)
    grouped.insert(1, "Storename", store_name)
    grouped["currency"] = "USD"

    # Rename columns to match your final desired output exactly
    grouped.rename(
        columns={
            "DATE": "date",
            "NAME": "Type"
        },
        inplace=True
    )

    return grouped


def main():
    # Ensure output directories exist
    os.makedirs(LOCAL_EXTRACT_PATH, exist_ok=True)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    # 1) Extract each zip and keep track of which store_id -> which folder
    store_folders = extract_zip_files(LOCAL_DOWNLOAD_PATH, LOCAL_EXTRACT_PATH)

    # 2) For each store’s extracted data, produce a DataFrame
    all_results = []
    for store_id, folder_path in store_folders:
        df_store = process_store_data(store_id, folder_path)
        if not df_store.empty:
            all_results.append(df_store)

    # 3) Combine results for all stores
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
    else:
        final_df = pd.DataFrame(
            columns=["Astoreid","Storename","date","Type","sale_amount","sale_count","currency"]
        )

    # 4) Output the aggregated results to CSV
    final_df.to_csv(REPORT_PATH, index=False)
    print(f"Monthly Sales Report generated at: {REPORT_PATH}")


if __name__ == "__main__":
    main()
