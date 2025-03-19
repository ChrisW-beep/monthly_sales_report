import os
import pandas as pd
from dbfread import DBF

LOCAL_UNZIPPED_BASE = "/tmp/extracted/6045/Data"  # Hard-coded path
REPORT_PATH = "./reports/monthly_sales_report.csv"

import os
import pandas as pd
from dbfread import DBF

def find_dbf_filename(folder_path, base_name):
    """
    Searches `folder_path` for a file whose name (ignoring case)
    matches `base_name + ".dbf"`.
    e.g. if base_name="str", we look for "str.dbf" in any case: STR.DBF, sTr.Dbf, etc.

    Returns the actual file name if found (e.g. "STR.DBF"), else None.
    """
    target = (base_name + ".dbf").lower()  # e.g. "str.dbf"
    if not os.path.isdir(folder_path):
        return None

    for fname in os.listdir(folder_path):
        if fname.lower() == target:
            return fname  # e.g. "STR.DBF"
    return None

def read_dbf_to_df(folder_path, base_name):
    """
    Combines find_dbf_filename + dbfread to read the DBF, ignoring case.
    Returns a DataFrame (empty if file not found).
    """
    dbf_file = find_dbf_filename(folder_path, base_name)
    if not dbf_file:
        print(f"Warning: Could not find {base_name}.dbf (any case) in {folder_path}")
        return pd.DataFrame()

    dbf_path = os.path.join(folder_path, dbf_file)
    if not os.path.isfile(dbf_path):
        print(f"Warning: DBF file missing: {dbf_path}")
        return pd.DataFrame()

    table = DBF(dbf_path, load=True)
    return pd.DataFrame(iter(table))

# Example usage
if __name__ == "__main__":
    folder_path = "/tmp/extracted/6045/Data"  # for example
    df_str = read_dbf_to_df(folder_path, "str")  # finds str.dbf or STR.DBF etc.
    df_jnl = read_dbf_to_df(folder_path, "jnl")  # finds jnl.dbf or JNL.DBF etc.

    print("STR rows:", len(df_str))
    print("JNL rows:", len(df_jnl))


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
    """
    1) Reads str.dbf and jnl.dbf from 'folder_path'
    2) Merges & groups line 950/980 pairs by date + Type
    3) Returns a DataFrame with columns [Astoreid, Storename, date, Type, sale_amount, sale_count, currency]
    """
    df_str = read_dbf_to_df(folder_path, "str")
    df_jnl = read_dbf_to_df(folder_path, "jnl")


    # Store name
    if not df_str.empty and "NAME" in df_str.columns:
        store_name = str(df_str.iloc[0]["NAME"])
    else:
        store_name = store_id

    if df_jnl.empty:
        print(f"Warning: No jnl data for store {store_id}.")
        return pd.DataFrame(columns=["Astoreid","Storename","date","Type","sale_amount","sale_count","currency"])

    # Normalize "Line", "Price", "Descript", "Date"
    found_line     = normalize_column(df_jnl, "Line")
    found_price    = normalize_column(df_jnl, "Price")
    found_descript = normalize_column(df_jnl, "Descript")
    found_date     = normalize_column(df_jnl, "Date")

    # If missing, create fallback columns
    if not found_line:
        print(f"Warning: 'Line' column missing for store {store_id}. {df_jnl.columns.tolist()}")
        df_jnl["Line"] = None
    if not found_price:
        print(f"Warning: 'Price' column missing for store {store_id}. {df_jnl.columns.tolist()}")
        df_jnl["Price"] = 0
    if not found_descript:
        print(f"Warning: 'Descript' column missing for store {store_id}. {df_jnl.columns.tolist()}")
        df_jnl["Descript"] = None
    if not found_date:
        print(f"Warning: 'Date' column missing for store {store_id}. {df_jnl.columns.tolist()}")
        df_jnl["Date"] = None

    # Convert "Line" to string, "Price" to numeric
    df_jnl["Line"] = df_jnl["Line"].astype(str)
    df_jnl["Price"] = pd.to_numeric(df_jnl["Price"], errors="coerce").fillna(0)

    # Convert date to YYYY-MM-DD if possible
    if df_jnl["Date"].notnull().any():
        try:
            df_jnl["Date"] = pd.to_datetime(df_jnl["Date"]).dt.strftime("%Y-%m-%d")
        except Exception as e:
            print(f"Warning: Error converting date for store {store_id}: {e}")
            df_jnl["Date"] = df_jnl["Date"].astype(str)
    else:
        df_jnl["Date"] = None

    # Keep the row order
    df_jnl.reset_index(drop=False, inplace=True)

    # Build pairs from consecutive (950->980) lines
    pairs = []
    for i in range(len(df_jnl) - 1):
        line_val = df_jnl.loc[i, "Line"]
        price_val= df_jnl.loc[i, "Price"]
        date_val = df_jnl.loc[i, "Date"]
        line_next= df_jnl.loc[i+1, "Line"]
        desc_next= df_jnl.loc[i+1, "Descript"]

        if line_val == "950" and line_next == "980":
            pairs.append({
                "date": date_val,
                "Type": desc_next,
                "sale_amount": price_val,
                "sale_count": 1
            })

    df_pairs = pd.DataFrame(pairs)
    if df_pairs.empty:
        print(f"Warning: No (950->980) pairs found for store {store_id}.")
        return pd.DataFrame(columns=["Astoreid","Storename","date","Type","sale_amount","sale_count","currency"])

    # Group by (date, Type)
    grouped = (
        df_pairs
        .groupby(["date","Type"], dropna=False)
        .agg(
            sale_amount=("sale_amount","sum"),
            sale_count=("sale_count","sum")
        )
        .reset_index()
    )

    # Insert store metadata
    grouped.insert(0, "Astoreid", store_id)
    grouped.insert(1, "Storename", store_name)
    grouped["currency"] = "USD"

    # Final columns
    expected = ["Astoreid","Storename","date","Type","sale_amount","sale_count","currency"]
    for col in expected:
        if col not in grouped.columns:
            grouped[col] = None
    grouped = grouped[expected]
    return grouped

def main():
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    # Hard-coded store ID & path (just for a single store test)
    store_id = "6045"
    folder_path = LOCAL_UNZIPPED_BASE

    # Process the DBFs directly
    df_store = process_store_data(store_id, folder_path)
    if df_store.empty:
        final_df = pd.DataFrame(columns=["Astoreid","Storename","date","Type","sale_amount","sale_count","currency"])
    else:
        final_df = df_store

    # (Optional) If you want to filter by the current month
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
