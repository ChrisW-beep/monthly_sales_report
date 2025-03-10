import os
import pandas as pd
import zipfile
from dbfread import DBF

LOCAL_DOWNLOAD_PATH = "./data/"
LOCAL_EXTRACT_PATH = "./extracted/"
REPORT_PATH = "./reports/monthly_sales_report.csv"


def extract_zip_files(zip_dir, extract_to):
    os.makedirs(extract_to, exist_ok=True)
    for file in os.listdir(zip_dir):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(zip_dir, file), "r") as zip_ref:
                zip_ref.extractall(extract_to)


def read_dbf_to_df(dbf_file):
    table = DBF(dbf_file, load=True)
    df = pd.DataFrame(iter(table))
    return df


def analyze_sales(df):
    # Example analysis, adjust according to actual columns
    summary = (
        df.groupby("item")
        .agg(total_quantity=("quantity", "sum"), total_sales=("totalprice", "sum"))
        .reset_index()
    )
    return summary


def main():
    zip_dir = "./data/"
    extract_to = "./extracted/"
    report_output = "./reports/monthly_sales_report.csv"

    os.makedirs(extract_to, exist_ok=True)
    os.makedirs("./reports/", exist_ok=True)

    # Extract zipped files
    for file in os.listdir(zip_dir := LOCAL_DOWNLOAD_PATH):
        if file.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(zip_dir, file), "r") as z:
                zip_ref.extractall(extract_to)

    # Read DBFs into a single DataFrame
    combined_df = pd.DataFrame()
    for root, _, files in os.walk(extract_to):
        for file in files:
            if file.lower().endswith(".dbf"):
                dbf_path = os.path.join(root := extract_to, file)
                df = read_dbf_to_df(dbf_path)
                combined_df = (
                    pd.concat([combined_df, df]) if "combined_df" in locals() else df
                )

    # Analyze Data (Adjust column names as per actual DBFs)
    summary = (
        combined_df.groupby("item")
        .agg({"quantity": "sum", "price": "mean"})
        .reset_index()
    )

    summary_report = summary.sort_values("quantity", ascending=False)
    summary_report.to_csv(report_output, index=False)

    print(f"Report generated at: {report_output}")


if __name__ == "__main__":
    main()
