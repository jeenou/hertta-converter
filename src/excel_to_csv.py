import os
import pandas as pd


def create_folder_structure(base_dir: str) -> dict:
    output_dir = os.path.join(base_dir, "output")
    csv_dir = os.path.join(output_dir, "csv")
    graphql_dir = os.path.join(output_dir, "graphql")

    os.makedirs(csv_dir, exist_ok=True)
    os.makedirs(graphql_dir, exist_ok=True)

    return {
        "output": output_dir,
        "csv": csv_dir,
        "graphql": graphql_dir,
    }


def excel_to_csv(excel_path: str, csv_dir: str):
    print(f"Reading Excel file: {excel_path}")
    xls = pd.ExcelFile(excel_path)

    for sheet_name in xls.sheet_names:
        print(f"  → Converting sheet: {sheet_name}")
        df = pd.read_excel(excel_path, sheet_name=sheet_name)

        safe_name = "".join(
            c for c in sheet_name if c.isalnum() or c in (" ", "_", "-")
        ).rstrip()
        if not safe_name:
            safe_name = "sheet"

        csv_path = os.path.join(csv_dir, f"{safe_name}.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8")

        print(f"     Saved: {csv_path}")

    print("Excel → CSV conversion done!")
