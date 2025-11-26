import os
import sys
import json

from excel_to_csv import create_folder_structure, excel_to_csv
from parse_setup import parse_setup_csv_to_inputdatasetup
from graphql_utils import build_graphql_payload, save_payload_to_file


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <excel_file>")
        sys.exit(1)

    excel_path = os.path.abspath(sys.argv[1])
    base_dir = os.path.dirname(excel_path)

    # Create /output/csv + /output/graphql
    dirs = create_folder_structure(base_dir)

    # 1) Convert Excel → CSV files
    excel_to_csv(excel_path, dirs["csv"])

    # 2) Parse setup.csv from csv folder
    setup_csv_path = os.path.join(dirs["csv"], "setup.csv")
    setup_input = parse_setup_csv_to_inputdatasetup(setup_csv_path)

    print("\nInputDataSetupInput object:")
    print(json.dumps(setup_input, indent=2))

    # 3) Build GraphQL mutation JSON
    payload = build_graphql_payload(setup_input)

    print("\nGraphQL payload:")
    print(json.dumps(payload, indent=2))

    # Save to file
    save_payload_to_file(payload, dirs["graphql"])
