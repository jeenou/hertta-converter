import os
import sys
import json

from excel_to_csv import create_folder_structure, excel_to_csv
from parse_setup import parse_setup_csv_to_inputdatasetup
from parse_nodes import (
    parse_nodes_csv_to_newnodes,
    parse_node_states_from_nodes_csv,
)
from parse_processes import parse_processes_csv_to_newprocesses
from graphql_utils import (
    build_setup_payload,
    save_payload_to_file,
    save_node_payloads_to_files,
    save_node_state_payloads_to_files,
    save_process_payloads_to_files,
    send_setup,
    send_nodes,
    send_node_states,
    send_processes,
)

# --- Config ---

GRAPHQL_URL = "http://localhost:3030/graphql"
GRAPHQL_HEADERS = {
    # "Authorization": "Bearer YOUR_TOKEN_HERE",
}
SEND_TO_SERVER = True  # toggle if you don't want to actually POST


def main(excel_file: str) -> None:
    excel_path = os.path.abspath(excel_file)
    base_dir = os.path.dirname(excel_path)

    # 1) Create /output/csv + /output/graphql
    dirs = create_folder_structure(base_dir)

    # 2) Convert Excel → CSV files into output/csv
    excel_to_csv(excel_path, dirs["csv"])

    # ---------- setup.csv → InputDataSetupInput ----------

    setup_csv_path = os.path.join(dirs["csv"], "setup.csv")
    print(f"\nReading setup from: {setup_csv_path}")
    setup_input = parse_setup_csv_to_inputdatasetup(setup_csv_path)

    print("\nInputDataSetupInput object:")
    print(json.dumps(setup_input, indent=2))

    setup_payload = build_setup_payload(setup_input)
    save_payload_to_file(setup_payload, dirs["graphql"], "inputdatasetup.json")

    # ---------- nodes.csv → NewNode inputs ----------

    nodes_csv_path = os.path.join(dirs["csv"], "nodes.csv")
    print(f"\nReading nodes from: {nodes_csv_path}")
    nodes_inputs = parse_nodes_csv_to_newnodes(nodes_csv_path)

    print(f"\nParsed {len(nodes_inputs)} nodes.")
    if nodes_inputs:
        print("Example first node:")
        print(json.dumps(nodes_inputs[0], indent=2))

    save_node_payloads_to_files(nodes_inputs, dirs["graphql"])

    # ---------- nodes.csv → node states (NewState) ----------

    print(f"\nReading node states from: {nodes_csv_path}")
    node_states = parse_node_states_from_nodes_csv(nodes_csv_path)

    print(f"\nParsed {len(node_states)} node states.")
    if node_states:
        print("Example first node state:")
        print(json.dumps(node_states[0], indent=2))

    save_node_state_payloads_to_files(node_states, dirs["graphql"])

    # ---------- processes.csv → NewProcess inputs ----------

    processes_csv_path = os.path.join(dirs["csv"], "processes.csv")
    print(f"\nReading processes from: {processes_csv_path}")
    processes_inputs = parse_processes_csv_to_newprocesses(processes_csv_path)

    print(f"\nParsed {len(processes_inputs)} processes.")
    if processes_inputs:
        print("Example first process:")
        print(json.dumps(processes_inputs[0], indent=2))

    save_process_payloads_to_files(processes_inputs, dirs["graphql"])

    # ---------- optionally send to GraphQL server ----------

    if SEND_TO_SERVER:
        print(f"\nSending setup mutation to {GRAPHQL_URL}")
        send_setup(GRAPHQL_URL, setup_input, headers=GRAPHQL_HEADERS)

        print(f"\nSending {len(nodes_inputs)} node mutations to {GRAPHQL_URL}")
        send_nodes(GRAPHQL_URL, nodes_inputs, headers=GRAPHQL_HEADERS)

        print(f"\nSending {len(node_states)} node state mutations to {GRAPHQL_URL}")
        send_node_states(GRAPHQL_URL, node_states, headers=GRAPHQL_HEADERS)

        print(f"\nSending {len(processes_inputs)} process mutations to {GRAPHQL_URL}")
        send_processes(GRAPHQL_URL, processes_inputs, headers=GRAPHQL_HEADERS)

    print("\nAll done.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <excel_file>")
        sys.exit(1)

    main(sys.argv[1])
