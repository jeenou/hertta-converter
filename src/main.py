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
from parse_groups import parse_groups_csv
from parse_topologies import parse_process_topologies_csv_to_inputs
from parse_cf import parse_cf_csv_to_process_cf
from parse_inflow import parse_inflow_csv_to_node_inflow
from parse_node_price import parse_node_price_csv_to_costs
from parse_markets import parse_markets_csv_to_newmarkets
from parse_market_prices import parse_market_prices_csv_to_prices
from parse_risk import parse_risk_csv_to_newrisks
from parse_scenarios import parse_scenarios_csv_to_list
from graphql_utils import (
    build_setup_payload,
    save_payload_to_file,
    save_node_payloads_to_files,
    save_node_state_payloads_to_files,
    save_process_payloads_to_files,
    save_group_payloads_to_files,
    save_topology_payloads_to_files,
    send_setup,
    send_nodes,
    send_node_states,
    send_processes,
    send_groups,
    send_topologies,
    save_market_payloads_to_files,
    send_markets,
    save_risk_payloads_to_files, 
    send_risks,
    save_scenario_payloads_to_files, 
    send_scenarios,
)

# --- Config ---

GRAPHQL_URL = "http://localhost:3030/graphql"
GRAPHQL_HEADERS = {
    # "Authorization": "Bearer YOUR_TOKEN_HERE",
}
SEND_TO_SERVER = True


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

    # ---------- price.csv → node cost (ValueInput) ----------

    price_csv_path = os.path.join(dirs["csv"], "price.csv")
    print(f"\nReading node prices from: {price_csv_path}")
    node_price_map = parse_node_price_csv_to_costs(price_csv_path)

    if node_price_map:
        for node in nodes_inputs:
            name = node.get("name")
            if name in node_price_map:
                node["cost"] = node_price_map[name]
        print("Attached node prices to nodes where available.")
        print("Example first node after prices:")
        print(json.dumps(nodes_inputs[0], indent=2))
    else:
        print("No node price data found; leaving node cost arrays (cost) as-is.")


    # ---------- inflow.csv → node inflow (ForecastValueInput) ----------

    inflow_csv_path = os.path.join(dirs["csv"], "inflow.csv")
    print(f"\nReading node inflow from: {inflow_csv_path}")
    inflow_map = parse_inflow_csv_to_node_inflow(inflow_csv_path)

    if inflow_map:
        for node in nodes_inputs:
            name = node.get("name")
            if name in inflow_map:
                node["inflow"] = inflow_map[name]
        print("Attached inflow to nodes where available.")
        print("Example first node after inflow:")
        print(json.dumps(nodes_inputs[0], indent=2))
    else:
        print("No inflow data found; leaving node inflow arrays empty.")

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

        # ---------- cf.csv → process CF time series ----------

    cf_csv_path = os.path.join(dirs["csv"], "cf.csv")
    print(f"\nReading process CF from: {cf_csv_path}")
    cf_map = parse_cf_csv_to_process_cf(cf_csv_path)

    # Attach CF to matching processes
    if cf_map:
        for proc in processes_inputs:
            name = proc.get("name")
            if name in cf_map:
                proc["cf"] = cf_map[name]
        print("Attached CF to processes where available.")
        print("Example first process after CF:")
        print(json.dumps(processes_inputs[0], indent=2))
    else:
        print("No CF data found; leaving process cf arrays empty.")

    save_process_payloads_to_files(processes_inputs, dirs["graphql"])

        # ---------- process_topologies.csv → createTopology calls ----------

    topo_csv_path = os.path.join(dirs["csv"], "process_topology.csv")
    print(f"\nReading process topologies from: {topo_csv_path}")
    topo_calls = parse_process_topologies_csv_to_inputs(topo_csv_path)

    print(f"\nParsed {len(topo_calls)} process topologies.")
    if topo_calls:
        print("Example first topology call:")
        print(json.dumps(topo_calls[0], indent=2))

    save_topology_payloads_to_files(topo_calls, dirs["graphql"])

        # ---------- groups.csv → groups & memberships ----------

    groups_csv_path = os.path.join(dirs["csv"], "groups.csv")
    print(f"\nReading groups from: {groups_csv_path}")
    groups_data = parse_groups_csv(groups_csv_path)

    total_groups = len(groups_data["node_groups"]) + len(groups_data["process_groups"])
    total_memberships = len(groups_data["node_memberships"]) + len(groups_data["process_memberships"])
    print(f"\nParsed {total_groups} groups and {total_memberships} memberships.")
    if total_groups or total_memberships:
        print("Groups data preview:")
        print(json.dumps(groups_data, indent=2))

    save_group_payloads_to_files(groups_data, dirs["graphql"])

        # ---------- markets.csv → NewMarket inputs ----------
    
    markets_csv_path = os.path.join(dirs["csv"], "markets.csv")
    print(f"\nReading markets from: {markets_csv_path}")
    markets_inputs = parse_markets_csv_to_newmarkets(markets_csv_path)

        # ---------- market_prices.csv → market.price (ForecastValueInput) ----------

    market_prices_csv_path = os.path.join(dirs["csv"], "market_prices.csv")
    print(f"\nReading market prices from: {market_prices_csv_path}")
    market_price_map = parse_market_prices_csv_to_prices(market_prices_csv_path)

    if market_price_map:
        for market in markets_inputs:
            name = market.get("name")
            if name in market_price_map:
                market["price"] = market_price_map[name]
        print("Attached price time series to markets where available.")
        if markets_inputs:
            print("Example first market after prices:")
            print(json.dumps(markets_inputs[0], indent=2))
    else:
        print("No market price data found; leaving market price arrays empty.")

    print(f"\nParsed {len(markets_inputs)} markets.")
    if markets_inputs:
        print("Example first market:")
        print(json.dumps(markets_inputs[0], indent=2))

    save_market_payloads_to_files(markets_inputs, dirs["graphql"])

        # ---------- risk.csv → NewRisk inputs ----------

    risk_csv_path = os.path.join(dirs["csv"], "risk.csv")  # sheet "risk" -> risk.csv
    print(f"\nReading risk from: {risk_csv_path}")
    risks_inputs = parse_risk_csv_to_newrisks(risk_csv_path)

    print(f"\nParsed {len(risks_inputs)} risk parameters.")
    if risks_inputs:
        print("Risk data:")
        print(json.dumps(risks_inputs, indent=2))

    save_risk_payloads_to_files(risks_inputs, dirs["graphql"])

    scenarios_csv_path = os.path.join(dirs["csv"], "scenarios.csv")
    print(f"\nReading scenarios from: {scenarios_csv_path}")
    scenarios = parse_scenarios_csv_to_list(scenarios_csv_path)

    print(f"\nParsed {len(scenarios)} scenarios.")
    if scenarios:
        print("Scenario data:")
        print(json.dumps(scenarios, indent=2))

    save_scenario_payloads_to_files(scenarios, dirs["graphql"])


    if SEND_TO_SERVER:
        print(f"\nSending setup mutation to {GRAPHQL_URL}")
        send_setup(GRAPHQL_URL, setup_input, headers=GRAPHQL_HEADERS)

        print(f"\nSending {len(scenarios)} scenario mutations to {GRAPHQL_URL}")
        send_scenarios(GRAPHQL_URL, scenarios, headers=GRAPHQL_HEADERS)

        print(f"\nSending {len(nodes_inputs)} node mutations to {GRAPHQL_URL}")
        send_nodes(GRAPHQL_URL, nodes_inputs, headers=GRAPHQL_HEADERS)

        print(f"\nSending {len(node_states)} node state mutations to {GRAPHQL_URL}")
        send_node_states(GRAPHQL_URL, node_states, headers=GRAPHQL_HEADERS)

        print(f"\nSending {len(processes_inputs)} process mutations to {GRAPHQL_URL}")
        send_processes(GRAPHQL_URL, processes_inputs, headers=GRAPHQL_HEADERS)

        print("\nSending group mutations to GraphQL")
        send_groups(GRAPHQL_URL, groups_data, headers=GRAPHQL_HEADERS)

        print(f"\nSending {len(topo_calls)} topology mutations to {GRAPHQL_URL}")
        send_topologies(GRAPHQL_URL, topo_calls, headers=GRAPHQL_HEADERS)

        print(f"\nSending {len(markets_inputs)} market mutations to {GRAPHQL_URL}")
        send_markets(GRAPHQL_URL, markets_inputs, headers=GRAPHQL_HEADERS)

        print(f"\nSending {len(risks_inputs)} risk mutations to {GRAPHQL_URL}")
        send_risks(GRAPHQL_URL, risks_inputs, headers=GRAPHQL_HEADERS)

    print("\nAll done.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <excel_file>")
        sys.exit(1)

    main(sys.argv[1])
