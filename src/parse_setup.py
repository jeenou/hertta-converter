import pandas as pd
import os


def parse_setup_csv_to_inputdatasetup(setup_csv_path: str) -> dict:
    df = pd.read_csv(setup_csv_path)

    field_map = {
        "use_market_bids": "useMarketBids",
        "use_reserves": "useReserves",
        "use_reserve_realisation": "useReserveRealisation",
        "use_node_dummy_variables": "useNodeDummyVariables",
        "use_ramp_dummy_variables": "useRampDummyVariables",
        "common_start_timesteps": "commonTimesteps",
        "common_scenario_name": "commonScenarioName",
        "node_dummy_variable_cost": "nodeDummyVariableCost",
        "ramp_dummy_variable_cost": "rampDummyVariableCost",
    }

    bool_params = {
        "use_market_bids",
        "use_reserves",
        "use_reserve_realisation",
        "use_node_dummy_variables",
        "use_ramp_dummy_variables",
    }

    int_params = {"common_start_timesteps"}

    setup_input = {}

    for _, row in df.iterrows():
        param = str(row["parameter"]).strip()
        if param not in field_map:
            continue

        raw_value = row["value"]
        gql_field = field_map[param]

        if pd.isna(raw_value):
            value = None
        else:
            if param in bool_params:
                if isinstance(raw_value, str):
                    v = raw_value.strip().lower()
                    value = v in ("1", "true", "yes")
                else:
                    value = bool(int(raw_value))
            elif param in int_params:
                value = int(raw_value)
            elif param == "common_scenario_name":
                value = str(raw_value)
            else:
                value = float(raw_value)

        setup_input[gql_field] = value

    return setup_input
