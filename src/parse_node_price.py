import os
from typing import Dict, List, Any

import pandas as pd


def _to_float_list(series) -> List[float]:
    """
    Convert a pandas Series to a clean list of floats, dropping NaNs.
    Handles both dot and comma decimals (e.g. 53.02752 or 53,02752).
    """
    values: List[float] = []
    for v in series:
        if pd.isna(v):
            continue
        s = str(v).strip()
        if not s:
            continue
        s = s.replace(",", ".") 
        try:
            values.append(float(s))
        except (ValueError, TypeError):
            continue
    return values


def parse_node_price_csv_to_costs(price_csv_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse price.csv (node price) and build mapping:
        { nodeName: [ValueInput, ValueInput, ...], ... }

    For each non-'t' column:

      * header is split by ',' → nodeName, scenarioName
      * if scenarioName == 'ALL' -> scenario is set to None
      * column values:
          - if all equal -> {scenario, constant: v}
          - else         -> {scenario, series: [v1, v2, ...]}

    If the file is missing or empty, returns {}.
    """
    if not os.path.isfile(price_csv_path):
        print(f"No node price csv found at {price_csv_path} → skipping node prices.")
        return {}

    try:
        df = pd.read_csv(price_csv_path)
    except pd.errors.EmptyDataError:
        print(f"Node price csv at {price_csv_path} is empty → skipping node prices.")
        return {}

    if df.empty or len(df.columns) <= 1:
        print(f"Node price csv at {price_csv_path} has no data columns → skipping node prices.")
        return {}

    node_costs: Dict[str, List[Dict[str, Any]]] = {}

    for col in df.columns[1:]:
        header = str(col).strip()
        if not header:
            continue

        if "," in header:
            node_name, scenario_raw = header.split(",", 1)
            node_name = node_name.strip()
            scenario_raw = scenario_raw.strip()
            scenario = None if scenario_raw.upper() == "ALL" else scenario_raw
        else:
            node_name = header
            scenario = None

        if not node_name:
            continue

        values = _to_float_list(df[col])
        if not values:
            continue

        if len(set(values)) == 1:
            vi: Dict[str, Any] = {
                "scenario": scenario,
                "constant": float(values[0]),
            }
        else:
            vi = {
                "scenario": scenario,
                "series": values,
            }

        node_costs.setdefault(node_name, []).append(vi)

    return node_costs
