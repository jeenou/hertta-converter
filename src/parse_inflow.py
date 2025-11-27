import os
from typing import Dict, List, Any

import pandas as pd


def _to_float_list(series) -> List[float]:
    """
    Convert a pandas Series to a clean list of floats, dropping NaNs.
    Handles both dot and comma decimals (e.g. -42.77 or -42,77).
    """
    values: List[float] = []
    for v in series:
        if pd.isna(v):
            continue
        s = str(v).strip()
        if not s:
            continue
        # convert Finnish-style comma decimals to dot
        s = s.replace(",", ".")
        try:
            values.append(float(s))
        except (ValueError, TypeError):
            continue
    return values


def parse_inflow_csv_to_node_inflow(inflow_csv_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse inflow.csv and build mapping:
        { nodeName: [ForecastValueInput, ForecastValueInput, ...], ... }

    Expected CSV layout:

        t, <nodeName1>,<scenario1>, <nodeName2>,<scenario2>, ...

    Example header:
        t, dh_htf,ALL

    For each non-'t' column:

      * header is split by ',' → nodeName, scenarioName
      * if scenarioName == 'ALL' -> scenario is set to None
      * column values:
          - if all equal -> {scenario, constant: v}
          - else         -> {scenario, series: [v1, v2, ...]}

    If the file is missing or empty, returns {}.
    """
    if not os.path.isfile(inflow_csv_path):
        print(f"No inflow csv found at {inflow_csv_path} → skipping inflow.")
        return {}

    try:
        df = pd.read_csv(inflow_csv_path)
    except pd.errors.EmptyDataError:
        print(f"inflow csv at {inflow_csv_path} is empty → skipping inflow.")
        return {}

    if df.empty or len(df.columns) <= 1:
        print(f"inflow csv at {inflow_csv_path} has no data columns → skipping inflow.")
        return {}

    inflow_map: Dict[str, List[Dict[str, Any]]] = {}

    # Assume first column is time 't'; all others are node,scenario
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

        # constant vs series
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

        inflow_map.setdefault(node_name, []).append(vi)

    return inflow_map
