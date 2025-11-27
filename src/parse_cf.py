import os
from typing import Dict, List, Any

import pandas as pd


def _to_float_list(series) -> List[float]:
    """Convert a pandas Series to a clean list of floats, dropping NaNs."""
    values = []
    for v in series:
        if pd.isna(v):
            continue
        try:
            values.append(float(v))
        except (ValueError, TypeError):
            continue
    return values


def parse_cf_csv_to_process_cf(cf_csv_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse cf.csv and build mapping:
        { processName: [ValueInput, ValueInput, ...], ... }

    Expected CSV layout:

        t, <processName1>,<scenario1>, <processName2>,<scenario2>, ...

    Example header:
        t, solar_collector,ALL

    For each non-'t' column:

      * header is split by ',' → processName, scenarioName
      * if scenarioName == 'ALL' -> scenario is set to None
      * column values:
          - if all equal -> ValueInput {scenario, constant: v}
          - else         -> ValueInput {scenario, series: [v1, v2, ...]}

    If the file is missing or empty, returns {}.
    """
    if not os.path.isfile(cf_csv_path):
        print(f"No cf.csv found at {cf_csv_path} → skipping cf.")
        return {}

    try:
        df = pd.read_csv(cf_csv_path)
    except pd.errors.EmptyDataError:
        print(f"cf.csv at {cf_csv_path} is empty → skipping cf.")
        return {}

    if df.empty or len(df.columns) <= 1:
        print(f"cf.csv at {cf_csv_path} has no data columns → skipping cf.")
        return {}

    cf_map: Dict[str, List[Dict[str, Any]]] = {}

    # Assume first column is time 't'; all others are process,scenario
    for col in df.columns[1:]:
        header = str(col).strip()
        if not header:
            continue

        if "," in header:
            process_name, scenario_raw = header.split(",", 1)
            process_name = process_name.strip()
            scenario_raw = scenario_raw.strip()
            scenario = None if scenario_raw.upper() == "ALL" else scenario_raw
        else:
            process_name = header
            scenario = None

        if not process_name:
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

        cf_map.setdefault(process_name, []).append(vi)

    return cf_map
