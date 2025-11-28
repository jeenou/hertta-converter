import os
from typing import Dict, List, Any

import pandas as pd


def _to_float_list(series) -> List[float]:
    """
    Convert a pandas Series to a clean list of floats, dropping NaNs.
    Handles both dot and comma decimals.
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


def parse_market_prices_csv_to_prices(
    prices_csv_path: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse market_prices.csv and build mapping:
        { marketName: [ForecastValueInput, ForecastValueInput, ...], ... }

    Expected CSV layout:

        t, <marketName1>,<scenario1>, <marketName2>,<scenario2>, ...

    For each non-'t' column:

      * header is split by ',' → marketName, scenarioName
      * if scenarioName == 'ALL' -> scenario is set to None
      * column values:
          - if all equal -> {scenario, constant: v}
          - else         -> {scenario, series: [v1, v2, ...]}

    Returns empty dict if file is missing or empty.
    """
    if not os.path.isfile(prices_csv_path):
        print(f"No market prices csv found at {prices_csv_path} → skipping market prices.")
        return {}

    try:
        df = pd.read_csv(prices_csv_path)
    except pd.errors.EmptyDataError:
        print(f"market prices csv at {prices_csv_path} is empty → skipping market prices.")
        return {}

    if df.empty or len(df.columns) <= 1:
        print(f"market prices csv at {prices_csv_path} has no data columns → skipping market prices.")
        return {}

    price_map: Dict[str, List[Dict[str, Any]]] = {}

    for col in df.columns[1:]:
        header = str(col).strip()
        if not header:
            continue

        if "," in header:
            market_name, scenario_raw = header.split(",", 1)
            market_name = market_name.strip()
            scenario_raw = scenario_raw.strip()
            scenario = None if scenario_raw.upper() == "ALL" else scenario_raw
        else:
            market_name = header
            scenario = None

        if not market_name:
            continue

        values = _to_float_list(df[col])
        if not values:
            continue

        if len(set(values)) == 1:
            fv: Dict[str, Any] = {
                "scenario": scenario,
                "constant": float(values[0]),
            }
        else:
            fv = {
                "scenario": scenario,
                "series": values,
            }

        price_map.setdefault(market_name, []).append(fv)

    return price_map
