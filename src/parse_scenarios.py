import os
from typing import List, Dict, Any

import pandas as pd


def _to_float(raw, default: float = 0.0) -> float:
    """Convert value to float, handling comma decimals."""
    if raw is None:
        return default
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return default
        s = s.replace(",", ".") 
        try:
            return float(s)
        except ValueError:
            return default
    try:
        return float(raw)
    except (ValueError, TypeError):
        return default


def parse_scenarios_csv_to_list(scen_csv_path: str) -> List[Dict[str, Any]]:
    """
    Parse scenarios.csv -> list of {name, weight} dicts.

    Expected columns:
      name, probability (or propability)

    Returns [] if file missing/empty.
    """
    if not os.path.isfile(scen_csv_path):
        print(f"scenarios csv not found at {scen_csv_path} → skipping scenarios.")
        return []

    try:
        df = pd.read_csv(scen_csv_path)
    except pd.errors.EmptyDataError:
        print(f"scenarios csv at {scen_csv_path} is empty → skipping scenarios.")
        return []

    if df.empty:
        print(f"scenarios csv at {scen_csv_path} has no data rows → skipping scenarios.")
        return []

    # handle typo: probability / propability
    prob_col = None
    for candidate in ("probability", "propability"):
        if candidate in df.columns:
            prob_col = candidate
            break

    if prob_col is None or "name" not in df.columns:
        print(
            f"scenarios csv missing required columns 'name' + probability/propability "
            f"(has {list(df.columns)}) → skipping scenarios."
        )
        return []

    scenarios: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        name = str(row["name"]).strip()
        if not name:
            continue

        prob = _to_float(row.get(prob_col), 0.0)

        scenarios.append(
            {
                "name": name,
                "weight": prob,
            }
        )

    return scenarios
