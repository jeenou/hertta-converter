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
        s = s.replace(",", ".")  # allow 0,1 style
        try:
            return float(s)
        except ValueError:
            return default
    try:
        return float(raw)
    except (ValueError, TypeError):
        return default


def parse_risk_csv_to_newrisks(risk_csv_path: str) -> List[Dict[str, Any]]:
    """
    Parse risk.csv (parameter,value) -> list of NewRisk dicts.

    Expected columns:
      parameter, value

    Returns [] if file missing/empty.
    """
    if not os.path.isfile(risk_csv_path):
        print(f"risk.csv not found at {risk_csv_path} → skipping risk.")
        return []

    try:
        df = pd.read_csv(risk_csv_path)
    except pd.errors.EmptyDataError:
        print(f"risk.csv at {risk_csv_path} is empty → skipping risk.")
        return []

    if df.empty:
        print(f"risk.csv at {risk_csv_path} has no data rows → skipping risk.")
        return []

    for col in ("parameter", "value"):
        if col not in df.columns:
            print(
                f"risk.csv missing column '{col}' (has {list(df.columns)}) "
                f"→ skipping risk."
            )
            return []

    risks: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        param = str(row["parameter"]).strip()
        if not param:
            continue

        value = _to_float(row.get("value"), 0.0)

        risks.append(
            {
                "parameter": param,
                "value": value,
            }
        )

    return risks
