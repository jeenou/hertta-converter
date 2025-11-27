import os
import pandas as pd
from typing import List, Dict, Any


def _to_bool(raw) -> bool:
    """Convert 0/1, true/false, yes/no, etc. to bool."""
    if raw is None:
        return False
    if isinstance(raw, str):
        v = raw.strip().lower()
        if v in ("1", "true", "yes", "y", "t"):
            return True
        if v in ("0", "false", "no", "n", "f", ""):
            return False
    try:
        return bool(int(raw))
    except (ValueError, TypeError):
        return bool(raw)


def _to_float(raw, default: float = 0.0) -> float:
    if raw is None:
        return default
    if isinstance(raw, str) and raw.strip() == "":
        return default
    try:
        return float(raw)
    except (ValueError, TypeError):
        return default


def _map_conversion(raw) -> str:
    """
    Map processes.csv 'conversion' column to GraphQL enum Conversion.

    Spreadsheet codes:
      1 -> UNIT
      2 -> TRANSFER
      3 -> MARKET

    Also accepts strings: "Unit", "Transfer", "Market".
    """
    if raw is None:
        return "UNIT"

    s = str(raw).strip().lower()

    if s in ("1", "unit", "u"):
        return "UNIT"
    if s in ("2", "transfer", "t"):
        return "TRANSFER"
    if s in ("3", "market", "m"):
        return "MARKET"

    raise ValueError(f"Unsupported conversion value {raw!r} in processes.csv; "
                     "expected 1/2/3 or Unit/Transfer/Market")


def parse_processes_csv_to_newprocesses(processes_csv_path: str) -> List[Dict[str, Any]]:
    """
    Read processes.csv and build a list of NewProcess inputs.

    CSV columns:
      process, is_cf, is_cf_fix, is_online, is_res,
      conversion, eff, load_min, load_max, start_cost,
      min_online, min_offline, max_online, max_offline,
      initial_state, scenario_independent_online, delay
    """
    if not os.path.isfile(processes_csv_path):
        raise FileNotFoundError(f"processes.csv not found at {processes_csv_path}")

    df = pd.read_csv(processes_csv_path)

    required_cols = [
        "process",
        "is_cf_fix",
        "is_online",
        "is_res",
        "conversion",
        "eff",
        "load_min",
        "load_max",
        "start_cost",
        "min_online",
        "min_offline",
        "max_online",
        "max_offline",
        "initial_state",
        "scenario_independent_online",
    ]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(
                f"processes.csv is missing required column '{col}'. "
                f"Available columns: {list(df.columns)}"
            )

    processes: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        name = str(row["process"]).strip()
        if not name:
            continue

        conversion = _map_conversion(row.get("conversion"))

        proc: Dict[str, Any] = {
            "name": name,
            "conversion": conversion,
            "isCfFix": _to_bool(row.get("is_cf_fix")),
            "isOnline": _to_bool(row.get("is_online")),
            "isRes": _to_bool(row.get("is_res")),
            "eff": _to_float(row.get("eff"), 1.0),
            "loadMin": _to_float(row.get("load_min"), 0.0),
            "loadMax": _to_float(row.get("load_max"), 1.0),
            "startCost": _to_float(row.get("start_cost"), 0.0),
            "minOnline": _to_float(row.get("min_online"), 0.0),
            "maxOnline": _to_float(row.get("max_online"), 0.0),
            "minOffline": _to_float(row.get("min_offline"), 0.0),
            "maxOffline": _to_float(row.get("max_offline"), 0.0),
            "initialState": _to_bool(row.get("initial_state")),
            "isScenarioIndependent": _to_bool(row.get("scenario_independent_online")),
            # time-series fields – to be filled from ts sheets later
            "cf": [],
            "effTs": [],
            "effOpsFun": [],
        }

        processes.append(proc)

    return processes
