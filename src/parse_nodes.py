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


# ------------ NODES: NewNode input ------------

def parse_nodes_csv_to_newnodes(nodes_csv_path: str) -> List[Dict[str, Any]]:
    """
    Read nodes.csv and build a list of NewNode input objects.

    NewNode in schema:
      input NewNode {
        name: String!
        isCommodity: Boolean!
        isMarket: Boolean!
        isRes: Boolean!
        cost: [ValueInput!]!
        inflow: [ForecastValueInput!]!
      }
    """
    if not os.path.isfile(nodes_csv_path):
        raise FileNotFoundError(f"nodes.csv not found at {nodes_csv_path}")

    df = pd.read_csv(nodes_csv_path)

    required_cols = ["node", "is_commodity", "is_res", "is_market"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(
                f"nodes.csv is missing required column '{col}'. "
                f"Available columns: {list(df.columns)}"
            )

    nodes: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        name = str(row["node"]).strip()
        if not name:
            continue  # skip empty rows

        node_input = {
            "name": name,
            "isCommodity": _to_bool(row.get("is_commodity")),
            "isMarket": _to_bool(row.get("is_market")),
            "isRes": _to_bool(row.get("is_res")),
            # cost & inflow will be filled later from other sheets
            "cost": [],
            "inflow": [],
        }

        nodes.append(node_input)

    return nodes


# ------------ NODE STATE: NewState input ------------

def parse_node_states_from_nodes_csv(nodes_csv_path: str) -> List[Dict[str, Any]]:
    """
    Read nodes.csv and build a list of objects of the form

      {
        "nodeName": <str>,
        "state": <NewState dict>
      }

    Only rows where is_state == 1/true will create a state.
    """
    if not os.path.isfile(nodes_csv_path):
        raise FileNotFoundError(f"nodes.csv not found at {nodes_csv_path}")

    df = pd.read_csv(nodes_csv_path)

    if "node" not in df.columns:
        raise ValueError("nodes.csv must have a 'node' column for node names.")

    state_col_map = {
        "in_max": "inMax",
        "out_max": "outMax",
        "state_loss_proportional": "stateLossProportional",
        "state_min": "stateMin",
        "state_max": "stateMax",
        "initial_state": "initialState",
        "scenario_independent_state": "isScenarioIndependent",
        "is_temp": "isTemp",
        "t_e_conversion": "tEConversion",
        "residual_value": "residualValue",
    }

    node_states: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        name = str(row["node"]).strip()
        if not name:
            continue

        # only create a state if is_state is true
        if "is_state" in df.columns and not _to_bool(row.get("is_state")):
            continue

        # defaults
        state: Dict[str, Any] = {
            "inMax": 0.0,
            "outMax": 0.0,
            "stateLossProportional": 0.0,
            "stateMin": 0.0,
            "stateMax": 0.0,
            "initialState": 0.0,
            "isScenarioIndependent": True,
            "isTemp": False,
            "tEConversion": 1.0,
            "residualValue": 0.0,
        }

        for col_name, field_name in state_col_map.items():
            if col_name not in df.columns:
                continue
            raw = row.get(col_name)
            if field_name in ("isScenarioIndependent", "isTemp"):
                state[field_name] = _to_bool(raw)
            else:
                state[field_name] = _to_float(raw, default=state[field_name])

        node_states.append({"nodeName": name, "state": state})

    return node_states
