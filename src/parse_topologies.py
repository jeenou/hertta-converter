import os
from typing import List, Dict, Any

import pandas as pd


def _to_float(raw, default: float = 0.0) -> float:
    if raw is None:
        return default
    if isinstance(raw, str) and raw.strip() == "":
        return default
    try:
        return float(raw)
    except (ValueError, TypeError):
        return default


def _split_source_sink(role_raw: str, node_name: str):
    """
    Decide whether the node is source or sink.

    source_sink column is expected to be like: "source" / "sink"
    (but we accept a few variants).
    """
    if not node_name:
        return None, None

    if not role_raw:
        return None, None

    r = role_raw.strip().lower()

    if r in ("source", "src", "s", "in", "input"):
        return node_name, None
    if r in ("sink", "snk", "d", "out", "output"):
        return None, node_name

    print(f"Warning: unknown source_sink value '{role_raw}' – row skipped.")
    return None, None


def parse_process_topologies_csv_to_inputs(
    topo_csv_path: str,
) -> List[Dict[str, Any]]:
    """
    Parse process topology sheet (CSV) and build call inputs for createTopology.

    Expected CSV columns:
      process, source_sink, node, conversion_coeff,
      capacity, vom_cost, ramp_up, ramp_down, initial_load, initial_flow

    Returns list of dicts:

      {
        "processName": <str>,
        "sourceNodeName": <str | None>,
        "sinkNodeName": <str | None>,
        "topology": {
          "capacity": Float,
          "vomCost": Float,
          "rampUp": Float,
          "rampDown": Float,
          "initialLoad": Float,
          "initialFlow": Float,
          "capTs": []   # no time series yet
        }
      }

    If file missing or empty → returns [].
    """
    if not os.path.isfile(topo_csv_path):
        print(f"No topology csv found at {topo_csv_path} → skipping topologies.")
        return []

    try:
        df = pd.read_csv(topo_csv_path)
    except pd.errors.EmptyDataError:
        print(f"Topology csv at {topo_csv_path} is empty → skipping topologies.")
        return []

    if df.empty:
        print(f"Topology csv at {topo_csv_path} has no data rows → skipping topologies.")
        return []

    required_cols = [
        "process",
        "source_sink",
        "node",
        "capacity",
        "vom_cost",
        "ramp_up",
        "ramp_down",
        "initial_load",
        "initial_flow",
    ]
    for col in required_cols:
        if col not in df.columns:
            print(
                f"Topology csv missing column '{col}' (has {list(df.columns)}) "
                f"→ skipping topologies."
            )
            return []

    topologies: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        process_name = str(row["process"]).strip()
        node_name = str(row["node"]).strip()

        if not process_name or not node_name:
            continue

        source_raw = str(row.get("source_sink", "")).strip()
        source_name, sink_name = _split_source_sink(source_raw, node_name)
        if source_name is None and sink_name is None:
            # unknown source_sink -> row already warned, skip
            continue

        topo_input: Dict[str, Any] = {
            "capacity": _to_float(row.get("capacity"), 0.0),
            "vomCost": _to_float(row.get("vom_cost"), 0.0),
            "rampUp": _to_float(row.get("ramp_up"), 0.0),
            "rampDown": _to_float(row.get("ramp_down"), 0.0),
            "initialLoad": _to_float(row.get("initial_load"), 0.0),
            "initialFlow": _to_float(row.get("initial_flow"), 0.0),
            "capTs": [],  # no time series yet
        }

        topologies.append(
            {
                "processName": process_name,
                "sourceNodeName": source_name,
                "sinkNodeName": sink_name,
                "topology": topo_input,
                # we currently ignore conversion_coeff – not in NewTopology
            }
        )

    return topologies
