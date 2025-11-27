import os
from typing import Dict, Any, List

import pandas as pd


def parse_groups_csv(groups_csv_path: str) -> Dict[str, Any]:
    """
    Parse groups.csv.

    Expected columns:
      group_type  entity  group

    group_type: "node" or "process"
    entity:     node or process name
    group:      group name

    Returns a dict:
      {
        "node_groups": [<groupName>, ...],
        "process_groups": [<groupName>, ...],
        "node_memberships": [
            {"nodeName": ..., "groupName": ...},
            ...
        ],
        "process_memberships": [
            {"processName": ..., "groupName": ...},
            ...
        ],
      }

    If the file is missing or has no data rows, returns all-empty.
    """
    empty_result = {
        "node_groups": [],
        "process_groups": [],
        "node_memberships": [],
        "process_memberships": [],
    }

    if not os.path.isfile(groups_csv_path):
        print(f"No groups.csv found at {groups_csv_path} → skipping groups.")
        return empty_result

    try:
        df = pd.read_csv(groups_csv_path)
    except pd.errors.EmptyDataError:
        print(f"groups.csv at {groups_csv_path} is empty → skipping groups.")
        return empty_result

    # no rows with data
    if df.empty:
        print(f"groups.csv at {groups_csv_path} has no data rows → skipping groups.")
        return empty_result

    # ensure required columns
    for col in ("group_type", "entity", "group"):
        if col not in df.columns:
            print(
                f"groups.csv missing column '{col}' (has {list(df.columns)}) "
                f"→ skipping groups."
            )
            return empty_result

    node_groups = set()
    process_groups = set()
    node_memberships: List[Dict[str, str]] = []
    process_memberships: List[Dict[str, str]] = []

    for _, row in df.iterrows():
        group_type = str(row["group_type"]).strip().lower()
        entity = str(row["entity"]).strip()
        group = str(row["group"]).strip()

        if not entity or not group:
            continue

        if group_type == "node":
            node_groups.add(group)
            node_memberships.append({"nodeName": entity, "groupName": group})
        elif group_type == "process":
            process_groups.add(group)
            process_memberships.append({"processName": entity, "groupName": group})
        else:
            print(f"Warning: unknown group_type '{group_type}' in groups.csv, row skipped.")

    return {
        "node_groups": sorted(node_groups),
        "process_groups": sorted(process_groups),
        "node_memberships": node_memberships,
        "process_memberships": process_memberships,
    }
