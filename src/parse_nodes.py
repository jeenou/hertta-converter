import pandas as pd
import os


def _to_bool(raw) -> bool:
    """Convert 0/1, true/false, yes/no, etc. to bool."""
    if raw is None:
        return False
    if isinstance(raw, str):
        v = raw.strip().lower()
        if v in ("1", "true", "yes", "y"):
            return True
        if v in ("0", "false", "no", "n", ""):
            return False
    try:
        return bool(int(raw))
    except (ValueError, TypeError):
        return bool(raw)


def parse_nodes_csv_to_newnodes(nodes_csv_path: str) -> list[dict]:
    """
    Read nodes.csv and build a list of NewNode input objects
    matching the GraphQL schema:

      input NewNode {
        name: String!
        isCommodity: Boolean!
        isMarket: Boolean!
        isRes: Boolean!
        cost: [ValueInput!]!
        inflow: [ForecastValueInput!]!
      }

    For now, cost and inflow are set as empty lists ([])
    and can be enriched later from other CSVs.
    """
    if not os.path.isfile(nodes_csv_path):
        raise FileNotFoundError(f"nodes.csv not found at {nodes_csv_path}")

    df = pd.read_csv(nodes_csv_path)

    required_cols = ["node", "is_commodity", "is_market", "is_res"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(
                f"nodes.csv is missing required column '{col}'. "
                f"Available columns: {list(df.columns)}"
            )

    nodes: list[dict] = []

    for _, row in df.iterrows():
        name = str(row["node"]).strip()
        if not name:
            continue  # skip empty rows

        is_commodity = _to_bool(row.get("is_commodity"))
        is_market = _to_bool(row.get("is_market"))
        is_res = _to_bool(row.get("is_res"))

        # For now we leave cost/inflow empty; they can be filled from other sheets later
        node_input = {
            "name": name,
            "isCommodity": is_commodity,
            "isMarket": is_market,
            "isRes": is_res,
            "cost": [],
            "inflow": [],
        }

        nodes.append(node_input)

    return nodes
