import os
from typing import List, Dict, Any

import pandas as pd


def _to_bool(raw) -> bool:
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


def _map_market_type(raw) -> str:
    """
    Map sheet 'market_type' to GraphQL MarketType enum.
      ENERGY or RESERVE
    """
    if raw is None:
        return "ENERGY"
    s = str(raw).strip().lower()
    if s in ("energy", "e"):
        return "ENERGY"
    if s in ("reserve", "res", "r"):
        return "RESERVE"
    # fallback
    print(f"Warning: unknown market_type '{raw}', defaulting to ENERGY")
    return "ENERGY"


def _map_direction(raw) -> str | None:
    """
    Map sheet 'direction' to MarketDirection enum.
      UP, DOWN, UP_DOWN, RES_UP, RES_DOWN
    Empty -> None
    """
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if not s:
        return None
    if s in ("up", "u"):
        return "UP"
    if s in ("down", "d"):
        return "DOWN"
    if s in ("up_down", "updown", "both"):
        return "UP_DOWN"
    if s in ("res_up", "rup", "reserve_up"):
        return "RES_UP"
    if s in ("res_down", "rdown", "reserve_down"):
        return "RES_DOWN"

    print(f"Warning: unknown direction '{raw}', leaving as None")
    return None


def parse_markets_csv_to_newmarkets(markets_csv_path: str) -> List[Dict[str, Any]]:
    """
    Read markets.csv and build a list of NewMarket inputs.

    CSV columns:
      market, market_type, node, processgroup, direction,
      realisation, reserve_type, is_bid, is_limited, min_bid, max_bid, fee

    NewMarket in schema:
      input NewMarket {
        name: String!
        mType: MarketType!
        node: String!
        processGroup: String!
        direction: MarketDirection
        realisation: [ValueInput!]!
        reserveType: String
        isBid: Boolean!
        isLimited: Boolean!
        minBid: Float!
        maxBid: Float!
        fee: Float!
        price: [ForecastValueInput!]!
        upPrice: [ForecastValueInput!]!
        downPrice: [ForecastValueInput!]!
        reserveActivationPrice: [ValueInput!]!
      }
    """
    if not os.path.isfile(markets_csv_path):
        print(f"markets.csv not found at {markets_csv_path} → skipping markets.")
        return []

    df = pd.read_csv(markets_csv_path)

    if df.empty:
        print(f"markets.csv at {markets_csv_path} has no data rows → skipping markets.")
        return []

    required_cols = [
        "market",
        "market_type",
        "node",
        "processgroup",
        "direction",
        "realisation",
        "reserve_type",
        "is_bid",
        "is_limited",
        "min_bid",
        "max_bid",
        "fee",
    ]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(
                f"markets.csv is missing required column '{col}'. "
                f"Available columns: {list(df.columns)}"
            )

    markets: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        name = str(row["market"]).strip()
        if not name:
            continue

        # realisation: if cell has a number, treat as scenario-independent constant
        real_raw = row.get("realisation")
        realisation: list[dict] = []
        if pd.notna(real_raw) and str(real_raw).strip() != "":
            realisation = [
                {
                    "scenario": None,
                    "constant": _to_float(real_raw, 0.0),
                }
            ]

        market_input: Dict[str, Any] = {
            "name": name,
            "mType": _map_market_type(row.get("market_type")),
            "node": str(row.get("market_node" if "market_node" in df.columns else "node")).strip(),
            "processGroup": str(row.get("processgroup")).strip(),
            "direction": _map_direction(row.get("direction")),
            "realisation": realisation,
            "reserveType": None if pd.isna(row.get("reserve_type")) else str(row.get("reserve_type")).strip(),
            "isBid": _to_bool(row.get("is_bid")),
            "isLimited": _to_bool(row.get("is_limited")),
            "minBid": _to_float(row.get("min_bid"), 0.0),
            "maxBid": _to_float(row.get("max_bid"), 0.0),
            "fee": _to_float(row.get("fee"), 0.0),
            # time-series fields (to be filled from price sheets later)
            "price": [],
            "upPrice": [],
            "downPrice": [],
            "reserveActivationPrice": [],
        }

        markets.append(market_input)

    return markets
