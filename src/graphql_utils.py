import json
import os
from typing import Dict, Any, Optional, List

import requests


# ---------- Core reusable helpers ----------

def build_graphql_payload(mutation: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a generic GraphQL payload.
    You can reuse this for ANY mutation or query.

    Example:
        payload = build_graphql_payload(
            \"\"\"mutation MyMutation($id: ID!) { ... }\"\"\",
            {"id": "123"}
        )
    """
    return {
        "query": mutation,
        "variables": variables or {},
    }


def send_graphql_payload(
    url: str,
    payload: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Send a GraphQL payload (dict with 'query' and 'variables')
    to the given HTTP endpoint.

    Returns the parsed JSON response as a dict.
    """
    if headers is None:
        headers = {}

    headers = {
        "Content-Type": "application/json",
        **headers,
    }

    resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
    print(f"→ POST {url} -> {resp.status_code}")

    try:
        data = resp.json()
        print(json.dumps(data, indent=2))
        return data
    except Exception:
        print("Non-JSON response from server:")
        print(resp.text)
        return {"raw": resp.text, "status_code": resp.status_code}


def save_payload_to_file(
    payload: Dict[str, Any],
    graphql_dir: str,
    filename: str,
) -> str:
    """
    Save any GraphQL payload (or list of payloads) to disk.
    """
    os.makedirs(graphql_dir, exist_ok=True)
    out_path = os.path.join(graphql_dir, filename)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved GraphQL payload: {out_path}")
    return out_path


# ---------- Convenience helpers for your existing mutations ----------

# 1) InputDataSetupInput

_SETUP_MUTATION = """
mutation CreateInputDataSetup($setup: InputDataSetupInput!) {
  createInputDataSetup(setupUpdate: $setup) {
    errors {
      field
      message
    }
  }
}
"""


def build_setup_payload(setup_input: Dict[str, Any]) -> Dict[str, Any]:
    return build_graphql_payload(_SETUP_MUTATION, {"setup": setup_input})


def send_setup(
    url: str,
    setup_input: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    payload = build_setup_payload(setup_input)
    return send_graphql_payload(url, payload, headers=headers)


# 2) NewNode

_NODE_MUTATION = """
mutation CreateNode($node: NewNode!) {
  createNode(node: $node) {
    errors {
      field
      message
    }
  }
}
"""


def build_node_payload(node_input: Dict[str, Any]) -> Dict[str, Any]:
    return build_graphql_payload(_NODE_MUTATION, {"node": node_input})


def save_node_payloads_to_files(nodes_inputs: List[Dict[str, Any]], graphql_dir: str):
    """
    Save one JSON file per node and one combined file with all node payloads.
    """
    os.makedirs(graphql_dir, exist_ok=True)

    all_payloads: List[Dict[str, Any]] = []

    for node in nodes_inputs:
        name = node.get("name", "node")
        safe = "".join(c for c in name if c.isalnum() or c in ("_", "-", " ")).strip()
        if not safe:
            safe = "node"
        safe = safe.replace(" ", "_")

        payload = build_node_payload(node)
        all_payloads.append(payload)

        filename = f"node_{safe}.json"
        save_payload_to_file(payload, graphql_dir, filename)

    save_payload_to_file(all_payloads, graphql_dir, "nodes_all.json")


def send_nodes(
    url: str,
    nodes_inputs: List[Dict[str, Any]],
    headers: Optional[Dict[str, str]] = None,
):
    """
    Send createNode mutation for all nodes, one by one.
    """
    for node in nodes_inputs:
        print(f"\nSending node: {node.get('name')}")
        payload = build_node_payload(node)
        send_graphql_payload(url, payload, headers=headers)
