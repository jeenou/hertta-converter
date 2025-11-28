import json
import os
from typing import Dict, Any, Optional, List
import requests


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
    payload: Any,          # <- allow dict OR list
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

# InputDataSetupInput

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


# NewNode

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


# Node state (setNodeState)

_STATE_MUTATION = """
mutation SetNodeState($nodeName: String!, $state: NewState) {
  setNodeState(state: $state, nodeName: $nodeName) {
    errors {
      field
      message
    }
  }
}
"""


def build_node_state_payload(node_name: str, state_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build payload for setting state of a single node.
      variables = { nodeName: "...", state: { ...NewState fields... } }
    """
    variables = {
        "nodeName": node_name,
        "state": state_input,
    }
    return build_graphql_payload(_STATE_MUTATION, variables)


def save_node_state_payloads_to_files(
    node_states: List[Dict[str, Any]],
    graphql_dir: str,
):
    """
    node_states: list of {"nodeName": <str>, "state": <NewState dict>}
    Saves one JSON per node and one combined JSON.
    """
    os.makedirs(graphql_dir, exist_ok=True)

    all_payloads: List[Dict[str, Any]] = []

    for item in node_states:
        node_name = item.get("nodeName", "node")
        state = item.get("state", {})

        safe = "".join(
            c for c in node_name if c.isalnum() or c in ("_", "-", " ")
        ).strip()
        if not safe:
            safe = "node"
        safe = safe.replace(" ", "_")

        payload = build_node_state_payload(node_name, state)
        all_payloads.append(payload)

        filename = f"node_state_{safe}.json"
        save_payload_to_file(payload, graphql_dir, filename)

    save_payload_to_file(all_payloads, graphql_dir, "node_states_all.json")


def send_node_states(
    url: str,
    node_states: List[Dict[str, Any]],
    headers: Optional[Dict[str, str]] = None,
):
    """
    Send setNodeState mutation for all given node states, one by one.
    node_states: list of {"nodeName": <str>, "state": <NewState dict>}
    """
    for item in node_states:
        node_name = item.get("nodeName")
        state = item.get("state")
        if not node_name or state is None:
            continue
        print(f"\nSending node state for: {node_name}")
        payload = build_node_state_payload(node_name, state)
        send_graphql_payload(url, payload, headers=headers)

# Processes (createProcess)

_PROCESS_MUTATION = """
mutation CreateProcess($process: NewProcess!) {
  createProcess(process: $process) {
    errors {
      field
      message
    }
  }
}
"""


def build_process_payload(process_input: Dict[str, Any]) -> Dict[str, Any]:
    return build_graphql_payload(_PROCESS_MUTATION, {"process": process_input})


def save_process_payloads_to_files(
    processes_inputs: List[Dict[str, Any]],
    graphql_dir: str,
):
    """
    Save one JSON file per process and one combined file with all process payloads.
    """
    os.makedirs(graphql_dir, exist_ok=True)

    all_payloads: List[Dict[str, Any]] = []

    for proc in processes_inputs:
        name = proc.get("name", "process")
        safe = "".join(c for c in name if c.isalnum() or c in ("_", "-", " ")).strip()
        if not safe:
            safe = "process"
        safe = safe.replace(" ", "_")

        payload = build_process_payload(proc)
        all_payloads.append(payload)

        filename = f"process_{safe}.json"
        save_payload_to_file(payload, graphql_dir, filename)

    save_payload_to_file(all_payloads, graphql_dir, "processes_all.json")


def send_processes(
    url: str,
    processes_inputs: List[Dict[str, Any]],
    headers: Optional[Dict[str, str]] = None,
):
    """
    Send createProcess mutation for all processes, one by one.
    """
    for proc in processes_inputs:
        print(f"\nSending process: {proc.get('name')}")
        payload = build_process_payload(proc)
        send_graphql_payload(url, payload, headers=headers)

# Groups (createNodeGroup / createProcessGroup / addNodeToGroup / addProcessToGroup)

_NODE_GROUP_MUTATION = """
mutation CreateNodeGroup($name: String!) {
  createNodeGroup(name: $name) {
    message
  }
}
"""

_PROCESS_GROUP_MUTATION = """
mutation CreateProcessGroup($name: String!) {
  createProcessGroup(name: $name) {
    message
  }
}
"""

_ADD_NODE_TO_GROUP_MUTATION = """
mutation AddNodeToGroup($nodeName: String!, $groupName: String!) {
  addNodeToGroup(nodeName: $nodeName, groupName: $groupName) {
    message
  }
}
"""

_ADD_PROCESS_TO_GROUP_MUTATION = """
mutation AddProcessToGroup($processName: String!, $groupName: String!) {
  addProcessToGroup(processName: $processName, groupName: $groupName) {
    message
  }
}
"""


def build_create_node_group_payload(name: str) -> Dict[str, Any]:
    return build_graphql_payload(_NODE_GROUP_MUTATION, {"name": name})


def build_create_process_group_payload(name: str) -> Dict[str, Any]:
    return build_graphql_payload(_PROCESS_GROUP_MUTATION, {"name": name})


def build_add_node_to_group_payload(node_name: str, group_name: str) -> Dict[str, Any]:
    vars_ = {"nodeName": node_name, "groupName": group_name}
    return build_graphql_payload(_ADD_NODE_TO_GROUP_MUTATION, vars_)


def build_add_process_to_group_payload(process_name: str, group_name: str) -> Dict[str, Any]:
    vars_ = {"processName": process_name, "groupName": group_name}
    return build_graphql_payload(_ADD_PROCESS_TO_GROUP_MUTATION, vars_)


def save_group_payloads_to_files(groups_data: Dict[str, Any], graphql_dir: str) -> None:
    """
    groups_data is the dict returned by parse_groups_csv.
    Writes JSONs for:
      - create node groups
      - create process groups
      - add node/process to groups
    """
    os.makedirs(graphql_dir, exist_ok=True)

    node_group_payloads: List[Dict[str, Any]] = []
    for name in groups_data["node_groups"]:
        payload = build_create_node_group_payload(name)
        node_group_payloads.append(payload)
        safe = "".join(c for c in name if c.isalnum() or c in ("_", "-", " ")).strip() or "node_group"
        safe = safe.replace(" ", "_")
        save_payload_to_file(payload, graphql_dir, f"node_group_{safe}.json")

    if node_group_payloads:
        save_payload_to_file(node_group_payloads, graphql_dir, "node_groups_all.json")

    process_group_payloads: List[Dict[str, Any]] = []
    for name in groups_data["process_groups"]:
        payload = build_create_process_group_payload(name)
        process_group_payloads.append(payload)
        safe = "".join(c for c in name if c.isalnum() or c in ("_", "-", " ")).strip() or "process_group"
        safe = safe.replace(" ", "_")
        save_payload_to_file(payload, graphql_dir, f"process_group_{safe}.json")

    if process_group_payloads:
        save_payload_to_file(process_group_payloads, graphql_dir, "process_groups_all.json")

    node_membership_payloads: List[Dict[str, Any]] = []
    for m in groups_data["node_memberships"]:
        payload = build_add_node_to_group_payload(m["nodeName"], m["groupName"])
        node_membership_payloads.append(payload)
    if node_membership_payloads:
        save_payload_to_file(node_membership_payloads, graphql_dir, "node_group_memberships_all.json")

    process_membership_payloads: List[Dict[str, Any]] = []
    for m in groups_data["process_memberships"]:
        payload = build_add_process_to_group_payload(m["processName"], m["groupName"])
        process_membership_payloads.append(payload)
    if process_membership_payloads:
        save_payload_to_file(process_membership_payloads, graphql_dir, "process_group_memberships_all.json")


def send_groups(
    url: str,
    groups_data: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
) -> None:
    """
    Send all group-related mutations in a sensible order:
      1) create node groups
      2) create process groups
      3) add nodes to groups
      4) add processes to groups
    """
    if not (groups_data["node_groups"] or groups_data["process_groups"] or
            groups_data["node_memberships"] or groups_data["process_memberships"]):
        print("No group data to send.")
        return

    # 1) create node groups
    for name in groups_data["node_groups"]:
        print(f"\nCreating node group: {name}")
        payload = build_create_node_group_payload(name)
        send_graphql_payload(url, payload, headers=headers)

    # 2) create process groups
    for name in groups_data["process_groups"]:
        print(f"\nCreating process group: {name}")
        payload = build_create_process_group_payload(name)
        send_graphql_payload(url, payload, headers=headers)

    # 3) add nodes to groups
    for m in groups_data["node_memberships"]:
        print(f"\nAdding node {m['nodeName']} to group {m['groupName']}")
        payload = build_add_node_to_group_payload(m["nodeName"], m["groupName"])
        send_graphql_payload(url, payload, headers=headers)

    # 4) add processes to groups
    for m in groups_data["process_memberships"]:
        print(f"\nAdding process {m['processName']} to group {m['groupName']}")
        payload = build_add_process_to_group_payload(m["processName"], m["groupName"])
        send_graphql_payload(url, payload, headers=headers)

# Process topologies (createTopology)

_TOPOLOGY_MUTATION = """
mutation CreateTopology(
  $topology: NewTopology!
  $sourceNodeName: String
  $processName: String!
  $sinkNodeName: String
) {
  createTopology(
    topology: $topology
    sourceNodeName: $sourceNodeName
    processName: $processName
    sinkNodeName: $sinkNodeName
  ) {
    errors {
      field
      message
    }
  }
}
"""


def build_topology_payload(call_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    call_input: {
      "processName": str,
      "sourceNodeName": str | None,
      "sinkNodeName": str | None,
      "topology": { ... NewTopology fields ... }
    }
    """
    vars_ = {
        "topology": call_input["topology"],
        "sourceNodeName": call_input.get("sourceNodeName"),
        "processName": call_input["processName"],
        "sinkNodeName": call_input.get("sinkNodeName"),
    }
    return build_graphql_payload(_TOPOLOGY_MUTATION, vars_)


def save_topology_payloads_to_files(
    topo_calls: List[Dict[str, Any]],
    graphql_dir: str,
) -> None:
    """
    Save one combined JSON file with all topology create calls.
    """
    if not topo_calls:
        return

    payloads = [build_topology_payload(t) for t in topo_calls]
    save_payload_to_file(payloads, graphql_dir, "topologies_all.json")


def send_topologies(
    url: str,
    topo_calls: List[Dict[str, Any]],
    headers: Optional[Dict[str, str]] = None,
) -> None:
    """
    Send createTopology for each topology definition.
    """
    for t in topo_calls:
        print(
            f"\nSending topology: process={t['processName']}, "
            f"source={t.get('sourceNodeName')}, sink={t.get('sinkNodeName')}"
        )
        payload = build_topology_payload(t)
        send_graphql_payload(url, payload, headers=headers)


# Markets (createMarket)

_MARKET_MUTATION = """
mutation CreateMarket($market: NewMarket!) {
  createMarket(market: $market) {
    errors {
      field
      message
    }
  }
}
"""


def build_market_payload(market_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build GraphQL payload for a single NewMarket input.
    """
    return build_graphql_payload(_MARKET_MUTATION, {"market": market_input})


def save_market_payloads_to_files(
    markets_inputs: List[Dict[str, Any]],
    graphql_dir: str,
) -> None:
    """
    Save one JSON file per market and one combined file with all market payloads.
    """
    if not markets_inputs:
        return

    os.makedirs(graphql_dir, exist_ok=True)

    all_payloads: List[Dict[str, Any]] = []

    for market in markets_inputs:
        name = market.get("name", "market")
        safe = "".join(c for c in name if c.isalnum() or c in ("_", "-", " ")).strip()
        if not safe:
            safe = "market"
        safe = safe.replace(" ", "_")

        payload = build_market_payload(market)
        all_payloads.append(payload)

        filename = f"market_{safe}.json"
        save_payload_to_file(payload, graphql_dir, filename)

    save_payload_to_file(all_payloads, graphql_dir, "markets_all.json")


def send_markets(
    url: str,
    markets_inputs: List[Dict[str, Any]],
    headers: Optional[Dict[str, str]] = None,
) -> None:
    """
    Send createMarket mutation for all markets, one by one.
    """
    for market in markets_inputs:
        print(f"\nSending market: {market.get('name')}")
        payload = build_market_payload(market)
        send_graphql_payload(url, payload, headers=headers)

# Risk (createRisk)

_RISK_MUTATION = """
mutation CreateRisk($risk: NewRisk!) {
  createRisk(risk: $risk) {
    errors {
      field
      message
    }
  }
}
"""


def build_risk_payload(risk_input: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build GraphQL payload for a single NewRisk input.
    """
    return build_graphql_payload(_RISK_MUTATION, {"risk": risk_input})


def save_risk_payloads_to_files(
    risks_inputs: List[Dict[str, Any]],
    graphql_dir: str,
) -> None:
    """
    Save one JSON with all risk payloads.
    """
    if not risks_inputs:
        return

    os.makedirs(graphql_dir, exist_ok=True)

    payloads: List[Dict[str, Any]] = [
        build_risk_payload(r) for r in risks_inputs
    ]
    save_payload_to_file(payloads, graphql_dir, "risks_all.json")


def send_risks(
    url: str,
    risks_inputs: List[Dict[str, Any]],
    headers: Optional[Dict[str, str]] = None,
) -> None:
    """
    Send createRisk mutation for all risks, one by one.
    """
    for r in risks_inputs:
        print(f"\nSending risk: {r.get('parameter')}")
        payload = build_risk_payload(r)
        send_graphql_payload(url, payload, headers=headers)


