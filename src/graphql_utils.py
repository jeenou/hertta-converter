import json


def build_graphql_payload(setup_input: dict) -> dict:
    mutation = """
    mutation CreateInputDataSetup($setup: InputDataSetupInput!) {
      createInputDataSetup(setupUpdate: $setup) {
        errors {
          field
          message
        }
      }
    }
    """

    return {
        "query": mutation,
        "variables": {
            "setup": setup_input
        },
    }


def save_payload_to_file(payload: dict, graphql_dir: str):
    out_path = f"{graphql_dir}/inputdatasetup.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved GraphQL payload: {out_path}")
