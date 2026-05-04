import json
import os
import time
import requests


class DbtClient:
    """Client for querying dbt Cloud Discovery + Admin APIs."""

    MAX_RETRIES = 3

    def __init__(self, config):
        self.discovery_url = config["discovery_url"]
        self.environment_id = int(config["environment_id"])
        self.account_id = config["account_id"]
        self.project_id = config["project_id"]
        self.host_url = config["host_url"]
        self.token = config["token"]
        self.name = config.get("name", "unknown")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        self.admin_headers = {
            "Authorization": f"Bearer {self.token}",
        }

    def _retry_request(self, request_fn):
        """Execute a request with retry on 429 rate limits and 502/503 transient errors."""
        for attempt in range(self.MAX_RETRIES):
            resp = request_fn()
            if resp.status_code in (429, 502, 503):
                wait = 2 ** attempt
                print(f"  Transient error ({resp.status_code}), retrying in {wait}s...")
                time.sleep(wait)
                continue
            return resp
        return resp

    def query_discovery(self, graphql_query, variables=None):
        """Execute a GraphQL query against the Discovery API."""
        payload = {"query": graphql_query}
        if variables:
            payload["variables"] = variables
        response = self._retry_request(
            lambda: requests.post(self.discovery_url, json=payload, headers=self.headers)
        )
        response.raise_for_status()
        result = response.json()
        if "errors" in result:
            raise Exception(f"GraphQL errors: {result['errors']}")
        return result["data"]

    def admin_get(self, path, params=None):
        """GET request to the Admin API v2."""
        url = f"https://{self.host_url}/api/v2/accounts/{self.account_id}/{path}"
        resp = self._retry_request(
            lambda: requests.get(url, params=params, headers=self.admin_headers)
        )
        resp.raise_for_status()
        return resp.json()

    def test_connection(self):
        """Verify connectivity."""
        query = """
        query ($environmentId: BigInt!) {
          environment(id: $environmentId) {
            dbtProjectName
            adapterType
            applied {
              lastUpdatedAt
              models(first: 3) {
                edges {
                  node { uniqueId }
                }
              }
            }
          }
        }
        """
        data = self.query_discovery(query, variables={"environmentId": self.environment_id})
        env = data["environment"]
        model_count = len(env["applied"]["models"]["edges"])
        print(f"[{self.name}] Connected to: {env['dbtProjectName']} ({env['adapterType']})")
        print(f"  Last updated: {env['applied']['lastUpdatedAt']}")
        print(f"  Sample models found: {model_count}")
        return True


CREDENTIALS_DIR = os.path.join(os.path.dirname(__file__), "config")
CREDENTIALS_PATH = os.path.join(CREDENTIALS_DIR, "credentials.json")


def load_credentials():
    """Read credentials from config/credentials.json. Returns dict or None."""
    if os.path.exists(CREDENTIALS_PATH):
        with open(CREDENTIALS_PATH) as f:
            data = json.load(f)
        required = ["host_url", "discovery_url", "account_id", "project_id", "environment_id", "token"]
        if all(data.get(k) for k in required):
            if not data.get("account_prefix"):
                data["account_prefix"] = data["host_url"].split(".")[0]
            return data
    return None


def save_credentials(data):
    """Write credentials to config/credentials.json."""
    os.makedirs(CREDENTIALS_DIR, exist_ok=True)
    for k in ["account_prefix", "host_url", "discovery_url", "account_id", "project_id", "environment_id", "token"]:
        if data.get(k):
            data[k] = str(data[k]).strip()
    if not data.get("name"):
        prefix = data.get("account_prefix") or data.get("host_url", "").split(".")[0]
        data["name"] = prefix
    with open(CREDENTIALS_PATH, "w") as f:
        json.dump(data, f, indent=2)


def get_client_from_config():
    """Load credentials and return a DbtClient, or None if not configured."""
    creds = load_credentials()
    if creds is None:
        return None
    return DbtClient(creds)
