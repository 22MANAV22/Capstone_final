"""
upload_to_databricks.py
Uploads local .py files to Databricks workspace as RAW Python files (not notebooks).
Deletes any existing notebook at the path before uploading.
"""

import base64
import os
import sys
import urllib.request
import urllib.error
import json


def api_post(host: str, token: str, endpoint: str, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        f"https://{host}{endpoint}",
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8")


def delete(remote_path: str, host: str, token: str):
    """Delete a workspace object if it exists — ignore 404."""
    status, body = api_post(host, token, "/api/2.0/workspace/delete", {
        "path": remote_path,
        "recursive": False
    })
    if status == 200:
        print(f"  Deleted existing object at {remote_path}")
    elif status in (404, 400) or "RESOURCE_DOES_NOT_EXIST" in body:
        print(f"  Nothing to delete at {remote_path}")
    else:
        print(f"  Warning: delete returned HTTP {status}: {body}")


def upload(local_path: str, remote_path: str, host: str, token: str):
    print(f"Uploading {local_path} -> {remote_path}")

    # Always delete first — clears any existing notebook or file type mismatch
    delete(remote_path, host, token)

    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")

    status, body = api_post(host, token, "/api/2.0/workspace/import", {
        "path": remote_path,
        "format": "RAW",
        "language": "PYTHON",
        "content": content,
        "overwrite": True
    })

    if status == 200:
        print(f"  OK")
    else:
        print(f"  FAILED HTTP {status}: {body}")
        sys.exit(1)


def main():
    host = os.environ.get("DATABRICKS_HOST", "").strip()
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()

    if not host or not token:
        print("DATABRICKS_HOST or DATABRICKS_TOKEN is not set")
        sys.exit(1)

    host = host.removeprefix("https://").removeprefix("http://").rstrip("/")
    print(f"Target host: {host}")

    base = "/Workspace/Shared/Capstone_final/Databricks"

    files = [
        ("Databricks/utils.py",              f"{base}/utils.py"),
        ("Databricks/table_config.py",        f"{base}/table_config.py"),
        ("Databricks/checkpoint_manager.py",  f"{base}/checkpoint_manager.py"),
        ("Databricks/bronze_engine.py",       f"{base}/bronze_engine.py"),
        ("Databricks/silver_engine.py",       f"{base}/silver_engine.py"),
        ("Databricks/gold_engine.py",         f"{base}/gold_engine.py"),
        ("Databricks/cdc_engine.py",          f"{base}/cdc_engine.py"),
    ]

    for local, remote in files:
        upload(local, remote, host, token)

    print("\nAll Python files deployed successfully")


if __name__ == "__main__":
    main()