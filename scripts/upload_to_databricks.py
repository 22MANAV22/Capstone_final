"""
upload_to_databricks.py
Uploads local .py files to Databricks workspace as RAW Python files (not notebooks).
Usage: python upload_to_databricks.py
Reads DATABRICKS_HOST and DATABRICKS_TOKEN from environment.
"""

import base64
import os
import sys
import urllib.request
import urllib.error
import json


def upload(local_path: str, remote_path: str, host: str, token: str):
    print(f"Uploading {local_path} -> {remote_path}")

    with open(local_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")

    payload = json.dumps({
        "path": remote_path,
        "format": "RAW",
        "language": "PYTHON",
        "content": content,
        "overwrite": True
    }).encode("utf-8")

    url = f"https://{host}/api/2.0/workspace/import"
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req) as resp:
            print(f"  OK (HTTP {resp.status})")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"  FAILED HTTP {e.code}: {body}")
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"  FAILED URL error: {e.reason}")
        print(f"  Host used: {host!r}")
        sys.exit(1)


def main():
    # Strip ALL whitespace including newlines — common when secrets are copy-pasted
    host = os.environ.get("DATABRICKS_HOST", "").strip()
    token = os.environ.get("DATABRICKS_TOKEN", "").strip()

    if not host or not token:
        print("DATABRICKS_HOST or DATABRICKS_TOKEN is not set")
        sys.exit(1)

    # Remove any protocol prefix — we always add https:// ourselves
    host = host.removeprefix("https://").removeprefix("http://")

    # Remove any trailing path slashes
    host = host.rstrip("/")

    print(f"Target host: {host}")

    base = "/Workspace/Shared/Capstone_final/Databricks"

    files = [
        ("Databricks/utils.py",               f"{base}/utils.py"),
        ("Databricks/table_config.py",         f"{base}/table_config.py"),
        ("Databricks/checkpoint_manager.py",   f"{base}/checkpoint_manager.py"),
        ("Databricks/bronze_engine.py",        f"{base}/bronze_engine.py"),
        ("Databricks/silver_engine.py",        f"{base}/silver_engine.py"),
        ("Databricks/gold_engine.py",          f"{base}/gold_engine.py"),
        ("Databricks/cdc_engine.py",           f"{base}/cdc_engine.py"),
    ]

    for local, remote in files:
        upload(local, remote, host, token)

    print("All Python files deployed successfully")


if __name__ == "__main__":
    main()