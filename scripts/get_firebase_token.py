"""Generate a Firebase ID token for testing the feed-debug API.

Usage:
  python scripts/get_firebase_token.py <did:plc:...>

Uses ``gcloud auth print-access-token`` to authenticate against the Firebase
REST API — no service account key file needed.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request

FIREBASE_API_KEY = "AIzaSyC7dpqOfJ_hzpuGkUhLA5x5qO6jMPUcrK8"
FIREBASE_PROJECT = "greenearth-471522"


def _gcloud_token() -> str:
    result = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(
            "Failed to get gcloud token. Run: gcloud auth login",
            file=sys.stderr,
        )
        sys.exit(1)
    return result.stdout.strip()


def _gcloud_email() -> str:
    result = subprocess.run(
        ["gcloud", "auth", "list", "--format=value(account)"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip().split("\n")[0] if result.stdout.strip() else "unknown"


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <did>", file=sys.stderr)
        print(f"Example: {sys.argv[0]} did:plc:abc123", file=sys.stderr)
        sys.exit(1)

    did = sys.argv[1]
    access_token = _gcloud_token()

    # 1. Create a custom token via the Firebase REST API.
    custom_url = (
        f"https://identitytoolkit.googleapis.com/v1/"
        f"projects/{FIREBASE_PROJECT}/accounts:createCustomToken?key={FIREBASE_API_KEY}"
    )
    body = json.dumps({"uid": did, "returnDct": True}).encode()
    req = urllib.request.Request(
        custom_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        },
    )
    try:
        resp = json.loads(urllib.request.urlopen(req).read())
    except urllib.request.HTTPError as e:
        body_text = e.read().decode()
        print(f"Error creating custom token: {e.code} {body_text}", file=sys.stderr)
        print(
            f"Your gcloud account ({_gcloud_email()}) may lack permissions.",
            file=sys.stderr,
        )
        print(
            "It needs: roles/iam.serviceAccountTokenCreator on greenearth-471522",
            file=sys.stderr,
        )
        sys.exit(1)

    custom_token: str = resp["customToken"]
    print(f"Custom token created for {did}", file=sys.stderr)

    # 2. Exchange custom token for an ID token.
    signin_body = json.dumps(
        {"token": custom_token, "returnSecureToken": True}
    ).encode()
    signin_url = (
        "https://identitytoolkit.googleapis.com/v1/"
        f"accounts:signInWithCustomToken?key={FIREBASE_API_KEY}"
    )
    req2 = urllib.request.Request(
        signin_url, data=signin_body, headers={"Content-Type": "application/json"}
    )
    resp2 = json.loads(urllib.request.urlopen(req2).read())

    id_token: str = resp2["idToken"]
    print(id_token)


if __name__ == "__main__":
    main()
