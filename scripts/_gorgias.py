"""Shared Gorgias helpers. Reads auth from env (GORGIAS_AUTH_TOKEN)."""

import base64
import json
import os
import sys
import time
import urllib.request
import urllib.error


BASE = "https://luxuryconfidence.gorgias.com"
UA = "Mozilla/5.0 (compatible; nwp-dashboard-bot/1.0)"

# env: GORGIAS_AUTH_TOKEN = "user:token" (or just the token — we infer)
_raw = os.environ.get("GORGIAS_AUTH_TOKEN", "")
if ":" not in _raw:
    print("ERROR: GORGIAS_AUTH_TOKEN must be in 'email:token' format", file=sys.stderr)
    sys.exit(2)
BASIC = base64.b64encode(_raw.encode()).decode()


def api(method, path, body=None, max_tries=6):
    """HTTP request with retry/backoff. Returns parsed JSON."""
    url = BASE + path if path.startswith("/") else path
    data = json.dumps(body).encode() if body is not None else None
    headers = {
        "Authorization": f"Basic {BASIC}",
        "User-Agent": UA,
        "Accept": "application/json",
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    last_err = None
    for attempt in range(max_tries):
        try:
            req = urllib.request.Request(url, data=data, method=method, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}: {e.read()[:200]}"
            if e.code == 429:
                time.sleep(2 ** attempt + 1)
                continue
            time.sleep(1 + attempt)
        except Exception as e:
            last_err = str(e)
            time.sleep(1 + attempt)
    raise RuntimeError(f"Gorgias {method} {path} failed: {last_err}")


def get(path):
    return api("GET", path)


def post(path, body):
    return api("POST", path, body)
