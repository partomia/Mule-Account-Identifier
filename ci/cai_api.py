"""Minimal CAI API v2 client (standard library only)."""
from __future__ import annotations

import json
import urllib.parse
import urllib.request

OK = {"succeeded"}
BAD = {"failed", "stopped", "timedout"}


class Workbench:
    def __init__(self, url: str, key: str):
        url = url.rstrip("/")
        self.base = f"{url if url.startswith('http') else 'https://' + url}/api/v2"
        self.headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}

    def __call__(self, method: str, path: str, body: dict | None = None, params: dict | None = None) -> dict:
        url = self.base + path + (f"?{urllib.parse.urlencode(params)}" if params else "")
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, headers=self.headers, method=method)
        with urllib.request.urlopen(req, timeout=120) as r:
            text = r.read().decode()
        return json.loads(text) if text else {}

    def download(self, project_id: str, path: str) -> bytes:
        """A project file: POST /files/<path>:download (a GET returns 404)."""
        quoted = urllib.parse.quote(path, safe="")
        req = urllib.request.Request(f"{self.base}/projects/{project_id}/files/{quoted}:download",
                                     data=b"", headers=self.headers, method="POST")
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.read()


def find_project(wb: Workbench, name: str) -> dict | None:
    found = wb("GET", "/projects", params={"search_filter": json.dumps({"name": name}), "page_size": 100})
    return next((p for p in found.get("projects", []) if p["name"] == name), None)


def job_ids(wb: Workbench, project_id: str) -> dict:
    return {j["name"]: j["id"] for j in wb("GET", f"/projects/{project_id}/jobs",
                                           params={"page_size": 200}).get("jobs", [])}


def status_of(run: dict) -> str:
    return str(run.get("status", "")).lower().replace("engine_", "")
