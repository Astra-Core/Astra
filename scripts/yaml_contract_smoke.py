#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VALID_SPEC = ROOT / "examples" / "postgres-to-warehouse.astra.yaml"
INVALID_SPEC = ROOT / "tests" / "fixtures" / "invalid-missing-pipeline-name.astra.yaml"
CONTROL_PLANE_PORT = int(os.environ.get("ASTRA_SMOKE_PORT", "18080"))
BASE_URL = f"http://127.0.0.1:{CONTROL_PLANE_PORT}"


class SmokeFailure(RuntimeError):
    pass


def run(cmd, *, cwd=ROOT, check=True, capture=True, env=None):
    printable = " ".join(cmd)
    print(f"\n$ {printable}")
    completed = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=capture,
    )
    if capture and completed.stdout:
        print(completed.stdout, end="")
    if capture and completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if check and completed.returncode != 0:
        raise SmokeFailure(f"command failed ({completed.returncode}): {printable}")
    return completed


def http_request(path, *, method="GET", json_body=None, expected_status=None):
    url = urllib.parse.urljoin(BASE_URL, path)
    data = None
    headers = {}
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
        headers["content-type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read().decode("utf-8")
            status = response.status
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8")
        status = error.code
    if expected_status is not None and status != expected_status:
        raise SmokeFailure(f"{method} {path} returned {status}, expected {expected_status}. body={body!r}")
    print(f"{method} {path} -> {status}")
    return status, body


def wait_for_server(proc):
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            raise SmokeFailure("control-plane exited before becoming ready")
        try:
            status, _ = http_request("/ready", expected_status=200)
            if status == 200:
                return
        except Exception:
            time.sleep(0.5)
    raise SmokeFailure("timed out waiting for control-plane readiness")


def main():
    if not VALID_SPEC.exists():
        raise SmokeFailure(f"missing valid spec fixture: {VALID_SPEC}")
    if not INVALID_SPEC.exists():
        raise SmokeFailure(f"missing invalid spec fixture: {INVALID_SPEC}")

    valid_yaml = VALID_SPEC.read_text()

    valid_cli = run(["cargo", "run", "-p", "astra", "--", "validate", str(VALID_SPEC)])
    if "valid Astra spec: postgres-analytics" not in valid_cli.stdout:
        raise SmokeFailure("CLI validate did not report the canonical pipeline name")

    invalid_cli = run(["cargo", "run", "-p", "astra", "--", "validate", str(INVALID_SPEC)], check=False)
    if invalid_cli.returncode == 0:
        raise SmokeFailure("CLI unexpectedly accepted the invalid spec")
    cli_error = invalid_cli.stderr + invalid_cli.stdout
    if "pipeline.name must not be empty" not in cli_error:
        raise SmokeFailure(f"CLI invalid-spec error drifted: {cli_error!r}")

    run(["npm", "run", "build"], cwd=ROOT / "apps" / "web")

    env = os.environ.copy()
    env["ASTRA_CONTROL_PLANE_ADDR"] = f"127.0.0.1:{CONTROL_PLANE_PORT}"
    control_plane = subprocess.Popen(
        ["cargo", "run", "-p", "astra-control-plane"],
        cwd=ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        wait_for_server(control_plane)

        _, example_body = http_request("/api/v1/examples/postgres-to-warehouse", expected_status=200)
        if example_body != valid_yaml:
            raise SmokeFailure("control-plane example endpoint drifted from the checked-in canonical YAML")

        _, apply_body = http_request(
            "/api/v1/specs/apply",
            method="POST",
            json_body={"yaml": example_body, "created_by": "yaml-contract-smoke"},
            expected_status=200,
        )
        apply_payload = json.loads(apply_body)
        if apply_payload["pipeline_name"] != "postgres-analytics":
            raise SmokeFailure(f"unexpected applied pipeline name: {apply_payload}")
        if apply_payload["message"] != "pipeline spec applied":
            raise SmokeFailure(f"unexpected apply response payload: {apply_payload}")

        _, pipelines_body = http_request("/api/v1/pipelines", expected_status=200)
        pipelines_payload = json.loads(pipelines_body)
        names = [pipeline["name"] for pipeline in pipelines_payload["pipelines"]]
        if "postgres-analytics" not in names:
            raise SmokeFailure(f"applied pipeline missing from list response: {pipelines_payload}")

        _, invalid_apply_body = http_request(
            "/api/v1/specs/apply",
            method="POST",
            json_body={"yaml": INVALID_SPEC.read_text(), "created_by": "yaml-contract-smoke"},
            expected_status=400,
        )
        if "pipeline.name must not be empty" not in invalid_apply_body:
            raise SmokeFailure(f"control-plane invalid-spec failure drifted: {invalid_apply_body!r}")

        _, index_body = http_request("/", expected_status=200)
        if '<div id="root"></div>' not in index_body:
            raise SmokeFailure("control-plane did not serve the built web UI")

        dist_assets = sorted((ROOT / "apps" / "web" / "dist" / "assets").glob("index-*.js"))
        if not dist_assets:
            raise SmokeFailure("web build did not produce an index JS asset")
        bundle = dist_assets[0].read_text()
        for marker in (
            "/api/v1/examples/postgres-to-warehouse",
            "/api/v1/specs/apply",
            "Canonical example loaded from examples/postgres-to-warehouse.astra.yaml",
        ):
            if marker not in bundle:
                raise SmokeFailure(f"web bundle no longer contains required UI contract marker: {marker}")

        print("\nYAML contract smoke test passed across CLI, control-plane, and web UI wiring.")
    finally:
        control_plane.terminate()
        try:
            control_plane.wait(timeout=10)
        except subprocess.TimeoutExpired:
            control_plane.kill()
            control_plane.wait(timeout=5)
        if control_plane.stdout is not None:
            output = control_plane.stdout.read()
            if output:
                print("\n--- control-plane log ---")
                print(output)


if __name__ == "__main__":
    try:
        main()
    except SmokeFailure as error:
        print(f"\nSMOKE FAILED: {error}", file=sys.stderr)
        sys.exit(1)
