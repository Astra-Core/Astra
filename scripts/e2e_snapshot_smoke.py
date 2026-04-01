#!/usr/bin/env python3
"""
End-to-end compose-backed snapshot smoke test.

Proves the full local vertical slice:
  seed source data → execute-local-snapshot (stage + load) → verify destination row counts
  → rerun with --no-resume → verify idempotent chunk application (no duplicates)

Local prerequisites:
  podman compose -f deploy/docker-compose/docker-compose.yml up -d

Local usage:
  python3 scripts/e2e_snapshot_smoke.py

CI usage (GitHub Actions):
  Runs automatically via the e2e-snapshot-smoke job in .github/workflows/ci.yml.
  Postgres is provided as a service container; psql is called directly via DSN.
  Set CI=true (GitHub Actions does this automatically).
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SMOKE_SPEC = ROOT / "examples" / "smoke-local-snapshot.astra.yaml"

PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "astra"
PG_USER = "astra"
PG_PASSWORD = "astra"

SMOKE_USERS_COUNT = 7
SMOKE_ORDERS_COUNT = 12

CONTAINER_NAME = "astra-postgres"


class SmokeFailure(RuntimeError):
    pass


def is_ci():
    return os.environ.get("CI") == "true"


def _psql_dsn():
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"


def detect_container_runtime():
    for runtime in ("podman", "docker"):
        if shutil.which(runtime):
            return runtime
    raise SmokeFailure(
        "neither 'podman' nor 'docker' found on PATH — cannot exec psql in the compose container"
    )


def _psql_run(sql, *, tuples_only=False, check=True):
    if is_ci():
        cmd = ["psql", _psql_dsn()]
    else:
        runtime = detect_container_runtime()
        cmd = [runtime, "exec", "-i", CONTAINER_NAME, "psql", "-U", PG_USER, "-d", PG_DB]

    if tuples_only:
        cmd += ["-t"]
    cmd += ["-c", sql]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if not tuples_only:
        if result.stdout:
            print(result.stdout, end="")
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
    if check and result.returncode != 0:
        raise SmokeFailure(f"psql failed (exit {result.returncode}): {sql!r}")
    return result


def psql(sql, *, check=True):
    print(f"  psql> {sql.strip()[:120]}")
    _psql_run(sql, check=check)


def psql_scalar(sql):
    result = _psql_run(sql, tuples_only=True, check=True)
    return result.stdout.strip()


def check_postgres():
    print("\n--- step 1/5: verify postgres connectivity ---")
    if is_ci():
        result = _psql_run("SELECT 1", tuples_only=True, check=False)
        if result.returncode != 0:
            raise SmokeFailure(
                f"cannot connect to Postgres at {PG_HOST}:{PG_PORT} — "
                "is the CI postgres service healthy?"
            )
        print(f"  Postgres reachable at {PG_HOST}:{PG_PORT} (CI service)")
    else:
        runtime = detect_container_runtime()
        result = subprocess.run(
            [runtime, "inspect", CONTAINER_NAME, "--format", "{{.State.Running}}"],
            capture_output=True, text=True,
        )
        if result.returncode != 0 or result.stdout.strip() != "true":
            raise SmokeFailure(
                f"Postgres container '{CONTAINER_NAME}' is not running.\n"
                "Start it with:\n"
                "  podman compose -f deploy/docker-compose/docker-compose.yml up -d"
            )
        print(f"  container '{CONTAINER_NAME}' is running")


def seed_source_fixtures():
    print("\n--- step 2/5: seed source fixtures ---")

    psql("DROP TABLE IF EXISTS public.smoke_users CASCADE")
    psql("DROP TABLE IF EXISTS public.smoke_orders CASCADE")

    psql(
        "CREATE TABLE public.smoke_users ("
        "id serial PRIMARY KEY, name text NOT NULL, email text NOT NULL)"
    )
    psql(
        "CREATE TABLE public.smoke_orders ("
        "id serial PRIMARY KEY, user_id int NOT NULL, "
        "product text NOT NULL, amount_cents int NOT NULL)"
    )

    users_values = ", ".join(
        f"('User {i}', 'user{i}@smoke.test')" for i in range(1, SMOKE_USERS_COUNT + 1)
    )
    psql(f"INSERT INTO public.smoke_users (name, email) VALUES {users_values}")

    orders_values = ", ".join(
        f"({(i % SMOKE_USERS_COUNT) + 1}, 'Product {i}', {i * 100})"
        for i in range(1, SMOKE_ORDERS_COUNT + 1)
    )
    psql(f"INSERT INTO public.smoke_orders (user_id, product, amount_cents) VALUES {orders_values}")

    got_users = int(psql_scalar("SELECT COUNT(*) FROM public.smoke_users"))
    got_orders = int(psql_scalar("SELECT COUNT(*) FROM public.smoke_orders"))

    if got_users != SMOKE_USERS_COUNT:
        raise SmokeFailure(f"expected {SMOKE_USERS_COUNT} seeded smoke_users rows, got {got_users}")
    if got_orders != SMOKE_ORDERS_COUNT:
        raise SmokeFailure(f"expected {SMOKE_ORDERS_COUNT} seeded smoke_orders rows, got {got_orders}")

    print(f"  seeded {got_users} smoke_users, {got_orders} smoke_orders")


def reset_destination():
    """Drop raw destination tables and applied-chunk tracking from any previous run."""
    psql("DROP TABLE IF EXISTS astra_raw.raw_public_smoke_users CASCADE", check=False)
    psql("DROP TABLE IF EXISTS astra_raw.raw_public_smoke_orders CASCADE", check=False)
    psql(
        "DELETE FROM astra_raw._applied_chunks WHERE pipeline_name = 'smoke-local-snapshot'",
        check=False,
    )


def run_execute_local_snapshot(staging_root, checkpoint_root, *, no_resume=False):
    env = os.environ.copy()
    env["ASTRA_SMOKE_PG_PASSWORD"] = PG_PASSWORD

    cmd = [
        "cargo", "run", "-p", "astra", "--",
        "execute-local-snapshot",
        str(SMOKE_SPEC),
        "--staging-root", str(staging_root),
        "--checkpoint-root", str(checkpoint_root),
        "--chunk-size", "100",
    ]
    if no_resume:
        cmd.append("--no-resume")

    print(f"\n$ {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=ROOT, env=env, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode != 0:
        raise SmokeFailure(f"execute-local-snapshot failed (exit {result.returncode})")


def assert_destination_counts(label, *, expect_users, expect_orders):
    got_users = int(psql_scalar("SELECT COUNT(*) FROM astra_raw.raw_public_smoke_users"))
    got_orders = int(psql_scalar("SELECT COUNT(*) FROM astra_raw.raw_public_smoke_orders"))

    ok = True
    if got_users != expect_users:
        print(
            f"  FAIL [{label}] raw_public_smoke_users: expected {expect_users}, got {got_users}",
            file=sys.stderr,
        )
        ok = False
    else:
        print(f"  OK   raw_public_smoke_users:  {got_users} rows")

    if got_orders != expect_orders:
        print(
            f"  FAIL [{label}] raw_public_smoke_orders: expected {expect_orders}, got {got_orders}",
            file=sys.stderr,
        )
        ok = False
    else:
        print(f"  OK   raw_public_smoke_orders: {got_orders} rows")

    if not ok:
        raise SmokeFailure(f"destination row count mismatch [{label}]")


def cleanup_source_fixtures():
    print("\n--- step 5/5: cleanup ---")
    psql("DROP TABLE IF EXISTS public.smoke_users CASCADE", check=False)
    psql("DROP TABLE IF EXISTS public.smoke_orders CASCADE", check=False)
    print("  fixture tables dropped")


def main():
    if not SMOKE_SPEC.exists():
        raise SmokeFailure(f"smoke spec not found: {SMOKE_SPEC}")

    check_postgres()
    seed_source_fixtures()

    staging_dir = tempfile.mkdtemp(prefix="astra-smoke-staging-")
    checkpoint_dir = tempfile.mkdtemp(prefix="astra-smoke-checkpoints-")

    try:
        print("\n--- step 3/5: first run (clean slate) ---")
        reset_destination()
        run_execute_local_snapshot(staging_dir, checkpoint_dir)
        print("\n  verifying destination after first run:")
        assert_destination_counts(
            "first run",
            expect_users=SMOKE_USERS_COUNT,
            expect_orders=SMOKE_ORDERS_COUNT,
        )

        print("\n--- step 4/5: rerun --no-resume (idempotency) ---")
        # --no-resume re-stages from sequence 0, producing identical object_keys.
        # The loader must skip every chunk via ON CONFLICT DO NOTHING on _applied_chunks.
        run_execute_local_snapshot(staging_dir, checkpoint_dir, no_resume=True)
        print("\n  verifying destination after idempotent rerun (counts must be unchanged):")
        assert_destination_counts(
            "idempotent rerun",
            expect_users=SMOKE_USERS_COUNT,
            expect_orders=SMOKE_ORDERS_COUNT,
        )

        chunk_count = int(psql_scalar(
            "SELECT COUNT(*) FROM astra_raw._applied_chunks "
            "WHERE pipeline_name = 'smoke-local-snapshot'"
        ))
        if chunk_count == 0:
            raise SmokeFailure("_applied_chunks is empty — idempotency bookkeeping did not run")
        print(f"  OK   _applied_chunks has {chunk_count} entry/entries — rerun was skipped correctly")

    finally:
        shutil.rmtree(staging_dir, ignore_errors=True)
        shutil.rmtree(checkpoint_dir, ignore_errors=True)
        cleanup_source_fixtures()

    print(
        "\nSMOKE PASSED: source rows arrived in destination, "
        "idempotent rerun produced no duplicates."
    )


if __name__ == "__main__":
    try:
        main()
    except SmokeFailure as error:
        print(f"\nSMOKE FAILED: {error}", file=sys.stderr)
        sys.exit(1)
