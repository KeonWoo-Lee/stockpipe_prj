"""Run dbt against PG2 using an ephemeral, permission-restricted profile."""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import yaml


def run_dbt(
    config: dict[str, Any],
    project_dir: Path,
    fdw_schema: str,
) -> None:
    target = config["postgres_analytics"]
    dbt_config = config["dbt"]
    profile = {
        dbt_config["profile_name"]: {
            "target": "pipeline",
            "outputs": {
                "pipeline": {
                    "type": "postgres",
                    "host": target["host"],
                    "port": int(target["port"]),
                    "user": target["user"],
                    "password": target["password"],
                    "dbname": target["database"],
                    "schema": target["schema"],
                    "threads": int(dbt_config.get("threads", 4)),
                }
            },
        }
    }

    # TemporaryDirectory is created with mode 0700. Restrict the profile file to 0600.
    with tempfile.TemporaryDirectory(prefix="stockpipe-dbt-profile-") as profile_dir:
        profile_path = Path(profile_dir) / "profiles.yml"
        profile_path.write_text(
            yaml.safe_dump(profile, sort_keys=False), encoding="utf-8"
        )
        profile_path.chmod(0o600)

        env = os.environ.copy()
        env["S2R_FDW_SCHEMA"] = fdw_schema
        common = [
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            profile_dir,
        ]
        subprocess.run(["dbt", "deps", *common], check=True, env=env)

        run_command = [
            "dbt",
            "run",
            *common,
            "--select",
            dbt_config["selector"],
        ]
        vars_value = dbt_config.get("vars") or {}
        if vars_value:
            run_command.extend(["--vars", json.dumps(vars_value, separators=(",", ":"))])
        subprocess.run(run_command, check=True, env=env)

        if dbt_config.get("run_tests", True):
            test_command = [
                "dbt",
                "test",
                *common,
                "--select",
                dbt_config["selector"],
            ]
            if vars_value:
                test_command.extend(["--vars", json.dumps(vars_value, separators=(",", ":"))])
            subprocess.run(test_command, check=True, env=env)
