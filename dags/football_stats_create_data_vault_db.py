import os
from datetime import datetime
from pathlib import Path
from airflow.datasets import Dataset

from cosmos import DbtDag, ExecutionMode, ExecutionConfig, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import TestBehavior

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
PROJECT_NAME = "fstats"

DBT_START = Dataset("greenplum://create_ext")

profile_config = ProfileConfig(
    profile_name="fstats",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="Greenplum",
        profile_args={"schema": ""},
    ),
)

run_dbt_fstats = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=ProjectConfig(
        DBT_ROOT_PATH / "fstats",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    render_config=RenderConfig(
        test_behavior=TestBehavior.NONE,
    ),
    operator_args={
        "install_deps": True,
    },
    # normal dag parameters
    schedule=[DBT_START, ],
    start_date=datetime(2024, 8, 9),
    catchup=False,
    dag_id="run_dbt_fstats",
    default_args={"retries": 2, "owner": "airflow"},
)

run_dbt_fstats
