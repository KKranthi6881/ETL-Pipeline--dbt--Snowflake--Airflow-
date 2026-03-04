import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakePrivateKeyPemProfileMapping, SnowflakeUserPasswordProfileMapping


def _build_profile_mapping():
    auth_method = os.getenv("AIRFLOW_SNOWFLAKE_AUTH_METHOD", "private_key").lower()
    mapping_cls = (
        SnowflakePrivateKeyPemProfileMapping
        if auth_method in {"private_key", "keypair", "jwt"}
        else SnowflakeUserPasswordProfileMapping
    )
    return mapping_cls(
        conn_id=os.getenv("AIRFLOW_SNOWFLAKE_CONN_ID", "snowflake_conn_id"),
        profile_args={
            "database": os.getenv("DBT_SNOWFLAKE_DATABASE", "DUCKCODE_TEST_DATA"),
            "schema": os.getenv("DBT_SNOWFLAKE_SCHEMA", "ANALYTICS"),
        },
    )


profile_config = ProfileConfig(
    profile_name="data_pipeline_snowflake",
    target_name="dev",
    profile_mapping=_build_profile_mapping(),
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/data_pipeline_snowflake",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag",
)
