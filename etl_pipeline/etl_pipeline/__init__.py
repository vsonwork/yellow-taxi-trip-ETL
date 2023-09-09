from dagster import Definitions, load_assets_from_modules, file_relative_path
from dagster_dbt import load_assets_from_dbt_project, DbtCliClientResource

from . import assets
from .resources.minio_io_manager import MinIOIOManager
from .resources.psql_io_manager import PostgreSQLIOManager

import os

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "minio_access_key_id": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_access_key": os.getenv("MINIO_SECRET_KEY"),
}

PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}



all_assets = load_assets_from_modules([assets])

DBT_PROJECT_PATH = file_relative_path(__file__, "../dbt_taxi")
DBT_PROFILES = file_relative_path(__file__, "../dbt_taxi")
dbt_asset = load_assets_from_dbt_project(
    profiles_dir=DBT_PROFILES,
    project_dir=DBT_PROJECT_PATH,
    key_prefix=["public"]
)
# define list of assets and resources for data pipeline
defs = Definitions(
    assets=all_assets + dbt_asset,
    resources={
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
        "dbt": DbtCliClientResource(
            profiles_dir=DBT_PROFILES,
            project_dir=DBT_PROJECT_PATH,
        )
    },
)