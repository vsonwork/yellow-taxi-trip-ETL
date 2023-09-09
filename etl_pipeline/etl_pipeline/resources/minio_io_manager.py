from contextlib import contextmanager

import polars as pl
from minio import Minio
from dagster import IOManager, OutputContext, InputContext
import os

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("minio_access_key_id"),
        secret_key=config.get("minio_secret_access_key"),
        secure=False
    )
    try:
        yield client
    except Exception:
        raise

class MinIOIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _get_path(self, context):
        # Get context.asset_key.path: ['bronze', 'trip', 'yellow_tripdata_{year}']
        layer, schema, table = context.asset_key.path
        # Ex: bronze/trip/yellow_tripdata_{year}
        key = f"{layer}/{schema}/{table}"
        # Ex: tmp/bronze/trip/
        tmp_dir_path = f"/tmp/{layer}/{schema}/"

        os.makedirs(tmp_dir_path, exist_ok=True)
        tmp_file_path = f"{tmp_dir_path}{table}.parquet"

        # Ex: bronze/trip/yellow_tripdata_{year}.parquet, tmp/bronze/trip/yellow_tripdata_{year}.parquet
        return f"{key}.parquet", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        key_name, tmp_file_path = self._get_path(context)
        obj.write_parquet(tmp_file_path)

        # Upload to MinIO
        bucket_name = self._config.get("bucket")
        with connect_minio(self._config) as client:
            found = client.bucket_exists(bucket_name)
            if not found:
                client.make_bucket(bucket_name)
            else:
                print(f"Bucket {bucket_name} already exists")

            client.fput_object(
                bucket_name, key_name, tmp_file_path
            )
            context.add_output_metadata({"path": key_name, "tmp": tmp_file_path})

        # Clean up tmp file
        os.remove(tmp_file_path)

    def load_input(self, context: InputContext) -> pl.DataFrame:
        key_name, tmp_file_path = self._get_path(context)
        bucket_name = self._config.get("bucket")
        with connect_minio(self._config) as client:
            client.fget_object(
                self._config["bucket"], key_name, tmp_file_path,
            )
        # read and return as Pandas Dataframe
        return pl.read_parquet(tmp_file_path)
