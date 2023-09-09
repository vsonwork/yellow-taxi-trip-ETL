from dagster import asset, AssetIn, Output
import polars as pl

COMPUTE_KIND = "Postgres"
LAYER = "warehouse"

@asset(
    io_manager_key="psql_io_manager",
    ins={
        "gold_pickup_location_lat_lon": AssetIn(key_prefix=["gold", "trip"]),
    },
    key_prefix=["public"],
    group_name=LAYER,
    compute_kind=COMPUTE_KIND
)
def pickup_location_lat_lon(context, gold_pickup_location_lat_lon) -> Output[pl.DataFrame]:

    return Output(
        gold_pickup_location_lat_lon,
        metadata={
            "table": "pickup_location_lat_lon",
            "records count": len(gold_pickup_location_lat_lon)
        }
    )

@asset(
    io_manager_key="psql_io_manager",
    ins={
        "gold_dropoff_location_lat_lon": AssetIn(key_prefix=["gold", "trip"]),
    },
    key_prefix=["public"],
    group_name=LAYER,
    compute_kind=COMPUTE_KIND
)
def dropoff_location_lat_lon(context, gold_dropoff_location_lat_lon) -> Output[pl.DataFrame]:

    return Output(
        gold_dropoff_location_lat_lon,
        metadata={
            "table": "dropoff_location_lat_lon",
            "records count": len(gold_dropoff_location_lat_lon)
        }
    )

@asset(
    io_manager_key="psql_io_manager",
    ins={
        "gold_trip_schedule": AssetIn(key_prefix=["gold", "trip"]),
    },
    key_prefix=["public"],
    group_name=LAYER,
    compute_kind=COMPUTE_KIND
)
def trip_schedule(context, gold_trip_schedule) -> Output[pl.DataFrame]:

    return Output(
        gold_trip_schedule,
        metadata={
            "table": "trip_schedule",
            "records count": len(gold_trip_schedule)
        }
    )
