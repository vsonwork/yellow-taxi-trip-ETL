from dagster import asset, AssetIn, Output
import polars as pl

COMPUTE_KIND = "Polars"
LAYER = "gold"
SCHEMA = "trip"

# pickup_location_lat_lon
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_pickup_locationID": AssetIn(key_prefix=["silver", "schema"]),
        "taxi_zone_lat_lon": AssetIn(key_prefix=["bronze", "zone"]),
    },
    key_prefix=[LAYER, SCHEMA],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Pick up location dimension table drop columns 'LocationID'"
)
def gold_pickup_location_lat_lon(context, silver_pickup_locationID, taxi_zone_lat_lon):
    taxi_zone_lat_lon = taxi_zone_lat_lon.with_columns(
        pl.arange(1, len(taxi_zone_lat_lon) + 1).alias("df_loc_id")
    )
    taxi_zone_lat_lon = taxi_zone_lat_lon.select([
        pl.col('df_loc_id').alias("LocationID"),
        pl.col('longitude'),
        pl.col('latitude')
    ])
    silver_pickup_locationID = silver_pickup_locationID.rename({"PULocationID": "LocationID"})
    silver_pickup_locationID = silver_pickup_locationID.join(taxi_zone_lat_lon, on="LocationID", how="left")
    pickup_location_lat_lon = silver_pickup_locationID.drop("LocationID")

    return Output(
        pickup_location_lat_lon,
        metadata={
            "table": "gold_pickup_location_lat_lon",
            "records count": len(pickup_location_lat_lon),
        }
    )

# dropoff_location_lat_lon
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_dropoff_locationID": AssetIn(key_prefix=["silver", "schema"]),
        "taxi_zone_lat_lon": AssetIn(key_prefix=["bronze", "zone"]),
    },
    key_prefix=[LAYER, SCHEMA],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Drop off location dimension table drop columns 'LocationID'"
)
def gold_dropoff_location_lat_lon(context, silver_dropoff_locationID, taxi_zone_lat_lon):
    taxi_zone_lat_lon = taxi_zone_lat_lon.with_columns(
        pl.arange(1, len(taxi_zone_lat_lon) + 1).alias("df_loc_id")
    )
    taxi_zone_lat_lon = taxi_zone_lat_lon.select([
        pl.col('df_loc_id').alias("LocationID"),
        pl.col('longitude'),
        pl.col('latitude')
    ])

    silver_dropoff_locationID = silver_dropoff_locationID.rename({"DOLocationID": "LocationID"})
    silver_dropoff_locationID = silver_dropoff_locationID.join(taxi_zone_lat_lon, on="LocationID", how="left")
    dropoff_location_lat_lon = silver_dropoff_locationID.drop("LocationID")

    return Output(
        dropoff_location_lat_lon,
        metadata={
            "table": "gold_dropoff_location_lat_lon",
            "records count": len(dropoff_location_lat_lon),
        }
    )

fact_trip_schedule = '''
    SELECT
        fact_table.VendorID, fact_table.pickup_location_ID, fact_table.dropoff_location_ID,
        datetime.pickup_weekday, datetime.pickup_hour,
        datetime.dropoff_weekday, datetime.dropoff_hour,
        passenger_count.passenger_count,
        trip_distance.trip_distance,
        rate_code.rate_code_name, payment_type_name.payment_type_name,
        fact_table.fare_amount,
        fact_table.extra,
        fact_table.mta_tax,
        fact_table.tip_amount,
        fact_table.tolls_amount,
        fact_table.improvement_surcharge,
        fact_table.total_amount
    FROM fact_table
    JOIN datetime ON fact_table.datetime_ID=datetime.datetime_ID
    JOIN passenger_count ON passenger_count.passenger_count_ID=fact_table.passenger_count_ID
    JOIN trip_distance ON trip_distance.trip_distance_ID=fact_table.trip_distance_ID
    JOIN rate_code ON rate_code.rate_code_ID=fact_table.rate_code_ID
    JOIN payment_type_name ON payment_type_name.payment_type_ID=fact_table.payment_type_ID
'''

# trip schedule
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_datetime"            :    AssetIn(key_prefix=["silver", "schema"]),
        "silver_passenger_count"     :    AssetIn(key_prefix=["silver", "schema"]),
        "silver_payment_type_name"   :    AssetIn(key_prefix=["silver", "schema"]),
        "silver_rate_code"           :    AssetIn(key_prefix=["silver", "schema"]),
        "silver_trip_distance"       :    AssetIn(key_prefix=["silver", "schema"]),
        "silver_fact_table"          :    AssetIn(key_prefix=["silver", "schema"]),
    },
    key_prefix=[LAYER, SCHEMA],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER
)
def gold_trip_schedule(context, silver_datetime, silver_passenger_count,
                        silver_payment_type_name, silver_rate_code,
                        silver_trip_distance, silver_fact_table):
    ctx = pl.SQLContext().register_many({"datetime": silver_datetime, "passenger_count": silver_passenger_count,
                                         "payment_type_name": silver_payment_type_name, "rate_code": silver_rate_code,
                                         "trip_distance": silver_trip_distance, "fact_table": silver_fact_table})
    df = ctx.execute(fact_trip_schedule).collect()
    return Output(
        df,
        metadata={
            "table": "gold_trip_schedule",
            "records count": len(df),
        }
    )
