from dagster import asset, AssetIn, Output
import polars as pl

COMPUTE_KIND = "Polars"
LAYER = "silver"
SCHEMA = "trip"

@asset(
    io_manager_key="minio_io_manager",
    ins={
        #"bronze_yellow_tripdata_2023_01": AssetIn(key_prefix=["bronze", SCHEMA]),
        #"bronze_yellow_tripdata_2023_02": AssetIn(key_prefix=["bronze", SCHEMA]),
        #"bronze_yellow_tripdata_2023_03": AssetIn(key_prefix=["bronze", SCHEMA]),
        #"bronze_yellow_tripdata_2023_04": AssetIn(key_prefix=["bronze", SCHEMA]),
        "bronze_yellow_tripdata_2023_05": AssetIn(key_prefix=["bronze", SCHEMA]),
        "bronze_yellow_tripdata_2023_06": AssetIn(key_prefix=["bronze", SCHEMA]),
    },
    key_prefix=[LAYER, SCHEMA],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Merge all trip files"
)
def silver_yellow_tripdata_2023(context,
                                bronze_yellow_tripdata_2023_05,
                                bronze_yellow_tripdata_2023_06) -> Output[pl.DataFrame]:

    df = pl.concat([bronze_yellow_tripdata_2023_05,
                    bronze_yellow_tripdata_2023_06])
    df = df.unique(maintain_order=True)
    return Output(
        df,
        metadata={
            "table": "silver_yellow_tripdata_2023",
            "records count": len(df),
        }
    )

# Split schema into 7 separate assets

# datetime
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_yellow_tripdata_2023": AssetIn(key_prefix=[LAYER, SCHEMA]),
    },
    key_prefix=[LAYER, "schema"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Datetime dimension table"
)
def silver_datetime(context, silver_yellow_tripdata_2023):
    datetime = silver_yellow_tripdata_2023.select([
        pl.col('tpep_pickup_datetime'),
        pl.col('tpep_dropoff_datetime')
    ]).unique(maintain_order=True)

    datetime_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    time_info = ["weekday", "day", "month", "year", "hour", "minute", "second"]

    for col in datetime_columns:
        dt = pl.col(col).dt
        for info in time_info:
            datetime = datetime.with_columns(
                pl.arange(1, len(datetime) + 1).alias("datetime_ID"),
                dt.__getattribute__(info)().alias(f"{col.split('_')[1]}_{info}")
            )

    return Output(
        datetime,
        metadata={
            "table": "silver_datetime",
            "records count": len(datetime),
        }
    )

# passenger_count
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_yellow_tripdata_2023": AssetIn(key_prefix=[LAYER, SCHEMA]),
    },
    key_prefix=[LAYER, "schema"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Passenger count dimension table"
)
def silver_passenger_count(context, silver_yellow_tripdata_2023):
    passenger_count = silver_yellow_tripdata_2023[['passenger_count']].unique(maintain_order=True)
    passenger_count = passenger_count.with_columns(
        pl.arange(1, len(passenger_count) + 1).alias("passenger_count_ID")
    )
    passenger_count = passenger_count.select([
        pl.col('passenger_count_ID'),
        pl.col('passenger_count')
    ])

    return Output(
        passenger_count,
        metadata={
            "table": "silver_passenger_count",
            "records count": len(passenger_count),
        }
    )

# trip_distance
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_yellow_tripdata_2023": AssetIn(key_prefix=[LAYER, SCHEMA]),
    },
    key_prefix=[LAYER, "schema"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Trip distance dimension table"
)
def silver_trip_distance(context, silver_yellow_tripdata_2023):
    trip_distance = silver_yellow_tripdata_2023[['trip_distance']].unique(maintain_order=True)
    trip_distance = trip_distance.with_columns(
        pl.arange(1, len(trip_distance) + 1).alias("trip_distance_ID")
    )
    trip_distance = trip_distance.select([
        pl.col('trip_distance_ID'),
        pl.col('trip_distance')
    ])
    return Output(
        trip_distance,
        metadata={
            "table": "silver_trip_distance",
            "records count": len(trip_distance),
        }
    )

# rate_code
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_yellow_tripdata_2023": AssetIn(key_prefix=[LAYER, SCHEMA]),
    },
    key_prefix=[LAYER, "schema"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Rate code name dimension table"
)
def silver_rate_code(context, silver_yellow_tripdata_2023):
    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    }

    rate_code = silver_yellow_tripdata_2023[['RatecodeID']].unique(maintain_order=True)
    rate_code = rate_code.with_columns(
        pl.arange(1, len(rate_code) + 1).alias("rate_code_ID"),
        rate_code['RatecodeID'].apply(rate_code_type.get).alias("rate_code_name")
    )
    rate_code = rate_code.select([
        pl.col('rate_code_ID'),
        pl.col('RatecodeID'),
        pl.col('rate_code_name')
    ])
    return Output(
        rate_code,
        metadata={
            "table": "silver_rate_code",
            "records count": len(rate_code),
        }
    )

# payment_type
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_yellow_tripdata_2023": AssetIn(key_prefix=[LAYER, SCHEMA]),
    },
    key_prefix=[LAYER, "schema"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Payment type name dimension table"
)
def silver_payment_type_name(context, silver_yellow_tripdata_2023):
    payment_type_name = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }

    payment_type = silver_yellow_tripdata_2023[['payment_type']].unique(maintain_order=True)
    payment_type = payment_type.with_columns(
        pl.arange(1, len(payment_type) + 1).alias("payment_type_ID"),
        payment_type['payment_type'].apply(payment_type_name.get).alias("payment_type_name")
    )
    payment_type = payment_type.select([
        pl.col('payment_type_ID'),
        pl.col('payment_type'),
        pl.col('payment_type_name')
    ])
    return Output(
        payment_type,
        metadata={
            "table": "silver_payment_type_name",
            "records count": len(payment_type),
        }
    )

# PULocationID
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_yellow_tripdata_2023": AssetIn(key_prefix=[LAYER, SCHEMA]),
    },
    key_prefix=[LAYER, "schema"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Pick up location ID dimension table"
)
def silver_pickup_locationID(context, silver_yellow_tripdata_2023):
    pickup_location = silver_yellow_tripdata_2023[['PULocationID']].unique(maintain_order=True)
    pickup_location = pickup_location.with_columns(
        pl.arange(1, len(pickup_location) + 1).alias("pickup_location_ID")
    )
    pickup_location = pickup_location.select([
        pl.col('pickup_location_ID'),
        pl.col('PULocationID')
    ])

    return Output(
        pickup_location,
        metadata={
            "table": "silver_pickup_locationID",
            "records count": len(pickup_location),
        }
    )

# DOLocationID
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_yellow_tripdata_2023": AssetIn(key_prefix=[LAYER, SCHEMA]),
    },
    key_prefix=[LAYER, "schema"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
    description="Drop off location ID dimension table"
)
def silver_dropoff_locationID(context, silver_yellow_tripdata_2023):
    dropoff_location = silver_yellow_tripdata_2023[['DOLocationID']].unique(maintain_order=True)
    dropoff_location = dropoff_location.with_columns(
        pl.arange(1, len(dropoff_location) + 1).alias("dropoff_location_ID")
    )
    dropoff_location = dropoff_location.select([
        pl.col('dropoff_location_ID'),
        pl.col('DOLocationID')
    ])


    return Output(
        dropoff_location,
        metadata={
            "table": "silver_dropoff_locationID",
            "records count": len(dropoff_location),
        }
    )

# fact table
@asset(
    io_manager_key="minio_io_manager",
    ins={
        "silver_datetime"            :    AssetIn(key_prefix=[LAYER, "schema"]),
        "silver_dropoff_locationID"  :    AssetIn(key_prefix=[LAYER, "schema"]),
        "silver_passenger_count"     :    AssetIn(key_prefix=[LAYER, "schema"]),
        "silver_payment_type_name"   :    AssetIn(key_prefix=[LAYER, "schema"]),
        "silver_pickup_locationID"   :    AssetIn(key_prefix=[LAYER, "schema"]),
        "silver_rate_code"           :    AssetIn(key_prefix=[LAYER, "schema"]),
        "silver_trip_distance"       :    AssetIn(key_prefix=[LAYER, "schema"]),
        "silver_yellow_tripdata_2023":    AssetIn(key_prefix=[LAYER, SCHEMA]),
    },
    key_prefix=[LAYER, "schema"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER
)
def silver_fact_table(context, silver_datetime, silver_dropoff_locationID,
                        silver_passenger_count, silver_payment_type_name,
                        silver_pickup_locationID, silver_rate_code,
                        silver_trip_distance, silver_yellow_tripdata_2023):
    fact_table = (
        silver_yellow_tripdata_2023
        .join(silver_datetime, on=["tpep_pickup_datetime", "tpep_dropoff_datetime"], how="left")
        .join(silver_dropoff_locationID, on='DOLocationID', how="left")
        .join(silver_passenger_count, on='passenger_count', how="left")
        .join(silver_payment_type_name, on='payment_type', how="left")
        .join(silver_pickup_locationID, on='PULocationID', how="left")
        .join(silver_rate_code, on='RatecodeID', how="left")
        .join(silver_trip_distance, on='trip_distance', how="left")
        .select([
            'VendorID', 'datetime_ID', 'passenger_count_ID',
            'trip_distance_ID', 'rate_code_ID', 'store_and_fwd_flag',
            'pickup_location_ID', 'dropoff_location_ID',
            'payment_type_ID', 'fare_amount', 'extra', 'mta_tax',
            'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount'
        ])
    )
    return Output(
        fact_table,
        metadata={
            "table": "silver_fact_table",
            "records count": len(fact_table),
        }
    )