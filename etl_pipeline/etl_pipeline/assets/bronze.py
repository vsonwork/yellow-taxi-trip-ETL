import polars as pl
import geopandas as gpd
import pyproj
from dagster import asset, Output
import os

COMPUTE_KIND = "Polars"
LAYER = "bronze"
SCHEMA = "trip"
YEAR = "2023"
adr_data = r"D:\UNIVERSITY\yellow-taxis-etl-pipeline\yellow_taxis_dataset"
zone_data = "taxi_zones"

def create_dataset(data_info):
    address, name_asset, name = data_info

    @asset(
        name=name_asset,
        io_manager_key="minio_io_manager",
        key_prefix=[LAYER, SCHEMA],
        compute_kind=COMPUTE_KIND,
        group_name=LAYER,
    )
    def bronze_dataset(context) -> Output[pl.DataFrame]:
        df = pl.read_parquet(address)
        if name not in ['yellow_tripdata_2023-01.parquet']:
            # Rename columns to have consistent names
            column_renames = {
                "Airport_fee": "airport_fee"
            }
            df = df.rename(column_renames)

        df = df.with_columns([df["VendorID"].cast(pl.Int64),
                              df["passenger_count"].cast(pl.Float64),
                              df["RatecodeID"].cast(pl.Float64),
                              df["PULocationID"].cast(pl.Int64),
                              df["DOLocationID"].cast(pl.Int64)])
        return Output(
            df,
            metadata={
                "directory": address,
                "table": name_asset,
                "records count": len(df),
            }
        )

    return bronze_dataset

# 2023: months(1 - 6)
# def get_file():
# #     list_data = []
# #     for root, dirs, files in os.walk(f"{adr_data}\\{YEAR}", topdown=False):
# #         for name in files:
# #             if (name.find(YEAR) >= 0):
# #                 file_name = os.path.splitext(name)[0].replace('-','_')
# #                 address = root + '\\' + name
# #                 name_asset = f"{LAYER}_{file_name}"
# #                 list_data.append([address, name_asset, name])
# #     return list_data
# #
# # list_data = get_file()
# #
# # silver_yellow_tripdata_2023_01 = create_dataset(list_data[0])
# # silver_yellow_tripdata_2023_02 = create_dataset(list_data[1])
# # silver_yellow_tripdata_2023_03 = create_dataset(list_data[2])
# # silver_yellow_tripdata_2023_04 = create_dataset(list_data[3])
# # silver_yellow_tripdata_2023_05 = create_dataset(list_data[4])
# # silver_yellow_tripdata_2023_06 = create_dataset(list_data[5])


for root, dirs, files in os.walk(f"{adr_data}\\{YEAR}", topdown=False):
    for name in files:
        if (name.find(YEAR) >= 0):
            file_name = os.path.splitext(name)[0].replace('-','_')
            address = root + '\\' + name
            name_asset = f"{LAYER}_{file_name}"
            # if (file_name.find("01") >= 0):
            #     silver_yellow_tripdata_2023_01 = create_dataset([address, name_asset, name])
            # elif (file_name.find("02") >= 0):
            #     silver_yellow_tripdata_2023_02 = create_dataset([address, name_asset, name])
            # elif (file_name.find("03") >= 0):
            #     silver_yellow_tripdata_2023_03 = create_dataset([address, name_asset, name])
            # elif (file_name.find("04") >= 0):
            #     silver_yellow_tripdata_2023_04 = create_dataset([address, name_asset, name])
            if (file_name.find("05") >= 0):
                silver_yellow_tripdata_2023_05 = create_dataset([address, name_asset, name])
            elif (file_name.find("06") >= 0):
                silver_yellow_tripdata_2023_06 = create_dataset([address, name_asset, name])
            elif (file_name.find("07") >= 0):
                silver_yellow_tripdata_2023_07 = create_dataset([address, name_asset, name])
            elif (file_name.find("08") >= 0):
                silver_yellow_tripdata_2023_08 = create_dataset([address, name_asset, name])
            elif (file_name.find("09") >= 0):
                silver_yellow_tripdata_2023_09 = create_dataset([address, name_asset, name])
            elif (file_name.find("10") >= 0):
                silver_yellow_tripdata_2023_10 = create_dataset([address, name_asset, name])
            elif (file_name.find("11") >= 0):
                silver_yellow_tripdata_2023_11 = create_dataset([address, name_asset, name])
            elif (file_name.find("12") >= 0):
                silver_yellow_tripdata_2023_12 = create_dataset([address, name_asset, name])


# ==================================================================================

@asset(
        io_manager_key="minio_io_manager",
        key_prefix=[LAYER, "zone"],
        compute_kind="Pandas",
        group_name=LAYER,
    )
def taxi_zone_lat_lon(context) -> Output[pl.DataFrame]:
    shapefile_path = f"{adr_data}/{zone_data}/{zone_data}.shp"

    gdf = gpd.read_file(shapefile_path)
    # Define
    source_crs = gdf.crs  # CRS of the shapefile
    target_crs = 'EPSG:4326'  # WGS84 - lat/lon CRS

    # Create a PyProj transformer
    transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)

    gdf['longitude'] = gdf.geometry.centroid.x
    gdf['latitude'] = gdf.geometry.centroid.y
    gdf['longitude'], gdf['latitude'] = transformer.transform(gdf['longitude'], gdf['latitude'])

    df = pl.DataFrame(gdf[['LocationID', 'longitude', 'latitude']])
    return Output(
        df,
        metadata={
            "directory": shapefile_path,
            "table": "taxi_zone_lat_lon",
            "records count": len(df),
        }
    )