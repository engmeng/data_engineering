import requests
from Taxi_Pipeline.assets import constants
from Taxi_Pipeline.partitions import monthly_partition
import dagster as dg
import duckdb
import os
import pandas as pd
from dagster_duckdb import DuckDBResource
from dagster_azure.adls2 import ADLS2Resource, ADLS2SASToken
from azure.storage.filedatalake import DataLakeFileClient
import io
from pyspark.sql import SparkSession
from deltalake import DeltaTable, write_deltalake

@dg.asset(
  partitions_def = monthly_partition,
  group_name="Raw_Files",
  compute_kind="Python",
  )
def yellow_taxi_trips_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
      The raw parquet files for the yellow taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    # Choose the month to fetch from the URL
    #month_to_fetch = '2023-03'
    # This is to allow backfilling
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    
    
    # Save the parquet file 
    with open(constants.YELLOW_TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)
        
    num_rows = len(pd.read_parquet(constants.YELLOW_TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))

    return dg.MaterializeResult(
      metadata={
                'Number of records': dg.MetadataValue.int(num_rows)
            }
        )
    
# We want to be able to get the yellow and green cab information
@dg.asset(
  partitions_def = monthly_partition,
  group_name="Raw_Files",
  compute_kind="Python",
  )
def green_taxi_trips_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
      The raw parquet files for the green taxi trips dataset.
    """
    # Choose the month to fetch from the URL
    # This is to allow backfilling
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{month_to_fetch}.parquet"
    )
    
    # Save the parquet file 
    with open(constants.GREEN_TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)
        
    num_rows = len(pd.read_parquet(constants.GREEN_TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))

    return dg.MaterializeResult(
      metadata={
                'Number of records': dg.MetadataValue.int(num_rows)
            }
        )
        
# After getting the raw files, we can push them into postgres or duckdb
# For this we will be using duckdb first
# We can dockerize and push to postgres as well!
# Dockerizing would probably be important since we would want to deploy on AKS as well

# Load the files in the datalake/ local directory into the the duckdb database
# The loading of the file is dependant on the taxi_trips_file function 
# For this part we are loading the taxi trips file to the taxi trips function
@dg.asset(
    deps=["yellow_taxi_trips_file"],
    partitions_def=monthly_partition,
    group_name="Local_Database",
    compute_kind="DuckDB",
)
def yellow_taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> dg.MaterializeResult:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    
    # So we will load in information into the duckdb by month  
    # We will need to create the table if it doesnt exist and then load in the information
    
    query = f"""
      CREATE TABLE IF NOT EXISTS yellow_trips
      (
      vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
      rate_code_id double, payment_type integer, dropoff_datetime timestamp,
      pickup_datetime timestamp, trip_distance double, passenger_count double,
      total_amount double, partition_date varchar, partition_type varchar
      );
      
      DELETE FROM yellow_trips WHERE partition_date ='{month_to_fetch}' AND partition_type = 'yellow';
      
      INSERT INTO yellow_trips
      select
      VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
      tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date,
      'yellow' AS partition_type
      FROM '{constants.YELLOW_TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """
    preview_query = "SELECT * from yellow_trips limit 10"
# So now instead of establishing the connections via the getenv part, we can run it using the resources instead
    with database.get_connection() as conn:
      conn.execute(query)
      preview_df = conn.execute(preview_query).fetchdf()
      row_count = conn.execute("SELECT COUNT(*) from yellow_trips").fetchone()
      count = row_count[0] if row_count else 0
      
# We also want to be able to view the information from the table directly
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        }
    )
      
@dg.asset(
    deps=["green_taxi_trips_file"],
    partitions_def=monthly_partition,
    group_name="Local_Database",
    compute_kind="DuckDB",
)
def green_taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> dg.MaterializeResult:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    
    # So we will load in information into the duckdb by month  
    # We will need to create the table if it doesnt exist and then load in the information
    query = f"""
      CREATE TABLE IF NOT EXISTS green_trips
      (
      VendorID integer, lpep_pickup_datetime timestamp, lpep_dropoff_datetime timestamp,
      store_and_fwd_flag varchar, RatecodeID integer, PULocationID integer,
      DOLocationID int, passenger_count double, trip_distance double,
      fare_amount double, extra float, mta_tax float,
      tip_amount float, tolls_amount float, ehail_fee float, improvement_surcharge float,
      total_amount double,payment_type int, trip_type int, congestion_surcharge float,
      partition_date varchar, partition_type varchar
      );
      
      DELETE FROM green_trips WHERE partition_date ='{month_to_fetch}' AND partition_type = 'green';
      
      INSERT INTO green_trips
      select
      VendorID, lpep_pickup_datetime, lpep_dropoff_datetime,
      store_and_fwd_flag, RatecodeID, PULocationID,
      DOLocationID, passenger_count, trip_distance,
      fare_amount, extra, mta_tax,
      tip_amount, tolls_amount, ehail_fee, improvement_surcharge,
      total_amount,payment_type, trip_type, congestion_surcharge,
      '{month_to_fetch}' as partition_date,'green' AS partition_type
      FROM '{constants.GREEN_TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """
    preview_query = "SELECT * from green_trips limit 10"
# So now instead of establishing the connections via the getenv part, we can run it using the resources instead
    with database.get_connection() as conn:
      conn.execute(query)
      preview_df = conn.execute(preview_query).fetchdf()
      row_count = conn.execute("SELECT COUNT(*) from green_trips").fetchone()
      count = row_count[0] if row_count else 0
      
# We also want to be able to view the information from the table directly
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(count),
            "preview": dg.MetadataValue.md(preview_df.to_markdown(index=False)),
        }
    )
    
# Pushing the data into ADLS2
@dg.asset(
  deps=["green_taxi_trips_file"],
  partitions_def = monthly_partition,
  group_name="Cloud_Ingestion",
  compute_kind="Azure",
  )
def green_taxi_trips_cloud(context: dg.AssetExecutionContext, adls2: ADLS2Resource) -> None:
    """
      The raw parquet files for the green taxi trips dataset.
    """
    # Choose the month to fetch from the URL
    # This is to allow backfilling
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    # Using the env to load the credentials to upload data to the ADLS2 
    file_client = adls2.adls2_client.get_file_client(
        file_system='data',
        file_path=f'input_data/green_tripdata_{month_to_fetch}.parquet'
    )
    # We load directly since we already have the local files in the first place
    # Makes no sense to do a download and then push to cloud
    with open(constants.GREEN_TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), 'rb') as file_data:
      file_client.upload_data(file_data, overwrite=True)
      
# Ingest data from ADLS2 and allow checking for metadata to ensure we have the information
@dg.asset(
  partitions_def = monthly_partition,
  group_name="Cloud_Read",
  compute_kind="Azure",
  )
def green_taxi_trips_cloud_read(context: dg.AssetExecutionContext, adls2: ADLS2Resource) -> dg.MaterializeResult:
    """
      Read the parquet files for the green taxi trips dataset directly from adls2.
    """
    # Choose the month to fetch from the URL
    # This is to allow backfilling
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    # Using the env to load the credentials to upload data to the ADLS2 
    file_client = adls2.adls2_client.get_file_client(
        file_system='data',
        file_path=f'input_data/green_tripdata_{month_to_fetch}.parquet'
    )
    
    # So now we need to read the data directly from ADLS2
    downloaded_data = file_client.download_file()
    file_content = downloaded_data.readall()
    num_rows = len(pd.read_parquet(io.BytesIO(file_content)))
    
    return dg.MaterializeResult(
      metadata={
                'Number of records': dg.MetadataValue.int(num_rows)
            }
        )
  
# I want to see if we can do some delta table stuff in adls2 directly from here
# But the first step is to push a new delta table first
# So we can locally process the parquet file into the delta table
# In this case maybe we try using the sql to pull the data from the duckdb into a pyspark dataframe
# convert it to a delta table and then push the delta table into the datalake

@dg.asset(
    partitions_def=monthly_partition,
    group_name="Local_Database",
    compute_kind="deltalake",
)
def green_taxi_trips_delta_table(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
      The raw taxi trips dataset taken from a DuckDB database and loaded into a delta table

    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    
    # Pull the query first
    query = f"""      
      SELECT
      *
      FROM green_trips
      WHERE partition_date ='{month_to_fetch}'
      LIMIT 10
      ;
    """
    # Writing via spark abit tricky
    # Write to delta lake using native api
    # Can slowly modify to allow for spark based approach
    with database.get_connection() as conn:
      delta_df = conn.execute(query).df()
      write_deltalake(f"data/staging/green_taxi_delta_table", delta_df,  partition_by=["partition_date"])
    
    # # 1. Initialize Spark with Delta support
    # spark = SparkSession.builder \
    #     .appName("DuckDBToDelta") \
    #     .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    #     .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    #     .getOrCreate()
        
    # with database.get_connection() as conn:
    #     arrow_batches = conn.execute(query) # Returns Arrow batches
    #     spark_df = spark.createDataFrame(arrow_batches)  # Arrow → Spark (optimized)
    
    # delta_path = "data/staging/green_taxi_delta_table"
    # spark_df.write.format("delta").mode("overwrite").save(delta_path)

# Now we want to be able to do some analysis of the data
# We will check what is the spread of the fare amounts
