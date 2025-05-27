import requests
from Taxi_Pipeline.assets import constants
from Taxi_Pipeline.partitions import monthly_partition
import dagster as dg
import duckdb
import os
import pandas as pd
from dagster_duckdb import DuckDBResource

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
def yellow_taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
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

# So now instead of establishing the connections via the getenv part, we can run it using the resources instead
    with database.get_connection() as conn:
      conn.execute(query)
      
@dg.asset(
    deps=["green_taxi_trips_file"],
    partitions_def=monthly_partition,
    group_name="Local_Database",
    compute_kind="DuckDB",
)
def green_taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
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

# So now instead of establishing the connections via the getenv part, we can run it using the resources instead
    with database.get_connection() as conn:
      conn.execute(query)