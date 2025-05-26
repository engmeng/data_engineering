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
def taxi_trips_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
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
    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)
        
    num_rows = len(pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))

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
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition,
    group_name="Local_Database",
    compute_kind="DuckDB",
)
def taxi_trips(context: dg.AssetExecutionContext, database: DuckDBResource) -> None:
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    
    # So we will load in information into the duckdb by month  
    # We will need to create the table if it doesnt exist and then load in the information
    
    query = f"""
      CREATE TABLE IF NOT EXISTS trips
      (
      vendor_id integer, pickup_zone_id integer, dropoff_zone_id integer,
      rate_code_id double, payment_type integer, dropoff_datetime timestamp,
      pickup_datetime timestamp, trip_distance double, passenger_count double,
      total_amount double, partition_date varchar
      );
      
      DELETE FROM trips WHERE partition_date ='{month_to_fetch}';
      
      INSERT INTO trips
      select
      VendorID, PULocationID, DOLocationID, RatecodeID, payment_type, tpep_dropoff_datetime,
      tpep_pickup_datetime, trip_distance, passenger_count, total_amount, '{month_to_fetch}' as partition_date
      FROM '{constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)}';
    """

# So now instead of establishing the connections via the getenv part, we can run it using the resources instead
    with database.get_connection() as conn:
      conn.execute(query)