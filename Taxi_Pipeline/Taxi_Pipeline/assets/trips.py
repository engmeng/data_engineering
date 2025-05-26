import requests
from Taxi_Pipeline.assets import constants
import dagster as dg
import duckdb
import os
import pandas as pd

@dg.asset(
  group_name="raw_files"
  )
def taxi_trips_file(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    # Choose the month to fetch from the URL
    month_to_fetch = '2023-03'
    #partition_date_str = context.partition_key
    #month_to_fetch = partition_date_str[:-3]
    
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