import dagster as dg
from Taxi_Pipeline.partitions import monthly_partition

trip_update_job = dg.define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition,
    selection=dg.AssetSelection.all() 
)