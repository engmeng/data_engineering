from dagster import Definitions, load_assets_from_modules

from data_engineering.Taxi_Pipeline.Taxi_Pipeline.assets import assets 

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
)
