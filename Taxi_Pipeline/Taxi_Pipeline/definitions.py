import dagster as dg
from dagster import Definitions, load_assets_from_modules

from data_engineering.Taxi_Pipeline.Taxi_Pipeline.assets import trips 

trip_assets = dg.load_assets_from_modules([trips])

defs = Definitions(
    assets=trip_assets,
    group_name="Raw Loading"
)
