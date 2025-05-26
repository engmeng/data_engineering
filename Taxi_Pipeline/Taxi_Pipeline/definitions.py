import dagster as dg

from dagster import Definitions, load_assets_from_modules
from Taxi_Pipeline.resources import database_resource
from Taxi_Pipeline.assets import trips 

trip_assets = dg.load_assets_from_modules([trips])

defs = Definitions(
    assets=trip_assets,
    resources={
    "database": database_resource,
    },
)
