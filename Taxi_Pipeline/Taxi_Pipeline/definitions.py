import dagster as dg

from dagster import Definitions, load_assets_from_modules
from Taxi_Pipeline.resources import database_resource, adls2_token_resource
from Taxi_Pipeline.assets import trips
from Taxi_Pipeline.jobs import trip_update_job
from Taxi_Pipeline.schedule import trip_update_schedule 


trip_assets = dg.load_assets_from_modules([trips])
all_jobs = [trip_update_job]
all_schedules = [trip_update_schedule]

defs = dg.Definitions(
    assets=trip_assets,
    resources={
    "database": database_resource,
    "adls2": adls2_token_resource
    },
    jobs=all_jobs,
    schedules=all_schedules,
)
