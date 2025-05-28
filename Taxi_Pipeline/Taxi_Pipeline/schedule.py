import dagster as dg
from Taxi_Pipeline.jobs import trip_update_job

trip_update_schedule = dg.ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="*/10 * * * *", # every 10 mins, just for testing
)