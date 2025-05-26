import dagster as dg
from Taxi_Pipeline.assets import constants

start_date = constants.START_DATE
end_date = constants.END_DATE

# Here we create a monthly partition
monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)
# Here we create a weekly partition
weekly_partition = dg.WeeklyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)
