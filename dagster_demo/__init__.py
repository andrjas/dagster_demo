from dagster import Definitions, load_assets_from_modules, ScheduleDefinition

from . import assets

all_assets = load_assets_from_modules([assets])

file_orders_and_orders_schedule = ScheduleDefinition(job=assets.asset_job, cron_schedule="0 0 * * *")
file_orders_and_orders_partitioned_schedule = ScheduleDefinition(job=assets.partitioned_asset_job, cron_schedule="0 0 * * *")

defs = Definitions(
    assets=all_assets,
    jobs=[assets.partitioned_asset_job],
    schedules=[file_orders_and_orders_schedule, file_orders_and_orders_partitioned_schedule],
)
