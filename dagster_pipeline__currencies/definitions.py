from dagster import Definitions, load_assets_from_modules, AssetSelection
from dagster_pipeline__currencies.assets import test_for_nulls_in_currencies_df
from dagster_pipeline__currencies import assets  # noqa: TID252
from dagster import define_asset_job, ScheduleDefinition


all_assets = load_assets_from_modules([assets])

currency_api_refresh_job = define_asset_job(
    "per_minute_refresh", selection=AssetSelection.assets(*all_assets),
)

hourly_schedule = ScheduleDefinition(
    job=currency_api_refresh_job,
    cron_schedule="* * * * *", # on the minute, every minute, every hour, every day
)


defs = Definitions(
    assets=all_assets,
    asset_checks=[test_for_nulls_in_stg_source__crypto_rates],
    jobs=[currency_api_refresh_job],
    schedules=[hourly_schedule]
)


