import requests
from pathlib import Path
from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, define_asset_job, AssetSelection


@asset
def file_orders():
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"
    data = requests.get(url)
    folder = Path("data/raw_orders")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"raw_orders.csv"
    path.write_text(data.text)
