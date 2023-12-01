
import requests
from pathlib import Path
from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition, MaterializeResult, define_asset_job, AssetSelection

# V1:

# @asset
# def file_orders():
#     url = "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"
#     data = requests.get(url)
#     folder = Path("data/raw_orders")
#     folder.mkdir(parents=True, exist_ok=True)
#     path = folder / f"raw_orders.csv"
#     path.write_text(data.text)


# V2:
@asset
def file_orders():
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"
    data = requests.get(url)
    folder = Path("data/raw_orders")
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"raw_orders.csv"
    path.write_text(data.text)


@asset(deps=[file_orders])
def orders():
    file_orders = Path(f"data/raw_orders/raw_orders.csv")
    num_lines = len(file_orders.read_text().splitlines())
    return MaterializeResult(metadata={"num_lines": num_lines})


# V3:

daily_partition_def = DailyPartitionsDefinition(start_date="2023-11-28")

@asset(partitions_def=daily_partition_def)
def file_orders_partitioned(context: AssetExecutionContext):
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"
    data = requests.get(url)
    folder = Path("data/raw_orders")
    folder.mkdir(parents=True, exist_ok=True)

    partition_date_str = context.asset_partition_key_for_output()
    path = folder / f"{partition_date_str}.csv"
    path.write_text(data.text)
    # return MaterializeResult(metadata={"partition_date": partition_date_str, "file": path.absolute()})
    # return MaterializeResult(metadata={"file": path.absolute()})


@asset(partitions_def=daily_partition_def, deps=[file_orders_partitioned])
def orders_partitioned(context: AssetExecutionContext):
    partition_date_str = context.asset_partition_key_for_output()
    file_orders = Path(f"data/raw_orders/{partition_date_str}.csv")
    num_lines = len(file_orders.read_text().splitlines())
    # return MaterializeResult(metadata={"partition_date": partition_date_str, "num_lines": num_lines})
    return MaterializeResult(metadata={"num_lines": num_lines})


# V4:

partitioned_asset_job = define_asset_job(
    name="file_orders_and_orders_partitioned_job",
    selection=AssetSelection.assets(file_orders_partitioned, orders_partitioned),
    partitions_def=daily_partition_def,
)

asset_job = define_asset_job(
    name="file_orders_and_orders_job",
    selection=AssetSelection.assets(file_orders, orders),
)
