"""Prefect version of the pipeline: same stages, as tasks inside one flow.

    uv run python -m green_taxi.flow
"""

from pathlib import Path

from prefect import flow, task

from .config import MONTHS
from .extract import download_month
from .load import load
from .transform import transform


# Network downloads are the step most likely to fail, so they get retries.
@task(retries=3, retry_delay_seconds=[10, 30, 60])
def extract_month(month: str, force: bool = False) -> Path:
    return download_month(month, force=force)


@task
def transform_task(paths: list[Path]):
    return transform(paths, MONTHS)


@task(retries=1, retry_delay_seconds=5)
def load_task(daily, report) -> list[Path]:
    return load(daily, report)


@flow(name="green-taxi-daily-revenue", log_prints=True)
def green_taxi_flow(force_download: bool = False) -> list[Path]:
    paths = [extract_month(m, force_download) for m in MONTHS]
    daily, report = transform_task(paths)
    outputs = load_task(daily, report)
    print(f"{report.rows_read} rows read, {report.rows_kept} kept; removed: {report.removed}")
    print(f"total revenue ${daily['revenue'].sum():,.2f} over {len(daily)} days")
    return outputs


if __name__ == "__main__":
    green_taxi_flow()
