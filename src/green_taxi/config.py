"""Settings shared by every stage of the pipeline."""

from pathlib import Path

# What to download.
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
FILE_TEMPLATE = "green_tripdata_{month}.parquet"
MONTHS = ("2025-01", "2025-02", "2025-03")
DOWNLOAD_TIMEOUT_SECONDS = 60

# Where things live. Paths are resolved from this file, so the pipeline
# works no matter which directory you run it from.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def file_name(month: str) -> str:
    """File name for one month, e.g. green_tripdata_2025-01.parquet."""
    return FILE_TEMPLATE.format(month=month)


def file_url(month: str, base_url: str = BASE_URL) -> str:
    """Download link for one month."""
    return f"{base_url.rstrip('/')}/{file_name(month)}"
