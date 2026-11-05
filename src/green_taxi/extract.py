"""Extract: download the monthly Parquet files into the staging directory."""

import logging
from collections.abc import Iterable
from pathlib import Path

import requests

from .config import BASE_URL, DOWNLOAD_TIMEOUT_SECONDS, MONTHS, RAW_DIR, file_name, file_url

log = logging.getLogger(__name__)


def download_month(
    month: str,
    raw_dir: Path = RAW_DIR,
    *,
    base_url: str = BASE_URL,
    force: bool = False,
) -> Path:
    """Download one month and return its local path.

    Re-running is safe: a file that is already staged is skipped unless
    force=True. The download goes to a .part file first and is only renamed
    once complete, so an interrupted run never leaves a half-written file
    that a later run would mistake for a finished one.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    destination = raw_dir / file_name(month)

    if destination.exists() and not force:
        log.info("skip %s (already staged)", destination.name)
        return destination

    url = file_url(month, base_url)
    partial = destination.with_suffix(destination.suffix + ".part")
    log.info("download %s", url)
    with requests.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
        response.raise_for_status()
        with partial.open("wb") as out:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                out.write(chunk)
    partial.replace(destination)
    return destination


def extract(
    months: Iterable[str] = MONTHS,
    raw_dir: Path = RAW_DIR,
    *,
    force: bool = False,
) -> list[Path]:
    """Download every requested month; return the staged file paths."""
    return [download_month(m, raw_dir, force=force) for m in months]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    for path in extract():
        print(path)
