"""Entry point: run extract -> transform -> load in order.

    uv run green-taxi                    # download if needed, then process
    uv run green-taxi --force-download   # download every file again
"""

import json
import logging

import click

from .config import MONTHS
from .extract import extract
from .load import load
from .transform import transform


def run(force_download: bool = False) -> list:
    paths = extract(MONTHS, force=force_download)
    daily, report = transform(paths, MONTHS)
    return load(daily, report)


@click.command()
@click.option("--force-download", is_flag=True, help="Download files even if already staged.")
def main(force_download: bool) -> None:
    """Build daily revenue for Green Taxi trips, Jan-Mar 2025."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    outputs = run(force_download)
    for path in outputs:
        click.echo(f"wrote {path}")
    click.echo(json.dumps(json.loads(outputs[-1].read_text()), indent=2))


if __name__ == "__main__":
    main()
