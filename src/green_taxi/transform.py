"""Transform: clean the staged trips and add up revenue per day.

This is an ETL pipeline: the cleaning and aggregation happen here in Python,
before anything is written. Every cleaning rule comes from the data checks in
notebooks/01-explore-green-taxi.ipynb and records how many rows it removed.

Revenue is `total_amount`: what the passenger was charged, including tips,
tolls, taxes and surcharges. A day is the calendar date of the pickup.
"""

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from .config import MONTHS

PICKUP = "lpep_pickup_datetime"
AMOUNT = "total_amount"
# Columns that identify one trip. A refund repeats these with a negative amount.
TRIP_KEY = ["VendorID", PICKUP, "lpep_dropoff_datetime", "PULocationID", "DOLocationID"]
COLUMNS = list(dict.fromkeys([*TRIP_KEY, AMOUNT]))


@dataclass
class CleaningReport:
    """Row counts at each step, so the effect of every rule is visible."""

    rows_read: int = 0
    removed: dict[str, int] = field(default_factory=dict)
    rows_kept: int = 0

    def drop(self, rule: str, count: int) -> None:
        self.removed[rule] = self.removed.get(rule, 0) + int(count)


def read_staged(paths: Iterable[Path]) -> pd.DataFrame:
    """Read the staged Parquet files into one frame (only the needed columns)."""
    paths = list(paths)
    if not paths:
        raise ValueError("no staged files to read")
    return pd.concat([pd.read_parquet(p, columns=COLUMNS) for p in paths], ignore_index=True)


def reporting_window(months: Iterable[str] = MONTHS) -> tuple[pd.Timestamp, pd.Timestamp]:
    """[start, end) of the requested months, e.g. 2025-01-01 to 2025-04-01."""
    starts = sorted(pd.Timestamp(f"{m}-01") for m in months)
    return starts[0], starts[-1] + pd.offsets.MonthBegin(1)


def remove_reversals(trips: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop each refund (negative amount) together with the charge it reverses.

    A refund has the same trip key as the original charge and the opposite
    amount. Pairs are matched one-to-one, so if a trip was charged twice and
    refunded once, one charge stays. Refunds without a matching charge are
    kept: they still reduce that day's revenue.
    """
    match_on = [*TRIP_KEY, "_abs"]
    t = trips.assign(_abs=trips[AMOUNT].abs().round(2))
    neg = t[t[AMOUNT] < 0]
    pos = t[t[AMOUNT] > 0]
    # Number each occurrence within a key so the n-th refund pairs with the n-th charge.
    neg = neg.assign(_n=neg.groupby(match_on, dropna=False).cumcount())
    pos = pos.assign(_n=pos.groupby(match_on, dropna=False).cumcount())
    pairs = neg.reset_index().merge(
        pos.reset_index(), on=[*match_on, "_n"], suffixes=("_neg", "_pos")
    )
    drop = pd.Index(pairs["index_neg"]).append(pd.Index(pairs["index_pos"]))
    return trips.drop(index=drop), len(pairs)


def clean(
    trips: pd.DataFrame, months: Iterable[str] = MONTHS
) -> tuple[pd.DataFrame, CleaningReport]:
    """Apply the cleaning rules; return the clean trips and a row-count report."""
    report = CleaningReport(rows_read=len(trips))

    missing = trips[PICKUP].isna() | trips[AMOUNT].isna()
    report.drop("missing pickup time or amount", missing.sum())
    trips = trips[~missing]

    # Files contain a few trips from neighbouring months (e.g. Dec 2024, Apr 2025).
    start, end = reporting_window(months)
    outside = (trips[PICKUP] < start) | (trips[PICKUP] >= end)
    report.drop("pickup outside reporting window", outside.sum())
    trips = trips[~outside]

    trips, n_pairs = remove_reversals(trips)
    report.drop("refunded trips (charge + refund pairs)", 2 * n_pairs)

    report.rows_kept = len(trips)
    return trips, report


def daily_revenue(trips: pd.DataFrame) -> pd.DataFrame:
    """Group clean trips by pickup date: paid trips and revenue per day."""
    by_day = trips.assign(date=trips[PICKUP].dt.date).groupby("date")[AMOUNT]
    result = pd.DataFrame(
        {
            # Paid trips only: $0 trips and stand-alone refunds add no trip.
            "trips": by_day.apply(lambda s: int((s > 0).sum())),
            "revenue": by_day.sum().round(2),
        }
    )
    result.index = pd.to_datetime(result.index)
    result.index.name = "date"
    return result.reset_index()


def transform(
    paths: Iterable[Path], months: Iterable[str] = MONTHS
) -> tuple[pd.DataFrame, CleaningReport]:
    """Read -> clean -> aggregate."""
    trips, report = clean(read_staged(paths), months)
    return daily_revenue(trips), report
