"""Transform raw Green Taxi trip data into daily revenue outputs."""

import json
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

# Running this file directly (e.g. VS Code's Run button) gives Python no package
# context, so the relative imports below would fail. Point Python at src/ and
# say which package this file belongs to.
if __name__ == "__main__" and not __package__:
    sys.path[0] = str(Path(__file__).resolve().parents[1])
    __package__ = "data_pipeline"

from .config import DEFAULT_MONTHS, PROCESSED_DIR

PICKUP_DATETIME_COLUMN = "lpep_pickup_datetime"
FALLBACK_REVENUE_COLUMNS = (
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "ehail_fee",
)


@dataclass(frozen=True)
class PipelineOutputs:
    """Paths written by one successful pipeline run."""

    daily_revenue_csv: Path
    daily_revenue_parquet: Path
    metadata_json: Path


def _build_revenue_series(frame: pd.DataFrame) -> pd.Series:
    """Return one revenue value per trip from the columns available in the data."""
    if "total_amount" in frame.columns:
        return pd.to_numeric(frame["total_amount"], errors="coerce")

    # Some taxi datasets expose fare components instead of total_amount.
    # Summing the components keeps the pipeline useful for those file variants.
    component_columns = [
        column for column in FALLBACK_REVENUE_COLUMNS if column in frame.columns
    ]
    if not component_columns:
        raise ValueError(
            "Could not determine a revenue column. Expected 'total_amount' or known fare components."
        )

    components = frame[component_columns].apply(pd.to_numeric, errors="coerce")
    # An entirely missing fare is unknown, not a zero-revenue trip.
    return components.sum(axis=1, min_count=1)


def prepare_trip_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep only valid pickup dates and revenue values needed for aggregation."""
    if PICKUP_DATETIME_COLUMN not in frame.columns:
        raise ValueError(f"Missing expected column: {PICKUP_DATETIME_COLUMN}")

    prepared = frame.copy()
    prepared[PICKUP_DATETIME_COLUMN] = pd.to_datetime(
        prepared[PICKUP_DATETIME_COLUMN],
        errors="coerce",
    )
    prepared = prepared.dropna(subset=[PICKUP_DATETIME_COLUMN])

    # Normalizing timestamps to midnight lets us group all trips by calendar day.
    prepared["service_date"] = prepared[PICKUP_DATETIME_COLUMN].dt.normalize()
    prepared["revenue_amount"] = _build_revenue_series(prepared)
    prepared = prepared.dropna(subset=["revenue_amount"])

    return prepared[["service_date", "revenue_amount"]]


def keep_selected_months(
    prepared: pd.DataFrame, months: Iterable[str]
) -> tuple[pd.DataFrame, int]:
    """Drop trips whose pickup date falls outside the requested months.

    Each monthly file holds a few trips from neighbouring months (e.g. late
    December 2024 in the January file), which would otherwise add extra days to
    the output. Returns the kept trips and how many were dropped.
    """
    inside = prepared["service_date"].dt.strftime("%Y-%m").isin(set(months))
    return prepared[inside], int((~inside).sum())


def calculate_daily_revenue(
    input_paths: Iterable[Path],
    months: Iterable[str] = DEFAULT_MONTHS,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Read Parquet files and calculate daily trip counts plus daily revenue.

    Only trips picked up within ``months`` (e.g. "2025-01") are counted.
    """
    per_file_frames: list[pd.DataFrame] = []
    input_path_list = [Path(path) for path in input_paths]
    month_list = sorted(set(months))

    if not input_path_list:
        raise ValueError("No input Parquet files were provided.")
    if not month_list:
        raise ValueError("No months were provided.")

    metadata_files = []
    total_rows_read = 0
    total_rows_outside_months = 0

    for input_path in input_path_list:
        trip_frame = pd.read_parquet(input_path)
        total_rows_read += len(trip_frame)

        prepared, rows_outside_months = keep_selected_months(
            prepare_trip_frame(trip_frame), month_list
        )
        total_rows_outside_months += rows_outside_months
        daily_summary = prepared.groupby("service_date", as_index=False).agg(
            trip_count=("revenue_amount", "size"),
            daily_revenue=("revenue_amount", "sum"),
        )
        per_file_frames.append(daily_summary)

        metadata_files.append(
            {
                "file_name": input_path.name,
                "rows_read": len(trip_frame),
                "rows_outside_months": rows_outside_months,
                "rows_used": len(prepared),
                "revenue_total": float(round(prepared["revenue_amount"].sum(), 2)),
            }
        )

    # Each file is summarized first, then the summaries are combined in case
    # multiple files contain trips for the same service date.
    daily_revenue = (
        pd.concat(per_file_frames, ignore_index=True)
        .groupby("service_date", as_index=False)
        .agg(
            trip_count=("trip_count", "sum"),
            daily_revenue=("daily_revenue", "sum"),
        )
        .sort_values("service_date")
    )
    daily_revenue["daily_revenue"] = daily_revenue["daily_revenue"].round(2)
    daily_revenue["service_date"] = daily_revenue["service_date"].dt.strftime(
        "%Y-%m-%d"
    )

    metadata: dict[str, object] = {
        "months": month_list,
        "source_files": metadata_files,
        "days_in_output": len(daily_revenue),
        "rows_read": int(total_rows_read),
        "rows_outside_months": total_rows_outside_months,
        "trips_in_output": int(daily_revenue["trip_count"].sum()),
        "revenue_total": float(round(daily_revenue["daily_revenue"].sum(), 2)),
    }
    return daily_revenue, metadata


def write_outputs(
    daily_revenue: pd.DataFrame,
    metadata: dict[str, object],
    output_dir: Path = PROCESSED_DIR,
) -> PipelineOutputs:
    """Write the final DataFrame and metadata to the processed data directory."""
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "daily_revenue.csv"
    parquet_path = output_dir / "daily_revenue.parquet"
    metadata_path = output_dir / "pipeline_metadata.json"

    daily_revenue.to_csv(csv_path, index=False)
    daily_revenue.to_parquet(parquet_path, index=False)
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    return PipelineOutputs(
        daily_revenue_csv=csv_path,
        daily_revenue_parquet=parquet_path,
        metadata_json=metadata_path,
    )


def run_pipeline(
    input_paths: Iterable[Path],
    output_dir: Path = PROCESSED_DIR,
    months: Iterable[str] = DEFAULT_MONTHS,
) -> tuple[PipelineOutputs, dict[str, object]]:
    """Convenience function used by the CLI and Prefect flow."""
    daily_revenue, metadata = calculate_daily_revenue(input_paths, months)
    outputs = write_outputs(daily_revenue, metadata, output_dir)
    return outputs, metadata


if __name__ == "__main__":
    # Same as `scripts/run_pipeline.py` without the download step.
    from .cli import main

    raise SystemExit(main(["run", *sys.argv[1:]]))
