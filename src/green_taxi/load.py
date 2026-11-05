"""Load: write the daily revenue table and the run report to data/processed."""

import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from .config import PROCESSED_DIR
from .transform import CleaningReport


def load(
    daily: pd.DataFrame, report: CleaningReport, out_dir: Path = PROCESSED_DIR
) -> list[Path]:
    """Write daily_revenue.parquet, daily_revenue.csv and run_report.json."""
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet = out_dir / "daily_revenue.parquet"
    csv = out_dir / "daily_revenue.csv"
    report_path = out_dir / "run_report.json"

    daily.to_parquet(parquet, index=False)
    daily.to_csv(csv, index=False, date_format="%Y-%m-%d")
    summary = {
        **asdict(report),
        "days": len(daily),
        "first_day": daily["date"].min().strftime("%Y-%m-%d"),
        "last_day": daily["date"].max().strftime("%Y-%m-%d"),
        "paid_trips": int(daily["trips"].sum()),
        "total_revenue": round(float(daily["revenue"].sum()), 2),
    }
    report_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return [parquet, csv, report_path]
