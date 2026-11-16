# Green Taxi Daily Revenue: project notes

_Last updated: 15 Nov 2026_

## Project
**Green Taxi Daily Revenue.** How much revenue did NYC green taxis take per day in
January to March 2025? An ETL pipeline downloads the monthly trip files, cleans them,
and writes one revenue figure per pickup date. It runs as a plain command or as a Prefect flow.

- **Data:** NYC TLC Green Taxi trip records, Jan–Mar 2025 (3 monthly Parquet files,
  146,486 rows, about 3.5 MB). Revenue is `total_amount`; a day is the pickup date.
- **Stack:** Python 3.13, pandas, Click CLI (`green-taxi`), Prefect flow with retries on
  downloads, pytest. Outputs: CSV, Parquet and `run_report.json` in `data/processed/`.

## Approach
1. **Extract:** built the download URLs from `config.py`; `extract.py` skips files that
   are already staged and writes through a `.part` file that is renamed only when complete.
2. **Explore** (notebook 01): checked types, missing values, date ranges and amount ranges
   for `lpep_pickup_datetime` and `total_amount`.
3. **Transform and load:** turned each finding into a cleaning rule in `transform.py`
   (drop pickups outside the window, drop refund + charge pairs matched one to one, keep
   `$0` trips and dropoff-before-pickup rows), grouped by day, wrote CSV, Parquet and a report.
4. **Test:** unit tests for each cleaning rule and for the download's re-run behaviour,
   with a fake HTTP response (no network).
5. **Orchestrate:** wrapped the stages as Prefect tasks in one flow, with retries on the
   three downloads.
6. **Present:** results deck `presentation/green-taxi-daily-revenue.pptx`, later with the
   three project questions added.

## Results
| Check | Result |
|---|---|
| Rows (run_report.json) | **146,486 read → 145,539 kept**; 11 outside the window, 936 refund/charge rows (468 pairs) removed |
| Paid trips, Jan–Mar 2025 | **145,275** over 90 days |
| Total revenue | **$3,395,290.56** (Jan $1,092,994.80; Feb $1,067,222.12; Mar $1,235,073.64) |
| Average revenue per day | Jan $35,257.90; Feb $38,115.08; Mar $39,841.09 |
| Busiest / quietest day | Thu 2025-03-13 ($48,362.43) / Sun 2025-01-19 ($23,401.09) |
| Negative amounts | 468 of 478 matched to an original charge (refunds); dropping only negatives would overstate revenue by about $6,460 |
| Rows kept on purpose | 254 `$0` trips (not counted as paid); 518 dropoff-before-pickup rows (vendor clock error) |
| Test suite | **11 / 11 pass** (`uv run pytest -q`, run 15 Nov 2026) |

## Future engineering
- Load the daily revenue into Postgres and schedule the Prefect flow for new months.
- Data-quality checks that fail the run, e.g. when the share of removed rows jumps or a day is missing.
- Make the months a command-line option instead of a constant.
- Integration test that runs the whole pipeline on a small Parquet fixture.
- Report fare-only revenue (`fare_amount`) next to total revenue.

## Update log
- 3 Nov 2026: repository created from the course template.
- 5 Nov 2026: extract, exploration notebook, transform/load, Prefect flow and README
  answers merged (PR #1); results deck added (PR #3), presenter name (PR #4); month-filter
  fix and CLI improvements (PR #5).
- 10 Nov 2026: the three project questions added to the presentation.
- 15 Nov 2026: project notes created.
