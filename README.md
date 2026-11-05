# Green Taxi Daily Revenue Pipeline

An ETL pipeline that downloads the NYC **Green Taxi** trip records for January–March 2025, cleans them, and calculates **revenue per day**. It runs as a plain Python command or as a Prefect flow.

Data source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

## Result

| Month | Paid trips | Revenue | Average per day |
|---|---:|---:|---:|
| Jan 2025 | 47,956 | $1,092,994.80 | $35,257.90 |
| Feb 2025 | 46,242 | $1,067,222.12 | $38,115.08 |
| Mar 2025 | 51,077 | $1,235,073.64 | $39,841.09 |
| **Total** | **145,275** | **$3,395,290.56** | 90 days |

The busiest day was Thu 2025-03-13 ($48,362.43) and the quietest was Sun 2025-01-19 ($23,401.09).

## How to run

Requires [uv](https://docs.astral.sh/uv/) 0.13.5 or newer. Python 3.13 is installed by uv if needed.

```bash
uv sync                          # install dependencies into .venv/

uv run green-taxi                # extract -> transform -> load
uv run green-taxi --force-download   # download the files again first

uv run pytest                    # run the tests
```

Download only:

```bash
uv run python -m green_taxi.extract
```

Prefect version (bonus). Prefect starts a temporary local server for the run:

```bash
uv run python -m green_taxi.flow
```

If Prefect complains about a server or `PREFECT_API_URL` from another project, run the cleanup steps in [solution/README.md](solution/README.md#optional-prefect-flow) first.

### Outputs

Everything is written to `data/processed/`:

| File | Contents |
|---|---|
| `daily_revenue.csv` / `daily_revenue.parquet` | One row per day: `date`, `trips` (paid trips), `revenue` |
| `run_report.json` | Rows read, rows removed per cleaning rule, rows kept, and totals |

## Project structure

```
data/
  raw/                 # staged downloads, never edited (git-ignored)
  processed/           # pipeline output (git-ignored)
notebooks/
  01-explore-green-taxi.ipynb   # data checks behind the cleaning rules (exploration only)
src/green_taxi/
  config.py            # URL, months, folders
  extract.py           # download the monthly Parquet files
  transform.py         # clean + group by day
  load.py              # write the output files and run report
  pipeline.py          # entry point: runs the three stages in order (`green-taxi`)
  flow.py              # Prefect flow: the same stages as tasks, with retries
tests/                 # transform rules and download re-run behaviour
solution/              # the course's reference solution (unchanged)
```

## Design choices

### Extract
- Each file's link is built from the host, the file name pattern and the month, all set in `config.py`.
- **Running it again is safe.** A file that is already staged is skipped, and `--force-download` fetches it again.
- Downloads go to a `.part` file that is renamed only once complete. An interrupted download can never be mistaken for a finished one.

### ETL, not ELT
The data is small: about 146k rows and 3.5 MB of Parquet for three months. The only target is a file. Cleaning in pandas before writing keeps everything in one language, with no database to run, and the cleaning rules can be unit-tested directly. ELT would make sense if the raw data had to be loaded into a warehouse anyway, and the transform could then be dbt models in SQL.

### What counts as revenue
- **Revenue** is `total_amount`: what the passenger was charged, including fare, tips, tolls, taxes and surcharges. It never has missing values, unlike `payment_type` and some of the surcharge columns.
- **A day** is the calendar date of the **pickup**.

### Cleaning rules
These come from the checks in the notebook. `run_report.json` records the row count for each rule:

| Finding | Rows | Rule |
|---|---:|---|
| Missing pickup time or amount | 0 | drop (kept as a safety check) |
| Pickup outside Jan–Mar 2025 (Dec 2024, Apr 2025) | 11 | drop |
| Refund (negative amount) plus the original charge it cancels | 936 (468 pairs) | drop both |
| Refund with no original charge in the data | 10 | keep: it still reduces revenue |
| `$0` trips | 254 | keep, but not counted as a paid trip |
| Dropoff earlier than pickup | 518 | keep: a clock error (vendor 6, a few minutes), and the amount is valid |

Rows: **146,486 read → 145,539 kept**. Of those, 145,275 are paid trips; the other 264 are the `$0` trips and the stand-alone refunds.

A refund repeats the original trip's vendor, pickup and dropoff times, and pickup and dropoff locations, with the opposite amount. Refunds are matched to charges one-to-one, so a trip charged twice and refunded once keeps one charge. Removing a pair does not change revenue, because the pair adds up to $0. What it does is keep voided trips out of the trip count.

## Review questions

### 1. What steps did you take to complete the project?
1. **Extract.** Listed what the download needs (host, file pattern, months, target folder), built the URLs, and wrote `extract.py`. It skips files that are already staged and writes through a temporary `.part` file.
2. **Explore.** Loaded all three months in a notebook. Checked types, missing values, date ranges and amount ranges for the two columns that matter, `lpep_pickup_datetime` and `total_amount`.
3. **Transform and load.** Turned each finding into a cleaning rule in `transform.py`, grouped by pickup date, and wrote CSV, Parquet and a report with row counts per rule. Compared the totals with the notebook's numbers.
4. **Test.** Wrote unit tests for each cleaning rule and for the download's re-run behaviour, using a fake HTTP response so no network is needed.
5. **Orchestrate.** Once the plain pipeline worked end to end, wrapped the stages as Prefect tasks in one flow, with retries on the downloads.

### 2. What challenges did you face?
- **Negative amounts.** At first they look like bad data to drop. Matching them against other trips showed that 468 of 478 are refunds of an earlier charge. Dropping only the negatives would have overstated revenue by about $6,460, so each refund is removed together with its charge.
- **Trips outside the month.** Each monthly file contains a few trips from other months, including Dec 2024 and Apr 2025. Filtering on the reporting window instead of on the file keeps the late January trips found in the February file.
- **Dropoff before pickup.** This looked like a reason to drop rows. Checking the size of the gap (minutes) and the vendor (almost all vendor 6) showed it is a clock problem that doesn't affect revenue.

### 3. What would you do differently with more time?
- Load the result into Postgres, alongside or instead of the files, and schedule the Prefect flow to pick up new months automatically.
- Add data-quality checks that fail the run when something changes, for example when the share of removed rows jumps or a day is missing. The current pipeline only reports these numbers.
- Make the months a command-line option instead of a constant.
- Add an integration test that runs the whole pipeline on a small Parquet fixture.
- Report fare-only revenue (`fare_amount`) next to total revenue, since tips and taxes are not really the operator's income.
