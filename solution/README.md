# Reference Solution

This folder contains a complete local-first reference implementation for an **ETL pipeline**.

## What the solution does

1. Downloads the January, February and March 2025 Green Taxi Parquet files into `data/raw/`.
2. Reads those Parquet files locally.
3. Drops trips whose pickup date falls outside the requested months. Each monthly file holds a few stray trips from neighbouring months, such as late December 2024 in the January file. The number dropped is recorded as `rows_outside_months` in the metadata.
4. Aggregates `total_amount` by pickup date to produce daily revenue.
5. Writes outputs to `data/processed/` as CSV, Parquet, and JSON metadata.
6. Includes an optional Prefect flow for orchestration.

The load step writes the output files into `data/processed/`, not into a database. This means there is no server to start and no connection to configure, so the focus stays on the pipeline stages themselves. Note that `data/raw/` is a staging area, not the destination.

If you want to extend the exercise and write to a database instead of the local filesystem, swap the file writes in [src/data_pipeline/transform.py](src/data_pipeline/transform.py) for a database write. The extract and transform stages stay exactly as they are.

## Project structure

| File | Description |
| --- | --- |
| [scripts/run_pipeline.py](scripts/run_pipeline.py) | Runs the full pipeline: download, transform, and write. |
| [scripts/download_data.py](scripts/download_data.py) | Downloads the raw files without running the transformation. |
| [scripts/run_prefect_flow.py](scripts/run_prefect_flow.py) | Runs the optional Prefect version of the same pipeline. |
| [src/data_pipeline/cli.py](src/data_pipeline/cli.py) | Exposes the `download`, `run` and `all` commands. |
| [src/data_pipeline/config.py](src/data_pipeline/config.py) | Holds the months, dataset URL and data directory paths. |
| [src/data_pipeline/download.py](src/data_pipeline/download.py) | Handles dataset downloads. |
| [src/data_pipeline/transform.py](src/data_pipeline/transform.py) | Calculates daily revenue and writes outputs. |
| [src/data_pipeline/prefect_flow.py](src/data_pipeline/prefect_flow.py) | Contains the optional Prefect flow. |
| [tests/test_transform.py](tests/test_transform.py) | Covers the revenue aggregation logic and the month filter. |
| [tests/test_entrypoints.py](tests/test_entrypoints.py) | Checks that the package modules can be run directly as files. |

## Run the reference solution

Run these commands from the project root, after following the Setup section in the [main README](../README.md):

```bash
uv run python solution/scripts/run_pipeline.py
uv run pytest solution/tests
```

The whole repository shares a single environment defined by the [pyproject.toml](../pyproject.toml) in the project root. `uv run` uses the exact versions locked in [uv.lock](../uv.lock) and installs them on first use, so there is no additional virtual environment to create or activate.

Outputs land in `solution/data/`, because the solution resolves its paths relative to its own source files rather than to the current directory.

With the default months (January to March 2025), the summary line reads `days=90, trips=146475, revenue_total=3395290.56`, after 11 out-of-range trips are dropped.

You can also run the modules in `src/data_pipeline/` directly, for example with VS Code's Run button. Running `cli.py` shows the `download`, `run` and `all` commands; `download.py` and `transform.py` run their own step, and `prefect_flow.py` runs the flow. Each module adds `src/` to the import path itself when started this way, because a plain `python some_module.py` would otherwise fail with `ImportError: attempted relative import with no known parent package`.

## Optional Prefect flow

Prefect is part of the project environment, so the orchestration version of the same local pipeline is ready to run.

This reference solution is meant to let Prefect start a temporary local server for the run.
If you previously ran another Prefect project, an old background server or a saved `PREFECT_API_URL` can cause errors.

Use the cleanup commands below before running the flow.
If Prefect says no server is running or the setting is not set, you can continue.

`macOS` / `Linux` / `Windows Git-Bash`:

```bash
uv run prefect server stop
uv run prefect config set PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true
uv run prefect config unset PREFECT_API_URL --yes
unset PREFECT_API_URL
uv run python solution/scripts/run_prefect_flow.py
```

`Windows PowerShell`:

```powershell
uv run prefect server stop
uv run prefect config set PREFECT_SERVER_ALLOW_EPHEMERAL_MODE=true
uv run prefect config unset PREFECT_API_URL --yes
Remove-Item Env:PREFECT_API_URL -ErrorAction SilentlyContinue
uv run python solution/scripts/run_prefect_flow.py
```

The helper scripts add the local `src/` directory to `PYTHONPATH` automatically, so you do not need an editable package install.

When you are done with the optional flow, you can stop any background Prefect server with `uv run prefect server stop`.

If you intentionally want to use a dedicated Prefect server instead of the temporary server, start it first with `uv run prefect server start --background`, check it with `uv run prefect server status`, and then run the flow.

### Where the flow runs and how to see it

By default the flow runs on your machine, on a temporary Prefect server that starts on a random port (for example `Starting temporary server on http://127.0.0.1:8852`) and stops when the run ends, so there is no dashboard to open during the run. Every run is still recorded in Prefect's local database at `~/.prefect/prefect.db`.

To browse the runs, start a server in one terminal and leave it running:

```bash
uv run prefect server start
```

Then open <http://127.0.0.1:4200> and go to **Runs**. Earlier runs of the `green-taxi-local-pipeline` flow are listed there too, because the server reads the same database.

To watch a new run live, point the flow at that server from a second terminal. The log then prints a `View at http://127.0.0.1:4200/runs/flow-run/...` link for the run:

```bash
PREFECT_API_URL=http://127.0.0.1:4200/api uv run python solution/scripts/run_prefect_flow.py
```

Stop the server with `Ctrl+C` in its terminal when you are done.

## Expected outputs

After a successful run, you should see:

- `data/raw/green_tripdata_2025-01.parquet`
- `data/raw/green_tripdata_2025-02.parquet`
- `data/raw/green_tripdata_2025-03.parquet`
- `data/processed/daily_revenue.csv`
- `data/processed/daily_revenue.parquet`
- `data/processed/pipeline_metadata.json`
