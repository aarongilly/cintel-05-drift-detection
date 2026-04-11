"""
data_journal_drift_detector.py - Project script (example).

Author: Aaron Gillespie
Date: 2026-04

Reference and Current System Metrics Data

- Data is taken from a system measured during two different periods.
- The reference data represents earlier, expected system behavior.
- The current data represents more recent system behavior.
- Each row represents one observation of system metrics.

- Each CSV file includes these columns:
  - requests: number of requests handled
  - errors: number of failed requests
  - total_latency_ms: total response time in milliseconds

Purpose

- Read reference and current system metrics from CSV files.
- Compare the two datasets using simple summary statistics.
- Detect meaningful changes in average system behavior.
- Provide a simple baseline comparison approach that supports drift detection.
- Save the comparison summary as a CSV artifact.
- Log the pipeline process to assist with debugging and transparency.

Questions to Consider

- What does "normal" system behavior look like?
- How can we compare current measurements to a historical baseline?
- When does a difference become large enough to indicate meaningful change?

Paths (relative to repo root)

    INPUT FILE: data/reference_metrics_data_journal.csv
    INPUT FILE: data/current_metrics_data_journal.csv
    OUTPUT FILE: artifacts/drift_summary_data_journal.csv

Terminal command to run this file from the root project folder

    uv run python -m cintel.data_journal_drift_detector
"""

# === DECLARE IMPORTS ===

import logging
from pathlib import Path
from typing import Final

import polars as pl
from datafun_toolkit.logger import get_logger, log_header, log_path

# === CONFIGURE LOGGER ===

LOG: logging.Logger = get_logger("P5", level="DEBUG")

# === DEFINE GLOBAL PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts"

REFERENCE_FILE: Final[Path] = DATA_DIR / "reference_metrics_data_journal.csv"
CURRENT_FILE: Final[Path] = DATA_DIR / "current_metrics_data_journal.csv"

OUTPUT_FILE: Final[Path] = ARTIFACTS_DIR / "drift_summary_data_journal.csv"
SUMMARY_LONG_FILE: Final[Path] = ARTIFACTS_DIR / "drift_summary_long_data_journal.csv"

# === DEFINE THRESHOLDS ===

# Analysts need to know their data and
# choose thresholds that make sense for their specific use case.

# Review the reference metrics to understand typical values
# and variability before setting thresholds.

# In this example, we compare current metrics to a reference period
# and flag drift when the difference exceeds these thresholds:

SATISFACTION_DRIFT_THRESHOLD: Final[float] = 2.0
HEALTH_DRIFT_THRESHOLD: Final[float] = 2.0
SLEEPDURATION_DRIFT_THRESHOLD: Final[float] = 2.0
HR_DRIFT_THRESHOLD: Final[float] = 5.0
HRV_DRIFT_THRESHOLD: Final[float] = 5.0
READINESS_DRIFT_THRESHOLD: Final[float] = 5.0
ACTIVITY_DRIFT_THRESHOLD: Final[float] = 5.0
SLEEP_DRIFT_THRESHOLD: Final[float] = 2.0
ACTIVECALS_DRIFT_THRESHOLD: Final[float] = 100.0

# === DEFINE THE MAIN FUNCTION ===


def main() -> None:
    """Run the pipeline.

    log_header() logs a standard run header.
    log_path() logs repo-relative paths (privacy-safe).
    """
    log_header(LOG, "CINTEL")

    LOG.info("========================")
    LOG.info("START main()")
    LOG.info("========================")

    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "REFERENCE_FILE", REFERENCE_FILE)
    log_path(LOG, "CURRENT_FILE", CURRENT_FILE)
    log_path(LOG, "OUTPUT_FILE", OUTPUT_FILE)

    # Ensure the artifacts folder exists.
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path(LOG, "ARTIFACTS_DIR", ARTIFACTS_DIR)

    # ----------------------------------------------------
    # STEP 1: READ REFERENCE AND CURRENT CSV INTO DATAFRAMES
    # ----------------------------------------------------
    reference_df = pl.read_csv(REFERENCE_FILE)
    current_df = pl.read_csv(CURRENT_FILE)

    LOG.info(f"Loaded {reference_df.height} reference records")
    LOG.info(f"Loaded {current_df.height} current records")

    # ----------------------------------------------------
    # STEP 2: CALCULATE AVERAGE METRICS FOR EACH PERIOD
    # ----------------------------------------------------
    # Summarize each dataset using average values so we can
    # compare earlier system behavior to current system behavior.
    # We can create the summary tables by calculating the mean of each metric column
    # and providing a name for each new column that indicates
    # whether it is a reference or current value and what metric it holds.

    reference_summary_df = reference_df.select(
        [
            pl.col("satisfaction").mean().alias("reference_avg_satisfaction"),
            pl.col("health").mean().alias("reference_avg_health"),
            pl.col("hr").mean().alias("reference_avg_hr"),
            pl.col("hrv").mean().alias("reference_avg_hrv"),
            pl.col("readiness").mean().alias("reference_avg_readiness"),
            pl.col("activity").mean().alias("reference_avg_activity"),
            pl.col("sleep").mean().alias("reference_avg_sleep"),
            pl.col("sleepduration").mean().alias("reference_avg_sleepduration"),
            pl.col("activecals").mean().alias("reference_avg_activecals"),
        ]
    )

    current_summary_df = current_df.select(
        [
            pl.col("satisfaction").mean().alias("current_avg_satisfaction"),
            pl.col("health").mean().alias("current_avg_health"),
            pl.col("sleepduration").mean().alias("current_avg_sleepduration"),
            pl.col("hr").mean().alias("current_avg_hr"),
            pl.col("hrv").mean().alias("current_avg_hrv"),
            pl.col("readiness").mean().alias("current_avg_readiness"),
            pl.col("activity").mean().alias("current_avg_activity"),
            pl.col("sleep").mean().alias("current_avg_sleep"),
            pl.col("activecals").mean().alias("current_avg_activecals"),
        ]
    )

    # ----------------------------------------------------
    # STEP 3: COMBINE THE TWO ONE-ROW SUMMARY TABLES
    # ----------------------------------------------------
    # Each summary table has one row.
    #
    # reference_summary_df:
    #   reference_avg_satisfaction
    #   reference_avg_health
    #   reference_avg_sleepduration
    #   reference_avg_hr
    #   reference_avg_hrv
    #   reference_avg_readiness
    #   reference_avg_activity
    #   reference_avg_sleep
    #   reference_avg_activecals
    #
    # current_summary_df:
    #   current_avg_satisfaction
    #   current_avg_health
    #   current_avg_sleepduration
    #   current_avg_hr
    #   current_avg_hrv
    #   current_avg_readiness
    #   current_avg_activity
    #   current_avg_sleep
    #   current_avg_activecals
    #
    # We combine them horizontally so both sets of values
    # appear side-by-side in a single row using the
    # concatenate function (pl.concat).
    #
    # This makes it easy to calculate:
    #   current value - reference value

    combined_df: pl.DataFrame = pl.concat(
        [reference_summary_df, current_summary_df],
        how="horizontal",
    )

    # ----------------------------------------------------
    # STEP 4: DEFINE DIFFERENCE RECIPES
    # ----------------------------------------------------
    # A difference recipe calculates:
    #
    #     current average - reference average
    #
    # Positive values mean the current period is larger.
    # Negative values mean the current period is smaller.

    satisfaction_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_satisfaction") - pl.col("reference_avg_satisfaction"))
        .round(2)
        .alias("satisfaction_mean_difference")
    )

    health_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_health") - pl.col("reference_avg_health"))
        .round(2)
        .alias("health_mean_difference")
    )

    sleepduration_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_sleepduration") - pl.col("reference_avg_sleepduration"))
        .round(2)
        .alias("sleepduration_mean_difference")
    )

    hr_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_hr") - pl.col("reference_avg_hr"))
        .round(2)
        .alias("hr_mean_difference")
    )

    hrv_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_hrv") - pl.col("reference_avg_hrv"))
        .round(2)
        .alias("hrv_mean_difference")
    )

    readiness_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_readiness") - pl.col("reference_avg_readiness"))
        .round(2)
        .alias("readiness_mean_difference")
    )

    activity_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_activity") - pl.col("reference_avg_activity"))
        .round(2)
        .alias("activity_mean_difference")
    )

    sleep_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_sleep") - pl.col("reference_avg_sleep"))
        .round(2)
        .alias("sleep_mean_difference")
    )

    activecals_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_activecals") - pl.col("reference_avg_activecals"))
        .round(2)
        .alias("activecals_mean_difference")
    )

    # ----------------------------------------------------
    # STEP 4.1: APPLY THE DIFFERENCE RECIPES TO EXPAND THE DATAFRAME
    # ----------------------------------------------------
    drift_df: pl.DataFrame = combined_df.with_columns(
        [
            satisfaction_mean_difference_recipe,
            health_mean_difference_recipe,
            sleepduration_mean_difference_recipe,
            hr_mean_difference_recipe,
            hrv_mean_difference_recipe,
            readiness_mean_difference_recipe,
            activity_mean_difference_recipe,
            sleep_mean_difference_recipe,
            activecals_mean_difference_recipe,
        ]
    )

    # ----------------------------------------------------
    # STEP 5: DEFINE DRIFT FLAG RECIPES
    # ----------------------------------------------------
    # A drift flag recipe checks whether the absolute size
    # of the difference exceeds a threshold.
    #
    # We use abs() because either direction may matter:
    # - much higher than reference
    # - much lower than reference

    satisfaction_is_drifting_flag_recipe: pl.Expr = (
        pl.col("satisfaction_mean_difference").abs() > SATISFACTION_DRIFT_THRESHOLD
    ).alias("satisfaction_is_drifting_flag")

    health_is_drifting_flag_recipe: pl.Expr = (
        pl.col("health_mean_difference").abs() > HEALTH_DRIFT_THRESHOLD
    ).alias("health_is_drifting_flag")

    sleepduration_is_drifting_flag_recipe: pl.Expr = (
        pl.col("sleepduration_mean_difference").abs() > SLEEPDURATION_DRIFT_THRESHOLD
    ).alias("sleepduration_is_drifting_flag")

    hr_is_drifting_flag_recipe: pl.Expr = (
        pl.col("hr_mean_difference").abs() > HR_DRIFT_THRESHOLD
    ).alias("hr_is_drifting_flag")

    hrv_is_drifting_flag_recipe: pl.Expr = (
        pl.col("hrv_mean_difference").abs() > HRV_DRIFT_THRESHOLD
    ).alias("hrv_is_drifting_flag")

    readiness_is_drifting_flag_recipe: pl.Expr = (
        pl.col("readiness_mean_difference").abs() > READINESS_DRIFT_THRESHOLD
    ).alias("readiness_is_drifting_flag")

    activity_is_drifting_flag_recipe: pl.Expr = (
        pl.col("activity_mean_difference").abs() > ACTIVITY_DRIFT_THRESHOLD
    ).alias("activity_is_drifting_flag")

    sleep_is_drifting_flag_recipe: pl.Expr = (
        pl.col("sleep_mean_difference").abs() > SLEEP_DRIFT_THRESHOLD
    ).alias("sleep_is_drifting_flag")


    activecals_is_drifting_flag_recipe: pl.Expr = (
        pl.col("activecals_mean_difference").abs() > ACTIVECALS_DRIFT_THRESHOLD
    ).alias("activecals_is_drifting_flag")


    # ----------------------------------------------------
    # STEP 5.1: APPLY THE DRIFT FLAG RECIPES TO EXPAND THE DATAFRAME
    # ----------------------------------------------------
    drift_df = drift_df.with_columns(
        [
            satisfaction_is_drifting_flag_recipe,
            health_is_drifting_flag_recipe,
            sleepduration_is_drifting_flag_recipe,
            hr_is_drifting_flag_recipe,
            hrv_is_drifting_flag_recipe,
            readiness_is_drifting_flag_recipe,
            activity_is_drifting_flag_recipe,
            sleep_is_drifting_flag_recipe,
            activecals_is_drifting_flag_recipe,
        ]
    )

    LOG.info("Calculated summary differences and drift flags")

    # ----------------------------------------------------
    # STEP 6: SAVE THE FLAT DRIFT SUMMARY AS AN ARTIFACT
    # ----------------------------------------------------
    drift_df.write_csv(OUTPUT_FILE)
    LOG.info(f"Wrote drift summary file: {OUTPUT_FILE}")

    # Take a look at the summary dataframe.
    # Lots of columns with one row of values.
    LOG.info("Drift summary dataframe:")
    LOG.info(drift_df)
    LOG.info("Let's make that a bit nicer to read...")
    LOG.info("All remaining steps are about creating a nicer display.")

    # ----------------------------------------------------
    # OPTIONAL STEP 6.1: LOG THE SUMMARY ONE FIELD PER LINE
    # ----------------------------------------------------
    # drift_df has one row with many columns.
    # Convert that one row to a dictionary so we can log:
    # column_name: value

    # The Polars to_dicts() function returns a list of dictionaries, one per row.
    # the [0] gets the first (and only) dictionary from the list.
    # We often count starting at zero
    # because the first row is 0 away from the start of the dataframe.
    drift_summary_dict = drift_df.to_dicts()[0]

    LOG.info("========================")
    LOG.info("Drift Detection Process: ")
    LOG.info("========================")
    LOG.info("1. Summarize each period with means.")
    LOG.info("2. Compute difference of means.")
    LOG.info("3. Flag drift if absolute difference of means exceeds a threshold.")
    LOG.info("========================")

    LOG.info("Drift summary (one field per line):")
    for field_name, field_value in drift_summary_dict.items():
        LOG.info(f"{field_name}: {field_value}")

    # ----------------------------------------------------
    # OPTIONAL STEP 7: CREATE A LONG-FORM ARTIFACT FOR DISPLAY
    # ----------------------------------------------------
    # Create a second artifact with one field per row.
    # This is easier to read than a single very wide row.
    # We create a new dataframe with two columns:
    # - field_name: the name of the summary field
    # - field_value: the value of the summary field (converted to string for display)

    drift_summary_long_df = pl.DataFrame(
        {
            "field_name": list(drift_summary_dict.keys()),
            "field_value": [str(value) for value in drift_summary_dict.values()],
        }
    )
    # ----------------------------------------------------
    # OPTIONAL STEP 7.1: SAVE THE LONG-FORM DRIFT SUMMARY AS AN ARTIFACT
    # ----------------------------------------------------
    drift_summary_long_df.write_csv(SUMMARY_LONG_FILE)
    LOG.info(f"Wrote long summary file: {SUMMARY_LONG_FILE}")

    LOG.info("========================")
    LOG.info("Pipeline executed successfully!")
    LOG.info("========================")
    LOG.info("END main()")


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
