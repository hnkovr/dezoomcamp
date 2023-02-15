"""
#from: https://github-com.translate.goog/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_2_workflow_orchestration/homework.md?_x_tr_sl=auto&_x_tr_tl=en&_x_tr_hl=en-US

Question 3. Loading data to BigQuery

Using etl_gcs_to_bq.py as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

    14,851,920
    12,282,990
    27,235,753
    11,338,483

"""
import functools
import os.path
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd
from prefect import flow, task
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

sys.path.append('..')
from util.printy import fmt_df
from util.logy import call_log, log

# printl = lambda a: (print(a), log.info(a))

# The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.
task = functools.partial(task, log_prints=True)

COLOR_YEAR_MONTHS = (
    ("yellow", 2019, 2),
    ("yellow", 2019, 3),
)


@task(retries=3)
@call_log
def extract_from_gcs(color, year, month) -> Path:
    """Data Extract from Google Cloud Storage"""
    gcs_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    print(f"# {gcs_path = }")
    return Path(f"../data/{gcs_path}")


@task
def transform(path: str,
              do_tranform=False
              ## The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.
              ) -> pd.DataFrame:
    """Data Cleaning"""
    df = pd.read_parquet(path)
    if do_tranform:
        print(f"pre: missing passenger count: {df['passenger_count'].isna.sum}")
        df['passenger_count'].fillna(0, inplace=True)
        print(f"post: missing passenger count: {df['passenger_count'].isna.sum}")
    print(f"# Proceed {len(pd)} dataframe rows.")
    return df


@task
def write_bq(df) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")
    destination_table = "dezoomcamp.rides"
    credentials = gcp_credentials_block.get_credentials_from_service_account
    # print(f"{locals() = }")
    print(f"Loading data to BigQuery:.. ({fmt_df(df)})")
    df.to_gbq(
        destination_table=destination_table,
        project_id="digital-aloe-375022",
        credentials=credentials,
        chunksize=500_000,
        if_exists="append"
    )


@flow(name=os.path.basename(__file__))
@call_log
def etl_gcs_to_bq(
        color_year_months_list: List[Tuple[str, str, str]],
):
    @log.catch
    def load_cym(
            color: str, year: str, month: str,
    ):
        print(f"Extract data from GCS for <{(color, year, month)=}>:..")
        path = extract_from_gcs(color, year, month)

        df = transform(path)
        write_bq(df)

    for color, year, month in color_year_months_list:
        load_cym(color, year, month)


if __name__ == "__main__":
    cym_list = []
    for month in (2, 3):
        a = list(dict(
            color="yellow",
            year=2019,
            month=month,
        ).values())
        cym_list.append(a)
    log.info(f"{cym_list:}")

    etl_gcs_to_bq(cym_list)
