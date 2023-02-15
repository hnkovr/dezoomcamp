# from file://Users/user/github.com/hnkovr/dezoomcamp/week_3_dwh/homework.md
# SETUP:
# Create an external table using the fhv 2019 data.
# Create a table in BQ using the fhv 2019 data (do not partition or cluster this table).
# Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv
# from: file://Users/user/github.com/hnkovr/dezoomcamp/week_2_prefect/Q#1_Load_January_2020_data/1_etl_web_to_gcs.py
import functools
import os
from pathlib import Path
from typing import List, Dict

import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from util.logy import log
from util.printy import fmt_df

tast = functools.partial(task, log_prints=True)
ef = log.catch


class CONF:
    TEST = True
    year = 2019
    user_data_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv'
    dataset_file_template: str = "{color}_tripdata_{year}-{month:02}"
    ##daset_url_example : str = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz'
    dataset_url_template: str = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    tgt_data_path_template: str = "data/{color}/{dataset_file}.parquet"
    GcsBucketName: str = "zoom-gcs"


@task(retries=3)
@ef
def ingest_data(dataset_url):
    df = pd.read_csv(dataset_url)
    log.debug(f"Data from <{dataset_url}>'s been read to df ({len(df)} rows): {fmt_df(df)}")
    return df


@task
@ef
def transform_data(df: pd.DataFrame):
    if 'tpep_pickup_datetime' not in df.columns:
        raise TypeError(f"Unknown data in df: {fmt_df(df)}")
    else:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df


@task
@ef
def export_data_local(df: pd.DataFrame, color: str, dataset_file: str):
    path = Path(CONF.tgt_data_path_template.format(**locals()))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    assert os.path.isdir(os.path.dirname(path)), os.path.dirname(path)
    log.info(f"Saving df to parquet by path: {path}:..")
    df.to_parquet(path, compression="gzip")
    return path


@task
@ef
def export_data_gcs(path):
    gcs_block = GcsBucket.load(CONF.GcsBucketName)
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return path


@flow(name="Ingest file to GCS")
@ef
def etl_web_to_gcs(*,
                   color="fhv",
                   year=2019,
                   month=1,
                   ):
    dataset_file = CONF.dataset_file_template.format(**locals())
    dataset_url = CONF.dataset_url_template.format(**locals())
    df = ingest_data(dataset_url)
    df_clean = transform_data(df)
    path = export_data_local(df_clean, color, dataset_file)
    export_data_gcs(path)


@flow
@ef
def run(test=False):
    # raise NotImplemented(f"{(cym_list_of_dicts:={})=}")
    cym_list_of_dicts: List[Dict] = []
    for m in range(12):
        d = dict(
            color=CONF.user_data_url.split('/')[-1],
            year=CONF.year,
            month=m + 1,
        )
        log.debug(f"Adding data {{{d = }}}..")
        cym_list_of_dicts.append(d)
    if not test:
        for d in cym_list_of_dicts:
            etl_web_to_gcs(**d)
    else:
        print(f"{CONF.TEST=} mode - do noting.")


@ef
def test1():
    cym_list_of_dicts = []
    for month in (2, 3):
        d = dict(
            color="yellow",
            year=2019,
            month=month,
        )
        cym_list_of_dicts.append(d)

    log.info(f"{cym_list_of_dicts = }")


@ef
def main():
    print(f"Load data from {CONF.user_data_url = }:..")
    run(test=CONF.TEST)


if __name__ == "__main__": main()
