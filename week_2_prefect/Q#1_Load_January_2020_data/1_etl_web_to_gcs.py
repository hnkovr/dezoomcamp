import os
from pathlib import Path

import pandas as pd
from loguru import logger as log
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from util.printy import fmt_df


@task(retries=3)
def ingest_data(dataset_url):
    df = pd.read_csv(dataset_url)
    log.debug(f"Data from <{dataset_url}>'s been read to df ({len(df)} rows): {fmt_df(df)}")
    return df


@task(log_prints=True)
def transform_data(df):
    if 'tpep_pickup_datetime' not in df.columns:
        raise TypeError(f"Unknown data in df: {fmt_df(df)}")
    else:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df


@task(log_prints=True)
def export_data_local(df: pd.DataFrame, color, dataset_file):
    path = Path(f"data/{color}/{dataset_file}.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    assert os.path.isdir(os.path.dirname(path)), os.path.dirname(path)
    log.info(f"Saving df to parquet by path: {path}:..")
    df.to_parquet(path, compression="gzip")
    return path


@task(log_prints=True)
def export_data_gcs(path):
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return path


@flow(name="Ingest file to GCS")
def etl_web_to_gcs(*,
                   color="yellow",
                   year=2020,
                   month=1,
                   ):
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = ingest_data(dataset_url)
    df_clean = transform_data(df)
    path = export_data_local(df_clean, color, dataset_file)
    export_data_gcs(path)

@flow
def run(test=False):
    cym_list_of_dicts = []
    for month in (2, 3):
        d = dict(
            color="yellow",
            year=2019,
            month=month,
        )
        cym_list_of_dicts.append(d)
    log.info(f"{cym_list_of_dicts:}")

    if not test:
        for d in cym_list_of_dicts:
            etl_web_to_gcs(**d)


if __name__ == "__main__": run(test=True)
