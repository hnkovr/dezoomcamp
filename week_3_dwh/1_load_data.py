# from file://Users/user/github.com/hnkovr/dezoomcamp/week_3_dwh/homework.md
# SETUP:
# Create an external table using the fhv 2019 data.
# Create a table in BQ using the fhv 2019 data (do not partition or cluster this table).
# Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv
# from: file://Users/user/github.com/hnkovr/dezoomcamp/week_2_prefect/Q#1_Load_January_2020_data/1_etl_web_to_gcs.py

#todo! try this code: https://chat.openai.com/chat/b1b1e0a0-5da2-403e-8c39-cca0bc0be8b8


# /Users/user/github.com/hnkovr/dezoomcamp/week_3_dwh/1_load_data.py
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


class CONF:
    TEST = False
    # TEST = True
    # TEST = None
    year = 2019
    GcsBucketName: str = "zoom-gcs"
    user_data_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv'
    dataset_file_template: str = "{color}_tripdata_{year}-{month:02}"
    daset_url_example: str = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz'
    dataset_url_template: str = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    tgt_data_path_template: str = "data/{color}/{dataset_file}.parquet"


@task(retries=3)
# @ef
def ingest_data(dataset_url) -> pd.DataFrame:
    df = pd.read_csv(dataset_url)
    log.debug(f"Data from <{dataset_url}>'s been read to df ({len(df)} rows): {fmt_df(df)}")
    return df


@task
# @ef
def transform_data(df: pd.DataFrame):
    proceed = False
    if 'tpep_pickup_datetime' in df.columns:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        proceed = True
    if 'PUlocationID' in df.columns and 'DOlocationID' in df.columns:
        df["PUlocationID"] = df.PUlocationID.astype("Int64")
        df["DOlocationID"] = df.DOlocationID.astype("Int64")
        proceed = True
    if not proceed:
        # raise TypeError(f"Unknown data in df: {fmt_df(df)}")
        print(f"No transformation needed for df: {fmt_df(df)}")
    return df

@task
# @ef
def export_data_local(df: pd.DataFrame, color: str, dataset_file: str):
    path = Path(CONF.tgt_data_path_template.format(**locals()))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    assert os.path.isdir(os.path.dirname(path)), os.path.dirname(path)
    log.info(f"Saving df to parquet by path: {path}:..")
    df.to_parquet(path, compression="gzip")
    return path


@task
# @ef
def export_data_gcs(path):
    gcs_block = GcsBucket.load(CONF.GcsBucketName)
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return path


def get_dataset_url(**kwargs):
    dataset_file = CONF.dataset_file_template.format(**kwargs)
    dataset_url = CONF.dataset_url_template.format(color=kwargs['color'], dataset_file=dataset_file)
    return dataset_url


@flow(name="Ingest file to GCS")
# @ef
def etl_web_to_gcs(*,
                   # color="fhv", year=2019, month=1,
                   color, year, month
                   ):
    dataset_url = get_dataset_url(color=color, year=year, month=month)
    dataset_file = CONF.dataset_file_template.format(color=color, year=year, month=month)
    df: pd.DataFrame = ingest_data(dataset_url)
    if CONF.TEST is None:
        print(
            df.head(3).T,
            df.dtypes,
            df.columns,
            sep='\n\n',
        )
        return
    df_clean = transform_data(df)
    path = export_data_local(df_clean, color, dataset_file)
    export_data_gcs(path)


def get_cym_list_of_dicts(months=tuple()):
    cym_list_of_dicts: List[Dict] = []
    months = range(*months) if months else range(12)
    log.debug(f"# {months = }")
    for m in months:
        d = dict(
            color=CONF.user_data_url.split('/')[-1],
            year=CONF.year,
            month=m + 1,
        )
        log.debug(f"Adding data {{{d = }}}..")
        cym_list_of_dicts.append(d)
    return cym_list_of_dicts


@flow
# @ef
def run(test=False, **kw):
    # raise NotImplemented(f"{(cym_list_of_dicts:={})=}")
    cym_list_of_dicts = get_cym_list_of_dicts(**kw)
    if not test:
        for d in cym_list_of_dicts:
            etl_web_to_gcs(**d)
            if CONF.TEST is None:
                return
    else:
        get_loading_links()
        print(f"{CONF.TEST=} mode - do noting.")


def get_loading_links():
    xxx = list(map(
        (lambda a: get_dataset_url(**a)),
        get_cym_list_of_dicts()
    ))
    log.debug(f"{xxx = }")
    return xxx


def test_run():
    xxx = get_loading_links()
    assert CONF.daset_url_example in xxx


# @ef
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


# @ef
def main():
    print(f"Load data from {CONF.user_data_url = }:..")
    run(test=CONF.TEST,
        # months=(8, 12),
        )


if __name__ == "__main__": main()
