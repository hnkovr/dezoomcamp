"""
# from: /Users/user/github.com/hnkovr/dezoomcamp/week_3_dwh/1_load_data.py
"""
import os.path
import sys

sys.path.append('..')

import pandas as pd
from fastcore.all import *
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from util.logy import log
from util.printy import fmt_df

tast = functools.partial(task, log_prints=True)
print = log.info
os.environ['GOOGLE_CLOUD_PROJECT'] = 'dez-2023'


class CONF:
    __repr__ = basic_repr('Test,')
    GcsBucketName: str = "zoom-gcs"
    skip_locally_reloading = True  # ..
    user_data_url = 'https://github.com/DataTalksClub/nyc-tlc-data'
    dataset_file_template: str = "{color}_tripdata_{year}-{month:02}"
    dataset_url_template: str = f"{user_data_url}/releases/download/" + "{color}/{dataset_file}.csv.gz"
    tgt_data_path_template: str = "data/{color}/{dataset_file}.parquet"


# @task(retries=3)
# @ef
def ingest_data(dataset_url) -> pd.DataFrame:
    df = pd.read_csv(dataset_url)
    log.debug(f"Data from <{dataset_url}>'s been read to df ({len(df)} rows): {fmt_df(df)}")
    return df


# @task
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


def get_path_4_color_n_dataset_file(color: str, dataset_file: str) -> str:
    return Path(CONF.tgt_data_path_template.format(**locals()))


# @task
# @ef
def export_data_local(df: pd.DataFrame, path: str = None, *, color: str = None, dataset_file: str = None):
    xxx = color, dataset_file
    assert all(xxx) or not any(xxx), xxx
    xxx = (path, all(xxx))
    assert any(xxx) and not all(xxx), xxx

    path = path or get_path_4_color_n_dataset_file(color, dataset_file)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    assert os.path.isdir(os.path.dirname(path)), os.path.dirname(path)
    log.info(f"Saving df to parquet by path: {path}:..")
    df.to_parquet(path, compression="gzip")
    return path


# @task
# @ef
def export_data_gcs(path):
    log.debug(f"> exporting {path=} to CG:..")
    gcs_block = GcsBucket.load(CONF.GcsBucketName)
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return path


def get_dataset_url(**kwargs):
    # if 'year_for_test' in kwargs.keys(): kwargs['year'] = kwargs['year_for_test']; del kwargs['year_for_test']
    dataset_file = _CONF.dataset_file_template.format(**kwargs)
    dataset_url = _CONF.dataset_url_template.format(color=kwargs['color'], dataset_file=dataset_file)
    return dataset_url


# @task(name="Ingest file to GCS")
# @ef
def etl_web_to_gcs(*,
                   color, year, month,
                   skip_locally_reloading=CONF.skip_locally_reloading,
                   ):
    log.info(f"Loading data for {color=} {year=} {year=} to local and to GCS after it:..")
    dataset_url = get_dataset_url(color=color, year=year, month=month)
    dataset_file = _CONF.dataset_file_template.format(color=color, year=year, month=month)
    path = get_path_4_color_n_dataset_file(color, dataset_file)

    if not (skip_locally_reloading and os.path.isfile(path)):
        log.debug(f"# {path=}; {os.path.isfile(path) = }")
        df: pd.DataFrame = ingest_data(dataset_url)
        if _CONF.TEST is None:
            log.info(f"{_CONF.TEST=} is None: print() and return...")
            print(
                df.head(3).T,
                df.dtypes,
                df.columns,
                sep='\n\n',
            )
            return
        df_clean = transform_data(df)
        path = export_data_local(df_clean, color=color, dataset_file=dataset_file)
    else:
        log.debug(f"# {_CONF.skip_locally_reloading=} and {path=} already loaded – skip loading to local.")

    export_data_gcs(path)


# @ef
# ?e: RuntimeError: Tasks cannot be run from within tasks. Did you mean to call this task in a flow?
@flow(name="Ingest file to GCS #main()")
def main():
    log.info(f"Starting prod loading!.. ({__file__=}")
    load_data_4_years_colors(
        years=(2020,),
        colors='yellow green'.split()
    )
    load_data_4_years_colors(
        years=(2019,), from_month=5,
        colors='yellow'.split()
    )
    load_data_4_years_colors(
        years=(2019,), from_month=1,
        colors='green'.split()
    )

    # ~^
    # load_data_4_years_colors(
    #     years=(2019,),
    #     colors=('fhv',),
    # )
    # load_data_4_years_colors(
    #     years=(2019, 2020),
    #     colors='yellow green'.split()
    # )


# @task(name="Ingest file to GCS #load_data_4_years_colors")
def load_data_4_years_colors(
        years: tuple[int],
        colors: tuple[str],  # ssv
        **kwargs
):
    years = map(str, years)
    for year in years:
        for color in colors:
            load_data_4_year_n_color(year, color, **kwargs)


@task
def load_data_4_year_n_color(year: int, color: str, *, from_month=1):
    for month in range(from_month, 13):
        etl_web_to_gcs(color=color, year=year, month=month)


# @task
def load_taxi_zone_lookup(): return load_custom_data(
    '/Users/user/github.com/mrSifon/my_taxi_rides_ny/data/taxi_zone_lookup.csv'
)


def get_path_4_custom_data(data_path, *,
                           add_data_file_name_subdir=False
                           ):
    """ :param data_path: url/local path    :return: subpath starting "data/<some struct for store data files>"
    >>> data_path = '/Users/user/github.com/mrSifon/my_taxi_rides_ny/data/taxi_zone_lookup.csv'
    >>> get_path_4_custom_data(data_path)
    'data/csv/taxi_zone_lookup.csv'
    >>> get_path_4_custom_data(data_path, add_data_file_name_subdir=True)
    'data/csv/taxi_zone_lookup/taxi_zone_lookup.csv'
    """
    fname = data_path.split('/')[-1]
    return ((res :=
             (f"data/{data_path.split('.')[-1]}"
              f"""{('/' + '.'.join(fname.split('.')[:-1])) if add_data_file_name_subdir else ''}"""
              f"/{fname}"
              ))
             , log.debug(f'''{res=}'''))[0]


def load_custom_data(data_path):
    log.info(f"Loading data <{data_path}> to GCS path <{(path := get_path_4_custom_data(data_path))=}> "
             f"(with using local path <{os.path.join(os.getcwd(), path)}>):..")

    def ingest_data(data_path) -> pd.DataFrame: log.debug(
        f"Data from <{data_path}>'s been read to df ({len((df := pd.read_csv(data_path)))} rows): {fmt_df(df)}"
    ); return df

    def transform_data(df): return [df, log.debug(f"No transormation to <{fmt_df(df)}> applied.")][0]

    df = ingest_data(data_path)
    df_clean = transform_data(df)
    # path = \
    export_data_local(df_clean, path)
    return export_data_gcs(path)


if __name__ == "__main__":
    class _CONF:
        TEST = 2  # cause tests're not ready!


    step = _CONF.TEST
    if step == 2:
        load_taxi_zone_lookup()
    elif step is True:
        log.info(f"{_CONF.TEST=}: starting main()...")
        main()
    else:
        raise NotImplementedError
