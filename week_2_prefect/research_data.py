# ? this's needed? file:/Users/user/github.com/hnkovr/prefect-zoomcamp/docs/homework.md
import pandas as pd

from util import log

# from loguru import logger
logger = log
from util.common import check_file_exists


def print_rows_counts(color_year_months=(
        ("yellow", 2019, 2),
        ("yellow", 2019, 3),
)):
    res = 0
    logger.info("Started!")
    for color, year, month in color_year_months:
        dataset_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02}.parquet'
        assert check_file_exists(dataset_url), dataset_url
        pdf = pd.read_parquet(dataset_url)
        dlt = len(pdf)
        print(
            # pdf.head(1), '...',
            f"{dataset_url:}\t{dlt=}",
            sep='\n'
        )
        res += dlt
    print(f"{res=}")
    logger.info("Finished!")


if __name__ == '__main__':
    # print_rows_counts((
    #     ("yellow", 2019, 2),
    #     ("yellow", 2019, 3),
    # ))
    # print_rows_counts((
    #     ("green", 2020, 11),
    # ))
    print_rows_counts((
        ("green", 2019, 3),
        ("green", 2019, 4),
        # ("green", 2020, 4),
    ))

"""
>>>
/Users/user/github.com/hnkovr/venv/bin/python /Users/user/github.com/hnkovr/#dezoomcamp/week_2_prefect/scratch_12.py 
23/02/08 17:20:41 D /Users/user/github.com/hnkovr/#dezoomcamp/util/deco.py:21 format='{time:YY/MM/DD HH:mm:ss} {level.name[0]} {file.path}:{line} {message}'
23/02/08 17:20:41 I /Users/user/github.com/hnkovr/#dezoomcamp/week_2_prefect/scratch_12.py:12 Started!
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-02.parquet	dlt=7049370
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-03.parquet	dlt=7866620
res=14915990
23/02/08 17:21:30 I /Users/user/github.com/hnkovr/#dezoomcamp/week_2_prefect/scratch_12.py:25 Finished!

Process finished with exit code 0

"""
####
# # color = "yellow"
# color = "green"
# year = 2020
# month = 1
# dataset_file = f"{color}_tripdata_{year}-{month:02}"
# taset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
# dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.csv.gz'
# dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{color}_tripdata_{year}-{month:02}.parquet'
# yellow_tripdata_2019-01.parquet
# taset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz'
# taset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{year}-{month:02}.parquet'
# log(f"{taset_url = }")

# path = "/Users/user/github.com/hnkovr/#dezoomcamp/week_2_prefect/data/yellow/yellow_tripdata_2020-01.parquet"
# os.system(f"wget {dataset_url} {}")

# len(pd.read_parquet(path))
