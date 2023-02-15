import pandas as pd
import requests
pd.options.display.max_columns = 111


def o(a): print(o); return o


def check_file_exists(url):
    response = requests.head(url)
    return response.status_code == 200
