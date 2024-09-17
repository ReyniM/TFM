from pathlib import Path

import gzip
import io
import re

import pytest
import requests

from pyspark.sql import SparkSession
from pandas import DataFrame as PandasDataFrame

spark = SparkSession.builder.config("spark.executor.memory", "6g") \
                                            .config("spark.driver.memory", "6g").getOrCreate()

# PATHS.
actual_folder = str(Path(__file__).parent)
path_yaml = actual_folder + '/resources/config_tests.yaml'
listings_csv = actual_folder + '/resources/csv/listings.csv'
crimes_csv = actual_folder + '/resources/csv/crimes.csv'
comments_csv = actual_folder + '/resources/csv/comments.csv'


# A DICTIONARY TO TEST 'get_links'.
dict1_for_get_links = {'landing': ['/Users/reynimanuelcalderoncots/Desktop/listings_4000.csv/',
                                   '/Users/reynimanuelcalderoncots/Desktop/crimes_4000.csv/',
                                   '/Users/reynimanuelcalderoncots/Desktop/comments_4000.csv/']}

# A DICTIONARY TO TEST 'get_links'.
dict2_for_get_links = {'listings': 'https://data.insideairbnb.com/united-states/ca/los-angeles/2024-03-11/visualisations/listings.csv',
                       'crimes': 'https://data.lacity.org/resource/2nrs-mtv8.csv',
                       'comments': 'https://data.insideairbnb.com/united-states/ca/los-angeles/2024-06-07/data/reviews.csv.gz'}


# RANDOM DATA TO TEST 'get_df'.
data_vehicles = [
    {'Brand': 'BMW', 'Country': 'Germany', 'Model': '3 Series'},
    {'Brand': 'Audi', 'Country': 'Germany', 'Model': 'A4'},
    {'Brand': 'Mercedes-Benz', 'Country': 'Germany', 'Model': 'C-Class'},
    {'Brand': 'Renault', 'Country': 'France', 'Model': 'Clio'},
    {'Brand': 'Ferrari', 'Country': 'Italy', 'Model': '488'},
    {'Brand': 'Volvo', 'Country': 'Sweden', 'Model': 'XC90'},
    {'Brand': 'Peugeot', 'Country': 'France', 'Model': '208'}
]

data_employers = [
    {'ID': '001', 'Name': 'Juan Pérez', 'Age': 28},
    {'ID': '002', 'Name': 'María López', 'Age': 34},
    {'ID': '003', 'Name': 'Carlos García', 'Age': 45},
    {'ID': '004', 'Name': 'Ana Martínez', 'Age': 25},
    {'ID': '005', 'Name': 'Luis Fernández', 'Age': 38}
]

data_weather = [
    {'Date': '2024-08-01', 'Temperature': 28.5, 'Condition': 'Sunny'},
    {'Date': '2024-08-02', 'Temperature': 30.2, 'Condition': 'Cloudy'},
    {'Date': '2024-08-03', 'Temperature': 27.8, 'Condition': 'Light Rain'},
    {'Date': '2024-08-04', 'Temperature': 25.6, 'Condition': 'Heavy Rain'},
    {'Date': '2024-08-05', 'Temperature': 29.1, 'Condition': 'Partially Cloudy'}
]

vehicles_df = spark.createDataFrame(data=data_vehicles)
employers_df = spark.createDataFrame(data=data_employers)
weather_df = spark.createDataFrame(data=data_weather)

dataframes = [vehicles_df, employers_df, weather_df]


def get_links(dic1: dict, dic2: dict) -> any:
    """
    This method will test the correct path
    where files are going to be archived and
    checks the 'write' mode of the files that
    are downloaded from the internet.
    """

    for key, value_ in dic1.items():
        list_df = []
        if key == 'landing':
            for path in value_:
                list_df.append(path)
                if len(list_df) == 3:
                    print('>>> Dataframes have been read!')
                    save_path = str(list_df[0])
        else:
            print('>>> Could not access landing paths!')

    for url, value in dic2.items():
        names = [url]

        r = requests.get(value)
        if r.status_code != 200:
            raise ConnectionError('Request to server failed!')
        else:
            pattern = re.compile(r'/\w.*gz')
            valid = pattern.findall(value)
            if valid:
                with gzip.open(io.BytesIO(r.content), 'rb') as f:
                    file_content = f.read()
                    decoded_text = file_content.decode('utf-8')

                    with open(f'{save_path}{names[0]}.csv', 'w', encoding='utf-8') as file_:
                        file_.write(decoded_text)
            else:
                with open(f'{save_path}{names[0]}.csv', 'wb') as _file:
                    _file.write(r.content)

                with open(f'{save_path}{names[0]}.csv', 'r') as f:
                    lec = f.readline()

                print('>>> Files downloaded from the internet, and saved!')
                return save_path, lec.strip()


def get_df(data_for_df: list) -> any:
    """
    This method will test if it can convert
    spark dataframes as 'Pandas' dataframes.
    """

    vehicles = data_for_df[0].toPandas()
    employers = data_for_df[1].toPandas()
    weather = data_for_df[2].toPandas()
    return type(vehicles), type(employers), type(weather)


@pytest.mark.parametrize(
'expected', [('/Users/reynimanuelcalderoncots/Desktop/listings_4000.csv/', 'id,name,host_id,host_name,'
              'neighbourhood_group,neighbourhood,latitude,longitude,room_type,price,minimum_nights,'
              'number_of_reviews,last_review,reviews_per_month,calculated_host_listings_count,availability_365,'
              'number_of_reviews_ltm,license')])
def test_get_links(expected):
    assert get_links(dict1_for_get_links, dict2_for_get_links) == expected


@pytest.mark.parametrize(
'expected', [(PandasDataFrame, PandasDataFrame, PandasDataFrame)])
def test_get_df(expected):
    assert get_df(dataframes) == expected
