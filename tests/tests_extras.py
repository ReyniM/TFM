from typing import Any
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pathlib import Path

import yaml
import pytest
import re

spark = SparkSession.builder.config("spark.executor.memory", "6g") \
                            .config("spark.driver.memory", "6g").getOrCreate()

actual_folder = str(Path(__file__).parent)
path_yaml = actual_folder + '/resources/config_tests.yaml'
listings_csv = actual_folder + '/resources/csv/listings.csv'
crimes_csv = actual_folder + '/resources/csv/crimes.csv'
comments_csv = actual_folder + '/resources/csv/comments.csv'

# A DICTIONARY TO TEST 'read_df'.
dict_for_read_df = {'landing': [listings_csv, crimes_csv, comments_csv]}

# LOADING A SPARK DATAFRAME TO TEST 'add_metadata'.
df_add_metadata = spark.read.csv(crimes_csv, header=True)


def open_config_file(conf: str) -> dict:
    """
    This function will test if it reads correctly
    the parameters from a configuration file '.yaml'.

    :param conf: Configuration file must be (.yaml).
    """

    try:
        with open(conf, 'r') as file:
            data = yaml.safe_load(file)

            df_urls = data['urls']
            return df_urls

    except FileNotFoundError:
        print('>>> Configuration file not founded!')
    except yaml.YAMLError as e:
        print(f'>>> Could not process yaml: {e}')
    except Exception as e:
        print(f'Error: {e}')


def read_df(landing_path: dict) -> any:
    """
    This function will test if it can read a path
    and return a 'pyspark' dataframe.

    :param landing_path: A web filepath that connects
    to the source of the data.
    """

    for key, value in landing_path.items():
        list_df = []
        if key == 'landing':
            for path in value:
                df = spark.read.csv(path, header=True, sep=',')
                list_df.append(df)
                if len(list_df) == 3:
                    print('>>> Dataframes have been read!')
                    return str(list_df[0])
        else:
            print('>>> Could not access landing paths!')


def take_zipcode(row: any) -> str | None:
    """
    This function will test if it returns
    the zip code of a given address.

    :param row: A row from a 'pandas' dataframe.
    """

    if row is not None:
        try:
            pattern = re.compile(r'CA\s(\d\d\d\d\d)')
            postal_code = pattern.findall(row)
            return postal_code[0]
        except IndexError:
            return None
    else:
        return None


def add_metadata(df_list: SparkDataFrame) -> tuple[Any, Any, Any]:
    """
    This function will test if metadata id added
    to the columns of a dataframe.

    :param df_list: A 'Spark' dataframe.
    """

    # METADATA FOR LISTINGS DATAFRAME.
    metadata_df_list = df_list.withMetadata('DATE OCC', {'comment': 'Date when the crime was committed'}) \
    .withMetadata('Vict Sex', {'comment': 'Victim gender.'}) \
    .withMetadata('LAT', {'comment': 'Is a measurement of a location north or south of the Equator.'})

    return metadata_df_list.schema['DATE OCC'].metadata, metadata_df_list.schema['Vict Sex'].metadata, metadata_df_list.schema['LAT'].metadata


@pytest.mark.parametrize(
'expected', [{'listings': 'https://data.insideairbnb.com/united-states/ca/los-angeles/2024-03-11/visualisations/listings.csv',
              'crime': 'https://data.lacity.org/resource/2nrs-mtv8.csv',
              'comments': 'https://data.insideairbnb.com/united-states/ca/los-angeles/2024-06-07/data/reviews.csv.gz'}])
def test_open_config_file(expected):
    assert open_config_file(path_yaml) == expected


@pytest.mark.parametrize(
'expected', ['DataFrame[id: string, name: string, host_id: string, host_name: string, neighbourhood_group: string, '
              'neighbourhood: string, latitude: string, longitude: string, room_type: string, price: string, '
              'minimum_nights: string, number_of_reviews: string, last_review: string, reviews_per_month: string, '
              'calculated_host_listings_count: string, availability_365: string, number_of_reviews_ltm: string, '
              'license: string]'])
def test_read_df(expected):
    assert read_df(dict_for_read_df) == expected


@pytest.mark.parametrize(
'expected1, expected2, expected3', [('90016', '90007', '91324')])
def test_take_zipcode(expected1, expected2, expected3):
    assert take_zipcode('2154 S Orange Dr, Los Angeles, CA 90016, USA') == expected1
    assert take_zipcode('3695 Normandie Ave, Los Angeles, CA 90007, USA') == expected2
    assert take_zipcode('19493 Roscoe Blvd, Northridge, CA 91324, USA') == expected3


@pytest.mark.parametrize(
'expected', [({'comment': 'Date when the crime was committed'}, {'comment': 'Victim gender.'},
              {'comment': 'Is a measurement of a location north or south of the Equator.'})])
def test_add_metadata(expected):
    assert add_metadata(df_add_metadata) == expected
