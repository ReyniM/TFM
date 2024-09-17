import gzip
import io
import re

import pandas as pd
import numpy as np
import requests

from pyspark.sql import SparkSession
from pandas import DataFrame as PandasDataFrame
from io import StringIO

from data_pipeline.extras import open_config_file, read_df, address_converter, take_zipcode, spark_ops


class WebScrape:

    def __init__(self, configuration_file: str):
        """
         Constructor method initiates the spark object for pyspark,
         and opens the configuration file for the project.
        """

        self.df_urls, self.read_adls_paths, self.write_adls_paths, self.api_google = open_config_file(configuration_file)
        self.spark = SparkSession.builder.getOrCreate()

    def get_links(self) -> None:
        """
        This method finds and downloads files
        from internet and then saves them to
        the path indicated in the configuration
        file.
        """

        dic1 = self.df_urls
        dic2 = self.write_adls_paths

        for key, value_ in dic2.items():
            list_df = []
            if key == 'bronze':
                for path in value_:
                    list_df.append(path)
                    print('>>> Bronze paths have been read!')
                    save_path = str(list_df[0])
            else:
                break

        for url, value in dic1.items():
            names = [url]

            r = requests.get(value)
            if r.status_code != 200:
                raise ConnectionError('Request to server failed!')
            else:
                pattern = re.compile(r'/\w.*gz')  # REGULAR EXPRESSION TO MAKE SURE WE'RE GETTING THE CORRECT FILE.
                valid = pattern.findall(value)
                if valid:
                    with gzip.open(io.BytesIO(r.content), 'rb') as f:
                        file_content = f.read()
                        decoded_text = file_content.decode('utf-8')

                        data = pd.read_csv(StringIO(decoded_text))
                        df = self.spark.createDataFrame(data)
                        df = df.coalesce(1)
                        df.write.mode('overwrite').csv(f'{save_path}{names[0]}', header=True)
                else:
                    content = r.content.decode('utf-8')
                    data = pd.read_csv(StringIO(content))
                    df = self.spark.createDataFrame(data)
                    df = df.coalesce(1)
                    df.write.mode('overwrite').csv(f'{save_path}{names[0]}', header=True)

        print('>>> Files downloaded from the internet, and saved!')

    def get_df(self) -> tuple[PandasDataFrame, PandasDataFrame, PandasDataFrame]:
        """
        This method will transform any spark
        dataframe to a 'Pandas' dataframe. Will
        be use inside the method 'transform' from the class
        'WebScrape'.
        """

        dataframes = read_df(self.read_adls_paths)  # CALLING 'read_df()' TO READ PATHS.
        listings_df = dataframes[0].toPandas()
        crimes_df = dataframes[1].toPandas()
        comments_df = dataframes[2].toPandas()

        return listings_df, crimes_df, comments_df

    def transform(self):
        """
        This method receives dataframes and makes few
        transformations for each of them. Then returns
        one complete dataframe joined with all the
        information from each one at the beginning.
        """

        # OPEN DICTIONARY WITH CONFIGURATION VALUES.
        dic3 = self.api_google
        for key, value in dic3.items():
            psw = [value]
            api = psw[0]

        # OPEN DICTIONARY WITH CONFIGURATION VALUES.
        dic4 = self.write_adls_paths
        for key, value_ in dic4.items():
            list_df = []
            if key == 'silver':
                for path in value_:
                    list_df.append(path)
                    if len(list_df) == 3:
                        print('>>> Silver paths have been read!')
                        save_path_listing = str(list_df[0])
                        save_path_crime = str(list_df[1])
                        save_path_comment = str(list_df[2])
            else:
                continue

        # CALLING 'get_df()' TO READ PANDAS DATAFRAMES.
        df_listings, df_crimes, df_comments = self.get_df()

        # CRIMES DATAFRAME TRANSFORMATION.

        df_crimes_pandas = df_crimes.drop(['date_rptd', 'area', 'area_name', 'rpt_dist_no', 'part_1_2', 'mocodes',
                                                'vict_descent', 'premis_cd', 'premis_desc', 'weapon_used_cd',
                                                'weapon_desc', 'status', 'status_desc', 'crm_cd_1', 'crm_cd_2',
                                                'crm_cd_3', 'crm_cd_4', 'location', 'cross_street'], axis=1) \
                                                .rename(columns={'dr_no': 'id_crime', 'date_occ': 'date_occur',
                                                                 'time_occ': 'time_occur', 'crm_cd': 'crime_code',
                                                                 'crm_cd_desc': 'crime_description',
                                                                 'vict_age': 'vict_age', 'vict_sex': 'vict_sex',
                                                                 'lat': 'latitude', 'lon': 'longitude'})

        df_crimes_pandas['address'] = df_crimes_pandas.apply(address_converter, axis=1, args=(api,))
        df_crimes_pandas['zip_code'] = df_crimes_pandas.apply(take_zipcode, axis=1)

        df_crimes_pandas = df_crimes_pandas.fillna(value=np.nan).replace({np.nan: None})

        # LISTINGS DATAFRAME TRANSFORMATION.

        df_listings_pandas = df_listings.drop(['host_id', 'host_name', 'neighbourhood_group', 'neighbourhood',
                                             'minimum_nights', 'number_of_reviews', 'last_review', 'reviews_per_month',
                                             'calculated_host_listings_count', 'availability_365',
                                             'number_of_reviews_ltm', 'license'], axis=1) \
                                                .rename(columns={'name': 'description'})

        df_listings_pandas['latitude'] = pd.to_numeric(df_listings_pandas['latitude'], errors='coerce')
        df_listings_pandas = df_listings_pandas.dropna(subset=['latitude'])
        df_listings_pandas['latitude'] = df_listings_pandas['latitude'].astype(float)

        df_listings_pandas['longitude'] = pd.to_numeric(df_listings_pandas['longitude'], errors='coerce')
        df_listings_pandas = df_listings_pandas.dropna(subset=['longitude'])
        df_listings_pandas['longitude'] = df_listings_pandas['longitude'].astype(float)

        df_listings_pandas['description'] = df_listings_pandas['description'].astype(str)
        df_listings_pandas['room_type'] = df_listings_pandas['room_type'].astype(str)

        df_listings_pandas['address'] = df_listings_pandas.apply(address_converter, axis=1, args=(api,))
        df_listings_pandas['zip_code'] = df_listings_pandas.apply(take_zipcode, axis=1)

        df_listings_pandas = df_listings_pandas.fillna(value=np.nan).replace({np.nan: None})

        # COMMENTS DATAFRAME TRANSFORMATION.

        df_comments_pandas = df_comments.drop(['id', 'date', 'reviewer_id', 'reviewer_name'], axis=1)
        df_comments_pandas['comments'] = df_comments_pandas['comments'].astype(str)
        df_comments_pandas = df_comments_pandas.fillna(value=np.nan).replace({np.nan: None})

        # CALL TO FUNCTION 'spark_ops()'
        listings_sp, crimes_sp, comments_sp = spark_ops(df_listings_pandas, df_crimes_pandas, df_comments_pandas)

        # SAVE TO SILVER.
        listings_sp = listings_sp.coalesce(1)
        listings_sp.write.format('delta').mode('overwrite').save(save_path_listing, header=True)
        crimes_sp = crimes_sp.coalesce(1)
        crimes_sp.write.format('delta').mode('overwrite').save(save_path_crime, header=True)
        comments_sp = comments_sp.coalesce(1)
        comments_sp.write.format('delta').mode('overwrite').save(save_path_comment, header=True)

        print(f'>>> Files processed and saved at:\n{save_path_listing}\n{save_path_crime}\n{save_path_comment}')
