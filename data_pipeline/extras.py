import googlemaps
import yaml
import re

from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame, SparkSession, functions as F, Window

spark = SparkSession.builder.getOrCreate()


def open_config_file(conf: str) -> tuple[dict, dict, dict, dict]:
    """
    Receives as parameter a '.yaml' config file and
    returns a tuple with dictionaries that contains
    the configuration parameters for the pipeline.
    This function is used in the constructor method
    of the class 'WebScrape'.

    :param conf: Configuration file must be (.yaml).
    """

    try:
        with open(conf, 'r') as file:
            data = yaml.safe_load(file)

            df_urls = data['urls']
            read_adls_paths = data['read_adls_container_paths']
            write_adls_paths = data['write_adls_container_paths']
            api_google = data['google_key']

            return df_urls, read_adls_paths, write_adls_paths, api_google
    except FileNotFoundError:
        print('>>> Configuration file not founded!')
    except yaml.YAMLError as e:
        print(f'>>> Could not process yaml: {e}')
    except Exception as e:
        print(f'Error: {e}')


def read_df(landing_path: dict) -> list[SparkDataFrame]:
    """
    This function will read the parameters from the configuration
    file and return a list with all the dataframes to work on the
    pipeline that are saved at the bronze layer of the lakehouse.
    This function is used in the method 'get_df' from the class
    'WebScrape'.

    :param landing_path: A web filepath that connects
    to the source of the data.
    """

    for key, value in landing_path.items():
        list_df = []
        if key == 'bronze':
            for path in value:
                df = spark.read.format('csv').load(path, header=True)
                list_df.append(df)
                if len(list_df) == 3:
                    print('>>> Dataframes have been read!')
                    return list_df
        else:
            break


def address_converter(row: any, api_key: str) -> any:
    """
    This function uses an external API from Google
    receiving as parameter a row from a dataframe,
    takes the columns 'latitude' and 'longitude',
    then calls out Google Maps to receive the exact
    address from the values given. This function is
    used in the method 'transform' from the class
    'WebScrape'. Costs may vary depending on the
    length of the rows in the file.

    :param row: A row from a 'pandas' dataframe.
    :param api_key: A string that represents the key
    for Google Maps usage, will be added automatically.
    """

    gmaps = googlemaps.Client(key=api_key)
    if all(x is not None for x in [row['latitude'], row['longitude']]):
        reverse_geocode_result = gmaps.reverse_geocode([row['latitude'], row['longitude']],
                                                       location_type='RANGE_INTERPOLATED')
        if len(reverse_geocode_result) > 0:
            dict_address = reverse_geocode_result[0]
            address = dict_address['formatted_address']
            return address
        else:
            address = None
            return address


def take_zipcode(row: any) -> str | None:
    """
    This function takes the column 'address' from the
    dataframe and returns the zip code. This function is
    used in the method 'transform' from the class 'WebScrape'.

    :param row: A row from a 'pandas' dataframe.
    """

    if row['address'] is not None:
        try:
            pattern = re.compile(r'CA\s(\d\d\d\d\d)')
            postal_code = pattern.findall(row['address'])
            return postal_code[0]
        except IndexError:
            return None
    else:
        return None


def add_metadata(df_list: SparkDataFrame, df_crime: SparkDataFrame, df_comme: SparkDataFrame) -> tuple[SparkDataFrame,
                                                                                        SparkDataFrame, SparkDataFrame]:
    """
    This function adds metadata to the columns
    of the dataframes, will be used inside 'spark_ops'.

    :param df_list: A 'Spark' dataframe.
    :param df_crime: A 'Spark' dataframe.
    :param df_comme: A 'Spark' dataframe.
    """

    # METADATA FOR LISTINGS DATAFRAME.
    metadata_df_list = df_list.withMetadata('id', {'comment': 'Airbnb listing reference number.'}) \
    .withMetadata('description', {'comment': 'Characteristics about the listing.'}) \
    .withMetadata('latitude', {'comment': 'Is a measurement of a location north or south of the Equator.'}) \
    .withMetadata('longitude', {'comment': 'Is a measurement of location east or west of the prime meridian at '
                                           'Greenwich.'}) \
    .withMetadata('room_type', {'comment': 'Description of how many rooms does the airbnb includes.'}) \
    .withMetadata('price', {'comment': 'Price tag per night for the airbnb.'}) \
    .withMetadata('listing_address', {'comment': 'Place where the airbnb is situated.'}) \
    .withMetadata('zip_code', {'comment': 'A number that identifies a particular area in a city.'})

    # METADATA FOR CRIMES DATAFRAME.
    metadata_df_crimes = df_crime.withMetadata('id_crime', {'comment': 'Crime or incident reference number.'}) \
        .withMetadata('occur_timestamp', {'comment': 'Date when the crime or incident was committed.'}) \
        .withMetadata('crime_code', {'comment': 'Number that classifies the crime or incident.'}) \
        .withMetadata('crime_description', {'comment': 'Brief meaning or type of the crime or incident.'}) \
        .withMetadata('vict_age', {'comment': 'Victims age.'}) \
        .withMetadata('vict_sex', {'comment': 'Victims gender.'}) \
        .withMetadata('latitude', {'comment': 'Is a measurement of a location north or south of the Equator.'}) \
        .withMetadata('longitude', {'comment': 'Is a measurement of location east or west of the prime '
                                                                                        'meridian at Greenwich.'}) \
        .withMetadata('crime_address', {'comment': 'Location where the crime or incident took place.'}) \
        .withMetadata('zip_code', {'comment': 'A number that identifies a particular area in a city.'}) \
        .withMetadata('previous_date', {'comment': 'Previous (occur_timestamp) date when a crime was committed'}) \
        .withMetadata('diff_days', {'comment': 'Spend time to wait since the last crime was commited'
                                                                        'for the next one to be reported.'}) \
        .withMetadata('mtbc_in_days', {'comment': 'Mean time between crimes. Average of (diff_days)'
                                                                            'for each zip_code.'})

    # METADATA FOR COMMENTS DATAFRAME.
    metadata_df_comm = df_comme.withMetadata('listing_id', {'comment': 'Airbnb listing reference number.'}) \
        .withMetadata('all_comments', {'comment': 'Customer point of view about the airbnb.'})

    return metadata_df_list, metadata_df_crimes, metadata_df_comm


def spark_ops(listing: PandasDataFrame, crime: PandasDataFrame, comment: PandasDataFrame) -> tuple[SparkDataFrame,
                                                                                        SparkDataFrame, SparkDataFrame]:
    """
    This function takes all the 'pandas' dataframes and
    converts them into spark dataframes, then it does
    transformations using pyspark functions. This function
    is used in the method 'transform' from the class 'WebScrape'.

    :param listing: A 'Pandas' dataframe.
    :param crime: A 'Pandas' dataframe.
    :param comment: A 'Pandas' dataframe.
    """

    listing_df = spark.createDataFrame(listing).withColumnRenamed('address', 'listing_address')
    crime_df = spark.createDataFrame(crime).withColumnRenamed('address', 'crime_address')
    comment_df = spark.createDataFrame(comment)

    # SPARK TRANSFORMATION FOR CRIMES DF.

    last_crime_df = crime_df.withColumn('ocurr_time',
                                        F.concat(F.substring(F.col('time_occur'), pos=0, len=2),
                                                 F.lit(':'),
                                                 F.substring(F.col('time_occur'), pos=3, len=2)
                                                 )) \
        .withColumn('take_date', F.substring(F.col('date_occur'), pos=0, len=10)) \
        .withColumn('date_occur', F.col('date_occur').cast('string')) \
        .withColumn('occur_timestamp', F.concat_ws(' ', F.col('take_date'), F.col('ocurr_time'))) \
        .withColumn('occur_timestamp', F.to_timestamp(F.col('occur_timestamp'))) \
        .drop('date_occur', 'time_occur', 'ocurr_time', 'take_date', 'Unnamed: 0', '_c0')

    last_crime_ = last_crime_df.filter(last_crime_df.zip_code.isNotNull()) \
                                .filter(last_crime_df.vict_sex != 'None')

    w1 = Window.partitionBy('zip_code').orderBy('occur_timestamp')
    w2 = Window.partitionBy('zip_code')

    r = last_crime_.withColumn('previous_date', F.lag(F.col('occur_timestamp')).over(w1))

    diff_crimes = r.withColumn('diff_seconds',
                        F.unix_timestamp(F.col('occur_timestamp')) - F.unix_timestamp(F.col('previous_date'))) \
        .withColumn('diff_days', F.round(F.col('diff_seconds') / 86400, 0)) \
        .withColumn('mtbc_in_days', F.round(F.avg(F.col('diff_days')).over(w2), 0)) \
        .drop('diff_seconds')

    # SPARK TRANSFORMATION FOR LISTINGS DF.
    filter_listing_df = listing_df.filter(listing_df.zip_code.isNotNull())

    # SPARK TRANSFORMATION FOR COMMENTS DF.
    all_comments = comment_df.groupBy(F.col('listing_id')).agg(F.collect_list('comments').alias('all_comments'))
    filter_df = all_comments.filter(F.col('listing_id').cast('int').isNotNull())

    # CALLING FUNCTION TO ADD METADATA.
    df1, df2, df3 = add_metadata(filter_listing_df, diff_crimes, filter_df)

    return df1, df2, df3
