# do all imports and installs here
import pandas as pd
import logging
logging.getLogger().setLevel(logging.INFO)
import datetime
import pyspark.sql.types as T
import pyspark.sql.functions as F
import pyspark.sql.dataframe as psd
import pyspark.sql.session as pss
import re
from utils.city_code import city_code
import os
from typing import List
    
    
def filter_immigration_data(immigration_df: psd.DataFrame) -> psd.DataFrame:
    """[Select the needed columns from the migration table. These columns are chosen based on the analytical needs.]

    Args:
        immigration_df (psd.DataFrame): [Spark dataframe including columns needed]

    Returns:
        psd.DataFrame: [Spark dataframe including needed columns that describe immigration movement]
    """
    # select needed columns
    try:
        immigration_usa_df = immigration_df.select(['i94yr', 'i94mon', 'i94addr', 'i94port'])
    except Exception as e:
        logging.info("Selection needed columns failed: {e}")
    return immigration_usa_df


def get_valid_ports() -> dict:
    """[Extract valid port codes from raw port values]

    Returns:
        dict: [dictionary including valid port as key, and its description as key value]
    """
    # Extract valid ports into a dictionary
    re_obj = re.compile("'(.*)'\s=\s'(.*)'") # '(\S{3})'\s=\s'(.*),\s{1}(\S{2}).*
    validPorts = {}
    with open('utils/port_code.py') as f:
         for data in f:
             match = re_obj.search(data)
             if match:
                 validPorts[match[1]] = match[2].strip()
    return validPorts


def clean_immigration_data(validPorts: dict, immigration_usa_df: psd.DataFrame, spark: pss.DataFrame) -> psd.DataFrame:
    """[This cleans immigration data in USA. It casts date of immigrant entry, city of destination, and port entry.]

    Args:
        validPorts (dict): [dictionery that includes valid entry ports in USA]
        immigration_usa_df (psd.DataFrame): [Spark dataframe that includes immigration data in USA]
        spark (pss.DataFrame): [Spark session that is used for executing queries]

    Returns:
        psd.DataFrame: [spark dataframe that includes clean data of immigration movement in usa]
    """
    # cast cities that are valid
    valid_city_code = list(set(city_code.keys()))
    str_valid_city_code = str(valid_city_code).replace('[', '(').replace(']', ')')

    # cast ports that are valid
    valid_port_code = list(set(validPorts.keys()))
    str_valid_port_code = str(valid_port_code).replace('[', '(').replace(']', ')')

    # clean immigration data 
    clean_immigration_usa_df = spark.sql(f'''
    select date(concat(cast(i94yr as int), "-", cast(i94mon as int), "-01")) as dt, cast(i94addr as varchar(2)), cast(i94port as varchar(3))
    from immigration_usa_table
    where i94yr is not null and i94mon is not null and i94addr is not null and i94port is not null and
    i94addr in {str_valid_city_code} and i94port in {str_valid_port_code} 
    ''')
    
    return clean_immigration_usa_df


def filter_temperature_data(temperature_df: psd.DataFrame) -> psd.DataFrame:
    """[Select the needed columns from the temperature table. These columns are chosen based on the analytical needs.
        It restricts data down to April, 2013.]

    Args:
        temperature_df (psd.DataFrame): [Spark dataframe including columns needed]

    Returns:
        psd.DataFrame: [Spark dataframe including needed columns that describe average temperature of US cities ]
    """
    # select only US data
    temperature_usa_df = temperature_df.where(temperature_df.Country == 'United States')
    
    # select needed columns
    temperature_usa_df = temperature_usa_df.select(['dt', 'Country', 'City', 'AverageTemperature'])

    # cast date from string
    temperature_usa_df = temperature_usa_df.withColumn('dt', F.to_date('dt', 'yyyy-MM-dd'))
    
    # select data for April, 2013
    temperature_usa_df = temperature_usa_df.where((temperature_usa_df['dt'] >= '2013-04-01') & (temperature_usa_df['dt'] < '2013-05-01'))
    
    return temperature_usa_df
 
    
def add_port_to_temperature_data(temperature_usa_df: psd.DataFrame, validPorts: dict) -> psd.DataFrame:
    """[This adds valid ports to input dataframe]

    Args:
        temperature_usa_df (psd.DataFrame): [Spark dataframe that includes temperature data of usa cities]
        validPorts (dict): [dictionary that includes valid entry ports in usa]

    Returns:
        psd.DataFrame: [sparf dataframe]
    """
    # register a function for extracting data lines with valid ports
    @F.udf
    def get_port(city):
        for key in validPorts:
            if city.lower() in validPorts[key].lower():
                return key

    # add entry ports data to temperature data
    temperature_usa_df = temperature_usa_df.withColumn('i94port', get_port(temperature_usa_df['City']))
    
    return temperature_usa_df
    
    
def clean_temerature_usa_data(temperature_usa_df: psd.DataFrame, spark: pss.SparkSession) -> psd.DataFrame:
    """[This cleans temperature data in USA. It casts date, city, average recorded temperature, and port entry.]

    Args:
        temperature_usa_df (psd.DataFrame): [spark dataframe that describes average temperature of usa cities]
        spark (pss.SparkSession): [Spark session that is used for executing queries]

    Returns:
        psd.DataFrame: [spark dataframe]
    """
    # clean the temperature data
    clean_temperature_usa_df = spark.sql('''
    select date(dt) as dt, cast(Country as varchar(13)), cast(City as string), round(AverageTemperature, 2) as AverageTemperature, cast(i94port as varchar(3))
    from temperature_usa_table
    where dt is not null and Country is not null and City is not null and  AverageTemperature is not null and i94port is not null
    ''')
    
    return clean_temperature_usa_df


def query_create_fact_table() -> str:
    """[this returns the query used for creating fact table. It uses the immigration and temperature tables' view.]

    Returns:
        str: [query to extract fact table.]
    """
    # defining query for creating fact table by joining temperature and immigration data
    query = '''
        select  im.dt                  AS dt,
                im.i94port             AS i94port,
                t.AverageTemperature   AS AverageTemperature
        from clean_immigration_usa_table AS im
        LEFT JOIN clean_temperature_usa_table AS t 
        ON im.i94port = t.i94port and t.dt = im.dt
    '''
    
    return query


def check_exist_rows(tables: List[str], spark: pss.SparkSession):
    """[tests if tables are not empty]

    Args:
        tables (List[str]): [list of tables to be tested]
        spark (pss.SparkSession): [spark session to execute queries.]

    Raises:
        ValueError: [in cases of empty table]
    """
    # checking number of entries for a given table
    for table in tables:
        logging.info(f'Running data quality check on table {table}')

        logging.info(f'Getting number of entries in table {table}')
        records = spark.sql(f'''select count(*) as entries from {table}''').toPandas().entries.tolist()
        logging.info(f'Table {table} has {records} numbers of entries.')

        if not records or len(records) < 1 or records[0] < 1:
            self.log.error(f"Data quality check failed for table {table}")
            raise ValueError(f"Data quality check failed for table {table}")

        logging.info(f"Data quality check passed for table {table}")
        

def check_tables_integrity(fact_table: str, dimmention_table: str, spark: pss.SparkSession):
    """[check if entries of tables are equal to expected numbers]

    Args:
        fact_table (str): [name of fact table]
        dimmention_table (str): [name of dimention table]
        spark (pss.SparkSession): [spark session to execute queries]

    Raises:
        ValueError: [in case of invalid number of entries in a given table]
    """
    # checking number of entries for a given table
    logging.info(f'Running data integrity check on fact and dimmention tables')

    logging.info(f'Getting number of entries in table {dimmention_table}')
    records_dimention = spark.sql(f'''select count(*) as entries from {dimmention_table}''').toPandas().entries.tolist()
    logging.info(f'Table {dimmention_table} has {records_dimention} numbers of entries.')

    logging.info(f'Getting number of entries in table {fact_table}')
    records_fact = spark.sql(f'''select count(*) as entries from {fact_table}''').toPandas().entries.tolist()
    logging.info(f'Table {fact_table} has {records_fact} numbers of entries.')

    if not records_dimention[0] == records_fact[0]:
        logging.error(f"Data integrity check failed for tables {fact_table} and {dimmention_table}")
        raise ValueError(f"Data integrity check failed for tables {fact_table} and {dimmention_table}")

    logging.info(f"Data integrity check passed for tables {fact_table} and {dimmention_table}")

def check_constrain_integrity(fact_table: str, spark: pss.SparkSession):
    """[checks if the key columns in a given table include null values]

    Args:
        fact_table (str): [name of fact table]
        spark (pss.SparkSession): [spark session to execute queries]

    Raises:
        ValueError: [if a given table's column includes null value]
    """
    # checking number of null entries for fact table
    logging.info(f'Running data constrain check on fact table')

    logging.info(f'Getting number of null values in table {fact_table}')
    records_fact = spark.sql(f'''select count(*) as null_vals from {fact_table}
                            where dt is null or i94port is null
                            ''').toPandas().null_vals.tolist()

    if not records_fact[0] == 0:
        logging.error(f"Constrain integrity check failed for tables {fact_table}, it has {records_fact} number of null rows!")
        raise ValueError(f"Constrain integrity check failed for tables {fact_table}")

    logging.info(f"Constrain integrity check passed for tables {fact_table}")