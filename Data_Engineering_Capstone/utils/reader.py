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
from pyspark.sql import SparkSession
from utils.city_code import city_code
import os
from typing import List

class Reader:
    """[Reader class loads raw data for further usage. It is written with Singleton pattern to prevent loading data
    multiple time, as the process is very slow.]

    Returns:
        [type]: [the instance of class]
    """
    _instance = None
    
    def __init__(self, *args, **kwargs):
        pass
    
    def __new__(cls, *args, **kwargs):
        """[initializes a new instance of class if not existed. It initializes parameters, creates a spark session, loads data]

        Returns:
            [type]: [paths, spark session, and data]
        """
        if not cls._instance:
            # create inatance
            spark_reader = super(Reader, cls).__new__(cls, *args, **kwargs)
            # fit parameters and data
            spark_reader.fit()
            cls._instance = spark_reader
        
        return cls._instance
        
    def parameters(self) -> str:
        """[initializes necessary parameters]

        Returns:
            [str]: [paths to read data]
        """
        # data sources
        paths = {
                "i94immigration" : "./datasources/i94immigration",
                "worldtemperature" : "./datasources/worldtemperature"
                }
        return paths
        
    def create_spark_session(self) -> pss.SparkSession: 
        """[initialize a spark session]

        Returns:
            [pyspark.sql.session.SparkSession]: [created spark session]
        """
        # build spark session
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
            .enableHiveSupport() \
            .getOrCreate()

        return spark

    def from_anyformat_to_parquet(self):
        # Read raw data and load them into parquet files only if not already done
        if not os.path.exists(self.paths['i94immigration']):
            logging.info("start transfering i94 immigration raw data")
            df_immigration_spark = self.spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
            df_immigration_spark.write.parquet(self.paths['i94immigration'])
            logging.info("i94 immigration raw data are written into parquet files")
        else:
            logging.info("i94 immigration raw data are are already written into parquet files")
            
        if not os.path.exists(self.paths['worldtemperature']):
            logging.info("start transfering world temperature raw data")
            df_worldtemperature_spark =self.spark.read.csv('../../data2/GlobalLandTemperaturesByCity.csv', header=True)
            df_worldtemperature_spark.write.parquet(self.paths['worldtemperature'])
            logging.info("world temperature raw data are written into parquet files")
        else:
            logging.info("world temperature raw data are are already written into parquet files")
            
    
    def load_i94immigration_data(self) -> psd.DataFrame:
        """[loads i94 immigration data]

        Returns:
            [pyspark.sql.dataframe.DataFrame]: [i94 immigration data as spark dataframe]
        """
        # read immigration data
        immigration_df = self.spark.read.parquet(self.paths['i94immigration'])

        return immigration_df


    def load_worldtemperature_data(self) -> psd.DataFrame:
        """[loads world temperature data]

        Returns:
            [pyspark.sql.dataframe.DataFrame]: [world temperature data as spark dataframe]
        """
        # read world temprature data
        temperature_df = self.spark.read.parquet(self.paths['worldtemperature'])

        return temperature_df
    
    def fit(self):
        """[fits necessary parameters, creats spark session, and loads i94immigration and worldtemperature data.]

        Returns:
            [type]: []
        """

        logging.info("geting root paths to read and write data")
        self.paths = self.parameters()
        
        logging.info("getting spark session")
        self.spark = self.create_spark_session()
        
        logging.info("stage raw data into parquet files if not already exist!!!")
        self.from_anyformat_to_parquet()
        
        logging.info("Start fitting i94immigration data to reader class ...")
        self.immigration_df = self.load_i94immigration_data()
        logging.info("End fitting i94immigration data to reader class!")
        
        logging.info("Start fitting worldtemperature data to reader class ...")
        self.temperature_df = self.load_worldtemperature_data()
        logging.info("End fitting worldtemperature data to reader class!")
        
        return True
