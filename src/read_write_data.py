import logging

import src.utils as util
import conf.configs as config

from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import col



class ReadWriteData:
    """
        Class that handles all parquet reads and writes
    """

    def __init__(self)->None:
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(filename=config.APP_LOGING_PATH,encoding="utf-8",level=config.APP_LOGING_LEVEL)
        self.spark=util.get_spark_session()

    def write_data_parquet(self,dataframe: DataFrame,location: str,mode: str)->None:
       """
            Write the passed dataframe as parquet to disk
            Arguments:
                dataframe: passed dataframe
                location: location to write the data as parquet
                mode: mode to write the data
        """
       try: 
           dataframe.write.mode(mode).parquet(location)
       except Exception as e:
           self.logger.error(f"Error when writing to parquet table {location}")
           raise e 

    def read_data_parquet(self,location)->DataFrame:
        """
            Read parquet table as a dataframe and return
            Arguments:
                location: location to read the data from
            Returns:
                Results dataframe
        """
        try: 
            df = self.spark.read.parquet(location)
            return df
        except Exception as e:
            self.logger.error(f"Error when reading from parquet table {location}")
            raise e
    
    def get_cust_number_list(self,location)->list:
        """
            Reads customer list and return as a list
            Arguments:
                location: location to read the data from
            Returns:
                customer numbers list
        """
        try: 
            cust_no_list = self.spark.read.parquet(location).select(col("CUSTOMER_NUMBER")).rdd.flatMap(lambda x:x).collect()
            return cust_no_list
        except Exception as e:
            self.logger.error(f"Error when reading for customer numbers list {location}")
            raise Exception(e)

