import logging
import pandas as pd

from src.departments import Department
import conf.configs as config
import src.utils as util
import conf.df_schema as schema
from src.read_write_data import ReadWriteData

from faker import Faker
from pyspark.sql import DataFrame


class Item:
    """
        Class handling item entity tasks
    """

    def __init__(self)->None:
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(filename=config.APP_LOGING_PATH,encoding="utf-8",level=config.APP_LOGING_LEVEL)
        self.rw_data = ReadWriteData()
       
    def generate_items(self, no_of_items: int=config.NO_OF_ITEMS)->None:
        """
            Generate and save item data to the parquet table
            Arguments:
                no_of_items: number of items to generate, configured in the config file
        """
        try: 
            self.logger.info("Start generating items")
            fake = Faker()
            dept = Department()
            departments = dept.generate_departments()
            spark = util.get_spark_session()

            empty_rdd = spark.sparkContext.emptyRDD()
            items_df = spark.createDataFrame(empty_rdd,schema.item_schema)

            for i in range(no_of_items):
                Faker.seed(i)
                item_value = fake.pyfloat(min_value=0,max_value=10000)
                item_ean = fake.ean()
                item_dept = fake.word(ext_word_list=departments)
                item_id = fake.unique.random_int(0,300)

                item_row = spark.createDataFrame([(item_id,item_ean,item_dept,item_value)],schema.item_schema)
                items_df = items_df.union(item_row)
                Faker.seed()
        
            self.logger.info("Writing items data to parquet table")

            self.rw_data.write_data_parquet(items_df,config.ITEMS_TABLE_LOCATION,"overwrite")
        except Exception as e:
            self.logger.error(f"item data generation error encountered")
            raise Exception(e)
       
    def read_items(self)->DataFrame:
        """
            Read item data parquet table and return as a dataframe

            Returns:
                Dataframe
        """
        df = self.rw_data.read_data_parquet(config.ITEMS_TABLE_LOCATION)
        return df

        