import logging

import conf.df_schema as schema
import conf.configs as config
from src.read_write_data import ReadWriteData
import src.utils as utils

from faker import Faker
from pyspark.sql import DataFrame

class Customer:
    """
        Class handling customer entity tasks
    """

    def __init__(self)->None:
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(filename=config.APP_LOGING_PATH,encoding="utf-8",level=config.APP_LOGING_LEVEL)
        self.rw_data = ReadWriteData()

    def id_generator(self,i: int)->str:
        """
            Generate customer ID based on the integer passed
            Arguments:
                i: integer to base the ID on
            Returns:
                generated string ID
        """
        try:
            alpha_dict = {0:'A',1:'B',2:'C',3:'D',4:'E',5:'F',6:'F',7:'G',8:'H',9:'I'}
            value = str(i).rjust(8,'0')
            id_char = alpha_dict.get(int(value[0]))+alpha_dict.get(int(value[1]))+alpha_dict.get(int(value[2]))+alpha_dict.get(int(value[3]))
            id_num = value[-4:]
            id = id_char+id_num
            return id
        except Exception as e:
            self.logger.error(f"id creation error encountered {e}")
            raise e
    
    def generate_customer_data(self,no_of_customers: int=config.NO_OF_CUSTOMERS)->None:
        """
            Generate and save customer data to the parquet table
            Arguments:
                no_of_customers: number of customers to generate, configured in the config file
        """
        try: 
            self.logger.info("Start generating customer data")
            fake = Faker()
            spark = utils.get_spark_session()
            customer_schema = schema.customer_schema
        
            empty_rdd = spark.sparkContext.emptyRDD()
            customers_df = spark.createDataFrame(empty_rdd,schema.customer_schema)
        
            for i in range(1,no_of_customers+1):
                customer_number = self.id_generator(i)
                new_row = spark.createDataFrame([(customer_number,fake.first_name(),fake.last_name(),fake.date_of_birth(),fake.ssn(),fake.street_name(),fake.building_number(),fake.city(),fake.state(),fake.country(),fake.zipcode(),fake.credit_card_number(),fake.credit_card_expire(),fake.credit_card_security_code(),fake.credit_card_provider())], customer_schema)
                customers_df = customers_df.union(new_row)
        
            self.logger.info("writing customer data to parquet table")
            self.rw_data.write_data_parquet(customers_df,config.CUSTOMERS_TABLE_LOCATION,"overwrite")
        except Exception as e:
            self.logger.error(f"customer data generation error encountered {e}")
            raise e
        
    def read_customer_data(self)->DataFrame:
        """
            Read customer data parquet table and return as a dataframe

            Returns:
                Dataframe
        """
        df = self.rw_data.read_data_parquet(config.CUSTOMERS_TABLE_LOCATION)
        return df

        

