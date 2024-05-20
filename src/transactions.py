import logging
import random
import datetime as dt

from src.items import Item
import conf.configs as config
import conf.df_schema as schema
from src.read_write_data import ReadWriteData
import src.utils as util

from faker import Faker
from pyspark.sql import DataFrame

class Transaction:
    """
        Class handling transaction entity tasks
    """

    def __init__(self)->None:
        self.logger = logging.getLogger(__name__)
        logging.basicConfig(filename=config.APP_LOGING_PATH,encoding="utf-8",level=config.APP_LOGING_LEVEL)
        self.rw_data = ReadWriteData()
    
    def generate_transaction(self)->None:
        """
            Generate creditcard and product transaction and save to parquet table
        """
        try: 
            self.logger.info("Start generating transactions")
            fake = Faker()
            trans_count = config.NO_OF_TRANSACTIONS
            it = Item()
            items_df = it.read_items()
            customer_number_list = self.rw_data.get_cust_number_list(config.CUSTOMERS_TABLE_LOCATION)
            spark = util.get_spark_session()
            creditcard_transaction_schema = schema.creditcard_transaction_schema
            product_transaction_schema = schema.product_transaction_schema

            empty_rdd = spark.sparkContext.emptyRDD()
            creditcard_df = spark.createDataFrame(empty_rdd,schema.creditcard_transaction_schema)

            product_df = spark.createDataFrame(empty_rdd,schema.product_transaction_schema)

            for i in range(trans_count):
                trans_id = fake.unique.random_int(min=20000, max=40000)
            
                item = items_df.sample(fraction=1.0, seed=i).limit(1).toPandas()
                item_quantity = fake.random_int(1,200)
                trans_value = float((item.iloc[0]['ITEM_VALUE'])*item_quantity)
                trans_date_time = fake.date_time_between(dt.date(2022,1,1), dt.date(2022,12,31))

                rand_idx = random.randrange(len(customer_number_list))
                cust_number = customer_number_list[rand_idx]

                creditcard_row = spark.createDataFrame([(trans_id,cust_number,trans_value,trans_date_time,item_quantity)],creditcard_transaction_schema)
                cc_json_array = creditcard_row.toJSON().collect()
                # print(cc_json_array)
                # util.kafka_producer(cc_json_array)
                
                creditcard_df = creditcard_df.union(creditcard_row)

                product_row = spark.createDataFrame([(trans_id,(item.iloc[0]['ITEM_EAN']),(item.iloc[0]['ITEM_DEPARTMENT']),float(item.iloc[0]['ITEM_VALUE']),int(item.iloc[0]['ITEM_ID']),item_quantity)],product_transaction_schema)
                product_df = product_df.union(product_row)
        
            self.logger.info("Writing transactions data to parquet table")
            self.rw_data.write_data_parquet(creditcard_df,config.CREDIT_CARD_TRANS_TABLE_LOCATION,"overwrite")
            self.rw_data.write_data_parquet(product_df,config.PROD_TRANS_TABLE_LOCATION,"overwrite")
        except Exception as e:
            self.logger.error(f"transactions data generation error encountered")
            raise Exception(e) 

    def read_cc_trans(self)->DataFrame:
        """
            Read creditcard transactions data parquet table and return as a dataframe

            Returns:
                Dataframe
        """
        df = self.rw_data.read_data_parquet(config.CREDIT_CARD_TRANS_TABLE_LOCATION)
        return df
    
    def read_product_trans(self)->DataFrame:
        """
            Read product transactions data parquet table and return as a dataframe

            Returns:
                Dataframe
        """
        df = self.rw_data.read_data_parquet(config.PROD_TRANS_TABLE_LOCATION)
        return df
