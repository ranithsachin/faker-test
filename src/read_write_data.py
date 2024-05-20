import logging

import src.utils as util
import conf.configs as config

from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import col, year, month, sum, rank, hour, quarter
from pyspark.sql.window import Window

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

    def read_data_parquet(self,location: str)->DataFrame:
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
    
    def get_cust_number_list(self,location: str)->list:
        """
            Reads customer list and return as a list
            Arguments:
                location: Customers table path
            Returns:
                customer numbers list
        """
        try: 
            cust_no_list = self.spark.read.parquet(location).select(col("CUSTOMER_NUMBER")).rdd.flatMap(lambda x:x).collect()
            return cust_no_list
        except Exception as e:
            self.logger.error(f"Error when reading for customer numbers list {location}")
            raise Exception(e)
        
    def compare_item_uniqueness(self,location: str)->bool:
        """
            Check whether the item ean has the same department and price value in the product transactions table
            Arguments:
                location: Product transactions table path
            Returns:
                bool: True or False
        """
        try:
            ean_distinct_count=self.spark.read.parquet(location).select(col("ITEM_EAN")).distinct().count()
            ean_dept_value_distinct_count=self.spark.read.parquet(location).select(col("ITEM_EAN"),col("ITEM_VALUE"),col("ITEM_DEPARTMENT")).distinct().count()

            if(ean_distinct_count==ean_dept_value_distinct_count):
                return True
        except Exception as e:
            self.logger.error(f"Error when reading prod transactions {location}")
            raise e
        
        return False
    
    def compare_item_quantities(self,prod_trans: str,cc_trans: str)->bool:
        """
            Compare the item quantities between products & credit_card trans
            Arguments:
                prod_trans: Product transactions table path
                cc_trans: Credit card transactions table path
            Returns:
                True or False based on the condition
        """
        try:
            products_df: DataFrame = self.spark.read.parquet(prod_trans).select(col("TRANSACTION_ID").alias('PRODUCT_TRANSACTION_ID'),col("ITEM_QUANTITY")).orderBy("TRANSACTION_ID")
            creditcard_df: DataFrame = self.spark.read.parquet(cc_trans).select(col("TRANSACTION_ID").alias('CREDITCARD_TRANSACTION_ID'),"NUMBER_OF_ITEMS").orderBy("TRANSACTION_ID")
            item_quantities_count = products_df.join(creditcard_df, products_df.PRODUCT_TRANSACTION_ID==creditcard_df.CREDITCARD_TRANSACTION_ID, "inner").select(products_df.PRODUCT_TRANSACTION_ID, products_df.ITEM_QUANTITY, creditcard_df.NUMBER_OF_ITEMS).filter(col("ITEM_QUANTITY")!=col("NUMBER_OF_ITEMS")).count()

            if(item_quantities_count==0):
                return True
            else:
                return False
            
        except Exception as e:
            raise e
        
    def compare_products_creditcard_trans_value(self,prod_trans: str,cc_trans: str)->bool:
        """
            Compare the transaction values between products & credit_card trans
            Arguments:
                prod_trans: Product transactions table path
                cc_trans: Credit card transactions table path
            Returns:
                True or False based on the condition
        """
        try:
            products_df = self.spark.read.parquet(prod_trans).select(col("TRANSACTION_ID").alias('PRODUCT_TRANSACTION_ID'),(col("ITEM_QUANTITY")*col("ITEM_VALUE")).alias("TOTAL_VALUE")).orderBy("TRANSACTION_ID")
            creditcard_df = self.spark.read.parquet(cc_trans).select(col("TRANSACTION_ID").alias('CREDITCARD_TRANSACTION_ID'),"TRANSACTION_VALUE").orderBy("TRANSACTION_ID")

            totals_compared = products_df.join(creditcard_df, products_df.PRODUCT_TRANSACTION_ID==creditcard_df.CREDITCARD_TRANSACTION_ID, "inner").select(products_df.PRODUCT_TRANSACTION_ID, products_df.TOTAL_VALUE, creditcard_df.TRANSACTION_VALUE).filter(col("TOTAL_VALUE")!=col("TRANSACTION_VALUE")).count()
        
            if(totals_compared==0):
                return True
            else:
                return False
        except Exception as e:
            raise e
        
    def check_item_quantities_values_per_customer(self,cc_trans: str)->DataFrame:
        """
            Generating total quantity and total value purchased by each customer per month per year
            Arguments:
                cc_trans: Credit card transactions table path
            Returns:
                Generated dataframe
        """
        try:
            customers_df = self.spark.read.parquet(cc_trans).select(col("CUSTOMER_NUMBER").alias("CUSTOMER_ID"),year(col("TRANSACTION_DATE_TIME")).alias("YEAR"),month(col("TRANSACTION_DATE_TIME")).alias("MONTH"),col("NUMBER_OF_ITEMS"),col("TRANSACTION_VALUE"))

            grouped_df = customers_df.groupby("CUSTOMER_ID","YEAR","MONTH").agg(sum("NUMBER_OF_ITEMS").alias("TOTAL_NO_ITEMS"),sum("TRANSACTION_VALUE").alias("TOTAL_TRANS_VALUE")).orderBy("CUSTOMER_ID")

            return grouped_df

        except Exception as e:
            raise e
    
    def sales_per_credit_card_provider(self,cc_trans: str,customer: str)->DataFrame:
        """
            Generating sales that happened between noon and midnight per credit card provider
            Arguments:
                cc_trans: credit card transactions table path
                customer: customers table path 
            Returns:
                Generated dataframe 
        """
        try:
            cc_provider_data_df = self.spark.read.parquet(customer).select("CUSTOMER_NUMBER","CREDITCARD_PROVIDER")

            trans_data_df = self.spark.read.parquet(cc_trans).select("CUSTOMER_NUMBER","NUMBER_OF_ITEMS","TRANSACTION_VALUE").filter(hour(col("TRANSACTION_DATE_TIME"))>12).filter(hour(col("TRANSACTION_DATE_TIME"))<24)

            agg_data_df = cc_provider_data_df.join(trans_data_df,cc_provider_data_df.CUSTOMER_NUMBER==trans_data_df.CUSTOMER_NUMBER,"inner").groupBy("CREDITCARD_PROVIDER").agg(sum("NUMBER_OF_ITEMS").alias("TOTAL_ITEMS"),sum("TRANSACTION_VALUE").alias("TOTAL_TRANSACTION_VALUE")).select("CREDITCARD_PROVIDER","TOTAL_ITEMS","TOTAL_TRANSACTION_VALUE")

            return agg_data_df

        except Exception as e:
            raise e
        
    def top_10_customers_per_year_month(self, cc_trans: str)->DataFrame:
        """
            Generating top 10 customers by sales value per month per year
            Arguments:
                cc_trans: credit card transactions table path 
            Returns:
                Generated dataframe 
        """
        try:
            customers_df = self.spark.read.parquet(cc_trans).select(col("CUSTOMER_NUMBER").alias("CUSTOMER_ID"),year(col("TRANSACTION_DATE_TIME")).alias("YEAR"),month(col("TRANSACTION_DATE_TIME")).alias("MONTH"),col("NUMBER_OF_ITEMS"),col("TRANSACTION_VALUE"))

            cust_window = Window.partitionBy("YEAR").partitionBy("MONTH").orderBy(col("TRANSACTION_VALUE").desc())
            top_10_customers = customers_df.withColumn("rank", rank().over(cust_window)).filter(col("rank")<=10).orderBy("CUSTOMER_ID")

            return top_10_customers
        
        except Exception as e:
            raise e
        
    def top_items_per_quarter(self,year_val: int,prod_trans: str,cc_trans: str)->DataFrame:
        """
            Generating the top items(quantity and value) per quarter of the given year
            Arguments:
                year: the year in consideration
                prod_trans: product transactions table path
                cc_trans: credit card transactions table path 
            Returns:
                Generated dataframe 
        """
        try:
            df_window = Window.partitionBy(quarter("TRANSACTION_DATE_TIME")).orderBy(col("ITEM_VALUE").desc()).orderBy(col("ITEM_QUANTITY").desc())

            creditcard_df = self.spark.read.parquet(cc_trans).select(col("TRANSACTION_ID"),col("TRANSACTION_DATE_TIME")).filter(year("TRANSACTION_DATE_TIME")==year_val)
            product_df = self.spark.read.parquet(prod_trans).select(col("TRANSACTION_ID"),col("ITEM_EAN"),col("ITEM_VALUE"),col("ITEM_QUANTITY"))

            quarterly_items = product_df.join(creditcard_df, creditcard_df.TRANSACTION_ID==product_df.TRANSACTION_ID, "inner").select("ITEM_EAN","ITEM_VALUE","ITEM_QUANTITY","TRANSACTION_DATE_TIME")

            quarterly_items = quarterly_items.withColumn("RANK", rank().over(df_window)).filter(col("rank")<=10).select("ITEM_EAN","ITEM_QUANTITY","ITEM_VALUE",quarter("TRANSACTION_DATE_TIME").alias("QUARTER"),col("RANK")).orderBy("QUARTER","RANK")

            return quarterly_items

        except Exception as e:
            raise e
        
    def top_fifty_expensive_purchases(self,cc_trans: str,customer: str)->DataFrame:
        """
            Generating the customer list with the top 50 most expensive purchases
            Arguments:
                cc_trans: credit card transactions table path
                customer: customers table path
        """
        try:
            customers_data_df = self.spark.read.parquet(customer).select("CUSTOMER_NUMBER","FIRST_NAME","LAST_NAME")
            creditcard_data_df = self.spark.read.parquet(cc_trans).select("TRANSACTION_ID","CUSTOMER_NUMBER","TRANSACTION_VALUE",month(col("TRANSACTION_DATE_TIME")).alias("MONTH"))

            joined_df = customers_data_df.join(creditcard_data_df,customers_data_df.CUSTOMER_NUMBER==creditcard_data_df.CUSTOMER_NUMBER,"inner").select("TRANSACTION_ID",customers_data_df.CUSTOMER_NUMBER.alias("CUSTOMER_NUMBER"),"FIRST_NAME","LAST_NAME","TRANSACTION_VALUE","MONTH").orderBy("TRANSACTION_VALUE").limit(50)

            return joined_df
        except Exception as e:
            raise e
        
    def top_50_customers_most_items_department(self,cc_trans: str,customer: str,prod_trans: str)->DataFrame:
        """
            Generating the top 10 department from which most items were bought for the first 50 customers
            Arguments:
                cc_trans: credit card transactions table path
                customer: customers table path
                prod_trans: product transactions table path
        """
        try:
            customers_data_df = self.spark.read.parquet(customer).select("CUSTOMER_NUMBER","FIRST_NAME","LAST_NAME")
            creditcard_data_df = self.spark.read.parquet(cc_trans).select("TRANSACTION_ID","CUSTOMER_NUMBER","TRANSACTION_VALUE",month(col("TRANSACTION_DATE_TIME")).alias("MONTH"))

            joined_df = customers_data_df.join(creditcard_data_df,customers_data_df.CUSTOMER_NUMBER==creditcard_data_df.CUSTOMER_NUMBER,"inner").select("TRANSACTION_ID",customers_data_df.CUSTOMER_NUMBER.alias("CUSTOMER_NUMBER"),"FIRST_NAME","LAST_NAME","TRANSACTION_VALUE","MONTH").orderBy("TRANSACTION_VALUE").limit(50)

            products_df = self.spark.read.parquet(prod_trans).select("TRANSACTION_ID","ITEM_DEPARTMENT","ITEM_QUANTITY")

            new_joined_df = joined_df.join(products_df,joined_df.TRANSACTION_ID==products_df.TRANSACTION_ID,"inner").select("ITEM_DEPARTMENT","ITEM_QUANTITY").orderBy(col("ITEM_QUANTITY").desc()).limit(10)

            return new_joined_df
        
        except Exception as e:
            raise e



