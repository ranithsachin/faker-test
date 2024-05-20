import logging

import conf.configs as config
from src.read_write_data import ReadWriteData

def check_item_attribute_uniqueness()->None:
    """
        Check whether the item ean has the same department and price value for all records
        
    """
    try:
        items_table: str = config.ITEMS_TABLE_LOCATION
        rw_data: ReadWriteData = ReadWriteData()
        status: bool = rw_data.compare_item_uniqueness(items_table)
        if(status):
            print("All item EAN confirms to the standards")
        else:
            print("Differences found!!")
    except Exception as e:
        raise e
    
def compare_item_quantities()->None:
    """
        Compare the item quantities between products & credit_card trans
    """
    try:
        product_trans_table: str = config.PROD_TRANS_TABLE_LOCATION
        cc_trans_table: str = config.CREDIT_CARD_TRANS_TABLE_LOCATION
        rw_data: ReadWriteData = ReadWriteData()
        status: bool = rw_data.compare_item_quantities(product_trans_table,cc_trans_table)

        if(status):
            print("All item quantities are equal")
        else:
            print("Item quantities not equal")

    except Exception as e:
        raise e
    
def compare_products_creditcard_trans_value()->None:
    """
        Compare products & credit card transaction values
    """
    try:
        product_trans_table: str = config.PROD_TRANS_TABLE_LOCATION
        cc_trans_table: str = config.CREDIT_CARD_TRANS_TABLE_LOCATION
        rw_data: ReadWriteData = ReadWriteData()
        status: bool = rw_data.compare_products_creditcard_trans_value(product_trans_table,cc_trans_table)

        if(status):
            print("All transaction values are equal")
        else:
            print("Transaction values are not equal")
    except Exception as e:
        raise e
    
def check_item_quantities_values_per_customer()->None:
    """
        Generating a dataframe of total quantity and total value purchased by each customer per month per year
    """
    try:
         cc_trans_table: str = config.CREDIT_CARD_TRANS_TABLE_LOCATION
         rw_data: ReadWriteData = ReadWriteData()
         df = rw_data.check_item_quantities_values_per_customer(cc_trans_table)

         df.show()
    except Exception as e:
        raise e
    
def sales_per_credit_card_provider()->None:
    """
        Generating a dataframe with sales that happened between noon and midnight per credit card provider
    """
    try:
         cc_trans_table: str = config.CREDIT_CARD_TRANS_TABLE_LOCATION
         customer_table: str = config.CUSTOMERS_TABLE_LOCATION
         rw_data: ReadWriteData = ReadWriteData()
         df = rw_data.sales_per_credit_card_provider(cc_trans_table,customer_table)

         df.show()
    except Exception as e:
        raise e
    
def top_10_customers_per_year_month()->None:
    """
        Generating top 10 customers by sales value per month per year
    """
    try:
        cc_trans_table: str = config.CREDIT_CARD_TRANS_TABLE_LOCATION
        rw_data: ReadWriteData = ReadWriteData()
        df = rw_data.top_10_customers_per_year_month(cc_trans_table)

        df.show()
    except Exception as e:
        raise e    
    
def top_items_per_quarter(year:int)->None:
    """
        Generating the top items(quantity and value) per quarter of the given year
        Arguments:
            year: the year in consideration
    """
    try:
        product_trans_table: str = config.PROD_TRANS_TABLE_LOCATION
        cc_trans_table: str = config.CREDIT_CARD_TRANS_TABLE_LOCATION
        rw_data: ReadWriteData = ReadWriteData()
        df = rw_data.top_items_per_quarter(year,product_trans_table,cc_trans_table)

        df.show()

    except Exception as e:
        raise e  
    
def top_50_customers_per_sales_value()->None:
    """
        Generating the customer list with the top 50 most expensive purchases
    """
    try:
        cc_trans_table: str = config.CREDIT_CARD_TRANS_TABLE_LOCATION
        customer_table: str = config.CUSTOMERS_TABLE_LOCATION
        rw_data: ReadWriteData = ReadWriteData()
        df = rw_data.top_fifty_expensive_purchases(cc_trans_table, customer_table)

        df.show()
        
    except Exception as e:
        raise e
    
def top_50_customers_most_items_department()->None:
    """
        Generating the top 10 department from which most items were bought for the first 50 customers
    """
    try:
        cc_trans_table: str = config.CREDIT_CARD_TRANS_TABLE_LOCATION
        customer_table: str = config.CUSTOMERS_TABLE_LOCATION
        rw_data: ReadWriteData = ReadWriteData()
        product_trans_table: str = config.PROD_TRANS_TABLE_LOCATION

        df = rw_data.top_50_customers_most_items_department(cc_trans_table,customer_table,product_trans_table)

        df.show()
    except Exception as e:
        raise e

