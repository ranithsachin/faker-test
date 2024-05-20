from src.customers import Customer
from src.items import Item
from src.transactions import Transaction
import src.queries as query

import os
import sys

def main():

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # cust = Customer()
    # cust.generate_customer_data()

    # it = Item()
    # it.generate_items()
    # print(it.read_items())

    # trans = Transaction()
    # trans.generate_transaction()
    #transaction.read_cc_trans()

    #query.check_item_attribute_uniqueness()
    #query.compare_item_quantities()
    #query.compare_products_creditcard_trans_value()
    # query.check_item_quantities_values_per_customer()
    # query.sales_per_credit_card_provider()
    # query.top_10_customers_per_year_month()
    # query.top_items_per_quarter(2022)
    # query.top_50_customers_per_sales_value()
    query.top_50_customers_most_items_department()

if __name__ == "__main__":
    main()

