from faker import Faker
from pyspark.sql.functions import col, year, month, sum, rank
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType,TimestampType
import random
import datetime as dt
import pandas as pd

def customer_id_generator(i):
    alpha_dict = {0:'A',1:'B',2:'C',3:'D',4:'E',5:'F',6:'F',7:'G',8:'H',9:'I'}
    value = str(i).rjust(8,'0')
    id_char = alpha_dict.get(int(value[0]))+alpha_dict.get(int(value[1]))+alpha_dict.get(int(value[2]))+alpha_dict.get(int(value[3]))
    id_num = value[-4:]
    id = id_char+id_num
    return id

def generate_customer_data():
    fake = Faker()

    customer_schema = StructType([StructField("CUSTOMER_NUMBER", StringType()),
                              StructField("FIRST_NAME", StringType()),
                              StructField("LAST_NAME", StringType()),
                              StructField("BIRTHDATE", DateType()),
                              StructField("SSN", StringType()),
                              StructField("CUSTOMER_ADDRESS_STREET", StringType()),
                              StructField("CUSTOMER_ADDRESS_HOUSE_NUMBER", StringType()),
                              StructField("CUSTOMER_ADDRESS_CITY", StringType()),
                              StructField("CUSTOMER_ADDRESS_STATE", StringType()),
                              StructField("CUSTOMER_ADDRESS_COUNTRY", StringType()),
                              StructField("CUSTOMER_ADDRESS_ZIP_CODE", StringType()),
                              StructField("CREDITCARD_NUMBER", StringType()),
                              StructField("CREDITCARD_EXPIRATION_DATE", StringType()),
                              StructField("CREDITCARD_VERIFICATION_CODE", StringType()),
                              StructField("CREDITCARD_PROVIDER", StringType())
                              ])
    
    new_df = spark.createDataFrame([],customer_schema)

    for i in range(1,100):
        customer_number = customer_id_generator(i)
        new_row = spark.createDataFrame([(customer_number,fake.first_name(),fake.last_name(),fake.date_of_birth(),fake.ssn(),fake.street_name(),fake.building_number(),fake.city(),fake.state(),fake.country(),fake.zipcode(),fake.credit_card_number(),fake.credit_card_expire(),fake.credit_card_security_code(),fake.credit_card_provider())], customer_schema)
        new_df = new_df.union(new_row)

    new_df.write.mode('append').saveAsTable('CUSTOMER_DATA')

def generate_items_data():
    fake = Faker()

    departments = []
    for i in range(50):
        Faker.seed(i)
        departments.append(fake.word())

    Faker.seed()
    items = []

    for i in range(300):
        Faker.seed(i)
        item_value = fake.pyfloat(min_value=0,max_value=10000)
        item_ean = fake.ean()
        item_dept = fake.word(ext_word_list=departments)
        item_id = fake.unique.random_int(0,300)

        item_row = {"ITEM_ID": item_id,"ITEM_EAN":item_ean,"ITEM_DEPARTMENT":item_dept,"ITEM_VALUE":item_value}

        items.append(item_row)
        Faker.seed()

    items_df = pd.DataFrame(items)
    return items_df

def generate_transactions_data():
    fake = Faker()

    creditcard_transaction_schema = StructType([StructField("TRANSACTION_ID", StringType()),
                                    StructField("CUSTOMER_NUMBER", StringType()),
                                    StructField("TRANSACTION_VALUE", FloatType()),
                                    StructField("TRANSACTION_DATE_TIME", TimestampType()),
                                    StructField("NUMBER_OF_ITEMS", IntegerType())])

    product_transaction_schema =    StructType([StructField("TRANSACTION_ID", StringType()),
                                    StructField("ITEM_EAN", StringType()),
                                    StructField("ITEM_DEPARTMENT", StringType()),
                                    StructField("ITEM_VALUE", FloatType()),
                                    StructField("ITEM_ID", StringType()),
                                    StructField("ITEM_QUANTITY", IntegerType())])
    
    creditcard_df = spark.createDataFrame([],creditcard_transaction_schema)
    product_df = spark.createDataFrame([],product_transaction_schema)

    customer_number_df = spark.read.table("CUSTOMER_DATA").select("CUSTOMER_NUMBER")
    customer_number_list = [row.CUSTOMER_NUMBER for row in customer_number_df.select('CUSTOMER_NUMBER').collect()]

    items_df = generate_items_data()

    for i in range(100):
        trans_id = fake.unique.random_int(min=20000, max=40000)
        item = items_df.sample()
        item_quantity = fake.random_int(1,200)
        trans_value = float((item.get('ITEM_VALUE').values[0])*item_quantity)

        trans_date_time = fake.date_time_between(dt.date(2022,1,1), dt.date(2022,12,31))

        rand_idx = random.randrange(len(customer_number_list))
        cust_number = customer_number_list[rand_idx]

        creditcard_row = spark.createDataFrame([(trans_id,cust_number,trans_value,trans_date_time,item_quantity)],creditcard_transaction_schema)
        creditcard_df = creditcard_df.union(creditcard_row)

        product_row = spark.createDataFrame([(trans_id,(item.get('ITEM_EAN').values[0]),(item.get('ITEM_DEPARTMENT').values[0]),float(item.get('ITEM_VALUE').values[0]),(item.get('ITEM_ID').values[0]),item_quantity)],product_transaction_schema)
        product_df = product_df.union(product_row)

    creditcard_df.write.mode('append').saveAsTable('CREDITCARD_TRANSACTION')
    product_df.write.mode('append').saveAsTable('PRODUCT_TRANSACTION')

#generate_customer_data()
generate_transactions_data()