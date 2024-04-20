from faker import Faker
from pyspark.sql.functions import col, year, month, sum, rank
from pyspark.sql.window import Window
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType,TimestampType
import random
import datetime as dt

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

generate_customer_data()
