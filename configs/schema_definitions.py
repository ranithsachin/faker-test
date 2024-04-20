from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType,FloatType,TimestampType

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