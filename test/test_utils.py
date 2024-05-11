from pyspark.sql import SparkSession

import src.utils as utils


def test_spark_session_object()->None:
    """
    Testing whether get_spark_session method in utils returns a spark session object
    """
    spark = utils.get_spark_session()
    assert type(spark) is SparkSession