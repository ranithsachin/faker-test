import logging
import json

import conf.configs as config

from pyspark.sql import SparkSession
from kafka import KafkaProducer

logger = logging.getLogger(__name__)
logging.basicConfig(filename=config.APP_LOGING_PATH,encoding="utf-8",level=config.APP_LOGING_LEVEL)

def get_spark_session(app_name="assignment")->SparkSession:
    """
        Generate a spark session and return
        Arguments:
            app_name: app name for the spark conf
    """ 
    try:
        v_spark_session = SparkSession.Builder().master("local[2]").appName(app_name).getOrCreate() 
        return v_spark_session
    except Exception as e:
        logger.error(f"Error encountered during spark session generation")
        raise e
    
def kafka_producer(message)->None:
        """
        Produce json messages to kafka

        Args:
            message: incoming json message
        """
        # producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        # producer.send('faker-transactions',key='trans',value=message)
    

    

