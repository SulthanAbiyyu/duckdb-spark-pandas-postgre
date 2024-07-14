import os
from dotenv import load_dotenv

load_dotenv()

def load_psql_config():
    return {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'dbname': os.getenv('DB_NAME'),
        'port': os.getenv('DB_PORT')
    }

def load_spark_config():
    return {
        'spark.driver.memory': '4g',
        'spark.executor.memory': '4g',
        'spark.executor.cores': '2',
        'spark.driver.cores': '2',
        'spark.sql.shuffle.partitions': '2'
    }

def create_spark_session():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("Sparkify")
    return spark

