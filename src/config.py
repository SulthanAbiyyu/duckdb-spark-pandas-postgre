import os
from dotenv import load_dotenv

load_dotenv()

def load_psql_config():
    return {
        'user': os.getenv('PG_USER'),
        'password': os.getenv('PG_PW'),
        'dbname': os.getenv('PG_DB'),
        'port': os.getenv('PG_PORT'),
        'host': os.getenv('PG_HOST')
    }

def load_spark_config():
    return {
        'spark.driver.memory': '4g',
        'spark.executor.memory': '8g',
        'spark.executor.cores': '2',
        'spark.driver.cores': '4',
    }

def init_spark(session_name='myapp'):
    from pyspark.sql import SparkSession
    config = load_spark_config()
    spark = SparkSession.builder.appName(session_name)\
        .getOrCreate()
        # .config('spark.executor.memory', config['spark.executor.memory']
        # ).config('spark.executor.cores', config['spark.executor.cores']
        # ).config('spark.driver.memory', config['spark.driver.memory']
        # ).config('spark.driver.cores', config['spark.driver.cores'])\
        
    return spark

def init_conn():
    import duckdb

    duckdb.execute("INSTALL postgres")
    duckdb.execute("LOAD postgres")
    duckdb.execute(f"ATTACH \
                'dbname={load_psql_config()['dbname']}\
                    user={load_psql_config()['user']}\
                    password={load_psql_config()['password']}\
                    host={load_psql_config()['host']}\
                    port={load_psql_config()['port']}'\
                        AS db (TYPE POSTGRES)\
                ")

