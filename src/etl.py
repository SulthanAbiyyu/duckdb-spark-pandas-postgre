import logging
import duckdb
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_db():
    try:
        duckdb.execute("CREATE SCHEMA IF NOT EXISTS db.landing")
        duckdb.execute("CREATE SCHEMA IF NOT EXISTS db.marts")
        logging.info("Successfully set up 'db.landing' and 'db.marts' schemas.")
    except Exception as e:
        logging.error(f"Error setting up schemas: {str(e)}")

def source_to_landing():
    try:
        data = duckdb.read_csv("data/source.csv")
        duckdb.execute("CREATE TABLE IF NOT EXISTS db.landing.customers AS FROM data")
        del data
        logging.info("Data loaded into 'db.landing.customers' successfully.")
    except Exception as e:
        logging.error(f"Error loading data into 'db.landing.customers': {str(e)}")

def dimension_modeling():
    try:
        data = duckdb.execute("SELECT * FROM db.landing.customers")
        data = data.df()
        
        # Create date dimension
        date_dim = data[["sale_date"]].copy()
        date_dim["sale_date"] = pd.to_datetime(date_dim["sale_date"])
        date_dim["date"] = date_dim["sale_date"].dt.date
        date_dim["year"] = date_dim["sale_date"].dt.year
        date_dim["month"] = date_dim["sale_date"].dt.month
        date_dim["day"] = date_dim["sale_date"].dt.day
        date_dim["day_of_week"] = date_dim["sale_date"].dt.dayofweek
        date_dim["quarter"] = date_dim["sale_date"].dt.quarter
        date_dim = date_dim.drop_duplicates()
        date_dim = date_dim.reset_index(drop=True)
        date_dim["date_id"] = date_dim["sale_date"].apply(lambda x: x.strftime('%Y%m%d')).astype(int)
        
        # Create customer dimension
        customer_dim = data[["customer_id", "customer_name", "customer_address"]].copy()
        customer_dim = customer_dim.drop_duplicates()
        
        # Create product dimension
        product_dim = data[["product_id", "product_name", "product_category"]].copy()
        product_dim = product_dim.drop_duplicates()
        
        # Create fact table
        fact_table = data[["sale_id", "customer_id", "product_id", "amount", "sale_date"]].copy()
        fact_table = fact_table.merge(date_dim[["sale_date", "date_id"]], on="sale_date", how="left")
        fact_table = fact_table[["sale_id", "customer_id", "product_id", "amount", "date_id"]]
        
        logging.info("Dimension modeling completed successfully.")
        return fact_table, customer_dim, product_dim, date_dim
    except Exception as e:
        logging.error(f"Error in dimension modeling: {str(e)}")

def load_to_marts():
    try:
        fact_table, customer_dim, product_dim, date_dim = dimension_modeling()
        
        duckdb.execute("CREATE TABLE IF NOT EXISTS db.marts.fact_table AS SELECT * FROM fact_table")
        duckdb.execute("CREATE TABLE IF NOT EXISTS db.marts.customer_dim AS SELECT * FROM customer_dim")
        duckdb.execute("CREATE TABLE IF NOT EXISTS db.marts.product_dim AS SELECT * FROM product_dim")
        duckdb.execute("CREATE TABLE IF NOT EXISTS db.marts.date_dim AS SELECT * FROM date_dim")
        
        logging.info("Tables created in 'db.marts' successfully.")
        
        del fact_table, customer_dim, product_dim, date_dim
        
    except Exception as e:
        logging.error(f"Error loading data into 'db.marts': {str(e)}")

# def dimension_modeling():
#     init_conn()
#     data = duckdb.execute("SELECT * FROM db.landing.customers")
#     data = data.df()
#     spark = init_spark("transform")
#     data = spark.createDataFrame(data)
    
#     # sale_id,customer_id,customer_name,customer_address,product_id,product_name,product_category,amount,sale_date
#         # Create fact table
#     fact_table = data.select(
#         col("sale_id").alias("sale_id"), 
#         col("customer_id").alias("customer_id"), 
#         col("product_id").alias("product_id"), 
#         col("amount").alias("amount"), 
#         col("sale_date").alias("sale_date")
#     )
    
#     # Create customer dimension
#     customer_dim = data.select(
#         col("customer_id").alias("customer_id"), 
#         col("customer_name").alias("customer_name"), 
#         col("customer_address").alias("customer_address")
#     ).dropDuplicates(["customer_id"])
    
#     # Create product dimension
#     product_dim = data.select(
#         col("product_id").alias("product_id"), 
#         col("product_name").alias("product_name"), 
#         col("product_category").alias("product_category")
#     ).dropDuplicates(["product_id"])
    
#     fact_table = fact_table.join(date_dim, fact_table.sale_date == date_dim.date, "left").select(
#         col("sale_id"), 
#         col("customer_id"), 
#         col("product_id"), 
#         col("amount"), 
#         col("date_id")
#     )
#     fact_table, customer_dim, product_dim, date_dim = fact_table.toPandas(), customer_dim.toPandas(), product_dim.toPandas(), date_dim.toPandas()
#     return fact_table, customer_dim, product_dim, date_dim


    
