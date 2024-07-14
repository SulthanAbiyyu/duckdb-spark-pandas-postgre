# ETL Pipeline with DuckDB, Spark, Pandas and Postgres

![banner](/img/pipeline.png)
This project implements an ETL (Extract, Transform, Load) pipeline that processes data from local CSV files, performs dimensional modeling using a star schema approach, and stores the transformed data in PostgreSQL.

## Project Structure

- `etl.py`: Contains functions for setting up databases, loading data into DuckDB, performing dimensional modeling, and loading transformed data into PostgreSQL.
- `config.py`: Configuration file for initializing connections to databases.
- `main.py`: Entrypoint
- `altering_relations.sql`: Alter the key columns and create foreign key references

## Installation

1. Clone the repository:

   ```
   git clone https://github.com/SulthanAbiyyu/duckdb-spark-pandas-postgre
   cd duckdb-spark-pandas-postgre
   ```

2. Install dependencies:

   ```
   pip install -r requirements.txt
   ```

## Pipeline Flow

0. **Data Generation**

   - Run `python data/utils/generator.py`

1. **Database Setup**:

   - Ensure PostgreSQL is installed and running.
   - Update database connection details in `.env`.
   - You can modify `config.py` to setup the spark configurations.

2. **Running the Pipeline**:
   - Execute the pipeline by running `python src/main.py`.
   - This will:
     - Create schemas (`db.landing` and `db.marts`) in DuckDB.
     - Extract data from `data/source.csv` into DuckDB in the `db.landing.customers` table on PostgreSQL.
     - Perform transformations such as dimensional modeling to create date, customer, product dimensions, and a fact table.
     - Load transformed data into PostgreSQL in the `db.marts` schema.

**Notes:**
![erd](/img/erd.png)

- The dimensional modeling creates a star schema:
  - **Fact Table**: Contains sales data with foreign keys to date, customer, and product dimensions.
  - **Dimensions**: Separate tables for date, customer, and product details.
- I'm having a hard time to create foreign key using duckdb Postgre extension, so I just do the altering in pgAdmin.
