# spark_jobs/silver_to_gold.py

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, when, coalesce, lit, year, month

def main():
    """
    This script reads data from the Silver layer Hive tables, builds the star schema
    (dimensions and facts), and writes the final tables to the Gold layer as 
    partitioned Parquet files and Hive tables.
    """
    spark = SparkSession.builder \
        .appName("Silver to Gold Transformation") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # region 1. Build and Write Dimension: dim_customers
    print("--- Building and Writing Gold Layer Dimension: dim_customers ---")
    df_silver_cust_info = spark.table("silver.crm_cust_info")
    df_silver_cust_az12 = spark.table("silver.erp_cust_az12")
    df_silver_loc_a101 = spark.table("silver.erp_loc_a101")

    df_joined = df_silver_cust_info.alias("ci") \
        .join(df_silver_cust_az12.alias("ca"), col("ci.cst_key") == col("ca.cid"), "left") \
        .join(df_silver_loc_a101.alias("la"), col("ci.cst_key") == col("la.cid"), "left")

    dim_customers = df_joined.select(
        col("ci.cst_id").alias("customer_id"), col("ci.cst_key").alias("customer_number"),
        col("ci.cst_firstname").alias("first_name"), col("ci.cst_lastname").alias("last_name"),
        col("la.cntry").alias("country"), col("ci.cst_marital_status").alias("marital_status"),
        when(col("ci.cst_gndr") != 'n/a', col("ci.cst_gndr")).otherwise(coalesce(col("ca.gen"), lit('n/a'))).alias("gender"),
        col("ca.bdate").alias("birthdate"), col("ci.cst_create_date").alias("create_date")
    )
    window_spec_cust = Window.orderBy("customer_id")
    DimCustomers = dim_customers.withColumn("customer_key", row_number().over(window_spec_cust).cast("long"))

    gold_path_customers = "hdfs:///user/project/gold/dim_customers"
    DimCustomers.write.mode("overwrite").parquet(gold_path_customers)
    # endregion

    # region 2. Build and Write Dimension: dim_products
    print("--- Building and Writing Gold Layer Dimension: dim_products ---")
    df_silver_prd_info = spark.table("silver.crm_prd_info")
    df_silver_px_cat_g1v2 = spark.table("silver.erp_px_cat_g1v2")

    df_joined_products = df_silver_prd_info.alias("pn") \
        .join(df_silver_px_cat_g1v2.alias("pc"), col("pn.cat_id") == col("pc.id"), "left") \
        .filter(col("pn.prd_end_dt").isNull())

    dim_products = df_joined_products.select(
        col("pn.prd_id").alias("product_id"), col("pn.prd_key").alias("product_number"),
        col("pn.prd_nm").alias("product_name"), col("pn.cat_id").alias("category_id"),
        col("pc.cat").alias("category"), col("pc.subcat").alias("subcategory"),
        col("pc.maintenance"), col("pn.prd_cost").alias("cost"),
        col("pn.prd_line").alias("product_line"), col("pn.prd_start_dt").alias("start_date")
    )
    window_spec_prod = Window.orderBy("start_date", "product_number")
    DimProducts = dim_products.withColumn("product_key", row_number().over(window_spec_prod).cast("long"))

    gold_path_products = "hdfs:///user/project/gold/dim_products"
    DimProducts.write.mode("overwrite").parquet(gold_path_products)
    # endregion

    # region 3. Create Gold Database and Dimension Tables in Hive
    print("--- Creating Gold Layer Hive Tables for Dimensions ---")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    # Drop and create ensures the table metadata is always up-to-date
    spark.sql("DROP TABLE IF EXISTS gold.dim_customers")
    spark.sql("""
        CREATE EXTERNAL TABLE gold.dim_customers (
          customer_id INT, customer_number STRING, first_name STRING, last_name STRING,
          country STRING, marital_status STRING, gender STRING, birthdate DATE,
          create_date DATE, customer_key BIGINT
        ) STORED AS PARQUET LOCATION 'hdfs:///user/project/gold/dim_customers'
    """)

    spark.sql("DROP TABLE IF EXISTS gold.dim_products")
    spark.sql("""
        CREATE EXTERNAL TABLE gold.dim_products (
          product_id INT, product_number STRING, product_name STRING, category_id STRING,
          category STRING, subcategory STRING, maintenance STRING, cost INT,
          product_line STRING, start_date DATE, product_key BIGINT
        ) STORED AS PARQUET LOCATION 'hdfs:///user/project/gold/dim_products'
    """)
    # endregion

    # region 4. Build and Write Fact Table: fact_sales
    print("--- Building and Writing PARTITIONED Gold Layer Fact Table: fact_sales ---")
    df_silver_sales_details = spark.table("silver.crm_sales_details")
    df_gold_dims_cust = spark.table("gold.dim_customers")
    df_gold_dims_prod = spark.table("gold.dim_products")

    FactSales = df_silver_sales_details.alias("sd") \
        .join(df_gold_dims_prod.alias("pr"), col("sd.sls_prd_key") == col("pr.product_number"), "left") \
        .join(df_gold_dims_cust.alias("cu"), col("sd.sls_cust_id") == col("cu.customer_id"), "left") \
        .select(
            col("sd.sls_ord_num").alias("order_number"), col("pr.product_key"),
            col("cu.customer_key"), col("sd.sls_order_dt").alias("order_date"),
            col("sd.sls_ship_dt").alias("shipping_date"), col("sd.sls_due_dt").alias("due_date"),
            col("sd.sls_sales").alias("sales_amount"), col("sd.sls_quantity").alias("quantity"),
            col("sd.sls_price").alias("price")
        )

    # Add 'year' and 'month' columns for partitioning
    FactSalesWithPartitions = FactSales.withColumn("year", year(col("order_date"))) \
                                       .withColumn("month", month(col("order_date")))

    gold_path_sales = "hdfs:///user/project/gold/fact_sales"
    FactSalesWithPartitions.write.mode("overwrite").partitionBy("year", "month").parquet(gold_path_sales)
    # endregion

    # region 5. Create Partitioned Fact Table in Hive and Repair
    print("--- Creating Partitioned Hive Table for fact_sales ---")
    spark.sql("DROP TABLE IF EXISTS gold.fact_sales")
    spark.sql("""
        CREATE EXTERNAL TABLE gold.fact_sales (
          order_number STRING, product_key BIGINT, customer_key BIGINT, order_date DATE,
          shipping_date DATE, due_date DATE, sales_amount INT, quantity INT, price DOUBLE
        )
        PARTITIONED BY (year INT, month INT)
        STORED AS PARQUET
        LOCATION 'hdfs:///user/project/gold/fact_sales'
    """)

    # Tell Hive to discover the partitions that Spark just created
    spark.sql("MSCK REPAIR TABLE gold.fact_sales")
    print("--- Repaired fact_sales table to discover partitions. ---")
    # endregion

    print("--- Silver to Gold pipeline completed successfully! ---")
    spark.stop()

if __name__ == '__main__':
    main()