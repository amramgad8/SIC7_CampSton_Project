# spark_jobs/bronze_to_silver.py

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, trim, upper, when, row_number
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DateType, TimestampType)

def main():
    """
    This script handles the transformation of data from the Bronze layer (raw CSV)
    to the Silver layer (cleaned, standardized Parquet files and Hive tables).
    """
    spark = SparkSession.builder \
        .appName("Bronze to Silver Transformation") \
        .enableHiveSupport() \
        .getOrCreate()

    print("--- Defining Manual Schemas for All Bronze Tables ---")

    # region 1. Define Schemas for Bronze Layer Tables
    # Schema for: crm_cust_info
    schema_crm_cust_info = StructType([
        StructField("cst_id", IntegerType(), True),
        StructField("cst_key", StringType(), True),
        StructField("cst_firstname", StringType(), True),
        StructField("cst_lastname", StringType(), True),
        StructField("cst_marital_status", StringType(), True),
        StructField("cst_gndr", StringType(), True),
        StructField("cst_create_date", DateType(), True)
    ])

    # Schema for: crm_prd_info
    schema_crm_prd_info = StructType([
        StructField("prd_id", IntegerType(), True),
        StructField("prd_key", StringType(), True),
        StructField("prd_nm", StringType(), True),
        StructField("prd_cost", IntegerType(), True),
        StructField("prd_line", StringType(), True),
        StructField("prd_start_dt", TimestampType(), True),
        StructField("prd_end_dt", TimestampType(), True)
    ])

    # Schema for: crm_sales_details
    schema_crm_sales_details = StructType([
        StructField("sls_ord_num", StringType(), True),
        StructField("sls_prd_key", StringType(), True),
        StructField("sls_cust_id", IntegerType(), True),
        StructField("sls_order_dt", IntegerType(), True),
        StructField("sls_ship_dt", IntegerType(), True),
        StructField("sls_due_dt", IntegerType(), True),
        StructField("sls_sales", IntegerType(), True),
        StructField("sls_quantity", IntegerType(), True),
        StructField("sls_price", IntegerType(), True)
    ])

    # Schema for: erp_loc_a101
    schema_erp_loc_a101 = StructType([
        StructField("cid", StringType(), True),
        StructField("cntry", StringType(), True)
    ])

    # Schema for: erp_cust_az12
    schema_erp_cust_az12 = StructType([
        StructField("cid", StringType(), True),
        StructField("bdate", DateType(), True),
        StructField("gen", StringType(), True)
    ])

    # Schema for: erp_px_cat_g1v2
    schema_erp_px_cat_g1v2 = StructType([
        StructField("id", StringType(), True),
        StructField("cat", StringType(), True),
        StructField("subcat", StringType(), True),
        StructField("maintenance", StringType(), True)
    ])
    # endregion

    # region 2. Read Data from Bronze Layer HDFS Paths
    print("--- Reading Bronze Layer data from HDFS ---")
    bronze_paths = {
        "crm_cust_info": "/user/project/raw/crm_cust_info",
        "crm_prd_info": "/user/project/raw/crm_prd_info",
        "crm_sales_details": "/user/project/raw/crm_sales_details",
        "erp_loc_a101": "/user/project/raw/erp_loc_a101",
        "erp_cust_az12": "/user/project/raw/erp_cust_az12",
        "erp_px_cat_g1v2": "/user/project/raw/erp_px_cat_g1v2"
    }

    df_bronze_cust_info = spark.read.schema(schema_crm_cust_info).option("delimiter", ",").csv(bronze_paths["crm_cust_info"])
    df_bronze_prd_info = spark.read.schema(schema_crm_prd_info).option("delimiter", ",").csv(bronze_paths["crm_prd_info"])
    df_bronze_sales_details = spark.read.schema(schema_crm_sales_details).option("delimiter", ",").csv(bronze_paths["crm_sales_details"])
    df_bronze_loc_a101 = spark.read.schema(schema_erp_loc_a101).option("delimiter", ",").csv(bronze_paths["erp_loc_a101"])
    df_bronze_cust_az12 = spark.read.schema(schema_erp_cust_az12).option("delimiter", ",").csv(bronze_paths["erp_cust_az12"])
    df_bronze_px_cat_g1v2 = spark.read.schema(schema_erp_px_cat_g1v2).option("delimiter", ",").csv(bronze_paths["erp_px_cat_g1v2"])
    # endregion
    
    # region 3. Apply Transformations to Create Silver DataFrames
    print("--- Applying transformations to create Silver DataFrames ---")

    # Transformation for crm_cust_info
    window_spec = Window.partitionBy("cst_id").orderBy(col("cst_create_date").desc())
    df_silver_cust_info = df_bronze_cust_info \
        .filter(col("cst_id").isNotNull()) \
        .withColumn("flag_last", row_number().over(window_spec)) \
        .filter(col("flag_last") == 1) \
        .withColumn("cst_firstname", trim(col("cst_firstname"))) \
        .withColumn("cst_lastname", trim(col("cst_lastname"))) \
        .withColumn("cst_marital_status",
            when(upper(trim(col("cst_marital_status"))) == 'S', "Single")
            .when(upper(trim(col("cst_marital_status"))) == 'M', "Married")
            .otherwise("n/a")
        ) \
        .withColumn("cst_gndr",
            when(upper(trim(col("cst_gndr"))) == 'F', "Female")
            .when(upper(trim(col("cst_gndr"))) == 'M', "Male")
            .otherwise("n/a")
        ) \
        .select("cst_id", "cst_key", "cst_firstname", "cst_lastname", "cst_marital_status", "cst_gndr", "cst_create_date")

    # Transformation for crm_prd_info
    df_bronze_prd_info.createOrReplaceTempView("bronze_prd_info_v")
    df_silver_prd_info = spark.sql("""
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, 1000) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                date_sub(
                    LEAD(prd_start_dt) OVER (PARTITION BY SUBSTRING(prd_key, 7, 1000) ORDER BY prd_start_dt), 
                    1
                ) 
            AS DATE) AS prd_end_dt
        FROM 
            bronze_prd_info_v
    """)
    
    # Transformation for crm_sales_details
    df_bronze_sales_details.createOrReplaceTempView("bronze_sales_details_v")
    df_silver_sales_details = spark.sql("""
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR length(CAST(sls_order_dt AS STRING)) != 8 THEN NULL
                ELSE to_date(CAST(sls_order_dt AS STRING), 'yyyyMMdd')
            END AS sls_order_dt,
            CASE 
                WHEN sls_ship_dt = 0 OR length(CAST(sls_ship_dt AS STRING)) != 8 THEN NULL
                ELSE to_date(CAST(sls_ship_dt AS STRING), 'yyyyMMdd')
            END AS sls_ship_dt,
            CASE 
                WHEN sls_due_dt = 0 OR length(CAST(sls_due_dt AS STRING)) != 8 THEN NULL
                ELSE to_date(CAST(sls_due_dt AS STRING), 'yyyyMMdd')
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * abs(sls_price) 
                THEN sls_quantity * abs(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN CASE WHEN sls_quantity != 0 THEN sls_sales / sls_quantity ELSE NULL END
                ELSE sls_price 
            END AS sls_price
        FROM 
            bronze_sales_details_v
    """)

    # Transformation for erp_loc_a101
    df_bronze_loc_a101.createOrReplaceTempView("bronze_loc_a101_v")
    df_silver_loc_a101 = spark.sql("""
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM
            bronze_loc_a101_v
    """)

    # Transformation for erp_cust_az12
    df_bronze_cust_az12.createOrReplaceTempView("bronze_cust_az12_v")
    df_silver_cust_az12 = spark.sql("""
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
                ELSE cid
            END AS cid, 
            CASE
                WHEN bdate > current_date() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM
            bronze_cust_az12_v
    """)

    # Transformation for erp_px_cat_g1v2
    df_silver_px_cat_g1v2 = df_bronze_px_cat_g1v2
    #endregion
    
    # region 4. Write Silver DataFrames to HDFS as Parquet
    silver_base_path = "/user/project/silver"
    print(f"--- Writing Silver DataFrames to HDFS path: {silver_base_path} ---")

    df_silver_cust_info.write.mode("overwrite").parquet(f"{silver_base_path}/crm_cust_info")
    df_silver_prd_info.write.mode("overwrite").parquet(f"{silver_base_path}/crm_prd_info")
    df_silver_sales_details.write.mode("overwrite").parquet(f"{silver_base_path}/crm_sales_details")
    df_silver_loc_a101.write.mode("overwrite").parquet(f"{silver_base_path}/erp_loc_a101")
    df_silver_cust_az12.write.mode("overwrite").parquet(f"{silver_base_path}/erp_cust_az12")
    df_silver_px_cat_g1v2.write.mode("overwrite").parquet(f"{silver_base_path}/erp_px_cat_g1v2")
    # endregion

    # region 5. Create Silver Layer Hive External Tables
    print("--- Creating Silver Layer Hive Tables ---")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver.crm_cust_info (
          cst_id INT, cst_key STRING, cst_firstname STRING, cst_lastname STRING,
          cst_marital_status STRING, cst_gndr STRING, cst_create_date DATE
        ) STORED AS PARQUET LOCATION '/user/project/silver/crm_cust_info'
    """)
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver.crm_prd_info (
          prd_id INT, cat_id STRING, prd_key STRING, prd_nm STRING, prd_cost INT,
          prd_line STRING, prd_start_dt DATE, prd_end_dt DATE
        ) STORED AS PARQUET LOCATION '/user/project/silver/crm_prd_info'
    """)
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver.crm_sales_details (
          sls_ord_num STRING, sls_prd_key STRING, sls_cust_id INT, sls_order_dt DATE,
          sls_ship_dt DATE, sls_due_dt DATE, sls_sales INT, sls_quantity INT, sls_price DOUBLE
        ) STORED AS PARQUET LOCATION '/user/project/silver/crm_sales_details'
    """)
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver.erp_loc_a101 (
          cid STRING, cntry STRING
        ) STORED AS PARQUET LOCATION '/user/project/silver/erp_loc_a101'
    """)
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver.erp_cust_az12 (
          cid STRING, bdate DATE, gen STRING
        ) STORED AS PARQUET LOCATION '/user/project/silver/erp_cust_az12'
    """)
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver.erp_px_cat_g1v2 (
          id STRING, cat STRING, subcat STRING, maintenance STRING
        ) STORED AS PARQUET LOCATION '/user/project/silver/erp_px_cat_g1v2'
    """)
    # endregion

    print("--- Bronze to Silver pipeline completed successfully! ---")
    spark.stop()

if __name__ == '__main__':
    main()