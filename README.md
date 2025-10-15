# End-to-End Sales Data Mart: A Dimensional Modeling Project



## 1. Project Overview

This project implements a complete ELT (Extract, Load, Transform) pipeline to build a centralized and analysis-ready Sales Data Mart. The primary objective is to create a single source of truth for sales performance, customer behavior, and product analysis, empowering business analysts and decision-makers with a unified view for efficient and accurate reporting.

The core of this project involves integrating data from two disparate operational systems:
* A **Customer Relationship Management (CRM)** system holding core customer and sales transaction data.
* An **Enterprise Resource Planning (ERP)** system containing supplementary product categories and additional customer details.

The final data model is structured as a **Star Schema** and is designed to support advanced analytics, including RFM (Recency, Frequency, Monetary) analysis for customer segmentation and implementing logic to track product price history over time.

## 2. Architecture

The pipeline follows the modern Medallion Architecture pattern to ensure data quality and modularity, processing data through three distinct layers.

![Data Pipeline Architecture](Data%20Pipline.jpg)


* **Bronze Layer (Raw Data):** Data is ingested "as-is" from the source PostgreSQL databases using Apache Sqoop and stored in HDFS. This layer serves as a historical archive of the raw source data.
* **Silver Layer (Cleansed & Conformed Data):** Apache Spark jobs read the raw data from the Bronze layer, apply cleaning and standardization rules, handle data quality issues, and conform the data from different sources. The result is stored as queryable, structured tables in Apache Hive.
* **Gold Layer (Business-Ready Data):** The final layer consists of business-level tables, modeled into a Star Schema for analytics. Data from the Silver layer is aggregated and transformed to create the `Dim_Customer`, `Dim_Product`, and `Fact_Sales` tables. Business logic for metrics and KPIs is applied at this stage.

## 3. Technology Stack

| Component             | Technology                                      |
| --------------------- | ----------------------------------------------- |
| **Environment** | Docker & Docker Compose                         |
| **Data Source** | PostgreSQL                                      |
| **Data Ingestion** | Apache Sqoop                                    |
| **Data Lake Storage** | Hadoop HDFS                                     |
| **Data Processing** | Apache Spark (using PySpark and Spark SQL)      |
| **Data Warehouse** | Apache Hive                                     |
| **BI / Analytics** | Power BI, Tableau, or any SQL-based client      |

---

## 4. Pipeline Workflow Breakdown

The project executes a sequential data flow, progressively refining data as it moves through the Medallion Architecture.

### Stage 1: Data Ingestion (Source → Bronze)
* **Tool:** Apache Sqoop
* **Process:** The pipeline begins with the `ingest_data.sh` script, which uses Sqoop to connect to the source PostgreSQL database. Sqoop efficiently pulls data in parallel from the CRM and ERP schemas.
* **Outcome:** The raw, unaltered data is loaded directly into designated directories on HDFS. This establishes the **Bronze Layer**, serving as a true, historical backup of the source systems' data.

### Stage 2: Data Cleansing and Standardization (Bronze → Silver)
* **Tool:** Apache Spark
* **Process:** The `bronze_to_silver.py` Spark job is executed. It reads the raw files from the HDFS Bronze layer. Inside Spark, a series of transformations are applied:
    * **Schema Enforcement:** A strict schema is applied to each dataset upon reading to ensure data types are correct from the start.
    * **Data Cleaning:** Operations include trimming whitespace, handling null values, and standardizing categorical data (e.g., converting 'S' and 'M' to 'Single' and 'Married').
    * **Data Conforming:** Data from different tables and sources are unified into a consistent and logical format.
* **Outcome:** The processed, clean DataFrames are written back to HDFS in the optimized Parquet format. Hive external tables are then created over these files, establishing the **Silver Layer**. The data here is clean, validated, and ready for business-level analysis.

### Stage 3: Dimensional Modeling (Silver → Gold)
* **Tool:** Apache Spark & Apache Hive
* **Process:** The final `silver_to_gold.py` Spark job reads the cleansed tables from the Silver Layer in Hive. This job is responsible for applying the core business logic and dimensional modeling:
    * **Dimension Table Creation:** The `Dim_Customer` and `Dim_Product` tables are built by joining multiple Silver tables to create comprehensive, denormalized views of customers and products. Surrogate keys are generated to uniquely identify each record.
    * **Fact Table Creation:** The `Fact_Sales` table is constructed by joining transactional sales data with the newly created dimension tables using their surrogate keys.
    * **Partitioning:** To optimize query performance, the `Fact_Sales` table is partitioned by `year` and `month` when written to HDFS.
* **Outcome:** The final dimension and fact tables are saved as partitioned Parquet files, forming the **Gold Layer**. Hive external tables are created on top, providing a robust, performant, and query-friendly Star Schema. This Data Mart serves as the single source of truth for all downstream analytics and BI reporting.

---

## 5. Project Structure

The repository is organized as follows:

```
/
├── data_ingestion/
│   └── ingest_data.sh       # Contains all Sqoop commands for data ingestion.
│
├── spark_jobs/
│   ├── bronze_to_silver.py  # Spark script for Bronze to Silver transformations.
│   └── silver_to_gold.py    # Spark script for building the Gold layer Star Schema.
│
├── docker-compose.yml       # Defines and configures all services (Hadoop, Spark, etc.).
├── Data Pipline.jpg         # The architecture diagram.
└── README.md                # This documentation file.
```
