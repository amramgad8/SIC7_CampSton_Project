#!/bin/bash

# =============================================================================
#  Data Ingestion Script
#  Description: This script uses Sqoop to import tables from a PostgreSQL
#               database (CRM and ERP schemas) into the HDFS raw layer.
# =============================================================================

# Exit the script immediately if any command fails
set -e

# --- Configuration Variables ---
# Define connection parameters once to reuse them in all commands.
# This makes it easier to update them in the future.

DB_CONNECT="jdbc:postgresql://host.docker.internal:5433/project_db"
DB_USER="user_de"
DB_PASS="password123"
HDFS_RAW_PATH="/user/project/raw"

# --- Start of Ingestion Process ---
echo "INFO: Starting data ingestion from PostgreSQL to HDFS..."

# 1. Ingest CRM Tables
# ------------------------------------------------
echo "INFO: Importing table 'crm.cust_info'..."
sqoop import \
  --connect "${DB_CONNECT}" \
  --username "${DB_USER}" \
  --password "${DB_PASS}" \
  --query 'SELECT * FROM crm.cust_info WHERE $CONDITIONS' \
  --target-dir "${HDFS_RAW_PATH}/crm_cust_info" \
  --delete-target-dir \
  -m 1

echo "INFO: Importing table 'crm.sales_details'..."
sqoop import \
  --connect "${DB_CONNECT}" \
  --username "${DB_USER}" \
  --password "${DB_PASS}" \
  --query 'SELECT * FROM crm.sales_details WHERE $CONDITIONS' \
  --target-dir "${HDFS_RAW_PATH}/crm_sales_details" \
  --delete-target-dir \
  -m 1

# 2. Ingest ERP Tables
# ------------------------------------------------
echo "INFO: Importing table 'erp.loc_a101'..."
sqoop import \
  --connect "${DB_CONNECT}" \
  --username "${DB_USER}" \
  --password "${DB_PASS}" \
  --query 'SELECT * FROM erp.loc_a101 WHERE $CONDITIONS' \
  --target-dir "${HDFS_RAW_PATH}/erp_loc_a101" \
  --delete-target-dir \
  -m 1

echo "INFO: Importing table 'erp.cust_az12'..."
sqoop import \
  --connect "${DB_CONNECT}" \
  --username "${DB_USER}" \
  --password "${DB_PASS}" \
  --query 'SELECT * FROM erp.cust_az12 WHERE $CONDITIONS' \
  --target-dir "${HDFS_RAW_PATH}/erp_cust_az12" \
  --delete-target-dir \
  -m 1

echo "INFO: Importing table 'erp.px_cat_g1v2'..."
sqoop import \
  --connect "${DB_CONNECT}" \
  --username "${DB_USER}" \
  --password "${DB_PASS}" \
  --query 'SELECT * FROM erp.px_cat_g1v2 WHERE $CONDITIONS' \
  --target-dir "${HDFS_RAW_PATH}/erp_px_cat_g1v2" \
  --delete-target-dir \
  -m 1

# --- End of Ingestion Process ---
echo "SUCCESS: All data ingestion tasks completed successfully."