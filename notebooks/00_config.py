"""
00_config.py

Global configuration for the PEI Lead Data Engineer Assessment.
Run this notebook first.
"""

import sys
import logging

PROJECT_ROOT = "<absolute_path_to_project_root>"

if not PROJECT_ROOT in sys.path:
    sys.path.append(PROJECT_ROOT)

# ------------------------------------------------------------------
# Database Names (Medallion Architecture: Bronze / Silver / Gold)
# ------------------------------------------------------------------

CATALOG = "<catalog_name>"
SCHEMA = "<schema_name>"



BRONZE_DB = f"{CATALOG}.{SCHEMA}"
SILVER_DB = f"{CATALOG}.{SCHEMA}"
GOLD_DB = f"{CATALOG}.{SCHEMA}"

# ------------------------------------------------------------------
# Input Data Paths
# ------------------------------------------------------------------


CUSTOMER_FILE = "<absolute_path_to_data>/Customer.xlsx"
PRODUCT_FILE = "<absolute_path_to_data>/Products.csv"
ORDER_FILE = "<absolute_path_to_data>/Orders.json"

# ------------------------------------------------------------------
# Delta Table Names (Medallion Architecture)
# ------------------------------------------------------------------

# Bronze (raw ingestion)
BRONZE_CUSTOMER_TABLE = f"{BRONZE_DB}.bronze_customer"
BRONZE_PRODUCT_TABLE = f"{BRONZE_DB}.bronze_product"
BRONZE_ORDER_TABLE = f"{BRONZE_DB}.bronze_order"

# Silver (cleansed & enriched)
SILVER_CUSTOMER_TABLE = f"{SILVER_DB}.silver_customer"
SILVER_PRODUCT_TABLE = f"{SILVER_DB}.silver_product"
SILVER_ORDER_TABLE = f"{SILVER_DB}.silver_order"

# Gold (aggregated / reporting)
GOLD_PROFIT_SUMMARY_TABLE = f"{GOLD_DB}.gold_profit_summary"

# Quarantine (rejected / failed validation)
QUARANTINE_ORPHAN_ORDERS_TABLE = f"{SILVER_DB}.quarantine_orphan_orders"

# Semantic View Names
SILVER_CUSTOMER_VIEW = f"{SILVER_DB}.vw_silver_customer"
SILVER_PRODUCT_VIEW = f"{SILVER_DB}.vw_silver_product"
SILVER_ORDER_VIEW = f"{SILVER_DB}.vw_silver_order"

GOLD_PROFIT_SUMMARY_VIEW = f"{GOLD_DB}.vw_gold_profit_summary"

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

logger.info("Configuration loaded successfully.")
