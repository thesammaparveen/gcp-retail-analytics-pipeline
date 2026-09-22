from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("RetailAnalyticsETL").getOrCreate()

sales_path = "gs://retail-landing-samma-2026/sales/sales.csv"
customer_path = "gs://retail-landing-samma-2026/customers/customers.csv"
inventory_path = "gs://retail-landing-samma-2026/inventory/inventory.csv"

output_path = "gs://retail-curated-samma-2026/fact_sales"

# Read source files
sales_df = spark.read.option("header", "true").option("inferSchema", "true").csv(sales_path)
customer_df = spark.read.option("header", "true").option("inferSchema", "true").csv(customer_path)
inventory_df = spark.read.option("header", "true").option("inferSchema", "true").csv(inventory_path)

# Basic data cleaning
sales_df = sales_df.dropna().dropDuplicates(["sale_id"])
customer_df = customer_df.dropna()
inventory_df = inventory_df.dropna()

# Join datasets
joined_df = (
    sales_df
    .join(customer_df, "customer_id", "left")
    .join(inventory_df, "product_id", "left")
)

# Add processing timestamp
joined_df = joined_df.withColumn(
    "processing_timestamp",
    current_timestamp()
)

# Rank sales within each store
window_spec = Window.partitionBy("store_id").orderBy(
    col("sale_amount").desc()
)

ranked_df = joined_df.withColumn(
    "sale_rank",
    rank().over(window_spec)
)

# Final fact table
fact_sales_df = ranked_df.select(
    "sale_id",
    "store_id",
    "product_id",
    "customer_id",
    "quantity",
    "sale_amount",
    "category",
    "membership",
    "sale_date",
    "processing_timestamp"
)

# Write curated data as Parquet
fact_sales_df.write.mode("overwrite").parquet(output_path)

spark.stop()
