# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Set catalog and schema
spark.sql("USE CATALOG customer360")
spark.sql("CREATE SCHEMA IF NOT EXISTS default")
spark.sql("USE SCHEMA default")

# Source CSV folder
source_path = "/Volumes/customer360/default/raw"

print("Bronze notebook setup completed!")

# COMMAND ----------

source_path = "/Volumes/customer360/default/raw"

files = dbutils.fs.ls(source_path)

for file in files:
    print(file.name, file.path, file.size)

# COMMAND ----------

customers_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{source_path}/customers.csv")
)

print("Customer records:", customers_df.count())

display(customers_df.limit(10))

# COMMAND ----------

orders_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{source_path}/orders.csv")
)

print("Order records:", orders_df.count())

display(orders_df.limit(10))

# COMMAND ----------

website_events_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{source_path}/web_events.csv")
)

print("Website event records:", website_events_df.count())

display(website_events_df.limit(10))

# COMMAND ----------

support_tickets_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{source_path}/support_tickets.csv")
)

print("Support ticket records:", support_tickets_df.count())

display(support_tickets_df.limit(10))

# COMMAND ----------

print("CUSTOMER:", customers_df.count())
print("ORDERS:", orders_df.count())
print("WEBSITE EVENTS:", website_events_df.count())
print("SUPPORT TICKETS:", support_tickets_df.count())

# COMMAND ----------

# Save raw source data as Bronze Delta tables

customers_df.write.format("delta").mode("overwrite").saveAsTable("bronze_customers")

orders_df.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")

website_events_df.write.format("delta").mode("overwrite").saveAsTable("bronze_website_events")

support_tickets_df.write.format("delta").mode("overwrite").saveAsTable("bronze_support_tickets")

print("All four Bronze Delta tables created successfully!")

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

spark.sql("DESCRIBE TABLE EXTENDED bronze_orders").show(truncate=False)

# COMMAND ----------

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_orders")

print("Bronze orders table updated successfully!")

# COMMAND ----------

spark.sql("DESCRIBE TABLE bronze_orders").show(truncate=False)

# COMMAND ----------

# Save customers
customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_customers")

# Save website events
website_events_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_website_events")

# Save support tickets
support_tickets_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_support_tickets")

print("All remaining Bronze tables saved successfully!")

# COMMAND ----------

tables = [
    "bronze_customers",
    "bronze_orders",
    "bronze_website_events",
    "bronze_support_tickets"
]

for table in tables:
    df = spark.table(table)
    print(f"{table}: {df.count()} records")