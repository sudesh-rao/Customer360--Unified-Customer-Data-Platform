# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Select catalog and schema
spark.sql("USE CATALOG customer360")
spark.sql("USE SCHEMA default")

print("Silver notebook setup completed!")

# COMMAND ----------

# Read data from Bronze Delta tables

customers_df = spark.table("bronze_customers")

orders_df = spark.table("bronze_orders")

website_events_df = spark.table("bronze_website_events")

support_tickets_df = spark.table("bronze_support_tickets")

print("All four Bronze tables loaded successfully!")

# COMMAND ----------

print("CUSTOMERS")
customers_df.printSchema()

print("ORDERS")
orders_df.printSchema()

print("WEBSITE EVENTS")
website_events_df.printSchema()

print("SUPPORT TICKETS")
support_tickets_df.printSchema()

# COMMAND ----------

customers_silver = (
    customers_df

    # Standardise customer information
    .withColumn("customer_id", F.col("customer_id").cast("string"))
    .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn("segment", F.upper(F.trim(F.col("segment"))))

    # Standardise phone numbers
    .withColumn(
        "phone",
        F.regexp_replace(F.col("phone"), r"[^0-9]", "")
    )

    # Handle multiple signup date formats
    .withColumn(
        "signup_date",
        F.coalesce(
            F.to_date(F.try_to_timestamp("signup_date", F.lit("dd-MM-yyyy"))),
            F.to_date(F.try_to_timestamp("signup_date", F.lit("MMM dd, yyyy"))),
            F.to_date(F.try_to_timestamp("signup_date", F.lit("yyyy-MM-dd")))
        )
    )

    # Remove records without customer IDs
    .filter(
        F.col("customer_id").isNotNull() &
        (F.col("customer_id") != "")
    )
)

print("Customer standardisation completed!")

# COMMAND ----------

customer_window = Window.partitionBy(
    "email",
    "first_name",
    "last_name"
).orderBy(F.col("customer_id").cast("long").asc())

customers_silver = (
    customers_silver
    .withColumn(
        "row_num",
        F.row_number().over(customer_window)
    )
    .filter(
        F.col("email").isNull() |
        (F.col("email") == "") |
        (F.col("row_num") == 1)
    )
    .drop("row_num")
)

print("Customer deduplication completed!")

# COMMAND ----------

print("Original customer count:", customers_df.count())

print("After standardisation and deduplication:", customers_silver.count())

remaining_duplicates = (
    customers_silver
    .filter(F.col("email").isNotNull() & (F.col("email") != ""))
    .groupBy("email", "first_name", "last_name")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Remaining duplicate groups:", remaining_duplicates)

# COMMAND ----------

customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_customers")

print("Silver customers table created successfully!")

# COMMAND ----------

display(orders_df.limit(10))

# COMMAND ----------

print("Total orders:", orders_df.count())

print("Unique order IDs:", orders_df.select("order_id").distinct().count())

print("Null values in each column:")

orders_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in orders_df.columns
]).show()

# COMMAND ----------

orders_df.select("order_date") \
    .distinct() \
    .show(20, truncate=False)

# COMMAND ----------

orders_df.groupBy("status").count().show()

# COMMAND ----------

orders_silver = (
    orders_df

    # Standardise customer details
    .withColumn("customer_email", F.lower(F.trim(F.col("customer_email"))))
    .withColumn(
        "customer_phone",
        F.regexp_replace(F.col("customer_phone"), r"[^0-9]", "")
    )

    # Standardise order status and category
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("product_category", F.initcap(F.trim(F.col("product_category"))))

    # Convert order date into date format
    .withColumn(
        "order_date",
        F.coalesce(
            F.to_date(F.try_to_timestamp("order_date", F.lit("yyyy-MM-dd"))),
            F.to_date(F.try_to_timestamp("order_date", F.lit("dd/MM/yyyy"))),
            F.to_date(F.try_to_timestamp("order_date", F.lit("MMM dd, yyyy")))
        )
    )

    # Remove records without order IDs
    .filter(
        F.col("order_id").isNotNull() &
        (F.trim(F.col("order_id")) != "")
    )
)

print("Order standardisation completed!")

# COMMAND ----------

order_window = Window.partitionBy("order_id").orderBy(
    F.col("order_date").asc_nulls_last()
)

orders_silver = (
    orders_silver
    .withColumn(
        "row_num",
        F.row_number().over(order_window)
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

print("Order deduplication completed!")

# COMMAND ----------

print("Original orders:", orders_df.count())

print("Cleaned orders:", orders_silver.count())

print(
    "Unique order IDs:",
    orders_silver.select("order_id").distinct().count()
)

print(
    "Remaining duplicate order IDs:",
    orders_silver.groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# COMMAND ----------

orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_orders")

print("Silver orders table created successfully!")

# COMMAND ----------

print("Total website events:", website_events_df.count())

print(
    "Unique event IDs:",
    website_events_df.select("event_id").distinct().count()
)

print("Null values in each column:")

website_events_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in website_events_df.columns
]).show()

# COMMAND ----------

display(website_events_df.limit(10))

# COMMAND ----------

print("Total website events:", website_events_df.count())

print(
    "Unique event IDs:",
    website_events_df.select("event_id").distinct().count()
)

print("Null values in each column:")

website_events_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in website_events_df.columns
]).show()

print("Event type distribution:")

website_events_df.groupBy("event_type").count().show()

# COMMAND ----------

website_events_silver = (
    website_events_df

    .withColumn(
        "visitor_email",
        F.lower(F.trim(F.col("visitor_email")))
    )
    .withColumn(
        "session_id",
        F.trim(F.col("session_id"))
    )
    .withColumn(
        "event_type",
        F.lower(F.trim(F.col("event_type")))
    )
    .withColumn(
        "page",
        F.trim(F.col("page"))
    )

    .filter(
        F.col("event_id").isNotNull() &
        (F.trim(F.col("event_id")) != "")
    )
)

print("Website events standardisation completed!")

# COMMAND ----------

event_window = Window.partitionBy("event_id").orderBy(
    F.col("event_ts").asc()
)

website_events_silver = (
    website_events_silver
    .withColumn(
        "row_num",
        F.row_number().over(event_window)
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

print("Website event deduplication completed!")

# COMMAND ----------

print("Original events:", website_events_df.count())

print("Cleaned events:", website_events_silver.count())

print(
    "Unique event IDs:",
    website_events_silver.select("event_id").distinct().count()
)

print(
    "Remaining duplicate event IDs:",
    website_events_silver.groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# COMMAND ----------

website_events_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_website_events")

print("Silver website events table created successfully!")

# COMMAND ----------

print("Total support tickets:", support_tickets_df.count())

print(
    "Unique ticket IDs:",
    support_tickets_df.select("ticket_id").distinct().count()
)

print("Null values in each column:")

support_tickets_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in support_tickets_df.columns
]).show()

print("Ticket status distribution:")

support_tickets_df.groupBy("status").count().show()

print("Ticket priority distribution:")

support_tickets_df.groupBy("priority").count().show()

# COMMAND ----------

support_tickets_df.printSchema()

support_tickets_df.select("created_at") \
    .distinct() \
    .show(20, truncate=False)

# COMMAND ----------

support_tickets_silver = (
    support_tickets_df

    # Standardise customer email
    .withColumn(
        "customer_email",
        F.lower(F.trim(F.col("customer_email")))
    )

    # Standardise status, priority and issue type
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("priority", F.upper(F.trim(F.col("priority"))))
    .withColumn("issue_type", F.initcap(F.trim(F.col("issue_type"))))

    # Convert created_at into date format
    .withColumn(
        "created_at",
        F.coalesce(
            F.to_date(F.try_to_timestamp("created_at", F.lit("yyyy-MM-dd"))),
            F.to_date(F.try_to_timestamp("created_at", F.lit("dd/MM/yyyy"))),
            F.to_date(F.try_to_timestamp("created_at", F.lit("MMM dd, yyyy")))
        )
    )

    # Remove records without ticket IDs
    .filter(
        F.col("ticket_id").isNotNull() &
        (F.trim(F.col("ticket_id")) != "")
    )
)

print("Support ticket standardisation completed!")

# COMMAND ----------

ticket_window = Window.partitionBy("ticket_id").orderBy(
    F.col("created_at").asc()
)

support_tickets_silver = (
    support_tickets_silver
    .withColumn(
        "row_num",
        F.row_number().over(ticket_window)
    )
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

print("Support ticket deduplication completed!")

# COMMAND ----------

print("Original tickets:", support_tickets_df.count())

print("Cleaned tickets:", support_tickets_silver.count())

print(
    "Unique ticket IDs:",
    support_tickets_silver.select("ticket_id").distinct().count()
)

print(
    "Remaining duplicate ticket IDs:",
    support_tickets_silver.groupBy("ticket_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# COMMAND ----------

support_tickets_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_support_tickets")

print("Silver support tickets table created successfully!")

# COMMAND ----------

silver_tables = [
    "silver_customers",
    "silver_orders",
    "silver_website_events",
    "silver_support_tickets"
]

for table in silver_tables:
    df = spark.table(table)
    print(f"{table}: {df.count()} records")