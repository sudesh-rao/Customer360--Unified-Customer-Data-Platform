# Databricks notebook source
from pyspark.sql import functions as F

spark.sql("USE CATALOG customer360")
spark.sql("USE SCHEMA default")

print("Gold notebook setup completed!")

# COMMAND ----------

customers_df = spark.table("silver_customers")
orders_df = spark.table("silver_orders")
website_events_df = spark.table("silver_website_events")
support_tickets_df = spark.table("silver_support_tickets")

print("All four Silver tables loaded successfully!")

# COMMAND ----------

duplicate_emails = (
    customers_df
    .filter(F.col("email").isNotNull() & (F.trim(F.col("email")) != ""))
    .groupBy("email")
    .count()
    .filter(F.col("count") > 1)
)

print("Number of duplicate email groups:", duplicate_emails.count())

display(duplicate_emails)

# COMMAND ----------

orders_summary = (
    orders_df
    .groupBy("customer_email")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum(
            F.when(
                F.col("status") == "DELIVERED",
                F.col("order_amount")
            ).otherwise(0)
        ).alias("lifetime_value"),
        F.max(
            F.when(
                F.col("status") == "DELIVERED",
                F.col("order_date")
            )
        ).alias("last_order_date")
    )
)

print("Orders summary created successfully!")
display(orders_summary.limit(5))

# COMMAND ----------

orders_summary = (
    orders_df
    .filter(
        F.col("customer_email").isNotNull()
        & (F.trim(F.col("customer_email")) != "")
    )
    .groupBy("customer_email")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.round(
            F.sum(
                F.when(
                    F.col("status") == "DELIVERED",
                    F.col("order_amount")
                ).otherwise(0)
            ),
            2
        ).alias("lifetime_value"),
        F.max(
            F.when(
                F.col("status") == "DELIVERED",
                F.col("order_date")
            )
        ).alias("last_order_date")
    )
)

print("Orders summary updated successfully!")
display(orders_summary.limit(5))

# COMMAND ----------

website_summary = (
    website_events_df
    .filter(
        F.col("visitor_email").isNotNull()
        & (F.trim(F.col("visitor_email")) != "")
    )
    .groupBy("visitor_email")
    .agg(
        F.count("event_id").alias("total_website_events"),
        F.sum(
            F.when(
                F.col("event_type") == "purchase",
                1
            ).otherwise(0)
        ).alias("purchase_events"),
        F.sum(
            F.when(
                F.col("event_type") == "add_to_cart",
                1
            ).otherwise(0)
        ).alias("add_to_cart_events"),
        F.max("event_ts").alias("last_website_activity")
    )
)

print("Website events summary created successfully!")
display(website_summary.limit(5))

# COMMAND ----------

support_summary = (
    support_tickets_df
    .filter(
        F.col("customer_email").isNotNull()
        & (F.trim(F.col("customer_email")) != "")
    )
    .groupBy("customer_email")
    .agg(
        F.count("ticket_id").alias("total_support_tickets"),
        F.sum(
            F.when(
                F.col("status").isin("OPEN", "PENDING"),
                1
            ).otherwise(0)
        ).alias("unresolved_tickets"),
        F.max("created_at").alias("last_support_ticket_date")
    )
)

print("Support tickets summary created successfully!")
display(support_summary.limit(5))

# COMMAND ----------

customer360_df = (
    customers_df
    .join(
        orders_summary,
        customers_df.email == orders_summary.customer_email,
        "left"
    )
    .join(
        website_summary,
        customers_df.email == website_summary.visitor_email,
        "left"
    )
    .join(
        support_summary,
        customers_df.email == support_summary.customer_email,
        "left"
    )
    .drop(
        "customer_email",
        "visitor_email"
    )
    .fillna({
        "total_orders": 0,
        "lifetime_value": 0.0,
        "total_website_events": 0,
        "purchase_events": 0,
        "add_to_cart_events": 0,
        "total_support_tickets": 0,
        "unresolved_tickets": 0
    })
)

print("Customer 360 profile created successfully!")
display(customer360_df.limit(10))

# COMMAND ----------

print("Total customer records:", customer360_df.count())

print(
    "Unique customer IDs:",
    customer360_df.select("customer_id").distinct().count()
)

print(
    "Duplicate customer IDs:",
    customer360_df.groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Customers with missing email:",
    customer360_df.filter(F.col("email").isNull()).count()
)

# COMMAND ----------

customer360_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_customer_360")

print("Gold Customer 360 table saved successfully!")

# COMMAND ----------

gold_df = spark.table("gold_customer_360")

print("Gold table record count:", gold_df.count())

display(gold_df.limit(10))

# COMMAND ----------

display(
    gold_df
    .filter(F.col("email").isNotNull())
    .filter(
        (F.col("total_orders") > 0) |
        (F.col("total_website_events") > 0) |
        (F.col("total_support_tickets") > 0)
    )
    .select(
        "customer_id",
        "email",
        "total_orders",
        "lifetime_value",
        "total_website_events",
        "total_support_tickets",
        "unresolved_tickets"
    )
    .limit(10)
)