# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG customer360;
# MAGIC
# MAGIC USE SCHEMA default;
# MAGIC
# MAGIC SELECT COUNT(*) AS total_customers
# MAGIC FROM gold_customer_360;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     segment,
# MAGIC     total_orders,
# MAGIC     lifetime_value
# MAGIC FROM gold_customer_360
# MAGIC WHERE lifetime_value > 0
# MAGIC ORDER BY lifetime_value DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     segment,
# MAGIC     COUNT(*) AS total_customers,
# MAGIC     SUM(total_orders) AS total_orders,
# MAGIC     ROUND(SUM(lifetime_value), 2) AS total_revenue,
# MAGIC     ROUND(AVG(lifetime_value), 2) AS average_customer_value
# MAGIC FROM gold_customer_360
# MAGIC GROUP BY segment
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     total_support_tickets,
# MAGIC     unresolved_tickets,
# MAGIC     last_support_ticket_date
# MAGIC FROM gold_customer_360
# MAGIC WHERE unresolved_tickets > 0
# MAGIC ORDER BY unresolved_tickets DESC,
# MAGIC          last_support_ticket_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     email,
# MAGIC     total_website_events,
# MAGIC     add_to_cart_events,
# MAGIC     total_orders,
# MAGIC     segment
# MAGIC FROM gold_customer_360
# MAGIC WHERE total_website_events >= 5
# MAGIC   AND total_orders = 0
# MAGIC ORDER BY total_website_events DESC,
# MAGIC          add_to_cart_events DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_customers,
# MAGIC     SUM(total_orders) AS total_orders,
# MAGIC     ROUND(SUM(lifetime_value), 2) AS total_revenue,
# MAGIC     ROUND(AVG(lifetime_value), 2) AS average_customer_lifetime_value,
# MAGIC     SUM(total_website_events) AS total_website_events,
# MAGIC     SUM(total_support_tickets) AS total_support_tickets,
# MAGIC     SUM(unresolved_tickets) AS unresolved_support_tickets
# MAGIC FROM gold_customer_360;