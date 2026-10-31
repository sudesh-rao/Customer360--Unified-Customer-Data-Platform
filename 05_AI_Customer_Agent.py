# Databricks notebook source
import sys

print("Python version:", sys.version)

try:
    import mlflow
    print("MLflow version:", mlflow.__version__)
except ImportError:
    print("MLflow is not installed")

try:
    import openai
    print("OpenAI library version:", openai.__version__)
except ImportError:
    print("OpenAI library is not installed")

# COMMAND ----------

# MAGIC %pip install openai

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import openai

print("OpenAI library imported successfully!")
print("OpenAI version:", openai.__version__)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("USE CATALOG customer360")
spark.sql("USE SCHEMA default")

customer_df = spark.table("gold_customer_360")

print("Customer 360 table connected successfully!")
print("Total customers:", customer_df.count())

display(customer_df.limit(5))

# COMMAND ----------

def get_customer_profile(email):

    email = email.strip().lower()

    result = (
        customer_df
        .filter(F.lower(F.trim(F.col("email"))) == email)
        .limit(1)
    )

    if result.count() == 0:
        return "Customer not found."

    return result.first().asDict()


# Test the function
get_customer_profile("aarav.bhat263@gmail.com")

# COMMAND ----------

def customer_summary(email):

    customer = get_customer_profile(email)

    if customer == "Customer not found.":
        return "I could not find a customer with that email address."

    summary = f"""
Customer Profile
----------------
Name: {customer['first_name']} {customer['last_name']}
Email: {customer['email']}
City: {customer['city']}
Customer Segment: {customer['segment']}

Purchase Information
--------------------
Total Orders: {customer['total_orders']}
Lifetime Value: ₹{customer['lifetime_value']}
Last Order Date: {customer['last_order_date']}

Website Activity
----------------
Total Website Events: {customer['total_website_events']}
Add to Cart Events: {customer['add_to_cart_events']}

Support Information
-------------------
Total Support Tickets: {customer['total_support_tickets']}
Unresolved Tickets: {customer['unresolved_tickets']}
"""

    return summary


print(customer_summary("aarav.bhat263@gmail.com"))

# COMMAND ----------

def answer_customer_question(email, question):

    customer = get_customer_profile(email)

    if customer == "Customer not found.":
        return "I could not find this customer."

    question = question.lower()

    if "spent" in question or "lifetime value" in question or "revenue" in question:
        return f"{customer['first_name']} has a lifetime value of ₹{customer['lifetime_value']}."

    elif "order" in question:
        return f"{customer['first_name']} has placed {customer['total_orders']} orders."

    elif "support" in question or "ticket" in question:
        return (
            f"{customer['first_name']} has {customer['total_support_tickets']} support tickets, "
            f"of which {customer['unresolved_tickets']} are unresolved."
        )

    elif "segment" in question:
        return f"{customer['first_name']} belongs to the {customer['segment']} customer segment."

    elif "website" in question or "activity" in question:
        return f"{customer['first_name']} has {customer['total_website_events']} website events."

    else:
        return "I cannot answer that question yet. Please ask about spending, orders, support tickets, customer segment, or website activity."


# Test questions
print(answer_customer_question(
    "aarav.bhat263@gmail.com",
    "How much has this customer spent?"
))

# COMMAND ----------

test_email = "aarav.bhat263@gmail.com"

questions = [
    "How much has this customer spent?",
    "How many orders has this customer placed?",
    "Does this customer have unresolved support tickets?",
    "Which customer segment does this customer belong to?",
    "What is this customer's website activity?"
]

for question in questions:
    print("Question:", question)
    print("Answer:", answer_customer_question(test_email, question))
    print("-" * 60)

# COMMAND ----------

def answer_customer_question(email, question):

    customer = get_customer_profile(email)

    if customer == "Customer not found.":
        return "I could not find this customer."

    question = question.lower()
    name = customer["first_name"]

    if any(word in question for word in ["spent", "lifetime value", "revenue"]):
        return f"{name}'s lifetime value is ₹{customer['lifetime_value']}."

    elif "order" in question:
        count = customer["total_orders"]
        word = "order" if count == 1 else "orders"
        return f"{name} has placed {count} {word}."

    elif any(word in question for word in ["support", "ticket"]):
        total = customer["total_support_tickets"]
        unresolved = customer["unresolved_tickets"]
        return (
            f"{name} has {total} support tickets, "
            f"of which {unresolved} are unresolved."
        )

    elif "segment" in question:
        return f"{name} belongs to the {customer['segment']} customer segment."

    elif any(word in question for word in ["website", "activity"]):
        return f"{name} has {customer['total_website_events']} website events."

    else:
        return (
            "I cannot answer that question yet. "
            "Please ask about spending, orders, support tickets, "
            "customer segment, or website activity."
        )

# COMMAND ----------

print(answer_customer_question(
    "aarav.bhat263@gmail.com",
    "How many orders has this customer placed?"
))

# COMMAND ----------

print(answer_customer_question(
    "unknown.customer@example.com",
    "How much has this customer spent?"
))

# COMMAND ----------

def search_customers_by_name(name):

    search_term = name.strip().lower()

    results = (
        customer_df
        .filter(
            F.lower(F.col("first_name")).contains(search_term) |
            F.lower(F.col("last_name")).contains(search_term)
        )
        .select(
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "segment",
            "total_orders",
            "lifetime_value"
        )
        .limit(10)
    )

    display(results)


# Test
search_customers_by_name("Aarav")

# COMMAND ----------

def get_customer_by_id(customer_id):

    result = (
        customer_df
        .filter(F.col("customer_id") == str(customer_id))
        .limit(1)
    )

    if result.count() == 0:
        return "Customer not found."

    return result.first().asDict()


# Test
get_customer_by_id("1238")

# COMMAND ----------

def find_customer(identifier):

    identifier = identifier.strip()

    if "@" in identifier:
        return get_customer_profile(identifier)

    return get_customer_by_id(identifier)


# Test using customer ID
print(find_customer("1238"))

# Test using email
print(find_customer("aarav.bhat263@gmail.com"))

# COMMAND ----------

def answer_customer_question(identifier, question):

    customer = find_customer(identifier)

    if customer == "Customer not found.":
        return "I could not find this customer."

    question = question.lower()
    name = customer["first_name"]

    if "last purchase" in question or "last order" in question:
        return f"{name}'s last delivered order date was {customer['last_order_date']}."

    elif "add to cart" in question or "cart" in question:
        return f"{name} has {customer['add_to_cart_events']} add-to-cart events."

    elif "purchase event" in question:
        return f"{name} has {customer['purchase_events']} purchase events on the website."

    elif "last support" in question:
        return f"{name}'s last support ticket date was {customer['last_support_ticket_date']}."

    elif "city" in question or "location" in question:
        return f"{name} is registered in {customer['city']}."

    elif any(word in question for word in ["spent", "lifetime value", "revenue"]):
        return f"{name}'s lifetime value is ₹{customer['lifetime_value']}."

    elif "order" in question:
        count = customer["total_orders"]
        word = "order" if count == 1 else "orders"
        return f"{name} has placed {count} {word}."

    elif any(word in question for word in ["support", "ticket"]):
        total = customer["total_support_tickets"]
        unresolved = customer["unresolved_tickets"]
        return f"{name} has {total} support tickets, of which {unresolved} are unresolved."

    elif "segment" in question:
        return f"{name} belongs to the {customer['segment']} customer segment."

    elif any(word in question for word in ["website", "activity"]):
        return f"{name} has {customer['total_website_events']} website events."

    else:
        return "I cannot answer that question yet."

# COMMAND ----------

print(answer_customer_question("1238", "When was the last purchase?"))
print(answer_customer_question("1238", "How many support tickets are unresolved?"))
print(answer_customer_question("1238", "Which city is this customer from?"))

# COMMAND ----------

def get_customer_risk(identifier):

    customer = find_customer(identifier)

    if customer == "Customer not found.":
        return "Customer not found."

    unresolved = customer["unresolved_tickets"]

    if unresolved >= 3:
        risk = "HIGH PRIORITY"
    elif unresolved >= 1:
        risk = "NEEDS ATTENTION"
    else:
        risk = "HEALTHY"

    return {
        "customer_name": customer["first_name"] + " " + customer["last_name"],
        "unresolved_tickets": unresolved,
        "customer_status": risk
    }


# Test
print(get_customer_risk("1238"))

# COMMAND ----------

risk_test = (
    customer_df
    .filter(F.col("email").isNotNull())
    .groupBy("email", "first_name", "last_name")
    .agg(F.max("unresolved_tickets").alias("unresolved_tickets"))
    .orderBy(F.col("unresolved_tickets").desc())
)

display(risk_test.limit(10))

# COMMAND ----------

print(get_customer_risk("sneha.singh3@hotmail.com"))

# COMMAND ----------

risk_overview = (
    customer_df
    .withColumn(
        "customer_status",
        F.when(F.col("unresolved_tickets") >= 3, "HIGH PRIORITY")
         .when(F.col("unresolved_tickets") >= 1, "NEEDS ATTENTION")
         .otherwise("HEALTHY")
    )
    .groupBy("customer_status")
    .agg(
        F.count("customer_id").alias("total_customers")
    )
    .orderBy("customer_status")
)

display(risk_overview)

# COMMAND ----------

risk_overview.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_customer_risk_overview")

print("Customer risk overview saved successfully!")

# COMMAND ----------

saved_risk_df = spark.table("gold_customer_risk_overview")

print("Saved risk overview records:", saved_risk_df.count())

display(saved_risk_df)

# COMMAND ----------

customer_identifier = input("Enter Customer ID or Email: ")

question = input("Ask a question about this customer: ")

answer = answer_customer_question(
    customer_identifier,
    question
)

print("\nCustomer Assistant:")
print(answer)

# COMMAND ----------

def get_business_kpis():

    kpis = (
        customer_df
        .agg(
            F.count("customer_id").alias("total_customers"),
            F.sum("total_orders").alias("total_orders"),
            F.round(F.sum("lifetime_value"), 2).alias("total_revenue"),
            F.sum("total_support_tickets").alias("total_support_tickets"),
            F.sum("unresolved_tickets").alias("unresolved_tickets")
        )
        .first()
        .asDict()
    )

    return kpis


print(get_business_kpis())

# COMMAND ----------

def answer_business_question(question):

    question = question.lower()
    kpis = get_business_kpis()

    if "how many customers" in question or "total customers" in question:
        return f"ShopSphere has {kpis['total_customers']} customers."

    elif "total orders" in question:
        return f"ShopSphere has processed {kpis['total_orders']} orders."

    elif "total revenue" in question or "revenue" in question:
        return f"ShopSphere's total delivered-order revenue is ₹{kpis['total_revenue']:,.2f}."

    elif "support tickets" in question:
        return f"ShopSphere has {kpis['total_support_tickets']} support tickets."

    elif "unresolved" in question:
        return f"ShopSphere has {kpis['unresolved_tickets']} unresolved support tickets."

    else:
        return "I cannot answer that business question yet."


# Test
print(answer_business_question("What is the total revenue?"))

# COMMAND ----------

business_questions = [
    "How many customers do we have?",
    "How many total orders have we processed?",
    "What is our total revenue?",
    "How many support tickets do we have?",
    "How many unresolved tickets are there?"
]

for question in business_questions:
    print("Question:", question)
    print("Answer:", answer_business_question(question))
    print("-" * 60)

# COMMAND ----------

def shopsphere_assistant(question, identifier=None):

    if identifier:
        return answer_customer_question(identifier, question)

    return answer_business_question(question)


# Test 1: Business question
print(shopsphere_assistant("What is our total revenue?"))

# Test 2: Customer question
print(shopsphere_assistant(
    "How many orders has this customer placed?",
    "1238"
))

# COMMAND ----------

print("Welcome to ShopSphere Customer Intelligence Assistant!")
print("Enter 'exit' to stop.\n")

while True:

    identifier = input("Enter Customer ID or Email (press Enter for business questions): ").strip()

    question = input("Ask your question: ").strip()

    if question.lower() == "exit":
        print("Thank you for using ShopSphere Assistant!")
        break

    answer = shopsphere_assistant(
        question,
        identifier if identifier else None
    )

    print("\nAssistant:", answer)
    print("-" * 60)

# COMMAND ----------

def answer_customer_risk_question(identifier):
    risk = get_customer_risk(identifier)

    if risk == "Customer not found.":
        return "I could not find this customer."

    count = risk["unresolved_tickets"]
    ticket_word = "ticket" if count == 1 else "tickets"

    return (
        f"{risk['customer_name']} is classified as "
        f"{risk['customer_status']}. "
        f"They have {count} unresolved support {ticket_word}."
    )

# COMMAND ----------

print(answer_customer_risk_question("1238"))
print(answer_customer_risk_question("sneha.singh3@hotmail.com"))

# COMMAND ----------

def shopsphere_assistant(question, identifier=None):

    if identifier:
        if any(word in question.lower() for word in ["risk", "priority", "health"]):
            return answer_customer_risk_question(identifier)

        return answer_customer_question(identifier, question)

    return answer_business_question(question)

# COMMAND ----------

# Test 1: Customer risk question
print(shopsphere_assistant(
    "What is this customer's risk status?",
    "1238"
))

# Test 2: Customer spending question
print(shopsphere_assistant(
    "How much has this customer spent?",
    "1238"
))

# Test 3: Business revenue question
print(shopsphere_assistant("What is our total revenue?"))