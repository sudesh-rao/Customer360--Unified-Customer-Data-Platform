# Customer 360 AI – Unified Customer Intelligence Platform

A data engineering portfolio project built using **Databricks, PySpark, SQL, and Delta Lake** to combine customer, order, website activity, and support data into a unified Customer 360 platform.

The project follows a Medallion Architecture (Bronze, Silver, and Gold) and includes a Python-based customer intelligence assistant for answering customer and business questions.


## Project Overview

Businesses often store customer information across multiple systems, making it difficult to understand customer behaviour in one place.

This project solves that problem by integrating four different datasets:

- **Customers:** Customer details, contact information, city, and segment.
- **Orders:** Order history, purchase amounts, dates, and order status.
- **Website Events:** Customer browsing activity, add-to-cart events, and purchase events.
- **Support Tickets:** Customer complaints, ticket status, priority, and issue type.

The data is processed through Bronze, Silver, and Gold layers to create a unified Customer 360 table.

The final platform supports customer-level analysis, business KPI reporting, and a rule-based Python assistant that answers questions using the processed data.
