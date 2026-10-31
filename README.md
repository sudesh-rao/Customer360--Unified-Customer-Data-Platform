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




## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Cloud-based data engineering platform |
| PySpark | Data cleaning, transformation, and processing |
| SQL | Business analytics and KPI reporting |
| Delta Lake | Reliable storage for Bronze, Silver, and Gold tables |
| Unity Catalog | Data catalog and table organisation |
| Python | Customer intelligence assistant and business logic |
| GitHub | Project version control and documentation |

## Architecture

The project follows the Medallion Architecture:

**Source CSV Files → Bronze → Silver → Gold → Customer Intelligence Assistant**

- **Bronze:** Ingests raw customer, order, website event, and support ticket data.
- **Silver:** Cleans, standardises, validates, and removes duplicate records.
- **Gold:** Combines the cleaned datasets into a unified Customer 360 profile.
- **Analytics:** Uses SQL to calculate business KPIs and identify customer trends.
- **Assistant:** Uses Python and the Gold Customer 360 table to answer customer and business questions.
