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


## Data Pipeline Implementation

### 1. Bronze Layer – Raw Data Ingestion

Ingested four CSV datasets into Databricks and stored them as Delta tables.

| Dataset | Records Ingested |
|---|---:|
| Customers | 2,100 |
| Orders | 6,080 |
| Website Events | 20,150 |
| Support Tickets | 1,830 |
| **Total** | **30,160** |

### 2. Silver Layer – Data Cleaning and Transformation

Applied data cleaning and standardisation techniques using PySpark.

- Standardised email addresses, names, phone numbers, and text fields.
- Converted date columns into appropriate date formats.
- Removed duplicate customer, order, event, and ticket records.
- Applied data validation and filtering rules.

| Dataset | Bronze Records | Silver Records |
|---|---:|---:|
| Customers | 2,100 | 2,000 |
| Orders | 6,080 | 6,000 |
| Website Events | 20,150 | 20,000 |
| Support Tickets | 1,830 | 1,800 |
| **Total** | **30,160** | **29,800** |

### 3. Gold Layer – Customer 360

Combined the four cleaned datasets into a unified customer profile using email-based joins and aggregations.

The Gold table contains:

- Customer details and segments.
- Total orders and delivered-order lifetime value.
- Last delivered order date.
- Website activity and purchase events.
- Total support tickets and unresolved tickets.

**Final Gold Records: 2,000 customers**



## Business Analytics

Created SQL queries to analyse customer behaviour and business performance.

The analysis includes:

- Top customers ranked by lifetime value.
- Revenue and order distribution by customer segment.
- Customers with unresolved support tickets.
- Customers with high website engagement but no orders.
- Overall business performance KPIs.

### Key Business Metrics

| Metric | Result |
|---|---:|
| Total Customers | 2,000 |
| Total Orders | 5,237 |
| Total Delivered-Order Revenue | ₹3,299,349.82 |
| Average Customer Lifetime Value | ₹1,649.67 |
| Website Events | 10,660 |
| Support Tickets | 1,692 |
| Unresolved Support Tickets | 422 |

## Customer Intelligence Assistant

Developed a Python-based, rule-driven assistant that retrieves information from the Gold Customer 360 table.

### Customer-Level Questions
- Find customers using their email address or customer ID.
- Retrieve customer spending and order information.
- Check website activity and support ticket details.
- Identify customers who need attention based on unresolved support tickets.

### Business-Level Questions
- How many customers do we have?
- What is our total revenue?
- How many orders have been processed?
- How many support tickets remain unresolved?

The assistant uses Python functions and predefined question-matching rules to retrieve and present answers from the processed data. It does not currently use an external LLM.




## Project Structure

```text
customer-360-ai/
│
├── README.md
│
└── notebooks/
    ├── 01_Bronze_Ingestion.py
    ├── 02_Silver_Transformation.py
    ├── 03_Gold_Customer360.py
    ├── 04_SQL_Analytics.py
    └── 05_AI_Customer_Agent.py
```

## Project Status

- [x] Bronze data ingestion
- [x] Silver data cleaning and transformation
- [x] Gold Customer 360 data model
- [x] SQL business analytics
- [x] Python-based customer intelligence assistant
- [x] Customer risk classification
- [x] GitHub documentation and notebook uploads

## Disclaimer

This is a personal portfolio project built using synthetic datasets. It demonstrates data engineering concepts and is not a production customer data platform.
