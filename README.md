# Customer 360 – Unified Customer Data Platform

An end-to-end data engineering portfolio project built using **Databricks, PySpark, SQL, Python, and Delta Lake** to integrate customer, order, website activity, and support data into a unified Customer 360 platform.

The project follows the **Medallion Architecture (Bronze, Silver, and Gold)** and includes data transformation, SQL analytics, business KPI reporting, and a Python-based query assistant.

## Project Overview

Businesses often store customer information across multiple systems, making it difficult to understand customer behaviour in one place.

This project addresses that challenge by integrating four datasets:

- **Customers:** Customer details, contact information, city, and segment.
- **Orders:** Order history, purchase amounts, dates, and order status.
- **Website Events:** Customer browsing activity, add-to-cart events, and purchase events.
- **Support Tickets:** Customer complaints, ticket status, priority, and issue type.

The data is processed through Bronze, Silver, and Gold layers to create a unified Customer 360 table.

The final platform supports customer-level analysis, business KPI reporting, and customer support insights.

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Cloud-based data engineering platform |
| PySpark | Data ingestion, cleaning, transformation, and processing |
| SQL | Business analytics and KPI reporting |
| Delta Lake | Reliable storage for Bronze, Silver, and Gold tables |
| Unity Catalog | Data catalog and table organisation |
| Python | Query assistant and business logic |
| GitHub | Project version control and documentation |

## Architecture

The project follows the Medallion Architecture:

**Source CSV Files → Bronze → Silver → Gold → SQL Analytics → Customer Query Assistant**

- **Bronze:** Ingests raw customer, order, website event, and support ticket data.
- **Silver:** Cleans, standardises, validates, and removes duplicate records.
- **Gold:** Aggregates and combines the cleaned datasets into a unified Customer 360 profile.
- **Analytics:** Uses SQL to calculate business KPIs and identify customer trends.
- **Query Assistant:** Uses Python and the Gold Customer 360 table to answer predefined customer and business questions.

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
- Converted date and timestamp columns into appropriate formats.
- Removed duplicate customer, order, event, and ticket records.
- Applied data validation and filtering rules.
- Used window functions to identify and retain unique records.

| Dataset | Bronze Records | Silver Records |
|---|---:|---:|
| Customers | 2,100 | 2,000 |
| Orders | 6,080 | 6,000 |
| Website Events | 20,150 | 20,000 |
| Support Tickets | 1,830 | 1,800 |
| **Total** | **30,160** | **29,800** |

**Result:** Removed 360 duplicate records across the four datasets.

### 3. Gold Layer – Customer 360

Created a unified customer profile by aggregating order, website activity, and support ticket data before joining them with customer information using email-based matching.

The Gold table contains:

- Customer details and segments.
- Total orders and delivered-order lifetime value.
- Last delivered order date.
- Website activity, purchase events, and add-to-cart events.
- Total support tickets and unresolved tickets.

Aggregating the source datasets before joining helps avoid duplicate customer rows caused by one-to-many relationships.

**Final Gold Records: 2,000 customer profiles**

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

## Customer Query Assistant

Developed a Python-based, rule-driven query assistant that retrieves information from the Gold Customer 360 table.

### Customer-Level Capabilities

- Find customers using email addresses or customer IDs.
- Retrieve customer spending and order information.
- Check last purchase dates and website activity.
- Retrieve support ticket details.
- Classify customer support risk based on unresolved ticket counts.

### Business-Level Capabilities

- Retrieve total customer and order counts.
- Calculate total delivered-order revenue.
- Retrieve support ticket and unresolved ticket counts.
- Answer predefined business questions using aggregated KPIs.

The assistant uses Python functions and predefined question-matching rules to retrieve and present information from the processed data.

Customer risk classification is based on predefined rules using unresolved support ticket counts.


## Disclaimer

This is a personal portfolio project built using synthetic datasets.
