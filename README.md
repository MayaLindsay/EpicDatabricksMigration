# EpicDatabricksMigration
Healthcare organizations generate large volumes of clinical and operational data through their electronic health record systems, this one being Epic Dimensions. Turning that data into decisions requires a data engineering architecture that can support complex transformations, data quality, scalability, governance, and downstream reporting.

# Healthcare Revenue Cycle Data Engineering Pipeline

## Project Overview

This project demonstrates an end-to-end healthcare data engineering pipeline designed to support Revenue Cycle Management (RCM) analytics.

The solution demonstrates how Epic-derived healthcare data can be ingested, transformed, validated, modeled, and prepared for business intelligence reporting using Databricks, PySpark, SQL, Python, and Power BI.


## Business Use Case

Healthcare Revenue Cycle teams need reliable data to monitor financial performance, claims processing, denials, payments, and outstanding accounts receivable.

This project demonstrates a data engineering solution that prepares data for metrics such as:

* Accounts Receivable (A/R) Days
* Claim Volume
* Billed Revenue
* Allowed Revenue
* Paid Revenue
* Outstanding A/R
* Denial Rate
* Denial Dollars
* Claim Status
* Payer Performance
* Payment Trends


## Technology Stack

| Technology    | Purpose                                    |
| ------------- | ------------------------------------------ |
| Databricks    | Data engineering and lakehouse platform    |
| PySpark       | Data transformation and processing         |
| Python        | Synthetic data generation and automation   |
| SQL           | Data transformation and analytical queries |
| Delta Lake    | Reliable data storage and table management |
| Unity Catalog | Data governance and organization           |
| Power BI      | Business intelligence and visualization    |
| GitHub        | Source control and project documentation   |


## Repository Structure



## Example Revenue Cycle Data Model


The dimensional model is designed to support analytical queries and Power BI reporting while separating transactional facts from descriptive dimensions.


