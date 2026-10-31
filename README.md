# STEDI Human Balance Analytics

**WGU D609 — Data Analytics at Scale**  
**Udacity Spark and Human Balance Project**

## Project Overview

This project implements a data lakehouse solution for STEDI Human Balance Analytics using AWS Glue, Apache Spark (PySpark), Amazon S3, and Amazon Athena.

The goal is to process customer, accelerometer, and Step Trainer sensor data into trusted and curated datasets suitable for machine learning, while ensuring that only data associated with customers who consented to research is included.

## Technologies Used

- Amazon S3 — Data storage for landing, trusted, and curated zones
- AWS Glue Studio — Visual ETL development and execution
- Apache Spark / PySpark — Distributed data processing
- AWS Glue Data Catalog — Table metadata management
- Amazon Athena — SQL querying and data validation
- Python and SQL — ETL scripts and table definitions
- GitHub — Source code and project documentation

## Data Lakehouse Architecture

The solution uses three data processing zones:

### 1. Landing Zone

Raw JSON data is ingested from three sources:

- `customer_landing` — Customer registration and research-consent information
- `accelerometer_landing` — Mobile accelerometer measurements
- `step_trainer_landing` — Step Trainer sensor measurements

AWS Glue Data Catalog tables describe the JSON datasets, allowing the records to be queried through Athena.

### 2. Trusted Zone

AWS Glue ETL jobs transform the landing data into trusted datasets.

- `customer_trusted` — Retains customers who agreed to share their information for research.
- `accelerometer_trusted` — Retains accelerometer measurements associated with research-consenting customers.
- `step_trainer_trusted` — Retains Step Trainer readings associated with eligible customers through curated customer serial numbers.

### 3. Curated Zone

The curated datasets prepare the data for downstream analytics and machine learning.

- `customer_curated` — Retains eligible customers with associated accelerometer activity.
- `machine_learning_curated` — Combines trusted accelerometer and Step Trainer readings using matching timestamps to produce a machine-learning-ready dataset.

## Data Validation Results

The following record counts were validated using Amazon Athena:

| Data Zone | Dataset | Record Count |
|---|---|---:|
| Landing | Customer Landing | 956 |
| Landing | Accelerometer Landing | 81,273 |
| Landing | Step Trainer Landing | 28,680 |
| Trusted | Customer Trusted | 482 |
| Trusted | Accelerometer Trusted | 40,981 |
| Trusted | Step Trainer Trusted | 14,460 |
| Curated | Customer Curated | 482 |
| Curated | Machine Learning Curated | 43,681 |

The resulting datasets match the expected record counts specified in the Udacity project requirements.

## Data Privacy and Quality

Data privacy is a central requirement of this project.

The ETL pipeline filters customer records based on research consent and limits accelerometer and Step Trainer data to eligible customers.

The project also addresses a known data quality issue involving duplicated customer serial numbers. A SQL membership filter is used when preparing trusted Step Trainer data to avoid multiplying sensor records through duplicate serial-number matches.

## Python ETL Scripts

- `customer_landing_to_trusted.py`
- `accelerometer_landing_to_trusted.py`
- `customer_trusted_to_curated.py`
- `step_trainer_trusted.py`
- `machine_learning_curated.py`

## SQL Table Definitions

- `customer_landing.sql`
- `accelerometer_landing.sql`
- `step_trainer_landing.sql`

These scripts define the three landing-zone tables, including JSON field names, data types, and Amazon S3 locations.

## Project Evidence

The `screenshots/` directory contains screenshots documenting AWS Glue workflows and Amazon Athena query results.

The evidence includes data validation and record-count checks across the landing, trusted, and curated zones.

## Project Outcome

The completed pipeline transforms raw STEDI customer and sensor data into validated trusted and curated datasets, producing **43,681 records** in the final machine learning curated table.

The project demonstrates cloud-based data engineering, ETL development, SQL transformations, data quality validation, and privacy-aware data processing.
