
# Online Retail Azure Data Engineering Project

## 📌 Project Overview

This project demonstrates an **end‑to‑end Azure Data Engineering pipeline** built using real-world best practices. The system ingests retail CSV data, processes it using Databricks, stores curated Delta tables in ADLS Gen2, and orchestrates workflows using Azure Data Factory.

This project was designed to simulate a **production‑grade Azure Data Engineering solution** suitable for interview preparation and real-world learning.

---

## 🎯 Business Use Case

A retail company wants to analyze:

* Daily revenue trends
* Top-selling products
* Customer purchase behavior
* Country-wise sales distribution

The company needs a scalable data pipeline to automate ingestion, transformation, and reporting.

---

## 🏗️ Architecture

Raw Data → ADLS Gen2 → Databricks Transform → Delta Tables → ADF Orchestration → Reporting Layer

### Layers Implemented

1. **Raw Layer** – Landing zone for CSV data
2. **Curated Layer** – Cleaned fact & dimension tables
3. **Presentation Layer** – Aggregated reporting tables
4. **Orchestration Layer** – Automated pipeline with Azure Data Factory

---

## 🧰 Technologies Used

* Azure Data Lake Storage Gen2
* Azure Databricks
* Azure Data Factory
* Delta Lake
* PySpark
* Azure Key Vault
* Git

---

## 📂 ADLS Folder Structure

```
datalake
 ├── raw
 │    ├── sql
 │    │    └── online_retail
 │    ├── api
 │    └── stream
 ├── curated
 │    ├── sales
 │    ├── dimensions
 └── presentation
      └── reporting
```

---

## 📊 Dataset

Dataset used: Online Retail Dataset
Contains:

* InvoiceNo
* StockCode
* Description
* Quantity
* InvoiceDate
* UnitPrice
* CustomerID
* Country

Used to simulate retail sales analytics.

---

## 🚀 Pipeline Steps

### Step 1 – Data Ingestion

* CSV files uploaded into ADLS raw layer
* Azure Data Factory pipeline created to ingest files
* Logging and validation enabled

### Step 2 – Databricks Setup

* Databricks workspace created
* ADLS access configured using service principal & Key Vault
* Notebook created for mounting and reading data

### Step 3 – Data Transformation

Notebook: `nb_retail_transform`

* Read raw CSV using ABFS path
* Fix timestamp parsing issues
* Remove nulls & duplicates
* Create Fact and Dimension tables
* Write Delta tables into curated layer

### Step 4 – Presentation Layer

Notebook: `nb_retail_reporting`
Created business reports:

* Daily revenue trend
* Top 10 products by revenue
* Customer lifetime value
* Country-wise sales

Also implemented:

* Incremental load logic
* Data quality checks
* Late arriving data handling

### Step 5 – Orchestration (Azure Data Factory)

Pipeline: `PL_OnlineRetail_ETL`

* Trigger Databricks notebooks
* Parameterized execution
* Retry & timeout handling
* Scheduled daily runs
* Monitoring via ADF Monitor

---

## 🔄 Incremental Load Logic

Implemented using:

* Watermark column (InvoiceDate)
* Pipeline parameters
* Merge into Delta tables

Benefits:

* Faster processing
* Production-ready design
* Handles new daily files

---

## 🧪 Data Quality Checks

* Null checks
* Duplicate removal
* Schema validation
* Timestamp parsing fixes
* Invalid record logging

---

## 📈 Reporting Output

Final tables used for BI tools:

* `fact_sales`
* `dim_customer`
* `dim_product`
* `daily_sales_report`
* `country_sales_report`

Ready for Power BI integration.

---

## 🧠 Key Learnings

* End‑to‑end Azure Data Engineering pipeline design
* ADLS Gen2 access configuration
* Databricks Delta Lake processing
* Incremental loading strategy
* Azure Data Factory orchestration
* Production-ready logging & monitoring

---

## 📝 How to Run This Project

1. Create Azure resources (ADLS, Databricks, ADF, Key Vault)
2. Upload dataset to raw layer
3. Run Databricks notebooks
4. Publish ADF pipeline
5. Trigger pipeline
6. Validate curated & presentation tables

---

## ⭐ Resume Description

Built an end‑to‑end Azure Data Engineering pipeline using ADLS Gen2, Databricks, Delta Lake, and Azure Data Factory, implementing incremental loads, curated Delta tables, parameterized orchestration, and automated scheduling for retail analytics.

---

## 📬 Future Improvements

* Add streaming ingestion using Event Hubs
* Add CI/CD using Azure DevOps
* Add Power BI dashboards
* Add data quality alerts

---

## 👨‍💻 Author

Saikumar Martha
.NET Developer transitioning into Azure Data Engineering
Texas, USA

---

## ⭐ If you found this useful

Give the repo a star and connect with me on LinkedIn!
