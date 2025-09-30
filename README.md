# 📦 Business Data Warehouse (From Raw CSV to Gold Layer)

## 📖 Project Description  
**Business Data Warehouse (From Raw CSV to Gold Layer)** is an end-to-end data engineering project that demonstrates how raw business data can be transformed into valuable insights.  

The project integrates **CRM and ERP datasets** stored as CSV files, processes them through a structured **ETL pipeline (Bronze → Silver → Gold)**, and delivers a clean, production-ready **Data Warehouse** that can be directly consumed by BI tools such as **Power BI** or **Tableau**.  

This project is designed as a **blueprint for companies and clients** who want to modernize their data infrastructure and unlock actionable insights from raw data.  

## 🏗️ Data Architecture
### The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
<img width="1544" height="912" alt="Untitled design" src="https://github.com/user-attachments/assets/de5a2496-aeef-4398-80f3-2d52e5a7cd17" />
## 🏗️ Data Warehouse Layers  

1. **Bronze Layer**  
   - Stores raw data *as-is* from the source systems.  
   - Data is ingested from **CSV files** into the **SQL Server/MySQL Database**.  

2. **Silver Layer**  
   - Performs **data cleansing, standardization, and normalization**.  
   - Prepares data for reliable analysis by removing duplicates and applying business rules.  

3. **Gold Layer**  
   - Houses **business-ready data** modeled into a **Star Schema**.  
   - Contains **fact and dimension tables** required for reporting and analytics.  
---

## 🚀 Key Features  
- **Data Ingestion (Bronze Layer):** Load raw CRM & ERP CSV files into staging tables.  
- **Data Transformation (Silver Layer):** Clean, standardize, and apply business rules.  
- **Data Modeling (Gold Layer):** Create star-schema-style fact and dimension tables for analytics.  
- **Business-Ready Outputs:** Customer 360, Product Insights, and Sales Fact tables.  
- **Scalability:** Easily extendable for additional business domains and datasets.  

---
## 🛠️ Tech Stack  
- **MySQL** – Data Warehouse & SQL transformations  
- **SQL (DDL & DML)** – ETL and business logic  
- **CSV Files** – Raw data source  
- *(Optional for visualization)* Power BI / Tableau  

---

## 📂 Data Pipeline Overview  
**Bronze → Silver → Gold** architecture ensures clean, reliable, and analytics-ready data:  

- **Bronze Layer:** Raw ingestion from CSV files  
- **Silver Layer:** Data cleaning, deduplication, and standardization  
- **Gold Layer:** Final fact & dimension tables for reporting  

---
## 📈 Business Use Cases  
- **Customer 360 Analysis** – Customer demographics and segmentation  
- **Sales Performance** – Revenue, quantity, and pricing trends  
- **Product Insights** – Category performance, product lifecycle tracking  
- **Time-Based Analytics** – Daily, monthly, and yearly reporting (with a date dimension)  

---
