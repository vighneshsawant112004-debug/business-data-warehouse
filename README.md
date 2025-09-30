# 📦 Business Data Warehouse (From Raw CSV to Gold Layer)

## 📖 Project Description  
**Business Data Warehouse (From Raw CSV to Gold Layer)** is an end-to-end data engineering project that demonstrates how raw business data can be transformed into valuable insights.  

The project integrates **CRM and ERP datasets** stored as CSV files, processes them through a structured **ETL pipeline (Bronze → Silver → Gold)**, and delivers a clean, production-ready **Data Warehouse** that can be directly consumed by BI tools such as **Power BI** or **Tableau**.  

This project is designed as a **blueprint for companies and clients** who want to modernize their data infrastructure and unlock actionable insights from raw data.  

## Data Architecture
<img width="1544" height="912" alt="Untitled design" src="https://github.com/user-attachments/assets/de5a2496-aeef-4398-80f3-2d52e5a7cd17" />
* 1]Bronze Layer * : Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
* 2]Silver Layer *: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
* 3]Gold Layer *: Houses business-ready data modeled into a star schema required for reporting and analytics.
