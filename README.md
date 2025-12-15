# Payment Reconciliation Data Warehouse

## 📌 Project Overview

This project implements an **end-to-end Payment Reconciliation Data Warehouse** using **Azure Data Platform** and **Databricks Delta Live Tables (DLT)**. The goal is to ingest raw transactional data from multiple sources, transform it into analytics-ready dimensions and facts, and perform **financial reconciliation** between **transactions, settlements, and bank records**.

The solution is designed with **scalability, reliability, and auditability** in mind and follows a **Medallion Architecture (Bronze → Silver → Gold)**.

---

## 🏗️ Architecture

**Data Sources**

* Azure SQL Database
* JSON / Parquet files

**Ingestion Layer**

* Azure Data Factory (ADF)
* Incremental ingestion pipelines

**Storage**

* Azure Data Lake Storage Gen2 (ADLS)

**Processing & Transformation**

* Azure Databricks
* Delta Live Tables (DLT)
* PySpark

**Serving Layer**

* Reconciliation Views
* Analytics & Reporting (Power BI / SQL)

---

## 🧱 Data Model

### Fact Tables

* **fact_transactions** – Customer payment transactions
* **fact_settlements** – Bank settlement records

### Dimension Tables

* **dim_users** – User / customer details
* **dim_merchants** – Merchant master data

### Reconciliation Views

* Matched transactions
* Missing settlements
* Amount mismatches
* Status-based reconciliation

---

## 📂 Project Structure

```
payment-reconciliation-datawarehouse-main/
│
├── PaymentReconciliation/
│   └── PaymentReconciliation/
│       ├── Autoloader.py
│       ├── Reconciliation_Views.py
│       ├── DLT/
│       │   ├── transformations/
│       │   │   ├── Users.py
│       │   │   ├── Merchants.py
│       │   │   ├── Transactions.py
│       │   │   └── Settlements.py
│       │   ├── utilities/
│       │   │   └── utils.py
│       │   └── explorations/
│       └── util/
│           └── Transformations.py
│
├── dataset/
│   ├── AzureSql.json
│   ├── Json_dynamic.json
│   └── Parquet_dynamic.json
│
├── factory/
│   └── RawIngestionADF.json
│
├── pipeline/
│   └── Incremental_Ingestion.json
│
├── linkedService/
│   ├── ls_AzureDataLakeStorage.json
│   └── ls_AzureSqlDatabase.json
│
└── README.md
```

---

## 🔄 Data Flow

1. **Raw Ingestion**

   * Azure Data Factory ingests data from Azure SQL and file-based sources
   * Supports incremental loading

2. **Bronze Layer**

   * Raw data stored in Delta format
   * Schema-on-read

3. **Silver Layer**

   * Cleaned and standardized datasets
   * DLT transformations for users, merchants, transactions, and settlements

4. **Gold Layer**

   * Business-ready tables
   * Reconciliation views for finance and audit teams

---

## 🧮 Reconciliation Logic

The reconciliation process compares:

* **Transaction Amount vs Settlement Amount**
* **Transaction Status vs Settlement Status**
* **Missing or Duplicate Records**

### Example Scenarios

* Transaction exists but settlement missing
* Settlement exists but transaction missing
* Amount mismatch
* Fully matched and reconciled records

---

## 🛠️ Technologies Used

* Azure Data Factory
* Azure Data Lake Storage Gen2
* Azure SQL Database
* Azure Databricks
* Delta Lake / Delta Live Tables
* PySpark
* SQL

---

## 🚀 How to Run

1. Deploy linked services in Azure Data Factory
2. Import ADF pipelines from `/factory` and `/pipeline`
3. Configure datasets and parameters
4. Deploy Databricks notebooks
5. Run DLT pipelines
6. Query reconciliation views for analysis

---

## 📊 Use Cases

* Financial reconciliation
* Payment audit and compliance
* Revenue assurance
* Data quality monitoring

---

## 🔮 Future Enhancements

* Automated data quality checks
* Alerting for reconciliation failures
* CI/CD pipeline integration
* Real-time streaming ingestion

---

## 👤 Author

**Rahul Mandaviya**
Data Engineer | Analytics & BI

---
