# 🎬 Netflix AWS Glue ETL & Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-blue) ![AWS Glue](https://img.shields.io/badge/AWS-Glue-orange) ![Apache Spark](https://img.shields.io/badge/Apache-Spark-red) ![Amazon S3](https://img.shields.io/badge/Amazon-S3-green) ![Amazon Athena](https://img.shields.io/badge/Amazon-Athena-purple)

> A production-ready, incremental ETL pipeline for processing Netflix datasets using AWS Glue, PySpark, and the Medallion Architecture (Bronze → Silver → Gold)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [Workflow Orchestration](#-workflow-orchestration)
- [License](#-license)

---

## 🎯 Overview

This project implements a scalable, incremental data pipeline designed to process Netflix datasets across multiple domains — titles, cast, directors, categories, and countries. The system ingests raw CSV files from Amazon S3, performs data validation and transformation using AWS Glue and PySpark, stores processed data in optimized Parquet format, and registers all outputs as **External Tables** in the AWS Glue Data Catalog for querying via Amazon Athena.

### Key Highlights

- ⚡ **Incremental Loading** — Job Bookmark-based processing, only new files are picked up each run
- 🥇 **Medallion Architecture** — Clean separation of Bronze, Silver, and Gold layers
- 📊 **Analytics Ready** — External tables registered in Glue Catalog, instantly queryable via Athena
- 🔄 **Automated Workflows** — End-to-end orchestration with AWS Glue Workflows and Triggers
- 🏗️ **Visual ETL** — Pipeline built using AWS Glue Visual ETL with auto-generated scripts
- 🔒 **Secure** — IAM role-based access control for all AWS resources

---

## ✨ Features

### 🔄 ETL Pipeline Capabilities

- **Incremental Ingestion** — Job Bookmark tracks processed files, avoiding reprocessing
- **Data Validation** — Schema validation and data quality enforcement
- **Data Transformation** — Cleaning, enrichment, and derived column generation via PySpark
- **External Table Registration** — Automatic Glue Catalog updates on every run
- **Error Handling** — Empty dataframe guards to prevent job failures when no new data exists

### 📈 Analytics & Querying

- **Athena Integration** — All layers queryable via standard SQL
- **Partitioned Storage** — Parquet files partitioned for fast query performance
- **OBT (One Big Table)** — Gold layer joins all domains into a single analytics-ready table

### 🏗️ Infrastructure

- **Multi-layer S3 Storage** — Separate prefixes per layer (bronze / silver / gold)
- **Glue Workflow Orchestration** — Trigger-based job chaining with ALL/ANY conditions
- **Visual ETL + Notebooks** — Combination of no-code Visual ETL and PySpark notebooks

---

## 🏗️ Architecture

### System Architecture

```
S3 (Raw CSV Files)
        │
        ▼
┌───────────────────────────────────────────────────────┐
│               AWS Glue Workflow (Orchestration)        │
│  ┌─────────────────────────────────────────────────┐  │
│  │  Trigger: startt (Schedule)                     │  │
│  │  ├─ bronzeLayer Job                             │  │
│  │  ├─ silver_start Trigger (ANY)                  │  │
│  │  │   ├─ dim_silver Job                          │  │
│  │  │   └─ silver_transformation Job               │  │
│  │  ├─ gold_start Trigger (ALL)                    │  │
│  │  └─ OBT gold Job                                │  │
│  └─────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────┐
│   Bronze Layer    │  ← Raw CSV → Parquet + Catalog External Tables
│   (Visual ETL)    │    5 sources processed incrementally
└───────────────────┘
        │
        ▼
┌───────────────────┐
│   Silver Layer    │  ← Cleaned, enriched, typed Parquet
│  (Glue Notebook)  │    + Catalog External Tables per domain
└───────────────────┘
        │
        ▼
┌───────────────────┐
│    Gold Layer     │  ← OBT: all domains joined on show_id
│   (Glue Script)   │    + goldOBT External Table in Catalog
└───────────────────┘
        │
        ▼
  Amazon Athena (SQL Analytics)
```

### Data Flow

```
Raw CSV (S3)
     │
     ├─► [Workflow Triggered]
     │
     ├─► Bronze Layer
     │   ├─ Incremental read via Job Bookmark
     │   ├─ Write Parquet to S3 (bronze_data/)
     │   └─ Register External Tables in Glue Catalog
     │
     ├─► Silver Layer
     │   ├─ Read Bronze Parquet
     │   ├─ Data cleaning & transformation
     │   ├─ Write Parquet to S3 (silver_data/)
     │   └─ Register External Tables in Glue Catalog
     │
     └─► Gold Layer (OBT)
         ├─ Left join all 5 silver tables on show_id
         ├─ Write Parquet to S3 (gold_data/)
         └─ Register goldOBT External Table in Catalog
              │
              ▼
         Amazon Athena Queries
```

### Component Interaction

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│  Glue Workflow  │──────▶│   Amazon S3     │──────▶│   AWS Glue      │
│                 │ Write │                 │  Read  │                 │
│  • Triggers     │       │  • bronze_data/ │        │  • Visual ETL   │
│  • Job Chaining │       │  • silver_data/ │        │  • Notebooks    │
│  • Monitoring   │       │  • gold_data/   │        │  • Scripts      │
└─────────────────┘       └─────────────────┘       └─────────────────┘
         │                         │                          │
         └─────────────────────────┴──────────────────────────┘
                                   │
                                   ▼
                     ┌─────────────────────────┐
                     │   Glue Data Catalog      │
                     │   External Tables         │
                     │   (Athena Queryable)      │
                     └─────────────────────────┘
```

---

## 🛠️ Technology Stack

| Component             | Technology                        | Purpose                          |
| --------------------- | --------------------------------- | -------------------------------- |
| ETL Processing        | AWS Glue (Visual ETL + Notebooks) | Pipeline authoring and execution |
| Distributed Computing | Apache Spark (PySpark)            | Data transformation at scale     |
| Storage               | Amazon S3                         | Raw, Bronze, Silver, Gold data   |
| Metadata              | AWS Glue Data Catalog             | External table registration      |
| Analytics             | Amazon Athena                     | SQL querying on Parquet          |
| Orchestration         | AWS Glue Workflows & Triggers     | Job scheduling and chaining      |
| Security              | AWS IAM                           | Role-based access control        |
| Language              | Python 3.10+                      | Transformation logic             |

---

## 📁 Project Structure

```
glue-etl-project/
│
├── scripts/
│   ├── bronze_layer.py            # Auto-generated Visual ETL script
│   ├── silver_transformation.py   # Silver layer transformations
│   ├── dim_silver.py              # Dimension table processing
│   └── gold_obt.py                # Gold OBT join script
│
├── visual_etl/
│   ├── bronzeLayer.json           # Visual ETL canvas definition
│   ├── silver.json
│   └── gold.json
│
├── workflows/
│   └── glue_workflow.json         # Workflow & trigger definitions
│
├── athena/
│   └── queries.sql                # Athena SQL queries
│
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites

- AWS Account with Glue, S3, and Athena access
- IAM Role with `AWSGlueServiceRole` permissions
- S3 Bucket created (e.g. `project-glue-uk`)

### Setup

**1. Upload source data to S3**

```
s3://your-bucket/source_data/cast/
s3://your-bucket/source_data/categories/
s3://your-bucket/source_data/countries/
s3://your-bucket/source_data/directors/
s3://your-bucket/source_data/titles/
```

**2. Create Glue Database**

```sql
CREATE DATABASE glue_project;
```

**3. Import Visual ETL jobs**

```
Glue → Visual ETL → Create job → Upload JSON
```

**4. Set up the Workflow**

```
Glue → Workflows → Create → Add triggers and jobs
```

**5. Reset bookmark and run**

```
Glue → Jobs → Actions → Reset job bookmark
Glue → Workflows → glue_project_flow → Run
```

**6. Query in Athena**

```sql
SELECT * FROM glue_project.goldOBT LIMIT 100;
```

---

## ⚙️ Workflow Orchestration

```
startt  (Scheduled Trigger)
    │
    ▼
bronzeLayer
    │
    ▼  ANY
silver_start
    │
    ├──► dim_silver ────────────┐
    │                           │ ALL
    └──► silver_transformation ─┤
                                ▼
                           gold_start
                                │
                                ▼
                            OBT gold
```

| Trigger      | Type        | Condition | Fires When                                    |
| ------------ | ----------- | --------- | --------------------------------------------- |
| startt       | Schedule    | —         | Scheduled time                                |
| silver_start | Conditional | ANY       | bronzeLayer completes                         |
| gold_start   | Conditional | ALL       | dim_silver AND silver_transformation complete |

---

## 🗂️ Data Sources

| Domain     | File                   | Description              |
| ---------- | ---------------------- | ------------------------ |
| Titles     | netflix_titles.csv     | Core show/movie metadata |
| Cast       | netflix_cast.csv       | Cast members per title   |
| Directors  | netflix_directors.csv  | Director information     |
| Categories | netflix_categories.csv | Genre and category tags  |
| Countries  | netflix_countries.csv  | Country of production    |

---

## 📊 Storage Layout

```
s3://project-glue-uk/
│
├── source_data/        ← Raw CSV files (input)
├── bronze_data/        ← Parquet after initial load
├── silver_data/        ← Cleaned & enriched Parquet
└── gold_data/
    └── goldOBT/        ← Final OBT, analytics-ready
```

---

## 📌 IAM Permissions Required

```json
{
  "Actions": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:ListBucket",
    "glue:CreateTable",
    "glue:UpdateTable",
    "glue:BatchCreatePartition",
    "glue:GetDatabase",
    "logs:CreateLogGroup",
    "logs:PutLogEvents"
  ]
}
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'Add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

## 👤 Author

**Utsav Kanani**  
AWS Glue ETL Pipeline — Netflix Dataset

---
