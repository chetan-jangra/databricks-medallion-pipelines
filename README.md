# Databricks Medallion Architecture Pipelines

Seven end-to-end data pipelines built on **Databricks** using the **Medallion Architecture (Bronze → Silver → Gold)** with **PySpark**.

## Architecture

```
Raw Data  →  🥉 Bronze  →  🥈 Silver  →  🥇 Gold  →  Analytics
            (raw ingest)  (cleaned data)  (business-ready aggregates)
```

- **Bronze:** raw data loaded as-is into a Spark DataFrame
- **Silver:** data cleaning, such as handling null values and removing duplicates
- **Gold:** aggregated, business-ready tables for analysis

## Pipelines Included

| # | Pipeline | Domain |
|---|----------|--------|
| 1 | E-Commerce Sales Data Pipeline | Retail / Sales |
| 2 | Customer Analytics Pipeline | Customer segmentation |
| 3 | Banking Transaction Analytics Pipeline | Finance |
| 4 | Employee HR Analytics Pipeline | Human Resources |
| 5 | Retail Inventory Analytics Pipeline | Inventory |
| 6 | Healthcare Analytics Pipeline | Healthcare |
| 7 | Telecom Customer Churn Analytics Pipeline | Telecom |

## Tech Stack

- Databricks (Free Edition)
- PySpark / Apache Spark
- Python
- SQL

## Repository Contents

| File | Description |
|------|-------------|
| `medallion_pipelines_all.ipynb` | Full notebook with code and outputs |
| `medallion_pipelines_all.py` | Same code as a plain Python file |

## How to Run

1. Sign up for a free [Databricks Free Edition](https://www.databricks.com/learn/free-edition) account
2. Import `medallion_pipelines_all.ipynb` into your workspace
3. Attach it to serverless compute and click **Run all**

## Author

**Chetan Jangra**
Aspiring Data Engineer | Databricks Certified Data Engineer Associate
[LinkedIn](https://www.linkedin.com/in/chetan-jangra-7b9982268)
