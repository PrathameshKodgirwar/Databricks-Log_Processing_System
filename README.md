Databricks ETL Pipeline (Medallion Architecture)

🚀 Overview

This project implements an end-to-end ETL pipeline using Databricks, following the Medallion Architecture (Bronze, Silver, Gold). It ingests semi-structured JSON data from APIs, processes it using PySpark, and transforms it into business-ready datasets for analytics.

---

🧱 Architecture

API → Bronze → Silver → Gold → Analytics

🥉 Bronze (Extract + Load)

- Ingests JSON data from REST APIs
- Adds ingestion metadata (timestamp, date)
- Stores raw data in Delta tables (Unity Catalog)

🥈 Silver (Transform)

- Deduplication using window functions
- Data validation (null checks, key constraints)
- Splits data into:
  - Clean data
  - Quarantine (invalid records)
  - Duplicate records

🥇 Gold (Load + Aggregation)

- Generates business insights:
  - User activity
  - Comments per post
  - User engagement
- Optimized for reporting and visualization

---

⚙️ Pipeline Orchestration

- Built using Databricks Jobs (multi-task workflow)
- Task flow:

01_Extract_Bronze → 02_Transform_Silver → 03_Load_Gold

- Fully parameterized using widgets:
  - catalog, schema, table names
  - API URLs
  - key columns

---

📈 Audit & Monitoring

A centralized audit framework tracks:

- Execution status (SUCCESS / FAILED)
- Record counts
- Start & end time
- Error messages

Enables observability and debugging across all pipeline stages.

---

🧰 Tech Stack

- Databricks (Lakehouse Platform)
- PySpark
- Delta Lake
- Unity Catalog
- REST APIs (JSON ingestion)

---

📊 Sample Outputs

- "gold.user_activity" → posts per user
- "gold.comments_per_post" → engagement per post
- "gold.user_engagement" → comments received

---

▶️ How to Run

1. Configure widgets in notebooks (catalog, schema, table, URL)
2. Create Databricks Job with 3 tasks:
   - Bronze → Silver → Gold
3. Set task dependencies
4. Run manually or schedule

---

🔮 Future Improvements

- Config-driven pipeline (metadata-based)
- Streaming ingestion support
- Dashboard integration

---

👨‍💻 Author

Built as part of hands-on Databricks Data Engineering.
