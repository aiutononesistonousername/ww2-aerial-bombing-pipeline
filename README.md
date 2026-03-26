# 🛩️ WWII Aerial Bombing Data Pipeline & Analytics

## 🎯 Project Overview
This project is an end-to-end Data Engineering and Analytics pipeline that processes historical World War II aerial bombing data (THOR dataset). The objective is to transform raw, noisy data into a clean, relational database to extract military strategic insights using advanced SQL.

## 🛠️ Tech Stack & Skills Demonstrated
- **Python (Pandas):** Data extraction, transformation, and cleaning.
- **PostgreSQL & SQLAlchemy:** Secure database loading and relational data management.
- **Advanced SQL:** CTEs, Window Functions, Partitioning, and derived KPIs.
- **AI-Augmented Development:** Leveraged LLMs as a technical co-pilot to standardize data transformation processes, accelerate ETL development, and troubleshoot complex debugging scenarios.
- **Data Quality & Lineage:** Exception handling and automated reporting.

## ⚙️ Architecture & Workflow

### 1. Data Extraction & Cleaning (Python & AI Integration)
- **Noise Reduction:** Removed over 20,000 invalid records containing missing aircraft or payload data using Pandas.
- **Data Standardization:** Normalized country names and handled missing categorical data (e.g., mapping missing countries to `UNKNOWN` to preserve historical payload totals). Utilized AI assistance to quickly standardize repetitive logic and optimize data-mapping structures.
- **Data Quality Logic:** Discovered 475 historical records where the sum of individual bombs did not match the official total weight recorded. To preserve data lineage and historical accuracy in the main DB, these records were isolated and exported into a separate Exception Report (`historical_weight_anomalies.csv`) for potential Domain Expert review.

### 2. Database Load (PostgreSQL)
- Established a secure connection to a local PostgreSQL database using environment variables (`.env`) to protect credentials.
- Automated the loading of the cleaned dataset into the `ww2_missions` table.

### 3. Data Analysis (Advanced SQL)
The `portfolio_queries.sql` file contains complex queries designed to answer specific strategic questions:
- **Strategic Shift Analysis (CTEs & `NULLIF`):** Analyzed the shift in US bombing strategy over Japan in 1945, calculating the percentage of incendiary vs. high-explosive bombs month by month while preventing divide-by-zero errors.
- **Time-Series Escalation (Window Functions):** Calculated the cumulative running total (`SUM() OVER(ORDER BY...)`) of the British Royal Air Force's war effort.
- **Shifting Fronts (Partitioned Ranking):** Identified the primary target country year by year by solving the "Top 1 per Group" problem using `ROW_NUMBER() OVER(PARTITION BY...)`.

## 📁 Repository Structure
- `main.py`: The Python script containing the Object-Oriented ETL pipeline.
- `portfolio_queries.sql`: The advanced SQL queries used for strategic analysis.
- `historical_weight_anomalies.csv`: The automated Data Quality log containing quarantined records.
- `.gitignore`: Security configuration to prevent uploading sensitive credentials (`.env`) and raw large files.

---
*Note: The original raw dataset (operations.csv) and the local .env file are not included in this repository due to size limits and security best practices.*