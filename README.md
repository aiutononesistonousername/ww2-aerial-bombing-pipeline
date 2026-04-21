# WWII Aerial Bombing Data Pipeline & Analytics

## Project Overview
This project is an end-to-end Data Engineering and Analytics pipeline that processes historical World War II aerial bombing data (THOR dataset). The objective is to transform raw, noisy data into a clean, relational database to extract military strategic insights using advanced SQL.

## Tech Stack & Skills Demonstrated
- **Python (Pandas):** Data extraction, transformation, and cleaning.
- **PostgreSQL & SQLAlchemy:** Secure database loading and relational data management.
- **Advanced SQL:** CTEs, Window Functions, Partitioning, and derived KPIs.
- **AI-Augmented Development:** Leveraged LLMs as a technical co-pilot to standardize data transformation processes, accelerate ETL development, and troubleshoot complex debugging scenarios.
- **Data Quality & Lineage:** Exception handling and automated reporting.

## Architecture & Workflow

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
- **Time-Series Escalation (Window Functions):** Calculated the cumulative running total (`SUM() OVER(ORDER BY...)`) of the war effort. 
    * **Update:** While the original objective was to analyze the British Royal Air Force (RAF), the focus was shifted to the **USAAF (USA)**. This adjustment was necessary as the source dataset contained significant gaps for RAF records between 1942 and 1945. Switching to USA data allowed for a continuous and statistically significant demonstration of the military escalation logic.
- **Shifting Fronts (Partitioned Ranking):** Identified the primary target country year by year by solving the "Top 1 per Group" problem using `ROW_NUMBER() OVER(PARTITION BY...)`.

## Data Visualization & Insights (Power BI)

In this final phase, the SQL results were connected to **Power BI** to create an interactive dashboard that translates raw numbers into strategic military history.

### 1. Strategic Payload Shift (Japan 1945)
![Strategic Shift Japan](<img width="2049" height="1151" alt="graph1 png" src="https://github.com/user-attachments/assets/5c2c2cff-6a22-4308-ae91-5e79dee42728" />)

* **Objective:** Visualize the tactical pivot in the Pacific Theater during the final year of the conflict.
* **Insight:** The chart highlights the dramatic shift in **March 1945**, where the USAAF moved from high-explosive precision bombing to large-scale incendiary missions.
* **Technical Link:** This visualization confirms the accuracy of the **CTE-based payload analysis** found in `portfolio_queries.sql`.

### 2. USAAF Escalation: Cumulative War Effort
![USA Escalation](<img width="2051" height="1151" alt="graph2 png" src="https://github.com/user-attachments/assets/5f63556f-573b-40d9-900c-d2ef7e601f27" />)

* **Objective:** Track the industrial and military buildup of the United States Air Forces.
* **Insight:** The near-exponential growth starting in 1943 showcases the massive mobilization of Allied resources, culminating in the strategic peak of 1945.
* **Technical Link:** This chart visualizes the **SQL Window Function** (`SUM() OVER`) logic, showing how payloads accumulated month-by-month.

### 3. Global Target Shifting (Ranking by Year)
![Target Ranking](<img width="2048" height="1150" alt="graph3 png" src="https://github.com/user-attachments/assets/ceafd95d-4e1c-475d-9378-14f141b44001" />)

* **Objective:** Rank the top 10 target countries to observe geographical shifts in the conflict.
* **Insight:** The ribbon chart highlights the 1944 focus on Europe (Germany/Italy) and the final 1945 pivot toward the Pacific Theater (Japan).
* **Technical Link:** Validates the **Partitioned Ranking** (`ROW_NUMBER() OVER`) used in the SQL portfolio.

## Repository Structure
- `main.py`: The Python script containing the Object-Oriented ETL pipeline.
- `portfolio_queries.sql`: The advanced SQL queries used for strategic analysis.
- `historical_weight_anomalies.csv`: The automated Data Quality log containing quarantined records.
- `.gitignore`: Security configuration to prevent uploading sensitive credentials (`.env`) and raw large files.

---
*Note: The original raw dataset (operations.csv) and the local .env file are not included in this repository due to size limits and security best practices.*
