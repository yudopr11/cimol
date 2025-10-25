## Data Pipeline Architecture: ELT Approach

My proposed architecture adopts a modern **ELT (Extract, Load, Transform)** approach, centered entirely on the **Google Cloud Platform (GCP)**. We're leveraging GCP's serverless nature—meaning minimal infrastructure management—to achieve the required scalability, reliability, and cost efficiency for a growing, medium-to-small business.

[![](https://mermaid.ink/img/pako:eNqNVNtu2kAQ_ZXRPkRGAgIYAvihKtc2EigkJqpUiKoNnthW7F20u25CQ_69szYEcmlUS5b3cuacmfHZfWIrGSDzWKj4OoL5cCmAHp3dFgtLVq_CkBsOvszUCvWSFQj79BazCx_8jTaYQm92fnPY6i9GV7OPtwaL75kIFAYa5B18kzJMEPwI0egdCkWwFO8SaVThXISoTSwFOKNHo_jKVCaSB6XjpAb-YpDILAB_FWGQJaiOtce7zXEmVjlRBWYbE0lxnPvlL2Jf9OPwMkO1AZqgEjyBOb9N8GafWiEGlcqX7VzFYYhKQ8DjZLMlmWPJHDJGQ_lYBPXyd8xtU7bQ-09c_x3O1q1B8YccubVJX_EHh16Y8A0q-Ar7AkpHwRRLUBgjN5lCy7Qr9235-y0i_OynuDt3_OAKI5lpBMc3UvEQ4QTmigt9J1X66gcVpDn_sD8_rNPECW5Naa_sm5C4eBiL8LOSCuSHdHumKVfGydO0o3-RfVxgswoDKXSWrgvfXeFaKmNzOoEemWJj4pV-W18uY8UnvjOR8p7kfJMFsTzKe5JbB66da03O8eBircvg8wTpQ_H3aEU-Ta1VhakUMbV7lw553Qa9Og3WLTCRoT4dKSXJolZ0MHWKY3CIL73tHMzpqMGYDE1G2YcdEU9zG-aauU9HKUFPBxE3W1uLQ-9p3vM58vRQByvTVRMHzLvjicYyS1FRIM3Zk8UsmYkwxSXzaBhQG2wxzxS05uKnlCnzjMooTMksjF5IsjUdAhzGnFqTvqwqkkM1kJkwzGt33ZyEeU_skXkNt1vtdtpNt1NvNWpus90usw2h6tVuvVHr1M-arW6r1uo8l9mfXLZW7bTbnY5bd88a7UbTrTXLDAPbvGlxgeb36PNf0h-SsA?type=png)](https://mermaid.live/edit#pako:eNqNVNtu2kAQ_ZXRPkRGAgIYAvihKtc2EigkJqpUiKoNnthW7F20u25CQ_69szYEcmlUS5b3cuacmfHZfWIrGSDzWKj4OoL5cCmAHp3dFgtLVq_CkBsOvszUCvWSFQj79BazCx_8jTaYQm92fnPY6i9GV7OPtwaL75kIFAYa5B18kzJMEPwI0egdCkWwFO8SaVThXISoTSwFOKNHo_jKVCaSB6XjpAb-YpDILAB_FWGQJaiOtce7zXEmVjlRBWYbE0lxnPvlL2Jf9OPwMkO1AZqgEjyBOb9N8GafWiEGlcqX7VzFYYhKQ8DjZLMlmWPJHDJGQ_lYBPXyd8xtU7bQ-09c_x3O1q1B8YccubVJX_EHh16Y8A0q-Ar7AkpHwRRLUBgjN5lCy7Qr9235-y0i_OynuDt3_OAKI5lpBMc3UvEQ4QTmigt9J1X66gcVpDn_sD8_rNPECW5Naa_sm5C4eBiL8LOSCuSHdHumKVfGydO0o3-RfVxgswoDKXSWrgvfXeFaKmNzOoEemWJj4pV-W18uY8UnvjOR8p7kfJMFsTzKe5JbB66da03O8eBircvg8wTpQ_H3aEU-Ta1VhakUMbV7lw553Qa9Og3WLTCRoT4dKSXJolZ0MHWKY3CIL73tHMzpqMGYDE1G2YcdEU9zG-aauU9HKUFPBxE3W1uLQ-9p3vM58vRQByvTVRMHzLvjicYyS1FRIM3Zk8UsmYkwxSXzaBhQG2wxzxS05uKnlCnzjMooTMksjF5IsjUdAhzGnFqTvqwqkkM1kJkwzGt33ZyEeU_skXkNt1vtdtpNt1NvNWpus90usw2h6tVuvVHr1M-arW6r1uo8l9mfXLZW7bTbnY5bd88a7UbTrTXLDAPbvGlxgeb36PNf0h-SsA)

The flow is straightforward: data comes from our sources, is ingested into BigQuery's raw layer, is transformed and quality-checked by dbt, and then is finally served to the business via Looker Studio.



## Technology Stack and Rationale

I've selected a concise stack designed for maximum *maintainability* and minimal *operational cost*, ensuring we're not overspending on complex services.

| Service | Role | My Rationale |
| :--- | :--- | :--- |
| **Google BigQuery** | Data Warehouse (DWH) | This is the foundation. It's **serverless** (scales automatically), uses **pay-as-you-go** pricing (perfect for our budget), and critically, it offers native **External Table** support for Google Sheets, simplifying a major integration hurdle. |
| **Cloud Functions & Cloud Scheduler** | Data Ingestion (EL) | To fetch data from the POS and ERP APIs, I'll use a Python-based Cloud Function triggered by a daily Cloud Scheduler. It's an extremely **low-cost**, **low-maintenance** serverless method, far cheaper than running dedicated server VMs. |
| **dbt (data build tool) Core** | Transformation (T), Quality, Lineage | To meet the requirement of one service handling ETL, I'm choosing **dbt Core** (the free, open-source version). It runs atop BigQuery, handling all SQL-based business logic, implementing built-in **data quality tests**, and automatically documenting **data lineage**. |
| **Looker Studio** | Reporting & Analytics | It's **free**, integrates seamlessly with BigQuery, and provides a user-friendly interface for our operational, sales, and marketing teams to build their dashboards. |
| **Cloud Monitoring & Email/Chat** | Monitoring & Alerting | I'll use Cloud Monitoring to watch the health and logs of the Cloud Functions and dbt jobs. This allows me to set up simple alerts (email or chat service) the moment an ingestion fails or a data quality test is violated, keeping our team informed immediately. |



## Strategy for Data Source Integration

### 1. POS System & ERP System (via API)

My plan uses a robust, daily batch process for this data, allowing us to hit the required **1-day latency**.

1.  **Extract & Load:** A **Cloud Function** will be triggered daily by Cloud Scheduler (e.g., at 3 AM). This function will call the POS and ERP APIs to fetch the previous day's (D-1) data.
2.  **Idempotency:** The data will be loaded directly into the BigQuery `Raw Layer`. I will design the loading process to be **idempotent**, meaning re-running the load for the same date simply overwrites the old data, making recovery simple and safe.
3.  **Batch Interval:** While the default is daily, if API limitations require it, I can easily configure the Cloud Scheduler to run the function in shorter batch intervals (e.g., three times per day) to reduce the data volume per call.

### 2. Hundreds of Google Sheets

This is the most complex source, and I've chosen a streamlined approach to avoid the maintenance nightmare of managing hundreds of Sheet IDs.

1.  **External Tables:** I will use BigQuery's native ability to connect to Google Sheets. I'll perform a **one-time setup** to register each Google Sheet as an **External Table** in BigQuery. This eliminates the need for any custom code to pull the data.
2.  **Standardization:** To ensure the pipeline works, I will work with the respective teams to **standardize the column formats** across all similar sheets (e.g., all "Sales Target" sheets must have the same column names).
3.  **Materialization:** We will not query the Sheets directly for reporting. Instead, a daily **dbt model** will copy the data from all these slow External Tables into a single, fast, **native BigQuery table** (our staging layer). This provides a single source of truth for all G-Sheet data and vastly improves dashboard performance.



## Managing Data Quality (Data Quality Checks)

I will manage data quality centrally using **dbt tests**.

* These tests are written in simple configuration files (`.yml`) and run automatically every time we transform the data (`dbt run`).
* **Preventing Bad Data:** I'll implement tests like checking for **`not_null`** on key IDs, ensuring **`unique`** transaction numbers, validating **`accepted_values`** for status fields (e.g., payment status), and checking **`relationships`** (like ensuring a product sold by POS exists in the ERP catalog).
* **Stopping the Pipeline:** If a test fails, the dbt run **stops immediately**. This ensures *bad data never reaches the Data Mart*, protecting our reporting from corruption.



## Monitoring, Alerting, and Recovery

My goal is to minimize manual intervention while ensuring fast recovery.

| Scenario | Monitoring | Alerting (Email/Chat) | Recovery Plan |
| :--- | :--- | :--- | :--- |
| **Ingestion Fails** (e.g., POS API is down) | **Cloud Monitoring** watches Cloud Function logs for HTTP errors. | Alert is sent when the **Cloud Function Error Rate** exceeds 0 in a 10-minute window. | I check the logs to diagnose the API issue. Once the source API is fixed, I **manually re-run the Cloud Function** for the failed date. (Idempotency ensures a clean load). |
| **Data Quality Fails** (e.g., Missing Product IDs) | **dbt** monitors test results during the `dbt run` command. | Alert is sent if the **`dbt test`** command returns any failures. | I coordinate with the business team to fix the data in the source system (e.g., ERP). Once the source data is corrected, I **re-run `dbt build`** to reprocess the data. |



## Approach for Cost Optimization

The entire design is built around cost efficiency.

1.  **Serverless Pay-As-You-Go:** By using **Cloud Functions** and **BigQuery**, we only pay when the code runs or when queries are executed. We avoid the high cost of *always-on* dedicated servers.
2.  **Smart BigQuery Usage:**
    * I will use **Partitioning** (by date) and **Clustering** on our largest tables (like `pos_transactions`). This is crucial because it makes our queries only scan the necessary data, **drastically reducing BigQuery's query costs.**
    * We use the **free dbt Core** instead of the paid dbt Cloud service.
3.  **Data Lifecycle Policy:** I will implement BigQuery **Data Lifecycle Policies** to automatically manage storage costs. For example, raw data older than 90 days can be automatically moved to cheaper archival storage or deleted, optimizing our monthly bill.