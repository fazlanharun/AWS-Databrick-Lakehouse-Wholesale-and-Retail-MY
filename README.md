# AWS-Databrick-Lakehouse-Wholesale-and-Retail-MY

## Overview
This project demonstrates how to build a cloud-native Lakehouse architecture for analytics using:  
•	Databricks on AWS (with Unity Catalog)  
•	Amazon S3 (for data lake storage)  
•	Delta Lake (for ACID-compliant data layers)  
•	Power BI (for dashboarding)  
•	SQL transformations   

Data flows through Bronze → Silver → Gold layers in a medallion structure using databricks workflow and ends with actionable business insights.
 
<img width="1911" height="947" alt="Databricks Workflow" src="https://github.com/user-attachments/assets/ae41a659-34e1-4666-99a5-69103b79b8cf" />

Each layer is stored as Delta tables in S3-backed Unity Catalog volumes, creating a scalable, queryable lakehouse.

## ☁️ AWS Setup

🔐 IAM Role
Databricks accesses S3 securely using a cross-account IAM role created via:  
-CloudFormation Quickstart (automated)  
-This avoids hardcoded credentials and enables secure write paths using Unity Catalog. External data section that mount path from Unity catalog in databricks to S3 in AWS.  

<img width="1920" height="1032" alt="External data path" src="https://github.com/user-attachments/assets/3c12b909-92ca-4a8c-a844-00639fb74ca7" />  

📁 S3 Bucket  
Example bucket: s3://wholesale-retail-lakehouse Structured as:  
s3://wholesale-retail-lakehouse/  
├── bronze/raw_trade/  
├── silver/cleaned_trade/  
├── silver/dim_economy/  
├── silver/dim_time/  
└── gold/enriched_trade/  

<img width="1904" height="860" alt="Screenshot 2025-07-22 221821" src="https://github.com/user-attachments/assets/0120aab3-e64f-4ece-b814-85e489ab0ad6" />

## Databricks Configuration
🔹 Unity Catalog Setup

CREATE CATALOG retail_catalog;

CREATE SCHEMA retail_catalog.bronze;  
CREATE SCHEMA retail_catalog.silver;  
CREATE SCHEMA retail_catalog.gold;  

CREATE VOLUME retail_catalog.bronze.raw_trade;  
CREATE VOLUME retail_catalog.silver.cleaned_trade;  
CREATE VOLUME retail_catalog.gold.enriched_trade;  

<img width="1897" height="838" alt="Schema structure" src="https://github.com/user-attachments/assets/ededef19-ac8b-414b-84a7-9c6bfc380833" />

🔹 Writing to S3 via PySpark  
df.write.format("delta").mode("overwrite").save("/Volumes/retail_catalog/gold/enriched_trade")

🔹 Register Table (SQL)  
CREATE TABLE retail_catalog.gold.enriched_trade  
AS SELECT * FROM delta.`/Volumes/retail_catalog/gold/enriched_trade`;

Data Analysis  
💡 Insight (SQL)  
Top sectors by sales  
WITH sector_sales AS (  
  SELECT sector_name_en, SUM(sales_value_rm) AS total_sales  
  FROM retail_catalog.gold.enriched_trade  
  GROUP BY sector_name_en  
)  
SELECT * FROM sector_sales ORDER BY total_sales DESC LIMIT 5;  

Rolling average  
SELECT year_month, sector_name_en, sales_value_rm,  
  AVG(sales_value_rm) OVER (PARTITION BY sector_name_en ORDER BY year_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg   
FROM retail_catalog.gold.enriched_trade;  
 
##  📈 BI Integration (Power BI)
•	Use Databricks SQL connector in Power BI Desktop  
•	Authenticate with SQL Warehouse + Personal Access Token

<img width="3840" height="1080" alt="Screenshot 2025-07-22 203431" src="https://github.com/user-attachments/assets/71ae39e5-1947-46cb-83a7-bbb7f7567bee" />

Data Model in PowerBI

<img width="1908" height="1015" alt="Screenshot 2025-07-22 231336" src="https://github.com/user-attachments/assets/c317ceec-bf21-4415-ab4a-fa19723abb44" />

Visualization

<img width="1413" height="788" alt="Screenshot 2025-07-22 235612" src="https://github.com/user-attachments/assets/acd0c627-a084-47be-af93-2a69f5ec0061" />

Top total sale in motor vehicle sector: Sale of motor vehicle 0.63 million  
Top total sale in motor vehicle sector: Retail Sale in non-specialized store 1.55 million    
Top total sale in motor vehicle sector: Other specialized wholesale 1.85 million    

✅ Key Highlights  
•	Fully serverless: Data stored in S3 and queried via Delta Lake  
•	Secure: IAM roles via CloudFormation   
•	BI-ready: Connects seamlessly to Power BI for dashboards  
•	Production-friendly: Modular notebook design + Databricks Workflows  
