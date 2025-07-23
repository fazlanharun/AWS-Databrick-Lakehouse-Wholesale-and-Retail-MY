# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS retail_catalog.bronze.raw_trade COMMENT 'Raw retail trade ingested data';
# MAGIC CREATE VOLUME IF NOT EXISTS retail_catalog.silver.cleaned_trade COMMENT 'Cleaned and joined data';
# MAGIC CREATE VOLUME IF NOT EXISTS retail_catalog.silver.dim_economy ;
# MAGIC CREATE VOLUME IF NOT EXISTS retail_catalog.silver.dim_time ;
# MAGIC CREATE VOLUME IF NOT EXISTS retail_catalog.gold.enriched_trade COMMENT 'Final enriched dataset';

# COMMAND ----------

from pyspark.sql import Row

data = [
    Row(year=2023, msic_code="47.11", sales_value_rm=120000),
    Row(year=2024, msic_code="47.11", sales_value_rm=145000)
]

df_spark_silver = spark.createDataFrame(data)
df_spark_silver.write.format("delta").mode("overwrite").save("/Volumes/retail_catalog/silver/cleaned_trade")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_catalog.gold.enriched_trade
# MAGIC AS SELECT * FROM delta.`/Volumes/retail_catalog/gold/enriched_trade`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM retail_catalog.gold.enriched_trade
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with top_5_sector_sales_2024 AS (
# MAGIC   SELECT sector_name_en,
# MAGIC           SUM(sales_value_rm) as total_sales
# MAGIC           FROM retail_catalog.gold.enriched_trade
# MAGIC           WHERE year = 2024
# MAGIC           GROUP BY sector_name_en
# MAGIC           ORDER BY total_sales DESC
# MAGIC           LIMIT 5
# MAGIC )
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM top_5_sector_sales_2024