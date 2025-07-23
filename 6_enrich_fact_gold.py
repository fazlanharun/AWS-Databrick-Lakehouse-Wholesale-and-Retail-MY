# Databricks notebook source
df_fact = spark.read.format("delta").load("/Volumes/retail_catalog/silver/cleaned_trade")
df_dim_economy = spark.read.format("delta").load("/Volumes/retail_catalog/silver/dim_economy")
df_dim_time = spark.read.format("delta").load("/Volumes/retail_catalog/silver/dim_time")

# COMMAND ----------

df_fact.createOrReplaceTempView("fact_retail_trade")
df_dim_economy.createOrReplaceTempView("dim_economy")
df_dim_time.createOrReplaceTempView("dim_time")

# COMMAND ----------

df_enriched = spark.sql("""
SELECT 
  f.date_key,
  t.year,
  t.month,
  t.quarter,
  t.year_month,
  f.msic_code,
  e.sector_name_en,
  e.sector_group_code,
  f.sales_value_rm,
  f.growth_yoy,
  f.growth_mom,
  f.volume_index,
  f.volume_growth_yoy,
  f.volume_growth_mom
FROM fact_retail_trade f
LEFT JOIN dim_economy e 
  ON f.msic_code = e.msic_code
LEFT JOIN dim_time t 
  ON f.date_key = t.date_key
""")

df_enriched.write.format("delta").mode("overwrite").save("/Volumes/retail_catalog/gold/enriched_trade")