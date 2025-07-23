# Databricks notebook source
from pyspark.sql.functions import year,month,quarter,date_format

#extract date dim from fact table to enable filter by year/month/quarter
df_fact = spark.read.format('delta').load("/Volumes/retail_catalog/silver/cleaned_trade")
date_df = df_fact.select('date_key').distinct()

df_dim_time = date_df \
    .withColumn("year", year("date_key")) \
    .withColumn("month", month("date_key")) \
    .withColumn("quarter", quarter("date_key")) \
    .withColumn("year_month", date_format("date_key", "yyyy-MM")) \
    .withColumn("month_name", date_format("date_key", "MMMM"))

df_dim_time.write.format("delta").mode("overwrite").save("/Volumes/retail_catalog/silver/dim_time")