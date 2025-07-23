# Databricks notebook source
import pandas as pd

df_raw = pd.read_parquet("https://storage.dosm.gov.my/iowrt/iowrt_3d.parquet")

df_bronze = spark.createDataFrame(df_raw)

df_bronze.write.format('delta').mode('overwrite').save('/Volumes/retail_catalog/bronze/raw_trade')