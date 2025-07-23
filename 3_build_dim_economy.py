# Databricks notebook source
import pandas as pd

df_pd = pd.read_parquet('https://storage.dosm.gov.my/dictionaries/msic.parquet')
df_pd = df_pd[df_pd['digits']==3][['group','desc_en','division']]
df_pd = df_pd.rename(columns={
    'group':'msic_code',
    'desc_en':'sector_name_en',
    'division':'sector_group_code'
})
df_pd['classification_level'] = 'group'

df_dim_economy = spark.createDataFrame(df_pd)
df_dim_economy.write.format("delta").mode("overwrite").save("/Volumes/retail_catalog/silver/dim_economy")