# Databricks notebook source
# Load via Parquet
import pandas as pd

df_fact_pd = pd.read_parquet("/Volumes/retail_catalog/silver/cleaned_trade", columns=["date_key", "msic_code"])

df_dim_time_pd = pd.read_parquet("/Volumes/retail_catalog/silver/dim_time")
df_dim_economy_pd = pd.read_parquet("/Volumes/retail_catalog/silver/dim_economy")


# COMMAND ----------

#pulling list of any msic_code from  retail trade data that doesn’t exist in the MSIC lookup
missing_msic_df = df_fact_pd.loc[
    ~df_fact_pd["msic_code"].isin(df_dim_economy_pd["msic_code"])]\
    [["msic_code"]].drop_duplicates()

print("Missing msic_code values (not found in dim_economy):")
print(missing_msic_df)

# COMMAND ----------

missing_dates_df = df_fact_pd.loc[
    ~df_fact_pd["date_key"].isin(df_dim_time_pd["date_key"])]\
        [["date_key"]].drop_duplicates()

print("Missing date_key values (not found in dim_time):")
print(missing_dates_df)

# COMMAND ----------

if  not missing_dates_df.empty or not missing_msic_df.empty:
    print("Warning: missing date_key or msic_code")