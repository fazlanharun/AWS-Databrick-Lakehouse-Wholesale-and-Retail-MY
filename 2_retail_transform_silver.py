# Databricks notebook source
from pyspark.sql.functions import col, to_date, first

df = spark.read.format('delta').load('/Volumes/retail_catalog/bronze/raw_trade')

df_pivoted = df.groupby('date','group')\
            .pivot(
                'series',['abs','growth_yoy','growth_mom']
                )\
            .agg(
                first('sales').alias('sales_value_rm'),
                first('sales').alias('volume_index')
            )

df_fact = df_pivoted\
    .withColumnRenamed('group','msic_code')\
    .withColumn('date_key',to_date('date'))\
    .withColumn('sales_value_rm',col('abs_sales_value_rm').cast('double'))\
    .withColumn('growth_yoy',col('growth_yoy_sales_value_rm').cast('double'))\
    .withColumn('growth_mom',col('growth_mom_sales_value_rm').cast('double'))\
    .withColumn('volume_index',col('abs_volume_index').cast('double'))\
    .withColumn('volume_growth_yoy',col('growth_yoy_volume_index').cast('double'))\
    .withColumn('volume_growth_mom',col('growth_mom_volume_index').cast('double'))

#save to silver
df_fact.write.format('delta').mode('overwrite').save('/Volumes/retail_catalog/silver/cleaned_trade')