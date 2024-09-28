# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

def convert_timestamp_to_date(df):
    columns=df.columns
    for col in columns:
        if "date" in col.lower():
            df=df.withColumn(col,F.to_date(F.col(col)))
    return df

# COMMAND ----------

for path in dbutils.fs.ls("/mnt/bronze/SalesLT"):
    df=spark.read.format("parquet").option("header","true").option("inferSchema","true").load(path.path)
    df=convert_timestamp_to_date(df)
    df.write.mode("overwrite").format("delta").save(path.path.replace("bronze","silver"))
