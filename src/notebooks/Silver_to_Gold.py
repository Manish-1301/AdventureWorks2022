# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

def pascal_to_snake(name: str) -> str:
    return "".join(["_" + char if char.isupper() and not name[i - 1].isupper() else char for i, char in enumerate(name)]).lstrip("_")

# COMMAND ----------

for path in dbutils.fs.ls("/mnt/silver/SalesLT"):
    df=spark.read.format("delta").option("header","true").option("inferSchema","true").load(path.path)
    for col in df.columns:
        df = df.withColumnRenamed(col, pascal_to_snake(col))
    df.write.mode("overwrite").format("delta").save(path.path.replace("silver","gold"))

# COMMAND ----------

dbutils.fs.ls("/mnt/gold/SalesLT")
