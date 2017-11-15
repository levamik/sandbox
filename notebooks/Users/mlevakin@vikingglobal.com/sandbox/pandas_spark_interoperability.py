# Databricks notebook source
"""read parquet file as spark dataframe"""
inputFile = "/mnt/altdata/mike-test"
df = spark.read.parquet(inputFile)

# COMMAND ----------

import pandas as pd

"""take first N records of spark dataframe and convert to pandas dataframe"""
pdf = df.limit(5000).toPandas()

"""create spark dataframe from pandas dataframe"""
sdf = spark.createDataFrame(pdf)
display(sdf.limit(100))

# COMMAND ----------

rc = sdf.agg({"year": "max"}).collect()
rc[0][0]

# COMMAND ----------

import pandas as pd
pd.__version__