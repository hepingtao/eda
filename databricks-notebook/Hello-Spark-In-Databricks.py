# Databricks notebook source exported at Thu, 22 Dec 2016 10:34:56 UTC
spark

# COMMAND ----------

sc

# COMMAND ----------

sqlContext

# COMMAND ----------

ssc

# COMMAND ----------

firstDataFrame = sqlContext.range(100000)

# COMMAND ----------

firstDataFrame

# COMMAND ----------

type(firstDataFrame)

# COMMAND ----------

secondDataFrame = spark.range(100000)

# COMMAND ----------

secondDataFrame

# COMMAND ----------

type(secondDataFrame)

# COMMAND ----------

firstDataFrame.selectExpr("(id * 2) as value")

# COMMAND ----------

firstDataFrame.take(5)

# COMMAND ----------

secondDataFrame.take(5)

# COMMAND ----------

firstDataFrame2 = firstDataFrame.selectExpr("(id * 2) as value")

# COMMAND ----------

type(firstDataFrame2)

# COMMAND ----------

firstDataFrame2.take(5)

# COMMAND ----------


