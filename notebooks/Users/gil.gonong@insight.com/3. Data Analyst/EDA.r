# Databricks notebook source
# MAGIC %python
# MAGIC # access storage account using access key
# MAGIC spark.conf.set(
# MAGIC   "fs.azure.account.key.dbricksdatasource.dfs.core.windows.net",
# MAGIC   dbutils.secrets.get(scope="dbricks-scope",key="storage-account-key"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM processing.bronze_delta_ChildPoverty_National

# COMMAND ----------

library(SparkR)
library(magrittr)

# COMMAND ----------

rdf <- sql("SELECT MsCode, Year, EstCode, SUM(Estimate) AS Estimate 
  FROM processing.bronze_delta_ChildPoverty_National 
  WHERE 
    Year BETWEEN 2019 AND 2022
  AND EstCode = 'A_Proportion'
  GROUP BY MsCode, Year, EstCode 
  ORDER BY Year")

createOrReplaceTempView(rdf, "cp_national")

display(rdf)

# COMMAND ----------

deltaSilverDataPath <- "abfss://data@dbricksdatasource.dfs.core.windows.net/silver/child-poverty/national/" 

deltaSilverDataPath

# COMMAND ----------

# Remove existing table
sql("DROP TABLE IF EXISTS loan_by_state_delta")

# Create table using Delta Lake 
sql(
    "CREATE TABLE loan_by_state_delta
     USING delta
     LOCATION '/tmp/loan_by_state_delta'
     AS SELECT * FROM loan_by_state"
)

# Show results
display(tableToDF("loan_by_state_delta"))