-- Databricks notebook source
-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.fs.mounts()

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS processing COMMENT "Contains the bronze, and silver layers for the lakehouse." LOCATION "/mnt/dbricksdatasource/databases/processing"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS gold COMMENT "Contains the gold tier data which is ready for internal and external consumption." LOCATION "/mnt/dbricksdatasource/databases/gold"

-- COMMAND ----------

DROP DATABASE IF EXISTS processing

-- COMMAND ----------

DROP DATABASE IF EXISTS gold