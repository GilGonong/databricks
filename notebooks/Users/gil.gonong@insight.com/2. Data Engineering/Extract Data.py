# Databricks notebook source
#INITIALISATION
#Initialise File System for the Notebook - Initialising the file system much like you would for a network drive
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true") #Initialises
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false") #Prvents it from reinitialising constantly

# COMMAND ----------

# access storage account using access key
spark.conf.set(
  "fs.azure.account.key.dbricksdatasource.dfs.core.windows.net",
  dbutils.secrets.get(scope="dbricks-scope",key="storage-account-key"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DEFINE STORAGE VARIABLES
storageAccountSource = "abfss://data@dbricksdatasource.dfs.core.windows.net"                   # Storage account Source
inputPath = storageAccountSource + "/datasources/child-poverty/cp-national-datafile.csv"       # read single file from the source
#inputPath = storageAccountSource + "/datasources/child-poverty/"                              # read multiple file from the source that has the same structure
deltaBronzeDataPath    = storageAccountSource + "/bronze/child-poverty/national/"              # Delta Bronze output path

# COMMAND ----------

#Start Timestamp for file processing
inputDF = spark.sql("SELECT current_timestamp() as CurrentTimestamp")
pStartTime = inputDF.select("CurrentTimestamp").collect()[0]['CurrentTimestamp']
#additionalColumns
pStartTime
#pEndTime = current_timestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Load Child Poverty National File

# COMMAND ----------

#read in Source Files
rawDataDF = (
 
    spark.read 
  
    .option("header", "True")
    .option("inferSchema", True) #infer the schema from the file
    .csv(inputPath)
  
    #ADDITIONAL COLUMNS
    .withColumn("FileName", input_file_name())  #File Name as Column
    .withColumn("InsertedDateTime", lit(pStartTime))

    .repartition(200)
)
display(rawDataDF.limit(100))

# COMMAND ----------

(
rawDataDF
  #WRITE OPTIONS
    .write
    .option("overwriteSchema", "true")
    .mode("Overwrite")
    .format("delta")
    
    .partitionBy("MsCode")  #Partition as we write - note this needs to be casted to date/int or a sensible datatype (perhaps not string)
    
    .save(deltaBronzeDataPath)
)

# COMMAND ----------

# create delta bronze table
spark.sql("""
  CREATE TABLE IF NOT EXISTS processing.bronze_delta_ChildPoverty_National
  USING DELTA 
  LOCATION '{}' 
""".format(deltaBronzeDataPath))

# COMMAND ----------

# Optimize the table to compress the files
spark.sql("""OPTIMIZE processing.bronze_delta_ChildPoverty_National""".format(deltaBronzeDataPath))