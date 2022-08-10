# Databricks notebook source
#list existing mounts
dbutils.fs.mounts()

# COMMAND ----------

#Create Secret Scope



# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Mount ADLS data container

# COMMAND ----------

#Retrieve secrets value and set the variables

azdbclientid = dbutils.secrets.get(scope="dbricks-scope",key="dbricks-blobstorage-appreg-clientid")
azdbclientsecret = dbutils.secrets.get(scope="dbricks-scope",key="dbricks-appreg-client-secret")
tenantid = dbutils.secrets.get(scope="dbricks-scope",key="dbricks-blobstorage-appreg-tenantid")

straccount = "dbricksdatasource"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": azdbclientid,
           "fs.azure.account.oauth2.client.secret": azdbclientsecret,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+tenantid+"/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://data@" + straccount + ".dfs.core.windows.net/",
  mount_point = "/mnt/" + straccount,
  extra_configs = configs)

# COMMAND ----------

#List all mount

dbutils.fs.mounts()