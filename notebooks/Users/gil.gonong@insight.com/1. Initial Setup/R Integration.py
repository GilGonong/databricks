# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Step 1: Download RStudio Open Source 

# COMMAND ----------

script = """#!/bin/bash

set -euxo pipefail
RSTUDIO_BIN="/usr/sbin/rstudio-server"

if [[ ! -f "$RSTUDIO_BIN" && $DB_IS_DRIVER = "TRUE" ]]; then
  apt-get update
  apt-get install -y gdebi-core
  cd /tmp
  # You can find new releases at https://rstudio.com/products/rstudio/download-server/debian-ubuntu/.
  wget https://download2.rstudio.org/server/bionic/amd64/rstudio-server-2022.02.1-461-amd64.deb -O rstudio-server.deb
  sudo gdebi -n rstudio-server.deb
  rstudio-server restart || true
fi
"""

dbutils.fs.mkdirs("/databricks/rstudio")
dbutils.fs.put("/databricks/rstudio/rstudio-install.sh", script, True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Step 2:
# MAGIC 
# MAGIC Terminate the cluster and add <b>dbfs:/databricks/rstudio/rstudio-install.sh</b> as an init script.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Step 3:
# MAGIC 
# MAGIC Start the cluster
# MAGIC 
# MAGIC 
# MAGIC <b>Note:</b> Integration will not work when Auto-Terminate is enabled.