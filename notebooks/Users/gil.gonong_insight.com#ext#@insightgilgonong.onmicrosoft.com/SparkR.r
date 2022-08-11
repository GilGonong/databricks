# Databricks notebook source
# MAGIC %md # Description
# MAGIC 
# MAGIC ### SparkR
# MAGIC 
# MAGIC This notebook provides a Delta Lake example against the Lending Club data. It illustrates all functionality available in Delta Lake such as:
# MAGIC 
# MAGIC  * Import data from Parquet to Delta Lake
# MAGIC  * Batch and streaming updates
# MAGIC  * Delete, update, and merge DML operations
# MAGIC  * Time Travel
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The Data
# MAGIC 
# MAGIC The data used is public data from Lending Club. It includes all funded loans from 2012 to 2017. Each loan includes applicant information provided by the applicant as well as the current loan status (Current, Late, Fully Paid, etc.) and latest payment information. For a full view of the data please view the data dictionary available [here](https://resources.lendingclub.com/LCDataDictionary.xlsx).
# MAGIC 
# MAGIC 
# MAGIC ![Loan_Data](https://preview.ibb.co/d3tQ4R/Screen_Shot_2018_02_02_at_11_21_51_PM.png)
# MAGIC 
# MAGIC https://www.kaggle.com/wendykan/lending-club-loan-data

# COMMAND ----------

# MAGIC %md ## Ingest Data to Data Lake
# MAGIC 
# MAGIC Optimization Layer a top blob storage for Reliability (i.e. ACID compliance) and Low Latency of Streaming + Batch data pipelines.

# COMMAND ----------

# MAGIC %md ## DE: Import Data and create pre-Delta Lake Table
# MAGIC 
# MAGIC * This will create a lot of small Parquet files emulating the typical small file problem that occurs with streaming or highly transactional data

# COMMAND ----------

# MAGIC %md
# MAGIC Import libraries

# COMMAND ----------

library(SparkR)
library(magrittr)

# COMMAND ----------

# MAGIC %md
# MAGIC Import data

# COMMAND ----------

# Configure location of loanstats_2012_2017.parquet
lspq_path <- "/databricks-datasets/samples/lending_club/parquet/"

# Read loanstats_2012_2017.parquet
data <- read.parquet(lspq_path)

# Register a table with SparkSQL
createOrReplaceTempView(data, "loan_data")

# Display data
display(data)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM loan_data LIMIT 10

# COMMAND ----------

# library(SparkR)
# library(magrittr)

# Configure location of loanstats_2012_2017.parquet
# lspq_path <- "/databricks-datasets/samples/lending_club/parquet/"

# Read loanstats_2012_2017.parquet
# data <- read.parquet(lspq_path)

# Reduce the amount of data (to run on DBCE)
# loan_stats <- randomSplit(
#   data,
#   c(0.02, 0.98),
#   seed = 206
# )[[1]]

# Select only the columns needed, aggregate count by state
# loan_by_state <- loan_stats %>% 
#     select("addr_state", "loan_status") %>%
#     groupBy("addr_state") %>%
#     count()

# Register a table with SparkSQL
# createOrReplaceTempView(loan_by_state, "loan_by_state")

# Display loans by state
# display(loan_by_state)

# COMMAND ----------

# MAGIC %md ## DE: Parquet to Delta Lake format
# MAGIC With Delta Lake, you can easily transform your Parquet data into Delta Lake format. 

# COMMAND ----------

# Configure Delta Lake Bronze Path
deltalake_bronze_path <- "/tmp/loan_data_bronze"

# Remove folder if it exists
dbutils.fs.rm(deltalake_bronze_path, recurse = 'True')

# COMMAND ----------

# Remove existing table
sql("DROP TABLE IF EXISTS loan_data_bronze_delta")

# Create table using Delta Lake 
sql(
    "CREATE TABLE loan_data_bronze_delta
     USING delta
     LOCATION '/tmp/loan_data'
     AS SELECT * FROM loan_data"
)

# Show results
display(tableToDF("loan_data_bronze_delta"))

# COMMAND ----------

display(sql("DESCRIBE DETAIL loan_data_bronze_delta"))

# COMMAND ----------

# MAGIC %md ### Silver Layer: Data Analyst

# COMMAND ----------

loan_state_status <- sql("SELECT addr_state, loan_status FROM loan_data_bronze_delta")

display(loan_state_status)

# COMMAND ----------

# Reduce the amount of data
 loan_stats <- randomSplit(
   loan_state_status,
   c(0.02, 0.98),
   seed = 206
 )[[1]]

# COMMAND ----------

# Select only the columns needed, aggregate count by state
loan_by_state <- loan_stats %>% 
     select("addr_state", "loan_status") %>%
     groupBy("addr_state") %>%
     count()

# Register a table with SparkSQL
createOrReplaceTempView(loan_by_state, "loan_by_state")

#
display(loan_by_state)

# COMMAND ----------

# Convert table to DF, count the number of loans in each state
tableToDF("loan_by_state") %>% 
groupBy("addr_state") %>% 
  agg(count = "sum") %>% 
  display()

# COMMAND ----------

# Remove existing table
sql("DROP TABLE IF EXISTS loan_data_by_state_silver_delta")

# Create table using Delta Lake 
sql(
    "CREATE TABLE loan_data_by_state_silver_delta
     USING delta
     LOCATION '/tmp/loan_by_state_silver'
     AS SELECT * FROM loan_by_state"
)

# Show results
display(tableToDF("loan_data_by_state_silver_delta"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Use the '%' command to switch from the default PL to another PL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT loan_amnt, term, emp_title FROM loan_data_bronze_delta LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT home_ownership, loan_status, COUNT(*) 
# MAGIC FROM loan_data_bronze_delta 
# MAGIC GROUP BY home_ownership, loan_status
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Widgets

# COMMAND ----------

dbutils.widgets.text("State", "")

pState = dbutils.widgets.get("State")

pState

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET DROPDOWN MultiState DEFAULT 'CA' CHOICES SELECT DISTINCT addr_state FROM loan_data_by_state_silver_delta ORDER BY addr_state

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT addr_state, COUNT(loan_status) 
# MAGIC FROM loan_data_bronze_delta 
# MAGIC WHERE addr_state = getArgument('MultiState') 
# MAGIC GROUP BY addr_state
# MAGIC LIMIT 100

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md ## Unified Batch and Streaming Source and Sink
# MAGIC 
# MAGIC These cells showcase streaming and batch concurrent queries (inserts and reads)
# MAGIC * This notebook will run an `INSERT` every 10s against our `loan_stats_delta` table
# MAGIC * We will run two streaming queries concurrently against this data
# MAGIC * Note, you can also use `writeStream` but this version is easier to run in DBCE

# COMMAND ----------

# Configure Delta Lake Bronze Path
deltalake_silver_path <- "/tmp/loan_by_state_silver"

# COMMAND ----------

# Read the insertion of data
loan_by_state_readStream <- read.stream(source = "delta", path = deltalake_silver_path)
createOrReplaceTempView(loan_by_state_readStream, "loan_by_state_readStream")

# Kick off the stream
display(sql("SELECT addr_state, sum(`count`) AS loans FROM loan_by_state_readStream GROUP BY addr_state"))

# COMMAND ----------

# MAGIC %md **Wait** until the stream is up and running before executing the code below

# COMMAND ----------

i <- 1
while (i <= 6){
  # Execute Insert statement
  insert_sql <- "INSERT INTO loan_data_by_state_silver_delta VALUES ('IA', 450)"
  sql(insert_sql)
  message(paste0("loan_data_by_state_silver_delta: inserted new row of data, loop: [", i, "]"))
  
  # Wait to insert the next batch
  i <- i + 1
  Sys.sleep(time = 3)
  }

# COMMAND ----------

# MAGIC %md 
# MAGIC **Note**: Once the previous cell is finished and the state of Iowa is fully populated in the map (in cell 14), click *Cancel* in Cell 14 to stop the `readStream`.

# COMMAND ----------

# MAGIC %md
# MAGIC Let's review our current set of loans using our map visualization.

# COMMAND ----------

# Convert table to DF, count the number of loans in each state
tableToDF("loan_data_by_state_silver_delta") %>% 
  groupBy("addr_state") %>% 
  agg(count = "sum") %>% 
  display()

# COMMAND ----------

# MAGIC %md Observe that the Iowa (middle state) has the largest number of loans due to the recent stream of data.  Note that the original `loan_by_state_delta` table is updated as we're reading `loan_by_state_readStream`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full DML Support
# MAGIC 
# MAGIC **Note**: Full DML Support is a feature that will be coming soon to Delta Lake; the preview is currently available in Databricks.
# MAGIC 
# MAGIC Delta Lake supports standard DML including UPDATE, DELETE and MERGE INTO providing developers more controls to manage their big datasets.

# COMMAND ----------

# MAGIC %md Let's start by creating a traditional Parquet table

# COMMAND ----------

# Load new DataFrame based on current Delta table
lbs_df <- tableToDF("loan_data_by_state_silver_delta")

# Save DataFrame to Parquet
write.parquet(lbs_df, path = "/tmp/loan_data_by_state_silver.parquet", mode = 'overwrite')

# Reload Parquet Data & create new table
lbs_pq <- read.parquet("/tmp/loan_data_by_state_silver.parquet")

# Create temp view
createOrReplaceTempView(lbs_pq, "loan_data_by_state_silver_pq")

# Review data
display(lbs_pq)

# COMMAND ----------

# MAGIC %md ### DELETE Support
# MAGIC 
# MAGIC The data was originally supposed to be assigned to `WA` state, so let's `DELETE` those values assigned to `IA`

# COMMAND ----------

sql("DELETE FROM loan_data_by_state_silver_pq WHERE addr_state = 'IA'")

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `DELETE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# Running `DELETE` on the Delta Lake table
sql("DELETE FROM loan_data_by_state_silver_delta WHERE addr_state = 'IA'")

# COMMAND ----------

# Review current loans within the `loan_by_state_delta` Delta Lake table
tableToDF("loan_data_by_state_silver_delta") %>% 
  groupBy("addr_state") %>% 
  agg(count = "sum") %>% 
  display()

# COMMAND ----------

# MAGIC %md ### UPDATE Support
# MAGIC The data was originally supposed to be assigned to `WA` state, so let's `UPDATE` those values

# COMMAND ----------

# Attempting to run `UPDATE` on the Parquet table
sql("UPDATE loan_data_by_state_silver_pq SET n = 2700 WHERE addr_state = 'WA'")

# COMMAND ----------

# MAGIC %md **Note**: This command fails because the `UPDATE` statements are not supported in Parquet, but are supported in Delta Lake.

# COMMAND ----------

# Running `UPDATE` on the Delta Lake table
sql("UPDATE loan_data_by_state_silver_delta SET `count` = 2700 WHERE addr_state = 'WA'")

# COMMAND ----------

# Review current loans within the `loan_by_state_delta` Delta Lake table
tableToDF("loan_data_by_state_silver_delta") %>% 
  groupBy("addr_state") %>% 
  agg(count = "sum") %>% 
  display()

# COMMAND ----------

# MAGIC %md ### MERGE INTO Support
# MAGIC 
# MAGIC #### INSERT or UPDATE parquet: 7-step process
# MAGIC 
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC 
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC 
# MAGIC 
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC 
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# Let's create a simple R dataframe to merge
merge_df <- data.frame(
  addr_state = c("IA", "CA", "OR"),
  count = c(0, 2500, 0)
)

# Copy to Spark and create temp view
mergeDF <- createDataFrame(merge_df) 
createOrReplaceTempView(mergeDF, "merge_table")

display(mergeDF)

# COMMAND ----------

# MAGIC %md Instead of writing separate `INSERT` and `UPDATE` statements, we can use a `MERGE` statement. 

# COMMAND ----------

sql(
  "
MERGE INTO loan_data_by_state_silver_delta as d
USING merge_table as m
on d.addr_state = m.addr_state
WHEN MATCHED THEN 
  UPDATE SET *
WHEN NOT MATCHED 
  THEN INSERT *
")

# COMMAND ----------

# Review current loans within the `loan_by_state_delta` Delta Lake table
tableToDF("loan_data_by_state_silver_delta") %>% 
  groupBy("addr_state") %>% 
  agg(count = "sum") %>% 
  display()

# COMMAND ----------

# MAGIC %md ## Let's Travel back in Time!
# MAGIC Databricks Delta’s time travel capabilities simplify building data pipelines for the following use cases. 
# MAGIC 
# MAGIC * Audit Data Changes
# MAGIC * Reproduce experiments & reports
# MAGIC * Rollbacks
# MAGIC 
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC 
# MAGIC You can query by:
# MAGIC 1. Using a timestamp
# MAGIC 1. Using a version number
# MAGIC 
# MAGIC using Python, Scala, and/or R syntax; for these examples we will use the SQL syntax.  
# MAGIC 
# MAGIC For more information, refer to [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %md ### Review Delta Lake Table History
# MAGIC All the transactions for this table are stored within this table including the initial set of insertions, update, delete, merge, and inserts with schema modification

# COMMAND ----------

display(sql("DESCRIBE HISTORY loan_data_by_state_silver_delta"))

# COMMAND ----------

# MAGIC %md ### Time Travel via Version Number
# MAGIC Below are SQL syntax examples of Delta Time Travel by using a Version Number

# COMMAND ----------

display(sql("SELECT * FROM loan_data_by_state_silver_delta VERSION AS OF 0"))

# COMMAND ----------

display(sql("SELECT * FROM loan_data_by_state_silver_delta VERSION AS OF 9"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean Up

# COMMAND ----------

sql("DROP TABLE loan_data_bronze_delta")
sql("DROP TABLE loan_data_by_state_silver_delta")
sql("DROP TABLE loan_data_by_state_silver_pq")

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /tmp/loan_by_state_delta

# COMMAND ----------

# MAGIC %fs
# MAGIC rm -r /tmp/loan_by_state.parquet