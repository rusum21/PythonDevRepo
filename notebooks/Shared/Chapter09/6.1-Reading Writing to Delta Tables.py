# Databricks notebook source
storageAccount="cookbookadlsgen2storage"
mountpoint = "/mnt/Gen2Source"
storageEndPoint ="abfss://rawdata@{}.dfs.core.windows.net/".format(storageAccount)
print ('Mount Point ='+mountpoint)

#ClientId, TenantId and Secret is for the Application(ADLSGen2App) was have created as part of this recipe
clientID ="2sdasdasd69-4asda-b6fa-adasd0db"
tenantID ="sdasdad2a4-asdasdesdasdas14-e5f4b5a5375d"
clientSecret ="asdasdasdd"
oauth2Endpoint = "https://login.microsoftonline.com/{}/oauth2/token".format(tenantID)


configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": clientID,
           "fs.azure.account.oauth2.client.secret": clientSecret,
           "fs.azure.account.oauth2.client.endpoint": oauth2Endpoint}

try:
  dbutils.fs.mount(
  source = storageEndPoint,
  mount_point = mountpoint,
  extra_configs = configs)
except:
    print("Already mounted...."+mountpoint)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/Gen2Source/Customer/parquetFiles"))

# COMMAND ----------

db = "tpchdb"
 
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")
 
spark.sql("SET spark.databricks.delta.formatCheck.enabled = false")
spark.sql("SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")

# COMMAND ----------

import random
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Customer parquet files from the mount point and writing to Delta tables.

# COMMAND ----------

#Reading parquet files and adding a new column to the dataframe and writing to delta table
cust_path = "/mnt/Gen2Source/Customer/parquetFiles"
 
df_cust = (spark.read.format("parquet").load(cust_path)
      .withColumn("timestamp", current_timestamp()))
 
df_cust.write.format("delta").mode("overwrite").save("/mnt/Gen2Source/Customer/delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating Delta table 
# MAGIC DROP TABLE IF EXISTS Customer;
# MAGIC CREATE TABLE Customer
# MAGIC USING delta
# MAGIC location "/mnt/Gen2Source/Customer/delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Customer limit 10;

# COMMAND ----------

# MAGIC %fs ls /mnt/Gen2Source/Orders/parquetFiles/

# COMMAND ----------

#Reading Orders parquet files and adding a new column to the dataframe and writing as delta format. Creating partition on year column.
ord_path = "/mnt/Gen2Source/Orders/parquetFiles"
 
df_ord = (spark.read.format("parquet").load(ord_path)
      .withColumn("timestamp", current_timestamp())
      .withColumn("O_OrderDateYear", year(col("O_OrderDate")))
     )

# df.printSchema()
 
df_ord.write.format("delta").partitionBy("O_OrderDateYear").mode("overwrite").save("/mnt/Gen2Source/Orders/delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Orders;
# MAGIC CREATE TABLE Orders
# MAGIC USING delta
# MAGIC location "/mnt/Gen2Source/Orders/delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted Orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Getting total customer based on priority and total account balance by joing the delta tables
# MAGIC SELECT o.O_ORDERPRIORITY,count(o.O_CustKey) As TotalCustomer,Sum(c.C_AcctBal) As CustAcctBal
# MAGIC FROM Orders o 
# MAGIC INNER JOIN Customer c on o.O_CustKey=c.C_CustKey 
# MAGIC WHERE o.O_OrderDateYear>1997
# MAGIC GROUP BY o.O_OrderPriority
# MAGIC ORDER BY TotalCustomer DESC;

# COMMAND ----------

# We can read the Delta files in Python using DeltaTable libraries as shown below 
deltaTable = DeltaTable.forPath(spark, "/mnt/Gen2Source/Orders/delta/")

# COMMAND ----------

#Converting to DF and viewing dataframe contents
dt=deltaTable.toDF()
dt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DML Support in Delta Table.
# MAGIC   We can perform DML opreations like insert,delete and updates to the Delta table.

# COMMAND ----------

# Reading new data which is in the folder newParquetFiles in the same orders folder in the /nnt/Gen2Source
# We will see how to perform merge to insert anf update the records in the delta table.

#Reading Orders parquet files and adding a new column to the dataframe and writing as delta format. Creating partition on year column.
new_ord_path = "/mnt/Gen2Source/Orders/newParquetFiles"
 
df_new_order = (spark.read.format("parquet").load(new_ord_path)
      .withColumn("timestamp", current_timestamp())
      .withColumn("O_OrderDateYear", year(col("O_OrderDate")))
     )

# df.printSchema()
 
df_new_order.write.format("delta").partitionBy("O_OrderDateYear").mode("overwrite").save("/mnt/Gen2Source/Orders/dailydata_delta")



# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Orders_Daily;
# MAGIC CREATE TABLE Orders_Daily
# MAGIC USING delta
# MAGIC location "/mnt/Gen2Source/Orders/dailydata_delta"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merging into Orders from Orders_Daily table. If records matche on OrderKey then perform update eles insert
# MAGIC MERGE INTO Orders AS o
# MAGIC USING Orders_daily AS d
# MAGIC ON o.O_ORDERKEY = D.O_ORDERKEY
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Deleting from Delta table
# MAGIC DELETE FROM Orders WHERE O_ORDERKEY=1359427

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ORDERS WHERE O_ORDERKEY=1359427

# COMMAND ----------

# MAGIC %fs ls /mnt/Gen2Source/Orders/ParquetOutputFormatted

# COMMAND ----------

# MAGIC %sql CONVERT TO DELTA parquet.`dbfs:/mnt/Gen2Source/Orders/ParquetOutputFormatted`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Orders_Test
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/Gen2Source/Orders/ParquetOutputFormatted"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted Orders_Test

# COMMAND ----------

#dbutils.fs.unmount("/mnt/Gen2Source")