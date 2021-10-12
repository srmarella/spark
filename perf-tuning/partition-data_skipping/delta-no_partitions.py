# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #### create db if does not exists

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled",False)

# COMMAND ----------

# create database if not exists
_target_database_name = 'dl_playground'
_target_table_name = 'perf_test_no_partitions'
_delta_target_db_path = 'abfss://regular@prd' + 'pldatalake' + '.dfs.core.windows.net/database/' + _target_database_name

print(_delta_target_db_path)
  

# COMMAND ----------

_sql_create_db = f"CREATE DATABASE IF NOT EXISTS {_target_database_name} COMMENT 'This is testing database' LOCATION '{_delta_target_db_path}'";
print(_sql_create_db)

spark.sql(_sql_create_db)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### drop table

# COMMAND ----------

# MAGIC %sql
# MAGIC --  proper cleanup
# MAGIC DELETE FROM dl_playground.perf_test_no_partitions;
# MAGIC VACUUM dl_playground.perf_test_no_partitions RETAIN 0 HOURS;
# MAGIC 
# MAGIC drop table dl_playground.perf_test_no_partitions;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### clear delta cache

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled",False)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### raw data and stats

# COMMAND ----------

# raw data
from pyspark.sql.functions import monotonically_increasing_id

flightsDF = (spark
           .read
           .format("csv")
           .option("header", "true")
           .option("inferSchema", "true")
           .load("/databricks-datasets/asa/airlines/2008.csv")
          )

for i in range(8):
  flightsDF = ( flightsDF
                 .union(flightsDF)
              )

flightsDF = flightsDF.withColumn("id", monotonically_increasing_id())

# display(flightsDF)

# COMMAND ----------

# get the raw data stats

display(flightsDF.describe())

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### save raw table as delta (heap table)

# COMMAND ----------

# create table 

(
  flightsDF
  .write
  .format("delta")
  .saveAsTable("dl_playground.perf_test_no_partitions")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### bench mark few random queries

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select count(*) from dl_playground.perf_test_no_partitions;

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select id, * from dl_playground.perf_test_no_partitions;

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select id, * from dl_playground.perf_test_no_partitions
# MAGIC WHERE TailNum IN ('N201LV', 'N201LV');

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select Year, Month, DayofMonth, count(*)
# MAGIC from dl_playground.perf_test_no_partitions
# MAGIC group by Year, Month, DayofMonth
# MAGIC order by 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select Year, Month, DayofMonth, count(*)
# MAGIC from dl_playground.perf_test_no_partitions
# MAGIC WHERE TailNum IN ('N201LV', 'N201LV')
# MAGIC group by Year, Month, DayofMonth
# MAGIC order by 1,2,3

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### test merge

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### incremental data for merge

# COMMAND ----------

# prep incremental data 
# re subset of original data for merge

incDF = flightsDF.sample(False, 0.1, seed=0)

# get the inc raw data stats
display(incDF.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### merge

# COMMAND ----------

from delta.tables import *

target_table = DeltaTable.forName(spark, "dl_playground.perf_test_no_partitions")

# merge
(target_table
 .alias("t")
 .merge(incDF.alias("s"),  "t.id = s.id") 
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute() 
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### get the physical layout of data in adls

# COMMAND ----------

