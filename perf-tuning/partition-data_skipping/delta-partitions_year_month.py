# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC #### create db if does not exists

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled",False)

# COMMAND ----------

# create database if not exists
_target_database_name = 'dl_playground'
_target_table_name = 'perf_test_partitions_by_year_month'
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
# MAGIC DELETE FROM dl_playground.perf_test_partitions_by_year_month;
# MAGIC VACUUM dl_playground.perf_test_partitions_by_year_month RETAIN 0 HOURS;
# MAGIC 
# MAGIC drop table dl_playground.perf_test_partitions_by_year_month;

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

# 1,000,000,000,
flightsDF.count()

# COMMAND ----------

# get the raw data stats

display(flightsDF.describe())

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### save raw table with partitions

# COMMAND ----------

# create table 

(
  flightsDF
  .write
  .format("delta")
  .partitionBy("Year", "Month")
  .saveAsTable("dl_playground.perf_test_partitions_by_year_month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### bench mark few random queries

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select count(*) from dl_playground.perf_test_partitions_by_year_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select id, * from dl_playground.perf_test_partitions_by_year_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select id, * from dl_playground.perf_test_partitions_by_year_month
# MAGIC WHERE TailNum IN ('N201LV', 'N201LV');

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select Year, Month, DayofMonth, count(*)
# MAGIC from dl_playground.perf_test_partitions_by_year_month
# MAGIC group by Year, Month, DayofMonth
# MAGIC order by 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select Year, Month, DayofMonth, count(*)
# MAGIC from dl_playground.perf_test_partitions_by_year_month
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

# build partition pruning helper

partition_columns = ["Year", "Month"] # List of table partition columnm names for source table

partitions = incDF.select(*partition_columns).distinct().collect()
print(partitions)

_partition_filter = ""
for p in partitions:
  _partition_filter = _partition_filter + " AND ( t.Year == " + str(p[0]) + " AND t.Month == " + str(p[1]) + ")" + "\n"

print(_partition_filter)


# COMMAND ----------

from delta.tables import *

target_table = DeltaTable.forName(spark, "dl_playground.perf_test_partitions_by_year_month")

# merge
(target_table
 .alias("t")
 .merge(incDF.alias("s"),  f"t.id = s.id {_partition_filter}") 
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute() 
)

# COMMAND ----------

# merge
(target_table
 .alias("t")
 .merge(incDF.alias("s"),  f"t.id = s.id {_partition_filter}") 
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### get the physical layout of data in adls

# COMMAND ----------

