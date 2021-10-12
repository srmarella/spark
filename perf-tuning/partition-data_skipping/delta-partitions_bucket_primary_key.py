# Databricks notebook source
# ALTER TABLE myDatabase.myTable SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### some delta perf configurations

# COMMAND ----------

# Standard EDSv4 Family vCPUs 128 GB, 16 Cores

spark.conf.set("spark.databricks.io.cache.enabled",False)
spark.conf.set("spark.databricks.delta.optimizeWrite", True)
# spark.conf.set("spark.databricks.delta.optimizeWrite.numShuffleBlocks", xxxxxx)
spark.conf.set("spark.databricks.delta.properties.defaults.randomizeFilePrefixes", True)
spark.conf.set("spark.databricks.optimizer.dynamicFilePruning", True)
# spark.databricks.delta.autoCompact.maxFileSize
spark.conf.set("spark.databricks.delta.autoCompact.enabled", True)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled",False)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### clear delta cache

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled",False)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### drop table

# COMMAND ----------

# MAGIC %sql
# MAGIC --  proper cleanup
# MAGIC DELETE FROM dl_playground.perf_test_partitions_bucketby_primarykey;
# MAGIC VACUUM dl_playground.perf_test_partitions_bucketby_primarykey RETAIN 0 HOURS;
# MAGIC 
# MAGIC drop table dl_playground.perf_test_partitions_bucketby_primarykey;

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### create db if does not exists

# COMMAND ----------

# create database if not exists
_target_database_name = 'dl_playground'
_target_table_name = 'perf_test_partitions_bucketby_primarykey'
_delta_target_db_path = 'abfss://regular@qas' + 'pldatalake' + '.dfs.core.windows.net/database/' + _target_database_name

print(_delta_target_db_path)
  

# COMMAND ----------

_sql_create_db = f"CREATE DATABASE IF NOT EXISTS {_target_database_name} COMMENT 'This is testing database' LOCATION '{_delta_target_db_path}'";
print(_sql_create_db)

spark.sql(_sql_create_db)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### raw data and stats

# COMMAND ----------

# raw data
from pyspark.sql.functions import monotonically_increasing_id, lit
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import expr

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
flightsDF = flightsDF.withColumn("_div", lit(100000))
flightsDF = flightsDF.withColumn("par", expr("mod(id, _div)"))

# display(flightsDF)

# COMMAND ----------

# 1,794,490,368
flightsDF.count()

# COMMAND ----------

display(flightsDF.groupBy("par").count())

# COMMAND ----------

display(flightsDF)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### save raw table with partitions

# COMMAND ----------

# create table shell
# this messed up partitions
# (
#   flightsDF
#   .limit(0)
#   .write
#   .format("delta")
#   .partitionBy("par")
#   .saveAsTable("dl_playground.perf_test_partitions_bucketby_primarykey")
# )


# COMMAND ----------

# MAGIC %sql 
# MAGIC -- set delta file size 32 MB
# MAGIC ALTER TABLE dl_playground.perf_test_partitions_bucketby_primarykey SET TBLPROPERTIES (delta.targetFileSize = 32000000);

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC describe table dl_playground.perf_test_partitions_bucketby_primarykey

# COMMAND ----------

# append data to table 
(
  flightsDF  
  .write
  .format("delta")
  .partitionBy("par")
  .saveAsTable("dl_playground.perf_test_partitions_bucketby_primarykey", mode="append")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE events
# MAGIC WHERE date >= current_timestamp() - INTERVAL 1 day
# MAGIC ZORDER BY (eventType)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### bench mark few random queries

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select count(*) from dl_playground.perf_test_partitions_bucketby_primarykey;

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select id, * from dl_playground.perf_test_partitions_bucketby_primarykey;

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select id, * from dl_playground.perf_test_partitions_bucketby_primarykey
# MAGIC WHERE TailNum IN ('N201LV', 'N201LV');

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select Year, Month, DayofMonth, count(*)
# MAGIC from dl_playground.perf_test_partitions_bucketby_primarykey
# MAGIC group by Year, Month, DayofMonth
# MAGIC order by 1,2,3

# COMMAND ----------

# MAGIC %sql
# MAGIC CLEAR CACHE;
# MAGIC 
# MAGIC select Year, Month, DayofMonth, count(*)
# MAGIC from dl_playground.perf_test_partitions_bucketby_primarykey
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


partition_columns = ["par"] # List of table partition columnm names for source table

partitions = incDF.select(*partition_columns).distinct().collect()
print(partitions)

for p in partitions:
  partition_filter = ' AND '.join(f'T.{col} = "{p[col]}"' for col in partition_columns)

print(partition_filter)

    



# COMMAND ----------

from delta.tables import *

target_table = DeltaTable.forName(spark, "dl_playground.perf_test_partitions_bucketby_primarykey")

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

