# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Auto Optimize

# COMMAND ----------

spark.sql("set spark.databricks.delta.optimizeWrite.enabled = true")
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")

# COMMAND ----------

_target_database_name = "dl_dde_regular"
_target_table_name = "dbo_decisionresultlog_new"
_ref_table_name = "dbo_decisionresultlog"
_delta_target_db_path = 'abfss://regular@qas' + 'pldatalake' + '.dfs.core.windows.net/database/' + _target_database_name

print(_delta_target_db_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### create db if does not exists

# COMMAND ----------

_sql_create_db = f"CREATE DATABASE IF NOT EXISTS {_target_database_name} COMMENT 'This is testing database' LOCATION '{_delta_target_db_path}'";

print(_sql_create_db)

spark.sql(_sql_create_db)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### drop table

# COMMAND ----------

# proper cleanup part 1
spark.sql(f"DELETE FROM {_target_database_name}.{_target_table_name};")
spark.sql(f"VACUUM {_target_database_name}.{_target_table_name} RETAIN 0 HOURS")

# COMMAND ----------

# proper cleanup part 1
spark.sql(f"drop table {_target_database_name}.{_target_table_name};")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### read original table

# COMMAND ----------

# raw data
from pyspark.sql.functions import monotonically_increasing_id, lit
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import expr

refDF = (spark
             .read
             .table(f"{_target_database_name}.{_ref_table_name}")
            )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### create new table using original table

# COMMAND ----------

# initial insert to table 
(
  refDF  
  .write
  .format("delta")
  .partitionBy("delta_load_date_utc")
  .saveAsTable(f"{_target_database_name}.{_target_table_name}")
)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### read data from new table

# COMMAND ----------

flightsDF = (spark
             .read
             .table(f"{_target_database_name}.{_target_table_name}")
            )

# COMMAND ----------

# bump up number of rows
# for i in range(8):
#   flightsDF = ( flightsDF
#                  .union(flightsDF)
#               )

# COMMAND ----------

# MAGIC %md 
# MAGIC #### create bucket calculated column to partittion data on primary key

# COMMAND ----------

from pyspark.sql.functions import round

# flightsDF = flightsDF.withColumn("id", monotonically_increasing_id())
flightsDF = flightsDF.withColumn("div", lit(1_000_000))
flightsDF = flightsDF.withColumn("par", round(expr("(decisionresultlogid/div)").cast('integer'),0))

# COMMAND ----------

display(flightsDF)

# COMMAND ----------

# 1,794,490,368

# 7_009_728
flightsDF.count()

# COMMAND ----------

display(flightsDF.groupBy("par").count().orderBy("par"))

# COMMAND ----------

display(flightsDF.select("decisionresultlogid", "par").orderBy("decisionresultlogid"))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### overwrite table with new partitin scheme

# COMMAND ----------

# change partitions
(
  flightsDF  
  .write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .partitionBy("par")
  .saveAsTable(f"{_target_database_name}.{_target_table_name}")
)

# COMMAND ----------

display(spark.sql(f"describe table {_target_database_name}.{_target_table_name};"))

# COMMAND ----------

display(spark.sql(f"describe history {_target_database_name}.{_target_table_name};"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Z-Order by primary key, hoping improve merge performance

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dl_newc_regular.dbo_decisionresultlog_new CHANGE data_lake_integrate_time_utc after decisionresultlogid 

# COMMAND ----------

spark.sql(f"OPTIMIZE {_target_database_name}.{_target_table_name} ZORDER BY decisionresultlogid, data_lake_integrate_time_utc;")

# COMMAND ----------

flightsDF = (spark
             .read
             .table(f"{_target_database_name}.{_target_table_name}")
            )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### test merge

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### incremental data for merge

# COMMAND ----------

# prep incremental data 
# add subset of original data for merge

# 70098
# incDF = flightsDF
incDF = flightsDF.filter("par = 9").sample(False, 0.05, seed=0).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### merge

# COMMAND ----------

# build partition pruning helper
partition_columns = ["par"] # List of table partition columnm names for source table

# partitions = incDF.select(*partition_columns).distinct().collect()
partitions = list(incDF.select('par').distinct().toPandas()['par'])

_partition_filter = " AND ( t.par in (" + ','.join(map(str, partitions)) + ") )" 

print(_partition_filter)

# COMMAND ----------

# MAGIC %md #### new merge

# COMMAND ----------

from delta.tables import *

target_table = DeltaTable.forName(spark, f"{_target_database_name}.{_target_table_name}")

# merge
(target_table
 .alias("t")
 .merge(incDF.alias("s"),  f"t.decisionresultlogid = s.decisionresultlogid {_partition_filter}") 
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute() 
)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### old merge

# COMMAND ----------

from delta.tables import *

target_table = DeltaTable.forName(spark, f"{_target_database_name}.{_ref_table_name}")

# merge
(target_table
 .alias("t")
 .merge(incDF.alias("s"),  f"t.decisionresultlogid = s.decisionresultlogid") 
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute() 
)

# COMMAND ----------

display(spark.sql(f"describe history {_target_database_name}.{_target_table_name};"))

# COMMAND ----------

incDF.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### bench mark few random queries

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled",False)

# COMMAND ----------

# MAGIC %md ##### sql test 1

# COMMAND ----------

display(spark.sql(f"select * from {_target_database_name}.{_target_table_name} order by data_lake_integrate_time_utc desc limit 10"))

# COMMAND ----------

display(spark.sql(f"select * from {_target_database_name}.{_ref_table_name} order by data_lake_integrate_time_utc desc limit 10"))

# COMMAND ----------

# MAGIC %md ##### sql test 2

# COMMAND ----------

display(spark.sql(f"select * from {_target_database_name}.{_target_table_name} where data_lake_integrate_time_utc >= '2021-10-01' "))

# COMMAND ----------

display(spark.sql(f"select * from {_target_database_name}.{_ref_table_name} where data_lake_integrate_time_utc >= '2021-10-01' "))

# COMMAND ----------

# MAGIC %md ##### sql test 3

# COMMAND ----------

display(spark.sql(f"select * from {_target_database_name}.{_target_table_name} where decisionresultlogid = 3216754 "))

# COMMAND ----------

display(spark.sql(f"select * from {_target_database_name}.{_ref_table_name} where decisionresultlogid = 3216754 "))

# COMMAND ----------

# MAGIC %md ##### sql test 4

# COMMAND ----------

display(spark.sql(f"select * from {_target_database_name}.{_target_table_name} where data_lake_integrate_key >= '615B4D77003875FE' "))

# COMMAND ----------

display(spark.sql(f"select * from {_target_database_name}.{_ref_table_name} where data_lake_integrate_key >= '615B4D77003875FE' "))

# COMMAND ----------



# COMMAND ----------

display(spark.sql(f"describe history {_target_database_name}.{_target_table_name} "))

# COMMAND ----------

display(spark.sql(f"describe history {_target_database_name}.{_ref_table_name} "))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from dl_dde_regular.dbo_decisionresultlog

# COMMAND ----------

