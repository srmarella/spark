# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Auto Optimize

# COMMAND ----------

spark.sql("set spark.databricks.delta.optimizeWrite.enabled = true")
spark.sql("set spark.databricks.delta.autoCompact.enabled = true")

# COMMAND ----------

_target_database_name = "dl_playground"
_target_table_name = "perf_test_partitions_bucketby_primarykey_v2"
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
# MAGIC #### raw data and stats

# COMMAND ----------

# get the source file size
dbutils.fs.ls("/databricks-datasets/asa/airlines/2008.csv")

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

# COMMAND ----------

# bump up number of rows
# for i in range(8):
#   flightsDF = ( flightsDF
#                  .union(flightsDF)
#               )

# COMMAND ----------

from pyspark.sql.functions import round

flightsDF = flightsDF.withColumn("id", monotonically_increasing_id())
flightsDF = flightsDF.withColumn("div", lit(1_000_000))
flightsDF = flightsDF.withColumn("par", round(expr("(id/div)").cast('integer'),0))

# COMMAND ----------

display(flightsDF)

# COMMAND ----------

# 1,794,490,368

# 7_009_728
flightsDF.count()

# COMMAND ----------

display(flightsDF.groupBy("par").count().orderBy("par"))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC #### save raw table with partitions

# COMMAND ----------

# initial insert to table 
(
  flightsDF  
  .write
  .format("delta")
  .partitionBy("par")
  .saveAsTable(f"{_target_database_name}.{_target_table_name}")
)

# COMMAND ----------

display(spark.sql(f"describe table {_target_database_name}.{_target_table_name};"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Z-Order by primary key, hoping improve merge performance

# COMMAND ----------

spark.sql(f"OPTIMIZE {_target_database_name}.{_target_table_name} ZORDER BY id;")

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

incDF = flightsDF.sample(False, 0.1, seed=0).cache()

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

from delta.tables import *

target_table = DeltaTable.forName(spark, f"{_target_database_name}.{_target_table_name}")

# merge
(target_table
 .alias("t")
 .merge(incDF.alias("s"),  f"t.id = s.id {_partition_filter}") 
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute() 
)

# COMMAND ----------

incDF.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### bench mark few random queries

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled",False)

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

display(spark.sql(f"select id, * from {_target_database_name}.{_target_table_name} WHERE TailNum IN ('N201LV', 'N201LV')"))

# COMMAND ----------

display(spark.sql(f"select Year, Month, DayofMonth, count(*) from {_target_database_name}.{_target_table_name} group by Year, Month, DayofMonth order by 1,2,3 "))

# COMMAND ----------

display(spark.sql(f"select Year, Month, DayofMonth, count(*) from {_target_database_name}.{_target_table_name} WHERE TailNum IN ('N201LV', 'N201LV') group by Year, Month, DayofMonth order by 1,2,3 "))

# COMMAND ----------

display(spark.sql(f"select Year, Month, DayofMonth, count(*) from {_target_database_name}.{_target_table_name} WHERE Id IN (42950000147,42950000148, 18) group by Year, Month, DayofMonth order by 1,2,3 "))

# COMMAND ----------

display(spark.sql(f"describe history {_target_database_name}.{_target_table_name} "))

# COMMAND ----------

