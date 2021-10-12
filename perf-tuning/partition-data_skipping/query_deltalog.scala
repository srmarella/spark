// Databricks notebook source
import com.databricks.sql.transaction.tahoe.DeltaLog
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// COMMAND ----------

val tableDeltaLogPath = "abfss://regular@qaspldatalake.dfs.core.windows.net/database/dl_newc_regular/dbo_loansuffix_new/_delta_log/"

// COMMAND ----------

val log = DeltaLog(spark, new Path(tableDeltaLogPath))

// COMMAND ----------

val logFileDF = log.snapshot.allFiles

// COMMAND ----------

log.snapshot.allFiles.printSchema()

// COMMAND ----------

display(log.snapshot.allFiles)

// COMMAND ----------

val df = log.snapshot.allFiles

df.createOrReplaceTempView("allLogFiles")

// COMMAND ----------

log.snapshot.allFiles.count()

// COMMAND ----------

// MAGIC %sql 
// MAGIC select 
// MAGIC case 
// MAGIC      when size < 200000 then '< 200 kb '
// MAGIC      when size >= 200000 and size < 300000 then '< 200 kb to 300 kb'
// MAGIC      when size >= 300000 and size < 500000 then '< 300 kb to 500 kb'
// MAGIC      when size >= 500000 and size < 1000000  then '500 kb to 1 mb'
// MAGIC      when size >= 1000000 and size < 8000000  then '1 mb to 8 mb'
// MAGIC      when size >= 8000000 and size < 128000000  then '8 mb to 128 mb'
// MAGIC      when size >= 128000000 and size < 256000000  then '128 mb to 256 mb'
// MAGIC      when size >= 256000000 and size < 512000000  then '256 mb to 512 mb'
// MAGIC      else 'over 512 mb'
// MAGIC  end as PartitionFileSizeRange
// MAGIC  ,Count(*) as no_filer_per_partition
// MAGIC from allLogFiles 
// MAGIC GROUP BY 1 order by no_filer_per_partition

// COMMAND ----------

