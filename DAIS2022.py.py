# Databricks notebook source
# MAGIC %md # run_parallel

# COMMAND ----------

# MAGIC %python
# MAGIC from multiprocessing.pool import ThreadPool
# MAGIC import multiprocessing as mp
# MAGIC 
# MAGIC def run_parallel(func, list_query) -> None:
# MAGIC   lst = spark.sql(list_query).collect()
# MAGIC   if len(lst) > 0:
# MAGIC     cpus = mp.cpu_count()
# MAGIC     with ThreadPool(cpus) as p:
# MAGIC       p.map(func, lst)

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# Gather file meta from all files in current_path and below
def catalog(current_path:str):
  return (spark.read.format("binaryFile")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.csv")
        .load(current_path)
        .drop('content'))
       
df = catalog('s3://bucket/data/raw')


# COMMAND ----------

database_name = 'information_schema'
spark.sql(F"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------


from pyspark.sql.utils import AnalysisException
def table_detail_func(row) -> None:
  """Capture DESCRIBE DETAIL metadata"""
  print(row)
  if row['isTemporary'] != 'true':
    try:
      (spark.sql(F"DESCRIBE DETAIL {row['database']}.{row['tableName']}")
              .write.format('delta')
              .mode('append').option('mergeSchema','true')
              .saveAsTable(F"{database_name}.table_details"))
    except AnalysisException as ae:
      print(ae)


run_parallel(table_detail_func, "show tables in default")

# COMMAND ----------


