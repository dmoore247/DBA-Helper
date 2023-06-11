# Databricks notebook source
# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %pip install --quiet databricks-sql-connector databricks-sdk botocore

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from principals import *

# COMMAND ----------

# MAGIC %md ## Walk groups

# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = "e2-demo-field-eng.cloud.databricks.com"


# COMMAND ----------

triples = walk_groups(host,token)
len(triples)

# COMMAND ----------

triples

# COMMAND ----------

# Save to KG
graph_name = "e2-demo groups 20230609"

from pyspark.sql.functions import lit
triples_df = spark.createDataFrame(triples, schema="s STRING, p STRING, o STRING")
quads_df = triples_df.withColumn('g',lit(f'ex:{graph_name}'))
quads_df.write.saveAsTable('main.triplestore.raw_quads',mode='append')

# COMMAND ----------

# MAGIC %md ## Walk Users

# COMMAND ----------

triples, count = walk_users(host,token)
pp.pprint((count, len(triples), triples[:100]))

# COMMAND ----------

len(triples)

# COMMAND ----------

triples_df = spark.createDataFrame(triples)

# COMMAND ----------

triples_df.count()

# COMMAND ----------

display(triples_df)

# COMMAND ----------

# Save to KG
graph_name = "e2-demo users 20230609"

from pyspark.sql.functions import lit
triples_df = spark.createDataFrame(triples, schema="s STRING, p STRING, o STRING")
quads_df = triples_df.withColumn('g',lit(f'ex:{graph_name}'))
quads_df.write.saveAsTable('main.triplestore.raw_quads',mode='append')
