# Databricks notebook source
# MAGIC %pip install --quiet databricks-sql-connector databricks-sdk boto3 botocore

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = "e2-demo-field-eng.cloud.databricks.com"

# COMMAND ----------

from clusters import *
from utils import save_graph

# COMMAND ----------

time1 = time.time()
triples = extract_clusters(host, token)
time2 = time.time()
print(f'Took {time2-time1:.2f} s')
pp.pprint(triples)
#main()

# COMMAND ----------

save_graph(spark, triples, graph_name="clusters_20230610")

# COMMAND ----------


