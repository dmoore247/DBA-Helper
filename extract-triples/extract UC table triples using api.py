# Databricks notebook source
# table scanner

# COMMAND ----------

# MAGIC %pip install --quiet databricks-sql-connector 

# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = "e2-demo-field-eng.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/ead10bf07050390f"
metastore = "metastore"
namespace = "dbx"

# COMMAND ----------

from databricks import sql
import os

connection = sql.connect(
                        server_hostname = host,
                        http_path = http_path,
                        access_token = token)

cursor = connection.cursor()

cursor.execute("SELECT current_metastore()")
metastores = [(row[0]) for row in cursor.fetchall()]
metastore = metastores[0]
print(metastore)

cursor.close()
connection.close()

# COMMAND ----------

# Connect to databricks SQL warehouse
from databricks import sql
from databricks.sql.exc import ServerOperationError, DatabaseError
import os

connection = sql.connect(
                        server_hostname = host,
                        http_path = http_path,
                        access_token = token)

cursor = connection.cursor()

#
triples = []

# generate a list of catalogs
catalogs = cursor.catalogs()
print("Catalogs found:")
for catalog_row in catalogs:
  catalog = catalog_row[0]
  triples.append((f"{namespace}:{metastore}",f"{namespace}x:hasCatalog",f"{namespace}:{catalog}"))
  print("Catalog: ", catalog)
  try:
    schemas = cursor.schemas(catalog_name=catalog)
    for schema_row in schemas:
        schema = schema_row[0]
        print(f'\tSchema: {schema}')
        triples.append((f"{namespace}:{catalog}",f"{namespace}:hasSchema",f"{namespace}:{schema}"))
        # print a list of tables for each schema
        try:
          tables = cursor.tables(catalog_name=catalog, schema_name=schema)
          for table_row in tables:
            table = table_row[0]
            #print(f'Table: {table}')
            triples.append((f"{namespace}:{schema}",f"{namespace}:hasTable",f"{namespace}:{table}"))
        except ServerOperationError as e:
          print(f"error {e}")
          if e.message:
            triples.append((f"{namespace}:{schema}",f"{namespace}:hasErrorMessage",f"{namespace}:{e.message}"))
  except ServerOperationError as e:
      print(f"error {e}")
      if e.message:
        triples.append((f"{namespace}:{catalog}",f"{namespace}:hasErrorMessage",f"{namespace}:{e.message}"))
  except DatabaseError as e:
      print(f"error {e}")
      if e.message:
        triples.append((f"{namespace}:{catalog}",f"{namespace}:hasErrorMessage",f"{namespace}:{e.message}"))
  

# COMMAND ----------

triples_df = spark.createDataFrame(triples,schema="s STRING, p STRING, o STRING")

# COMMAND ----------

triples_df.count()

# COMMAND ----------

dbutils.data.summarize(triples_df)

# COMMAND ----------

triples_df.filter("p = 'dbx:hasErrorMessage'").display()

# COMMAND ----------

from pyspark.sql.functions import lit
quads_df = triples_df.withColumn('g',lit('ex:metastoreScan20230603'))
quads_df.write.saveAsTable('main.triplestore.raw_quads',mode='overwrite')

# COMMAND ----------



# COMMAND ----------


