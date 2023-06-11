# Databricks notebook source
# MAGIC %md ## Setup

# COMMAND ----------

# MAGIC %pip install --quiet databricks-sql-connector

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import boto3

from databricks import sql
from databricks.sql.exc import ServerOperationError, DatabaseError
import os

# COMMAND ----------

# DBA Helper utilities
from hms import getHMSTriples, getMetastoreURI
from utils import coalesce
from glue import *

# COMMAND ----------

# MAGIC %md ### Config

# COMMAND ----------

namespace = "dbx"
catalog = "hive_metastore"
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = "e2-demo-field-eng.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/ead10bf07050390f"
region = spark.conf.get("spark.databricks.clusterUsageTags.region", None)
graph_name = "defaultHMSScan20230606"

# COMMAND ----------

# MAGIC %md ## Metastore Configuration Info

# COMMAND ----------

getMetastoreURI(spark, namespace, host)

# COMMAND ----------


triples = getHMSTriples(spark=spark,namespace=namespace,host=host)

# COMMAND ----------

hms_df = spark.createDataFrame(triples, schema="s STRING, p STRING, o STRING")
hms_df.orderBy('s','p','o').display()

# COMMAND ----------

from pyspark.sql.functions import lit
quads_df = hms_df.withColumn('g',lit(f'ex:{graph_name}'))
quads_df.write.saveAsTable('main.triplestore.raw_quads',mode='append')

# COMMAND ----------

# MAGIC %md ## Walk GLUE catalog with recursive code
# MAGIC Use Boto APIs directly

# COMMAND ----------

# MAGIC %reload_ext autoreload

# COMMAND ----------

glue_direct_access = False

if glue_direct_access:
    from glue import getGlueDatabases, getGlueTables, tbl_record_func
    r = getGlueDatabases(getGlueTables, tbl_record_func, region)
    results = [x for x in list(r) if x]
    glue_tables_df = spark.createDataFrame(results,
                                          schema="database STRING, table STRING, createTime TIMESTAMP, locationUri STRING, description STRING")
    glue_tables_df.display()                                       

# COMMAND ----------

# MAGIC %md # Walk hive_metastore with SQL Endpoint
# MAGIC - Extract Schemas
# MAGIC - Extract Tables

# COMMAND ----------

metastore_uri = getMetastoreURI(spark=spark, namespace=namespace, host=host)

# COMMAND ----------

from databricks import sql
from databricks.sql.exc import ServerOperationError, DatabaseError
import os



connection = sql.connect(
                        server_hostname = host,
                        http_path = http_path,
                        access_token = token)

cursor = connection.cursor()


# COMMAND ----------


def extractHiveSchemas(metastore_uri:str, catalog:str) -> list[str]:
    cursor.schemas(catalog_name=catalog)
    schemas = [(row[0]) for row in cursor.fetchall()]

    triples = []
    for schema in schemas:
        triples.append((
            f"{namespace}:schema/{catalog}.{schema}",
            f"schema:memberOf",
             metastore_uri
            ))
    return triples

def extractHivetables(metastore_uri:str, catalog:str) -> list[str]:
    cursor.tables(catalog_name=catalog)

    triples = []
    for row in cursor.fetchall():
        catalog,schema,table = row[0:3]
        table_uri  = f"{namespace}:table/'{catalog}'.'{schema}'.'{table}'"
        schema_uri = f"{namespace}:table/'{catalog}'.'{schema}'"
        triples.append((table_uri,f"schema:memberOf", metastore_uri))
        triples.append((table_uri,f"schema:memberOf", schema_uri))
    return triples

triples = (
  extractHiveSchemas(metastore_uri, catalog) + 
  extractHivetables(metastore_uri, catalog)
  )

triples_df = spark.createDataFrame(triples, schema="s STRING, p STRING, o STRING")
triples_df.display()

# COMMAND ----------

dbutils.data.summarize(triples_df)

# COMMAND ----------

from pyspark.sql.functions import lit
quads_df = triples_df.withColumn('g',lit(f'ex:{graph_name}'))
quads_df.write.saveAsTable('main.triplestore.raw_quads',mode='append')

# COMMAND ----------

# MAGIC %md # Table ACLs

# COMMAND ----------

def extractHiveSchemas(metastore_uri:str, catalog:str) -> list[str]:
    cursor.schemas(catalog_name=catalog)
    schemas = [(row[0]) for row in cursor.fetchall()]
    acl_counter = 0

    triples = []
    for schema in schemas:
        schema_uri = f"{namespace}:schema/{catalog}.{schema}"
        print(schema_uri)
        triples.append((
            schema_uri,
            f"schema:memberOf",
             metastore_uri
            ))
        try:
          cursor.execute(f"SHOW GRANTS ON SCHEMA `{catalog}`.`{schema}`")
          for grants in cursor.fetchall():
              acl_uri = f'dbx:tacl{acl_counter}'
              triples.append((schema_uri, 'dbx:hasACL', ))
              triples.append((acl_uri, 'dbx:principal', grants[0]))
              triples.append((acl_uri, 'dbx:actionType', grants[1]))
              triples.append((acl_uri, 'dbx:objectType', grants[2]))
              triples.append((acl_uri, 'dbx:objectKey', grants[3]))
              acl_counter = acl_counter + 1 
        except ServerOperationError as e:
          print(e)
    return triples

extractHiveSchemas(metastore_uri, catalog)

# COMMAND ----------

from pyspark.sql.functions import lit
quads_df = triples_df.withColumn('g',lit(f'ex:{graph_name}'))
quads_df.write.saveAsTable('main.triplestore.raw_quads',mode='append')

# COMMAND ----------

from pyspark.sql.functions import udf

# COMMAND ----------

import re

# COMMAND ----------

def grants_on_schema(cursor, metastore_uri:str, catalog:str, schema:str):
  from databricks import sql
  from databricks.sql.exc import ServerOperationError, DatabaseError
  import os
  t = []
  #print(f'starting {schema}')
  
  schema_uri = f"{namespace}:schema/{catalog}.{schema}"
  t.append((
              schema_uri,
              f"schema:memberOf",
              metastore_uri
              ))

  query = f"SHOW GRANTS ON SCHEMA `{catalog}`.`{schema}`"
  cursor.execute(query)
  for grants in cursor.fetchall():
      acl_uri = f'dbx:tacl/schema/{catalog}.{schema}'
      t.append((schema_uri, 'dbx:hasACL', ))
      t.append((acl_uri, 'dbx:principal', grants[0]))
      t.append((acl_uri, 'dbx:actionType', grants[1]))
      t.append((acl_uri, 'dbx:objectType', grants[2]))
      t.append((acl_uri, 'dbx:objectKey', grants[3]))
  return t

# COMMAND ----------

def describe_schema(cursor, catalog:str, schema:str):
  query = f"DESCRIBE SCHEMA EXTENDED `{catalog}`.`{schema}`"
  cursor.execute(query)
  schema_uri = f"{namespace}:schema/{catalog}.{schema}"
  t = []
  kv_pattern = r"\(([\w]+),([\w]+)\)"

  for attributes in cursor.fetchall():
    if attributes[0] == 'Comment': t.append((schema_uri, 'dbx:hasComment', attributes[1]))
    if attributes[0] == 'Owner': t.append((schema_uri, 'dbx:owner', attributes[1]))
    if attributes[0] == 'Location': t.append((schema_uri, 'dbx:location', attributes[1]))
    if attributes[0] == 'Properties':
      property_uri = f'dbx:prop/schema/{catalog}.{schema}/{i}'
      t.append((schema_uri,'dbx:property', f'dbx:prop/schema/{catalog}.{schema}/{i}'))
      kvpairs = re.findall(kv_pattern, property)
      for kv in kvpairs:
        t.append((property_uri, 'dbx:propertyKey',   kv[0]))
        t.append((property_uri, 'dbx:propertyValue', kv[1]))
  return t

# COMMAND ----------

def run_schema(catalog:str, schema:str):
    connection = sql.connect(
                          server_hostname = host,
                          http_path = http_path,
                          access_token = token)
    cursor = connection.cursor()
    t1 = grants_on_schema(cursor, metastore_uri, catalog, schema)
    t2 = describe_schema(cursor, catalog, schema)
    cursor.close()
    return t1+t2

# COMMAND ----------

import concurrent.futures

def run_parallel(schemas):
  triples = []

  # We can use a with statement to ensure threads are cleaned up promptly
  with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
      # Start the load operations and mark each future with its URL
      future_to_schema = {executor.submit(run_schema, catalog, schema): schema for schema in schemas}
      for future in concurrent.futures.as_completed(future_to_schema):
          schema = future_to_schema[future]
          try:
              result = future.result()
              triples = triples + result
          except Exception as exc:
              print('%r generated an exception: %s' % (schema, exc))
          else:
              print(f'{len(result)}\t{schema} ')
  return triples

triples = run_parallel(schemas=schemas[:2])
len(triples), triples

# COMMAND ----------

connection = sql.connect(
                          server_hostname = host,
                          http_path = http_path,
                          access_token = token)

cursor = connection.cursor()
cursor.schemas(catalog_name=catalog)
schemas = [(row[0]) for row in cursor.fetchall()]
cursor.close()
schemas[:10]

# COMMAND ----------


