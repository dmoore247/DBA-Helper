# Databricks notebook source
# MAGIC %pip install --quiet databricks-sql-connector

# COMMAND ----------

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = "e2-demo-field-eng.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/ead10bf07050390f"
metastore = "metastore"
namespace = "dbx"


from databricks import sql
from databricks.sql.exc import ServerOperationError, DatabaseError
import os

connection = sql.connect(
                        server_hostname = host,
                        http_path = http_path,
                        access_token = token)

cursor = connection.cursor()

cursor.execute("SELECT current_metastore()")
metastores = [(row[0]) for row in cursor.fetchall()]
metastore = metastores[0]
print(f"Metastore: {metastore}")

def catalogExtract(metastore:str) -> list[str]:
    query = """SELECT
                    catalog_name,
                    catalog_owner,
                    comment,
                    created,
                    created_by,
                    last_altered,
                    last_altered_by
    FROM `system`.`information_schema`.`catalogs`"""

    triples = []
    cursor.execute(query)
    for row in cursor.fetchall():
        triples.append((
            f"{namespace}:metastore/{metastore}", 
            f"{namespace}:hasCatalog",
            f"{namespace}:catalog/{row['catalog_name']}"))
    return triples

def schemaExtract(metastore:str) -> list[str]:
    query = """SELECT
                    catalog_name,
                    schema_name,
                    schema_owner,
                    comment,
                    created,
                    created_by,
                    last_altered,
                    last_altered_by
    FROM `system`.`information_schema`.`schemata`"""

    triples = []
    cursor.execute(query)
    for row in cursor.fetchall():
        triples.append((
            f"{namespace}:metastore/{metastore}", 
            f"{namespace}:hasSchema",
            f"{namespace}:schema/{row['catalog_name']}.{row['schema_name']}"))
    return triples


def tableExtract(metastore:str) -> list[str]:
    query = """SELECT
                    table_catalog,
                    table_schema,
                    table_name,
                    table_type,
                    is_insertable_into,
                    commit_action,
                    table_owner,
                    comment,
                    created,
                    created_by,
                    last_altered,
                    last_altered_by,
                    data_source_format,
                    storage_sub_directory
    FROM `system`.`information_schema`.`tables`"""

    triples = []
    cursor.execute(query)
    for row in cursor.fetchall():
        triples.append((
            f"{namespace}:metastore/{metastore}", 
            f"{namespace}:hasTable",
            f"{namespace}:table/{row['table_catalog']}.{row['table_schema']}.{row['table_name']}"))
    return triples

triples = catalogExtract(metastore=metastore) + schemaExtract(metastore=metastore) + tableExtract(metastore=metastore)
triples_df = spark.createDataFrame(triples, schema="s STRING, p STRING, o STRING")

# COMMAND ----------

display(triples_df)

# COMMAND ----------

triples_df.count()

# COMMAND ----------

from pyspark.sql.functions import lit
quads_df = triples_df.withColumn('g',lit('ex:metastoreScan20230603'))
quads_df.write.saveAsTable('main.triplestore.raw_quads',mode='overwrite')
